# Stage 9B: production KEK providers

Status: Implemented
Author: bootjp
Date: 2026-07-18

## Scope

Stage 9B completes the §5.1 KEK source matrix:

- `--kekUri=aws-kms://<key-arn>` uses an immutable AWS KMS `key/` ARN with
  `Encrypt` / `Decrypt`, derives
  the client region from the ARN, and binds every request to the fixed
  `elastickv-purpose=dek-wrap-v1` encryption context.
- `--kekUri=gcp-kms://<crypto-key-resource>` uses Google Cloud KMS with fixed
  AAD. Request and response CRC32C values are supplied and verified before a
  wrapped DEK or plaintext DEK is accepted.
- `--kekUri=vault-transit://<mount>/<key>` uses Vault Transit through standard
  `VAULT_*` client configuration. Binary DEKs are base64 encoded for the API;
  only `vault:v<positive-version>:<non-empty-payload>` ciphertexts are accepted.
- `ELASTICKV_KEK_BASE64` supplies a 32-byte test/CI-only static KEK. The
  variable is unset immediately after the decode attempt, including malformed
  input. It is also unset when source ambiguity is rejected.
- `--kekFile` keeps its existing owner-only regular-file contract.

File, URI, and environment sources are mutually exclusive. Ambiguous
configuration fails closed instead of selecting a source by precedence.

## Runtime wiring

The startup loader returns the actual loaded `kek.Wrapper`. Startup guards now
derive `KEKConfigured` from that non-nil wrapper rather than from `--kekFile`
text. The same loaded-state boolean is threaded to the EncryptionAdmin mutator
gate, so URI and environment providers can bootstrap/rotate while a failed or
absent provider cannot expose mutating RPCs.

Before that gate can open, startup performs a real random 32-byte DEK
wrap/unwrap round trip and compares the result in constant time. This proves
provider reachability, credentials, encrypt/decrypt permission, key binding,
and response shape on a fresh data directory where no sidecar DEK exists to
exercise the provider. A failed preflight refuses process start before Raft or
the mutating RPC surface is available.

Remote wrappers use a 30-second operation deadline and reject nil, empty,
integrity-invalid, or non-32-byte provider responses before sidecar state can be
updated. Provider clients are safe for concurrent use and are hidden behind
narrow interfaces for deterministic tests.

## Compatibility and verification

No envelope, sidecar, Raft opcode, or snapshot format changes. Existing wrapped
DEKs remain provider-specific opaque bytes, and the `kek.Wrapper` contract is
unchanged.

Verification includes:

- fake-client unit tests for AWS encryption context, GCP AAD/CRC32C, and Vault
  binary request/response encoding;
- malformed provider response, strict Vault ciphertext, immutable AWS key ARN,
  wrong DEK length, invalid URI, source conflict, environment-unset, and
  startup wrap/unwrap preflight tests;
- a production-wiring integration test that loads the environment KEK through
  the source selector, bootstraps both DEKs, enables the storage envelope,
  writes encrypted data, snapshots it, restores it into an encrypted Pebble
  store, and reads the original plaintext;
- startup/mutator caller audit confirming the gate consumes loaded-wrapper
  state rather than the legacy file flag.

## Remaining work

Stage 5E discovery batching and Stage 9C+ rotation-budget, rewrap, rewrite,
retirement, metrics, remaining benchmark, and encrypted Jepsen requirements are
not part of this milestone.
