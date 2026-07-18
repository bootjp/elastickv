# Stage 9A data-at-rest compression

Status: Implemented
Author: bootjp
Date: 2026-07-18

## Scope

This milestone implements the compress-then-encrypt storage path from
`2026_04_29_partial_data_at_rest_encryption.md` §6.4.

- Snappy runs on plaintext before AES-256-GCM.
- The compressed representation is selected only when it is strictly smaller.
- `FlagCompressed` is authenticated as part of the envelope AAD.
- Reads decompress only after GCM authentication succeeds.
- Unknown v1 flag bits fail closed.
- The cleartext-rebadge guard verifies both valid compression-flag variants.
- Pebble block compression is disabled for encryption-wired stores, including
  database reopen and snapshot-restore temporary databases.

Raft proposal envelopes remain uncompressed. Their apply path is latency
sensitive and the storage layer already performs the only value-compression
pass after proposal decryption.

## Compatibility

Existing v1 envelopes have `flag=0` and remain readable. New readers accept
both `flag=0` and `FlagCompressed`; legacy cleartext operation and stores with
no encryption wiring retain Pebble's default block-compression policy.

## Verification

- Storage round trips for compressed, uncompressed, empty, and already
  compressed values.
- Authenticated compression-flag tamper rejection.
- Fail-closed handling for an authenticated malformed Snappy payload.
- Property testing over arbitrary byte slices for compression framing.
- Pebble compression-policy tests for encrypted and legacy stores.
- Paired cleartext/encrypted 1 KiB storage benchmark for `benchstat` review.
