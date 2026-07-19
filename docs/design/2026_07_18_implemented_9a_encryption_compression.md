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
- V1 envelopes require `flag=0`; compressed values use envelope V2.
- Unknown version/flag combinations fail closed.
- The cleartext-rebadge guard verifies both valid compression-flag variants.
- Pebble block compression is disabled for encryption-wired stores, including
  database reopen and snapshot-restore temporary databases.

Raft proposal envelopes remain uncompressed. Their apply path is latency
sensitive and the storage layer already performs the only value-compression
pass after proposal decryption.

## Compatibility

Existing V1 envelopes have `flag=0` and remain readable. Compressed writes use
V2 with `FlagCompressed`, so a V1-only reader rejects the version instead of
returning Snappy-framed bytes as plaintext during a rolling rollback. Legacy
cleartext operation and stores with no encryption wiring retain Pebble's
default block-compression policy. Visibility-only scans authenticate V2 rows
without decompressing values they will discard.

## Verification

- Storage round trips for compressed, uncompressed, empty, and already
  compressed values.
- Authenticated compression-flag tamper rejection.
- V1-reader fail-closed rejection of compressed V2 envelopes.
- Fail-closed handling for an authenticated malformed Snappy payload.
- Prefix-delete visibility checks that authenticate without Snappy expansion.
- Property testing over arbitrary byte slices for compression framing.
- Pebble compression-policy tests for encrypted and legacy stores.
- Paired cleartext/encrypted 1 KiB storage benchmark for `benchstat` review.
