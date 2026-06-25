// Package encryption implements the AES-256-GCM envelope encryption scheme
// described in
// docs/design/2026_04_29_proposed_data_at_rest_encryption.md §4.
//
// Stage 0 (foundation) provides the primitive Encrypt/Decrypt operations,
// the wire format envelope encoder/decoder, and the in-memory keystore.
// Composition of AAD bytes for storage-layer envelopes (§4.1) and
// raft-layer envelopes (§4.2) is the responsibility of callers in
// store/ and internal/raftengine/etcd/, added in later stages.
//
// Wire format (§4.1):
//
//	+--------+------+---------+----------+-----------+--------+
//	| 0x01   | flag | key_id  | nonce    | ciphertext| tag    |
//	| 1 byte | 1 B  | 4 bytes | 12 bytes | N bytes   | 16 B   |
//	+--------+------+---------+----------+-----------+--------+
//
// Per-value overhead is 34 bytes (HeaderSize + TagSize).
package encryption
