package encryption

import "github.com/cockroachdb/errors"

var (
	// ErrUnknownKeyID is returned when a wrap/unwrap call references a key_id
	// that is not present in the Keystore. Surfaces as `unknown_key_id` on
	// the §9.2 elastickv_encryption_decrypt_failures_total counter.
	ErrUnknownKeyID = errors.New("encryption: unknown key_id")

	// ErrReservedKeyID is returned when a caller tries to install or use
	// key_id 0; that value is reserved cluster-wide as the
	// "not bootstrapped" sentinel per §5.1.
	ErrReservedKeyID = errors.New("encryption: key_id 0 is reserved as the not-bootstrapped sentinel")

	// ErrBadNonceSize indicates the nonce passed to Encrypt/Decrypt was not
	// exactly NonceSize bytes.
	ErrBadNonceSize = errors.New("encryption: nonce size invalid")

	// ErrBadKeySize indicates the DEK passed to Keystore.Set was not exactly
	// KeySize bytes (AES-256 requires 32).
	ErrBadKeySize = errors.New("encryption: DEK size invalid")

	// ErrIntegrity indicates a GCM tag mismatch on Decrypt — i.e., the
	// ciphertext was tampered with, the AAD does not match the one used at
	// Encrypt, or the wrong DEK is loaded. Per §4.1, callers MUST treat this
	// as a typed read error and never silently zero or retry.
	ErrIntegrity = errors.New("encryption: integrity check failed (GCM tag mismatch)")

	// ErrEnvelopeShort indicates DecodeEnvelope received fewer bytes than the
	// minimum envelope size (HeaderSize + TagSize).
	ErrEnvelopeShort = errors.New("encryption: envelope shorter than header+tag")

	// ErrEnvelopeVersion indicates DecodeEnvelope saw a version byte the
	// current build does not know how to parse. Reserved values per §11.3.
	ErrEnvelopeVersion = errors.New("encryption: unknown envelope version")

	// ErrNilKeystore indicates NewCipher was called with a nil Keystore.
	// Surfaced at construction time so a wiring mistake is caught
	// before the first Encrypt/Decrypt would otherwise nil-deref panic.
	ErrNilKeystore = errors.New("encryption: keystore is nil")

	// ErrKeyConflict indicates Keystore.Set was called with a keyID
	// already loaded under DIFFERENT key material. Replacing live key
	// bytes for an in-use key_id would render every envelope already
	// persisted under that id undecryptable, so Set fails closed
	// rather than silently overwriting. Set with the SAME bytes is
	// idempotent (returns nil) and does not raise this error.
	ErrKeyConflict = errors.New("encryption: key_id already loaded with different key material")

	// ErrUnsupportedFilesystem indicates the parent directory of the
	// sidecar cannot guarantee crash-durability of os.Rename via
	// fsync (typical on NFS, some FUSE mounts). Per §5.1 the
	// encryption package refuses to start in that situation rather
	// than silently degrading the durability guarantee. Two paths
	// surface this sentinel:
	//
	//   - WriteSidecar wraps any fsync-on-directory failure on the
	//     real keys.json write path. This catches the failure on the
	//     first encryption-relevant write — which on a fresh cluster
	//     may be hours into operation, well past the point where
	//     catching the misconfiguration would have been cheap.
	//
	//   - ProbeSidecarFilesystem (Stage 6C-2) runs at process startup
	//     and exercises the same write+rename+dir.Sync sequence on a
	//     sentinel file, then deletes it. The probe surfaces the
	//     failure BEFORE any encryption-relevant Raft entry commits,
	//     so the operator gets the unambiguous startup-time refusal
	//     rather than a halted apply loop later.
	ErrUnsupportedFilesystem = errors.New("encryption: filesystem does not support durable directory sync (NFS, some FUSE mounts are unsupported)")

	// ErrSidecarActiveKeyMissing indicates the Sidecar has a non-zero
	// Active.{Storage,Raft} key_id that does not appear in the Keys
	// map. The two halves are written together by every rotation /
	// bootstrap path; an Active id without a corresponding wrapped
	// DEK is malformed input.
	ErrSidecarActiveKeyMissing = errors.New("encryption: sidecar active key_id has no entry in keys map")

	// ErrSidecarActivePurposeMismatch indicates the Sidecar has a
	// non-zero Active.{Storage,Raft} key_id pointing to a Keys entry
	// whose Purpose does not match the slot. e.g., active.storage=7
	// but Keys["7"].purpose == "raft". Crossed pointers would route
	// the wrong DEK into a purpose-specific encryption path after
	// restart or rotation, so the reader fails closed.
	ErrSidecarActivePurposeMismatch = errors.New("encryption: sidecar active key_id references a key with mismatched purpose")

	// ErrEncryptionApply is the §6.3 / §11.3 fatal-apply sentinel
	// surfaced by encryption FSM handlers (kv/fsm_encryption.go)
	// when one of the opcodes (0x03 registration, 0x04 bootstrap,
	// 0x05 rotation) cannot be applied — malformed payload,
	// KEK-unwrap failure, local-epoch rollback, sidecar write
	// failure, etc.
	//
	// The FSM packs this error in a haltApplyResponse value;
	// internal/raftengine/etcd's applyNormalCommitted recognises
	// the HaltApply interface, returns the error, and runLoop's
	// fatal-error path takes the process down without advancing
	// setApplied — the next restart must replay the entry.
	//
	// Defined here (and not in internal/raftengine/etcd) so
	// kv/fsm_encryption.go can errors.Mark its handler outputs
	// without importing the engine package, which would close
	// the kv ↔ engine cycle (engine_test imports kv as a fake
	// FSM).
	ErrEncryptionApply = errors.New("encryption: FSM apply failed; halting apply (see design §6.3)")

	// ErrKEKNotConfigured is the defense-in-depth marker returned
	// by Applier.ApplyBootstrap and Applier.ApplyRotation when no
	// KEK unwrapper is wired (Stage 6A scaffolding before Stage 6B
	// fills the KEK plumbing). It is wrapped with ErrEncryptionApply
	// at the kv/fsm dispatch layer so it routes through the same
	// HaltApply seam as any other applier error.
	//
	// Per PR #762's Stage 6 plan, the production safety boundary
	// is the gRPC-layer mutator gate in registerEncryptionAdminServer
	// (which Stage 5D left OFF, and which 6B re-enables only when
	// both --encryption-enabled is set AND KEKConfigured() is true);
	// this typed error exists so a future refactor that bypasses
	// that gate produces a named, grep-able failure mode rather
	// than a nil-pointer panic deep in the apply path.
	ErrKEKNotConfigured = errors.New("encryption: KEK not configured on this node; cannot unwrap wrapped DEK material")

	// ErrSidecarPresentWithoutFlag is the §9.1 startup-refusal guard
	// raised when the data dir already contains a sidecar (keys.json)
	// but --encryption-enabled is NOT set. Continuing would silently
	// downgrade the cluster to cleartext: new writes would land
	// unencrypted while the prior wrapped DEKs sit untouched on disk,
	// inverting the operator's intent. The process refuses to start
	// rather than half-honor a prior bootstrap. Recovery is either
	// to set --encryption-enabled (resume encrypted operation) or to
	// move/delete the sidecar with deliberate runbook acknowledgement.
	ErrSidecarPresentWithoutFlag = errors.New("encryption: sidecar present on disk but --encryption-enabled is not set; refusing to start to avoid silent downgrade to cleartext (set --encryption-enabled or remove the sidecar per the §9.1 runbook)")

	// ErrKEKRequiredWithFlag is the §9.1 startup-refusal guard
	// raised when --encryption-enabled is set but no KEK source
	// (--kekFile) is supplied. Without a KEK the applier cannot
	// unwrap any sidecar DEK, so the first mutating EncryptionAdmin
	// RPC would HaltApply on every replica via ErrKEKNotConfigured.
	// The Stage 6B-2 mutator gate keeps that path unreachable at
	// the RPC boundary, but a flag-on / KEK-off node is misconfigured
	// and the operator's clear intent (enable encryption) cannot be
	// satisfied. Fail fast at startup rather than discover the
	// mismatch later via a halted apply loop.
	ErrKEKRequiredWithFlag = errors.New("encryption: --encryption-enabled is set but no KEK source (--kekFile) was provided; refusing to start (set --kekFile or unset --encryption-enabled)")

	// ErrKEKMismatch is the §9.1 startup-refusal guard raised when
	// the data dir contains a sidecar whose wrapped DEKs do NOT
	// decrypt under the configured KEK. The classic operator
	// error this catches is "wrong --kekFile points at a key from
	// a different cluster / environment" — continuing past it
	// would render every encrypted value on disk effectively
	// permanently lost the moment a write tries to use the wrong
	// DEK. Recovery requires the operator to either point
	// --kekFile at the correct KEK file or restore the data dir
	// from a backup that matches the supplied KEK.
	ErrKEKMismatch = errors.New("encryption: configured KEK cannot unwrap one or more wrapped DEKs in the sidecar; refusing to start (verify --kekFile matches the KEK that bootstrapped this data dir)")

	// ErrLocalEpochExhausted is the §9.1 startup-refusal guard
	// raised when any active DEK in the sidecar has reached the
	// uint16 saturation value (0xFFFF). The §4.1 nonce construction
	// reserves only 16 bits for local_epoch, so a node that already
	// emitted a nonce with local_epoch == 0xFFFF cannot safely
	// emit another one under the same DEK without rolling the
	// counter back to 0 and re-issuing a nonce that has already
	// been used (GCM catastrophic — distinct plaintexts encrypted
	// under the same (key, nonce) pair reveal plaintext XOR via
	// the keystream). Recovery is a deliberate DEK rotation (§5.2)
	// which retires the exhausted DEK; the next process startup
	// then sees a fresh DEK with local_epoch=0 and can proceed.
	//
	// The check runs at process startup, before any apply or new
	// write can reach the cipher; an active DEK with
	// local_epoch==0xFFFF is a guaranteed-future nonce-reuse
	// liability that must be rotated before the node is allowed
	// to participate.
	ErrLocalEpochExhausted = errors.New("encryption: active DEK has reached local_epoch=0xFFFF saturation; refusing to start (rotate the affected DEK via `encryption rotate-dek` before the next startup or risk GCM nonce reuse — see §4.1)")

	// ErrSidecarBehindRaftLog is the §9.1 startup-refusal guard
	// raised when the sidecar's raft_applied_index is behind the
	// raftengine's persisted applied index AND the gap covers any
	// SIDECAR-MUTATING Raft entry (per §5.5's
	// IsEncryptionRelevantOpcode predicate: 0x04 OpBootstrap,
	// 0x05 OpRotation, plus the reserved opcodes 0x06 / 0x07
	// from the fsmwire OpEncryption range). 0x03 OpRegistration
	// is in the OpEncryption range but is NOT sidecar-mutating
	// — its apply path writes writer-registry rows only and
	// never WriteSidecar, so registration-only gaps would be
	// spurious refusals.
	//
	// The classic scenario this catches is a partial-write crash:
	// an encryption-relevant entry was applied to the engine and
	// the engine's applied index was advanced, but the §5.1
	// sidecar write that should have committed the corresponding
	// keys.json change did not complete. On the next startup the
	// node sees a stale sidecar that does not reflect the
	// already-Raft-committed mutation, and the encryption package
	// would silently rebuild keystore state from an outdated
	// wrapped-DEK snapshot — a fail-closed read would fire on the
	// next post-cutover entry with `unknown_key_id`, halting apply.
	//
	// Refusing at startup with this typed error means the operator
	// sees a single unambiguous failure pointing at the right
	// runbook (`encryption resync-sidecar`) rather than a
	// downstream HaltApply on the first post-cutover Raft entry.
	//
	// Restricting the gap check to specific encryption opcodes
	// (rather than ANY gap) keeps non-encryption-relevant lag
	// from forcing a spurious refusal. See §5.5 for the
	// IsEncryptionRelevantOpcode rationale.
	ErrSidecarBehindRaftLog = errors.New("encryption: sidecar raft_applied_index is behind the raftengine's persisted applied index and the gap covers an encryption-relevant entry; refusing to start (run `encryption resync-sidecar` to advance the sidecar past the encryption-relevant entries before retrying — see §9.1 + §5.5)")
)
