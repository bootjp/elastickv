package raftadmin

import "context"

// MembershipChangeInterceptor lets a caller of [Server] inject a
// pre-step into [Server.AddVoter] and [Server.AddLearner] that runs
// **before** the underlying Raft engine proposes the configuration
// change. A non-nil error from PreAddMember aborts the conf-change
// proposal; the error is returned to the gRPC caller verbatim.
//
// Stage 7c uses this hook to pre-register a new node's writer-
// registry row (see
// docs/design/2026_05_29_implemented_7c_confchange_time_registration.md).
// Keeping the encryption-aware adapter outside this package preserves
// raftadmin's engine-generic posture — no concrete dependency on
// the KV or encryption layers.
//
// The hook is intentionally optional: when nil is passed to [NewServer]
// (or [NewServerWithInterceptor] is not used), AddVoter/AddLearner run
// exactly as they did pre-7c. Encryption-unaware builds and
// encryption-disabled clusters therefore see no behavior change.
type MembershipChangeInterceptor interface {
	// PreAddMember runs on the leader before AddVoter/AddLearner
	// proposes the conf-change. The raftID is the same string the
	// caller passed in the AddVoter/AddLearner request. address is the
	// target node's Raft/gRPC endpoint and may be used for capability checks.
	PreAddMember(ctx context.Context, raftID, address string) error
}
