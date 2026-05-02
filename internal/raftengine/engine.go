package raftengine

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/bootjp/elastickv/internal/monoclock"
)

// Shared sentinel errors that both engine implementations should wrap
// so callers can test with errors.Is across engine backends.
var (
	// ErrNotLeader indicates the operation was rejected because the
	// local node is not the Raft leader for the target group.
	// Callers that care about leadership (e.g. lease invalidation
	// logic) should match via errors.Is.
	ErrNotLeader = errors.New("raft engine: not leader")
	// ErrLeadershipLost indicates the local node was leader when the
	// operation began but lost leadership before it could complete.
	ErrLeadershipLost = errors.New("raft engine: leadership lost")
	// ErrLeadershipTransferInProgress indicates a leadership transfer
	// is under way and proposals are being held back.
	ErrLeadershipTransferInProgress = errors.New("raft engine: leadership transfer in progress")
)

type State string

const (
	StateFollower  State = "follower"
	StateCandidate State = "candidate"
	StateLeader    State = "leader"
	StateShutdown  State = "shutdown"
	StateUnknown   State = "unknown"
)

type Server struct {
	ID       string
	Address  string
	Suffrage string
}

type LeaderInfo struct {
	ID      string
	Address string
}

type Configuration struct {
	Servers []Server
}

type Status struct {
	State             State
	Leader            LeaderInfo
	Term              uint64
	CommitIndex       uint64
	AppliedIndex      uint64
	LastLogIndex      uint64
	LastSnapshotIndex uint64
	FSMPending        uint64
	NumPeers          uint64
	LastContact       time.Duration
	// LeadTransferee is non-zero on the current leader while a leadership
	// transfer is in progress, and zero otherwise (including on followers).
	// Writers should hold new proposals while this is non-zero, since etcd/raft
	// drops proposals during transfer.
	LeadTransferee uint64
}

type ProposalResult struct {
	CommitIndex uint64
	Response    any
}

type Proposer interface {
	Propose(ctx context.Context, data []byte) (*ProposalResult, error)
}

type LeaderView interface {
	State() State
	Leader() LeaderInfo
	VerifyLeader(ctx context.Context) error
	// LinearizableRead blocks until the returned index is safe to read from the
	// local FSM on that node.
	LinearizableRead(ctx context.Context) (uint64, error)
}

// LeaseProvider is an optional capability implemented by engines that support
// leader-local lease reads. Callers that want lease-based reads should
// type-assert to this interface and fall back to LinearizableRead when the
// underlying engine does not implement it.
type LeaseProvider interface {
	// LeaseDuration returns the time during which a lease holder can serve
	// reads from local state without re-confirming leadership via ReadIndex.
	LeaseDuration() time.Duration
	// AppliedIndex returns the highest log index applied to the local FSM.
	AppliedIndex() uint64
	// LastQuorumAck returns the monotonic-raw instant at which the
	// engine most recently observed majority liveness on the leader
	// -- i.e. the CLOCK_MONOTONIC_RAW reading at which a quorum of
	// follower Progress entries had responded. The engine maintains
	// this in the background from MsgHeartbeatResp / MsgAppResp traffic
	// on the leader, so a fast-path lease read does not need to issue
	// its own ReadIndex to "warm" the lease.
	//
	// Safety: callers must verify the lease against a single
	// `now := monoclock.Now()` sample:
	//   state == raftengine.StateLeader &&
	//   !now.IsZero() && !ack.IsZero() && !ack.After(now) &&
	//   now.Sub(ack) < LeaseDuration()
	//
	// The !now.IsZero() guard fails closed when the caller's
	// clock_gettime read errored (e.g. seccomp denies it) and
	// monoclock.Now() returned the zero Instant; without it, a
	// persistent clock failure could keep a once-warmed lease valid
	// forever. See kv.engineLeaseAckValid.
	//
	// The monotonic-raw clock (CLOCK_MONOTONIC_RAW on Linux / Darwin;
	// runtime-monotonic fallback on FreeBSD / Windows / others, see
	// internal/monoclock) is immune to NTP rate adjustment and
	// wall-clock step events on the raw-clock platforms, so the
	// comparison stays safe even if the system's time daemon slews
	// or steps the wall clock. The !ack.After(now) guard remains as
	// a defensive fail-closed for a zero / bogus ack reading.
	// LeaseDuration is bounded by electionTimeout - safety_margin,
	// guaranteeing no successor leader has accepted writes within
	// that window.
	//
	// Returns the zero Instant when no quorum has been confirmed yet
	// or when the local node is not the leader. Single-node LEADERS
	// may return a recent monoclock.Now() since self is the quorum;
	// non-leader single-node replicas still return the zero Instant.
	LastQuorumAck() monoclock.Instant
	// RegisterLeaderLossCallback registers fn to be invoked whenever the
	// local node leaves the leader role (graceful transfer, partition
	// step-down, or shutdown). Callers use this to invalidate any
	// leader-local lease they hold so the next read takes the slow path.
	// Multiple callbacks can be registered.
	//
	// Callbacks fire synchronously from the engine's status-refresh
	// / shutdown path and MUST be non-blocking -- each should be a
	// lock-free flag flip (e.g. atomic invalidate). A panicking
	// callback is contained so a bug in one holder cannot break
	// others, but a blocking callback would stall the engine's main
	// loop, so the contract is strict. Lease-read fast paths also
	// guard on engine.State() to close the narrow race between a
	// transition and this callback completing.
	//
	// The returned function deregisters this callback and is safe to
	// call multiple times. Callers whose lifetime is shorter than the
	// engine's (ephemeral Coordinators in tests, for example) MUST
	// invoke the returned deregister when they are done so the engine
	// does not accumulate dead callbacks.
	RegisterLeaderLossCallback(fn func()) (deregister func())
}

type StatusReader interface {
	Status() Status
}

type ConfigReader interface {
	Configuration(ctx context.Context) (Configuration, error)
}

type HealthReader interface {
	CheckServing(ctx context.Context) error
}

type Admin interface {
	LeaderView
	StatusReader
	ConfigReader
	AddVoter(ctx context.Context, id string, address string, prevIndex uint64) (uint64, error)
	// AddLearner attaches a non-voting replica that receives MsgApp /
	// MsgSnap and applies log entries locally but does not contribute
	// to the voter quorum. Use this instead of AddVoter when joining
	// a fresh node so the cluster's effective fault tolerance is not
	// reduced during catch-up. Promote with PromoteLearner once the
	// learner has caught up. See
	// docs/design/2026_04_26_proposed_raft_learner.md.
	AddLearner(ctx context.Context, id string, address string, prevIndex uint64) (uint64, error)
	// PromoteLearner promotes an existing learner to voter. The
	// minAppliedIndex precondition is enforced against the leader's
	// Progress[nodeID].Match before the conf change is proposed: if
	// the learner has not yet caught up to that index, the call
	// returns FailedPrecondition.
	//
	// minAppliedIndex == 0 is REJECTED unless skipMinAppliedCheck is
	// also true, so an operator running a copy-pasted script that
	// omits the catch-up check gets a clean FailedPrecondition
	// instead of a silent quorum stall. Set skipMinAppliedCheck only
	// when the catch-up has been confirmed out-of-band.
	PromoteLearner(ctx context.Context, id string, prevIndex uint64, minAppliedIndex uint64, skipMinAppliedCheck bool) (uint64, error)
	RemoveServer(ctx context.Context, id string, prevIndex uint64) (uint64, error)
	TransferLeadership(ctx context.Context) error
	TransferLeadershipToServer(ctx context.Context, id string, address string) error
	// RegisterLeaderAcquiredCallback registers fn to fire every
	// time the local node's Raft state transitions INTO leader
	// (initial election, re-election, transfer target completion).
	// Callbacks fire on the previous!=Leader → status==Leader edge
	// AFTER the engine has published isLeader, so a callback that
	// calls engine.State() observes StateLeader.
	//
	// Use case: per-shard policy hooks that need to audit a
	// freshly-acquired leadership ("am I still allowed to be
	// leader of this group?"). The SQS HT-FIFO leadership-refusal
	// hook (§8 of the split-queue FIFO design) hangs off this to
	// TransferLeadership when the binary lacks the htfifo
	// capability but a partitioned queue is mapped to this Raft
	// group.
	//
	// Same non-blocking + panic-contained contract as
	// LeaseProvider.RegisterLeaderLossCallback. A callback that
	// needs to do real work (enumerate the catalog, call
	// TransferLeadership) MUST offload to a goroutine.
	//
	// The returned function deregisters this specific registration
	// and is safe to call multiple times.
	RegisterLeaderAcquiredCallback(fn func()) (deregister func())
}

type Engine interface {
	Proposer
	LeaderView
	StatusReader
	ConfigReader
	io.Closer
}
