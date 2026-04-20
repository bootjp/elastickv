package raftengine

import (
	"context"
	"errors"
	"io"
	"time"
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
	RemoveServer(ctx context.Context, id string, prevIndex uint64) (uint64, error)
	TransferLeadership(ctx context.Context) error
	TransferLeadershipToServer(ctx context.Context, id string, address string) error
}

type Engine interface {
	Proposer
	LeaderView
	StatusReader
	ConfigReader
	io.Closer
}
