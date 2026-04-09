package raftengine

import (
	"context"
	"io"
	"time"
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
