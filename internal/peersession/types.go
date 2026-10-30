package peersession

import (
	"context"
	"crypto/sha256"
	"net"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
)

type Nonce [32]byte

type Node struct {
	Cluster raftbootstrap.UUID
	Member  uint64
	Address string
	Key     raftbootstrap.Digest // SHA-256 of x509 RawSubjectPublicKeyInfo.
}

type Provision struct {
	Node   Node
	Groups []uint64
}

type State uint8

const (
	Intact  State = 1
	Prejoin State = 2
	Lost    State = 3
)

type Stage uint8

const (
	Reconciling Stage = iota
	IdentityReady
	ControlReady
	DataReady
)

// Pending is intentionally not a boolean with a permissive zero value.
type Pending uint8

const (
	PendingUnknown Pending = iota
	PendingClear
	PendingTransition
)

// Group is an owner-published projection, NOT a received peer assertion. Every
// update revokes old sessions before publishing the new generation. A default
// Pending/Stage keeps restricted traffic closed. Configuration comes from the
// locally recovered applied roster, never the provisioned prospective roster.
// Group.Pending concerns local journal/publication reconciliation. A replicated
// learner reservation does not stop voter replication: the candidate transition
// is checked separately by CatchupStatus.Pending.
type Group struct {
	ID            uint64
	Generation    uint64
	Configuration raftbootstrap.Configuration
	Policy        raftbootstrap.Digest
	Floor         raftbootstrap.Floor
	Minimum       raftbootstrap.Versions
	State         State
	Stage         Stage
	Pending       Pending
	MarkerReady   bool
}

type Traffic uint8

const (
	Probe Traffic = iota + 1
	Catchup
	Vote
	Replication
	Registration
	Control
	Data
	PolicyControl // Identity-lane activation/reconfiguration, no user timestamps.
)

// Message must be decoded from the actual payload by a trusted method adapter.
// Raft adapters must fully decode the inner message and derive Traffic/From/To
// from it, not from metadata, flags, capability fields or caller context.
type Message struct {
	Group    uint64
	From, To uint64
	Traffic  Traffic
}

type Decoder func(any) (Message, error)
type ReplyDecoder func(request Message, reply any) (Message, error)
type StreamDecoders struct {
	Request Decoder
	Reply   ReplyDecoder
}

// StreamFactory creates fresh per-stream decoding state, for example to bind
// metadata-free snapshot chunks to their validated initial metadata. It is a
// trusted parser factory, not an authorizer; no I/O or spooling belongs here.
// Its decoders must support gRPC's concurrent send/receive directions safely.
type StreamFactory func(Binding) (StreamDecoders, error)
type Method struct {
	Name    string
	Request Decoder
	Reply   ReplyDecoder
	Stream  StreamFactory
}

func validDecoders(m Method) bool {
	if (m.Request == nil) != (m.Reply == nil) {
		return false
	}
	return m.Request != nil || m.Stream != nil
}

func decodersForStream(h *Handle, m Method) (Method, error) {
	if !h.owned() {
		return Method{}, ErrDenied
	}
	if m.Stream != nil {
		decoders, err := m.Stream(h.s.binding)
		if err != nil {
			return Method{}, err
		}
		m.Request, m.Reply = decoders.Request, decoders.Reply
	}
	if m.Request == nil || m.Reply == nil {
		return Method{}, ErrDenied
	}
	return m, nil
}

// Binding is minted from the accepted physical connection and its transcript.
// It is supplied TO the trusted authorizer, not accepted from an RPC caller.
type Binding struct {
	Session               raftbootstrap.Digest
	Group, Generation     uint64
	Local, Remote         Node
	LocalBoot, RemoteBoot Nonce
	Configuration, Policy raftbootstrap.Digest
}

type GrantPhase uint8

const (
	GrantUnknown GrantPhase = iota
	GrantReciprocallyAcknowledged
)

// CatchupStatus must be a CURRENT projection of verified v5 G from the policy
// owner, including D, epoch, fixed read barrier and leader-term fencing. This
// package does not mint/sign G or treat applied-add evidence as authority.
// Omitted Pending/Phase or a nil authorizer denies. Recheck runs outside Owner's
// mutex, with session/generation/role checks before AND after the callback. It
// must be bounded and honor cancellation; it receives a session-owned context.
type CatchupStatus struct {
	Binding             Binding
	Pending             Pending
	Phase               GrantPhase
	Candidate, Leader   Node
	LeaderTerm, Barrier uint64
	AppliedAdd, Grant   raftbootstrap.Digest
}

type CatchupAuthorizer interface {
	Recheck(context.Context, Binding) (CatchupStatus, error)
}

type Config struct {
	Local         Node
	Provisioned   []Provision
	Versions      raftbootstrap.Versions
	Methods       []Method
	PublicMethods []string // Exact allowlist; all other unknown methods deny.
	Catchup       CatchupAuthorizer
}

var (
	ErrDenied = errors.New("peer session denied")
	ErrFormat = errors.New("invalid peer session format")
	ErrClosed = errors.New("peer session closed")
)

func validNode(n Node) bool {
	if n.Cluster == (raftbootstrap.UUID{}) || n.Member == 0 || n.Key == (raftbootstrap.Digest{}) {
		return false
	}
	if !utf8.ValidString(n.Address) || strings.ContainsAny(n.Address, " \t\r\n\x00") {
		return false
	}
	host, port, err := net.SplitHostPort(n.Address)
	if err != nil || host == "" {
		return false
	}
	p, err := strconv.ParseUint(port, 10, 16)
	return err == nil && p > 0
}

func validVersions(v raftbootstrap.Versions) bool {
	return v == (raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1})
}

func validState(s State) bool { return s >= Intact && s <= Lost }

func validateGroup(g Group) error {
	if g.Generation == 0 || !validVersions(g.Minimum) || !validState(g.State) {
		return ErrFormat
	}
	if g.Stage > DataReady || g.Pending > PendingTransition {
		return ErrFormat
	}
	if (g.Floor.ActivationIndex == 0) != (g.Floor.Generation == 0) {
		return ErrFormat
	}
	_, err := raftbootstrap.ConfigurationDigest(g.Configuration)
	return errors.Wrap(err, "peer group configuration")
}

func copyGroup(g Group) Group {
	g.Configuration.Members = slices.Clone(g.Configuration.Members)
	s := &g.Configuration.State
	s.Voters = slices.Clone(s.Voters)
	s.VotersOutgoing = slices.Clone(s.VotersOutgoing)
	s.Learners = slices.Clone(s.Learners)
	s.LearnersNext = slices.Clone(s.LearnersNext)
	return g
}

func keyDigest(spki []byte) raftbootstrap.Digest { return sha256.Sum256(spki) }
