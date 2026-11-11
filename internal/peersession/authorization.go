package peersession

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strings"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/metadata"
)

const sessionHeader = "x-elastickv-peer-session"
const proofHeader = "x-elastickv-peer-proof"

var errCatchupRequired = errors.New("owned catch-up recheck required")

func rpcProof(s *session, method string) raftbootstrap.Digest {
	e := &encoder{}
	e.text(method)
	e.fixed(s.id)
	e.fixed(s.binding.Group)
	h := hmac.New(sha256.New, s.key[:])
	h.Write([]byte("elastickv/bootstrap-v1/client-rpc\x00"))
	h.Write(e.Bytes())
	var digest raftbootstrap.Digest
	copy(digest[:], h.Sum(nil))
	return digest
}

func incomingDigest(ctx context.Context, name string) (raftbootstrap.Digest, error) {
	var digest raftbootstrap.Digest
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return digest, ErrDenied
	}
	values := md.Get(name)
	if len(values) != 1 || len(values[0]) != hex.EncodedLen(len(digest)) || strings.ToLower(values[0]) != values[0] {
		return digest, ErrDenied
	}
	if _, err := hex.Decode(digest[:], []byte(values[0])); err != nil {
		return digest, ErrDenied
	}
	return digest, nil
}

func (o *Owner) incoming(ctx context.Context, method string) (*Handle, error) {
	c, err := o.serverConnection(ctx)
	if err != nil {
		return nil, denied()
	}
	id, err := incomingDigest(ctx, sessionHeader)
	if err != nil {
		return nil, denied()
	}
	supplied, err := incomingDigest(ctx, proofHeader)
	if err != nil {
		return nil, denied()
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	s := findSession(c, id)
	if s == nil || s.phase != complete || !o.currentLocked(c, s) {
		return nil, denied()
	}
	want := rpcProof(s, method)
	if !hmac.Equal(want[:], supplied[:]) {
		return nil, denied()
	}
	return &Handle{owner: o, c: c, s: s}, nil
}

func (h *Handle) validate(ctx context.Context, message Message, outbound bool) error {
	if !h.owned() {
		return denied()
	}
	o := h.owner
	o.mu.Lock()
	decision := h.authorizationLocked(message, outbound)
	o.mu.Unlock()
	if errors.Is(decision, errCatchupRequired) {
		return h.recheckCatchup(ctx, message, outbound)
	}
	if decision != nil {
		return denied()
	}
	return nil
}

func (h *Handle) authorizationLocked(message Message, outbound bool) error {
	o := h.owner
	if !o.currentLocked(h.c, h.s) || h.s.phase != complete {
		return ErrDenied
	}
	if !o.validRouteLocked(h, message, outbound) {
		return ErrDenied
	}
	return o.authorizeLocked(h.s, message.Traffic)
}

func (h *Handle) recheckCatchup(ctx context.Context, message Message, outbound bool) error {
	owned, cancel := boundContext(ctx, h.s.ctx)
	defer cancel()
	current, err := h.owner.catchup.Recheck(owned, h.s.binding)
	if err != nil || owned.Err() != nil {
		return denied()
	}
	o := h.owner
	o.mu.Lock()
	defer o.mu.Unlock()
	if !errors.Is(h.authorizationLocked(message, outbound), errCatchupRequired) {
		return denied()
	}
	if err := o.checkCatchupStatusLocked(h.s, o.groups[message.Group], current); err != nil {
		return denied()
	}
	return nil
}

func (o *Owner) validRouteLocked(h *Handle, m Message, outbound bool) bool {
	b := h.s.binding
	if m.Group != b.Group || !o.allowedLocked(b.Local, b.Group) || !o.allowedLocked(b.Remote, b.Group) {
		return false
	}
	from, to := b.Remote.Member, b.Local.Member
	if outbound {
		from, to = to, from
	}
	return m.From == from && m.To == to
}

func (o *Owner) authorizeLocked(s *session, traffic Traffic) error {
	if traffic == Probe {
		return nil
	}
	g := o.groups[s.binding.Group]
	if g.Pending != PendingClear || g.Stage < IdentityReady || !s.local.MarkerReady || !s.remote.MarkerReady {
		return ErrDenied
	}
	if s.local.State == Lost || s.remote.State == Lost {
		return ErrDenied
	}
	return o.authorizeTrafficLocked(s, g, traffic)
}

func (o *Owner) authorizeTrafficLocked(s *session, g Group, traffic Traffic) error {
	switch traffic {
	case Probe:
		return nil
	case Catchup:
		return o.needsCatchupLocked(g)
	case Replication:
		return o.authorizeReplicationLocked(s, g)
	case Vote, PolicyControl:
		return requireVoters(s, g, IdentityReady)
	case Registration:
		return requireMembers(s, g)
	case Control, Data:
		return requireActivated(s, g, traffic)
	default:
		return ErrDenied
	}
}

func (o *Owner) authorizeReplicationLocked(s *session, g Group) error {
	if intactVoters(s, g) {
		return nil
	}
	return o.needsCatchupLocked(g)
}

func requireActivated(s *session, g Group, traffic Traffic) error {
	if g.Floor.ActivationIndex == 0 || g.Policy == (raftbootstrap.Digest{}) {
		return ErrDenied
	}
	stage := ControlReady
	if traffic == Data {
		stage = DataReady
	}
	return requireVoters(s, g, stage)
}

func requireMembers(s *session, g Group) error {
	if !intactMembers(s, g) {
		return ErrDenied
	}
	return nil
}

func requireVoters(s *session, g Group, stage Stage) error {
	if g.Stage < stage || !intactVoters(s, g) {
		return ErrDenied
	}
	return nil
}

func memberMatches(n Node, g Group) bool {
	for _, m := range g.Configuration.Members {
		if m.ID == n.Member {
			return m.Address == n.Address && m.PublicKeyDigest == n.Key
		}
	}
	return false
}
func voter(n Node, g Group) bool {
	s := g.Configuration.State
	return memberMatches(n, g) && (slices.Contains(s.Voters, n.Member) || slices.Contains(s.VotersOutgoing, n.Member))
}
func learner(n Node, g Group) bool {
	s := g.Configuration.State
	return memberMatches(n, g) && slices.Contains(s.Learners, n.Member) && !slices.Contains(s.Voters, n.Member) && !slices.Contains(s.VotersOutgoing, n.Member) && !slices.Contains(s.LearnersNext, n.Member)
}
func intactMembers(s *session, g Group) bool {
	return s.local.State == Intact && s.remote.State == Intact && memberMatches(s.binding.Local, g) && memberMatches(s.binding.Remote, g)
}
func intactVoters(s *session, g Group) bool {
	return intactMembers(s, g) && voter(s.binding.Local, g) && voter(s.binding.Remote, g)
}

func (o *Owner) needsCatchupLocked(g Group) error {
	if o.catchup == nil || g.Floor.ActivationIndex == 0 {
		return ErrDenied
	}
	return errCatchupRequired
}

func (o *Owner) checkCatchupStatusLocked(s *session, g Group, current CatchupStatus) error {
	if err := validateCatchupStatus(s, current); err != nil {
		return err
	}
	// The leader side checks its OWN applied roster as well as G. The candidate
	// side relies on its trusted authorizer's signature/lineage verification;
	// it cannot claim locally applied state before receiving its first snapshot.
	if current.Leader == o.local && (!voter(current.Leader, g) || !learner(current.Candidate, g)) {
		return ErrDenied
	}
	return nil
}

func validateCatchupStatus(s *session, c CatchupStatus) error {
	if c.Binding != s.binding || c.Pending != PendingClear || c.Phase != GrantReciprocallyAcknowledged {
		return ErrDenied
	}
	if !completeGrant(c) || !catchupEndpoints(s, c) {
		return ErrDenied
	}
	return nil
}
func completeGrant(c CatchupStatus) bool {
	return c.LeaderTerm != 0 && c.Barrier != 0 && c.AppliedAdd != (raftbootstrap.Digest{}) && c.Grant != (raftbootstrap.Digest{})
}

func catchupEndpoints(s *session, c CatchupStatus) bool {
	if c.Leader == s.binding.Local && c.Candidate == s.binding.Remote {
		return s.local.State == Intact && s.remote.State != Lost
	}
	if c.Leader == s.binding.Remote && c.Candidate == s.binding.Local {
		return s.remote.State == Intact && s.local.State != Lost
	}
	return false
}
