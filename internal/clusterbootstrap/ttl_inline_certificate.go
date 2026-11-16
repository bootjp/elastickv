package clusterbootstrap

import (
	"context"
	"crypto/rand"
	"fmt"
	"slices"
	"time"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

const ttlInlineCertificateTimeout = 5 * time.Second

// TTLInlineCapabilityCertificate is transient evidence from every member in
// one native leader cut. It is not a replicated marker or an activation grant.
type TTLInlineCapabilityCertificate struct {
	Group              uint64
	ConfigurationIndex uint64
	Term               uint64
	Native             raftengine.NativeConfiguration
	Members            []uint64
}

type ttlInlineCertificateEngine interface {
	ttlInlineNativeSnapshot
	State() raftengine.State
}

type ttlInlineCapabilityProbe func(context.Context, uint64, string, *pb.GetTTLInlineCapabilityRequest) (*pb.GetTTLInlineCapabilityResponse, error)

// CollectRuntimeTTLInlineCapabilityCertificate uses the process's protected
// transport and current Raft/FSM instances. It cannot activate a capability.
func CollectRuntimeTTLInlineCapabilityCertificate(ctx context.Context, rt *GroupRuntime) (TTLInlineCapabilityCertificate, error) {
	var empty TTLInlineCapabilityCertificate
	if rt == nil || rt.GroupRuntime == nil || rt.BootstrapPeerConfig == nil ||
		rt.BootstrapNetwork == nil || rt.BootstrapNetwork.Control == nil {
		return empty, errors.New("TTL-inline capability collection requires a protected runtime")
	}
	engine, ok := rt.SnapshotEngine().(ttlInlineCertificateEngine)
	if !ok {
		return empty, errors.New("TTL-inline native snapshot is unsupported")
	}
	fsm, ok := rt.StateMachine.(ttlInlineFenceState)
	if !ok {
		return empty, errors.New("TTL-inline local fence is unsupported")
	}
	return CollectTTLInlineCapabilityCertificate(ctx, rt.Spec.ID, rt.BootstrapPeerConfig,
		engine, fsm, rt.BootstrapNetwork.Control.ProbeTTLInlineCapability)
}

// CollectTTLInlineCapabilityCertificate refuses missing, stale, or mismatched
// voter and learner proof. The caller must still gate any marker proposal on
// the same native cut and the Raft admission boundary.
func CollectTTLInlineCapabilityCertificate(ctx context.Context, group uint64, config *BootstrapPeerConfig,
	engine ttlInlineCertificateEngine, fsm ttlInlineFenceState, probe ttlInlineCapabilityProbe,
) (TTLInlineCapabilityCertificate, error) {
	var empty TTLInlineCapabilityCertificate
	if !ttlInlineCertificateReady(ctx, group, config, engine, fsm, probe) {
		return empty, errors.New("TTL-inline capability collector is not ready")
	}
	native, term, ok := engine.NativeConfigurationSnapshot()
	if !ok || native.Index == 0 || term == 0 {
		return empty, errors.New("TTL-inline leader native cut is unavailable")
	}
	members, addresses, err := ttlInlineCertificateMembers(native, config, group)
	if err != nil {
		return empty, err
	}
	if err := ttlInlineProbeAllMembers(ctx, group, config.Local.Member, native, term, members, addresses, probe); err != nil {
		return empty, err
	}
	if !ttlInlineCertificateCutCurrent(ctx, engine, fsm, native, term) {
		return empty, errors.New("TTL-inline leader cut changed during peer proof")
	}
	return TTLInlineCapabilityCertificate{Group: group, ConfigurationIndex: native.Index, Term: term,
		Native: native, Members: members}, nil
}

func ttlInlineCertificateCutCurrent(ctx context.Context, engine ttlInlineCertificateEngine, fsm ttlInlineFenceState,
	native raftengine.NativeConfiguration, term uint64,
) bool {
	after, afterTerm, ok := engine.NativeConfigurationSnapshot()
	return ok && ctx.Err() == nil && engine.State() == raftengine.StateLeader && fsm.TTLInlineFenceReady() &&
		afterTerm == term && sameTTLInlineNativeCut(native, after)
}

func ttlInlineCertificateReady(ctx context.Context, group uint64, config *BootstrapPeerConfig,
	engine ttlInlineCertificateEngine, fsm ttlInlineFenceState, probe ttlInlineCapabilityProbe,
) bool {
	return ctx != nil && group != 0 && config != nil && engine != nil && fsm != nil && probe != nil &&
		engine.State() == raftengine.StateLeader && fsm.TTLInlineFenceReady()
}

func ttlInlineProbeAllMembers(ctx context.Context, group, local uint64, native raftengine.NativeConfiguration, term uint64,
	members []uint64, addresses map[uint64]string, probe ttlInlineCapabilityProbe,
) error {
	bounded, cancel := context.WithTimeout(ctx, ttlInlineCertificateTimeout)
	defer cancel()
	workers, workerCtx := errgroup.WithContext(bounded)
	for _, member := range members {
		if member == local {
			continue
		}
		address := addresses[member]
		workers.Go(func() error {
			return ttlInlineProbeMember(workerCtx, group, member, address, native, term, probe)
		})
	}
	return errors.WithStack(workers.Wait())
}

func ttlInlineProbeMember(ctx context.Context, group, member uint64, address string,
	native raftengine.NativeConfiguration, term uint64, probe ttlInlineCapabilityProbe,
) error {
	var challenge [ttlInlineChallengeSize]byte
	if _, err := rand.Read(challenge[:]); err != nil {
		return errors.Wrap(err, "generate TTL-inline peer challenge")
	}
	request := &pb.GetTTLInlineCapabilityRequest{GroupId: group, Challenge: challenge[:],
		ExpectedConfigurationIndex: native.Index, ExpectedTerm: term}
	response, err := probe(ctx, group, address, request)
	if err != nil {
		return errors.Wrapf(err, "probe TTL-inline member %d", member)
	}
	if !ttlInlineCertificateReplyMatches(response, request, native, member) {
		return errors.Errorf("TTL-inline member %d returned a mismatched native cut", member)
	}
	return nil
}

func ttlInlineCertificateMembers(native raftengine.NativeConfiguration, config *BootstrapPeerConfig, group uint64) ([]uint64, map[uint64]string, error) {
	roles := raftbootstrap.ConfState{Voters: native.Voters, VotersOutgoing: native.VotersOutgoing,
		Learners: native.Learners, LearnersNext: native.LearnersNext, AutoLeave: native.AutoLeave}
	if !validTTLInlineRoles(roles, config.Local.Member) {
		return nil, nil, errors.New("TTL-inline native roles exclude the local member")
	}
	members := append([]uint64{}, native.Voters...)
	members = append(members, native.VotersOutgoing...)
	members = append(members, native.Learners...)
	members = append(members, native.LearnersNext...)
	slices.Sort(members)
	members = slices.Compact(members)
	actual, err := ttlInlineNativeAddresses(native.Members)
	if err != nil {
		return nil, nil, err
	}
	provisioned := bootstrapSessionNodes(config, group)
	addresses := make(map[uint64]string, len(members))
	for _, id := range members {
		peer, ok := provisioned[id]
		if !ok || peer.Member != id || peer.Address == "" || actual[id] != peer.Address {
			return nil, nil, fmt.Errorf("TTL-inline member %d lacks a matching native and peer address", id)
		}
		addresses[id] = peer.Address
	}
	if actual[config.Local.Member] != config.Local.Address {
		return nil, nil, errors.New("TTL-inline local address differs from native configuration")
	}
	return members, addresses, nil
}

func ttlInlineNativeAddresses(members []raftengine.NativeConfigurationMember) (map[uint64]string, error) {
	actual := make(map[uint64]string, len(members))
	for _, member := range members {
		if member.ID == 0 || member.Address == "" || actual[member.ID] != "" {
			return nil, errors.New("TTL-inline native member address is missing or repeated")
		}
		actual[member.ID] = member.Address
	}
	return actual, nil
}

func ttlInlineCertificateReplyMatches(reply *pb.GetTTLInlineCapabilityResponse, request *pb.GetTTLInlineCapabilityRequest,
	native raftengine.NativeConfiguration, member uint64,
) bool {
	return bootstrapExactProto(reply) && reply.GetSupported() && reply.GetFenced() &&
		reply.GetGroupId() == request.GetGroupId() && reply.GetMemberId() == member &&
		reply.GetConfigurationIndex() == native.Index && reply.GetTerm() == request.GetExpectedTerm() &&
		slices.Equal(reply.GetChallenge(), request.GetChallenge()) &&
		sameTTLInlineRoles(native, reply)
}

func sameTTLInlineRoles(native raftengine.NativeConfiguration, reply *pb.GetTTLInlineCapabilityResponse) bool {
	return sameTTLInlineIDs(native.Voters, reply.GetVoters()) &&
		sameTTLInlineIDs(native.VotersOutgoing, reply.GetVotersOutgoing()) &&
		sameTTLInlineIDs(native.Learners, reply.GetLearners()) &&
		sameTTLInlineIDs(native.LearnersNext, reply.GetLearnersNext()) &&
		native.AutoLeave == reply.GetAutoLeave()
}

func sameTTLInlineIDs(left, right []uint64) bool {
	left, right = slices.Clone(left), slices.Clone(right)
	slices.Sort(left)
	slices.Sort(right)
	return slices.Equal(left, right)
}

func sameTTLInlineNativeCut(left, right raftengine.NativeConfiguration) bool {
	return left.Index == right.Index && left.AutoLeave == right.AutoLeave &&
		slices.Equal(left.Voters, right.Voters) && slices.Equal(left.VotersOutgoing, right.VotersOutgoing) &&
		slices.Equal(left.Learners, right.Learners) && slices.Equal(left.LearnersNext, right.LearnersNext) &&
		slices.Equal(left.Members, right.Members)
}
