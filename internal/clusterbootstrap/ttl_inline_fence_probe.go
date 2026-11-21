package clusterbootstrap

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"go.etcd.io/raft/v3/raftpb"
)

// TTLInlineFenceReport is local evidence for a future peer capability exchange.
// Supported does not authorize a proposal or prove another member's capability.
type TTLInlineFenceReport struct {
	Supported          bool                    `json:"supported"`
	Group              uint64                  `json:"group"`
	LocalMember        uint64                  `json:"local_member"`
	ConfigurationIndex uint64                  `json:"configuration_index"`
	NativeRoles        raftbootstrap.ConfState `json:"native_roles"`
	Fenced             bool                    `json:"fenced"`
}

type ttlInlineRecoveredEvidence interface {
	Identity() (etcd.WALIdentity, bool)
	AppliedIndex() uint64
	ConfigurationIndex() uint64
	HardState() *raftpb.HardState
	ConfState() *raftpb.ConfState
	Peers() []etcd.Peer
}

// ProbeTTLInlineFence only inspects local durable and recovered native evidence.
// A failed probe returns a zero report so callers cannot publish partial proof.
func ProbeTTLInlineFence(spec *GroupBootstrapSpec, state etcd.RecoveredState) (TTLInlineFenceReport, error) {
	return probeTTLInlineFence(spec, state)
}

type ttlInlineRecoveryTarget interface {
	TTLInlineCapabilityRecorded() bool
	InstallTTLInlineRecoveryProof(context.Context, bool, raftengine.NativeConfiguration) error
	InvalidateTTLInlineRecoveryProof()
}

func installTTLInlineRecoveryProof(ctx context.Context, sm raftengine.StateMachine, spec *GroupBootstrapSpec, state ttlInlineRecoveredEvidence) error {
	target, ok := sm.(ttlInlineRecoveryTarget)
	if !ok {
		return nil
	}
	report, err := probeTTLInlineFence(spec, state)
	if err != nil {
		target.InvalidateTTLInlineRecoveryProof()
		if target.TTLInlineCapabilityRecorded() {
			return fmt.Errorf("recover recorded TTL-inline capability: %w", err)
		}
		return nil
	}
	native := raftengine.NativeConfiguration{Index: report.ConfigurationIndex,
		Voters: report.NativeRoles.Voters, VotersOutgoing: report.NativeRoles.VotersOutgoing,
		Learners: report.NativeRoles.Learners, LearnersNext: report.NativeRoles.LearnersNext,
		AutoLeave: report.NativeRoles.AutoLeave}
	for _, peer := range state.Peers() {
		native.Members = append(native.Members, raftengine.NativeConfigurationMember{ID: peer.NodeID, Address: peer.Address})
	}
	if err := target.InstallTTLInlineRecoveryProof(ctx, report.Fenced, native); err != nil {
		return fmt.Errorf("install TTL-inline recovery proof: %w", err)
	}
	return nil
}

func probeTTLInlineFence(spec *GroupBootstrapSpec, state ttlInlineRecoveredEvidence) (TTLInlineFenceReport, error) {
	var empty TTLInlineFenceReport
	want, member, err := ttlInlineProbeIdentity(spec, state)
	if err != nil {
		return empty, err
	}
	if err := verifyTTLInlineFenceFiles(spec, want); err != nil {
		return empty, err
	}
	if err := verifyTTLInlineRecoveredIdentity(spec, state, want, member); err != nil {
		return empty, err
	}
	index, roles, err := ttlInlineRecoveredRoles(spec, state, member)
	if err != nil {
		return empty, err
	}
	return TTLInlineFenceReport{Supported: true, Group: want.Group, LocalMember: member,
		ConfigurationIndex: index, NativeRoles: roles, Fenced: true}, nil
}

func ttlInlineProbeIdentity(spec *GroupBootstrapSpec, state ttlInlineRecoveredEvidence) (raftbootstrap.Identity, uint64, error) {
	if spec == nil || spec.Files == nil || spec.Config == nil || spec.Dir == "" || state == nil {
		return raftbootstrap.Identity{}, 0, errors.New("TTL-inline fence proof lacks protected group inputs")
	}
	want := spec.Intent.Identity
	member := spec.Config.Local.Member
	if want.Cluster == (raftbootstrap.UUID{}) || member == 0 || spec.Config.Local.Cluster != want.Cluster {
		return raftbootstrap.Identity{}, 0, errors.New("TTL-inline fence proof has invalid expected identity")
	}
	return want, member, nil
}

func verifyTTLInlineRecoveredIdentity(spec *GroupBootstrapSpec, state ttlInlineRecoveredEvidence, want raftbootstrap.Identity, member uint64) error {
	digest, err := raftbootstrap.ConfigurationDigest(spec.Intent.Baseline)
	if err != nil {
		return fmt.Errorf("TTL-inline baseline: %w", err)
	}
	identity, verified := state.Identity()
	if !verified || identity.Identity != want || identity.NodeID != member || identity.BaselineDigest != digest {
		return etcd.ErrWALIdentity
	}
	return nil
}

func ttlInlineRecoveredRoles(spec *GroupBootstrapSpec, state ttlInlineRecoveredEvidence, member uint64) (uint64, raftbootstrap.ConfState, error) {
	index, applied := state.ConfigurationIndex(), state.AppliedIndex()
	hard, conf := state.HardState(), state.ConfState()
	if index == 0 || index > applied || index < spec.Intent.Baseline.Index || hard == nil || hard.GetTerm() == 0 || hard.GetCommit() > applied || conf == nil {
		return 0, raftbootstrap.ConfState{}, errors.New("TTL-inline native recovery prefix is incomplete")
	}
	roles := raftbootstrap.ConfState{
		Voters: slices.Clone(conf.GetVoters()), VotersOutgoing: slices.Clone(conf.GetVotersOutgoing()),
		Learners: slices.Clone(conf.GetLearners()), LearnersNext: slices.Clone(conf.GetLearnersNext()),
		AutoLeave: conf.GetAutoLeave(),
	}
	if !validTTLInlineRoles(roles, member) {
		return 0, raftbootstrap.ConfState{}, errors.New("TTL-inline native membership is invalid or excludes local member")
	}
	if err := verifyTTLInlineLocalPeer(state.Peers(), member, spec.Config.Local.Address); err != nil {
		return 0, raftbootstrap.ConfState{}, err
	}
	return index, roles, nil
}

func verifyTTLInlineFenceFiles(spec *GroupBootstrapSpec, want raftbootstrap.Identity) error {
	wantIntent, err := raftbootstrap.MarshalIntent(spec.Intent)
	if err != nil {
		return fmt.Errorf("TTL-inline expected intent: %w", err)
	}
	// Inspect the configured directory independently: Store has no public Dir accessor.
	for _, files := range []*raftbootstrap.Store{spec.Files, raftbootstrap.NewStore(spec.Dir)} {
		fenced, err := files.InspectRequiredMarkerFence(want)
		if err != nil {
			return fmt.Errorf("TTL-inline required marker: %w", err)
		}
		if !fenced {
			return errors.New("TTL-inline required marker is not V2 fenced")
		}
		intent, present, err := files.LoadIntent()
		if err != nil || !present {
			return fmt.Errorf("TTL-inline durable intent missing or invalid: %w", errors.Join(err, raftbootstrap.ErrInconsistent))
		}
		gotIntent, err := raftbootstrap.MarshalIntent(intent)
		if err != nil || !bytes.Equal(gotIntent, wantIntent) {
			return fmt.Errorf("TTL-inline durable intent differs: %w", errors.Join(err, raftbootstrap.ErrInconsistent))
		}
	}
	return nil
}

func verifyTTLInlineLocalPeer(peers []etcd.Peer, member uint64, address string) error {
	localPeers := 0
	for _, peer := range peers {
		if peer.NodeID != member {
			continue
		}
		localPeers++
		if peer.Address != address {
			return errors.New("TTL-inline recovered local address differs")
		}
	}
	if localPeers != 1 {
		return errors.New("TTL-inline recovered local peer is missing or duplicated")
	}
	return nil
}

func validTTLInlineRoles(roles raftbootstrap.ConfState, member uint64) bool {
	sets := [][]uint64{roles.Voters, roles.VotersOutgoing, roles.Learners, roles.LearnersNext}
	if len(roles.Voters) == 0 {
		return false
	}
	present := false
	roleSets := make([]map[uint64]bool, len(sets))
	for i, ids := range sets {
		seen, valid := ttlInlineRoleSet(ids)
		if !valid {
			return false
		}
		roleSets[i] = seen
		present = present || seen[member]
	}
	return present && validTTLInlineRoleOverlap(roleSets, roles.AutoLeave)
}

func validTTLInlineRoleOverlap(roleSets []map[uint64]bool, autoLeave bool) bool {
	for id := range roleSets[2] {
		if roleSets[0][id] || roleSets[1][id] || roleSets[3][id] {
			return false
		}
	}
	for id := range roleSets[3] {
		if roleSets[0][id] || !roleSets[1][id] {
			return false
		}
	}
	return !autoLeave || len(roleSets[1]) != 0
}

func ttlInlineRoleSet(ids []uint64) (map[uint64]bool, bool) {
	seen := make(map[uint64]bool, len(ids))
	for _, id := range ids {
		if id == 0 || seen[id] {
			return nil, false
		}
		seen[id] = true
	}
	return seen, true
}
