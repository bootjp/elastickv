package clusterbootstrap

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/raftenginetest"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

type ttlInlineCertificateEngineStub struct {
	native      raftengine.NativeConfiguration
	term        uint64
	state       raftengine.State
	changeTerm  bool
	snapshotNum atomic.Int64
}

type ttlInlineRuntimeProposalEngine struct {
	raftenginetest.StubEngine
	native raftengine.NativeConfiguration
	term   uint64
	called bool
}

func (e *ttlInlineRuntimeProposalEngine) NativeConfigurationSnapshot() (raftengine.NativeConfiguration, uint64, bool) {
	return e.native, e.term, true
}

func (e *ttlInlineRuntimeProposalEngine) ProposeTTLInlineCapability(_ context.Context, native raftengine.NativeConfiguration, term uint64, payload []byte) (*raftengine.ProposalResult, error) {
	e.called = true
	if term != e.term || !sameTTLInlineNativeCut(native, e.native) {
		return nil, errors.New("incorrect certificate cut")
	}
	return &raftengine.ProposalResult{CommitIndex: 6}, (&ttlInlineMarkerPayloadProbe{native: native}).validate(payload)
}

type ttlInlineMarkerPayloadProbe struct {
	native raftengine.NativeConfiguration
}

func (p *ttlInlineMarkerPayloadProbe) validate(payload []byte) error {
	want, err := kv.EncodeTTLInlineCapabilityMarker(p.native)
	if err != nil {
		return err
	}
	if !bytes.Equal(payload, want) {
		return errors.New("incorrect marker payload")
	}
	return nil
}

type ttlInlineRuntimeFenceFSM struct{ raftengine.StateMachine }

func (*ttlInlineRuntimeFenceFSM) TTLInlineFenceReady() bool { return true }

func TestProposeRuntimeTTLInlineCapabilityUsesCollectedCut(t *testing.T) {
	t.Parallel()
	native := raftengine.NativeConfiguration{Index: 5, Voters: []uint64{1},
		Members: []raftengine.NativeConfigurationMember{{ID: 1, Address: "n1:50051"}}}
	engine := &ttlInlineRuntimeProposalEngine{native: native, term: 9}
	engine.StaticLeaderView.StateValue = raftengine.StateLeader
	runtime := &GroupRuntime{GroupRuntime: &noderuntime.GroupRuntime{
		Spec: noderuntime.GroupSpec{ID: 7}, Engine: engine, StateMachine: &ttlInlineRuntimeFenceFSM{},
	}, BootstrapPeerConfig: &BootstrapPeerConfig{
		Local:      peersession.Node{Member: 1, Address: "n1:50051"},
		Provisions: []peersession.Provision{{Node: peersession.Node{Member: 1, Address: "n1:50051"}, Groups: []uint64{7}}},
	}, BootstrapNetwork: &BootstrapNetwork{Control: &bootstrapControlClient{}}}
	result, err := ProposeRuntimeTTLInlineCapability(t.Context(), runtime, &kv.ShardGroup{Engine: engine})
	require.NoError(t, err)
	require.Equal(t, uint64(6), result.CommitIndex)
	require.True(t, engine.called)
}

func (e *ttlInlineCertificateEngineStub) State() raftengine.State { return e.state }

func (e *ttlInlineCertificateEngineStub) NativeConfigurationSnapshot() (raftengine.NativeConfiguration, uint64, bool) {
	n := e.snapshotNum.Add(1)
	if e.changeTerm && n > 1 {
		return e.native, e.term + 1, true
	}
	return e.native, e.term, true
}

func ttlInlineCertificateFixture() (*BootstrapPeerConfig, *ttlInlineCertificateEngineStub) {
	config := &BootstrapPeerConfig{Local: peersession.Node{Member: 1, Address: "n1:50051"},
		Provisions: []peersession.Provision{
			{Node: peersession.Node{Member: 1, Address: "n1:50051"}, Groups: []uint64{7}},
			{Node: peersession.Node{Member: 2, Address: "n2:50051"}, Groups: []uint64{7}},
			{Node: peersession.Node{Member: 3, Address: "n3:50051"}, Groups: []uint64{7}},
		}}
	engine := &ttlInlineCertificateEngineStub{state: raftengine.StateLeader, term: 9,
		native: raftengine.NativeConfiguration{Index: 5, Voters: []uint64{1, 2}, Learners: []uint64{3},
			Members: []raftengine.NativeConfigurationMember{
				{ID: 1, Address: "n1:50051"}, {ID: 2, Address: "n2:50051"}, {ID: 3, Address: "n3:50051"},
			}}}
	return config, engine
}

func ttlInlineCertificateReply(member uint64, req *pb.GetTTLInlineCapabilityRequest) *pb.GetTTLInlineCapabilityResponse {
	return &pb.GetTTLInlineCapabilityResponse{GroupId: req.GetGroupId(), MemberId: member,
		ConfigurationIndex: req.GetExpectedConfigurationIndex(), Term: req.GetExpectedTerm(),
		Voters: []uint64{2, 1}, Learners: []uint64{3}, Fenced: true, Supported: true,
		Challenge: bytes.Clone(req.GetChallenge())}
}

func TestCollectTTLInlineCapabilityCertificateChecksEveryNativeMember(t *testing.T) {
	t.Parallel()
	config, engine := ttlInlineCertificateFixture()
	var calls atomic.Int64
	certificate, err := CollectTTLInlineCapabilityCertificate(t.Context(), 7, config, engine,
		ttlInlineFenceStateStub{ready: true}, func(_ context.Context, group uint64, address string, req *pb.GetTTLInlineCapabilityRequest) (*pb.GetTTLInlineCapabilityResponse, error) {
			calls.Add(1)
			require.Equal(t, uint64(7), group)
			require.Len(t, req.GetChallenge(), ttlInlineChallengeSize)
			member := uint64(2)
			if address == "n3:50051" {
				member = 3
			} else {
				require.Equal(t, "n2:50051", address)
			}
			return ttlInlineCertificateReply(member, req), nil
		})
	require.NoError(t, err)
	require.Equal(t, int64(2), calls.Load())
	require.Equal(t, uint64(5), certificate.ConfigurationIndex)
	require.Equal(t, uint64(9), certificate.Term)
	require.Equal(t, []uint64{1, 2, 3}, certificate.Members)
	require.Equal(t, int64(2), engine.snapshotNum.Load())
}

func TestCollectTTLInlineCapabilityCertificateRejectsBadPeerProof(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func(*pb.GetTTLInlineCapabilityResponse)
	}{
		{"wrong member", func(r *pb.GetTTLInlineCapabilityResponse) { r.MemberId = 4 }},
		{"wrong term", func(r *pb.GetTTLInlineCapabilityResponse) { r.Term++ }},
		{"wrong cut", func(r *pb.GetTTLInlineCapabilityResponse) { r.ConfigurationIndex++ }},
		{"wrong roles", func(r *pb.GetTTLInlineCapabilityResponse) { r.Learners = nil }},
		{"wrong challenge", func(r *pb.GetTTLInlineCapabilityResponse) { r.Challenge[0]++ }},
		{"unfenced", func(r *pb.GetTTLInlineCapabilityResponse) { r.Fenced = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config, engine := ttlInlineCertificateFixture()
			certificate, err := CollectTTLInlineCapabilityCertificate(t.Context(), 7, config, engine,
				ttlInlineFenceStateStub{ready: true}, func(_ context.Context, _ uint64, address string, req *pb.GetTTLInlineCapabilityRequest) (*pb.GetTTLInlineCapabilityResponse, error) {
					member := uint64(2)
					if address == "n3:50051" {
						member = 3
					}
					response := ttlInlineCertificateReply(member, req)
					if member == 2 {
						tc.mutate(response)
					}
					return response, nil
				})
			require.Error(t, err)
			require.Empty(t, certificate.Members)
		})
	}
}

func TestCollectTTLInlineCapabilityCertificateRejectsIncompleteOrChangedCut(t *testing.T) {
	t.Parallel()
	config, engine := ttlInlineCertificateFixture()
	engine.native.Members = slices.Delete(engine.native.Members, 2, 3)
	_, err := CollectTTLInlineCapabilityCertificate(t.Context(), 7, config, engine,
		ttlInlineFenceStateStub{ready: true}, func(context.Context, uint64, string, *pb.GetTTLInlineCapabilityRequest) (*pb.GetTTLInlineCapabilityResponse, error) {
			t.Fatal("must not probe an incomplete roster")
			return nil, nil
		})
	require.Error(t, err)

	config, engine = ttlInlineCertificateFixture()
	engine.changeTerm = true
	_, err = CollectTTLInlineCapabilityCertificate(t.Context(), 7, config, engine,
		ttlInlineFenceStateStub{ready: true}, func(_ context.Context, _ uint64, address string, req *pb.GetTTLInlineCapabilityRequest) (*pb.GetTTLInlineCapabilityResponse, error) {
			member := uint64(2)
			if address == "n3:50051" {
				member = 3
			}
			return ttlInlineCertificateReply(member, req), nil
		})
	require.Error(t, err)

	config, engine = ttlInlineCertificateFixture()
	_, err = CollectTTLInlineCapabilityCertificate(t.Context(), 7, config, engine,
		ttlInlineFenceStateStub{ready: true}, func(context.Context, uint64, string, *pb.GetTTLInlineCapabilityRequest) (*pb.GetTTLInlineCapabilityResponse, error) {
			return nil, errors.New("unreachable")
		})
	require.Error(t, err)
}

func TestCollectRuntimeTTLInlineCapabilityCertificateRequiresProtectedNetwork(t *testing.T) {
	t.Parallel()
	for _, runtime := range []*GroupRuntime{
		nil,
		{},
		{GroupRuntime: &noderuntime.GroupRuntime{}},
		{GroupRuntime: &noderuntime.GroupRuntime{}, BootstrapPeerConfig: &BootstrapPeerConfig{}},
	} {
		certificate, err := CollectRuntimeTTLInlineCapabilityCertificate(t.Context(), runtime)
		require.Error(t, err)
		require.Empty(t, certificate.Members)
	}
}
