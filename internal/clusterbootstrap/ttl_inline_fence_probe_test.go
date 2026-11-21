package clusterbootstrap

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

type ttlInlineRecoveryFixture struct {
	identity etcd.WALIdentity
	index    uint64
	conf     *raftpb.ConfState
	peers    []etcd.Peer
}

func (f ttlInlineRecoveryFixture) Identity() (etcd.WALIdentity, bool) { return f.identity, true }
func (f ttlInlineRecoveryFixture) AppliedIndex() uint64               { return f.index }
func (f ttlInlineRecoveryFixture) ConfigurationIndex() uint64         { return f.index }
func (f ttlInlineRecoveryFixture) HardState() *raftpb.HardState {
	term := uint64(1)
	return &raftpb.HardState{Term: &term}
}
func (f ttlInlineRecoveryFixture) ConfState() *raftpb.ConfState { return f.conf }
func (f ttlInlineRecoveryFixture) Peers() []etcd.Peer           { return f.peers }

func ttlInlineProbeFixture(t *testing.T) (*GroupBootstrapSpec, ttlInlineRecoveryFixture) {
	t.Helper()
	config, intent, _ := bootstrapStartupFixture()
	dir := t.TempDir()
	encoded, err := raftbootstrap.MarshalIntent(intent)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, raftbootstrap.IntentFile), encoded, 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, raftbootstrap.MarkerFile), []byte(raftbootstrap.MarkerV2), 0600))
	digest, err := raftbootstrap.ConfigurationDigest(intent.Baseline)
	require.NoError(t, err)
	fixture := ttlInlineRecoveryFixture{
		identity: etcd.WALIdentity{Identity: intent.Identity, NodeID: config.Local.Member, BaselineDigest: digest},
		index:    1,
		conf:     &raftpb.ConfState{Voters: []uint64{config.Local.Member}},
		peers:    []etcd.Peer{{NodeID: config.Local.Member, Address: config.Local.Address}},
	}
	return &GroupBootstrapSpec{Config: config, Files: raftbootstrap.NewStore(dir), Intent: intent, Dir: dir}, fixture
}

func TestProbeTTLInlineFenceSupported(t *testing.T) {
	t.Parallel()
	spec, recovered := ttlInlineProbeFixture(t)
	report, err := probeTTLInlineFence(spec, recovered)
	require.NoError(t, err)
	require.Equal(t, TTLInlineFenceReport{Supported: true, Group: spec.Intent.Identity.Group,
		LocalMember: spec.Config.Local.Member, ConfigurationIndex: 1,
		NativeRoles: raftbootstrap.ConfState{Voters: []uint64{spec.Config.Local.Member}}, Fenced: true}, report)
}

func TestProbeTTLInlineFenceFailsClosed(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *GroupBootstrapSpec, *ttlInlineRecoveryFixture)
	}{
		{"missing-marker", func(t *testing.T, spec *GroupBootstrapSpec, _ *ttlInlineRecoveryFixture) {
			require.NoError(t, os.Remove(filepath.Join(spec.Dir, raftbootstrap.MarkerFile)))
		}},
		{"v1-marker", func(t *testing.T, spec *GroupBootstrapSpec, _ *ttlInlineRecoveryFixture) {
			require.NoError(t, os.WriteFile(filepath.Join(spec.Dir, raftbootstrap.MarkerFile), []byte(raftbootstrap.MarkerV1), 0600))
		}},
		{"intent-mismatch", func(_ *testing.T, spec *GroupBootstrapSpec, _ *ttlInlineRecoveryFixture) {
			spec.Intent.Identity.Group++
		}},
		{"wal-mismatch", func(_ *testing.T, _ *GroupBootstrapSpec, recovered *ttlInlineRecoveryFixture) {
			recovered.identity.NodeID++
		}},
		{"missing-native-conf", func(_ *testing.T, _ *GroupBootstrapSpec, recovered *ttlInlineRecoveryFixture) { recovered.conf = nil }},
		{"missing-local-member", func(_ *testing.T, _ *GroupBootstrapSpec, recovered *ttlInlineRecoveryFixture) {
			recovered.conf.Voters = []uint64{999}
		}},
		{"contradictory-roles", func(_ *testing.T, spec *GroupBootstrapSpec, recovered *ttlInlineRecoveryFixture) {
			recovered.conf.Learners = []uint64{spec.Config.Local.Member}
		}},
		{"missing-config-index", func(_ *testing.T, _ *GroupBootstrapSpec, recovered *ttlInlineRecoveryFixture) { recovered.index = 0 }},
		{"wrong-local-address", func(_ *testing.T, _ *GroupBootstrapSpec, recovered *ttlInlineRecoveryFixture) {
			recovered.peers[0].Address = "localhost:9999"
		}},
		{"wrong-dir", func(t *testing.T, spec *GroupBootstrapSpec, _ *ttlInlineRecoveryFixture) { spec.Dir = t.TempDir() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec, recovered := ttlInlineProbeFixture(t)
			tc.mutate(t, spec, &recovered)
			report, err := probeTTLInlineFence(spec, recovered)
			require.Error(t, err)
			require.Equal(t, TTLInlineFenceReport{}, report)
		})
	}
	// The public API cannot accept a fabricated zero-value native recovery state.
	spec, _ := ttlInlineProbeFixture(t)
	report, err := ProbeTTLInlineFence(spec, etcd.RecoveredState{})
	require.Error(t, err)
	require.Equal(t, TTLInlineFenceReport{}, report)
}

func TestInstallTTLInlineRecoveryProof(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		breakFence bool
		breakState bool
		wantErr    bool
	}{
		{name: "matching durable proof"},
		{name: "unfenced directory", breakFence: true, wantErr: true},
		{name: "unverified native state", breakState: true, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec, recovered := ttlInlineProbeFixture(t)
			st := store.NewMVCCStore()
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			writer, ok := st.(store.ReplicatedControlStore)
			require.True(t, ok)
			marker := fmt.Sprintf(`{"version":1,"index":1,"voters":[%d]}`, spec.Config.Local.Member)
			_, err := writer.ApplyReplicatedControlState(context.Background(),
				[]byte(store.ReplicatedControlPrefix+"absence-capability"), []byte(marker), 2)
			require.NoError(t, err)
			sm := kv.NewKvFSMWithHLC(st, nil)
			target, ok := sm.(ttlInlineRecoveryTarget)
			require.True(t, ok)
			reader, ok := sm.(interface{ TTLInlineCapabilityActive() bool })
			require.True(t, ok)
			require.True(t, target.TTLInlineCapabilityRecorded())
			if tc.breakFence {
				require.NoError(t, os.WriteFile(filepath.Join(spec.Dir, raftbootstrap.MarkerFile), []byte(raftbootstrap.MarkerV1), 0600))
			}
			if tc.breakState {
				recovered.identity.NodeID++
			}
			err = installTTLInlineRecoveryProof(t.Context(), sm, spec, recovered)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, !tc.wantErr, reader.TTLInlineCapabilityActive())
			target.InvalidateTTLInlineRecoveryProof()
			require.False(t, reader.TTLInlineCapabilityActive())
		})
	}
}

func TestInstallTTLInlineRecoveryProofWithoutRecordedMarkerKeepsLegacyServing(t *testing.T) {
	t.Parallel()
	spec, recovered := ttlInlineProbeFixture(t)
	require.NoError(t, os.WriteFile(filepath.Join(spec.Dir, raftbootstrap.MarkerFile), []byte(raftbootstrap.MarkerV1), 0600))
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	sm := kv.NewKvFSMWithHLC(st, nil)
	require.NoError(t, installTTLInlineRecoveryProof(t.Context(), sm, spec, recovered))
	reader, ok := sm.(interface{ TTLInlineCapabilityActive() bool })
	require.True(t, ok)
	require.False(t, reader.TTLInlineCapabilityActive())
}
