package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type bootstrapFormatFactory struct{ creates int }

func (*bootstrapFormatFactory) EngineType() string { return "etcd" }

func (f *bootstrapFormatFactory) Create(raftengine.FactoryConfig) (*raftengine.FactoryResult, error) {
	f.creates++
	return nil, errors.New("legacy factory must not open required bootstrap state")
}

func TestPublishedBootstrapMarkerRejectsLegacyEngineBeforeFactory(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	dir := groupDataDir(base, "n1", 1, false)
	require.NoError(t, os.MkdirAll(dir, raftDirPerm))
	intent := raftbootstrap.Intent{
		Identity:     raftbootstrap.Identity{Cluster: raftbootstrap.UUID{1}, Group: 1},
		BaselineKind: raftbootstrap.Seed,
		Baseline: raftbootstrap.Configuration{
			State: raftbootstrap.ConfState{Voters: []uint64{1}},
			Members: []raftbootstrap.Member{{
				ID: 1, Address: "127.0.0.1:50051", PublicKeyDigest: raftbootstrap.Digest{1},
			}},
		},
		Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1},
	}
	digest, err := raftbootstrap.ConfigurationDigest(intent.Baseline)
	require.NoError(t, err)
	files := raftbootstrap.NewStore(dir)
	evidence := raftbootstrap.Evidence{
		Identity: &intent.Identity, BaselineDigest: &digest, RaftState: raftbootstrap.ProvisionedSeed,
	}
	require.NoError(t, files.Publish(intent, evidence))
	inspection, err := files.Inspect(evidence)
	require.NoError(t, err)
	require.Equal(t, raftbootstrap.Required, inspection.Class)
	marker := filepath.Join(dir, raftEngineMarkerFile)
	raw, err := os.ReadFile(marker)
	require.NoError(t, err)
	require.Equal(t, []byte("etcd-bootstrap-v1\n"), raw)
	_, _, err = readRaftEngineMarker(marker)
	require.ErrorIs(t, err, ErrUnsupportedRaftEngine)
	factory := &bootstrapFormatFactory{}
	runtime, err := buildRuntimeForGroup("n1", groupSpec{id: 1}, base, false, true, nil, nil, nil, nil, factory, false)
	require.ErrorIs(t, err, ErrUnsupportedRaftEngine)
	require.Nil(t, runtime)
	require.Zero(t, factory.creates)
	retained, err := os.ReadFile(marker)
	require.NoError(t, err)
	require.Equal(t, raw, retained)
	inspection, err = files.Inspect(evidence)
	require.NoError(t, err)
	require.Equal(t, raftbootstrap.Required, inspection.Class)
}
