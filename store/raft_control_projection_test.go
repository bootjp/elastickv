package store

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func configurationProjection(index uint64) []*KVPairMutation {
	return []*KVPairMutation{{Op: OpTypePut, Key: MigrationConfigurationIndexKey(),
		Value: binary.BigEndian.AppendUint64(nil, index)}}
}

func TestRaftControlProjectionWatermarkSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	api := raftMetadata(t, s)
	require.NoError(t, raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 9, func(ctx context.Context) error {
		if err := api.StageRaftEntryMutationsPreservingLastCommitTS(ctx, configurationProjection(9), nil, 1, 1); err != nil {
			return err
		}
		return api.StageRaftControlMetadata(ctx, []byte("P"), []byte("policy-9"))
	}))
	require.Zero(t, s.LastCommitTS())
	require.NoError(t, s.Close())
	s, err = NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	value, err := s.GetAt(t.Context(), MigrationConfigurationIndexKey(), 1)
	require.NoError(t, err)
	require.Equal(t, uint64(9), binary.BigEndian.Uint64(value))
	index, present := raftEntryAppliedIndex(t, s)
	require.True(t, present)
	require.Equal(t, uint64(9), index)
	require.Zero(t, s.LastCommitTS(), "configuration-only recovery must not advertise user data")
	adjacent := append(MigrationConfigurationIndexKey(), []byte("-ordinary")...)
	require.NoError(t, s.PutAt(t.Context(), adjacent, []byte("data"), 20, 0))
	require.NoError(t, s.Close())
	s, err = NewPebbleStore(dir)
	require.NoError(t, err)
	require.Equal(t, uint64(20), s.LastCommitTS(), "auxiliary classification must not consume adjacent ordinary keys")
}

func TestRaftControlProjectionConflictsSeeNonAdvertisedVersions(t *testing.T) {
	for name, factory := range absenceTestStores(t) {
		for _, separateEntry := range []bool{false, true} {
			for _, preserving := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/separate=%t/preserving=%t", name, separateEntry, preserving), func(t *testing.T) {
					testRaftControlProjectionConflict(t, factory(t), separateEntry, preserving)
				})
			}
		}
	}
}

func testRaftControlProjectionConflict(t *testing.T, s MVCCStore, separateEntry, preserving bool) {
	t.Helper()
	api := raftMetadata(t, s)
	seed := func(ctx context.Context) error {
		return api.StageRaftEntryMutationsPreservingLastCommitTS(ctx, configurationProjection(9), nil, 0, 10)
	}
	if separateEntry {
		require.NoError(t, raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 9, func(ctx context.Context) error {
			if err := seed(ctx); err != nil {
				return err
			}
			return api.StageRaftControlMetadata(ctx, []byte("P"), []byte("policy-9"))
		}))
	}
	index := uint64(9)
	if separateEntry {
		index++
	}
	require.NoError(t, raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), index, func(ctx context.Context) error {
		if !separateEntry {
			if err := seed(ctx); err != nil {
				return err
			}
		}
		var err error
		if preserving {
			err = api.StageRaftEntryMutationsPreservingLastCommitTS(ctx, configurationProjection(10),
				[][]byte{MigrationConfigurationIndexKey()}, 5, 11)
		} else {
			err = s.ApplyMutationsRaftAt(ctx, raftEntryPut("dependent-write", "must-not-land"),
				[][]byte{MigrationConfigurationIndexKey()}, 5, 11, index)
		}
		require.ErrorIs(t, err, ErrWriteConflict)
		return api.StageRaftControlMetadata(ctx, []byte("R/rejected"), []byte("conflict"))
	}))
	value, err := s.GetAt(t.Context(), MigrationConfigurationIndexKey(), 100)
	require.NoError(t, err)
	require.Equal(t, uint64(9), binary.BigEndian.Uint64(value))
	_, err = s.GetAt(t.Context(), []byte("dependent-write"), 100)
	require.ErrorIs(t, err, ErrKeyNotFound)
	require.Zero(t, s.LastCommitTS())
}

func TestRaftControlProjectionRejectsOrdinaryKeysAtomically(t *testing.T) {
	for name, factory := range absenceTestStores(t) {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			api := raftMetadata(t, s)
			err := raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 9, func(ctx context.Context) error {
				require.NoError(t, api.StageRaftControlMetadata(ctx, []byte("P"), []byte("must-not-land")))
				require.Error(t, api.StageRaftEntryMutationsPreservingLastCommitTS(ctx, raftEntryPut("ordinary", "bad"), nil, 1, 1))
				return nil
			})
			require.Error(t, err, "ignoring invalid projection staging cannot commit the earlier policy")
			_, err = api.ReadRaftControlMetadata(t.Context(), []byte("P"))
			require.ErrorIs(t, err, ErrKeyNotFound)
			_, err = s.GetAt(t.Context(), []byte("ordinary"), 100)
			require.ErrorIs(t, err, ErrKeyNotFound)
		})
	}
}

func TestRaftControlProjectionRejectsMixedKeysBeforeStaging(t *testing.T) {
	for name, factory := range absenceTestStores(t) {
		for _, knownFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/knownFirst=%t", name, knownFirst), func(t *testing.T) {
				s := factory(t)
				api := raftMetadata(t, s)
				known := configurationProjection(9)[0]
				unknown := &KVPairMutation{Op: OpTypePut, Key: append(MigrationConfigurationIndexKey(), []byte("-ordinary")...), Value: []byte("not-a-projection")}
				mutations := []*KVPairMutation{unknown, known}
				if knownFirst {
					mutations = []*KVPairMutation{known, unknown}
				}
				err := raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 9, func(ctx context.Context) error {
					require.NoError(t, api.StageRaftControlMetadata(ctx, []byte("P"), []byte("must-abort")))
					require.Error(t, api.StageRaftEntryMutationsPreservingLastCommitTS(ctx, mutations, nil, 1, 1))
					for _, key := range [][]byte{known.Key, unknown.Key} {
						_, err := s.GetAt(ctx, key, 100)
						require.ErrorIs(t, err, ErrKeyNotFound, "reject the entire mutation list before staging any row")
					}
					return nil
				})
				require.Error(t, err, "invalid projection must latch scope failure")
				require.Zero(t, s.LastCommitTS())
				_, err = api.ReadRaftControlMetadata(t.Context(), []byte("P"))
				require.ErrorIs(t, err, ErrKeyNotFound)
			})
		}
	}
}

func TestRaftControlProjectionDoesNotAdvertiseStagedMaximum(t *testing.T) {
	for name, factory := range absenceTestStores(t) {
		for _, projectionFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/projectionFirst=%t", name, projectionFirst), func(t *testing.T) {
				s := factory(t)
				api := raftMetadata(t, s)
				projection := func(ctx context.Context) error {
					return api.StageRaftEntryMutationsPreservingLastCommitTS(ctx, configurationProjection(9), nil, 100, 10)
				}
				require.NoError(t, raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 9, func(ctx context.Context) error {
					if projectionFirst {
						require.NoError(t, projection(ctx))
					}
					require.NoError(t, s.ApplyMutationsRaftAt(ctx, raftEntryPut("data", "value"), nil, 100, 6, 9))
					if !projectionFirst {
						require.NoError(t, projection(ctx))
					}
					return api.StageRaftControlMetadata(ctx, []byte("P"), []byte("policy"))
				}))
				require.Equal(t, uint64(6), s.LastCommitTS(), "only advertised data advances the watermark")
				_, err := s.GetAt(t.Context(), MigrationConfigurationIndexKey(), 10)
				require.NoError(t, err)
			})
		}
	}
}

func TestNormalMutationsCheckNonAdvertisedDependencies(t *testing.T) {
	for name, factory := range absenceTestStores(t) {
		for _, key := range [][]byte{MigrationConfigurationIndexKey(), []byte("!s3|chunkblob|dependency")} {
			for _, writeDependency := range []bool{false, true} {
				for _, path := range []string{"direct", "raft", "indexed", "fenced", "scoped"} {
					t.Run(fmt.Sprintf("%s/%s/write=%t/%s", name, key, writeDependency, path), func(t *testing.T) {
						s := factory(t)
						auxiliary, ok := s.(interface {
							ApplyMutationsPreservingLastCommitTS(context.Context, []*KVPairMutation, [][]byte, uint64, uint64) error
						})
						require.True(t, ok)
						require.NoError(t, auxiliary.ApplyMutationsPreservingLastCommitTS(t.Context(), []*KVPairMutation{{Op: OpTypePut, Key: key, Value: []byte("auxiliary")}}, nil, 10, 10))
						require.Zero(t, s.LastCommitTS())
						mutations, readKeys := raftEntryPut("dependent", "must-not-land"), [][]byte{key}
						if writeDependency {
							mutations, readKeys = []*KVPairMutation{{Op: OpTypePut, Key: key, Value: []byte("must-not-overwrite")}}, nil
						}
						require.ErrorIs(t, applyNormalDependencyPath(t, s, path, mutations, readKeys), ErrWriteConflict)
						value, err := s.GetAt(t.Context(), key, 100)
						require.NoError(t, err)
						require.Equal(t, []byte("auxiliary"), value)
						_, err = s.GetAt(t.Context(), []byte("dependent"), 100)
						require.ErrorIs(t, err, ErrKeyNotFound)
						require.Zero(t, s.LastCommitTS())
					})
				}
			}
		}
	}
}

func applyNormalDependencyPath(t *testing.T, s MVCCStore, path string, mutations []*KVPairMutation, readKeys [][]byte) error {
	t.Helper()
	switch path {
	case "direct":
		return s.ApplyMutations(t.Context(), mutations, readKeys, 5, 11)
	case "raft":
		return s.ApplyMutationsRaft(t.Context(), mutations, readKeys, 5, 11)
	case "indexed":
		return s.ApplyMutationsRaftAt(t.Context(), mutations, readKeys, 5, 11, 2)
	case "fenced":
		return s.ApplyMutationsRaftAtFenced(t.Context(), mutations, readKeys, 5, 11, 2)
	case "scoped":
		return raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 2, func(ctx context.Context) error {
			return s.ApplyMutationsRaftAt(ctx, mutations, readKeys, 5, 11, 2)
		})
	default:
		t.Fatalf("unknown normal mutation path %q", path)
		return nil
	}
}
