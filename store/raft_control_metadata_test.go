package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func raftMetadata(t *testing.T, s MVCCStore) RaftControlMetadataStore {
	t.Helper()
	api, ok := s.(RaftControlMetadataStore)
	require.True(t, ok)
	return api
}

func TestRaftControlMetadataAtomicVisibility(t *testing.T) {
	for name, factory := range absenceTestStores(t) {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			api := raftMetadata(t, s)
			require.NoError(t, raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 7, func(ctx context.Context) error {
				require.NoError(t, api.StageRaftControlMetadata(ctx, []byte("P"), []byte("policy")))
				require.NoError(t, s.ApplyMutationsRaftAt(ctx, raftEntryPut("inner", "data"), nil, 10, 10, 7))
				got, err := api.ReadRaftControlMetadata(ctx, []byte("P"))
				require.NoError(t, err)
				require.Equal(t, []byte("policy"), got)
				got[0] = 'x'
				got, err = api.ReadRaftControlMetadata(ctx, []byte("P"))
				require.NoError(t, err)
				require.Equal(t, []byte("policy"), got)
				got, err = s.GetAt(ctx, []byte("inner"), 10)
				require.NoError(t, err)
				require.Equal(t, []byte("data"), got)
				_, err = api.ReadRaftControlMetadata(t.Context(), []byte("P"))
				require.ErrorIs(t, err, ErrKeyNotFound)
				_, err = s.GetAt(t.Context(), []byte("inner"), 10)
				require.ErrorIs(t, err, ErrKeyNotFound)
				require.Zero(t, s.LastCommitTS())
				return api.StageRaftControlMetadata(ctx, []byte("R/7"), []byte("accepted"))
			}))
			got, err := api.ReadRaftControlMetadata(t.Context(), []byte("R/7"))
			require.NoError(t, err)
			require.Equal(t, []byte("accepted"), got)
			require.Equal(t, uint64(10), s.LastCommitTS())
			if _, ok := s.(*pebbleStore); ok {
				index, present := raftEntryAppliedIndex(t, s)
				require.True(t, present)
				require.Equal(t, uint64(7), index)
			}
		})
	}
}

func TestRaftControlMetadataAbortAndErrorLatch(t *testing.T) {
	for name, factory := range absenceTestStores(t) {
		for _, failure := range []string{"callback", "ignored-canceled-stage", "ignored-nested-error", "ignored-invalid-control"} {
			t.Run(name+"/"+failure, func(t *testing.T) {
				s := factory(t)
				api := raftMetadata(t, s)
				boom := errors.New("abort entry")
				err := raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 8, func(ctx context.Context) error {
					require.NoError(t, api.StageRaftControlMetadata(ctx, []byte("P"), []byte("not-published")))
					require.NoError(t, s.ApplyMutationsRaftAt(ctx, raftEntryPut("inner", "not-published"), nil, 10, 10, 8))
					switch failure {
					case "callback":
						return boom
					case "ignored-canceled-stage":
						canceled, cancel := context.WithCancel(ctx)
						cancel()
						require.Error(t, api.StageRaftControlMetadata(canceled, []byte("R"), []byte("bad")))
					case "ignored-nested-error":
						require.ErrorIs(t, raftEntryApplier(t, s).ApplyRaftEntry(ctx, 8, func(context.Context) error { return boom }), boom)
					case "ignored-invalid-control":
						require.ErrorIs(t, api.StageRaftControlMetadata(ctx, nil, nil), ErrRaftControlKey)
					}
					return nil
				})
				require.Error(t, err)
				_, err = api.ReadRaftControlMetadata(t.Context(), []byte("P"))
				require.ErrorIs(t, err, ErrKeyNotFound)
				_, err = s.GetAt(t.Context(), []byte("inner"), 100)
				require.ErrorIs(t, err, ErrKeyNotFound)
				require.Zero(t, s.LastCommitTS())
			})
		}
	}
}

func TestRaftControlMetadataCanceledReadAbortsScope(t *testing.T) {
	for name, factory := range absenceTestStores(t) {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			api := raftMetadata(t, s)
			err := raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 7, func(ctx context.Context) error {
				require.NoError(t, api.StageRaftControlMetadata(ctx, []byte("P"), []byte("must-abort")))
				require.NoError(t, s.ApplyMutationsRaftAt(ctx, raftEntryPut("inner", "must-abort"), nil, 10, 10, 7))
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				_, readErr := api.ReadRaftControlMetadata(canceled, []byte("P"))
				require.ErrorIs(t, readErr, context.Canceled)
				return nil
			})
			require.ErrorIs(t, err, context.Canceled)
			_, err = api.ReadRaftControlMetadata(t.Context(), []byte("P"))
			require.ErrorIs(t, err, ErrKeyNotFound)
			_, err = s.GetAt(t.Context(), []byte("inner"), 100)
			require.ErrorIs(t, err, ErrKeyNotFound)
			require.Zero(t, s.LastCommitTS())
		})
	}
}

func TestRaftControlMetadataReceiptAndJournalOnly(t *testing.T) {
	for name, factory := range absenceTestStores(t) {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			api := raftMetadata(t, s)
			require.ErrorIs(t, api.StageRaftControlMetadata(t.Context(), []byte("P"), []byte("bad")), ErrRaftControlScopeRequired)
			require.NoError(t, api.ApplyRaftControlMetadata(t.Context(), func(ctx context.Context) error {
				return api.StageRaftControlMetadata(ctx, []byte("J/9"), []byte("PREPARED"))
			}))
			require.Zero(t, s.LastCommitTS())
			if _, ok := s.(*pebbleStore); ok {
				_, present := raftEntryAppliedIndex(t, s)
				require.False(t, present)
			}
			require.NoError(t, raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 9, func(ctx context.Context) error {
				if err := api.StageRaftControlMetadata(ctx, []byte("R/9"), []byte("deterministic-rejection")); err != nil {
					return err
				}
				return api.StageRaftControlMetadata(ctx, []byte("J/9"), []byte("FSM_RECEIPTED"))
			}))
			require.Zero(t, s.LastCommitTS())
			err := api.ApplyRaftControlMetadata(t.Context(), func(ctx context.Context) error {
				require.NoError(t, api.StageRaftControlMetadata(ctx, []byte("J/9"), []byte("bad")))
				require.Error(t, s.ApplyMutationsRaftAt(ctx, raftEntryPut("forbidden", "data"), nil, 10, 10, 0))
				return nil
			})
			require.Error(t, err)
			require.NoError(t, api.ApplyRaftControlMetadata(t.Context(), func(ctx context.Context) error {
				return api.StageRaftControlMetadata(ctx, []byte("J/9"), []byte("PUBLISHED"))
			}))
			require.Zero(t, s.LastCommitTS())
			if _, ok := s.(*pebbleStore); ok {
				index, present := raftEntryAppliedIndex(t, s)
				require.True(t, present)
				require.Equal(t, uint64(9), index)
			}
		})
	}
}

func TestRaftControlMetadataOrdinaryWritersCannotChangeRecords(t *testing.T) {
	for name, factory := range absenceTestStores(t) {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			api := raftMetadata(t, s)
			key := []byte(RaftControlMetadataPrefix + "P")
			require.NoError(t, raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 1, func(ctx context.Context) error {
				return api.StageRaftControlMetadata(ctx, []byte("P"), []byte("protected"))
			}))
			require.Error(t, s.PutAt(t.Context(), key, []byte("raw"), 20, 0))
			require.Error(t, s.DeleteAt(t.Context(), key, 20))
			require.Error(t, s.ExpireAt(t.Context(), key, 30, 20))
			require.Error(t, s.ApplyMutations(t.Context(), []*KVPairMutation{{Op: OpTypePut, Key: key, Value: []byte("txn")}}, nil, 20, 20))
			_, err := s.ImportVersions(t.Context(), ImportVersionsOptions{JobID: 1, BatchSeq: 1, Versions: []MVCCVersion{{Key: key, CommitTS: 20, Value: []byte("import")}}})
			require.Error(t, err)
			require.NoError(t, s.DeletePrefixAt(t.Context(), nil, nil, 30))
			require.NoError(t, s.Compact(t.Context(), 40))
			got, err := api.ReadRaftControlMetadata(t.Context(), []byte("P"))
			require.NoError(t, err)
			require.Equal(t, []byte("protected"), got)
			rows, err := s.ScanAt(t.Context(), nil, nil, 10, 40)
			require.NoError(t, err)
			require.Empty(t, rows)
			// Adjacent ordinary keys remain usable, including embedded NULs.
			ordinary := []byte("!txn|meta|raft-controL|P\x00")
			require.NoError(t, s.PutAt(t.Context(), ordinary, []byte("user"), 50, 0))
			got, err = s.GetAt(t.Context(), ordinary, 50)
			require.NoError(t, err)
			require.Equal(t, []byte("user"), got)
		})
	}
}

func TestMemoryRaftEntryOverlaySharesUnmodifiedPayloads(t *testing.T) {
	s := newMVCCStore()
	large := bytes.Repeat([]byte("payload"), 1<<20)
	require.NoError(t, s.PutAt(t.Context(), []byte("large"), large, 1, 0))
	before, _ := s.tree.Get([]byte("large"))
	old, ok := before.([]VersionedValue)
	require.True(t, ok)
	var start, end runtime.MemStats
	runtime.ReadMemStats(&start)
	require.NoError(t, s.ApplyRaftEntry(t.Context(), 2, func(ctx context.Context) error {
		staged := s.memoryEntryStore(ctx)
		require.Same(t, s.tree, staged.tree)
		require.Empty(t, staged.entryOverlay)
		require.NoError(t, s.ApplyMutationsRaftAt(ctx, raftEntryPut("small", "value"), nil, 2, 2, 2))
		require.Len(t, staged.entryOverlay, 1)
		return nil
	}))
	runtime.ReadMemStats(&end)
	require.Less(t, end.TotalAlloc-start.TotalAlloc, uint64(len(large)/2), "a point entry must not copy unrelated payloads")
	after, _ := s.tree.Get([]byte("large"))
	current, ok := after.([]VersionedValue)
	require.True(t, ok)
	require.Same(t, &old[0].Value[0], &current[0].Value[0])
	// Overwriting an existing version must copy the slice, not alter its owner.
	require.Error(t, s.ApplyRaftEntry(t.Context(), 3, func(ctx context.Context) error {
		require.NoError(t, s.ApplyMutationsRaftAt(ctx, raftEntryPut("large", "replacement"), nil, ^uint64(0), 1, 3))
		return fmt.Errorf("abort replacement")
	}))
	require.Equal(t, large, old[0].Value)
	got, err := s.GetAt(t.Context(), []byte("large"), 2)
	require.NoError(t, err)
	require.Equal(t, large, got)
}

func TestMemoryRaftEntryAbortIsAtomic(t *testing.T) {
	s := NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	want := errors.New("callback failed")
	err := raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 7, func(ctx context.Context) error {
		require.NoError(t, s.ApplyMutationsRaftAt(ctx, raftEntryPut("staged", "value"), nil, 10, 10, 7))
		return want
	})
	require.ErrorIs(t, err, want)
	_, err = s.GetAt(t.Context(), []byte("staged"), 100)
	require.ErrorIs(t, err, ErrKeyNotFound)
	require.Zero(t, s.LastCommitTS())
}

func TestRaftControlEntryReadOwnWritesAndPrefixDelete(t *testing.T) {
	for name, factory := range absenceTestStores(t) {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			require.NoError(t, raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 7, func(ctx context.Context) error {
				require.NoError(t, s.ApplyMutationsRaftAt(ctx, raftEntryPut("m/one", "first"), nil, 10, 10, 7))
				require.NoError(t, s.ApplyMutationsRaftAt(ctx, raftEntryPut("m/two", "second"), nil, 11, 11, 7))
				rows, err := s.ScanAt(ctx, []byte("m/"), []byte("n"), 10, 20)
				require.NoError(t, err)
				require.Len(t, rows, 2)
				keys, err := s.ScanKeysAt(ctx, []byte("m/"), []byte("n"), 10, 20)
				require.NoError(t, err)
				require.Equal(t, [][]byte{[]byte("m/one"), []byte("m/two")}, keys)
				rows, err = s.ReverseScanAt(ctx, []byte("m/"), []byte("n"), 10, 20)
				require.NoError(t, err)
				require.Len(t, rows, 2)
				require.Equal(t, []byte("m/two"), rows[0].Key)
				latest, present, err := s.LatestCommitTS(ctx, []byte("m/two"))
				require.NoError(t, err)
				require.True(t, present)
				require.Equal(t, uint64(11), latest)
				present, err = s.CommittedVersionAt(ctx, []byte("m/two"), 11)
				require.NoError(t, err)
				require.True(t, present)
				require.NoError(t, s.DeletePrefixAtRaftAt(ctx, []byte("m/"), nil, 12, 7))
				rows, err = s.ScanAt(ctx, []byte("m/"), []byte("n"), 10, 20)
				require.NoError(t, err)
				require.Empty(t, rows)
				return raftMetadata(t, s).StageRaftControlMetadata(ctx, []byte("R/7"), []byte("done"))
			}))
			_, err := s.GetAt(t.Context(), []byte("m/two"), 20)
			require.ErrorIs(t, err, ErrKeyNotFound)
		})
	}
}

func TestRaftControlEntryProjectionAndStaleIndex(t *testing.T) {
	for name, factory := range absenceTestStores(t) {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			api := raftMetadata(t, s)
			require.NoError(t, raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 9, func(ctx context.Context) error {
				if err := api.StageRaftEntryMutationsPreservingLastCommitTS(ctx, raftEntryPut("projection", "config"), nil, 1, 1); err != nil {
					return err
				}
				return api.StageRaftControlMetadata(ctx, []byte("P"), []byte("generation-9"))
			}))
			require.Zero(t, s.LastCommitTS())
			got, err := s.GetAt(t.Context(), []byte("projection"), 1)
			require.NoError(t, err)
			require.Equal(t, []byte("config"), got)
			err = raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 8, func(ctx context.Context) error {
				return api.StageRaftControlMetadata(ctx, []byte("P"), []byte("stale"))
			})
			require.ErrorIs(t, err, ErrRaftControlStaleIndex)
			got, err = api.ReadRaftControlMetadata(t.Context(), []byte("P"))
			require.NoError(t, err)
			require.Equal(t, []byte("generation-9"), got)
		})
	}
}

func TestRaftControlMetadataRejectsTimestampSuffixCollision(t *testing.T) {
	physical, err := raftControlKey([]byte("P"))
	require.NoError(t, err)
	logical := physical[:len(physical)-timestampSize]
	ts := ^binary.BigEndian.Uint64(physical[len(physical)-timestampSize:])
	for name, factory := range absenceTestStores(t) {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			require.Error(t, s.PutAt(t.Context(), logical, []byte("alias"), ts, 0))
			_, err := s.ImportVersions(t.Context(), ImportVersionsOptions{JobID: 1, BatchSeq: 1, Versions: []MVCCVersion{{Key: logical, CommitTS: ts, Value: []byte("alias")}}})
			require.Error(t, err)
		})
	}
}

func TestMemoryRaftEntryDirectWriterWaits(t *testing.T) {
	s := NewMVCCStore()
	defer s.Close()
	done := make(chan error, 1)
	require.NoError(t, raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 7, func(ctx context.Context) error {
		require.NoError(t, s.ApplyMutationsRaftAt(ctx, raftEntryPut("x", "entry"), nil, 10, 10, 7))
		go func() { done <- s.ApplyMutations(ctx, raftEntryPut("x", "direct"), nil, 5, 20) }()
		select {
		case err := <-done:
			return errors.Wrap(err, "direct writer returned inside entry")
		case <-time.After(20 * time.Millisecond):
		}
		return nil
	}))
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrWriteConflict)
	case <-time.After(5 * time.Second):
		t.Fatal("direct writer did not complete after publication")
	}
}

func TestRaftControlMetadataMigrationRetention(t *testing.T) {
	for name, factory := range absenceTestStores(t) {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			api := raftMetadata(t, s)
			require.NoError(t, raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 7, func(ctx context.Context) error {
				return api.StageRaftControlMetadata(ctx, []byte("P"), []byte("protected"))
			}))
			require.NoError(t, s.PutAt(t.Context(), []byte("stage/key"), []byte("staged"), 10, 0))
			promoter, ok := s.(MigrationPromoter)
			require.True(t, ok)
			_, err := promoter.PromoteVersions(t.Context(), PromoteVersionsOptions{
				JobID: 1, StartKey: []byte("stage/"), EndKey: []byte("stage0"), MaxVersions: 100,
				TargetKey: func([]byte) ([]byte, bool) { return []byte(RaftControlMetadataPrefix + "P"), true },
			})
			require.Error(t, err)
			got, err := s.GetAt(t.Context(), []byte("stage/key"), 10)
			require.NoError(t, err)
			require.Equal(t, []byte("staged"), got)
			cleaner, ok := s.(MigrationCleaner)
			require.True(t, ok)
			_, err = cleaner.CleanupVersions(t.Context(), CleanupVersionsOptions{JobID: 2, MaxCommitTS: 20, MaxVersions: 100})
			require.NoError(t, err)
			exported, err := s.ExportVersions(t.Context(), ExportVersionsOptions{MaxCommitTSInclusive: ^uint64(0), MaxVersions: 100})
			require.NoError(t, err)
			require.Empty(t, exported.Versions, "migration data export must not reinterpret raw control rows")
			got, err = api.ReadRaftControlMetadata(t.Context(), []byte("P"))
			require.NoError(t, err)
			require.Equal(t, []byte("protected"), got)
		})
	}
}

func TestRaftControlMetadataNeverActsAsMVCCVersion(t *testing.T) {
	physical, err := raftControlKey([]byte("P"))
	require.NoError(t, err)
	alias := physical[:len(physical)-timestampSize]
	for _, mode := range []string{"presence", "absence-guard", "reverse-value", "keys-after-delete"} {
		for name, factory := range absenceTestStores(t) {
			t.Run(name+"/"+mode, func(t *testing.T) {
				s := factory(t)
				require.NoError(t, raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 7, func(ctx context.Context) error {
					return raftMetadata(t, s).StageRaftControlMetadata(ctx, []byte("P"), []byte("private-policy"))
				}))
				switch mode {
				case "presence":
					reader, ok := s.(VersionPresenceReader)
					require.True(t, ok)
					present, err := reader.VersionExistsAtOrBefore(t.Context(), alias, ^uint64(0))
					require.NoError(t, err)
					require.False(t, present)
				case "absence-guard":
					err := s.ApplyMutations(t.Context(), []*KVPairMutation{{Op: OpTypePut, Key: alias, Value: []byte("user"), RequireAbsent: true}}, nil, 0, 10)
					require.NoError(t, err, "control rows must not cause a phantom OCC conflict")
				case "reverse-value":
					require.NoError(t, s.PutAt(t.Context(), alias, []byte("user"), 10, 0))
					rows, err := s.ReverseScanAt(t.Context(), nil, nil, 10, ^uint64(0))
					require.NoError(t, err)
					require.Len(t, rows, 1)
					require.Equal(t, []byte("user"), rows[0].Value)
				case "keys-after-delete":
					require.NoError(t, s.PutAt(t.Context(), alias, []byte("user"), 10, 0))
					require.NoError(t, s.DeleteAt(t.Context(), alias, 11))
					keys, err := s.ScanKeysAt(t.Context(), nil, nil, 10, ^uint64(0))
					require.NoError(t, err)
					require.Empty(t, keys)
				}
			})
		}
	}
}
