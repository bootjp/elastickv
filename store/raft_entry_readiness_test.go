package store

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestRaftEntryReadinessTracksAtomically(t *testing.T) {
	for name, factory := range absenceTestStores(t) {
		for _, abort := range []bool{false, true} {
			t.Run(name+map[bool]string{false: "/commit", true: "/abort"}[abort], func(t *testing.T) {
				s := factory(t)
				writer, ok := s.(MigrationTargetReadinessRaftWriter)
				require.True(t, ok)
				reader, ok := s.(MigrationTargetReadinessReader)
				require.True(t, ok)
				admission, ok := s.(MigrationServingAdmissionReader)
				require.True(t, ok)
				metadata, ok := s.(MigrationImportMetadataReader)
				require.True(t, ok)
				state := TargetStagedReadinessState{JobID: 1, MigrationJobID: 1, Armed: true, TrackWrites: true, RetentionPinTS: 1, MinWriteTSExclusive: 10}
				require.NoError(t, writer.ApplyTargetStagedReadinessAt(t.Context(), state, 1))
				boom := errors.New("abort readiness and data")
				err := raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 2, func(ctx context.Context) error {
					for _, job := range []uint64{1, 2} {
						state.JobID, state.MigrationJobID = job, job
						state.MinAdmittedTS, state.MinWriteTSExclusive = 30, 20+job
						// The real FSM tracker intentionally supplies zero, leaving
						// the entry index to the following data mutation.
						require.NoError(t, writer.ApplyTargetStagedReadinessAt(ctx, state, 0))
					}
					states, err := reader.MigrationTargetReadinessStates(ctx)
					require.NoError(t, err)
					require.Len(t, states, 2)
					require.Equal(t, uint64(30), states[0].MinAdmittedTS)
					for _, job := range []uint64{1, 2} {
						floor, err := s.MigrationHLCFloor(ctx, job)
						require.NoError(t, err)
						require.Equal(t, 20+job, floor)
						present, err := metadata.MigrationImportMetadataPresent(ctx, job)
						require.NoError(t, err)
						require.True(t, present)
					}
					view, err := admission.MigrationServingAdmissionState(ctx)
					require.NoError(t, err)
					require.Equal(t, states, view.Readiness)
					published, err := reader.MigrationTargetReadinessStates(t.Context())
					require.NoError(t, err)
					require.Len(t, published, 1)
					require.Zero(t, published[0].MinAdmittedTS)
					require.Equal(t, uint64(10), s.LastCommitTS())
					require.NoError(t, s.ApplyMutationsRaftAt(ctx, raftEntryPut("tracked", "data"), nil, 30, 30, 2))
					require.NoError(t, raftMetadata(t, s).StageRaftControlMetadata(ctx, []byte("R"), []byte("tracked")))
					if abort {
						return boom
					}
					return nil
				})
				states, readErr := reader.MigrationTargetReadinessStates(t.Context())
				require.NoError(t, readErr)
				if abort {
					require.ErrorIs(t, err, boom)
					require.Len(t, states, 1)
					require.Zero(t, states[0].MinAdmittedTS)
					require.Equal(t, uint64(10), s.LastCommitTS())
					_, err = s.GetAt(t.Context(), []byte("tracked"), 30)
					require.ErrorIs(t, err, ErrKeyNotFound)
					_, err = raftMetadata(t, s).ReadRaftControlMetadata(t.Context(), []byte("R"))
					require.ErrorIs(t, err, ErrKeyNotFound)
				} else {
					require.NoError(t, err)
					require.Len(t, states, 2)
					require.Equal(t, uint64(30), states[0].MinAdmittedTS)
					require.Equal(t, uint64(30), s.LastCommitTS())
				}
				assertEntryReadinessFloors(t, s, abort)
			})
		}
	}
}

func assertEntryReadinessFloors(t *testing.T, s MVCCStore, abort bool) {
	t.Helper()
	for _, job := range []uint64{1, 2} {
		floor, err := s.MigrationHLCFloor(t.Context(), job)
		require.NoError(t, err)
		want := 20 + job
		if abort {
			want = map[uint64]uint64{1: 10, 2: 0}[job]
		}
		require.Equal(t, want, floor)
	}
}

func TestRaftEntryReadinessFailureLatches(t *testing.T) {
	for name, factory := range absenceTestStores(t) {
		for _, failure := range []string{"cancel", "index", "invalid", "retired", "journal"} {
			t.Run(name+"/"+failure, func(t *testing.T) {
				s := factory(t)
				writer, ok := s.(MigrationTargetReadinessRaftWriter)
				require.True(t, ok)
				state := TargetStagedReadinessState{JobID: 1, MigrationJobID: 1, Armed: true, MinWriteTSExclusive: 20}
				if failure == "retired" {
					cleaner, ok := s.(MigrationCleaner)
					require.True(t, ok)
					require.NoError(t, cleaner.ClearMigrationState(t.Context(), 1, 1))
				}
				callback := func(ctx context.Context) error {
					require.NoError(t, raftMetadata(t, s).StageRaftControlMetadata(ctx, []byte("R"), []byte("must-abort")))
					index := uint64(0)
					if failure == "cancel" {
						var cancel context.CancelFunc
						ctx, cancel = context.WithCancel(ctx)
						cancel()
					}
					if failure == "index" {
						index = 99
					}
					if failure == "invalid" {
						state.MigrationJobID = 0
					}
					require.Error(t, writer.ApplyTargetStagedReadinessAt(ctx, state, index))
					return nil // An ignored staging failure must still abort.
				}
				var err error
				if failure == "journal" {
					err = raftMetadata(t, s).ApplyRaftControlMetadata(t.Context(), callback)
				} else {
					err = raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 2, callback)
				}
				require.Error(t, err)
				_, err = raftMetadata(t, s).ReadRaftControlMetadata(t.Context(), []byte("R"))
				require.ErrorIs(t, err, ErrKeyNotFound)
				require.Zero(t, s.LastCommitTS())
				floor, err := s.MigrationHLCFloor(t.Context(), 1)
				require.NoError(t, err)
				require.Zero(t, floor)
			})
		}
	}
}

func TestRaftEntryReadinessNativeReopen(t *testing.T) {
	for _, abort := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "abort"}[abort], func(t *testing.T) {
			dir := t.TempDir()
			s, err := NewPebbleStore(dir)
			require.NoError(t, err)
			writer, ok := s.(MigrationTargetReadinessRaftWriter)
			require.True(t, ok)
			boom := errors.New("abort before native commit")
			err = raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 7, func(ctx context.Context) error {
				require.NoError(t, writer.ApplyTargetStagedReadinessAt(ctx, TargetStagedReadinessState{JobID: 1, MigrationJobID: 1, Armed: true, MinWriteTSExclusive: 20}, 0))
				require.NoError(t, s.ApplyMutationsRaftAt(ctx, raftEntryPut("tracked", "data"), nil, 30, 30, 7))
				require.NoError(t, raftMetadata(t, s).StageRaftControlMetadata(ctx, []byte("R"), []byte("committed")))
				if abort {
					return boom
				}
				return nil
			})
			if abort {
				require.ErrorIs(t, err, boom)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, s.Close())
			s, err = NewPebbleStore(dir)
			require.NoError(t, err)
			defer s.Close()
			reader, ok := s.(MigrationTargetReadinessReader)
			require.True(t, ok)
			states, err := reader.MigrationTargetReadinessStates(t.Context())
			require.NoError(t, err)
			index, present := raftEntryAppliedIndex(t, s)
			floor, err := s.MigrationHLCFloor(t.Context(), 1)
			require.NoError(t, err)
			_, dataErr := s.GetAt(t.Context(), []byte("tracked"), 30)
			_, receiptErr := raftMetadata(t, s).ReadRaftControlMetadata(t.Context(), []byte("R"))
			if abort {
				require.Empty(t, states)
				require.False(t, present)
				require.Zero(t, floor)
				require.Zero(t, s.LastCommitTS())
				require.ErrorIs(t, dataErr, ErrKeyNotFound)
				require.ErrorIs(t, receiptErr, ErrKeyNotFound)
			} else {
				require.Len(t, states, 1)
				require.True(t, present)
				require.Equal(t, uint64(7), index)
				require.Equal(t, uint64(20), floor)
				require.Equal(t, uint64(30), s.LastCommitTS())
				require.NoError(t, dataErr)
				require.NoError(t, receiptErr)
			}
		})
	}
}
