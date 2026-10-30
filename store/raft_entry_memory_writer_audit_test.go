package store

import (
	"bytes"
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type memoryFenceWriter struct {
	name    string
	frame   string
	prepare func(*testing.T, *mvccStore) func() error
}

func memoryFenceWriters() []memoryFenceWriter {
	ctx := context.Background()
	return []memoryFenceWriter{
		{"put", "PutAt", func(_ *testing.T, s *mvccStore) func() error {
			return func() error { return s.PutAt(ctx, []byte("queued"), []byte("value"), 3000, 0) }
		}},
		{"ttl-put", "PutAt", func(_ *testing.T, s *mvccStore) func() error {
			return func() error { return s.PutWithTTLAt(ctx, []byte("queued"), []byte("value"), 3000, 4000) }
		}},
		{"delete", "DeleteAt", func(_ *testing.T, s *mvccStore) func() error {
			return func() error { return s.DeleteAt(ctx, []byte("published"), 3000) }
		}},
		{"expire", "ExpireAt", func(_ *testing.T, s *mvccStore) func() error {
			return func() error { return s.ExpireAt(ctx, []byte("published"), 4000, 3000) }
		}},
		{"mutations", "applyMutations", func(_ *testing.T, s *mvccStore) func() error {
			return func() error { return s.ApplyMutations(ctx, raftEntryPut("queued", "value"), nil, ^uint64(0), 3000) }
		}},
		{"raft-mutations", "applyMutations", func(_ *testing.T, s *mvccStore) func() error {
			return func() error { return s.ApplyMutationsRaft(ctx, raftEntryPut("queued", "value"), nil, ^uint64(0), 3000) }
		}},
		{"indexed-mutations", "applyMutations", func(_ *testing.T, s *mvccStore) func() error {
			return func() error {
				return s.ApplyMutationsRaftAt(ctx, raftEntryPut("queued", "value"), nil, ^uint64(0), 3000, 100)
			}
		}},
		{"fenced-mutations", "applyMutations", func(_ *testing.T, s *mvccStore) func() error {
			return func() error {
				return s.ApplyMutationsRaftAtFenced(ctx, raftEntryPut("queued", "value"), nil, ^uint64(0), 3000, 100)
			}
		}},
		{"preserving-mutations", "ApplyMutationsPreservingLastCommitTS", func(_ *testing.T, s *mvccStore) func() error {
			return func() error {
				return s.ApplyMutationsPreservingLastCommitTS(ctx, raftEntryPut("queued", "value"), nil, ^uint64(0), 3000)
			}
		}},
		{"prefix-delete", "deletePrefixesAt", func(_ *testing.T, s *mvccStore) func() error {
			return func() error { return s.DeletePrefixAt(ctx, []byte("published"), nil, 3000) }
		}},
		{"indexed-prefix-delete", "deletePrefixesAt", func(_ *testing.T, s *mvccStore) func() error {
			return func() error {
				return s.DeletePrefixesAtRaftAt(ctx, []PrefixDelete{{Prefix: []byte("published")}}, 3000, 100)
			}
		}},
		{"retention", "SetMinRetainedTS", func(_ *testing.T, s *mvccStore) func() error {
			return func() error { s.SetMinRetainedTS(2); return nil }
		}},
		{"gc-versions", "compactPhase2", func(_ *testing.T, s *mvccStore) func() error {
			pending := s.compactPhase1(2)
			return func() error { s.compactPhase2(pending, 2); return nil }
		}},
		{"gc-frontier", "Compact", func(_ *testing.T, s *mvccStore) func() error {
			return func() error { return s.Compact(ctx, 0) }
		}},
		{"restore", "restoreStreamingSnapshot", memoryRestoreFenceWriter(false)},
		{"external-restore", "restoreStreamingSnapshot", memoryRestoreFenceWriter(true)},
		{"import", "importMemoryVersions", func(_ *testing.T, s *mvccStore) func() error {
			return func() error {
				_, err := s.ImportVersions(ctx, ImportVersionsOptions{JobID: 100, BatchSeq: 1, Versions: []MVCCVersion{{Key: []byte("queued"), CommitTS: 3000, Value: []byte("value")}}})
				return err
			}
		}},
		{"import-raft", "importMemoryVersions", func(_ *testing.T, s *mvccStore) func() error {
			return func() error {
				_, err := s.ImportVersionsRaft(ctx, ImportVersionsOptions{JobID: 100, BatchSeq: 1, Versions: []MVCCVersion{{Key: []byte("queued"), CommitTS: 3000, Value: []byte("value")}}})
				return err
			}
		}},
		{"retire", "RetireMigration", func(_ *testing.T, s *mvccStore) func() error {
			return func() error { return s.RetireMigration(ctx, 99) }
		}},
		{"retire-raft", "RetireMigration", func(_ *testing.T, s *mvccStore) func() error {
			return func() error { return s.RetireMigrationRaft(ctx, 99, 100) }
		}},
		{"cleanup", "CleanupVersions", func(_ *testing.T, s *mvccStore) func() error {
			return func() error {
				_, err := s.CleanupVersions(ctx, CleanupVersionsOptions{JobID: 99, MaxVersions: 100, MaxCommitTS: 10})
				return err
			}
		}},
		{"clear", "ClearMigrationState", func(_ *testing.T, s *mvccStore) func() error {
			return func() error { return s.ClearMigrationState(ctx, 99, 100) }
		}},
		{"promote", "PromoteVersions", func(_ *testing.T, s *mvccStore) func() error {
			return func() error {
				_, err := s.PromoteVersions(ctx, PromoteVersionsOptions{JobID: 99, StartKey: []byte("stage/"), EndKey: []byte("stage0"), MaxVersions: 100,
					TargetKey: func([]byte) ([]byte, bool) { return []byte("promoted"), true }})
				return err
			}
		}},
		{"readiness", "ApplyTargetStagedReadiness", func(_ *testing.T, s *mvccStore) func() error {
			return func() error {
				return s.ApplyTargetStagedReadiness(ctx, TargetStagedReadinessState{JobID: 100, MigrationJobID: 100})
			}
		}},
		{"readiness-indexed", "ApplyTargetStagedReadiness", func(_ *testing.T, s *mvccStore) func() error {
			return func() error {
				return s.ApplyTargetStagedReadinessAt(ctx, TargetStagedReadinessState{JobID: 100, MigrationJobID: 100}, 100)
			}
		}},
		{"ownership-acquire", "AcquireMigrationOwnership", func(t *testing.T, s *mvccStore) func() error {
			state := ownershipFixture(t, s)
			state.Role, state.OwnerGroupID = MigrationOwner, state.LocalGroupID
			state.Staged = true
			require.NoError(t, s.ApplyTargetStagedReadiness(t.Context(), TargetStagedReadinessState{JobID: state.JobID, MigrationJobID: state.JobID,
				RouteStart: state.RouteStart, RouteEnd: state.RouteEnd, Armed: true, MinWriteTSExclusive: state.MinWriteTSExclusive, ExpectedCutoverVersion: state.CatalogVersion}))
			return func() error { return s.AcquireMigrationOwnership(ctx, state, 100) }
		}},
		{"ownership-clear", "ClearMigrationStateWithOwnership", func(t *testing.T, s *mvccStore) func() error {
			state := ownershipFixture(t, s)
			return func() error { return s.ClearMigrationStateWithOwnership(ctx, state, 100) }
		}},
		{"ownership-refresh", "RefreshMigrationOwnership", func(t *testing.T, s *mvccStore) func() error {
			state := ownershipFixture(t, s)
			state.Generation, state.RouteID = "0123456789abcdef0123456789abcdef", 3
			state.DescriptorHash = bytes.Repeat([]byte{1}, 32)
			require.NoError(t, s.ClearMigrationStateWithOwnership(ctx, state, 9))
			state.CatalogVersion, state.ConfigurationIndex = 15, 4
			return func() error { return s.RefreshMigrationOwnership(ctx, state, 100) }
		}},
		{"partition", "ApplyMigrationOwnershipPartition", func(t *testing.T, s *mvccStore) func() error {
			_, transition := partitionFixture(t, s)
			return func() error { return s.ApplyMigrationOwnershipPartition(ctx, transition, 100) }
		}},
		{"legacy-control", "ApplyReplicatedControlState", func(_ *testing.T, s *mvccStore) func() error {
			return func() error {
				_, err := s.ApplyReplicatedControlState(ctx, []byte(ReplicatedControlPrefix+"queued"), []byte("value"), 100)
				return err
			}
		}},
		{"control-journal", "applyMemoryRaftEntry", func(_ *testing.T, s *mvccStore) func() error {
			return func() error {
				return s.ApplyRaftControlMetadata(ctx, func(tx context.Context) error {
					return s.StageRaftControlMetadata(tx, []byte("queued"), []byte("value"))
				})
			}
		}},
		{"control-indexed", "applyMemoryRaftEntry", func(_ *testing.T, s *mvccStore) func() error {
			return func() error {
				return s.ApplyRaftEntry(ctx, 100, func(tx context.Context) error {
					return s.StageRaftControlMetadata(tx, []byte("queued"), []byte("value"))
				})
			}
		}},
	}
}

func memoryRestoreFenceWriter(external bool) func(*testing.T, *mvccStore) func() error {
	return func(t *testing.T, s *mvccStore) func() error {
		image := newMVCCStore()
		require.NoError(t, image.PutAt(t.Context(), []byte("restored"), []byte("value"), 1, 0))
		snapshot, err := image.Snapshot()
		require.NoError(t, err)
		var payload bytes.Buffer
		_, err = snapshot.WriteTo(&payload)
		require.NoError(t, err)
		require.NoError(t, snapshot.Close())
		return func() error {
			if external {
				return s.RestoreExternal(context.Background(), bytes.NewReader(payload.Bytes()))
			}
			return s.Restore(bytes.NewReader(payload.Bytes()))
		}
	}
}

// The named, non-inlined frame identifies the one writer whose real mutex
// wait is inspected; starting a goroutine alone is not a blocking witness.
func runMemoryFenceWriter(started, finished chan struct{}, result chan error, fn func() error) {
	defer close(finished)
	close(started)
	result <- fn()
}

func waitForMemoryFenceWriter(t *testing.T, frame string, result <-chan error) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	buffer := make([]byte, 1<<20)
	for time.Now().Before(deadline) {
		select {
		case err := <-result:
			t.Fatalf("writer %s escaped the entry's applyMu: %v", frame, err)
		default:
		}
		n := runtime.Stack(buffer, true)
		for _, stack := range strings.Split(string(buffer[:n]), "\n\n") {
			if strings.Contains(stack, "runMemoryFenceWriter") && strings.Contains(stack, "(*mvccStore)."+frame) && strings.Contains(stack, "sync.(*Mutex).lockSlow") {
				return
			}
		}
		runtime.Gosched()
	}
	t.Fatalf("writer %s never reached its actual mutex wait", frame)
}

func TestMemoryRaftEntryFencesEveryPublishedWriter(t *testing.T) {
	for _, writer := range memoryFenceWriters() {
		for _, journal := range []bool{false, true} {
			mode := map[bool]string{false: "indexed", true: "journal"}[journal]
			t.Run(writer.name+"/"+mode, func(t *testing.T) {
				testMemoryFenceWriter(t, writer, journal)
			})
		}
	}
}

func testMemoryFenceWriter(t *testing.T, writer memoryFenceWriter, journal bool) {
	t.Helper()
	s := newMVCCStore()
	defer s.Close()
	for ts := uint64(1); ts <= 3; ts++ {
		require.NoError(t, s.PutAt(t.Context(), []byte("published"), []byte{byte(ts)}, ts, 0))
	}
	_, err := s.ImportVersions(t.Context(), ImportVersionsOptions{JobID: 99, BatchSeq: 1, Versions: []MVCCVersion{{Key: []byte("stage/key"), CommitTS: 3, Value: []byte("staged")}}})
	require.NoError(t, err)
	fn := writer.prepare(t, s)
	started, finished := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	launched := false
	defer func() {
		if launched {
			select {
			case <-finished:
			case <-time.After(5 * time.Second):
				t.Error("writer did not join during failure cleanup")
			}
		}
	}()
	callback := func(ctx context.Context) error {
		require.NoError(t, s.StageRaftControlMetadata(ctx, []byte("P"), []byte("staged")))
		if !journal {
			require.NoError(t, s.ApplyMutationsRaftAt(ctx, raftEntryPut("inside", "staged"), nil, ^uint64(0), 2000, 99))
		}
		require.True(t, s.mtx.TryLock(), "published-state mutex must be free during callback")
		s.mtx.Unlock()
		launched = true
		go runMemoryFenceWriter(started, finished, result, fn)
		<-started
		waitForMemoryFenceWriter(t, writer.frame, result)
		staged := s.memoryEntryStore(ctx)
		for range 16 {
			value, err := s.GetAt(ctx, []byte("published"), ^uint64(0))
			require.NoError(t, err)
			require.Equal(t, []byte{3}, value)
			_, err = staged.MigrationServingState(ctx)
			require.NoError(t, err)
			_, err = staged.ExportVersions(ctx, ExportVersionsOptions{MaxVersions: 100, MaxCommitTSInclusive: ^uint64(0)})
			require.NoError(t, err)
			_, err = s.ReadRaftControlMetadata(ctx, []byte("P"))
			require.NoError(t, err)
		}
		return nil
	}
	if journal {
		err = s.ApplyRaftControlMetadata(t.Context(), callback)
	} else {
		err = s.ApplyRaftEntry(t.Context(), 99, callback)
	}
	require.NoError(t, err)
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not finish after entry released applyMu")
	}
	<-finished
}
