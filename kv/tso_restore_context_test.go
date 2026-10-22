package kv

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestTSORestoreContextRequiresOptionalCapability(t *testing.T) {
	fsm := NewTSOStateMachine(nil)
	restorer, ok := any(fsm).(raftengine.ContextSnapshotRestorer)
	require.True(t, ok, "group-zero external restore needs explicit origin-aware validation")
	raw := snapshotBytesFor(t, &tsoFSMSnapshot{ceilingMs: 100})
	require.NoError(t, restorer.RestoreWithContext(t.Context(), bytes.NewReader(raw), raftengine.SnapshotRestoreOptions{
		Origin: raftengine.SnapshotRestoreExternalRebase,
	}))
	require.Equal(t, tsoLeaseAllocationFloor(100), fsm.AllocationFloor())
}

type cancelTSORestoreReader struct {
	r      io.Reader
	cancel context.CancelFunc
}

func (r cancelTSORestoreReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.cancel()
	return n, err
}

func TestTSORestoreContextCancellationAndUnknownOriginDoNotMutate(t *testing.T) {
	for _, scenario := range []string{"canceled", "during_read", "unknown", "nil_reader"} {
		t.Run(scenario, func(t *testing.T) {
			fsm := NewTSOStateMachine(nil)
			require.Nil(t, fsm.Apply(marshalTSOAllocationFloor(50)))
			restorer, ok := any(fsm).(raftengine.ContextSnapshotRestorer)
			require.True(t, ok)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var reader io.Reader = bytes.NewReader(snapshotBytesFor(t, &tsoFSMSnapshot{ceilingMs: 100, allocationFloor: 200}))
			opts := raftengine.SnapshotRestoreOptions{Origin: raftengine.SnapshotRestoreExternalRebase}
			switch scenario {
			case "canceled":
				cancel()
			case "during_read":
				reader = cancelTSORestoreReader{r: reader, cancel: cancel}
			case "unknown":
				opts.Origin = 255
			case "nil_reader":
				reader = nil
			}
			err := restorer.RestoreWithContext(ctx, reader, opts)
			require.Error(t, err)
			if scenario == "canceled" || scenario == "during_read" {
				require.True(t, errors.Is(err, context.Canceled))
			}
			require.Equal(t, uint64(50), fsm.AllocationFloor())
			require.Zero(t, fsm.ceilingMs.Load())
		})
	}
}

func TestTSORestoreContextNativeLayoutsAndPending(t *testing.T) {
	for _, snap := range []tsoFSMSnapshot{
		{ceilingMs: 100},
		{ceilingMs: 100, allocationFloor: 200},
		{ceilingMs: 100, allocationFloor: 200, cutoverActive: true},
		{ceilingMs: 100, allocationFloor: 200, cutoverActive: true, phaseDActive: true, phaseDFloor: 200},
		{ceilingMs: 100, allocationFloor: 200, cutoverActive: true, pendingFloor: 200},
	} {
		raw := snapshotBytesFor(t, &snap)
		want := NewTSOStateMachine(nil)
		require.NoError(t, want.Restore(bytes.NewReader(raw)))
		for _, origin := range []raftengine.SnapshotRestoreOrigin{raftengine.SnapshotRestoreRaft, raftengine.SnapshotRestoreExternalRebase} {
			got := NewTSOStateMachine(nil)
			require.NoError(t, got.RestoreWithContext(t.Context(), bytes.NewReader(raw), raftengine.SnapshotRestoreOptions{Origin: origin}))
			require.Equal(t, want.ceilingMs.Load(), got.ceilingMs.Load())
			require.Equal(t, want.AllocationFloor(), got.AllocationFloor())
			require.Equal(t, want.CutoverActive(), got.CutoverActive())
			require.Equal(t, want.PhaseDActive(), got.PhaseDActive())
			require.Equal(t, want.PhaseDFloor(), got.PhaseDFloor())
			require.Equal(t, want.PendingPhaseDFloor(), got.PendingPhaseDFloor())
		}
	}
}

func TestTSORestoreContextStrictNativeErrorsPreserveState(t *testing.T) {
	valid := snapshotBytesFor(t, &tsoFSMSnapshot{ceilingMs: 100, allocationFloor: 200, cutoverActive: true})
	invalidFlag := bytes.Clone(valid)
	invalidFlag[tsoSnapshotV2Len] = 2
	for _, raw := range [][]byte{valid[:2], append(bytes.Clone(valid), 1), invalidFlag, append(bytes.Clone(valid), []byte("EKVPBBL1extra")...)} {
		fsm := NewTSOStateMachine(nil)
		require.Nil(t, fsm.Apply(marshalTSOAllocationFloor(50)))
		require.Error(t, fsm.RestoreWithContext(t.Context(), bytes.NewReader(raw), raftengine.SnapshotRestoreOptions{Origin: raftengine.SnapshotRestoreExternalRebase}))
		require.Equal(t, uint64(50), fsm.AllocationFloor())
		require.False(t, fsm.CutoverActive())
	}
}

func tsoLegacyStorePayload(t *testing.T, format string) []byte {
	t.Helper()
	var st store.MVCCStore
	if format == "memory" {
		st = store.NewMVCCStore()
	} else {
		var err error
		st, err = store.NewPebbleStore(t.TempDir(), store.WithSSTIngestSnapshots(format == "sst"))
		require.NoError(t, err)
	}
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	require.NoError(t, st.PutAt(t.Context(), []byte("ordinary"), []byte("value"), 100, 0))
	snap, err := st.Snapshot()
	require.NoError(t, err)
	var out bytes.Buffer
	_, err = snap.WriteTo(&out)
	require.NoError(t, err)
	require.NoError(t, snap.Close())
	return out.Bytes()
}

func TestTSORestoreContextLegacyExternalRequiresDecodedGate(t *testing.T) {
	probe, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	_, supported := probe.(tsoExternalStoreRestorer)
	require.NoError(t, probe.Close())
	for _, format := range []string{"memory", "native", "sst"} {
		t.Run(format, func(t *testing.T) {
			body := tsoLegacyStorePayload(t, format)
			for _, header := range []bool{false, true} {
				raw := bytes.Clone(body)
				if header {
					prefix := make([]byte, hlcSnapshotHeaderLen)
					copy(prefix, hlcSnapshotMagic[:])
					binary.BigEndian.PutUint64(prefix[len(hlcSnapshotMagic):], 100)
					raw = bytes.Join([][]byte{prefix, raw}, nil)
				}
				fsm := NewTSOStateMachine(nil)
				err := fsm.RestoreWithContext(t.Context(), bytes.NewReader(raw), raftengine.SnapshotRestoreOptions{Origin: raftengine.SnapshotRestoreExternalRebase})
				if supported {
					require.NoError(t, err, "proof-free legacy snapshots must use the integrated store detector")
				} else {
					require.ErrorIs(t, err, ErrTSOExternalLegacySnapshotUnsupported)
					require.Zero(t, fsm.AllocationFloor())
					require.Zero(t, fsm.ceilingMs.Load())
				}
				ordinary := NewTSOStateMachine(nil)
				require.NoError(t, ordinary.RestoreWithContext(t.Context(), bytes.NewReader(raw), raftengine.SnapshotRestoreOptions{Origin: raftengine.SnapshotRestoreRaft}))
				if header {
					require.Equal(t, int64(100), ordinary.ceilingMs.Load())
				}
			}
		})
	}
}

type recordingTSOLegacyGate struct {
	store.MVCCStore
	called bool
	err    error
}

func (s *recordingTSOLegacyGate) RestoreExternal(ctx context.Context, r io.Reader) error {
	s.called = true
	if s.err != nil {
		return s.err
	}
	return s.Restore(r)
}

func TestTSORestoreContextLegacyGateDelegation(t *testing.T) {
	body := tsoLegacyStorePayload(t, "memory")
	rejected := errors.New("migration-bearing external restore unsupported")
	for _, failure := range []error{nil, rejected} {
		st := &recordingTSOLegacyGate{MVCCStore: store.NewMVCCStore(), err: failure}
		err := validateTSOLegacyStore(t.Context(), bytes.NewReader(body), st)
		require.True(t, st.called)
		if failure != nil {
			require.ErrorIs(t, err, rejected)
		} else {
			require.NoError(t, err)
			value, err := st.GetAt(t.Context(), []byte("ordinary"), 100)
			require.NoError(t, err)
			require.Equal(t, []byte("value"), value)
		}
		require.NoError(t, st.Close())
	}
}
