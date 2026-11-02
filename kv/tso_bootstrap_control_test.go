package kv

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func tsoControlStores(t *testing.T) map[string]func(*testing.T) store.MVCCStore {
	t.Helper()
	return map[string]func(*testing.T) store.MVCCStore{
		"memory": func(t *testing.T) store.MVCCStore { t.Helper(); return store.NewMVCCStore() },
		"pebble": func(t *testing.T) store.MVCCStore {
			t.Helper()
			s, err := store.NewPebbleStore(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			return s
		},
	}
}

func tsoTestBacking(t *testing.T, s store.MVCCStore) TSODurableBacking {
	t.Helper()
	b, ok := s.(TSODurableBacking)
	require.True(t, ok)
	return b
}

func TestTSOControlAtomicPublicationAndAbort(t *testing.T) {
	for name, factory := range tsoControlStores(t) {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			b := tsoTestBacking(t, s)
			hlc := NewHLC()
			f, err := NewDurableTSOStateMachine(hlc, b)
			require.NoError(t, err)
			boom := errors.New("injected commit abort")
			stage := func(ctx context.Context, metadata store.RaftControlMetadataStore) error {
				require.Zero(t, f.AllocationFloor())
				require.Zero(t, hlc.Current())
				require.NoError(t, metadata.StageRaftControlMetadata(ctx, []byte("P/test"), []byte("opaque policy")))
				require.NoError(t, metadata.StageRaftControlMetadata(ctx, []byte("R/test"), []byte("opaque receipt")))
				return nil
			}
			err = requireTSOHaltError(t, f.ApplyWithControl(t.Context(), 1, marshalTSOAllocationFloor(100), func(ctx context.Context, metadata store.RaftControlMetadataStore) error {
				require.NoError(t, stage(ctx, metadata))
				return boom
			}))
			require.ErrorIs(t, err, boom)
			require.Zero(t, f.AllocationFloor())
			require.Zero(t, hlc.Current())
			for _, key := range []string{tsoControlStateKey, "P/test", "R/test"} {
				_, err := b.ReadRaftControlMetadata(t.Context(), []byte(key))
				require.ErrorIs(t, err, store.ErrKeyNotFound)
			}
			require.Nil(t, f.ApplyWithControl(t.Context(), 1, marshalTSOAllocationFloor(100), stage))
			require.Equal(t, uint64(100), f.AllocationFloor())
			require.Equal(t, uint64(100), hlc.Current())
			require.Zero(t, s.LastCommitTS())
			raw, err := b.ReadRaftControlMetadata(t.Context(), []byte(tsoControlStateKey))
			require.NoError(t, err)
			_, index, _, err := decodeTSOControlState(raw)
			require.NoError(t, err)
			require.Equal(t, uint64(1), index)
			require.ErrorIs(t, requireTSOHaltError(t, f.ApplyWithControl(t.Context(), 1, marshalTSOAllocationFloor(200), nil)), ErrTSODurableBacking)
			require.Equal(t, uint64(100), f.AllocationFloor())
		})
	}
}

func TestTSOControlSnapshotCarriesCompleteState(t *testing.T) {
	for name, factory := range tsoControlStores(t) {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			b := tsoTestBacking(t, s)
			f, err := NewDurableTSOStateMachine(NewHLC(), b)
			require.NoError(t, err)
			entries := [][]byte{marshalHLCLeaseRenew(123), marshalTSOAllocationFloor(100), marshalTSOCutover(), marshalTSOPendingPhaseD(100), marshalTSOPhaseD(100)}
			for i, entry := range entries {
				f.SetApplyIndex(uint64(i + 1))
				require.Nil(t, f.Apply(entry))
				require.False(t, f.IsVolatileOnlyPayload(entry))
			}
			require.NoError(t, f.ApplyConfigurationWithControl(t.Context(), 6, func(ctx context.Context, metadata store.RaftControlMetadataStore) error {
				require.NoError(t, metadata.StageRaftControlMetadata(ctx, []byte("P/test"), []byte("policy")))
				return metadata.StageRaftControlMetadata(ctx, []byte("R/6"), []byte("config receipt"))
			}))
			snapshot, err := f.Snapshot()
			require.NoError(t, err)
			require.Nil(t, f.ApplyWithControl(t.Context(), 7, marshalTSOAllocationFloor(200), nil))
			var image bytes.Buffer
			_, err = snapshot.WriteTo(&image)
			require.NoError(t, err)
			require.NoError(t, snapshot.Close())
			require.NoError(t, f.Restore(bytes.NewReader(image.Bytes())))
			require.Equal(t, uint64(100), f.AllocationFloor(), "restore must replace, not merge newer scalars")
			require.Equal(t, int64(123), f.ceilingMs.Load())
			require.True(t, f.CutoverActive())
			require.True(t, f.PhaseDActive())
			require.Equal(t, uint64(100), f.PhaseDFloor())
			require.Equal(t, uint64(100), f.PrepareEvidenceProtocolFloor())
			require.Equal(t, uint64(6), f.AppliedConfigurationIndex())
			raw, err := b.ReadRaftControlMetadata(t.Context(), []byte("R/6"))
			require.NoError(t, err)
			require.Equal(t, []byte("config receipt"), raw)
			reloaded, err := NewDurableTSOStateMachine(nil, b)
			require.NoError(t, err)
			require.Equal(t, f.AllocationFloor(), reloaded.AllocationFloor())
			require.Equal(t, f.AppliedConfigurationIndex(), reloaded.AppliedConfigurationIndex())
			require.Error(t, f.Restore(bytes.NewReader(make([]byte, tsoSnapshotV1Len))))
			require.Equal(t, uint64(100), f.AllocationFloor())
		})
	}
}

func TestTSOControlRejectMalformedSnapshotBeforeSwap(t *testing.T) {
	for name, factory := range tsoControlStores(t) {
		t.Run(name, func(t *testing.T) {
			b := tsoTestBacking(t, factory(t))
			f, err := NewDurableTSOStateMachine(nil, b)
			require.NoError(t, err)
			require.Nil(t, f.ApplyWithControl(t.Context(), 1, marshalTSOAllocationFloor(100), nil))
			bad := tsoTestBacking(t, factory(t))
			require.NoError(t, bad.ApplyRaftEntry(t.Context(), 1, func(ctx context.Context) error {
				return bad.StageRaftControlMetadata(ctx, []byte(tsoControlStateKey), []byte("bad scalar record"))
			}))
			snapshot, err := bad.Snapshot()
			require.NoError(t, err)
			defer func() { require.NoError(t, snapshot.Close()) }()
			var image bytes.Buffer
			image.WriteString(tsoControlSnapshotMagic)
			_, err = snapshot.WriteTo(&image)
			require.NoError(t, err)
			require.Error(t, f.Restore(&image))
			reloaded, err := NewDurableTSOStateMachine(nil, b)
			require.NoError(t, err)
			require.Equal(t, uint64(100), reloaded.AllocationFloor())
		})
	}
}

func TestTSOControlConcurrentReaders(t *testing.T) {
	f, err := NewDurableTSOStateMachine(nil, tsoTestBacking(t, store.NewMVCCStore()))
	require.NoError(t, err)
	var readers sync.WaitGroup
	readers.Go(func() {
		for range 1000 {
			_ = f.AllocationFloor()
			_ = f.CutoverActive()
			_ = f.AppliedConfigurationIndex()
		}
	})
	for index := uint64(1); index <= 30; index++ {
		require.Nil(t, f.ApplyWithControl(t.Context(), index, marshalTSOAllocationFloor(index), nil))
	}
	readers.Wait()
}

func TestTSOControlRequiresBackingAndIndex(t *testing.T) {
	_, err := NewDurableTSOStateMachine(nil, nil)
	require.ErrorIs(t, err, ErrTSODurableBacking)
	f, err := NewDurableTSOStateMachine(nil, tsoTestBacking(t, store.NewMVCCStore()))
	require.NoError(t, err)
	require.ErrorIs(t, requireTSOHaltError(t, f.Apply(marshalTSOCutover())), ErrTSODurableBacking)
	require.False(t, f.CutoverActive())
}

func TestTSOControlLegacyInitialization(t *testing.T) {
	for name, factory := range tsoControlStores(t) {
		t.Run(name, func(t *testing.T) {
			legacy := NewTSOStateMachine(nil)
			require.Nil(t, legacy.Apply(marshalHLCLeaseRenew(123)))
			require.Nil(t, legacy.Apply(marshalTSOAllocationFloor(456)))
			snapshot, err := legacy.Snapshot()
			require.NoError(t, err)
			var image bytes.Buffer
			_, err = snapshot.WriteTo(&image)
			require.NoError(t, err)
			require.NoError(t, snapshot.Close())
			f, err := NewDurableTSOStateMachine(nil, tsoTestBacking(t, factory(t)))
			require.NoError(t, err)
			require.NoError(t, f.InitializeLegacyControl(t.Context(), 10, 7, bytes.NewReader(image.Bytes()), nil))
			require.Equal(t, int64(123), f.ceilingMs.Load())
			require.Equal(t, uint64(456), f.AllocationFloor())
			require.Equal(t, uint64(7), f.AppliedConfigurationIndex())
			require.ErrorIs(t, f.InitializeLegacyControl(t.Context(), 11, 7, bytes.NewReader(image.Bytes()), nil), ErrTSODurableBacking)
		})
	}
}

func TestTSOControlDiskReopen(t *testing.T) {
	t.Setenv("ELASTICKV_FSM_SYNC_MODE", "nosync")
	dir := t.TempDir()
	s, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	f, err := NewDurableTSOStateMachine(nil, tsoTestBacking(t, s))
	require.NoError(t, err)
	require.Nil(t, f.ApplyWithControl(t.Context(), 1, marshalHLCLeaseRenew(123), nil))
	require.Nil(t, f.ApplyWithControl(t.Context(), 2, marshalTSOAllocationFloor(456), nil))
	require.NoError(t, f.ApplyConfigurationWithControl(t.Context(), 3, func(ctx context.Context, metadata store.RaftControlMetadataStore) error {
		return metadata.StageRaftControlMetadata(ctx, []byte("P/opaque"), []byte("do not interpret"))
	}))
	require.NoError(t, s.Close())
	s, err = store.NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	b := tsoTestBacking(t, s)
	f, err = NewDurableTSOStateMachine(nil, b)
	require.NoError(t, err)
	require.Equal(t, int64(123), f.ceilingMs.Load())
	require.Equal(t, uint64(456), f.AllocationFloor())
	require.Equal(t, uint64(3), f.AppliedConfigurationIndex())
	raw, err := b.ReadRaftControlMetadata(t.Context(), []byte("P/opaque"))
	require.NoError(t, err)
	require.Equal(t, []byte("do not interpret"), raw)
}

type tsoFailCommitBacking struct{ TSODurableBacking }

func (b tsoFailCommitBacking) ApplyRaftEntry(ctx context.Context, index uint64, stage func(context.Context) error) error {
	return b.TSODurableBacking.ApplyRaftEntry(ctx, index, func(ctx context.Context) error {
		if err := stage(ctx); err != nil {
			return err
		}
		return errors.New("injected final batch failure")
	})
}

func TestTSOControlCommitFailureDoesNotPublish(t *testing.T) {
	b := tsoTestBacking(t, store.NewMVCCStore())
	hlc := NewHLC()
	f, err := NewDurableTSOStateMachine(hlc, tsoFailCommitBacking{TSODurableBacking: b})
	require.NoError(t, err)
	require.Error(t, requireTSOHaltError(t, f.ApplyWithControl(t.Context(), 1, marshalHLCLeaseRenew(123), nil)))
	require.Zero(t, f.ceilingMs.Load())
	require.Zero(t, hlc.PhysicalCeiling())
	_, err = b.ReadRaftControlMetadata(t.Context(), []byte(tsoControlStateKey))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestTSOControlCheckpointPinsWholeState(t *testing.T) {
	for name, factory := range tsoControlStores(t) {
		t.Run(name, func(t *testing.T) {
			b := tsoTestBacking(t, factory(t))
			f, err := NewDurableTSOStateMachine(nil, b)
			require.NoError(t, err)
			require.Nil(t, f.ApplyWithControl(t.Context(), 1, marshalTSOAllocationFloor(123), nil))
			require.NoError(t, f.ApplyConfigurationWithControl(t.Context(), 2, nil))
			require.NoError(t, f.RecordAppliedConfiguration(2))
			require.NoError(t, f.SetDurableAppliedIndex(10))
			require.NoError(t, f.SetDurableAppliedIndex(3))
			raw, err := b.ReadRaftControlMetadata(t.Context(), []byte(tsoControlStateKey))
			require.NoError(t, err)
			c, index, configuration, err := decodeTSOControlState(raw)
			require.NoError(t, err)
			require.Equal(t, uint64(10), index)
			require.Equal(t, uint64(2), configuration)
			require.Equal(t, uint64(123), c.AllocationFloor())
		})
	}
}
