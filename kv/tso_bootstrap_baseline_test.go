package kv

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func tsoBaselineReplay(entries ...TSOBaselineEntry) TSOBaselineReplay {
	return func(yield func(TSOBaselineEntry) error) error {
		for _, entry := range entries {
			if err := yield(entry); err != nil {
				return err
			}
		}
		return nil
	}
}

func TestTSOBaselineReplayAndExactScalarImport(t *testing.T) {
	t.Parallel()
	cut := TSOBaselineCut{SnapshotIndex: 1, SnapshotTerm: 1, CommitIndex: 4, CommitTerm: 2, LastLogIndex: 4, ConfigurationIndex: 1}
	baseline, err := ReconstructTSOBaseline(t.Context(), bytes.NewReader(make([]byte, tsoSnapshotV1Len)), cut, tsoBaselineReplay(
		TSOBaselineEntry{Index: 2, Term: 1, Payload: marshalHLCLeaseRenew(20)},
		TSOBaselineEntry{Index: 3, Term: 2, Payload: marshalTSOAllocationFloor(100)},
		TSOBaselineEntry{Index: 4, Term: 2},
	))
	require.NoError(t, err)
	require.Len(t, baseline.ScalarSnapshot, tsoSnapshotV4Len)
	require.Equal(t, uint64(100), baseline.Watermark)
	require.Equal(t, uint64(4), baseline.AppliedIndex)
	require.Equal(t, uint64(1), baseline.ConfigurationIndex)
	for name, factory := range tsoControlStores(t) {
		t.Run(name, func(t *testing.T) {
			state, err := NewDurableTSOStateMachine(nil, tsoTestBacking(t, factory(t)))
			require.NoError(t, err)
			require.NoError(t, state.InitializeLegacyControl(t.Context(), baseline.AppliedIndex, baseline.ConfigurationIndex, bytes.NewReader(baseline.ScalarSnapshot), nil))
			got, err := state.ScalarBaseline(baseline.AppliedIndex, baseline.ConfigurationIndex)
			require.NoError(t, err)
			require.Equal(t, baseline, got)
		})
	}
}

func TestTSOBaselineLeaseOnlyDoesNotSynthesizeFloorOnImport(t *testing.T) {
	t.Parallel()
	cut := TSOBaselineCut{SnapshotIndex: 1, SnapshotTerm: 1, CommitIndex: 2, CommitTerm: 1, LastLogIndex: 2, ConfigurationIndex: 1}
	baseline, err := ReconstructTSOBaseline(t.Context(), bytes.NewReader(make([]byte, tsoSnapshotV1Len)), cut, tsoBaselineReplay(TSOBaselineEntry{Index: 2, Term: 1, Payload: marshalHLCLeaseRenew(20)}))
	require.NoError(t, err)
	require.Zero(t, baseline.Watermark)
	state, err := NewDurableTSOStateMachine(nil, tsoTestBacking(t, store.NewMVCCStore()))
	require.NoError(t, err)
	require.NoError(t, state.InitializeLegacyControl(t.Context(), 2, 1, bytes.NewReader(baseline.ScalarSnapshot), nil))
	require.Zero(t, state.AllocationFloor())
	require.Equal(t, int64(20), state.ceilingMs.Load())
}

func TestTSOBaselineCanonicalAcrossLegacySnapshotLayouts(t *testing.T) {
	t.Parallel()
	cut := TSOBaselineCut{SnapshotIndex: 7, SnapshotTerm: 2, CommitIndex: 7, CommitTerm: 2, LastLogIndex: 7, ConfigurationIndex: 1}
	legacy := &tsoFSMSnapshot{ceilingMs: 20, allocationFloor: 100}
	short, err := legacy.encode()
	require.NoError(t, err)
	first, err := ReconstructTSOBaseline(t.Context(), bytes.NewReader(short), cut, nil)
	require.NoError(t, err)
	second, err := ReconstructTSOBaseline(t.Context(), bytes.NewReader(first.ScalarSnapshot), cut, nil)
	require.NoError(t, err)
	require.Equal(t, first, second)
}

func TestTSOBaselineRejectsInvalidSource(t *testing.T) {
	t.Parallel()
	valid := TSOBaselineCut{SnapshotIndex: 1, SnapshotTerm: 1, CommitIndex: 3, CommitTerm: 2, LastLogIndex: 3, ConfigurationIndex: 1}
	for _, kind := range []string{"gap", "duplicate", "tail", "bad-term", "unknown-type", "unknown-payload", "encrypted", "missing"} {
		t.Run(kind, func(t *testing.T) {
			cut := valid
			entries := []TSOBaselineEntry{{Index: 2, Term: 1, Payload: marshalHLCLeaseRenew(20)}, {Index: 3, Term: 2}}
			switch kind {
			case "gap":
				entries[0].Index = 3
			case "duplicate":
				entries[1].Index = 2
			case "tail":
				cut.LastLogIndex++
			case "bad-term":
				entries[0].Term = 3
			case "unknown-type":
				entries[0].Type = 99
			case "unknown-payload":
				entries[0].Payload = []byte{0xff}
			case "encrypted":
				entries[0].Payload = []byte("EKVENC01")
			case "missing":
				entries = entries[:1]
			}
			_, err := ReconstructTSOBaseline(t.Context(), bytes.NewReader(make([]byte, tsoSnapshotV1Len)), cut, tsoBaselineReplay(entries...))
			require.Error(t, err)
		})
	}
}

func TestTSOBaselineRejectsIncompletePhaseD(t *testing.T) {
	t.Parallel()
	cut := TSOBaselineCut{SnapshotIndex: 1, SnapshotTerm: 1, CommitIndex: 3, CommitTerm: 2, LastLogIndex: 3, ConfigurationIndex: 1}
	_, err := ReconstructTSOBaseline(t.Context(), bytes.NewReader(make([]byte, tsoSnapshotV1Len)), cut, tsoBaselineReplay(
		TSOBaselineEntry{Index: 2, Term: 1, Payload: marshalTSOCutover()}, TSOBaselineEntry{Index: 3, Term: 2, Payload: marshalTSOPendingPhaseD(100)},
	))
	require.ErrorIs(t, err, ErrTSOBaseline)
}

func TestTSOBaselineRejectsIgnoredReplayErrorAndCancellation(t *testing.T) {
	t.Parallel()
	cut := TSOBaselineCut{SnapshotIndex: 1, SnapshotTerm: 1, CommitIndex: 2, CommitTerm: 1, LastLogIndex: 2, ConfigurationIndex: 1}
	_, err := ReconstructTSOBaseline(t.Context(), bytes.NewReader(make([]byte, tsoSnapshotV1Len)), cut, func(yield func(TSOBaselineEntry) error) error {
		_ = yield(TSOBaselineEntry{Index: 2, Term: 1, Payload: []byte{0xff}})
		_ = yield(TSOBaselineEntry{Index: 2, Term: 1, Payload: marshalHLCLeaseRenew(20)})
		return nil
	})
	require.Error(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = ReconstructTSOBaseline(ctx, nil, cut, nil)
	require.ErrorIs(t, err, context.Canceled)
	_, err = ReconstructTSOBaseline(t.Context(), bytes.NewReader(make([]byte, tsoSnapshotV5Len+1)), cut, nil)
	require.Error(t, err)
	_, err = ReconstructTSOBaseline(t.Context(), nil, cut, nil)
	require.ErrorIs(t, err, ErrTSOBaseline)
}

func TestTSOBaselineRequiresCompleteConfigurationSuffix(t *testing.T) {
	t.Parallel()
	cut := TSOBaselineCut{CommitIndex: 3, CommitTerm: 1, LastLogIndex: 3, ConfigurationIndex: 1}
	result, err := ReconstructTSOBaseline(t.Context(), nil, cut, tsoBaselineReplay(
		TSOBaselineEntry{Index: 1, Term: 1, Type: 1, Payload: []byte("native-validated-conf")},
		TSOBaselineEntry{Index: 2, Term: 1, Payload: marshalHLCLeaseRenew(20)}, TSOBaselineEntry{Index: 3, Term: 1},
	))
	require.NoError(t, err)
	require.Equal(t, uint64(3), result.AppliedIndex)
	_, err = ReconstructTSOBaseline(t.Context(), nil, cut, tsoBaselineReplay(TSOBaselineEntry{Index: 1, Term: 1}, TSOBaselineEntry{Index: 2, Term: 1}, TSOBaselineEntry{Index: 3, Term: 1}))
	require.True(t, errors.Is(err, ErrTSOBaseline))
}
