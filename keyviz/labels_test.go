package keyviz

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAllLabelsAvoidBucketIDSeparator(t *testing.T) {
	t.Parallel()
	for _, label := range AllLabels {
		require.NotContains(t, string(label), ":", "label %q must not break route:<id>:<label>", label)
	}
}

func TestAllLabelConstantsInAllLabels(t *testing.T) {
	t.Parallel()
	want := []Label{LabelDynamo, LabelRedis, LabelS3, LabelSQS, LabelRawKV}
	for _, label := range want {
		require.True(t, labelInAllLabels(label), "AllLabels must include %q", label)
	}
}

func TestAllLabelsWithLegacyFreshSlice(t *testing.T) {
	t.Parallel()
	labels := allLabelsWithLegacy()
	require.Len(t, labels, len(AllLabels)+1)
	require.Equal(t, LabelLegacy, labels[len(labels)-1])
	labels[0] = Label("mutated")
	require.Equal(t, LabelDynamo, AllLabels[0], "helper must not expose AllLabels storage")
}

func TestLabelSlotPrecreationAndRemoveRoute(t *testing.T) {
	t.Parallel()
	s := NewMemSampler(MemSamplerOptions{Step: time.Second, HistoryColumns: 4, KeyVizLabelsEnabled: true})
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("z"), 7))

	tbl := s.table.Load()
	require.Len(t, tbl.slots, len(AllLabels)+1)
	for _, label := range allLabelsWithLegacy() {
		slot := tbl.slots[slotKey{RouteID: 1, Label: label}]
		require.NotNil(t, slot, "missing pre-created slot for label %q", label)
		require.Equal(t, label, slot.Label)
	}

	s.RemoveRoute(1)
	require.Empty(t, s.table.Load().slots)
	s.retiredMu.Lock()
	retired := len(s.retiredSlots)
	s.retiredMu.Unlock()
	require.Equal(t, len(AllLabels)+1, retired)
}

func TestLabelDisabledKeepsLegacySlotOnly(t *testing.T) {
	t.Parallel()
	s := NewMemSampler(MemSamplerOptions{Step: time.Second, HistoryColumns: 4})
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("z"), 0))
	require.Len(t, s.table.Load().slots, 1)

	s.Observe(1, []byte("k"), OpWrite, 0, LabelDynamo)
	s.Flush()
	rows := s.Snapshot(time.Time{}, time.Time{})[0].Rows
	require.Len(t, rows, 1)
	require.Equal(t, LabelLegacy, rows[0].Label)
	require.Equal(t, uint64(1), rows[0].Writes)
}

func TestObserveSplitsRowsByLabelWhenEnabled(t *testing.T) {
	t.Parallel()
	s := NewMemSampler(MemSamplerOptions{Step: time.Second, HistoryColumns: 4, KeyVizLabelsEnabled: true})
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("z"), 0))

	s.Observe(1, []byte("d"), OpWrite, 0, LabelDynamo)
	s.Observe(1, []byte("r"), OpWrite, 0, LabelRedis)
	s.Flush()

	rows := s.Snapshot(time.Time{}, time.Time{})[0].Rows
	require.Len(t, rows, 2)
	byLabel := map[Label]MatrixRow{}
	for _, row := range rows {
		byLabel[row.Label] = row
	}
	require.Equal(t, uint64(1), byLabel[LabelDynamo].Writes)
	require.Equal(t, uint64(1), byLabel[LabelRedis].Writes)
}

func TestCoarsenedRouteEmitsLegacyLabel(t *testing.T) {
	t.Parallel()
	s := NewMemSampler(MemSamplerOptions{Step: time.Second, HistoryColumns: 4, MaxTrackedRoutes: 1, KeyVizLabelsEnabled: true})
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("m"), 0))
	require.False(t, s.RegisterRoute(2, []byte("m"), []byte("z"), 0))

	s.Observe(2, []byte("n"), OpWrite, 0, LabelS3)
	s.Flush()
	rows := s.Snapshot(time.Time{}, time.Time{})[0].Rows
	require.Len(t, rows, 1)
	require.True(t, rows[0].Aggregate)
	require.Equal(t, LabelLegacy, rows[0].Label)
	require.Equal(t, uint64(1), rows[0].Writes)
}

func labelInAllLabels(label Label) bool {
	for _, candidate := range AllLabels {
		if candidate == label {
			return true
		}
	}
	return false
}
