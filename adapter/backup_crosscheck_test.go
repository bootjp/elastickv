package adapter

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"testing"

	logicalbackup "github.com/bootjp/elastickv/internal/backup"
	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

// The backup_crosscheck_*_test.go files pin the adapter encoders that
// internal/backup re-implements (M4 of
// docs/design/2026_10_16_proposed_production_commonization.md and the
// "duplicated ... covered by a cross-check test" promise of
// docs/design/2026_05_25_implemented_snapshot_logical_encoder.md).
// internal/backup may not import this package, so it carries its own copies
// of the key builders and value codecs. Each cross-check builds a logical
// object with the live builders and pushes those exact bytes through the
// offline tools, using only internal/backup's exported API:
//
//	live records --LiveDecoder--> dump tree --EncodeSnapshot--> .fsm --DecodeLiveEntries--> restored records
//
// The restored user keys and values must be byte-identical to the live ones.
// The dump step exercises the decoder's mirrors (prefix routing, key parsing,
// value magic, number canonicalisation); the restore step exercises the
// encoder's mirrors (key builders, value codecs, derived index rows). Where a
// restore cannot reproduce live state by design, the live side is built in
// the state the restore is documented to produce, and the test says so.

// xcheckCommitTS is the snapshot-global commit timestamp the restore stamps
// on every record.
const xcheckCommitTS uint64 = 0x0001_9A2B_3C4D_0007

// xcheckPair is one record as a live write path stages it.
type xcheckPair struct {
	key   []byte
	value []byte
}

// xcheckRow is the comparable form of a record. Keys and values are quoted
// so a mismatch prints a readable diff of binary bytes.
type xcheckRow struct {
	Key      string
	Value    string
	ExpireAt uint64
}

// xcheckPutPairs returns the records a live OperationGroup writes. The
// fixtures only model writes into an empty keyspace, so any element other
// than a Put is a fixture bug.
func xcheckPutPairs(t *testing.T, elems []*kv.Elem[kv.OP]) []xcheckPair {
	t.Helper()
	out := make([]xcheckPair, 0, len(elems))
	for _, e := range elems {
		require.Equal(t, kv.Put, e.Op, "unexpected non-Put element for key %q", e.Key)
		out = append(out, xcheckPair{key: bytes.Clone(e.Key), value: bytes.Clone(e.Value)})
	}
	return out
}

// xcheckRoundTrip dumps the live records with the logical-backup decoder,
// restores the dump with the encoder, and returns the dump root together
// with the records the restored snapshot holds.
func xcheckRoundTrip(t *testing.T, adapters logicalbackup.AdapterSet, live []xcheckPair) (string, []logicalbackup.RoundTripEntry) {
	t.Helper()
	dumpDir := t.TempDir()
	var warnings []string
	dec, err := logicalbackup.NewLiveDecoder(logicalbackup.DecodeOptions{
		OutRoot:  dumpDir,
		Adapters: adapters,
		WarnSink: func(event string, fields ...any) {
			warnings = append(warnings, fmt.Sprint(append([]any{event}, fields...)...))
		},
	})
	require.NoError(t, err)
	// Pebble snapshot scans yield records in key order; feed them the same way.
	for _, p := range xcheckSortedPairs(t, live) {
		require.NoError(t, dec.Add(p.key, p.value), "dump record %q", p.key)
	}
	counters, err := dec.Finalize()
	require.NoError(t, err)
	require.Empty(t, warnings, "decoder warnings")
	// Every live record must reach its adapter's decoder. An Unknown or
	// Internal count means the backup prefix table no longer recognises a
	// live key family.
	require.Zero(t, counters.Unknown, "live records with no backup route")
	require.Zero(t, counters.Internal, "live records dropped as internal")
	require.Equal(t, uint64(len(live)), counters.Total)

	var fsm bytes.Buffer
	_, err = logicalbackup.EncodeSnapshot(logicalbackup.EncodeOptions{
		InputRoot:            dumpDir,
		Adapters:             adapters,
		LastCommitTS:         xcheckCommitTS,
		AllowMissingManifest: true,
	}, &fsm)
	require.NoError(t, err)
	restored, hdr, err := logicalbackup.DecodeLiveEntries(&fsm)
	require.NoError(t, err)
	require.Equal(t, xcheckCommitTS, hdr.LastCommitTS)
	return dumpDir, restored
}

// xcheckRequireIdentical requires the restored records to equal the live
// records byte for byte: the same key set, the same values, and no MVCC
// expiry (live adapter writes never set one; kv.Elem has no expiry field).
func xcheckRequireIdentical(t *testing.T, live []xcheckPair, restored []logicalbackup.RoundTripEntry) {
	t.Helper()
	want := make([]xcheckRow, 0, len(live))
	for _, p := range xcheckSortedPairs(t, live) {
		want = append(want, xcheckRow{Key: strconv.Quote(string(p.key)), Value: strconv.Quote(string(p.value))})
	}
	require.Equal(t, want, xcheckRestoredRows(restored))
}

// xcheckRestoredRows converts restored records into rows in key order.
func xcheckRestoredRows(restored []logicalbackup.RoundTripEntry) []xcheckRow {
	sorted := make([]logicalbackup.RoundTripEntry, len(restored))
	copy(sorted, restored)
	sort.Slice(sorted, func(i, j int) bool { return bytes.Compare(sorted[i].UserKey, sorted[j].UserKey) < 0 })
	rows := make([]xcheckRow, 0, len(sorted))
	for _, e := range sorted {
		rows = append(rows, xcheckRow{
			Key:      strconv.Quote(string(e.UserKey)),
			Value:    strconv.Quote(string(e.UserValue)),
			ExpireAt: e.ExpireAt,
		})
	}
	return rows
}

// xcheckSortedPairs returns the records in key order and fails on a
// duplicated key, which would mean two fixture writes collide.
func xcheckSortedPairs(t *testing.T, pairs []xcheckPair) []xcheckPair {
	t.Helper()
	sorted := make([]xcheckPair, len(pairs))
	copy(sorted, pairs)
	sort.Slice(sorted, func(i, j int) bool { return bytes.Compare(sorted[i].key, sorted[j].key) < 0 })
	for i := 1; i < len(sorted); i++ {
		require.NotEqual(t, sorted[i-1].key, sorted[i].key, "fixture writes the same key twice")
	}
	return sorted
}
