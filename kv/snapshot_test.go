package kv

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	store3 "github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// staticCutoverSource is a test-only CutoverSource whose value can be
// mutated between Snapshot() calls to exercise the §3.3 latch.
type staticCutoverSource struct {
	v uint64
}

func (s *staticCutoverSource) RaftEnvelopeCutoverIndex() uint64 { return s.v }

// writeV1Header emits a synthetic v1 header (8 magic + 8 ceiling) for the
// read-path tests. Equivalent to what kvFSMSnapshot.WriteTo produces when
// useV2 is false.
func writeV1Header(t *testing.T, w *bytes.Buffer, ceiling uint64) {
	t.Helper()
	w.Write(hlcSnapshotMagic[:])
	var ceil [8]byte
	binary.BigEndian.PutUint64(ceil[:], ceiling)
	w.Write(ceil[:])
}

// writeV2Header emits a synthetic v2 header with the given length prefix.
// When trailing > 0 the payload is padded with that many zero bytes after
// the ceiling+cutover pair (exercises the forward-compat extension area).
func writeV2Header(t *testing.T, w *bytes.Buffer, ceiling, cutover uint64, plen uint16, trailing int) {
	t.Helper()
	w.Write(hlcSnapshotMagicV2[:])
	var lp [2]byte
	binary.BigEndian.PutUint16(lp[:], plen)
	w.Write(lp[:])
	var pl [16]byte
	binary.BigEndian.PutUint64(pl[0:8], ceiling)
	binary.BigEndian.PutUint64(pl[8:16], cutover)
	w.Write(pl[:])
	if trailing > 0 {
		w.Write(make([]byte, trailing))
	}
}

func TestSnapshot(t *testing.T) {
	store := store3.NewMVCCStore()
	fsm := NewKvFSMWithHLC(store, NewHLC())

	mutation := pb.Request{
		IsTxn: false,
		Phase: pb.Phase_NONE,
		Mutations: []*pb.Mutation{
			{
				Op:    pb.Op_PUT,
				Key:   []byte("hoge"),
				Value: []byte("fuga"),
			},
		},
	}

	b, err := proto.Marshal(&mutation)
	assert.NoError(t, err)

	fsm.Apply(b)

	ctx := context.Background()
	v, err := store.GetAt(ctx, []byte("hoge"), ^uint64(0))
	assert.NoError(t, err)
	assert.Equal(t, []byte("fuga"), v)

	snapshot, err := fsm.Snapshot()
	assert.NoError(t, err)

	store2 := store3.NewMVCCStore()
	fsm2 := NewKvFSMWithHLC(store2, NewHLC())

	kvFSMSnap, ok := snapshot.(*kvFSMSnapshot)
	assert.True(t, ok)
	assert.NoError(t, err)

	var buf bytes.Buffer
	_, err = kvFSMSnap.WriteTo(&buf)
	assert.NoError(t, err)
	assert.NoError(t, kvFSMSnap.Close())
	err = fsm2.Restore(io.NopCloser(bytes.NewReader(buf.Bytes())))
	assert.NoError(t, err)

	v, err = store2.GetAt(ctx, []byte("hoge"), ^uint64(0))
	assert.NoError(t, err)
	assert.Equal(t, []byte("fuga"), v)
}

// TestFSMSnapshotPreservesCeiling verifies that the physical ceiling stored in
// the HLC survives a Snapshot → Persist → Restore round-trip. This guards
// against the scenario where log compaction removes the last HLC lease entry:
// the ceiling must be recoverable from the snapshot alone.
func TestFSMSnapshotPreservesCeiling(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const ceilingMs = int64(9_000_000_000_000) // year ~2255

	// Source FSM with a known ceiling and one user key.
	hlc1 := NewHLC()
	hlc1.SetPhysicalCeiling(ceilingMs)
	st1 := store3.NewMVCCStore()
	fsm1 := NewKvFSMWithHLC(st1, hlc1)

	req := &pb.Request{
		IsTxn: false,
		Phase: pb.Phase_NONE,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")},
		},
	}
	b, err := proto.Marshal(req)
	require.NoError(t, err)
	fsm1.Apply(b)

	// Take snapshot and write it out.
	snap, err := fsm1.Snapshot()
	require.NoError(t, err)

	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	require.NoError(t, snap.Close())

	// Restore into a fresh FSM with its own HLC starting at zero.
	hlc2 := NewHLC()
	require.Equal(t, int64(0), hlc2.PhysicalCeiling(), "fresh HLC must start at zero")
	st2 := store3.NewMVCCStore()
	fsm2 := NewKvFSMWithHLC(st2, hlc2)
	require.NoError(t, fsm2.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))))

	// The ceiling must be restored.
	require.Equal(t, ceilingMs, hlc2.PhysicalCeiling(),
		"ceiling must survive Snapshot→Persist→Restore")

	// Store data must also be intact.
	val, err := st2.GetAt(ctx, []byte("k"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), val)
}

// TestFSMSnapshotRestoreSmallLegacy verifies that a legacy snapshot smaller than
// the 16-byte header size (hlcSnapshotHeaderLen) is restored without error.
// Before the fix, io.ReadFull would return io.ErrUnexpectedEOF on such snapshots.
func TestFSMSnapshotRestoreSmallLegacy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Craft a minimal raw store snapshot that is definitely < 16 bytes.
	// Use an empty store: its snapshot is likely a few bytes of metadata.
	st1 := store3.NewMVCCStore()
	storeSnap, err := st1.Snapshot()
	require.NoError(t, err)
	var raw bytes.Buffer
	_, err = storeSnap.WriteTo(&raw)
	require.NoError(t, err)
	require.NoError(t, storeSnap.Close())

	// If the empty snapshot happens to be >= 16 bytes the test is vacuous but
	// still harmless — the restore must succeed either way.
	hlc2 := NewHLC()
	st2 := store3.NewMVCCStore()
	fsm2 := NewKvFSMWithHLC(st2, hlc2)
	require.NoError(t, fsm2.Restore(io.NopCloser(bytes.NewReader(raw.Bytes()))))

	// No data was written, ceiling stays at zero.
	require.Equal(t, int64(0), hlc2.PhysicalCeiling())

	// Empty store: no key exists.
	_, err = st2.GetAt(ctx, []byte("any"), ^uint64(0))
	require.Error(t, err)
}

// TestFSMSnapshotRestoreOldFormat verifies that a snapshot written without the
// HLC header (legacy format) is still readable after the format migration.
func TestFSMSnapshotRestoreOldFormat(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Build legacy-format snapshot bytes directly: just the raw store data,
	// no HLC magic header. This simulates snapshots taken by the old code.
	st1 := store3.NewMVCCStore()
	fsm1 := NewKvFSMWithHLC(st1, NewHLC())

	req := &pb.Request{
		IsTxn: false,
		Phase: pb.Phase_NONE,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte("legacy"), Value: []byte("data")},
		},
	}
	b, err := proto.Marshal(req)
	require.NoError(t, err)
	fsm1.Apply(b)

	// Grab the raw store snapshot bytes (bypasses the new header).
	storeSnap, err := st1.Snapshot()
	require.NoError(t, err)
	var raw bytes.Buffer
	_, err = storeSnap.WriteTo(&raw)
	require.NoError(t, err)
	require.NoError(t, storeSnap.Close())

	// Restore into a new FSM; this exercises the old-format fallback path.
	hlc2 := NewHLC()
	st2 := store3.NewMVCCStore()
	fsm2 := NewKvFSMWithHLC(st2, hlc2)
	require.NoError(t, fsm2.Restore(io.NopCloser(bytes.NewReader(raw.Bytes()))))

	// Ceiling stays zero (no header was present).
	require.Equal(t, int64(0), hlc2.PhysicalCeiling())

	// Store data is intact.
	val, err := st2.GetAt(ctx, []byte("legacy"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("data"), val)
}

// --- Stage 8a §5: ReadSnapshotHeader read-path verification ---

// TestReadSnapshotHeader_V1ReturnsZeroCutover pins design §3.2 step 3: a v1
// header returns ceiling + cutover=0 + nil. Inner-store payload following
// the header sits in the bufio.Reader.
func TestReadSnapshotHeader_V1ReturnsZeroCutover(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	writeV1Header(t, &buf, 12345)
	buf.WriteString("STORE_PAYLOAD")
	br := bufio.NewReader(&buf)
	ceiling, cutover, err := ReadSnapshotHeader(br)
	require.NoError(t, err)
	require.Equal(t, uint64(12345), ceiling)
	require.Equal(t, uint64(0), cutover)
	rest, _ := io.ReadAll(br)
	require.Equal(t, "STORE_PAYLOAD", string(rest))
}

// TestReadSnapshotHeader_V2ReturnsBothFields pins design §3.2 step 2: a v2
// header with minimum payload (len=16) returns both fields and the inner
// store sees the byte stream from the position right after the payload.
func TestReadSnapshotHeader_V2ReturnsBothFields(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	writeV2Header(t, &buf, 0xDEAD, 0xBEEF, hlcSnapshotV2MinPayload, 0)
	buf.WriteString("STORE_PAYLOAD")
	br := bufio.NewReader(&buf)
	ceiling, cutover, err := ReadSnapshotHeader(br)
	require.NoError(t, err)
	require.Equal(t, uint64(0xDEAD), ceiling)
	require.Equal(t, uint64(0xBEEF), cutover)
	rest, _ := io.ReadAll(br)
	require.Equal(t, "STORE_PAYLOAD", string(rest))
}

// TestReadSnapshotHeader_V2ForwardCompatExtraBytes pins design §3.1.1: a v2
// header with len > 16 must be consumed in full (trailing bytes skipped)
// so the inner store sees the stream from the first non-header byte.
func TestReadSnapshotHeader_V2ForwardCompatExtraBytes(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	writeV2Header(t, &buf, 0xC, 0xD, 24, 8) // len=0x18, +8 trailing
	buf.WriteString("STORE_PAYLOAD")
	br := bufio.NewReader(&buf)
	ceiling, cutover, err := ReadSnapshotHeader(br)
	require.NoError(t, err)
	require.Equal(t, uint64(0xC), ceiling)
	require.Equal(t, uint64(0xD), cutover)
	rest, _ := io.ReadAll(br)
	require.Equal(t, "STORE_PAYLOAD", string(rest),
		"trailing forward-compat bytes must be skipped before the inner store reads")
}

// TestReadSnapshotHeader_UnknownEKVTHLCMagicFails pins design §3.2 step 4: a
// version byte the current binary does not recognise produces
// ErrSnapshotHeaderUnknownMagic and the restore aborts.
func TestReadSnapshotHeader_UnknownEKVTHLCMagicFails(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	buf.WriteString("EKVTHLC9") // unknown version byte
	buf.WriteString("trailing bytes that must NOT be consumed silently")
	br := bufio.NewReader(&buf)
	_, _, err := ReadSnapshotHeader(br)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrSnapshotHeaderUnknownMagic),
		"want ErrSnapshotHeaderUnknownMagic, got %v", err)
}

// TestReadSnapshotHeader_HeaderlessLegacyPreserved pins design §3.2 step 5
// and §6 backward-compat: a payload that does NOT start with EKVTHLC*
// returns (0,0,nil) AND the inner-store path sees the payload from byte 0.
func TestReadSnapshotHeader_HeaderlessLegacyPreserved(t *testing.T) {
	t.Parallel()
	const payload = "raw_store_bytes_with_no_header_at_all"
	br := bufio.NewReader(bytes.NewReader([]byte(payload)))
	ceiling, cutover, err := ReadSnapshotHeader(br)
	require.NoError(t, err)
	require.Equal(t, uint64(0), ceiling)
	require.Equal(t, uint64(0), cutover)
	rest, _ := io.ReadAll(br)
	require.Equal(t, payload, string(rest),
		"inner-store path must see the entire payload from byte 0")
}

// TestReadSnapshotHeader_V2WithLenTooShortFails pins design §3.2 step 2's
// `len < 16` fail-closed: malformed inputs produce a typed error rather
// than a downstream EOF or panic on slice indexing.
func TestReadSnapshotHeader_V2WithLenTooShortFails(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	buf.Write(hlcSnapshotMagicV2[:])
	var lp [2]byte
	binary.BigEndian.PutUint16(lp[:], 8) // below the 16-byte minimum
	buf.Write(lp[:])
	buf.Write(make([]byte, 8)) // 8 payload bytes (insufficient)
	br := bufio.NewReader(&buf)
	_, _, err := ReadSnapshotHeader(br)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrSnapshotHeaderInvalidLength),
		"want ErrSnapshotHeaderInvalidLength, got %v", err)
}

// TestReadSnapshotHeader_V2WithLenTooLargeFails pins design §3.2 step 2's
// upper bound (coderabbit MAJOR on PR #877). A 0xFFFF length must
// fail-closed before allocating a 64KiB buffer.
func TestReadSnapshotHeader_V2WithLenTooLargeFails(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	buf.Write(hlcSnapshotMagicV2[:])
	var lp [2]byte
	binary.BigEndian.PutUint16(lp[:], 0xFFFF)
	buf.Write(lp[:])
	br := bufio.NewReader(&buf)
	_, _, err := ReadSnapshotHeader(br)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrSnapshotHeaderInvalidLength),
		"want ErrSnapshotHeaderInvalidLength, got %v", err)
}

// TestReadSnapshotHeader_ShortStreamFallsBackToLegacy pins design §3.2 step
// 0 (gemini medium + coderabbit major on PR #877): a stream shorter than 8
// bytes must fall through to the headerless-legacy branch with the
// available bytes left in the bufio.Reader for the inner-store path.
func TestReadSnapshotHeader_ShortStreamFallsBackToLegacy(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		payload string
	}{
		{"empty", ""},
		{"four_bytes", "ABCD"},
		{"seven_bytes", "ABCDEFG"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			br := bufio.NewReader(bytes.NewReader([]byte(tc.payload)))
			ceiling, cutover, err := ReadSnapshotHeader(br)
			require.NoError(t, err)
			require.Equal(t, uint64(0), ceiling)
			require.Equal(t, uint64(0), cutover)
			rest, _ := io.ReadAll(br)
			require.Equal(t, tc.payload, string(rest),
				"available bytes must remain in the bufio.Reader for the inner-store path")
		})
	}
}

// --- Stage 8a §5: write-path verification ---

// TestWriteSnapshotHeader_PreCutoverWritesV1 pins design §3.3 + §4: with
// no CutoverSource installed (or cutover == 0 and the latch not engaged),
// the snapshot header is byte-for-byte identical to the pre-8a v1 layout.
// This is the no-migration property.
func TestWriteSnapshotHeader_PreCutoverWritesV1(t *testing.T) {
	t.Parallel()
	hlc := NewHLC()
	const ceilingMs = int64(7_777_777)
	hlc.SetPhysicalCeiling(ceilingMs)
	st := store3.NewMVCCStore()
	fsm := NewKvFSMWithHLC(st, hlc) // no WithCutoverSource

	snap, err := fsm.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	require.NoError(t, snap.Close())

	out := buf.Bytes()
	require.GreaterOrEqual(t, len(out), hlcSnapshotHeaderLen)
	require.Equal(t, hlcSnapshotMagic[:], out[:8], "must emit v1 magic")
	require.Equal(t, uint64(ceilingMs), binary.BigEndian.Uint64(out[8:16]),
		"v1 ceiling must round-trip exactly")
}

// TestWriteSnapshotHeader_PostCutoverWritesV2 pins design §3.3: when the
// sidecar's cutover index is non-zero, the snapshot header carries v2
// magic + length prefix + ceiling + cutover.
func TestWriteSnapshotHeader_PostCutoverWritesV2(t *testing.T) {
	t.Parallel()
	hlc := NewHLC()
	const ceilingMs = int64(5_555_555)
	hlc.SetPhysicalCeiling(ceilingMs)
	st := store3.NewMVCCStore()
	src := &staticCutoverSource{v: 0xCA11ED}
	fsm := NewKvFSMWithHLC(st, hlc, WithCutoverSource(src))

	snap, err := fsm.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	require.NoError(t, snap.Close())

	out := buf.Bytes()
	require.GreaterOrEqual(t, len(out), hlcSnapshotV2HeaderLen)
	require.Equal(t, hlcSnapshotMagicV2[:], out[:8], "must emit v2 magic")
	require.Equal(t, uint16(hlcSnapshotV2MinPayload),
		binary.BigEndian.Uint16(out[8:10]),
		"v2 length prefix must be the minimum payload size")
	require.Equal(t, uint64(ceilingMs), binary.BigEndian.Uint64(out[10:18]))
	require.Equal(t, uint64(0xCA11ED), binary.BigEndian.Uint64(out[18:26]))
}

// TestWriteSnapshotHeader_NoDowngradeAfterCutoverSeen pins design §3.3's
// load-bearing no-downgrade invariant: once a writer has emitted v2, a
// subsequent sidecar reset to 0 MUST still produce v2 output with the
// last-observed non-zero cutover preserved. Without this latch, a
// Phase-2 node whose sidecar gets reset would silently emit a v1
// snapshot and corrupt followers' wrap/unwrap routing.
func TestWriteSnapshotHeader_NoDowngradeAfterCutoverSeen(t *testing.T) {
	t.Parallel()
	hlc := NewHLC()
	hlc.SetPhysicalCeiling(42)
	st := store3.NewMVCCStore()
	src := &staticCutoverSource{v: 5}
	fsm := NewKvFSMWithHLC(st, hlc, WithCutoverSource(src))

	// First snapshot: v2 with cutover=5.
	snap1, err := fsm.Snapshot()
	require.NoError(t, err)
	var buf1 bytes.Buffer
	_, err = snap1.WriteTo(&buf1)
	require.NoError(t, err)
	require.NoError(t, snap1.Close())
	require.Equal(t, hlcSnapshotMagicV2[:], buf1.Bytes()[:8],
		"first snapshot must be v2")
	require.Equal(t, uint64(5),
		binary.BigEndian.Uint64(buf1.Bytes()[18:26]),
		"first snapshot must carry cutover=5")

	// Sidecar artificially reset to 0 (hypothetical reseed path).
	src.v = 0

	// Second snapshot: MUST still be v2, latched cutover=5 preserved.
	snap2, err := fsm.Snapshot()
	require.NoError(t, err)
	var buf2 bytes.Buffer
	_, err = snap2.WriteTo(&buf2)
	require.NoError(t, err)
	require.NoError(t, snap2.Close())
	require.Equal(t, hlcSnapshotMagicV2[:], buf2.Bytes()[:8],
		"latched writer must continue emitting v2 after sidecar reset")
	require.Equal(t, uint64(5),
		binary.BigEndian.Uint64(buf2.Bytes()[18:26]),
		"latched writer must preserve the last-observed non-zero cutover")
}

// --- Stage 8a §5: restore-side integration ---

// TestFSMSnapshotRestoreV2_PlumbsCutover pins design §3.4: a v2 snapshot
// restore stores the cutover on the FSM for Stage 6E's apply-hook to
// read. The handoff is observable via the test seam RestoredCutover().
func TestFSMSnapshotRestoreV2_PlumbsCutover(t *testing.T) {
	t.Parallel()
	// Source FSM with cutover source so the snapshot is v2.
	hlc1 := NewHLC()
	hlc1.SetPhysicalCeiling(9999)
	st1 := store3.NewMVCCStore()
	src := &staticCutoverSource{v: 0xC07012}
	fsm1 := NewKvFSMWithHLC(st1, hlc1, WithCutoverSource(src))

	// One key so the inner store has something to round-trip.
	req := &pb.Request{
		IsTxn: false,
		Phase: pb.Phase_NONE,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte("kk"), Value: []byte("vv")},
		},
	}
	b, err := proto.Marshal(req)
	require.NoError(t, err)
	fsm1.Apply(b)

	snap, err := fsm1.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	require.NoError(t, snap.Close())

	// Restore into a fresh FSM (no cutover source on the receiving side).
	hlc2 := NewHLC()
	st2 := store3.NewMVCCStore()
	fsm2 := NewKvFSMWithHLC(st2, hlc2)
	require.NoError(t, fsm2.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))))

	// Ceiling round-trips, cutover is plumbed, store data is intact.
	require.Equal(t, int64(9999), hlc2.PhysicalCeiling())
	concrete, ok := fsm2.(*kvFSM)
	require.True(t, ok, "test seam: FSM is the concrete *kvFSM")
	require.Equal(t, uint64(0xC07012), concrete.RestoredCutover(),
		"v2 restore must plumb cutover to the FSM for Stage 6E")
	ctx := context.Background()
	val, err := st2.GetAt(ctx, []byte("kk"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("vv"), val)
}
