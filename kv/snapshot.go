package kv

import (
	"bufio"
	"encoding/binary"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// hlcSnapshotMagic is the v1 8-byte sentinel written at the start of every
// pre-Stage-8a FSM snapshot. Layout: magic(8) + ceilingMs(8) = 16 bytes.
var hlcSnapshotMagic = [8]byte{'E', 'K', 'V', 'T', 'H', 'L', 'C', '1'}

// hlcSnapshotHeaderLen is the total v1 header size: 8 magic + 8 ceiling ms.
const hlcSnapshotHeaderLen = 16 //nolint:mnd

// hlcSnapshotMagicV2 is the v2 sentinel introduced by Stage 8a to carry
// raft_envelope_cutover_index alongside the HLC ceiling. Layout:
// magic(8) + len(2,BE) + ceilingMs(8) + cutover(8) + optional trailing
// bytes for forward-compat extensions (skipped by current readers).
var hlcSnapshotMagicV2 = [8]byte{'E', 'K', 'V', 'T', 'H', 'L', 'C', '2'}

// hlcSnapshotV2MinPayload is the minimum v2 payload length (ceiling+cutover).
const hlcSnapshotV2MinPayload = 16 //nolint:mnd

// hlcSnapshotV2LenBytes is the size of the v2 length prefix.
const hlcSnapshotV2LenBytes = 2 //nolint:mnd

// maxSnapshotHeaderPayload bounds the v2 length prefix to prevent a malformed
// or malicious snapshot from triggering a large allocation per restore. The
// constant is intentionally small; raise only when a v2 extension genuinely
// needs the headroom.
const maxSnapshotHeaderPayload = 1024 //nolint:mnd

// hlcSnapshotMagicPrefix is the 7-byte common prefix of all EKVTHLC* magics;
// used by the read-path step-4 fail-closed check for unknown version bytes.
var hlcSnapshotMagicPrefix = []byte("EKVTHLC")

// ErrSnapshotHeaderInvalidLength indicates a v2 header whose length prefix
// is below the required minimum (cannot hold ceiling+cutover) or above the
// maxSnapshotHeaderPayload DoS bound.
var ErrSnapshotHeaderInvalidLength = errors.New("snapshot header: invalid v2 length prefix")

// ErrSnapshotHeaderUnknownMagic indicates an EKVTHLC* magic whose version
// byte the current binary does not recognise. The restore fail-closes; the
// operator must upgrade.
var ErrSnapshotHeaderUnknownMagic = errors.New("snapshot header: unknown EKVTHLC* magic")

// CutoverSource is the writer-side view of the Phase-2 envelope cutover.
// kvFSMSnapshot consults it once per snapshot to decide v1 vs v2 layout. A
// nil source means "always v1" (matches the Phase-0/Phase-1 posture and
// every pre-8a code path).
type CutoverSource interface {
	RaftEnvelopeCutoverIndex() uint64
}

// noDowngradeLatch implements the §3.3 no-downgrade invariant: once a writer
// observes a non-zero cutover, it MUST continue emitting v2 even if a later
// sidecar read returns 0. The latch also preserves the last observed
// non-zero cutover so a transient sidecar reset (hypothetical reseed) does
// not lose the value that subsequent Raft entries depend on for unwrap.
type noDowngradeLatch struct {
	lastSeen atomic.Uint64
	once     sync.Once
	log      *slog.Logger
}

// observe records the current sidecar cutover and returns the value the
// writer should emit. If the latch is engaged (lastSeen != 0) and the
// caller passes 0, the latch holds the previous non-zero value AND logs a
// single Error to surface the invariant violation without taking the
// process down mid-snapshot (design §3.3 closing paragraph).
func (l *noDowngradeLatch) observe(current uint64) uint64 {
	if current != 0 {
		l.lastSeen.Store(current)
		return current
	}
	seen := l.lastSeen.Load()
	if seen != 0 && l.log != nil {
		l.once.Do(func() {
			l.log.Error("snapshot: sidecar cutover transitioned non-zero -> 0; no-downgrade latch holding v2 with last-observed cutover",
				slog.Uint64("latched_cutover", seen))
		})
	}
	return seen
}

// latched reports whether the writer has ever observed a non-zero cutover.
// The v2 format is forced from then on.
func (l *noDowngradeLatch) latched() bool {
	return l.lastSeen.Load() != 0
}

var _ raftengine.Snapshot = (*kvFSMSnapshot)(nil)

type kvFSMSnapshot struct {
	snapshot  store.Snapshot
	ceilingMs int64
	// cutover is the value to emit at snapshot time. 0 + latch-not-engaged
	// produces v1 output (byte-for-byte pre-8a); any non-zero value, or
	// any cutover at all when the latch is engaged, produces v2.
	cutover uint64
	// useV2 records the v1/v2 selection made at construction time so a
	// concurrent sidecar mutation after Snapshot() returns cannot flip
	// the format mid-write.
	useV2 bool
	once  sync.Once
	err   error
}

func (f *kvFSMSnapshot) WriteTo(w io.Writer) (int64, error) {
	hdr, hdrLen := f.encodeHeader()
	n, err := w.Write(hdr[:hdrLen])
	if err != nil {
		return int64(n), errors.WithStack(err)
	}

	m, err := f.snapshot.WriteTo(w)
	total := int64(n) + m
	if err != nil {
		return total, errors.WithStack(err)
	}
	return total, nil
}

// encodeHeader serialises the v1 or v2 header into a stack-allocated buffer
// and returns the populated prefix length. v2 emits the minimum payload
// (ceiling+cutover) — forward-compat trailing bytes are a reader-side
// concern; the writer never emits unused trailing slots.
func (f *kvFSMSnapshot) encodeHeader() ([hlcSnapshotV2HeaderLen]byte, int) {
	var hdr [hlcSnapshotV2HeaderLen]byte
	if !f.useV2 {
		copy(hdr[:8], hlcSnapshotMagic[:])
		binary.BigEndian.PutUint64(hdr[8:16], uint64(f.ceilingMs)) //nolint:gosec // ceiling is a Unix ms timestamp, always positive
		return hdr, hlcSnapshotHeaderLen
	}
	copy(hdr[:8], hlcSnapshotMagicV2[:])
	binary.BigEndian.PutUint16(hdr[8:10], uint16(hlcSnapshotV2MinPayload))
	binary.BigEndian.PutUint64(hdr[10:18], uint64(f.ceilingMs)) //nolint:gosec // same as v1
	binary.BigEndian.PutUint64(hdr[18:26], f.cutover)
	return hdr, hlcSnapshotV2HeaderLen
}

// hlcSnapshotV2HeaderLen is the on-wire size of a minimum-payload v2 header:
// magic(8) + len(2) + ceiling(8) + cutover(8) = 26 bytes.
const hlcSnapshotV2HeaderLen = 8 + hlcSnapshotV2LenBytes + hlcSnapshotV2MinPayload

func (f *kvFSMSnapshot) Close() error {
	return f.closeSnapshot()
}

func (f *kvFSMSnapshot) closeSnapshot() error {
	if f == nil {
		return nil
	}
	f.once.Do(func() {
		if f.snapshot != nil {
			f.err = errors.WithStack(f.snapshot.Close())
			f.snapshot = nil
		}
	})
	return f.err
}

// ReadSnapshotHeader is the §3.2 read path. The caller wraps the input
// io.Reader in a *bufio.Reader and passes it here once, then MUST reuse the
// same *bufio.Reader for the inner-store restore — buffered bytes can sit
// in the bufio.Reader between calls; switching readers silently loses them.
//
// Return contract:
//   - v1 magic: (ceiling, 0, nil), magic + ceiling consumed.
//   - v2 magic: (ceiling, cutover, nil), full v2 header consumed; trailing
//     bytes above the parsed fields (forward-compat extension area) are
//     consumed and discarded.
//   - EKVTHLC* with an unknown version byte: (0, 0,
//     ErrSnapshotHeaderUnknownMagic) — fail-closed, restore aborts.
//   - v2 magic with a malformed length prefix: (0, 0,
//     ErrSnapshotHeaderInvalidLength) — fail-closed.
//   - Anything else (including streams shorter than 8 bytes): (0, 0, nil)
//     and ALL bytes left in the *bufio.Reader for the inner-store path.
func ReadSnapshotHeader(r *bufio.Reader) (ceiling, cutover uint64, err error) {
	// Step 0: peek up to 8 bytes. Short streams (< 8 bytes) fall through
	// to the headerless-legacy branch with the partial bytes left in
	// place — this preserves TestFSMSnapshotRestoreSmallLegacy and is
	// the gemini-medium / coderabbit-major fix from PR #877.
	peeked, perr := r.Peek(8) //nolint:mnd
	if len(peeked) < 8 {      //nolint:mnd
		// io.EOF / io.ErrUnexpectedEOF on a short stream: treat as
		// headerless legacy. Bytes that were peeked stay in r per
		// bufio.Reader semantics.
		_ = perr
		return 0, 0, nil
	}

	switch {
	case isV2Magic(peeked):
		return readV2(r)
	case isV1Magic(peeked):
		return readV1(r)
	case isUnknownEKVTHLC(peeked):
		return 0, 0, ErrSnapshotHeaderUnknownMagic
	default:
		// Headerless legacy: leave the peeked bytes in r so the
		// inner-store path sees the payload from byte 0.
		return 0, 0, nil
	}
}

func isV1Magic(p []byte) bool {
	return len(p) >= 8 && //nolint:mnd
		p[0] == hlcSnapshotMagic[0] && p[1] == hlcSnapshotMagic[1] &&
		p[2] == hlcSnapshotMagic[2] && p[3] == hlcSnapshotMagic[3] &&
		p[4] == hlcSnapshotMagic[4] && p[5] == hlcSnapshotMagic[5] &&
		p[6] == hlcSnapshotMagic[6] && p[7] == hlcSnapshotMagic[7]
}

func isV2Magic(p []byte) bool {
	return len(p) >= 8 && //nolint:mnd
		p[0] == hlcSnapshotMagicV2[0] && p[1] == hlcSnapshotMagicV2[1] &&
		p[2] == hlcSnapshotMagicV2[2] && p[3] == hlcSnapshotMagicV2[3] &&
		p[4] == hlcSnapshotMagicV2[4] && p[5] == hlcSnapshotMagicV2[5] &&
		p[6] == hlcSnapshotMagicV2[6] && p[7] == hlcSnapshotMagicV2[7]
}

// isUnknownEKVTHLC matches the 7-byte EKVTHLC prefix where the 8th byte
// is neither '1' nor '2'. This is the §3.2 step-4 fail-closed branch.
func isUnknownEKVTHLC(p []byte) bool {
	return len(p) >= 8 && //nolint:mnd
		p[0] == hlcSnapshotMagicPrefix[0] && p[1] == hlcSnapshotMagicPrefix[1] &&
		p[2] == hlcSnapshotMagicPrefix[2] && p[3] == hlcSnapshotMagicPrefix[3] &&
		p[4] == hlcSnapshotMagicPrefix[4] && p[5] == hlcSnapshotMagicPrefix[5] &&
		p[6] == hlcSnapshotMagicPrefix[6]
}

func readV1(r *bufio.Reader) (uint64, uint64, error) {
	var rest [16]byte //nolint:mnd
	if _, err := io.ReadFull(r, rest[:]); err != nil {
		return 0, 0, errors.Wrap(err, "v1 header: read magic+ceiling")
	}
	ceiling := binary.BigEndian.Uint64(rest[8:16])
	return ceiling, 0, nil
}

func readV2(r *bufio.Reader) (uint64, uint64, error) {
	// Consume the 8 magic bytes.
	if _, err := r.Discard(8); err != nil { //nolint:mnd
		return 0, 0, errors.Wrap(err, "v2 header: discard magic")
	}
	// Read the 2-byte length prefix.
	var lenBuf [hlcSnapshotV2LenBytes]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return 0, 0, errors.Wrap(err, "v2 header: read length prefix")
	}
	plen := int(binary.BigEndian.Uint16(lenBuf[:]))
	if plen < hlcSnapshotV2MinPayload || plen > maxSnapshotHeaderPayload {
		return 0, 0, ErrSnapshotHeaderInvalidLength
	}
	// Read exactly plen payload bytes. Parse ceiling/cutover from the
	// first 16; ignore the remainder (forward-compat extension area).
	payload := make([]byte, plen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, 0, errors.Wrap(err, "v2 header: read payload")
	}
	ceiling := binary.BigEndian.Uint64(payload[0:8])
	cutover := binary.BigEndian.Uint64(payload[8:16])
	return ceiling, cutover, nil
}
