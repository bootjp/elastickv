package backup

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"

	cockroachdberr "github.com/cockroachdb/errors"
)

// snapshot_file.go reads a snapshot in every form it takes on disk or in
// transit and hands the store payload to the reader for its format:
//
//	file a node persists, fsm-snap/<index>.fsm (internal/raftengine/etcd
//	writeFSMSnapshotPayload):
//	  [FSM snapshot header: EKVTHLC1 | EKVTHLC2]          kv/snapshot.go
//	  [store payload: EKVPBBL1 stream | EKVSSTI1 bundle]  store/
//	  [CRC32C footer: 4 bytes BE, Castagnoli, over everything before it]
//
//	encoder output, or a store payload taken from a running process:
//	  [store payload: EKVPBBL1 stream | EKVSSTI1 bundle]
//	  [optional CRC32C footer]  (a headerless payload persisted by an
//	                             older node carries one)
//
// The constants mirror kv/snapshot.go and
// internal/raftengine/etcd/fsm_snapshot_file.go; they are duplicated
// here, like the store constants in snapshot_reader.go, so this package
// links no live-cluster machinery. The CLI test that persists a real
// FSM snapshot through the engine's writer pins the mirror.

const (
	// fsmHeaderMagicPrefix is the common prefix of every FSM snapshot
	// header magic (kv/snapshot.go hlcSnapshotMagicPrefix).
	fsmHeaderMagicPrefix = "EKVTHLC"
	// FSMSnapshotHeaderV1 / FSMSnapshotHeaderV2 are the FSM snapshot
	// header magics kv/snapshot.go writes in front of the store payload.
	FSMSnapshotHeaderV1 = "EKVTHLC1"
	FSMSnapshotHeaderV2 = "EKVTHLC2"
	// fsmHeaderV1Len is the v1 header: magic(8) + ceiling ms(8).
	fsmHeaderV1Len = 16
	// fsmHeaderV2LenBytes is the v2 header's BE uint16 payload length,
	// after the magic; the payload holds ceiling ms(8) + cutover(8) and
	// optional trailing extension bytes, bounded like the live reader.
	fsmHeaderV2LenBytes   = 2
	fsmHeaderV2MinPayload = 16
	fsmHeaderV2MaxPayload = 1024
	// fsmFooterSize is the engine's CRC32C footer.
	fsmFooterSize = 4
	// snapshotMagicLen is the length of every magic above.
	snapshotMagicLen = 8
	// fsmFooterChunk sizes the footer reader's refill buffer.
	fsmFooterChunk = 64 << 10
)

// SSTIngestSnapshotMagic introduces the SST ingest bundle a live store
// emits when ELASTICKV_PEBBLE_SST_INGEST_SNAPSHOT is on
// (store/snapshot_pebble_sst.go pebbleSSTIngestSnapshotMagic).
const SSTIngestSnapshotMagic = "EKVSSTI1"

var fsmFooterTable = crc32.MakeTable(crc32.Castagnoli)

// ErrSnapshotFSMHeaderUnknown is returned for an EKVTHLC* magic whose
// version byte this decoder does not know. Like the live restore path, the
// decoder fails closed rather than guess at the header's length.
var ErrSnapshotFSMHeaderUnknown = cockroachdberr.New("backup: snapshot carries an FSM header version this decoder does not know")

// ErrSnapshotFSMHeaderInvalid is returned for an EKVTHLC2 header whose
// length prefix is outside the bounds the live reader accepts.
var ErrSnapshotFSMHeaderInvalid = cockroachdberr.New("backup: snapshot FSM header has an invalid length prefix")

// ErrSnapshotFooterMismatch is returned when a file framed by an FSM
// snapshot header does not end with a CRC32C footer matching the bytes
// before it: the file is truncated or corrupt.
var ErrSnapshotFooterMismatch = cockroachdberr.New("backup: snapshot CRC32C footer does not match its payload")

// ErrSnapshotTrailingBytes is returned when bytes follow a store payload
// whose format defines its own end (the SST ingest bundle).
var ErrSnapshotTrailingBytes = cockroachdberr.New("backup: snapshot has trailing bytes after its store payload")

// SnapshotFraming records how ReadSnapshotFile found the input framed.
type SnapshotFraming struct {
	// FSMHeader is FSMSnapshotHeaderV1 or FSMSnapshotHeaderV2 for a file a
	// node persisted, and empty for an input that starts at the store
	// payload.
	FSMHeader string
	// StoreFormat is PebbleSnapshotMagic or SSTIngestSnapshotMagic.
	StoreFormat string
	// FooterVerified reports that the input ended with a CRC32C footer
	// matching the bytes before it; FooterCRC32C is its value.
	FooterVerified bool
	FooterCRC32C   uint32
}

// ReadSnapshotFile reads a snapshot in any of the forms described at the
// top of this file and yields every physical entry through fn, in key
// order: every MVCC version of a key, newest first, tombstones included,
// with the store's reserved metadata keys skipped. Callers that want one
// record per key (the logical dump) filter the versions themselves; see
// DecodeSnapshot.
//
// fn receives transient byte slices, as with ReadSnapshot.
func ReadSnapshotFile(r io.Reader, fn func(SnapshotHeader, SnapshotEntry) error) (SnapshotHeader, SnapshotFraming, error) {
	var magic [snapshotMagicLen]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return SnapshotHeader{}, SnapshotFraming{}, cockroachdberr.WithStack(err)
	}
	var framing SnapshotFraming
	fsmHeader, err := classifySnapshotMagic(magic[:])
	if err != nil {
		return SnapshotHeader{}, framing, err
	}
	framing.FSMHeader = fsmHeader

	// A node always closes a file framed by an FSM header with the
	// footer; an input that starts at the store payload may carry one.
	footer := newFooterReader(io.MultiReader(bytes.NewReader(magic[:]), r), fsmHeader != "")
	br := bufio.NewReader(footer)
	if framing.FSMHeader != "" {
		if err := skipFSMSnapshotHeader(br, framing.FSMHeader); err != nil {
			return SnapshotHeader{}, framing, err
		}
	}
	header, storeFormat, err := readStorePayload(br, fn)
	framing.StoreFormat = storeFormat
	if err != nil {
		return header, framing, err
	}
	framing.FooterVerified = footer.verified
	framing.FooterCRC32C = footer.sum
	return header, framing, nil
}

// classifySnapshotMagic returns the FSM header magic when the input is a
// file a node persisted, "" when it starts at the store payload, and an
// error for anything else.
func classifySnapshotMagic(magic []byte) (string, error) {
	switch m := string(magic); {
	case m == FSMSnapshotHeaderV1 || m == FSMSnapshotHeaderV2:
		return m, nil
	case m == PebbleSnapshotMagic || m == SSTIngestSnapshotMagic:
		return "", nil
	case bytes.HasPrefix(magic, []byte(fsmHeaderMagicPrefix)):
		return "", cockroachdberr.Wrapf(ErrSnapshotFSMHeaderUnknown, "got %q", magic)
	default:
		return "", cockroachdberr.Wrapf(ErrSnapshotBadMagic,
			"got %q, want %s, %s, %s, or %s", magic,
			FSMSnapshotHeaderV1, FSMSnapshotHeaderV2, PebbleSnapshotMagic, SSTIngestSnapshotMagic)
	}
}

// readStorePayload dispatches on the store payload's magic.
func readStorePayload(br *bufio.Reader, fn func(SnapshotHeader, SnapshotEntry) error) (SnapshotHeader, string, error) {
	magic, err := br.Peek(snapshotMagicLen)
	if err != nil {
		return SnapshotHeader{}, "", cockroachdberr.WithStack(err)
	}
	switch string(magic) {
	case PebbleSnapshotMagic:
		header, err := readPebbleStream(br, fn)
		return header, PebbleSnapshotMagic, err
	case SSTIngestSnapshotMagic:
		header, err := readSSTIngestBundle(br, fn)
		if err != nil {
			return header, SSTIngestSnapshotMagic, err
		}
		return header, SSTIngestSnapshotMagic, expectEndOfPayload(br)
	default:
		return SnapshotHeader{}, "", cockroachdberr.Wrapf(ErrSnapshotBadMagic,
			"store payload magic %q, want %s or %s", magic, PebbleSnapshotMagic, SSTIngestSnapshotMagic)
	}
}

// expectEndOfPayload fails when bytes remain after a self-delimiting store
// payload. The footer reader has already withheld and checked the footer,
// so anything left is not part of the snapshot.
func expectEndOfPayload(br *bufio.Reader) error {
	if _, err := br.ReadByte(); err == nil {
		return cockroachdberr.WithStack(ErrSnapshotTrailingBytes)
	} else if !cockroachdberr.Is(err, io.EOF) {
		return cockroachdberr.WithStack(err)
	}
	return nil
}

// skipFSMSnapshotHeader consumes the FSM snapshot header. Its fields (the
// HLC ceiling and the Raft envelope cutover index) matter to a restoring
// node, not to the logical dump.
func skipFSMSnapshotHeader(br *bufio.Reader, magic string) error {
	if magic == FSMSnapshotHeaderV1 {
		if _, err := br.Discard(fsmHeaderV1Len); err != nil {
			return cockroachdberr.Wrap(truncatedIfEOF(err), "FSM header v1")
		}
		return nil
	}
	if _, err := br.Discard(snapshotMagicLen); err != nil {
		return cockroachdberr.Wrap(truncatedIfEOF(err), "FSM header v2 magic")
	}
	var lenBuf [fsmHeaderV2LenBytes]byte
	if _, err := io.ReadFull(br, lenBuf[:]); err != nil {
		return cockroachdberr.Wrap(truncatedIfEOF(err), "FSM header v2 length")
	}
	payloadLen := int(binary.BigEndian.Uint16(lenBuf[:]))
	if payloadLen < fsmHeaderV2MinPayload || payloadLen > fsmHeaderV2MaxPayload {
		return cockroachdberr.Wrapf(ErrSnapshotFSMHeaderInvalid, "payload length %d not in [%d, %d]",
			payloadLen, fsmHeaderV2MinPayload, fsmHeaderV2MaxPayload)
	}
	if _, err := br.Discard(payloadLen); err != nil {
		return cockroachdberr.Wrap(truncatedIfEOF(err), "FSM header v2 payload")
	}
	return nil
}

func truncatedIfEOF(err error) error {
	if cockroachdberr.Is(err, io.EOF) || cockroachdberr.Is(err, io.ErrUnexpectedEOF) {
		return cockroachdberr.WithStack(ErrSnapshotTruncated)
	}
	return cockroachdberr.WithStack(err)
}

// footerReader passes its source through while withholding the last
// fsmFooterSize bytes, so the consumer never sees the engine's CRC32C
// footer. At the source's end it compares the withheld bytes with the
// CRC32C of everything before them. When they match, the footer is
// verified and dropped. When they do not, a required footer (a file
// framed by an FSM header, which the engine always closes with one) is
// ErrSnapshotFooterMismatch, while an optional one (an input that starts
// at the store payload, such as encoder output) means there was no
// footer, and the withheld bytes are released as data. An optional
// footer is therefore misread only when the last four bytes of a
// footer-less payload happen to equal the CRC32C of the rest; the payload
// then reads as truncated, never as different data.
type footerReader struct {
	src      io.Reader
	required bool
	crc      hash.Hash32
	buf      []byte // bytes read from src and not yet returned
	srcDone  bool
	release  bool // the withheld bytes turned out to be data
	verified bool
	sum      uint32
	err      error
}

func newFooterReader(src io.Reader, required bool) *footerReader {
	return &footerReader{src: src, required: required, crc: crc32.New(fsmFooterTable)}
}

func (f *footerReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		if f.err != nil {
			return 0, f.err
		}
		withhold := fsmFooterSize
		if f.release {
			withhold = 0
		}
		if avail := len(f.buf) - withhold; avail > 0 {
			n := copy(p, f.buf[:min(avail, len(p))])
			if !f.release {
				_, _ = f.crc.Write(p[:n])
			}
			f.buf = f.buf[n:]
			return n, nil
		}
		if f.srcDone {
			f.finish()
			continue
		}
		f.fill()
	}
}

// fill appends the next chunk of the source to buf.
func (f *footerReader) fill() {
	if cap(f.buf)-len(f.buf) < fsmFooterChunk {
		grown := make([]byte, len(f.buf), len(f.buf)+fsmFooterChunk)
		copy(grown, f.buf)
		f.buf = grown
	}
	n, err := f.src.Read(f.buf[len(f.buf):cap(f.buf)])
	f.buf = f.buf[:len(f.buf)+n]
	switch {
	case cockroachdberr.Is(err, io.EOF):
		f.srcDone = true
	case err != nil:
		f.err = cockroachdberr.WithStack(err)
	}
}

// finish decides what the withheld bytes were once the source has ended.
func (f *footerReader) finish() {
	if f.release {
		f.err = io.EOF
		return
	}
	if len(f.buf) == fsmFooterSize && binary.BigEndian.Uint32(f.buf) == f.crc.Sum32() {
		f.verified = true
		f.sum = binary.BigEndian.Uint32(f.buf)
		f.buf = nil
		f.err = io.EOF
		return
	}
	if f.required {
		f.err = cockroachdberr.Wrapf(ErrSnapshotFooterMismatch, "computed %08x over the payload", f.crc.Sum32())
		return
	}
	f.release = true
}
