package backup

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"

	cockroachdberr "github.com/cockroachdb/errors"
)

// snapshot_reader.go consumes the native Pebble snapshot format
// produced by store/lsm_store.go::pebbleSnapshotMagic +
// restoreBatchLoopInto and yields each entry as a (userKey,
// userValue, tombstone, expireAt) tuple after stripping the MVCC
// encoding the live store layers on top of raw Pebble bytes.
//
// Snapshot file shape:
//
//	[8 bytes]   magic "EKVPBBL1"
//	[8 bytes]   lastCommitTS (LittleEndian uint64)
//	repeated:
//	  [8 bytes]   keyLen (LittleEndian uint64)
//	  [keyLen]    encoded key = <userKey><invTS(8 BE)>
//	  [8 bytes]   valLen (LittleEndian uint64)
//	  [valLen]    encoded value = <flags(1)><expireAt(8 LE)><body>
//	            (flags bit 0 = tombstone, bits 1-2 = encryption_state)
//
// Mirrors store/lsm_store.go:1670-1697 (readRestoreEntry) and
// :336-340 (fillEncodedKey) and :419-422 (fillEncodedValue). The
// constants are duplicated here so this package stays
// adapter/store-independent (the design requires the decoder to
// run as an offline tool against a `.fsm` file with no live cluster
// libraries linked).

// Snapshot format constants — mirror store/lsm_store.go.
const (
	// PebbleSnapshotMagicLen is the byte length of the "EKVPBBL1"
	// header. Exposed so callers can sniff the first 8 bytes of a
	// file to decide whether to dispatch into ReadSnapshot or fall
	// through to another reader.
	PebbleSnapshotMagicLen = 8

	// snapshotTSSize is the 8-byte inverted-TS suffix appended to
	// every encoded key (`store.fillEncodedKey`).
	snapshotTSSize = 8

	// snapshotValueHeaderSize is the 9-byte value-header prefix
	// (flags + expireAt) on every encoded value
	// (`store.fillEncodedValue`).
	snapshotValueHeaderSize = 9

	// snapshotTombstoneMask / snapshotEncStateMask / snapshotEncStateShift
	// mirror store.tombstoneMask / encStateMask / encStateShift. A
	// rename on the live side without an accompanying update here
	// would surface at the snapshot reader's table-driven tests.
	snapshotTombstoneMask    byte = 0b0000_0001
	snapshotEncStateMask     byte = 0b0000_0110
	snapshotEncStateShift         = 1
	snapshotEncStateReserved byte = 0b1111_1000 // bits 3-7 must be zero
	snapshotEncStateCleartx  byte = 0b00
	snapshotEncStateEncrypt  byte = 0b01

	// MaxSnapshotEncodedKeySize / MaxSnapshotEncodedValueSize bound the
	// per-entry allocations made by readExact / readExactGrow. Mirrors
	// the live store's `maxPebbleEncodedKeySize` (1 MiB user-key cap +
	// 8-byte TS suffix; store/mvcc_store.go:29 + store/lsm_store.go:51)
	// and `maxSnapshotValueSize + valueHeaderSize +
	// encryption.EnvelopeOverhead` (256 MiB cleartext + 9-byte header +
	// 34-byte envelope; store/mvcc_store.go:37 +
	// store/lsm_store.go:1692). Without these guards a corrupt or
	// adversarial snapshot whose length-prefix declares a huge size
	// would either OOM the decoder via `make([]byte, n)` or, on 32-bit
	// architectures, panic when narrowing `uint64` → `int` for the
	// slice length. Codex P1 + gemini security-high (PR #792 round 1).
	//
	// Duplicated here (rather than imported from `store`) so the
	// backup package keeps the adapter-independence required by the
	// design (it must run as a standalone offline tool with no live-
	// cluster libraries linked). The values are reviewed for staleness
	// alongside the live store's constants at every PR round.

	// maxSnapshotUserKeySize mirrors store/mvcc_store.go:29
	// `maxSnapshotKeySize` = 1 MiB.
	maxSnapshotUserKeySize = 1 << maxSnapshotUserKeyShift
	// maxSnapshotUserValueSize mirrors store/mvcc_store.go:37
	// `maxSnapshotValueSize` = 256 MiB.
	maxSnapshotUserValueSize    = maxSnapshotValueMiB << maxSnapshotValueShift
	maxSnapshotUserKeyShift     = 20 // 1 << 20 == 1 MiB
	maxSnapshotValueShift       = 20 // <Mib> << 20 == byte count
	maxSnapshotValueMiB         = 256
	MaxSnapshotEncodedKeySize   = maxSnapshotUserKeySize + snapshotTSSize
	MaxSnapshotEncodedValueSize = maxSnapshotUserValueSize + snapshotValueHeaderSize + envelopeMaxB

	// envelopeMaxB mirrors internal/encryption.EnvelopeOverhead (12-
	// byte nonce + 16-byte tag + 6-byte AAD-binding header = 34).
	// Duplicated here so we do not import the encryption package
	// from the backup-package's offline-tool boundary.
	envelopeMaxB = 34
)

// PebbleSnapshotMagic is the 8-byte file header that introduces a
// native Pebble snapshot. Exposed for callers that need to sniff a
// file before deciding which reader to dispatch to. Declared as an
// untyped string CONSTANT (not a `var [8]byte`) so an importer
// cannot mutate the bytes — a writable package variable would let
// any caller corrupt the header globally and break parsing for
// every consumer (coderabbit Major on PR #792 round 2).
// Callers comparing against the magic should treat the encoded-key
// type of their own data: most call sites convert to a byte slice
// via `[]byte(PebbleSnapshotMagic)` at the comparison point.
const PebbleSnapshotMagic = "EKVPBBL1"

// ErrSnapshotBadMagic is returned when the first 8 bytes of the
// reader do not match `EKVPBBL1`. The decoder caller should treat
// this as an immediate hard failure rather than try to skip past
// the bad header — a wrong magic almost always indicates the file
// is not actually a Pebble snapshot (an MVCC streaming snapshot,
// a tar archive, a partial truncate, etc.).
var ErrSnapshotBadMagic = cockroachdberr.New("backup: snapshot magic header does not match \"EKVPBBL1\"")

// ErrSnapshotTruncated is returned when the snapshot ends mid-entry
// (after a key length but before the key, or after a value length
// but before the value). A clean EOF at the start of the
// key-length field is a normal terminator and is NOT an error.
var ErrSnapshotTruncated = cockroachdberr.New("backup: snapshot truncated mid-entry")

// ErrSnapshotEncryptedReserved is returned when a value-header
// carries reserved encryption_state bits (0b10 or 0b11). Mirrors
// store.ErrEncryptedValueReservedState — the decoder fails closed
// rather than treat the body as cleartext, matching the design's
// §7.1 fail-closed contract.
var ErrSnapshotEncryptedReserved = cockroachdberr.New("backup: value header carries reserved encryption_state; decoder cannot interpret this entry")

// ErrSnapshotEncryptedEntry is returned when a value-header
// declares the entry is encrypted (encState=0b01). Phase 0a does
// NOT carry the decryption keyring; an encrypted snapshot must be
// decoded with a Phase 0a+keyring binary or after Stage 8 of the
// encryption rollout reverses the encryption.
var ErrSnapshotEncryptedEntry = cockroachdberr.New("backup: snapshot contains encrypted entries — Phase 0a does not link the decryption keyring")

// ErrSnapshotShortKey is returned when an entry's encoded key is
// shorter than the 8-byte timestamp suffix that
// `store.fillEncodedKey` always appends. Indicates a corrupt
// snapshot — the live store would never emit such a key.
var ErrSnapshotShortKey = cockroachdberr.New("backup: encoded key shorter than timestamp suffix")

// ErrSnapshotKeyTooLarge / ErrSnapshotValueTooLarge are returned
// when the on-disk length prefix declares an entry larger than the
// MaxSnapshotEncodedKeySize / MaxSnapshotEncodedValueSize budgets.
// Mirrors `store.ErrSnapshotKeyTooLarge` and `store.ErrValueTooLarge`
// from the live restore path so a corrupt or adversarial snapshot
// fails closed at the length-prefix layer instead of triggering an
// OOM-sized allocation (codex P1 + gemini security-high on PR #792).
var ErrSnapshotKeyTooLarge = cockroachdberr.New("backup: snapshot key length exceeds limit")
var ErrSnapshotValueTooLarge = cockroachdberr.New("backup: snapshot value length exceeds limit")

// ErrSnapshotShortValue is returned when an entry's encoded value
// is shorter than the 9-byte value header. Indicates a corrupt
// snapshot — the live store always writes the header even for
// tombstones.
var ErrSnapshotShortValue = cockroachdberr.New("backup: encoded value shorter than value-header")

// SnapshotEntry is one decoded entry emitted by ReadSnapshot's
// callback. Fields are the user-visible key / value bytes plus the
// MVCC metadata the decoder peeled off (commit timestamp, expiry,
// tombstone marker). Slices are owned by the snapshot reader's
// scratch buffer and may be overwritten when the callback returns —
// callers that need to retain bytes across iterations must
// `bytes.Clone` them.
type SnapshotEntry struct {
	UserKey   []byte
	UserValue []byte
	CommitTS  uint64
	ExpireAt  uint64
	Tombstone bool
}

// SnapshotHeader is the decoded preamble returned to the caller
// before iteration begins so the caller can record the snapshot's
// commit-time horizon in its MANIFEST.json (per design §380-422).
type SnapshotHeader struct {
	LastCommitTS uint64
}

// ReadSnapshot reads the EKVPBBL1 header from r, then yields every
// entry through fn. fn receives a transient SnapshotEntry whose
// byte slices are NOT safe to retain across calls (the reader
// reuses scratch buffers to keep per-entry allocations bounded for
// multi-GB snapshots). If fn returns an error, iteration stops and
// the error is returned verbatim.
//
// Iteration terminates cleanly on EOF at the start of an entry's
// key-length field. EOF inside an entry returns
// ErrSnapshotTruncated.
//
// Tombstone entries (flags bit 0 set) are surfaced via
// SnapshotEntry.Tombstone — callers decide whether to suppress
// them (Phase 0a's intended behavior for backup output) or include
// them (a multi-version diagnostic dump might want both).
//
// Callers that need the header even when the snapshot has zero
// entries (and so the callback never fires) should use
// ReadSnapshotWithHeader, which surfaces it as a separate return
// value. The callback's per-entry SnapshotHeader argument carries
// the same value but only fires when at least one entry exists.
func ReadSnapshot(r io.Reader, fn func(SnapshotHeader, SnapshotEntry) error) error {
	_, err := ReadSnapshotWithHeader(r, fn)
	return err
}

// ReadSnapshotWithHeader is the variant of ReadSnapshot that returns
// the decoded EKVPBBL1 header even when the snapshot contained zero
// entries (in which case fn is never called). Phase 0a's
// DecodeSnapshot needs this so MANIFEST.json's `last_commit_ts`
// field is populated for an empty-snapshot dump — the per-entry
// callback's header argument cannot fire on a header-only file.
func ReadSnapshotWithHeader(r io.Reader, fn func(SnapshotHeader, SnapshotEntry) error) (SnapshotHeader, error) {
	br := bufio.NewReader(r)
	header, err := readSnapshotHeader(br)
	if err != nil {
		return SnapshotHeader{}, err
	}
	var (
		keyBuf [1 << 16]byte
		valBuf []byte
	)
	for {
		stop, err := readOneEntry(br, header, keyBuf[:], &valBuf, fn)
		if err != nil {
			return header, err
		}
		if stop {
			return header, nil
		}
	}
}

// readOneEntry handles one (key, value) tuple plus the callback
// dispatch. Extracted from ReadSnapshot so the parent stays under
// the cyclop budget — the same shape every backup encoder uses
// (small fixed driver loop + extracted per-record helper).
// Returns (true, nil) on the natural inter-entry EOF terminator.
func readOneEntry(
	r *bufio.Reader,
	header SnapshotHeader,
	keyScratch []byte,
	valBuf *[]byte,
	fn func(SnapshotHeader, SnapshotEntry) error,
) (bool, error) {
	key, eof, err := readEntryKey(r, keyScratch)
	if err != nil {
		return false, err
	}
	if eof {
		// Clean inter-entry EOF — natural terminator.
		return true, nil
	}
	value, err := readEntryValue(r, valBuf)
	if err != nil {
		return false, err
	}
	entry, err := decodeSnapshotEntry(key, value)
	if err != nil {
		return false, err
	}
	if err := fn(header, entry); err != nil {
		return false, err
	}
	return false, nil
}

// readEntryKey reads the per-entry length-prefix-then-bytes for the
// key half of one entry. Returns (nil, true, nil) on the natural
// inter-entry EOF (clean stream terminator). Applies the
// MaxSnapshotEncodedKeySize bound BEFORE allocating the read
// buffer; without this guard a corrupt or adversarial snapshot
// whose length prefix declares a huge size would OOM the decoder
// or panic on 32-bit narrowing. Mirrors
// `store/lsm_store.go::readRestoreFieldLen`. Codex P1 + gemini
// security-high on PR #792.
func readEntryKey(r *bufio.Reader, scratch []byte) ([]byte, bool, error) {
	kLen, kEof, err := readEntryLen(r)
	if err != nil {
		return nil, false, err
	}
	if kEof {
		return nil, true, nil
	}
	if kLen > MaxSnapshotEncodedKeySize {
		return nil, false, cockroachdberr.Wrapf(ErrSnapshotKeyTooLarge,
			"length %d > %d", kLen, MaxSnapshotEncodedKeySize)
	}
	key, err := readExact(r, scratch[:0], kLen)
	if err != nil {
		return nil, false, cockroachdberr.WithStack(err)
	}
	return key, false, nil
}

// readEntryValue reads the per-entry length-prefix-then-bytes for
// the value half of one entry. A mid-entry EOF (length prefix
// missing after the key was already consumed) surfaces as
// ErrSnapshotTruncated rather than the natural inter-entry EOF —
// gemini high finding on PR #792 (the previous code ignored
// readEntryLen's eof return value here, allowing a truncated stream
// to flow into decodeSnapshotEntry with vLen=0 and surface as the
// wrong error). Applies MaxSnapshotEncodedValueSize before allocation
// (same rationale as readEntryKey).
func readEntryValue(r *bufio.Reader, valBuf *[]byte) ([]byte, error) {
	vLen, vEof, err := readEntryLen(r)
	if err != nil {
		return nil, err
	}
	if vEof {
		return nil, cockroachdberr.WithStack(ErrSnapshotTruncated)
	}
	if vLen > MaxSnapshotEncodedValueSize {
		return nil, cockroachdberr.Wrapf(ErrSnapshotValueTooLarge,
			"length %d > %d", vLen, MaxSnapshotEncodedValueSize)
	}
	*valBuf, err = readExactGrow(r, (*valBuf)[:0], vLen)
	if err != nil {
		return nil, cockroachdberr.WithStack(err)
	}
	return *valBuf, nil
}

// readSnapshotHeader consumes the 8-byte magic and the 8-byte LE
// lastCommitTS. Returns ErrSnapshotBadMagic on header mismatch.
func readSnapshotHeader(r io.Reader) (SnapshotHeader, error) {
	var magic [PebbleSnapshotMagicLen]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return SnapshotHeader{}, cockroachdberr.WithStack(err)
	}
	if !bytes.Equal(magic[:], []byte(PebbleSnapshotMagic)) {
		return SnapshotHeader{}, cockroachdberr.Wrapf(ErrSnapshotBadMagic,
			"got %q", magic[:])
	}
	var ts uint64
	if err := binary.Read(r, binary.LittleEndian, &ts); err != nil {
		return SnapshotHeader{}, cockroachdberr.WithStack(err)
	}
	return SnapshotHeader{LastCommitTS: ts}, nil
}

// readEntryLen reads an 8-byte LittleEndian length prefix. Returns
// (0, true, nil) on clean EOF — used to detect the natural end of
// the snapshot. Any other read error (including unexpected EOF) is
// returned verbatim.
func readEntryLen(r io.Reader) (uint64, bool, error) {
	var raw [8]byte
	n, err := io.ReadFull(r, raw[:])
	if err == nil {
		return binary.LittleEndian.Uint64(raw[:]), false, nil
	}
	if cockroachdberr.Is(err, io.EOF) && n == 0 {
		return 0, true, nil
	}
	if cockroachdberr.Is(err, io.ErrUnexpectedEOF) {
		return 0, false, cockroachdberr.WithStack(ErrSnapshotTruncated)
	}
	return 0, false, cockroachdberr.WithStack(err)
}

// readExact reads exactly n bytes into dst (extending it as
// needed). The returned slice aliases dst's underlying array — the
// caller must not retain it across loop iterations.
func readExact(r io.Reader, dst []byte, n uint64) ([]byte, error) {
	if uint64(cap(dst)) < n {
		// Cap fallback path: allocate a fresh slice when the
		// caller's scratch buffer isn't large enough. For the
		// stack-allocated keyBuf this only kicks in on
		// pathologically long keys.
		return readExactGrow(r, dst, n)
	}
	dst = dst[:n]
	if _, err := io.ReadFull(r, dst); err != nil {
		if cockroachdberr.Is(err, io.ErrUnexpectedEOF) || cockroachdberr.Is(err, io.EOF) {
			return nil, cockroachdberr.WithStack(ErrSnapshotTruncated)
		}
		return nil, cockroachdberr.WithStack(err)
	}
	return dst, nil
}

// readExactGrow is the heap-fallback variant of readExact. Used
// for value bodies, which can be up to several MiB and so live in
// a separately grown buffer rather than a fixed stack array.
func readExactGrow(r io.Reader, dst []byte, n uint64) ([]byte, error) {
	if uint64(cap(dst)) < n {
		dst = make([]byte, n)
	} else {
		dst = dst[:n]
	}
	if _, err := io.ReadFull(r, dst); err != nil {
		if cockroachdberr.Is(err, io.ErrUnexpectedEOF) || cockroachdberr.Is(err, io.EOF) {
			return nil, cockroachdberr.WithStack(ErrSnapshotTruncated)
		}
		return nil, cockroachdberr.WithStack(err)
	}
	return dst, nil
}

// decodeSnapshotEntry strips the 8-byte inverted-TS key suffix and
// the 9-byte value header, surfacing the user-visible byte slices
// plus the MVCC metadata. Returns ErrSnapshotShortKey /
// ErrSnapshotShortValue on length violations and
// ErrSnapshotEncryptedReserved / ErrSnapshotEncryptedEntry on bad
// or unsupported encryption_state bits.
func decodeSnapshotEntry(encKey, encVal []byte) (SnapshotEntry, error) {
	if len(encKey) < snapshotTSSize {
		return SnapshotEntry{}, cockroachdberr.Wrapf(ErrSnapshotShortKey,
			"encoded key length %d < %d", len(encKey), snapshotTSSize)
	}
	if len(encVal) < snapshotValueHeaderSize {
		return SnapshotEntry{}, cockroachdberr.Wrapf(ErrSnapshotShortValue,
			"encoded value length %d < %d", len(encVal), snapshotValueHeaderSize)
	}
	userKey := encKey[:len(encKey)-snapshotTSSize]
	invTS := binary.BigEndian.Uint64(encKey[len(encKey)-snapshotTSSize:])
	commitTS := ^invTS

	flags := encVal[0]
	if flags&snapshotEncStateReserved != 0 {
		return SnapshotEntry{}, cockroachdberr.Wrapf(ErrSnapshotEncryptedReserved,
			"value header byte %#08b", flags)
	}
	encState := (flags & snapshotEncStateMask) >> snapshotEncStateShift
	switch encState {
	case snapshotEncStateCleartx:
		// fall through
	case snapshotEncStateEncrypt:
		return SnapshotEntry{}, cockroachdberr.WithStack(ErrSnapshotEncryptedEntry)
	default:
		return SnapshotEntry{}, cockroachdberr.Wrapf(ErrSnapshotEncryptedReserved,
			"encryption_state=%#x is reserved", encState)
	}
	tombstone := (flags & snapshotTombstoneMask) != 0
	expireAt := binary.LittleEndian.Uint64(encVal[1:snapshotValueHeaderSize])
	userValue := encVal[snapshotValueHeaderSize:]
	return SnapshotEntry{
		UserKey:   userKey,
		UserValue: userValue,
		CommitTS:  commitTS,
		ExpireAt:  expireAt,
		Tombstone: tombstone,
	}, nil
}
