package backup

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"io"
	"testing"
	"testing/iotest"

	"github.com/cockroachdb/errors"
)

// withFooter appends the engine's CRC32C footer (BE, Castagnoli, over
// everything before it) to payload.
func withFooter(payload []byte) []byte {
	sum := crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli))
	return binary.BigEndian.AppendUint32(bytes.Clone(payload), sum)
}

// fsmHeaderV1 is kv/snapshot.go's v1 header: magic + BE ceiling ms.
func fsmHeaderV1(ceilingMs uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte(FSMSnapshotHeaderV1), ceilingMs)
}

// fsmHeaderV2 is kv/snapshot.go's v2 header with extLen bytes in its
// forward-compatible extension area.
func fsmHeaderV2(ceilingMs, cutover uint64, extLen uint16) []byte {
	hdr := binary.BigEndian.AppendUint16([]byte(FSMSnapshotHeaderV2), fsmHeaderV2MinPayload+extLen)
	hdr = binary.BigEndian.AppendUint64(hdr, ceilingMs)
	hdr = binary.BigEndian.AppendUint64(hdr, cutover)
	return append(hdr, bytes.Repeat([]byte{0xAB}, int(extLen))...)
}

// redisStringStream is a one-entry EKVPBBL1 stream for !redis|str|<key>.
func redisStringStream(t *testing.T, key, body string, commitTS uint64) []byte {
	t.Helper()
	var buf bytes.Buffer
	buf.WriteString(PebbleSnapshotMagic)
	mustBinaryWrite(t, &buf, commitTS)
	encKey := binary.BigEndian.AppendUint64([]byte(RedisStringPrefix+key), ^commitTS)
	mustBinaryWrite(t, &buf, uint64(len(encKey)))
	buf.Write(encKey)
	encVal := append(make([]byte, snapshotValueHeaderSize), liveRedisStringValue(body)...)
	mustBinaryWrite(t, &buf, uint64(len(encVal)))
	buf.Write(encVal)
	return buf.Bytes()
}

type readResult struct {
	keys    []string
	framing SnapshotFraming
	err     error
}

func readFile(input []byte, r func([]byte) io.Reader) readResult {
	var res readResult
	_, res.framing, res.err = ReadSnapshotFile(r(input), func(_ SnapshotHeader, e SnapshotEntry) error {
		res.keys = append(res.keys, string(e.UserKey))
		return nil
	})
	return res
}

// TestReadSnapshotFile_Framings covers every accepted framing, each read
// through a whole-buffer reader and a one-byte reader so the footer
// reader's withholding is exercised at every boundary.
func TestReadSnapshotFile_Framings(t *testing.T) {
	t.Parallel()
	stream := redisStringStream(t, "greeting", "hello", 7)
	cases := []struct {
		name       string
		input      []byte
		wantHeader string
		wantFooter bool
	}{
		{"raw stream", stream, "", false},
		{"raw stream with footer", withFooter(stream), "", true},
		{"v1 header and footer", withFooter(append(fsmHeaderV1(1234), stream...)), FSMSnapshotHeaderV1, true},
		{"v2 header with extension and footer", withFooter(append(fsmHeaderV2(1234, 99, 6), stream...)), FSMSnapshotHeaderV2, true},
	}
	readers := map[string]func([]byte) io.Reader{
		"whole":    func(b []byte) io.Reader { return bytes.NewReader(b) },
		"one-byte": func(b []byte) io.Reader { return iotest.OneByteReader(bytes.NewReader(b)) },
	}
	for _, tc := range cases {
		for rname, r := range readers {
			t.Run(tc.name+"/"+rname, func(t *testing.T) {
				t.Parallel()
				assertOneGreetingRead(t, tc.input, readFile(tc.input, r), tc.wantHeader, tc.wantFooter)
			})
		}
	}
}

func assertOneGreetingRead(t *testing.T, input []byte, res readResult, wantHeader string, wantFooter bool) {
	t.Helper()
	if res.err != nil {
		t.Fatalf("ReadSnapshotFile: %v", res.err)
	}
	if len(res.keys) != 1 || res.keys[0] != RedisStringPrefix+"greeting" {
		t.Fatalf("keys = %q", res.keys)
	}
	f := res.framing
	if f.FSMHeader != wantHeader || f.StoreFormat != PebbleSnapshotMagic || f.FooterVerified != wantFooter {
		t.Fatalf("framing = %+v, want header %q, store %s, footer %v", f, wantHeader, PebbleSnapshotMagic, wantFooter)
	}
	if wantFooter && f.FooterCRC32C != binary.BigEndian.Uint32(input[len(input)-fsmFooterSize:]) {
		t.Fatalf("FooterCRC32C = %08x, want the file's footer", f.FooterCRC32C)
	}
}

// TestReadSnapshotFile_Rejects covers the inputs the reader must refuse.
func TestReadSnapshotFile_Rejects(t *testing.T) {
	t.Parallel()
	stream := redisStringStream(t, "greeting", "hello", 7)
	framed := withFooter(append(fsmHeaderV1(1234), stream...))
	corrupt := bytes.Clone(framed)
	corrupt[len(corrupt)-1] ^= 0xFF
	payloadFlip := bytes.Clone(framed)
	payloadFlip[len(fsmHeaderV1(0))+len(stream)-1] ^= 0x01
	cases := []struct {
		name  string
		input []byte
		want  error
	}{
		{"framed file without footer", append(fsmHeaderV1(1234), stream...), ErrSnapshotFooterMismatch},
		{"footer bytes corrupted", corrupt, ErrSnapshotFooterMismatch},
		{"payload corrupted under a footer", payloadFlip, ErrSnapshotFooterMismatch},
		{"unknown FSM header version", append([]byte("EKVTHLC9"), stream...), ErrSnapshotFSMHeaderUnknown},
		{"v2 header length below minimum", withFooter(append(append([]byte(FSMSnapshotHeaderV2), 0, 8), stream...)), ErrSnapshotFSMHeaderInvalid},
		{"v2 header length above maximum", withFooter(append(append([]byte(FSMSnapshotHeaderV2), 0x04, 0x01), stream...)), ErrSnapshotFSMHeaderInvalid},
		{"unknown magic", []byte("NOTASNAP0000000000000000"), ErrSnapshotBadMagic},
		{"unknown store payload under a header", withFooter(append(fsmHeaderV1(1), []byte("NOTASNAP00000000")...)), ErrSnapshotBadMagic},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			res := readFile(tc.input, func(b []byte) io.Reader { return bytes.NewReader(b) })
			if !errors.Is(res.err, tc.want) {
				t.Fatalf("err = %v, want %v", res.err, tc.want)
			}
		})
	}
}

// TestReadSnapshotFile_SSTIngestBundleIntegrity corrupts a real SST ingest
// bundle in the places its own checks cover and adds a byte after it.
func TestReadSnapshotFile_SSTIngestBundleIntegrity(t *testing.T) {
	t.Setenv(sstIngestSnapshotEnv, "true")
	bundle := liveStoreSnapshotWithHistory(t)
	if !bytes.HasPrefix(bundle, []byte(SSTIngestSnapshotMagic)) {
		t.Fatalf("expected an %s bundle", SSTIngestSnapshotMagic)
	}
	// The manifest JSON follows magic(8), length(8), and its SHA-256; its
	// length is that of the one JSON value there.
	manifestStart := 16 + sstIngestDigestSize
	var manifestJSON json.RawMessage
	if err := json.NewDecoder(bytes.NewReader(bundle[manifestStart:])).Decode(&manifestJSON); err != nil {
		t.Fatalf("locate manifest: %v", err)
	}
	manifestLen := len(manifestJSON)
	firstFileByte := manifestStart + manifestLen

	flip := func(at int) []byte {
		out := bytes.Clone(bundle)
		out[at] ^= 0x01
		return out
	}
	cases := []struct {
		name  string
		input []byte
		want  error
	}{
		{"manifest byte flipped", flip(manifestStart + manifestLen/2), ErrSnapshotSSTInvalid},
		{"SST byte flipped", flip(firstFileByte + 1), ErrSnapshotSSTInvalid},
		{"trailing byte after the bundle", append(bytes.Clone(bundle), 0x00), ErrSnapshotTrailingBytes},
		{"framed bundle with a bad footer", append(append(fsmHeaderV1(1), bundle...), 0, 0, 0, 0), ErrSnapshotFooterMismatch},
	}
	for _, tc := range cases {
		res := readFile(tc.input, func(b []byte) io.Reader { return bytes.NewReader(b) })
		if !errors.Is(res.err, tc.want) {
			t.Fatalf("%s: err = %v, want %v", tc.name, res.err, tc.want)
		}
	}

	res := readFile(withFooter(append(fsmHeaderV1(1), bundle...)), func(b []byte) io.Reader { return bytes.NewReader(b) })
	if res.err != nil {
		t.Fatalf("framed bundle: %v", res.err)
	}
	if res.framing.StoreFormat != SSTIngestSnapshotMagic || !res.framing.FooterVerified {
		t.Fatalf("framed bundle framing = %+v", res.framing)
	}
}

// TestDecodeSnapshot_CountsSupersededVersions pins the counters for the
// live history: the overwritten version of "updated" and the version under
// the delete of "deleted" are superseded, the delete itself is the one
// tombstone, and the invariant on DecodeCounters holds.
func TestDecodeSnapshot_CountsSupersededVersions(t *testing.T) {
	t.Parallel()
	res, err := DecodeSnapshot(bytes.NewReader(liveStoreSnapshotWithHistory(t)), DecodeOptions{
		OutRoot:  t.TempDir(),
		Adapters: AdapterSet{Redis: true},
	})
	if err != nil {
		t.Fatalf("DecodeSnapshot: %v", err)
	}
	c := res.Counters
	if c.Superseded != 2 || c.Tombstone != 1 || c.Redis != 2 {
		t.Fatalf("counters = %+v, want superseded 2, tombstone 1, redis 2", c)
	}
	if sum := c.Superseded + c.Tombstone + c.DynamoDB + c.S3 + c.Redis + c.SQS + c.Internal + c.Unknown; sum != c.Total {
		t.Fatalf("Total %d != sum of classes %d (%+v)", c.Total, sum, c)
	}
}
