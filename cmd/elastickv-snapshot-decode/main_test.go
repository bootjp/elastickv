package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/backup"
	"github.com/cockroachdb/errors"
)

// snapshot constants — duplicated from internal/backup so the test
// can synthesise a wire-format snapshot without exporting the
// package-private encoding helpers. Changes to the live format
// surface here as a test failure, which is the early signal we
// want from the CLI's golden-path test.
const (
	pebbleSnapshotMagic     = "EKVPBBL1"
	snapshotTSSize          = 8
	snapshotValueHeaderSize = 9
)

// TestRun_DecodesSimpleRedisStringSnapshot drives the CLI from
// argv-in to dump-tree-out: a synthetic 1-entry .fsm round-trips
// through run() and produces the expected MANIFEST.json,
// CHECKSUMS, and redis/db_0/strings/<key>.bin.
func TestRun_DecodesSimpleRedisStringSnapshot(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	in := filepath.Join(dir, "0000000000000064.fsm")
	out := filepath.Join(dir, "out")
	mustWriteFSM(t, in, redisStringSnapshot(t, "greeting", "hello"))

	var stderr bytes.Buffer
	if err := run([]string{
		"-input", in,
		"-output", out,
		"-cluster-id", "ek-test",
	}, &stderr); err != nil {
		t.Fatalf("run: %v\nstderr=%s", err, stderr.String())
	}
	assertBlobMatches(t, filepath.Join(out, "redis", "db_0", "strings", "greeting.bin"), "hello")
	assertManifest(t, filepath.Join(out, "MANIFEST.json"), manifestShape{
		Phase:         "phase0-snapshot-decode",
		SnapshotIndex: 64,
		ClusterID:     "ek-test",
	})
	if _, err := os.Stat(filepath.Join(out, backup.CHECKSUMSFilename)); err != nil {
		t.Fatalf("CHECKSUMS missing: %v", err)
	}
	if err := backup.VerifyChecksums(out); err != nil {
		t.Fatalf("VerifyChecksums: %v", err)
	}
}

// assertBlobMatches reads path and asserts its bytes equal want.
// Extracted from the parent test so the parent stays under the
// project's cyclop=10 ceiling.
func assertBlobMatches(t *testing.T, path, want string) {
	t.Helper()
	body, err := os.ReadFile(path) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if string(body) != want {
		t.Fatalf("%s = %q, want %q", path, body, want)
	}
}

// assertManifest parses MANIFEST.json at path and asserts the
// fields the test cares about. Extracted from the parent test for
// the same cyclop reason.
func assertManifest(t *testing.T, path string, want manifestShape) {
	t.Helper()
	body, err := os.ReadFile(path) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var got manifestShape
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal %s: %v\nbody=%s", path, err, body)
	}
	if got.Phase != want.Phase {
		t.Fatalf("phase = %q, want %q", got.Phase, want.Phase)
	}
	if got.SnapshotIndex != want.SnapshotIndex {
		t.Fatalf("snapshot_index = %d, want %d", got.SnapshotIndex, want.SnapshotIndex)
	}
	if got.ClusterID != want.ClusterID {
		t.Fatalf("cluster_id = %q, want %q", got.ClusterID, want.ClusterID)
	}
}

// TestRun_RejectsMissingInput pins the flag-validation surface so a
// missing --input prints a useful error rather than panicking.
func TestRun_RejectsMissingInput(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	err := run([]string{"-output", filepath.Join(dir, "out")}, io.Discard)
	if err == nil {
		t.Fatalf("expected error for missing --input")
	}
}

// TestRun_RejectsUnknownAdapter pins that a typo in --adapter
// surfaces as a hard error instead of silently disabling the
// adapter (Phase 0a is fail-noisy on operator misuse).
func TestRun_RejectsUnknownAdapter(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	in := filepath.Join(dir, "1.fsm")
	out := filepath.Join(dir, "out")
	mustWriteFSM(t, in, emptyFSM(t))
	err := run([]string{
		"-input", in,
		"-output", out,
		"-adapter", "redis,bogus",
	}, io.Discard)
	if err == nil {
		t.Fatalf("expected error for unknown adapter")
	}
}

// TestRun_RejectsZeroAdapters pins that --adapter "" is a hard
// error — silently producing an empty dump is worse than telling
// the operator their flag set selected nothing.
func TestRun_RejectsZeroAdapters(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	in := filepath.Join(dir, "1.fsm")
	out := filepath.Join(dir, "out")
	mustWriteFSM(t, in, emptyFSM(t))
	err := run([]string{
		"-input", in,
		"-output", out,
		"-adapter", " ,,",
	}, io.Discard)
	if err == nil {
		t.Fatalf("expected error for zero-adapter selection")
	}
}

// TestRun_UnparseableFilenameLeavesIndexZero pins the soft-failure
// path documented in source.go: an operator-named .fsm without the
// `<uint64>.fsm` convention still decodes; MANIFEST.snapshot_index
// is omitted (zero value, omitempty).
func TestRun_UnparseableFilenameLeavesIndexZero(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	in := filepath.Join(dir, "from-prod.fsm")
	out := filepath.Join(dir, "out")
	mustWriteFSM(t, in, emptyFSM(t))
	if err := run([]string{"-input", in, "-output", out}, io.Discard); err != nil {
		t.Fatalf("run: %v", err)
	}
	manifestBytes, err := os.ReadFile(filepath.Join(out, "MANIFEST.json"))
	if err != nil {
		t.Fatalf("read MANIFEST: %v", err)
	}
	var m manifestShape
	if err := json.Unmarshal(manifestBytes, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m.SnapshotIndex != 0 {
		t.Fatalf("snapshot_index = %d, want 0 for unparseable filename", m.SnapshotIndex)
	}
}

// manifestShape is the minimal subset of MANIFEST.json the CLI's
// golden tests check. Avoiding a hard import of backup.Manifest
// keeps the test surface tight to what the CLI actually stamps.
type manifestShape struct {
	Phase         string `json:"phase"`
	SnapshotIndex uint64 `json:"snapshot_index,omitempty"`
	ClusterID     string `json:"cluster_id,omitempty"`
}

// mustWriteFSM writes body to path, t.Fatal-ing on any error. The
// returned snapshot is plain bytes; the parent test wrote it via
// the synthesise-snapshot helpers below.
func mustWriteFSM(t *testing.T, path string, body []byte) {
	t.Helper()
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// emptyFSM returns a header-only snapshot — magic + zero
// last_commit_ts. Used to exercise the CLI's flag-validation
// surface without spending bytes on entry encoding.
func emptyFSM(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	buf.WriteString(pebbleSnapshotMagic)
	if err := binary.Write(&buf, binary.LittleEndian, uint64(0)); err != nil {
		t.Fatalf("write ts: %v", err)
	}
	return buf.Bytes()
}

// redisStringSnapshot returns a one-entry snapshot whose key is
// `!redis|str|<userKey>` and whose value is the magic-prefixed
// no-TTL string-value envelope the live Redis adapter writes (see
// adapter/redis_storage_codec.go).
func redisStringSnapshot(t *testing.T, userKey, body string) []byte {
	t.Helper()
	var buf bytes.Buffer
	buf.WriteString(pebbleSnapshotMagic)
	if err := binary.Write(&buf, binary.LittleEndian, uint64(1)); err != nil {
		t.Fatalf("write ts: %v", err)
	}
	encKey := append([]byte(backup.RedisStringPrefix+userKey), make([]byte, snapshotTSSize)...) //nolint:gocritic // explicit append for clarity
	// invTS = ^1
	binary.BigEndian.PutUint64(encKey[len(encKey)-snapshotTSSize:], ^uint64(1))
	// value-header: flags=0 (no tombstone, no encryption), expireAt=0.
	encVal := make([]byte, snapshotValueHeaderSize+3+len(body))
	encVal[0] = 0
	// 0xFF 0x01 is the live magic prefix the Redis string-value
	// codec uses to mark a non-legacy value (internal/backup/
	// redis_string.go: redisStrMagic / redisStrVersion). The third
	// header byte is flags (bit 0 = redisStrHasTTL); 0 means
	// no-TTL and the body follows immediately.
	encVal[snapshotValueHeaderSize] = 0xFF
	encVal[snapshotValueHeaderSize+1] = 0x01
	encVal[snapshotValueHeaderSize+2] = 0x00
	copy(encVal[snapshotValueHeaderSize+3:], body)
	if err := binary.Write(&buf, binary.LittleEndian, uint64(len(encKey))); err != nil {
		t.Fatalf("write keylen: %v", err)
	}
	buf.Write(encKey)
	if err := binary.Write(&buf, binary.LittleEndian, uint64(len(encVal))); err != nil {
		t.Fatalf("write vallen: %v", err)
	}
	buf.Write(encVal)
	return buf.Bytes()
}

// TestParseAdapterSet pins the CSV-to-AdapterSet conversion's
// error reporting. The function is exported via run() only, but
// the routing matters enough that a direct test pins the
// per-name match table.
func TestParseAdapterSet(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		in      string
		want    backup.AdapterSet
		wantErr bool
	}{
		{"empty defaults to all", "", backup.AllAdapters(), false},
		{"all keyword", "all", backup.AllAdapters(), false},
		{"single adapter", "redis", backup.AdapterSet{Redis: true}, false},
		{"two adapters", "redis,sqs", backup.AdapterSet{Redis: true, SQS: true}, false},
		{"trailing comma", "redis,", backup.AdapterSet{Redis: true}, false},
		{"unknown adapter", "bogus", backup.AdapterSet{}, true},
		{"empty list", " ,,", backup.AdapterSet{}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseAdapterSet(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if got != tc.want {
				t.Fatalf("set = %+v, want %+v", got, tc.want)
			}
		})
	}
}

var _ = errors.New // keep the import even if assertions don't reference it directly
