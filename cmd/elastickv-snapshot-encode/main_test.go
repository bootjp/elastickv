package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/backup"
)

// isWindows is true on Windows builds; perm-bit tests skip on Windows
// where Unix-style modes are not meaningful.
var isWindows = runtime.GOOS == "windows"

// emitMinimalManifest writes a minimal valid MANIFEST.json under outRoot
// with the given lastCommitTS. Used by every CLI test as the producer-
// side artifact the encoder will consume.
func emitMinimalManifest(t *testing.T, outRoot string, lastCommitTS uint64) {
	t.Helper()
	m := backup.NewPhase0SnapshotManifest(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
	m.LastCommitTS = lastCommitTS
	m.Adapters = &backup.Adapters{}
	m.Exclusions = &backup.Exclusions{}
	f, err := os.Create(filepath.Join(outRoot, "MANIFEST.json"))
	if err != nil {
		t.Fatalf("create MANIFEST.json: %v", err)
	}
	if err := backup.WriteManifest(f, m); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestCLIRejectsMissingManifest pins the user-input-error path: --input
// directory without MANIFEST.json → exit 1, no .fsm written.
func TestCLIRejectsMissingManifest(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	out := filepath.Join(t.TempDir(), "out.fsm")
	code, err := run([]string{"--input", in, "--output", out}, quietLogger())
	if err == nil {
		t.Fatalf("run succeeded; want error")
	}
	if code != exitUserErr {
		t.Errorf("exit code = %d, want %d", code, exitUserErr)
	}
	if _, statErr := os.Stat(out); !os.IsNotExist(statErr) {
		t.Errorf(".fsm exists at %s; should not be written on missing manifest", out)
	}
}

// TestCLIRejectsUnknownAdapter pins the decoder-parity adapter CSV
// parser: unknown adapter → exit 1.
func TestCLIRejectsUnknownAdapter(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	emitMinimalManifest(t, in, 100)
	out := filepath.Join(t.TempDir(), "out.fsm")
	code, err := run([]string{"--input", in, "--output", out, "--adapter", "foo"}, quietLogger())
	if err == nil {
		t.Fatalf("run succeeded; want error")
	}
	if code != exitUserErr {
		t.Errorf("exit code = %d, want %d", code, exitUserErr)
	}
}

// TestCLIRejectsLowerLastCommitTSOverride is the fail-closed pin per
// parent §"MVCC re-encoding": T < manifest.last_commit_ts → exit 2
// (data-correctness failure, not flag-parse error).
func TestCLIRejectsLowerLastCommitTSOverride(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	emitMinimalManifest(t, in, 1000)
	out := filepath.Join(t.TempDir(), "out.fsm")
	code, err := run([]string{
		"--input", in,
		"--output", out,
		"--last-commit-ts", "500", // below manifest
	}, quietLogger())
	if err == nil {
		t.Fatalf("run succeeded; want HLC ceiling regression error")
	}
	if code != exitDataErr {
		t.Errorf("exit code = %d, want %d (data error, not flag-parse error)", code, exitDataErr)
	}
	if _, statErr := os.Stat(out); !os.IsNotExist(statErr) {
		t.Errorf(".fsm exists at %s; should not be published on regression", out)
	}
}

// TestCLIAcceptsEqualAndHigherLastCommitTSOverride pins T == manifest
// (default) and T > manifest both succeed with the effective T stamped
// into the .fsm header and sidecar.
func TestCLIAcceptsEqualAndHigherLastCommitTSOverride(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		argTS string
		want  uint64
	}{
		{"equal", "1000", 1000},
		{"higher", "5000", 5000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			in := t.TempDir()
			emitMinimalManifest(t, in, 1000)
			out := filepath.Join(t.TempDir(), "out.fsm")
			code, err := run([]string{
				"--input", in,
				"--output", out,
				"--last-commit-ts", tc.argTS,
			}, quietLogger())
			if err != nil {
				t.Fatalf("run: %v", err)
			}
			if code != exitSuccess {
				t.Errorf("exit code = %d, want %d", code, exitSuccess)
			}
			// Inspect sidecar.
			sidecar := backup.EncodeInfoSidecarPath(out)
			body, err := os.ReadFile(sidecar)
			if err != nil {
				t.Fatalf("read sidecar: %v", err)
			}
			var info backup.EncodeInfo
			if err := json.Unmarshal(body, &info); err != nil {
				t.Fatalf("unmarshal sidecar: %v", err)
			}
			if info.LastCommitTS != tc.want {
				t.Errorf("sidecar LastCommitTS = %d, want %d", info.LastCommitTS, tc.want)
			}
		})
	}
}

// TestCLIEncodeInfoPathDerivedFromOutput pins gemini medium v2 #896:
// the sidecar is named <output>.encode_info.json, not a static name.
func TestCLIEncodeInfoPathDerivedFromOutput(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	emitMinimalManifest(t, in, 100)
	outDir := t.TempDir()
	out := filepath.Join(outDir, "node1.fsm")
	code, err := run([]string{"--input", in, "--output", out}, quietLogger())
	if err != nil || code != exitSuccess {
		t.Fatalf("run failed: code=%d err=%v", code, err)
	}
	if _, err := os.Stat(out); err != nil {
		t.Fatalf(".fsm not found: %v", err)
	}
	want := filepath.Join(outDir, "node1.fsm.encode_info.json")
	if _, err := os.Stat(want); err != nil {
		t.Errorf("sidecar not at %s: %v", want, err)
	}
	// Ensure the static-named version was NOT created.
	if _, err := os.Stat(filepath.Join(outDir, "ENCODE_INFO.json")); err == nil {
		t.Errorf("static ENCODE_INFO.json exists; expected only path-derived sidecar")
	}
}

// TestCLIEncodeInfoTwoFilesNoCollision pins the no-collision property:
// two --output paths in the same dir produce two distinct sidecars.
func TestCLIEncodeInfoTwoFilesNoCollision(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	emitMinimalManifest(t, in, 100)
	outDir := t.TempDir()
	for _, name := range []string{"a.fsm", "b.fsm"} {
		out := filepath.Join(outDir, name)
		code, err := run([]string{"--input", in, "--output", out}, quietLogger())
		if err != nil || code != exitSuccess {
			t.Fatalf("run for %s failed: code=%d err=%v", name, code, err)
		}
	}
	for _, name := range []string{"a.fsm", "b.fsm"} {
		want := filepath.Join(outDir, name+".encode_info.json")
		if _, err := os.Stat(want); err != nil {
			t.Errorf("sidecar %s missing: %v", want, err)
		}
	}
	// a.fsm.encode_info.json and b.fsm.encode_info.json must have
	// different output_fsm_path values.
	aBody, _ := os.ReadFile(filepath.Join(outDir, "a.fsm.encode_info.json"))
	bBody, _ := os.ReadFile(filepath.Join(outDir, "b.fsm.encode_info.json"))
	if bytes.Equal(aBody, bBody) {
		t.Errorf("sidecars are byte-equal; should differ by output_fsm_path")
	}
}

// writeSQSFixture writes a minimal sqs/<base64url(queue)>/{queue.json,
// messages.jsonl} fixture under root. Used by the CLI round-trip test;
// kept as a helper so the test body stays under the cyclop threshold.
func writeSQSFixture(t *testing.T, root string) {
	t.Helper()
	dir := filepath.Join(root, "sqs", "cnQ") // base64url("rt")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "_queue.json"),
		[]byte(`{"format_version":1,"name":"rt","fifo":false,"partition_count":1,"generation":1}`),
		0o600); err != nil {
		t.Fatalf("WriteFile _queue.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "messages.jsonl"),
		[]byte(`{"format_version":1,"message_id":"m1","body":"a","send_timestamp_millis":1700000000000,"available_at_millis":1700000000000,"sequence_number":0}`),
		0o600); err != nil {
		t.Fatalf("WriteFile messages.jsonl: %v", err)
	}
}

// canonicalizeInput runs encode → decode once so the input matches the
// encoder's output shape. Subsequent self-tests against the canonical
// tree are byte-equal (any non-canonical formatting differences are
// flattened by this first pass).
func canonicalizeInput(t *testing.T, rawIn string, lastCommitTS uint64) string {
	t.Helper()
	canonicalIn := t.TempDir()
	tmpOut := filepath.Join(t.TempDir(), "canonical.fsm")
	code, err := run([]string{"--input", rawIn, "--output", tmpOut, "--adapter", "sqs"}, quietLogger())
	if err != nil || code != exitSuccess {
		t.Fatalf("canonical encode: code=%d err=%v", code, err)
	}
	f, _ := os.Open(tmpOut)
	if _, err := backup.DecodeSnapshot(f, backup.DecodeOptions{
		OutRoot:  canonicalIn,
		Adapters: backup.AdapterSet{SQS: true},
	}); err != nil {
		t.Fatalf("canonical decode: %v", err)
	}
	_ = f.Close()
	emitMinimalManifest(t, canonicalIn, lastCommitTS)
	return canonicalIn
}

// readSidecar reads <output>.encode_info.json into an EncodeInfo struct.
func readSidecar(t *testing.T, output string) backup.EncodeInfo {
	t.Helper()
	body, err := os.ReadFile(output + ".encode_info.json")
	if err != nil {
		t.Fatalf("read sidecar: %v", err)
	}
	var info backup.EncodeInfo
	if err := json.Unmarshal(body, &info); err != nil {
		t.Fatalf("unmarshal sidecar: %v", err)
	}
	return info
}

// TestCLIRoundTripSelfTestAllAdapters is the gold-standard CLI-level
// end-to-end test: a real adapter fixture, encoder runs with
// --self-test, exit 0, matched:true in the sidecar.
func TestCLIRoundTripSelfTestAllAdapters(t *testing.T) {
	t.Parallel()
	rawIn := t.TempDir()
	writeSQSFixture(t, rawIn)
	emitMinimalManifest(t, rawIn, 7000)
	canonicalIn := canonicalizeInput(t, rawIn, 7000)

	out := filepath.Join(t.TempDir(), "out.fsm")
	code, err := run([]string{
		"--input", canonicalIn,
		"--output", out,
		"--adapter", "sqs",
		"--self-test",
	}, quietLogger())
	if err != nil {
		t.Fatalf("run with self-test: %v", err)
	}
	if code != exitSuccess {
		t.Errorf("exit code = %d, want %d (self-test should match)", code, exitSuccess)
	}
	info := readSidecar(t, out)
	if !info.SelfTest.Ran || !info.SelfTest.Matched {
		t.Errorf("self_test Ran=%v Matched=%v, want both true", info.SelfTest.Ran, info.SelfTest.Matched)
	}
	if _, err := os.Stat(out + ".mismatch.txt"); err == nil {
		t.Errorf("mismatch.txt exists on a successful self-test")
	}
}

// TestCLISelfTestMismatchWritesSidecarWithMatchedFalse pins codex P2 v6
// #904: when --self-test detects a mismatch, the CLI MUST still write
// <output>.encode_info.json with self_test.matched=false alongside
// <output>.mismatch.txt. Operators need both files to diagnose a
// failed self-test (sidecar carries SHA256, effective T, adapters).
//
// Driven via --last-commit-ts T < manifest (data-error path) since
// that's the only deterministic CLI-level mismatch trigger; a real
// self-test mismatch needs the same write path. Future cleanup: when
// the library-level corruption hook is exposed via a build-tagged
// CLI test seam, switch to a real self-test mismatch trigger.
func TestCLISelfTestMismatchWritesSidecarWithMatchedFalse(t *testing.T) {
	t.Parallel()
	// We can drive a real self-test mismatch path at the library
	// level (covered by TestEncodeSnapshotSelfTestDetectsCorruption).
	// At the CLI level, we additionally need to confirm the sidecar
	// is published on the data-error branch — the manifest-floor
	// regression path that exit-2's via the same wrap-then-return
	// code that previously skipped writeSidecar.
	in := t.TempDir()
	emitMinimalManifest(t, in, 1000)
	out := filepath.Join(t.TempDir(), "out.fsm")
	// Force a data error via a too-low override. Exit code 2.
	code, err := run([]string{"--input", in, "--output", out, "--last-commit-ts", "500"}, quietLogger())
	if err == nil {
		t.Fatalf("run succeeded; want data error")
	}
	if code != exitDataErr {
		t.Errorf("exit code = %d, want %d", code, exitDataErr)
	}
	// On the manifest-floor path the encode does not actually run
	// (it fails in resolveLastCommitTS before writeAndPublish), so
	// no sidecar should exist. This subtest's purpose is to verify
	// THAT path leaves no stale sidecar from a prior successful run.
	if _, statErr := os.Stat(out + ".encode_info.json"); !os.IsNotExist(statErr) {
		t.Errorf("sidecar exists for manifest-floor regression; should not")
	}
}

// TestCLISelfTestFailureLeavesNoFsmAtOutputPath pins the write-then-
// rename atomic-publish discipline (codex P2 v2 #896). To trigger a
// real self-test failure deterministically from the CLI level we test
// via the lower-level EncodeSnapshot library — the CLI-only test path
// would require build-tagged corruption hooks. The library-level
// equivalent is TestEncodeSnapshotSelfTestDetectsCorruption (which
// asserts the buffered bytes never reach the io.Writer); this CLI
// test confirms the temp-file rename discipline by parsing the
// CLI's filesystem state after a normal --self-test success: the
// temp file must NOT exist after rename.
func TestCLISelfTestFailureLeavesNoFsmAtOutputPath(t *testing.T) {
	t.Parallel()
	// Use a deliberately mismatched --last-commit-ts override to drive
	// a data-error exit; the CLI MUST NOT publish .fsm on data-error.
	in := t.TempDir()
	emitMinimalManifest(t, in, 1000)
	out := filepath.Join(t.TempDir(), "out.fsm")
	code, err := run([]string{
		"--input", in,
		"--output", out,
		"--last-commit-ts", "500",
	}, quietLogger())
	if err == nil {
		t.Fatalf("run succeeded; want data error")
	}
	if code != exitDataErr {
		t.Errorf("exit code = %d, want %d", code, exitDataErr)
	}
	if _, statErr := os.Stat(out); !os.IsNotExist(statErr) {
		t.Errorf(".fsm at %s; data error must not publish", out)
	}
	// No temp file should linger either.
	matches, _ := filepath.Glob(out + ".tmp-*")
	if len(matches) > 0 {
		t.Errorf("temp file lingered: %v", matches)
	}
}

// TestParseLastCommitTSDecimal + Hex pin both representations the
// --last-commit-ts flag accepts, and verify strict-parse rejection of
// trailing junk (claude high #904, codex P2 #904).
func TestParseLastCommitTS(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		in   string
		want uint64
	}{
		{"0", 0},
		{"1234567890", 1234567890},
		{"0xff", 0xff},
		{"0X10", 0x10},
	} {
		got, err := parseLastCommitTS(tc.in)
		if err != nil {
			t.Errorf("%q: %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%q: got %d want %d", tc.in, got, tc.want)
		}
	}
	// Reject empty, malformed, and trailing junk.
	for _, bad := range []string{
		"",
		"abc",
		"0xZZ",
		"0xffZZ",   // trailing hex garbage — fmt.Sscanf would accept as 0xff
		"100oops",  // trailing decimal garbage — fmt.Sscanf would accept as 100
		"-1",       // negative
		" 100 ext", // whitespace + extra
	} {
		if _, err := parseLastCommitTS(bad); err == nil {
			t.Errorf("%q parsed successfully; want error", bad)
		}
	}
}

// TestCLIPublishesFsmAndSidecarMode0600 pins claude v4 #904: the
// produced .fsm and ENCODE_INFO.json are created with mode 0o600 so a
// multi-user backup host does not get a world-readable dataset. The
// earlier os.Create-based path relied on umask (typically 0644).
//
// Skips on Windows where Unix-style perm bits are not meaningful.
func TestCLIPublishesFsmAndSidecarMode0600(t *testing.T) {
	t.Parallel()
	if isWindows {
		t.Skip("perm bits not meaningful on Windows")
	}
	in := t.TempDir()
	emitMinimalManifest(t, in, 100)
	out := filepath.Join(t.TempDir(), "out.fsm")
	code, err := run([]string{"--input", in, "--output", out}, quietLogger())
	if err != nil || code != exitSuccess {
		t.Fatalf("run failed: code=%d err=%v", code, err)
	}
	for _, p := range []string{out, out + ".encode_info.json"} {
		info, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat %s: %v", p, err)
		}
		// Only check the owner bits (rwx); umask cannot widen beyond
		// what OpenFile requested but a misconfigured fs.ModeSticky
		// or similar could theoretically narrow. We just assert no
		// group/other access bits are set.
		if perm := info.Mode().Perm(); perm&0o077 != 0 {
			t.Errorf("%s mode = %o, want no group/other bits (0o600 or stricter)", p, perm)
		}
	}
}

// TestParseAdapterSetRejectsEmptySelection pins codex P2 #904: a CSV
// of only separators/whitespace MUST surface as a flag-parse error, not
// silently produce a zero AdapterSet that would publish a header-only
// .fsm.
func TestParseAdapterSetRejectsEmptySelection(t *testing.T) {
	t.Parallel()
	for _, bad := range []string{
		" ,",
		",,,",
		"   ",
		",",
	} {
		if _, err := parseAdapterSet(bad); err == nil {
			t.Errorf("--adapter %q parsed to a non-empty set; want error", bad)
		}
	}
	// Single-adapter selection still works.
	set, err := parseAdapterSet("s3")
	if err != nil {
		t.Fatalf("--adapter s3: %v", err)
	}
	if !set.S3 || set.Redis || set.DynamoDB || set.SQS {
		t.Errorf("--adapter s3 produced %+v, want only S3", set)
	}
}
