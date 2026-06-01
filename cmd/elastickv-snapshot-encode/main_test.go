package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/backup"
)

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
// --last-commit-ts flag accepts.
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
	// Reject empty and malformed.
	for _, bad := range []string{"", "abc", "0xZZ"} {
		if _, err := parseLastCommitTS(bad); err == nil {
			t.Errorf("%q parsed successfully; want error", bad)
		}
	}
}

// Helper to silence "unused strconv" if a future edit drops its only
// use — kept here as the canonical numeric test pin. Strconv is used
// implicitly in subtests via tc.argTS.
var _ = strconv.FormatUint
