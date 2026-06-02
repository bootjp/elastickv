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
	"github.com/cockroachdb/errors"
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

// TestCLIAdapterDataErrorExitsTwo pins codex P2 v9 #904: when an
// adapter encoder rejects the input tree's contents (e.g. a malformed
// DynamoDB _schema.json with an empty table_name), the CLI exits 2
// (data-correctness) rather than 1 (operator/flag error) so runbooks
// can branch on exit status to quarantine bad dump data. Pinned via
// the same ErrDDBEncodeInvalidSchema fixture pattern used by the
// in-package DynamoDB encoder tests.
func TestCLIAdapterDataErrorExitsTwo(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	emitMinimalManifest(t, in, 100)
	// Empty table_name triggers ErrDDBEncodeInvalidSchema inside the
	// DynamoDB encoder; runAdapterEncoders marks it with
	// ErrEncodeAdapterData; run() maps that to exitDataErr.
	schemaDir := filepath.Join(in, "dynamodb", "tbl")
	if err := os.MkdirAll(schemaDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	schemaPath := filepath.Join(schemaDir, "_schema.json")
	body := []byte(`{"format_version":1,"table_name":"","primary_key":{"hash_key":{"name":"id","type":"S"}}}`)
	if err := os.WriteFile(schemaPath, body, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	out := filepath.Join(t.TempDir(), "out.fsm")
	code, err := run([]string{
		"--input", in,
		"--output", out,
		"--adapter", "dynamodb",
	}, quietLogger())
	if err == nil {
		t.Fatalf("run succeeded; want adapter rejection error")
	}
	if code != exitDataErr {
		t.Errorf("exit code = %d, want %d (data error from adapter rejection, not flag-parse error)", code, exitDataErr)
	}
	if _, statErr := os.Stat(out); !os.IsNotExist(statErr) {
		t.Errorf(".fsm exists at %s; should not be published on adapter rejection", out)
	}
}

// TestCLIRejectsUnsupportedManifestExclusions pins codex P2 v21 #904
// end-to-end: when MANIFEST.json sets one of the three exclusion
// flags the encoder cannot honor (include_incomplete_uploads,
// include_orphans, preserve_sqs_visibility) AND the corresponding
// adapter is enabled, the CLI must exit 2 (data-correctness) before
// any bytes are written. Mirrors the DynamoDB JSONL guard.
func TestCLIRejectsUnsupportedManifestExclusions(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		mutate   func(*backup.Exclusions)
		adapters string
	}{
		{
			name:     "include_incomplete_uploads with --adapter=s3",
			mutate:   func(e *backup.Exclusions) { e.IncludeIncompleteUploads = true },
			adapters: "s3",
		},
		{
			name:     "include_orphans with --adapter=s3",
			mutate:   func(e *backup.Exclusions) { e.IncludeOrphans = true },
			adapters: "s3",
		},
		{
			name:     "preserve_sqs_visibility with --adapter=sqs",
			mutate:   func(e *backup.Exclusions) { e.PreserveSQSVisibility = true },
			adapters: "sqs",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			in := t.TempDir()
			m := backup.NewPhase0SnapshotManifest(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
			m.LastCommitTS = 100
			m.Adapters = &backup.Adapters{}
			m.Exclusions = &backup.Exclusions{}
			tc.mutate(m.Exclusions)
			f, ferr := os.Create(filepath.Join(in, "MANIFEST.json"))
			if ferr != nil {
				t.Fatalf("create MANIFEST.json: %v", ferr)
			}
			if werr := backup.WriteManifest(f, m); werr != nil {
				t.Fatalf("WriteManifest: %v", werr)
			}
			if cerr := f.Close(); cerr != nil {
				t.Fatalf("close: %v", cerr)
			}
			out := filepath.Join(t.TempDir(), "out.fsm")
			code, err := run([]string{
				"--input", in,
				"--output", out,
				"--adapter", tc.adapters,
			}, quietLogger())
			if err == nil {
				t.Fatalf("run succeeded; want exit-2 from unsupported manifest exclusion")
			}
			if code != exitDataErr {
				t.Errorf("exit code = %d, want %d (data-correctness for unsupported exclusion)", code, exitDataErr)
			}
			if _, statErr := os.Stat(out); !os.IsNotExist(statErr) {
				t.Errorf(".fsm exists at %s; should not be published on unsupported-feature rejection", out)
			}
		})
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
// canonicalRoundTripTS is the fixed last_commit_ts used by every
// canonicalizeInput call site. Kept as a const so a future test that
// wants a different value can lift it back into a parameter.
const canonicalRoundTripTS uint64 = 7000

func canonicalizeInput(t *testing.T, rawIn string) string {
	t.Helper()
	canonicalIn := t.TempDir()
	tmpOut := filepath.Join(t.TempDir(), "canonical.fsm")
	code, err := run([]string{"--input", rawIn, "--output", tmpOut, "--adapter", "sqs"}, quietLogger())
	if err != nil || code != exitSuccess {
		t.Fatalf("canonical encode: code=%d err=%v", code, err)
	}
	f, oerr := os.Open(tmpOut)
	if oerr != nil {
		t.Fatalf("open canonical output: %v", oerr)
	}
	defer func() { _ = f.Close() }()
	if _, err := backup.DecodeSnapshot(f, backup.DecodeOptions{
		OutRoot:  canonicalIn,
		Adapters: backup.AdapterSet{SQS: true},
	}); err != nil {
		t.Fatalf("canonical decode: %v", err)
	}
	emitMinimalManifest(t, canonicalIn, canonicalRoundTripTS)
	return canonicalIn
}

// flipBytesPastHeaderInTempCorruptHook returns a corrupt-buffer hook
// that flips one byte every 13 starting at offset 200 in the on-disk
// self-test buffer — the same pattern as the library's
// flipBytesPastHeaderHelper. Extracted so the three CLI mismatch
// tests share one body rather than each open-coding the same loop.
func flipBytesPastHeaderInTempCorruptHook(t *testing.T) func(*os.File) {
	t.Helper()
	return func(f *os.File) {
		info, ferr := f.Stat()
		if ferr != nil {
			t.Fatalf("temp Stat: %v", ferr)
		}
		const headerSkip = 200
		if info.Size() <= headerSkip {
			t.Fatalf("temp file too small to corrupt past header: %d bytes", info.Size())
		}
		buf := make([]byte, info.Size()-headerSkip)
		if _, rerr := f.ReadAt(buf, headerSkip); rerr != nil {
			t.Fatalf("ReadAt: %v", rerr)
		}
		for i := 0; i < len(buf); i += 13 {
			buf[i] ^= 0xFF
		}
		if _, werr := f.WriteAt(buf, headerSkip); werr != nil {
			t.Fatalf("WriteAt: %v", werr)
		}
	}
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

// TestCLIAdapterErrorPreservesPriorSidecar pins codex P2 v17 #904: when
// a run gets past manifest/TS validation and then fails inside an
// adapter encoder (non-self-test exit-2), the prior <output>.fsm is
// preserved (only the self-test mismatch path removes it), so the
// prior <output>.encode_info.json must ALSO be preserved — wiping it
// while leaving the .fsm would orphan the restore artifact from its
// provenance metadata. The v17 fix drops the pre-encode sidecar
// cleanup; this test pins the resulting invariant end-to-end.
func TestCLIAdapterErrorPreservesPriorSidecar(t *testing.T) {
	t.Parallel()
	in, out, priorFSM, priorSidecar := setupAdapterErrorFixture(t)
	code, err := run([]string{
		"--input", in,
		"--output", out,
		"--adapter", "dynamodb",
	}, quietLogger())
	if err == nil {
		t.Fatalf("run succeeded; want adapter-data rejection")
	}
	if code != exitDataErr {
		t.Errorf("exit code = %d, want %d", code, exitDataErr)
	}
	assertFilePreserved(t, out, priorFSM, "prior .fsm")
	// Prior sidecar unchanged (codex P2 v17: the v17 fix drops the
	// pre-encode sidecar cleanup so the sidecar+.fsm stay paired).
	assertFilePreserved(t, out+".encode_info.json", priorSidecar, "prior sidecar")
}

// setupAdapterErrorFixture builds a fixture for
// TestCLIAdapterErrorPreservesPriorSidecar: an InputRoot with a valid
// MANIFEST.json plus a malformed dynamodb _schema.json (empty
// table_name → ErrDDBEncodeInvalidSchema), and a pre-placed FSM +
// sidecar at the output path representing a hypothetical earlier
// successful run. Returns (inputRoot, outputPath, priorFSMBytes,
// priorSidecarBytes).
func setupAdapterErrorFixture(t *testing.T) (string, string, []byte, []byte) {
	t.Helper()
	in := t.TempDir()
	emitMinimalManifest(t, in, 1000)
	out := filepath.Join(t.TempDir(), "out.fsm")
	priorFSM := []byte("PRIOR FSM BYTES")
	priorSidecar := []byte(`{"format_version":1,"encoder_version":"prior","input_root":"x","output_fsm_path":"x"}`)
	if err := os.WriteFile(out, priorFSM, 0o600); err != nil {
		t.Fatalf("WriteFile prior fsm: %v", err)
	}
	if err := os.WriteFile(out+".encode_info.json", priorSidecar, 0o600); err != nil {
		t.Fatalf("WriteFile prior sidecar: %v", err)
	}
	schemaDir := filepath.Join(in, "dynamodb", "tbl")
	if err := os.MkdirAll(schemaDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	body := []byte(`{"format_version":1,"table_name":"","primary_key":{"hash_key":{"name":"id","type":"S"}}}`)
	if err := os.WriteFile(filepath.Join(schemaDir, "_schema.json"), body, 0o600); err != nil {
		t.Fatalf("WriteFile bad schema: %v", err)
	}
	return in, out, priorFSM, priorSidecar
}

// assertFilePreserved asserts the named file is still present and its
// contents exactly match wantBody. label appears in error messages.
func assertFilePreserved(t *testing.T, path string, wantBody []byte, label string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s at %s: %v", label, path, err)
	}
	if !bytes.Equal(got, wantBody) {
		t.Errorf("%s mutated; codex P2 v17 expected adapter-error to preserve", label)
	}
}

// TestCLISelfTestMismatchSkipsDirectoryAtOutputPath pins codex P2 v14
// #904: the self-test-mismatch cleanup must NOT delete an --output
// path that resolves to a directory (or any non-regular file). The
// prior unconditional os.Remove(cfg.outputPath) would have wiped an
// empty directory the operator passed in error.
//
// The fixture pre-creates an empty directory at the --output path,
// drives a self-test mismatch, asserts publishErr == errSelfTestMismatch,
// and asserts the directory is STILL PRESENT (the destructive
// cleanup did not fire). The normal publish path would have failed
// at os.Rename — this test pins the mismatch-cleanup-specific guard.
func TestCLISelfTestMismatchSkipsDirectoryAtOutputPath(t *testing.T) {
	t.Parallel()
	rawIn := t.TempDir()
	writeSQSFixture(t, rawIn)
	emitMinimalManifest(t, rawIn, 7000)
	canonicalIn := canonicalizeInput(t, rawIn)

	out := filepath.Join(t.TempDir(), "out.fsm")
	// Pre-create a directory at the --output path — an operator
	// typo, but the cleanup MUST NOT destructively remove it.
	if err := os.Mkdir(out, 0o755); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}

	scratchBase := t.TempDir()
	encodeOpts := backup.EncodeOptions{
		InputRoot:            canonicalIn,
		Adapters:             backup.AdapterSet{SQS: true},
		LastCommitTS:         7000,
		ManifestLastCommitTS: 7000,
		SelfTest:             true,
		SelfTestDecodeOptions: backup.DecodeOptions{
			OutRoot:  scratchBase,
			Adapters: backup.AdapterSet{SQS: true},
		},
	}
	encodeOpts.SetSelfTestCorruptHookForTest(flipBytesPastHeaderInTempCorruptHook(t))

	cfg := &config{
		inputPath:  canonicalIn,
		outputPath: out,
		adapters:   backup.AdapterSet{SQS: true},
		selfTest:   true,
	}
	mismatchPath := out + ".mismatch.txt"

	_, publishErr := writeAndPublish(cfg, encodeOpts, mismatchPath, quietLogger())
	if !errors.Is(publishErr, errSelfTestMismatch) {
		t.Fatalf("publishErr = %v, want errSelfTestMismatch", publishErr)
	}
	info, statErr := os.Stat(out)
	if statErr != nil {
		t.Fatalf("output path missing after mismatch (codex P2 v14 destructive cleanup regression): %v", statErr)
	}
	if !info.IsDir() {
		t.Errorf("output mode = %s; expected the pre-placed directory to be preserved", info.Mode())
	}
}

// TestCLIInvalidManifestExitsTwo pins codex P2 v14 #904: a malformed
// MANIFEST.json (invalid JSON or unsupported format_version) surfaces
// backup.ErrInvalidManifest / backup.ErrUnsupportedFormatVersion from
// readInputManifest, and the CLI MUST map both to exit 2
// (data-correctness). Treating a broken manifest as exit 1 misroutes
// runbook recovery for corrupt-dump scenarios.
func TestCLIInvalidManifestExitsTwo(t *testing.T) {
	t.Parallel()
	t.Run("invalid JSON body", func(t *testing.T) {
		t.Parallel()
		in := t.TempDir()
		if err := os.WriteFile(filepath.Join(in, "MANIFEST.json"), []byte("{not json"), 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		out := filepath.Join(t.TempDir(), "out.fsm")
		code, err := run([]string{"--input", in, "--output", out}, quietLogger())
		if err == nil {
			t.Fatalf("run succeeded; want manifest parse error")
		}
		if code != exitDataErr {
			t.Errorf("exit code = %d, want %d (invalid manifest is data-correctness)", code, exitDataErr)
		}
	})
	t.Run("unsupported format_version", func(t *testing.T) {
		t.Parallel()
		in := t.TempDir()
		if err := os.WriteFile(filepath.Join(in, "MANIFEST.json"),
			[]byte(`{"format_version":99,"last_commit_ts":1}`), 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		out := filepath.Join(t.TempDir(), "out.fsm")
		code, err := run([]string{"--input", in, "--output", out}, quietLogger())
		if err == nil {
			t.Fatalf("run succeeded; want unsupported format_version")
		}
		if code != exitDataErr {
			t.Errorf("exit code = %d, want %d (unsupported manifest format_version is data-correctness)", code, exitDataErr)
		}
	})
}

// TestCLISelfTestMismatchRemovesSymlinkOutputButPreservesTarget pins
// codex P2 v19 #904: when --output is a symlink to a prior .fsm file
// and the new --self-test invocation mismatches, the cleanup must
// unlink the symlink (so the restore-visible --output path now
// resolves to ENOENT, matching the mismatch contract) while leaving
// the underlying target file intact (os.Remove on a symlink operates
// on the link, not the resolved target).
//
// Before v21 the IsRegular()-only check silently skipped the symlink
// cleanup; the new sidecar (matched=false) then described a fresh
// failed encode while --output still resolved to a prior valid .fsm,
// breaking the "no restore-visible FSM after self-test mismatch"
// invariant. Linux-only because Windows symlink semantics differ.
func TestCLISelfTestMismatchRemovesSymlinkOutputButPreservesTarget(t *testing.T) {
	if isWindows {
		t.Skip("symlink semantics differ on Windows")
	}
	t.Parallel()
	rawIn := t.TempDir()
	writeSQSFixture(t, rawIn)
	emitMinimalManifest(t, rawIn, 7000)
	canonicalIn := canonicalizeInput(t, rawIn)

	targetDir := t.TempDir()
	target := filepath.Join(targetDir, "real.fsm")
	const targetBody = "TARGET FSM BYTES (must survive symlink removal)"
	if err := os.WriteFile(target, []byte(targetBody), 0o600); err != nil {
		t.Fatalf("WriteFile target: %v", err)
	}
	out := filepath.Join(t.TempDir(), "out.fsm")
	if err := os.Symlink(target, out); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	scratchBase := t.TempDir()
	encodeOpts := backup.EncodeOptions{
		InputRoot:            canonicalIn,
		Adapters:             backup.AdapterSet{SQS: true},
		LastCommitTS:         7000,
		ManifestLastCommitTS: 7000,
		SelfTest:             true,
		SelfTestDecodeOptions: backup.DecodeOptions{
			OutRoot:  scratchBase,
			Adapters: backup.AdapterSet{SQS: true},
		},
	}
	encodeOpts.SetSelfTestCorruptHookForTest(flipBytesPastHeaderInTempCorruptHook(t))

	cfg := &config{
		inputPath:  canonicalIn,
		outputPath: out,
		adapters:   backup.AdapterSet{SQS: true},
		selfTest:   true,
	}
	mismatchPath := out + ".mismatch.txt"

	_, publishErr := writeAndPublish(cfg, encodeOpts, mismatchPath, quietLogger())
	if !errors.Is(publishErr, errSelfTestMismatch) {
		t.Fatalf("publishErr = %v, want errSelfTestMismatch", publishErr)
	}
	// --output (the symlink) must now resolve to ENOENT.
	if _, statErr := os.Lstat(out); !os.IsNotExist(statErr) {
		t.Errorf("symlink at --output not removed after mismatch (codex P2 v19 regression)")
	}
	// The target file (which the symlink pointed to) must survive.
	gotTarget, rerr := os.ReadFile(target)
	if rerr != nil {
		t.Fatalf("target file vanished (os.Remove operated on resolved target instead of symlink): %v", rerr)
	}
	if string(gotTarget) != targetBody {
		t.Errorf("target body mutated; want preserved")
	}
}

// TestCLIWriteAndPublishRemovesStaleFSMOnSelfTestMismatch pins codex
// P2 v10 #904: when a prior successful run left an <output>.fsm on
// disk and a new --self-test invocation produces a mismatch,
// writeAndPublish must remove that stale .fsm. Otherwise encodeOne
// writes a fresh sidecar (matched=false, NEW SHA) alongside the OLD
// bytes — violating the CLI contract that a self-test failure leaves
// no restore-visible FSM, and making the sidecar describe an FSM that
// is not on disk.
//
// To drive a deterministic self-test mismatch end-to-end through the
// CLI's writeAndPublish, the test uses the backup package's exported
// test seam (SetSelfTestCorruptHookForTest) to flip bytes in the
// disk-backed self-test buffer between WriteTo and the re-decode.
func TestCLIWriteAndPublishRemovesStaleFSMOnSelfTestMismatch(t *testing.T) {
	t.Parallel()
	rawIn := t.TempDir()
	writeSQSFixture(t, rawIn)
	emitMinimalManifest(t, rawIn, 7000)
	canonicalIn := canonicalizeInput(t, rawIn)

	out := filepath.Join(t.TempDir(), "out.fsm")
	// Pre-place a stale .fsm — what a prior successful run would have
	// left behind. The codex P2 v10 contract is that a subsequent
	// self-test mismatch invalidates this file.
	stalePayload := []byte("STALE FSM FROM PRIOR SUCCESSFUL RUN")
	if err := os.WriteFile(out, stalePayload, 0o600); err != nil {
		t.Fatalf("WriteFile stale: %v", err)
	}

	scratchBase := t.TempDir()
	encodeOpts := backup.EncodeOptions{
		InputRoot:            canonicalIn,
		Adapters:             backup.AdapterSet{SQS: true},
		LastCommitTS:         7000,
		ManifestLastCommitTS: 7000,
		SelfTest:             true,
		SelfTestDecodeOptions: backup.DecodeOptions{
			OutRoot:  scratchBase,
			Adapters: backup.AdapterSet{SQS: true},
		},
	}
	// Flip bytes past the EKVPBBL1 header so the re-decode trips on
	// a malformed entry length and the self-test returns matched=false.
	encodeOpts.SetSelfTestCorruptHookForTest(flipBytesPastHeaderInTempCorruptHook(t))

	cfg := &config{
		inputPath:  canonicalIn,
		outputPath: out,
		adapters:   backup.AdapterSet{SQS: true},
		selfTest:   true,
	}
	mismatchPath := out + ".mismatch.txt"

	_, publishErr := writeAndPublish(cfg, encodeOpts, mismatchPath, quietLogger())
	if !errors.Is(publishErr, errSelfTestMismatch) {
		t.Fatalf("publishErr = %v, want errSelfTestMismatch", publishErr)
	}
	if _, statErr := os.Stat(out); !os.IsNotExist(statErr) {
		t.Errorf("stale .fsm at %s not removed after self-test mismatch (codex P2 v10)", out)
	}
	// The mismatch.txt should be present as the operator-visible
	// record of the failed encode attempt.
	if _, statErr := os.Stat(mismatchPath); statErr != nil {
		t.Errorf("mismatch.txt missing after self-test mismatch: %v", statErr)
	}
}

// TestCLINonSelfTestExitTwoPreservesPriorFSM pins the surgical scope
// of the codex P2 v10 fix: non-self-test exit-2 paths (e.g. the
// manifest-floor HLC regression that fails BEFORE writeAndPublish)
// must NOT remove a prior <output>.fsm. Only self-test mismatch
// triggers the cleanup; a runbook recovering from a typo'd
// --last-commit-ts still has its last good FSM on disk.
func TestCLINonSelfTestExitTwoPreservesPriorFSM(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	emitMinimalManifest(t, in, 1000)
	out := filepath.Join(t.TempDir(), "out.fsm")
	stalePayload := []byte("STALE FSM FROM PRIOR SUCCESSFUL RUN")
	if err := os.WriteFile(out, stalePayload, 0o600); err != nil {
		t.Fatalf("WriteFile stale: %v", err)
	}
	// Manifest-floor regression → exit-2 from resolveLastCommitTS,
	// before writeAndPublish runs. Stale .fsm should be preserved.
	code, err := run([]string{
		"--input", in,
		"--output", out,
		"--last-commit-ts", "500", // below manifest 1000
	}, quietLogger())
	if err == nil {
		t.Fatalf("run succeeded; want manifest-floor regression")
	}
	if code != exitDataErr {
		t.Errorf("exit code = %d, want %d", code, exitDataErr)
	}
	body, rerr := os.ReadFile(out)
	if rerr != nil {
		t.Fatalf("read stale .fsm: %v (must be preserved on non-self-test exit-2)", rerr)
	}
	if !bytes.Equal(body, stalePayload) {
		t.Errorf("stale .fsm mutated; want preserved on manifest-floor regression")
	}
}

// TestCLIRoundTripSelfTestAllAdapters is the gold-standard CLI-level
// end-to-end test: a real adapter fixture, encoder runs with
// --self-test, exit 0, matched:true in the sidecar.
func TestCLIRoundTripSelfTestAllAdapters(t *testing.T) {
	t.Parallel()
	rawIn := t.TempDir()
	writeSQSFixture(t, rawIn)
	emitMinimalManifest(t, rawIn, 7000)
	canonicalIn := canonicalizeInput(t, rawIn)

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

// TestCLIManifestFloorLeavesNoStaleSidecar pins that the
// manifest-floor preflight failure (--last-commit-ts T < manifest;
// fails in resolveLastCommitTS BEFORE writeAndPublish) leaves NO
// <output>.encode_info.json on disk — neither a fresh one nor a
// stale one from a prior run (the pre-encode cleanup at the start
// of encodeOne removes it).
//
// Note: the test name was previously TestCLISelfTestMismatchWritesSidecarWithMatchedFalse,
// which contradicted the assertion (the encode does NOT run on this
// path, so no sidecar is written). The actual sidecar-on-mismatch
// behavior is now pinned end-to-end by
// TestCLIWriteAndPublishRemovesStaleFSMOnSelfTestMismatch using the
// CLI-level corruption seam (codex P2 v6/v10 #904; claude v12 rename).
func TestCLIManifestFloorLeavesNoStaleSidecar(t *testing.T) {
	t.Parallel()
	// The pre-encode cleanup at the top of encodeOne removes any
	// stale <output>.encode_info.json before writeAndPublish runs.
	// On the manifest-floor path, resolveLastCommitTS exits with
	// exit-2 BEFORE that cleanup even runs (it's the second step in
	// encodeOne after readInputManifest). So the assertion is: a
	// fresh TempDir produces no sidecar at all.
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
