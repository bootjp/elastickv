package backup

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
)

// TestEncodeInfoRoundTrip pins WriteEncodeInfo -> ReadEncodeInfo for a
// populated struct. Forward-compat: an ENCODE_INFO.json with unknown
// extra fields at the same format_version must decode cleanly.
func TestEncodeInfoRoundTrip(t *testing.T) {
	t.Parallel()
	info := NewEncodeInfo(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
	info.EncoderVersion = "test-rev"
	info.InputRoot = "/in"
	info.OutputFSMPath = "/out.fsm"
	info.OutputFSMSHA256 = "deadbeef"
	info.LastCommitTS = 18446744073709551610
	info.LastCommitTSOverridden = false
	info.ManifestLastCommitTS = 18446744073709551610
	info.ManifestClusterID = "cluster-1"
	info.AdaptersEnabled = []string{"redis", "dynamodb", "s3", "sqs"}
	info.SelfTest = EncodeInfoSelfTest{Ran: true, Matched: true}

	var buf bytes.Buffer
	if err := WriteEncodeInfo(&buf, info); err != nil {
		t.Fatalf("WriteEncodeInfo: %v", err)
	}
	got, err := ReadEncodeInfo(&buf)
	if err != nil {
		t.Fatalf("ReadEncodeInfo: %v", err)
	}
	if got.EncoderVersion != "test-rev" || got.OutputFSMSHA256 != "deadbeef" || got.LastCommitTS != 18446744073709551610 {
		t.Errorf("round-trip mismatch: %+v", got)
	}
	if got.SelfTest.Ran != true || got.SelfTest.Matched != true {
		t.Errorf("self_test field round-trip: %+v", got.SelfTest)
	}

	// Forward-compat: extra field at same version decodes cleanly.
	withExtra := `{"format_version":1,"encoder_version":"x","wall_time_iso":"2026-06-01T12:00:00Z","input_root":"/in","output_fsm_path":"/out.fsm","output_fsm_sha256":"d","last_commit_ts":1,"last_commit_ts_overridden":false,"manifest_last_commit_ts":1,"adapters_enabled":[],"self_test":{"ran":false,"matched":false},"future_field":"ignored"}`
	if _, err := ReadEncodeInfo(strings.NewReader(withExtra)); err != nil {
		t.Errorf("forward-compat unknown field rejected: %v", err)
	}
}

// TestEncodeInfoRejectsUnknownFormatVersion mirrors the decoder's
// TestManifestVersionGate: a future schema bump surfaces as a typed
// error rather than a silent field drop.
func TestEncodeInfoRejectsUnknownFormatVersion(t *testing.T) {
	t.Parallel()
	bad := `{"format_version":99,"encoder_version":"x","wall_time_iso":"2026-06-01T12:00:00Z","input_root":"/in","output_fsm_path":"/out.fsm","output_fsm_sha256":"d","last_commit_ts":1,"last_commit_ts_overridden":false,"manifest_last_commit_ts":1,"adapters_enabled":[],"self_test":{"ran":false,"matched":false}}`
	_, err := ReadEncodeInfo(strings.NewReader(bad))
	if !errors.Is(err, ErrUnsupportedEncodeInfoFormatVersion) {
		t.Fatalf("err = %v, want ErrUnsupportedEncodeInfoFormatVersion", err)
	}
}

// TestExclusionsLegacyManifestOmitsRenameS3Collisions pins forward-compat
// on the new rename_s3_collisions field. Older manifests written before
// M6 do not include the field; ReadManifest must decode them with the
// zero value (false) — NOT reject as ErrInvalidManifest (gemini medium
// v5 #896).
func TestExclusionsLegacyManifestOmitsRenameS3Collisions(t *testing.T) {
	t.Parallel()
	// Build a known-valid manifest via the public constructor, then
	// rewrite the JSON to omit the rename_s3_collisions field — this
	// is exactly the on-disk shape a pre-M6 decoder run would produce.
	m := NewPhase0SnapshotManifest(time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC))
	m.Exclusions = &Exclusions{}
	m.Adapters = &Adapters{}
	var buf bytes.Buffer
	if err := WriteManifest(&buf, m); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	// Strip the new field to simulate a legacy producer.
	legacy := strings.ReplaceAll(buf.String(), `"rename_s3_collisions":false,`, ``)
	legacy = strings.ReplaceAll(legacy, `,"rename_s3_collisions":false`, ``)
	legacy = strings.ReplaceAll(legacy, `"rename_s3_collisions":false`, ``)

	got, err := ReadManifest(strings.NewReader(legacy))
	if err != nil {
		t.Fatalf("legacy manifest must decode without error, got: %v", err)
	}
	if got.Exclusions == nil {
		t.Fatalf("Exclusions = nil")
	}
	if got.Exclusions.RenameS3Collisions != false {
		t.Errorf("RenameS3Collisions = %v, want false (zero value for missing field)", got.Exclusions.RenameS3Collisions)
	}
}
