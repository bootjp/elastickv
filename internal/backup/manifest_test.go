package backup

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
)

func TestManifest_Phase0RoundTrip(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 4, 29, 15, 42, 11, 94_000_000, time.UTC)
	m := NewPhase0SnapshotManifest(now)
	m.ElastickvVersion = "v1.7.3"
	m.ClusterID = "ek-prod-us-east-1"
	m.SnapshotIndex = 18432021
	m.LastCommitTS = 4517352099840000
	m.Source = &Source{FSMPath: "/data/fsm-snap/0000000000000064.fsm", FSMCRC32C: "deadbeef"}
	m.Adapters = Adapters{
		DynamoDB: &Adapter{Tables: []string{"orders", "users"}},
		S3:       &Adapter{Buckets: []string{"photos"}},
		Redis:    &Adapter{Databases: []uint32{0}},
		SQS:      &Adapter{Queues: []string{"orders-fifo.fifo"}},
	}
	m.Exclusions = Exclusions{} // all defaults

	var buf bytes.Buffer
	if err := WriteManifest(&buf, m); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	got, err := ReadManifest(&buf)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	if got.Phase != PhasePhase0SnapshotDecode {
		t.Fatalf("Phase = %q, want %q", got.Phase, PhasePhase0SnapshotDecode)
	}
	if got.SnapshotIndex != m.SnapshotIndex {
		t.Fatalf("SnapshotIndex = %d, want %d", got.SnapshotIndex, m.SnapshotIndex)
	}
	if got.Source == nil || got.Source.FSMPath != m.Source.FSMPath {
		t.Fatalf("Source.FSMPath = %v, want %v", got.Source, m.Source)
	}
	if got.Live != nil {
		t.Fatalf("phase0 manifest must not set Live, got %+v", got.Live)
	}
}

func TestManifest_Phase1MustNotSetSource(t *testing.T) {
	t.Parallel()
	m := NewPhase0SnapshotManifest(time.Now())
	m.Phase = PhasePhase1LivePinned
	m.Source = &Source{FSMPath: "ignored"}
	var buf bytes.Buffer
	err := WriteManifest(&buf, m)
	if !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("WriteManifest err=%v want ErrInvalidManifest", err)
	}
}

func TestManifest_Phase0MustNotSetLive(t *testing.T) {
	t.Parallel()
	m := NewPhase0SnapshotManifest(time.Now())
	m.Live = &Live{ReadTS: 12345}
	var buf bytes.Buffer
	err := WriteManifest(&buf, m)
	if !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("WriteManifest err=%v want ErrInvalidManifest", err)
	}
}

func TestReadManifest_RejectsFutureFormatVersion(t *testing.T) {
	t.Parallel()
	m := NewPhase0SnapshotManifest(time.Now())
	m.FormatVersion = CurrentFormatVersion + 1
	// validate() runs before encoding, so go around it.
	body, _ := json.Marshal(m)
	_, err := ReadManifest(bytes.NewReader(body))
	if !errors.Is(err, ErrUnsupportedFormatVersion) {
		t.Fatalf("err=%v want ErrUnsupportedFormatVersion", err)
	}
}

func TestReadManifest_RejectsZeroFormatVersion(t *testing.T) {
	t.Parallel()
	m := NewPhase0SnapshotManifest(time.Now())
	m.FormatVersion = 0
	body, _ := json.Marshal(m)
	_, err := ReadManifest(bytes.NewReader(body))
	if !errors.Is(err, ErrUnsupportedFormatVersion) {
		t.Fatalf("err=%v want ErrUnsupportedFormatVersion", err)
	}
}

func TestReadManifest_RejectsUnknownFields(t *testing.T) {
	t.Parallel()
	// Format drift safety: an unknown field surfaces loudly rather than
	// being silently ignored.
	body := `{
		"format_version": 1,
		"phase": "phase0-snapshot-decode",
		"wall_time_iso": "2026-04-29T00:00:00Z",
		"adapters": {"dynamodb":{}, "s3":{}, "redis":{}, "sqs":{}},
		"exclusions": {"include_incomplete_uploads":false,"include_orphans":false,"preserve_sqs_visibility":false,"include_sqs_side_records":false},
		"checksum_algorithm": "sha256",
		"checksum_format": "sha256sum",
		"encoded_filename_charset": "rfc3986-unreserved-plus-percent",
		"key_segment_max_bytes": 240,
		"s3_meta_suffix": ".elastickv-meta.json",
		"s3_collision_strategy": "leaf-data-suffix",
		"dynamodb_layout": "per-item",
		"unknown_field": "ahoy"
	}`
	_, err := ReadManifest(strings.NewReader(body))
	if !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("err=%v want ErrInvalidManifest", err)
	}
}

func TestReadManifest_RejectsUnknownPhase(t *testing.T) {
	t.Parallel()
	body := `{
		"format_version": 1,
		"phase": "phase99-future",
		"wall_time_iso": "2026-04-29T00:00:00Z",
		"adapters": {"dynamodb":{}, "s3":{}, "redis":{}, "sqs":{}},
		"exclusions": {"include_incomplete_uploads":false,"include_orphans":false,"preserve_sqs_visibility":false,"include_sqs_side_records":false},
		"checksum_algorithm": "sha256",
		"checksum_format": "sha256sum",
		"encoded_filename_charset": "rfc3986-unreserved-plus-percent",
		"key_segment_max_bytes": 240,
		"s3_meta_suffix": ".elastickv-meta.json",
		"s3_collision_strategy": "leaf-data-suffix",
		"dynamodb_layout": "per-item"
	}`
	_, err := ReadManifest(strings.NewReader(body))
	if !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("err=%v want ErrInvalidManifest", err)
	}
}

func TestReadManifest_RejectsBadWallTime(t *testing.T) {
	t.Parallel()
	body := `{
		"format_version": 1,
		"phase": "phase0-snapshot-decode",
		"wall_time_iso": "not-a-date",
		"adapters": {"dynamodb":{}, "s3":{}, "redis":{}, "sqs":{}},
		"exclusions": {"include_incomplete_uploads":false,"include_orphans":false,"preserve_sqs_visibility":false,"include_sqs_side_records":false},
		"checksum_algorithm": "sha256",
		"checksum_format": "sha256sum",
		"encoded_filename_charset": "rfc3986-unreserved-plus-percent",
		"key_segment_max_bytes": 240,
		"s3_meta_suffix": ".elastickv-meta.json",
		"s3_collision_strategy": "leaf-data-suffix",
		"dynamodb_layout": "per-item"
	}`
	_, err := ReadManifest(strings.NewReader(body))
	if !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("err=%v want ErrInvalidManifest", err)
	}
}

func TestReadManifest_RejectsUnsupportedDynamoDBLayout(t *testing.T) {
	t.Parallel()
	m := NewPhase0SnapshotManifest(time.Now())
	m.DynamoDBLayout = "bogus"
	body, _ := json.Marshal(m)
	_, err := ReadManifest(bytes.NewReader(body))
	if !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("err=%v want ErrInvalidManifest", err)
	}
}

func TestNewPhase0SnapshotManifest_DefaultsArePopulated(t *testing.T) {
	t.Parallel()
	m := NewPhase0SnapshotManifest(time.Now())
	if m.FormatVersion != CurrentFormatVersion {
		t.Fatalf("FormatVersion = %d, want %d", m.FormatVersion, CurrentFormatVersion)
	}
	if m.Phase != PhasePhase0SnapshotDecode {
		t.Fatalf("Phase = %q, want %q", m.Phase, PhasePhase0SnapshotDecode)
	}
	if m.ChecksumAlgorithm != ChecksumAlgorithmSHA256 {
		t.Fatalf("ChecksumAlgorithm = %q, want %q", m.ChecksumAlgorithm, ChecksumAlgorithmSHA256)
	}
	if m.ChecksumFormat != ChecksumFormatSha256sum {
		t.Fatalf("ChecksumFormat = %q, want %q", m.ChecksumFormat, ChecksumFormatSha256sum)
	}
	if m.S3MetaSuffix != S3MetaSuffixDefault {
		t.Fatalf("S3MetaSuffix = %q", m.S3MetaSuffix)
	}
	if m.S3CollisionStrategy != S3CollisionStrategyLeafDataSuffix {
		t.Fatalf("S3CollisionStrategy = %q", m.S3CollisionStrategy)
	}
	if m.DynamoDBLayout != DynamoDBLayoutPerItem {
		t.Fatalf("DynamoDBLayout = %q", m.DynamoDBLayout)
	}
	if m.KeySegmentMaxBytes != KeySegmentMaxBytesDefault {
		t.Fatalf("KeySegmentMaxBytes = %d, want %d", m.KeySegmentMaxBytes, KeySegmentMaxBytesDefault)
	}
}

func TestReadManifest_RejectsTrailingBytes(t *testing.T) {
	t.Parallel()
	// Two manifests concatenated; the second must surface as a
	// trailing-bytes error rather than be silently discarded — Codex
	// P2 #194.
	m := NewPhase0SnapshotManifest(time.Now())
	body, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	bad := append([]byte{}, body...)
	bad = append(bad, body...)
	_, err = ReadManifest(bytes.NewReader(bad))
	if !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("err=%v want ErrInvalidManifest on trailing bytes", err)
	}
}

func TestReadManifest_RejectsTrailingNonWhitespace(t *testing.T) {
	t.Parallel()
	m := NewPhase0SnapshotManifest(time.Now())
	body, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	bad := append([]byte{}, body...)
	bad = append(bad, []byte("garbage")...)
	_, err = ReadManifest(bytes.NewReader(bad))
	if !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("err=%v want ErrInvalidManifest on trailing garbage", err)
	}
}

func TestAdaptersStruct_NilVsEmptyDistinguishedOnDisk(t *testing.T) {
	t.Parallel()
	// Gemini #98: an excluded adapter (nil pointer) must serialize
	// differently from an included-but-empty adapter (non-nil pointer
	// to Adapter{}).
	excluded := Adapters{
		DynamoDB: &Adapter{}, // present, no scopes
		// S3 / Redis / SQS left nil — out of scope
	}
	body, err := json.Marshal(excluded)
	if err != nil {
		t.Fatal(err)
	}
	out := string(body)
	if !strings.Contains(out, `"dynamodb":{}`) {
		t.Fatalf("included-empty must serialise as `dynamodb:{}`, got %s", out)
	}
	if strings.Contains(out, `"s3"`) || strings.Contains(out, `"redis"`) || strings.Contains(out, `"sqs"`) {
		t.Fatalf("excluded adapters must be omitted, got %s", out)
	}
}

func TestWriteManifest_ProducesPrettyJSON(t *testing.T) {
	t.Parallel()
	m := NewPhase0SnapshotManifest(time.Now())
	var buf bytes.Buffer
	if err := WriteManifest(&buf, m); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	out := buf.String()
	// Pretty: contains newlines and the 2-space indent we configured.
	if !strings.Contains(out, "\n  \"format_version\"") {
		t.Fatalf("expected pretty 2-space indent in output:\n%s", out)
	}
}
