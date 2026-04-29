package backup

import (
	"encoding/json"
	"io"
	"time"

	"github.com/cockroachdb/errors"
)

// MANIFEST.json is the only file a restore tool must read first. All other
// files in a dump are decoded from their on-disk path and contents. The
// manifest records:
//
//   - format_version (the only field a restore tool MUST consult before
//     trusting anything else)
//   - phase ("phase0-snapshot-decode" or "phase1-live-pinned") so a
//     consumer that cares about cross-shard PIT consistency can warn or
//     refuse on Phase 0 inputs
//   - source/origin metadata so a restore is auditable
//   - exclusion flags + format-policy fields so the producer's rendering
//     choices are explicit at restore time

// CurrentFormatVersion is the format major-version this code emits and
// accepts. Restore-side code MUST refuse `format_version > current`. A
// minor-version bump (e.g., adding optional fields) does not change this
// constant.
const CurrentFormatVersion uint32 = 1

const (
	// PhasePhase0SnapshotDecode marks dumps produced by Phase 0a (offline
	// snapshot decoder).
	PhasePhase0SnapshotDecode = "phase0-snapshot-decode"
	// PhasePhase1LivePinned marks dumps produced by Phase 1 (live PIT
	// extraction with cluster-wide read_ts pinning).
	PhasePhase1LivePinned = "phase1-live-pinned"
)

const (
	// ChecksumAlgorithmSHA256 is the only checksum algorithm Phase 0a writes.
	// Phase 1 may add others later (e.g. blake3) under the same field.
	ChecksumAlgorithmSHA256 = "sha256"
	// ChecksumFormatSha256sum identifies the line-oriented sha256sum(1)
	// format used by the CHECKSUMS file. Operators verify with
	// `sha256sum -c CHECKSUMS` from the dump root.
	ChecksumFormatSha256sum = "sha256sum"
	// EncodedFilenameCharsetRFC3986 is the EncodeSegment charset used for
	// every non-S3-object filename in the dump.
	EncodedFilenameCharsetRFC3986 = "rfc3986-unreserved-plus-percent"
	// S3MetaSuffixDefault is the reserved suffix for the S3 sidecar
	// metadata file (`<obj>.elastickv-meta.json`).
	S3MetaSuffixDefault = ".elastickv-meta.json"
	// S3CollisionStrategyLeafDataSuffix renames the shorter of two
	// colliding S3 keys to `<obj>.elastickv-leaf-data` and records the
	// rename in KEYMAP.jsonl.
	S3CollisionStrategyLeafDataSuffix = "leaf-data-suffix"
	// DynamoDBLayoutPerItem emits one item per file
	// (`items/<pk>/<sk>.json`); the user's stated default.
	DynamoDBLayoutPerItem = "per-item"
	// DynamoDBLayoutJSONL bundles items into `items/data-<part>.jsonl`
	// (opt-in via --dynamodb-bundle-mode jsonl).
	DynamoDBLayoutJSONL = "jsonl"
	// KeySegmentMaxBytesDefault matches EncodeSegment's maxSegmentBytes.
	KeySegmentMaxBytesDefault uint32 = 240
)

// Source records where a Phase 0 dump came from. Phase 1 dumps leave Source
// nil and populate Live instead.
type Source struct {
	// FSMPath is the absolute or relative path of the .fsm file the
	// decoder consumed.
	FSMPath string `json:"fsm_path"`
	// FSMCRC32C is the CRC32C value the decoder verified against the
	// .fsm file's footer (lowercase hex).
	FSMCRC32C string `json:"fsm_crc32c,omitempty"`
}

// Live records the cluster-wide pinning information that produced a Phase 1
// dump. Phase 0 dumps leave this nil.
type Live struct {
	// ReadTS is the pinned read_ts at which BackupScanner traversed the
	// keyspace.
	ReadTS uint64 `json:"read_ts"`
	// PinTokenSHA256 is the hex SHA-256 of the pin_token issued by
	// BeginBackup. Stored as a hash rather than the raw token so the
	// manifest carries no auth-sensitive material.
	PinTokenSHA256 string `json:"pin_token_sha256,omitempty"`
}

// Adapters lists which scopes were dumped per adapter. An empty slice
// means "no scopes for this adapter were dumped"; a nil slice means
// "this adapter was not in the dump's scope filter."
type Adapters struct {
	DynamoDB Adapter `json:"dynamodb"`
	S3       Adapter `json:"s3"`
	Redis    Adapter `json:"redis"`
	SQS      Adapter `json:"sqs"`
}

// Adapter holds the scope identifiers for one adapter. Field names are
// per-adapter to match the protocol's natural vocabulary.
type Adapter struct {
	Tables    []string `json:"tables,omitempty"`
	Buckets   []string `json:"buckets,omitempty"`
	Databases []uint32 `json:"databases,omitempty"`
	Queues    []string `json:"queues,omitempty"`
}

// Exclusions records the producer-side flags that affected which records
// were emitted. Restore tools log these so an operator can correlate a
// surprising dump shape with the producer invocation.
type Exclusions struct {
	IncludeIncompleteUploads bool `json:"include_incomplete_uploads"`
	IncludeOrphans           bool `json:"include_orphans"`
	PreserveSQSVisibility    bool `json:"preserve_sqs_visibility"`
	IncludeSQSSideRecords    bool `json:"include_sqs_side_records"`
}

// Manifest is the on-disk MANIFEST.json structure. Field tags match the
// spec in docs/design/2026_04_29_proposed_snapshot_logical_decoder.md.
type Manifest struct {
	FormatVersion     uint32     `json:"format_version"`
	Phase             string     `json:"phase"`
	ElastickvVersion  string     `json:"elastickv_version,omitempty"`
	ClusterID         string     `json:"cluster_id,omitempty"`
	SnapshotIndex     uint64     `json:"snapshot_index,omitempty"`
	LastCommitTS      uint64     `json:"last_commit_ts,omitempty"`
	WallTimeISO       string     `json:"wall_time_iso"`
	Source            *Source    `json:"source,omitempty"`
	Live              *Live      `json:"live,omitempty"`
	Adapters          Adapters   `json:"adapters"`
	Exclusions        Exclusions `json:"exclusions"`
	ChecksumAlgorithm string     `json:"checksum_algorithm"`
	ChecksumFormat    string     `json:"checksum_format"`

	EncodedFilenameCharset string `json:"encoded_filename_charset"`
	KeySegmentMaxBytes     uint32 `json:"key_segment_max_bytes"`
	S3MetaSuffix           string `json:"s3_meta_suffix"`
	S3CollisionStrategy    string `json:"s3_collision_strategy"`
	DynamoDBLayout         string `json:"dynamodb_layout"`
}

// ErrUnsupportedFormatVersion is returned by ReadManifest when the on-disk
// format_version is greater than CurrentFormatVersion or zero.
var ErrUnsupportedFormatVersion = errors.New("backup: manifest format_version unsupported")

// ErrInvalidManifest is returned by ReadManifest when the JSON parses but
// fails structural validation (missing required field, unknown phase, etc.).
var ErrInvalidManifest = errors.New("backup: manifest invalid")

// NewPhase0SnapshotManifest seeds a manifest with the Phase 0a defaults.
// Callers fill in scope (Adapters), Source/wall time and exclusions before
// passing it to WriteManifest.
func NewPhase0SnapshotManifest(now time.Time) Manifest {
	return Manifest{
		FormatVersion:          CurrentFormatVersion,
		Phase:                  PhasePhase0SnapshotDecode,
		WallTimeISO:            now.UTC().Format(time.RFC3339Nano),
		ChecksumAlgorithm:      ChecksumAlgorithmSHA256,
		ChecksumFormat:         ChecksumFormatSha256sum,
		EncodedFilenameCharset: EncodedFilenameCharsetRFC3986,
		KeySegmentMaxBytes:     KeySegmentMaxBytesDefault,
		S3MetaSuffix:           S3MetaSuffixDefault,
		S3CollisionStrategy:    S3CollisionStrategyLeafDataSuffix,
		DynamoDBLayout:         DynamoDBLayoutPerItem,
	}
}

// WriteManifest serialises m as pretty-printed JSON to w.
//
// Pretty-printing is deliberate — MANIFEST.json is operator-facing and is
// expected to be `cat`-ed and `jq`-ed during incident response.
func WriteManifest(w io.Writer, m Manifest) error {
	if err := m.validate(); err != nil {
		return err
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ") //nolint:mnd // 2-space indent matches `jq -.` default
	enc.SetEscapeHTML(false)
	if err := enc.Encode(m); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// ReadManifest decodes and validates a MANIFEST.json from r. The returned
// error is wrapped as ErrUnsupportedFormatVersion or ErrInvalidManifest so
// callers can branch on errors.Is.
func ReadManifest(r io.Reader) (Manifest, error) {
	var m Manifest
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields() // surface format drift loudly
	if err := dec.Decode(&m); err != nil {
		return Manifest{}, errors.Wrap(ErrInvalidManifest, err.Error())
	}
	if m.FormatVersion == 0 {
		return Manifest{}, errors.Wrapf(ErrUnsupportedFormatVersion,
			"format_version is zero")
	}
	if m.FormatVersion > CurrentFormatVersion {
		return Manifest{}, errors.Wrapf(ErrUnsupportedFormatVersion,
			"format_version %d > current %d (newer producer)", m.FormatVersion, CurrentFormatVersion)
	}
	if err := m.validate(); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

func (m Manifest) validate() error {
	if err := m.validateRequiredFields(); err != nil {
		return err
	}
	if err := m.validatePolicyFields(); err != nil {
		return err
	}
	return m.validatePhaseSpecific()
}

func (m Manifest) validateRequiredFields() error {
	if m.FormatVersion == 0 {
		return errors.Wrap(ErrInvalidManifest, "format_version is zero")
	}
	switch m.Phase {
	case PhasePhase0SnapshotDecode, PhasePhase1LivePinned:
	default:
		return errors.Wrapf(ErrInvalidManifest, "unknown phase %q", m.Phase)
	}
	if m.WallTimeISO == "" {
		return errors.Wrap(ErrInvalidManifest, "wall_time_iso missing")
	}
	if _, err := time.Parse(time.RFC3339Nano, m.WallTimeISO); err != nil {
		return errors.Wrapf(ErrInvalidManifest, "wall_time_iso unparseable: %v", err)
	}
	return nil
}

func (m Manifest) validatePolicyFields() error {
	if m.ChecksumAlgorithm == "" {
		return errors.Wrap(ErrInvalidManifest, "checksum_algorithm missing")
	}
	if m.ChecksumFormat == "" {
		return errors.Wrap(ErrInvalidManifest, "checksum_format missing")
	}
	if m.EncodedFilenameCharset == "" {
		return errors.Wrap(ErrInvalidManifest, "encoded_filename_charset missing")
	}
	if m.KeySegmentMaxBytes == 0 {
		return errors.Wrap(ErrInvalidManifest, "key_segment_max_bytes is zero")
	}
	if m.S3MetaSuffix == "" {
		return errors.Wrap(ErrInvalidManifest, "s3_meta_suffix missing")
	}
	if m.S3CollisionStrategy == "" {
		return errors.Wrap(ErrInvalidManifest, "s3_collision_strategy missing")
	}
	if m.DynamoDBLayout != DynamoDBLayoutPerItem && m.DynamoDBLayout != DynamoDBLayoutJSONL {
		return errors.Wrapf(ErrInvalidManifest, "dynamodb_layout %q unsupported", m.DynamoDBLayout)
	}
	return nil
}

func (m Manifest) validatePhaseSpecific() error {
	switch m.Phase {
	case PhasePhase0SnapshotDecode:
		if m.Live != nil {
			return errors.Wrap(ErrInvalidManifest, "phase0 must not set live")
		}
	case PhasePhase1LivePinned:
		if m.Source != nil {
			return errors.Wrap(ErrInvalidManifest, "phase1 must not set source")
		}
	}
	return nil
}
