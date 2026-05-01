package backup

import (
	"bytes"
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

// Adapters lists which scopes were dumped per adapter. The pointer
// values express two distinguishable on-disk states:
//
//   - nil   -> the adapter was excluded from this dump (e.g.
//     `--adapter dynamodb,s3` filtered it out). The corresponding
//     JSON key is absent.
//   - non-nil pointer to Adapter{}  -> the adapter was in scope but
//     no scopes for it were emitted (no tables, no buckets, etc.).
//     The JSON key is present with an empty object.
//   - non-nil pointer to a populated Adapter -> the listed scopes
//     were emitted.
//
// Storing pointers (rather than zero-value Adapter structs) is what
// keeps "excluded by filter" distinguishable from "included but
// empty" through json.Marshal — non-pointer fields would collapse
// both states into the same on-disk shape.
type Adapters struct {
	DynamoDB *Adapter `json:"dynamodb,omitempty"`
	S3       *Adapter `json:"s3,omitempty"`
	Redis    *Adapter `json:"redis,omitempty"`
	SQS      *Adapter `json:"sqs,omitempty"`
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
	FormatVersion    uint32  `json:"format_version"`
	Phase            string  `json:"phase"`
	ElastickvVersion string  `json:"elastickv_version,omitempty"`
	ClusterID        string  `json:"cluster_id,omitempty"`
	SnapshotIndex    uint64  `json:"snapshot_index,omitempty"`
	LastCommitTS     uint64  `json:"last_commit_ts,omitempty"`
	WallTimeISO      string  `json:"wall_time_iso"`
	Source           *Source `json:"source,omitempty"`
	Live             *Live   `json:"live,omitempty"`
	// Adapters and Exclusions are pointer types so ReadManifest can
	// distinguish "section omitted entirely" (a corrupted or
	// truncated dump that should fail validation) from "section
	// present but populated with default values" (legitimate
	// scope-everything-excluded). Codex P2 #146 (round 3).
	Adapters          *Adapters   `json:"adapters"`
	Exclusions        *Exclusions `json:"exclusions"`
	ChecksumAlgorithm string      `json:"checksum_algorithm"`
	ChecksumFormat    string      `json:"checksum_format"`

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
// passing it to WriteManifest. Adapters and Exclusions are seeded to
// non-nil zero values so the resulting manifest passes the
// "section-present" validation; callers populating individual scopes
// reach in via the now-non-nil pointer.
func NewPhase0SnapshotManifest(now time.Time) Manifest {
	return Manifest{
		FormatVersion:          CurrentFormatVersion,
		Phase:                  PhasePhase0SnapshotDecode,
		WallTimeISO:            now.UTC().Format(time.RFC3339Nano),
		Adapters:               &Adapters{},
		Exclusions:             &Exclusions{},
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
	// Read the entire payload once so we can pre-decode just the
	// format_version before strict struct decoding. Without this
	// two-phase approach, a manifest produced by a newer major version
	// that also changed the JSON type of a known field (e.g. `phase`
	// switched from string to int) would surface as
	// ErrInvalidManifest instead of ErrUnsupportedFormatVersion,
	// breaking the documented version-branching contract for callers
	// that key off errors.Is(err, ErrUnsupportedFormatVersion). See
	// Codex P2, round 5.
	payload, err := io.ReadAll(r)
	if err != nil {
		return Manifest{}, errors.Wrap(ErrInvalidManifest, err.Error())
	}
	if err := probeManifestFormatVersion(payload); err != nil {
		return Manifest{}, err
	}
	// Phase 2: strict struct decode on a known-supported version.
	var m Manifest
	dec := json.NewDecoder(bytes.NewReader(payload))
	// We intentionally do NOT call DisallowUnknownFields here.
	// The format-version contract (Codex P1, follow-up) is:
	//   - format_version > CurrentFormatVersion -> hard refuse
	//     (the major break signal)
	//   - format_version == CurrentFormatVersion AND extra unknown
	//     fields appear -> a newer minor version added them; the
	//     older reader silently ignores. That's the documented
	//     same-major minor-evolution path.
	// Rejecting unknown fields outright would turn every minor
	// optional-field addition into a hard read failure during
	// mixed-version operation.
	if err := dec.Decode(&m); err != nil {
		return Manifest{}, errors.Wrap(ErrInvalidManifest, err.Error())
	}
	// MANIFEST.json is exactly one JSON object. Trailing bytes
	// (a second object, junk, even whitespace-only padding) point at
	// concatenation bugs or partial-write corruption — both of which
	// must surface here rather than be silently discarded. We use
	// io.Discard rather than parsing because we only care that
	// nothing-decodable is present; structural validation lives in
	// validate().
	if dec.More() {
		return Manifest{}, errors.Wrap(ErrInvalidManifest,
			"trailing bytes after manifest JSON object")
	}
	if err := validateExclusionsFieldsPresent(payload); err != nil {
		return Manifest{}, err
	}
	if err := m.validate(); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

// probeManifestFormatVersion runs the relaxed-shape format_version
// gate that ReadManifest applies before the strict struct decode.
// Splitting it into its own function keeps ReadManifest under the
// project's cyclomatic-complexity ceiling. The contract:
//
//   - missing or null `format_version` -> ErrInvalidManifest
//     (truncated/malformed file; Codex P2 round 8). Without this
//     branch json.Unmarshal would collapse absence to zero and the
//     version gate would misclassify as upgrade-required.
//   - `format_version` = 0 -> ErrUnsupportedFormatVersion (the
//     reserved sentinel for "no version assigned").
//   - `format_version` > CurrentFormatVersion ->
//     ErrUnsupportedFormatVersion (newer producer; upgrade-required).
//   - within range -> nil; the strict struct decode runs next.
func probeManifestFormatVersion(payload []byte) error {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(payload, &top); err != nil {
		return errors.Wrap(ErrInvalidManifest, err.Error())
	}
	rawFV, hasFV := top["format_version"]
	if !hasFV {
		return errors.Wrap(ErrInvalidManifest, "format_version missing")
	}
	if bytes.Equal(rawFV, jsonNullLiteral) {
		return errors.Wrap(ErrInvalidManifest, "format_version is null")
	}
	var probe struct {
		FormatVersion uint32 `json:"format_version"`
	}
	if err := json.Unmarshal(payload, &probe); err != nil {
		return errors.Wrap(ErrInvalidManifest, err.Error())
	}
	if probe.FormatVersion == 0 {
		return errors.Wrap(ErrUnsupportedFormatVersion, "format_version is zero")
	}
	if probe.FormatVersion > CurrentFormatVersion {
		return errors.Wrapf(ErrUnsupportedFormatVersion,
			"format_version %d > current %d (newer producer)", probe.FormatVersion, CurrentFormatVersion)
	}
	return nil
}

// validateExclusionsFieldsPresent rejects manifests whose `exclusions`
// section omits any of the required boolean flags. Go's
// json.Unmarshal silently fills missing booleans with `false`, so a
// truncated or partially-corrupted manifest would otherwise pass with
// altered exclusion semantics — losing the producer-side provenance
// the section is meant to capture (Codex P2 round 7). Each flag must
// be present and not the JSON `null` literal; type validation already
// runs as part of the strict struct decode.
func validateExclusionsFieldsPresent(payload []byte) error {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(payload, &top); err != nil {
		return errors.Wrap(ErrInvalidManifest, err.Error())
	}
	rawExcl, ok := top["exclusions"]
	if !ok {
		// validateRequiredFields surfaces the absent-section error
		// with a clearer message; defer to it.
		return nil
	}
	var excl map[string]json.RawMessage
	if err := json.Unmarshal(rawExcl, &excl); err != nil {
		return errors.Wrap(ErrInvalidManifest, err.Error())
	}
	for _, name := range exclusionsRequiredFields {
		raw, present := excl[name]
		if !present {
			return errors.Wrapf(ErrInvalidManifest,
				"exclusions.%s missing (cannot infer producer-side default)", name)
		}
		if bytes.Equal(raw, jsonNullLiteral) {
			return errors.Wrapf(ErrInvalidManifest,
				"exclusions.%s is null", name)
		}
	}
	return nil
}

// exclusionsRequiredFields lists the JSON tag names of every
// Exclusions field that must be explicitly present in the manifest.
// Kept in sync with the struct definition above; a missing entry
// here would silently re-introduce the omitted-flag bug.
var exclusionsRequiredFields = [...]string{
	"include_incomplete_uploads",
	"include_orphans",
	"preserve_sqs_visibility",
	"include_sqs_side_records",
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
	// WriteManifest must refuse manifests advertising a version this
	// build cannot produce — without this gate, a caller mutating
	// `m.FormatVersion = CurrentFormatVersion + 1` would write a
	// manifest that ReadManifest in the same package then rejects as
	// ErrUnsupportedFormatVersion, producing self-incompatible
	// backup metadata. Codex P2 round 8.
	if m.FormatVersion > CurrentFormatVersion {
		return errors.Wrapf(ErrInvalidManifest,
			"format_version %d > current %d (this build cannot produce that)", m.FormatVersion, CurrentFormatVersion)
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
	// Adapters and Exclusions are required structural sections.
	// A manifest that omits either is treated as truncated/corrupted
	// (Codex P2 #146 round 3).
	if m.Adapters == nil {
		return errors.Wrap(ErrInvalidManifest, "adapters section missing")
	}
	if m.Exclusions == nil {
		return errors.Wrap(ErrInvalidManifest, "exclusions section missing")
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
		// A phase1 dump's whole point is the cluster-wide read_ts
		// pin recorded under Live. A manifest that omits Live cannot
		// describe its consistency point and downstream restore /
		// audit logic must not silently accept it as valid (Codex
		// P1 #295).
		if m.Live == nil {
			return errors.Wrap(ErrInvalidManifest, "phase1 must set live")
		}
		if m.Live.ReadTS == 0 {
			return errors.Wrap(ErrInvalidManifest, "phase1 live.read_ts must be non-zero")
		}
	}
	return nil
}
