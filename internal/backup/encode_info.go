package backup

import (
	"encoding/json"
	"io"
	"time"

	"github.com/cockroachdb/errors"
)

// EncodeInfoFormatVersion is the on-disk schema version for ENCODE_INFO.json.
// Bumped on incompatible schema changes; ReadEncodeInfo rejects unknown
// versions with ErrUnsupportedEncodeInfoFormatVersion so a future encoder
// release cannot silently drop fields a current operator relies on.
const EncodeInfoFormatVersion uint32 = 1

// ErrUnsupportedEncodeInfoFormatVersion is returned by ReadEncodeInfo when
// the sidecar's format_version is not EncodeInfoFormatVersion. Mirrors
// the decoder's ErrUnsupportedFormatVersion contract so callers can branch
// on errors.Is.
var ErrUnsupportedEncodeInfoFormatVersion = errors.New("backup: unsupported ENCODE_INFO format_version")

// EncodeInfoSelfTest captures the self-test outcome (parent §"Round-trip
// self-test"). Ran=false when --self-test was off; Matched is only
// meaningful when Ran=true.
type EncodeInfoSelfTest struct {
	Ran     bool `json:"ran"`
	Matched bool `json:"matched"`
}

// EncodeInfo is the on-disk shape of <output>.encode_info.json. Schema
// pinned by docs/design/2026_06_01_proposed_snapshot_encode_cli.md
// §"ENCODE_INFO.json". Restore operators rely on this for "encoded for
// the right cluster, by the right encoder version, against this exact
// file" confirmation; tag changes are a breaking schema bump.
type EncodeInfo struct {
	FormatVersion           uint32             `json:"format_version"`
	EncoderVersion          string             `json:"encoder_version"`
	EncoderKeyFormatVersion uint32             `json:"encoder_key_format_version"`
	WallTimeISO             string             `json:"wall_time_iso"`
	InputRoot               string             `json:"input_root"`
	OutputFSMPath           string             `json:"output_fsm_path"`
	OutputFSMSHA256         string             `json:"output_fsm_sha256"`
	LastCommitTS            uint64             `json:"last_commit_ts"`
	LastCommitTSOverridden  bool               `json:"last_commit_ts_overridden"`
	ManifestLastCommitTS    uint64             `json:"manifest_last_commit_ts"`
	ManifestClusterID       string             `json:"manifest_cluster_id,omitempty"`
	AdaptersEnabled         []string           `json:"adapters_enabled"`
	SelfTest                EncodeInfoSelfTest `json:"self_test"`
}

// NewEncodeInfo stamps the current format version + wall time so callers
// only fill in the encode-specific fields. Mirrors NewPhase0SnapshotManifest.
// EncoderKeyFormatVersion is the on-disk key format the encoder produces;
// today it tracks CurrentFormatVersion (no separate key-format version
// has been declared), which is conservative: future encoder bumps that
// change MVCC layout MUST bump both manifest and encoder-key formats so
// restore operators can correlate.
func NewEncodeInfo(now time.Time) EncodeInfo {
	return EncodeInfo{
		FormatVersion:           EncodeInfoFormatVersion,
		EncoderKeyFormatVersion: CurrentFormatVersion,
		WallTimeISO:             now.UTC().Format(time.RFC3339Nano),
	}
}

// WriteEncodeInfo serializes info to w. Caller is responsible for the
// fsync+close discipline (the cmd wrapper uses os.File.Sync then Close
// to surface late writeback errors — gemini r1 medium on #810).
func WriteEncodeInfo(w io.Writer, info EncodeInfo) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(info); err != nil {
		return errors.Wrap(err, "encode ENCODE_INFO.json")
	}
	return nil
}

// ReadEncodeInfo parses an ENCODE_INFO.json payload from r. Rejects
// unknown format_version values with ErrUnsupportedEncodeInfoFormatVersion
// so a future schema bump surfaces as a typed error rather than a silent
// field drop. Unknown JSON fields are tolerated to allow forward-compat
// additions within the same format_version.
func ReadEncodeInfo(r io.Reader) (EncodeInfo, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return EncodeInfo{}, errors.Wrap(err, "read ENCODE_INFO.json")
	}
	var probe struct {
		FormatVersion uint32 `json:"format_version"`
	}
	if err := json.Unmarshal(body, &probe); err != nil {
		return EncodeInfo{}, errors.Wrap(err, "decode ENCODE_INFO.json format_version")
	}
	if probe.FormatVersion != EncodeInfoFormatVersion {
		return EncodeInfo{}, errors.Wrapf(ErrUnsupportedEncodeInfoFormatVersion, "got %d, want %d", probe.FormatVersion, EncodeInfoFormatVersion)
	}
	var info EncodeInfo
	if err := json.Unmarshal(body, &info); err != nil {
		return EncodeInfo{}, errors.Wrap(err, "decode ENCODE_INFO.json")
	}
	return info, nil
}

// EncodeInfoSidecarPath returns the path-derived sidecar location for a
// given .fsm output path. Multiple .fsm files can share a directory
// (e.g., per-node dumps under /backups/); a static "ENCODE_INFO.json"
// name would silently overwrite siblings (gemini medium #896).
//
// Convention: append ".encode_info.json" to the full output path. The
// same scheme gpg and sha256sum follow when their input is path-addressable.
func EncodeInfoSidecarPath(fsmPath string) string {
	return fsmPath + ".encode_info.json"
}
