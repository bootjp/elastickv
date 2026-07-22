package snapshotoffload

import (
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

const (
	ManifestSchemaVersion = 1
	payloadObjectSuffix   = ".fsm"
	manifestObjectSuffix  = ".json"
)

var (
	ErrInvalidOptions = errors.New("snapshot offload: invalid options")
	ErrIntegrity      = errors.New("snapshot offload: integrity check failed")
	ErrObjectNotFound = errors.New("snapshot offload: object not found")
)

type Manifest struct {
	SchemaVersion  int               `json:"schema_version"`
	CreatedAt      time.Time         `json:"created_at"`
	SourceCluster  string            `json:"source_cluster,omitempty"`
	GroupID        uint64            `json:"group_id"`
	SnapshotIndex  uint64            `json:"snapshot_index"`
	SnapshotTerm   uint64            `json:"snapshot_term"`
	ConfState      ManifestConfState `json:"conf_state"`
	Payload        PayloadDescriptor `json:"payload"`
	BinaryVersion  string            `json:"binary_version,omitempty"`
	ManifestKey    string            `json:"manifest_key"`
	ManifestSHA256 string            `json:"manifest_sha256,omitempty"`
}

type ManifestConfState struct {
	Voters         []uint64 `json:"voters,omitempty"`
	Learners       []uint64 `json:"learners,omitempty"`
	VotersOutgoing []uint64 `json:"voters_outgoing,omitempty"`
	LearnersNext   []uint64 `json:"learners_next,omitempty"`
	AutoLeave      bool     `json:"auto_leave,omitempty"`
}

type PayloadDescriptor struct {
	Key          string `json:"key"`
	Bytes        int64  `json:"bytes"`
	SHA256       string `json:"sha256"`
	SourceCRC32C uint32 `json:"source_crc32c"`
}

func (m Manifest) MarshalCanonical() ([]byte, string, error) {
	m.ManifestSHA256 = ""
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, "", errors.WithStack(err)
	}
	sum := hexSHA256Bytes(data)
	m.ManifestSHA256 = sum
	final, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, "", errors.WithStack(err)
	}
	return append(final, '\n'), sum, nil
}

func DecodeManifest(data []byte) (Manifest, error) {
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, errors.WithStack(err)
	}
	if err := validateManifest(manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func validateManifest(manifest Manifest) error {
	switch {
	case manifest.SchemaVersion != ManifestSchemaVersion:
		return errors.Wrapf(ErrInvalidOptions, "unsupported manifest schema version %d", manifest.SchemaVersion)
	case manifest.GroupID == 0 && strings.TrimSpace(manifest.SourceCluster) == "":
		return errors.Wrap(ErrInvalidOptions, "source cluster is required for group 0 manifests")
	case manifest.SnapshotIndex == 0:
		return errors.Wrap(ErrInvalidOptions, "snapshot index must be > 0")
	case manifest.SnapshotTerm == 0:
		return errors.Wrap(ErrInvalidOptions, "snapshot term must be > 0")
	case manifest.Payload.Key == "":
		return errors.Wrap(ErrInvalidOptions, "payload key is required")
	case manifest.Payload.Bytes < 0:
		return errors.Wrap(ErrInvalidOptions, "payload byte count must be >= 0")
	case !isSHA256Hex(manifest.Payload.SHA256):
		return errors.Wrap(ErrInvalidOptions, "payload sha256 must be 64 lowercase hex characters")
	case manifest.ManifestKey == "":
		return errors.Wrap(ErrInvalidOptions, "manifest key is required")
	}
	return nil
}

func payloadKey(prefix, sha256Hex string) (string, error) {
	if !isSHA256Hex(sha256Hex) {
		return "", errors.Wrap(ErrInvalidOptions, "payload sha256 must be 64 lowercase hex characters")
	}
	prefix = cleanObjectPrefix(prefix)
	return path.Join(prefix, "v1", "payloads", "sha256", sha256Hex[:2], sha256Hex+payloadObjectSuffix), nil
}

func manifestKey(prefix string, groupID, index, term uint64) (string, error) {
	if index == 0 || term == 0 {
		return "", errors.Wrap(ErrInvalidOptions, "snapshot index and term must be > 0")
	}
	prefix = cleanObjectPrefix(prefix)
	return path.Join(prefix, "v1", "groups", fmt.Sprintf("%d", groupID), "snapshots",
		fmt.Sprintf("%020d-%020d%s", index, term, manifestObjectSuffix)), nil
}

func cleanObjectPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return "."
	}
	return path.Clean(prefix)
}

func manifestConfState(confState *raftpb.ConfState) ManifestConfState {
	if confState == nil {
		return ManifestConfState{}
	}
	return ManifestConfState{
		Voters:         append([]uint64(nil), confState.GetVoters()...),
		Learners:       append([]uint64(nil), confState.GetLearners()...),
		VotersOutgoing: append([]uint64(nil), confState.GetVotersOutgoing()...),
		LearnersNext:   append([]uint64(nil), confState.GetLearnersNext()...),
		AutoLeave:      confState.GetAutoLeave(),
	}
}
