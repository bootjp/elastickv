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
	ErrObjectConflict = errors.New("snapshot offload: object conflict")
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
	if err := verifyManifestSelfHash(manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func validateManifest(manifest Manifest) error {
	if err := validateManifestIdentity(manifest); err != nil {
		return err
	}
	if err := validateManifestPayload(manifest.Payload); err != nil {
		return err
	}
	if stringsTrim(manifest.ManifestKey) == "" {
		return errors.Wrap(ErrInvalidOptions, "manifest key is required")
	}
	if len(manifest.ConfState.Voters) == 0 {
		return errors.Wrap(ErrInvalidOptions, "manifest ConfState requires voters")
	}
	return validateManifestConfState(manifest.ConfState)
}

func validateManifestIdentity(manifest Manifest) error {
	switch {
	case manifest.SchemaVersion != ManifestSchemaVersion:
		return errors.Wrapf(ErrInvalidOptions, "unsupported manifest schema version %d", manifest.SchemaVersion)
	case manifest.GroupID == 0 && strings.TrimSpace(manifest.SourceCluster) == "":
		return errors.Wrap(ErrInvalidOptions, "source cluster is required for group 0 manifests")
	case manifest.SnapshotIndex == 0:
		return errors.Wrap(ErrInvalidOptions, "snapshot index must be > 0")
	case manifest.SnapshotTerm == 0:
		return errors.Wrap(ErrInvalidOptions, "snapshot term must be > 0")
	default:
		return nil
	}
}

func validateManifestPayload(payload PayloadDescriptor) error {
	switch {
	case payload.Key == "":
		return errors.Wrap(ErrInvalidOptions, "payload key is required")
	case payload.Bytes < 0:
		return errors.Wrap(ErrInvalidOptions, "payload byte count must be >= 0")
	case !isSHA256Hex(payload.SHA256):
		return errors.Wrap(ErrInvalidOptions, "payload sha256 must be 64 lowercase hex characters")
	default:
		return nil
	}
}

func validateManifestConfState(conf ManifestConfState) error {
	roles := []struct {
		name   string
		values []uint64
	}{
		{name: "voters", values: conf.Voters},
		{name: "learners", values: conf.Learners},
		{name: "voters_outgoing", values: conf.VotersOutgoing},
		{name: "learners_next", values: conf.LearnersNext},
	}
	for _, role := range roles {
		seen := make(map[uint64]struct{}, len(role.values))
		for _, id := range role.values {
			if id == 0 {
				return errors.Wrapf(ErrInvalidOptions, "conf_state.%s contains zero", role.name)
			}
			if _, ok := seen[id]; ok {
				return errors.Wrapf(ErrInvalidOptions, "conf_state.%s contains duplicate node %d", role.name, id)
			}
			seen[id] = struct{}{}
		}
	}
	return validateManifestConfStateMembership(conf)
}

func validateManifestConfStateMembership(conf ManifestConfState) error {
	learners := uint64Set(conf.Learners)
	outgoing := uint64Set(conf.VotersOutgoing)
	learnersNext := uint64Set(conf.LearnersNext)
	if err := validateManifestConfStateOverlaps(conf, learners, outgoing, learnersNext); err != nil {
		return err
	}
	for _, id := range conf.LearnersNext {
		if _, ok := outgoing[id]; !ok {
			return errors.Wrapf(ErrInvalidOptions, "conf_state.learners_next node %d is not an outgoing voter", id)
		}
	}
	if len(conf.VotersOutgoing) == 0 && conf.AutoLeave {
		return errors.Wrap(ErrInvalidOptions, "conf_state.auto_leave requires joint consensus")
	}
	return nil
}

func validateManifestConfStateOverlaps(
	conf ManifestConfState,
	learners, outgoing, learnersNext map[uint64]struct{},
) error {
	for _, id := range conf.Voters {
		if _, ok := learners[id]; ok {
			return invalidManifestConfStateOverlap(id, "voters", "learners")
		}
		if _, ok := learnersNext[id]; ok {
			return invalidManifestConfStateOverlap(id, "voters", "learners_next")
		}
	}
	for _, id := range conf.Learners {
		if _, ok := outgoing[id]; ok {
			return invalidManifestConfStateOverlap(id, "learners", "voters_outgoing")
		}
		if _, ok := learnersNext[id]; ok {
			return invalidManifestConfStateOverlap(id, "learners", "learners_next")
		}
	}
	return nil
}

func uint64Set(values []uint64) map[uint64]struct{} {
	set := make(map[uint64]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func invalidManifestConfStateOverlap(id uint64, first, second string) error {
	return errors.Wrapf(ErrInvalidOptions, "node %d appears in conf_state.%s and conf_state.%s", id, first, second)
}

func verifyManifestSelfHash(manifest Manifest) error {
	if !isSHA256Hex(manifest.ManifestSHA256) {
		return errors.Wrap(ErrInvalidOptions, "manifest sha256 must be 64 lowercase hex characters")
	}
	_, sum, err := manifest.MarshalCanonical()
	if err != nil {
		return err
	}
	if sum != manifest.ManifestSHA256 {
		return errors.Wrapf(ErrIntegrity, "manifest sha256 %s, expected %s", manifest.ManifestSHA256, sum)
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
