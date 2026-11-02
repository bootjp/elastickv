package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
)

const (
	bootstrapBaselineJSONDepth = 4
	bootstrapBaselineInspect   = "inspect"
)

func bootstrapBaselineCommand(command string) bool {
	return command == bootstrapBaselineInspect || command == "prepare" || command == "resume"
}

// Operator subcommands have a separate strict argument set from runtime startup.
type bootstrapBaselineCLIRequest struct {
	Command                                       string
	ExpectedConfigPath, ManifestPath, RaftDataDir string
	OperationID                                   raftbootstrap.OperationID
	MemberID                                      uint64
	Encryption                                    bootstrapOfflineEncryptionCLI
}

func parseBootstrapBaselineCLI(args []string) (bootstrapBaselineCLIRequest, error) {
	var request bootstrapBaselineCLIRequest
	if err := bootstrapBaselineArgsBound(args); err != nil {
		return request, err
	}
	if !bootstrapBaselineCommand(args[0]) {
		return request, errors.New("unsupported baseline command")
	}
	operation, member, err := parseBootstrapBaselineFlags(args[1:], &request)
	if err != nil {
		return bootstrapBaselineCLIRequest{}, err
	}
	request.OperationID, err = parseBootstrapBaselineOperationID(operation)
	if err != nil {
		return bootstrapBaselineCLIRequest{}, err
	}
	request.MemberID, err = bootstrapBaselineUint(member)
	if err != nil || request.MemberID == 0 {
		return bootstrapBaselineCLIRequest{}, errors.New("invalid baseline member ID")
	}
	request.Command = args[0]
	if err := validateBootstrapBaselineRequest(request); err != nil {
		return bootstrapBaselineCLIRequest{}, err
	}
	return request, nil
}

func parseBootstrapBaselineFlags(args []string, request *bootstrapBaselineCLIRequest) (operation, member string, err error) {
	fs := flag.NewFlagSet("baseline inspect", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	request.Encryption.flags(fs)
	values := map[string]*string{
		"expected-config": &request.ExpectedConfigPath, "manifest": &request.ManifestPath,
		"raft-data-dir": &request.RaftDataDir, "operation-id": &operation, "member-id": &member,
	}
	seen := map[string]bool{}
	for name, target := range values {
		fs.Func(name, "required offline inspection input", func(value string) error {
			if seen[name] {
				return errors.New("duplicate baseline option")
			}
			seen[name] = true
			*target = value
			return nil
		})
	}
	if err := fs.Parse(args); err != nil {
		return "", "", errors.New("invalid baseline options")
	}
	if fs.NArg() != 0 || len(seen) != len(values) {
		return "", "", errors.New("baseline command requires exactly five named inputs and no positional arguments")
	}
	return operation, member, nil
}

func bootstrapBaselineArgsBound(args []string) error {
	if len(args) == 0 || len(args) > raftbootstrap.MaxFileSize {
		return errors.New("missing or excessive baseline arguments")
	}
	remaining := raftbootstrap.MaxFileSize
	for _, arg := range args {
		if len(arg) > remaining {
			return errors.New("baseline arguments exceed 16 MiB")
		}
		remaining -= len(arg)
	}
	return nil
}

func parseBootstrapBaselineOperationID(value string) (raftbootstrap.OperationID, error) {
	var id raftbootstrap.OperationID
	if len(value) != hex.EncodedLen(len(id)) || strings.ToLower(value) != value {
		return id, errors.New("operation ID must be 32 lowercase hexadecimal digits")
	}
	b, err := hex.DecodeString(value)
	if err != nil {
		return id, errors.New("invalid operation ID")
	}
	copy(id[:], b)
	if id == (raftbootstrap.OperationID{}) {
		return id, errors.New("operation ID must be a fresh nonzero preparation ID")
	}
	return id, nil
}

func bootstrapBaselineUint(value string) (uint64, error) {
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil || strconv.FormatUint(n, 10) != value {
		return 0, errors.New("expected canonical unsigned decimal integer")
	}
	return n, nil
}

func validateBootstrapBaselineRequest(r bootstrapBaselineCLIRequest) error {
	if err := r.Encryption.validate(); err != nil {
		return err
	}
	if !bootstrapBaselineCommand(r.Command) || r.MemberID == 0 || r.OperationID == (raftbootstrap.OperationID{}) {
		return errors.New("invalid baseline inspection request")
	}
	paths := []string{r.ExpectedConfigPath, r.ManifestPath, r.RaftDataDir}
	paths = append(paths, r.Encryption.paths()...)
	seen := map[string]bool{}
	for _, path := range paths {
		if !bootstrapBaselineValidPath(path) || seen[path] {
			return errors.New("baseline paths must be distinct canonical absolute paths")
		}
		seen[path] = true
	}
	return nil
}

func bootstrapBaselineValidPath(path string) bool {
	return utf8.ValidString(path) && filepath.IsAbs(path) && filepath.Clean(path) == path &&
		path != string(filepath.Separator) && !strings.ContainsAny(path, "\x00\r\n\t")
}

// This is an independent operator input, not a context extracted from the
// manifest being checked. Identity/cut integers are canonical decimal strings,
// including group zero and watermark zero, so omitted zero fields cannot pass.
// Version and minimum format versions are JSON numbers. Public keys are SPKI
// SHA-256 pins, not certificate/private-key paths; inspection needs no signer.
type bootstrapBaselineExpectedJSON struct {
	Version            uint32                        `json:"version"`
	Cluster            string                        `json:"cluster_uuid"`
	Group              string                        `json:"group_id"`
	ConfigurationIndex string                        `json:"configuration_index"`
	AutoLeave          bool                          `json:"auto_leave"`
	Minimum            bootstrapBaselineMinimumJSON  `json:"minimum"`
	CommitIndex        string                        `json:"commit_index"`
	CommitTerm         string                        `json:"commit_term"`
	Watermark          string                        `json:"watermark"`
	DataDigest         string                        `json:"data_sha256"`
	Members            []bootstrapBaselineMemberJSON `json:"members"`
}

type bootstrapBaselineMinimumJSON struct {
	Entry    uint32 `json:"entry"`
	Snapshot uint32 `json:"snapshot"`
	Session  uint32 `json:"session"`
}

type bootstrapBaselineMemberJSON struct {
	ID      string   `json:"member_id"`
	Address string   `json:"address"`
	Key     string   `json:"public_key_sha256"`
	Roles   []string `json:"roles"`
}

func parseBootstrapBaselineExpectedConfig(data []byte, operation raftbootstrap.OperationID) (raftbootstrap.BaselineContext, error) {
	var empty raftbootstrap.BaselineContext
	if len(data) == 0 || len(data) > raftbootstrap.MaxFileSize || !utf8.Valid(data) {
		return empty, errors.New("invalid baseline expected config size/encoding")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	if err := scanBootstrapBaselineJSON(d, 0); err != nil {
		return empty, err
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) {
		return empty, errors.New("trailing baseline config data")
	}
	d = json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	var raw bootstrapBaselineExpectedJSON
	if err := d.Decode(&raw); err != nil {
		return empty, errors.New("invalid baseline expected config schema")
	}
	return buildBootstrapBaselineExpected(raw, operation)
}

func scanBootstrapBaselineJSON(d *json.Decoder, depth int) error {
	if depth > bootstrapBaselineJSONDepth {
		return errors.New("baseline config nesting too deep")
	}
	token, err := d.Token()
	if err != nil || token == nil {
		return errors.New("invalid baseline JSON token")
	}
	delim, container := token.(json.Delim)
	if !container {
		return nil
	}
	seen := map[string]bool{}
	for d.More() {
		if delim == '{' {
			if err := readBootstrapBaselineJSONKey(d, seen); err != nil {
				return err
			}
		}
		if err := scanBootstrapBaselineJSON(d, depth+1); err != nil {
			return err
		}
	}
	_, err = d.Token()
	return errors.Wrap(err, "close baseline JSON container")
}

func readBootstrapBaselineJSONKey(d *json.Decoder, seen map[string]bool) error {
	key, err := d.Token()
	if err != nil {
		return errors.New("invalid baseline JSON key")
	}
	name, ok := key.(string)
	if !ok || seen[name] || !bootstrapBaselineJSONKey(name) {
		return errors.New("unknown or duplicate baseline config field")
	}
	seen[name] = true
	return nil
}

func bootstrapBaselineJSONKey(key string) bool {
	switch key {
	case "version", "cluster_uuid", "group_id", "configuration_index", "minimum", "entry", "snapshot", "session",
		"commit_index", "commit_term", "watermark", "data_sha256", "members", bootstrapMemberIDJSONField, "address", "public_key_sha256", "roles", "auto_leave":
		return true
	default:
		return false
	}
}

func buildBootstrapBaselineExpected(raw bootstrapBaselineExpectedJSON, operation raftbootstrap.OperationID) (raftbootstrap.BaselineContext, error) {
	c := raftbootstrap.BaselineContext{Version: raw.Version, OperationID: operation,
		Minimum: raftbootstrap.Versions{Entry: raw.Minimum.Entry, Snapshot: raw.Minimum.Snapshot, Session: raw.Minimum.Session}}
	c.Configuration.State.AutoLeave = raw.AutoLeave
	var err error
	c.Identity.Cluster, err = raftbootstrap.ParseUUID(raw.Cluster)
	if err != nil {
		return raftbootstrap.BaselineContext{}, errors.Wrap(err, "parse expected cluster UUID")
	}
	// Equal numeric values still need independent validation and assignment.
	numbers := []struct {
		text   string
		target *uint64
	}{
		{raw.Group, &c.Identity.Group},
		{raw.ConfigurationIndex, &c.Configuration.Index}, {raw.CommitIndex, &c.CommitIndex},
		{raw.CommitTerm, &c.CommitTerm}, {raw.Watermark, &c.Watermark},
	}
	for _, n := range numbers {
		*n.target, err = bootstrapBaselineUint(n.text)
		if err != nil {
			return raftbootstrap.BaselineContext{}, err
		}
	}
	c.DataDigest, err = bootstrapBaselineDigest(raw.DataDigest)
	if err != nil {
		return raftbootstrap.BaselineContext{}, err
	}
	if !bootstrapBaselineValidExpectedCut(c) {
		return raftbootstrap.BaselineContext{}, errors.New("invalid expected baseline cut")
	}
	for _, member := range raw.Members {
		if err := appendBootstrapBaselineMember(&c.Configuration, member); err != nil {
			return raftbootstrap.BaselineContext{}, err
		}
	}
	// Reuse the protocol's complete role/roster validation and canonicalization.
	b, err := raftbootstrap.MarshalIntent(raftbootstrap.Intent{Identity: c.Identity, BaselineKind: raftbootstrap.Accepted, Baseline: c.Configuration, Minimum: c.Minimum})
	if err != nil {
		return raftbootstrap.BaselineContext{}, errors.Wrap(err, "validate expected roster")
	}
	intent, err := raftbootstrap.ParseIntent(b)
	if err != nil {
		return raftbootstrap.BaselineContext{}, errors.Wrap(err, "canonicalize expected roster")
	}
	c.Configuration = intent.Baseline
	return c, nil
}

func bootstrapBaselineValidExpectedCut(c raftbootstrap.BaselineContext) bool {
	return c.Version == raftbootstrap.BaselineVersion && c.OperationID != (raftbootstrap.OperationID{}) &&
		c.CommitIndex != 0 && c.CommitTerm != 0 && c.Configuration.Index <= c.CommitIndex && c.Watermark != ^uint64(0) && !c.Configuration.State.AutoLeave
}

func bootstrapBaselineDigest(value string) (raftbootstrap.Digest, error) {
	var digest raftbootstrap.Digest
	if len(value) != hex.EncodedLen(len(digest)) || strings.ToLower(value) != value {
		return digest, errors.New("expected lowercase SHA-256 digest")
	}
	b, err := hex.DecodeString(value)
	if err != nil {
		return digest, errors.New("invalid SHA-256 digest")
	}
	copy(digest[:], b)
	if digest == (raftbootstrap.Digest{}) {
		return digest, errors.New("zero SHA-256 digest")
	}
	return digest, nil
}

func appendBootstrapBaselineMember(c *raftbootstrap.Configuration, raw bootstrapBaselineMemberJSON) error {
	id, err := bootstrapBaselineUint(raw.ID)
	if err != nil || id == 0 {
		return errors.New("invalid roster member ID")
	}
	key, err := bootstrapBaselineDigest(raw.Key)
	if err != nil {
		return err
	}
	if len(raw.Roles) == 0 {
		return errors.New("missing roster member roles")
	}
	seen := map[string]bool{}
	for _, role := range raw.Roles {
		if seen[role] {
			return errors.New("duplicate roster role")
		}
		seen[role] = true
		if err := appendBootstrapBaselineRole(c, role, id); err != nil {
			return err
		}
	}
	c.Members = append(c.Members, raftbootstrap.Member{ID: id, Address: raw.Address, PublicKeyDigest: key})
	return nil
}

func appendBootstrapBaselineRole(c *raftbootstrap.Configuration, role string, id uint64) error {
	switch role {
	case "voter":
		c.State.Voters = append(c.State.Voters, id)
	case "outgoing":
		c.State.VotersOutgoing = append(c.State.VotersOutgoing, id)
	case "learner":
		c.State.Learners = append(c.State.Learners, id)
	case "next":
		c.State.LearnersNext = append(c.State.LearnersNext, id)
	default:
		return errors.New("unknown roster role")
	}
	return nil
}

// A successful summary means only independent-context signature verification.
// It neither opens native state nor proves quiescence or historical correctness.
type bootstrapBaselineDryRunSummary struct {
	Status              string `json:"status"`
	ClusterUUID         string `json:"cluster_uuid"`
	GroupID             uint64 `json:"group_id"`
	OperationID         string `json:"operation_id"`
	MemberID            uint64 `json:"member_id"`
	MemberCount         int    `json:"member_count"`
	CommitIndex         uint64 `json:"commit_index"`
	CommitTerm          uint64 `json:"commit_term"`
	Watermark           uint64 `json:"watermark"`
	ManifestDigest      string `json:"manifest_sha256"`
	ConfigurationIndex  uint64 `json:"configuration_index"`
	ConfigurationDigest string `json:"configuration_sha256"`
	DataDigest          string `json:"data_sha256"`
	NativeStateVerified bool   `json:"native_state_verified"`
	Prepared            bool   `json:"prepared"`
	Activated           bool   `json:"activated"`
	Mutated             bool   `json:"mutated"`
}

// The parent supplies bounded bytes read from the two explicit request paths;
// it must not synthesize expectedConfig from the untrusted manifest. Path checks
// here are lexical only: no symlink resolution, locks or filesystem assertions.
func bootstrapBaselineDryRun(request bootstrapBaselineCLIRequest, expectedConfig, manifest []byte) (bootstrapBaselineDryRunSummary, error) {
	var empty bootstrapBaselineDryRunSummary
	if err := validateBootstrapBaselineRequest(request); err != nil {
		return empty, err
	}
	if len(expectedConfig) > raftbootstrap.MaxFileSize || len(manifest) > raftbootstrap.MaxFileSize-len(expectedConfig) {
		return empty, errors.New("baseline dry-run inputs exceed 16 MiB aggregate")
	}
	expected, err := parseBootstrapBaselineExpectedConfig(expectedConfig, request.OperationID)
	if err != nil {
		return empty, err
	}
	m, err := raftbootstrap.ParseBaselineManifest(manifest)
	if err != nil {
		return empty, errors.Wrap(err, "parse baseline manifest")
	}
	if err := raftbootstrap.VerifyBaselineManifest(m, expected); err != nil {
		return empty, errors.Wrap(err, "verify independent baseline expectation")
	}
	if !bootstrapBaselineHasMember(expected.Configuration, request.MemberID) {
		return empty, errors.New("inspection target is not in pinned roster")
	}
	return summarizeBootstrapBaseline(request, m)
}

func summarizeBootstrapBaseline(request bootstrapBaselineCLIRequest, m raftbootstrap.BaselineManifest) (bootstrapBaselineDryRunSummary, error) {
	digest, err := raftbootstrap.BaselineManifestDigest(m)
	if err != nil {
		return bootstrapBaselineDryRunSummary{}, errors.Wrap(err, "digest baseline manifest")
	}
	configurationDigest, err := raftbootstrap.ConfigurationDigest(m.Configuration)
	if err != nil {
		return bootstrapBaselineDryRunSummary{}, errors.Wrap(err, "digest baseline configuration")
	}
	cluster := m.Identity.Cluster
	return bootstrapBaselineDryRunSummary{Status: "signatures-verified-dry-run-only", OperationID: hex.EncodeToString(request.OperationID[:]),
		ClusterUUID: fmt.Sprintf("%x-%x-%x-%x-%x", cluster[:4], cluster[4:6], cluster[6:8], cluster[8:10], cluster[10:]), GroupID: m.Identity.Group,
		MemberID: request.MemberID, MemberCount: len(m.Members), CommitIndex: m.CommitIndex, CommitTerm: m.CommitTerm,
		Watermark: m.Watermark, ManifestDigest: hex.EncodeToString(digest[:]), ConfigurationIndex: m.Configuration.Index,
		ConfigurationDigest: hex.EncodeToString(configurationDigest[:]), DataDigest: hex.EncodeToString(m.DataDigest[:])}, nil
}

func bootstrapBaselineHasMember(c raftbootstrap.Configuration, id uint64) bool {
	for _, member := range c.Members {
		if member.ID == id {
			return true
		}
	}
	return false
}
