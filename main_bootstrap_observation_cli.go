package main

import (
	"bytes"
	"context"
	"crypto"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/cockroachdb/errors"
)

const (
	bootstrapObservationProbe         = "probe"
	bootstrapObservationManifest      = "manifest"
	bootstrapObservationOperationFlag = "operation-id"
	bootstrapObservationLearner       = "learner"
)

type bootstrapObservationCommand struct {
	Command, Config, PeerConfig, DataDir, Output string
	OperationID                                  raftbootstrap.OperationID
	MemberID                                     uint64
	Probes                                       []string
	Encryption                                   bootstrapOfflineEncryptionCLI
}

// Pins deliberately contain no configuration epoch, committed cut or data hash.
type bootstrapObservationPinsJSON struct {
	Version   uint32                        `json:"version"`
	Cluster   string                        `json:"cluster_uuid"`
	Group     string                        `json:"group_id"`
	AutoLeave bool                          `json:"auto_leave"`
	Minimum   bootstrapBaselineMinimumJSON  `json:"minimum"`
	Members   []bootstrapBaselineMemberJSON `json:"members"`
}

func dispatchBootstrapObservationCommand(args []string, output io.Writer) (bool, error) {
	if len(args) == 0 || (args[0] != bootstrapObservationProbe && args[0] != bootstrapObservationManifest) {
		return false, nil
	}
	r, err := parseBootstrapObservationCommand(args)
	if err != nil {
		return true, err
	}
	budget := bootstrapArtifactBudget(raftbootstrap.MaxFileSize)
	raw, err := budget.read(r.Config)
	if err != nil {
		return true, err
	}
	if r.Command == bootstrapObservationManifest {
		return true, assembleBootstrapObservation(r, raw, &budget)
	}
	return true, executeBootstrapObservation(r, raw, &budget, output)
}

func dispatchBootstrapObservationOrArtifact(args []string, output io.Writer) (bool, error) {
	if handled, err := dispatchBootstrapObservationCommand(args, output); handled {
		return true, err
	}
	return dispatchBootstrapArtifactCommand(args, output)
}

func executeBootstrapObservation(r bootstrapObservationCommand, raw []byte, budget *bootstrapArtifactBudget, output io.Writer) error {
	if output == nil {
		return errors.New("probe requires measured configuration output")
	}
	pins, err := parseBootstrapObservationPins(raw)
	if err != nil {
		return err
	}
	config, err := readBootstrapArtifactPeer(r.PeerConfig, budget)
	if err != nil {
		return err
	}
	if err := bootstrapObservationOutputOutsideSource(r); err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	probe, err := observeBootstrapOriginal(ctx, r, pins, config)
	if err != nil {
		return err
	}
	return writeBootstrapObservedProbe(r.Output, probe, output)
}

func bootstrapObservationOutputOutsideSource(r bootstrapObservationCommand) error {
	source, err := filepath.EvalSymlinks(r.DataDir)
	if err != nil {
		return errors.WithStack(err)
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(r.Output))
	if err != nil {
		return errors.WithStack(err)
	}
	relative, err := filepath.Rel(source, parent)
	if err != nil {
		return errors.WithStack(err)
	}
	if relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return errors.New("probe output must be outside the observed data directory")
	}
	return nil
}

func writeBootstrapObservedProbe(path string, probe raftbootstrap.BaselineProbe, output io.Writer) error {
	encoded, err := raftbootstrap.MarshalBaselineProbe(probe)
	if err != nil {
		return errors.WithStack(err)
	}
	measured, err := marshalBootstrapObservedConfig(probe.BaselineContext)
	if err != nil {
		return err
	}
	if err := writeBootstrapArtifactExclusive(path, encoded); err != nil {
		return err
	}
	n, err := output.Write(append(measured, '\n'))
	if err == nil && n != len(measured)+1 {
		err = io.ErrShortWrite
	}
	return errors.WithStack(err)
}

func parseBootstrapObservationCommand(args []string) (bootstrapObservationCommand, error) {
	var r bootstrapObservationCommand
	if err := bootstrapBaselineArgsBound(args); err != nil {
		return r, err
	}
	if args[0] != bootstrapObservationProbe && args[0] != bootstrapObservationManifest {
		return r, errors.New("unsupported observation command")
	}
	r.Command = args[0]
	operation, member, err := parseBootstrapObservationFlags(args[1:], &r)
	if err != nil {
		return r, err
	}
	r.OperationID, err = parseBootstrapBaselineOperationID(operation)
	if err != nil {
		return r, err
	}
	if r.Command == bootstrapObservationProbe {
		r.MemberID, err = bootstrapBaselineUint(member)
		if err != nil || r.MemberID == 0 {
			return r, errors.New("invalid probing member")
		}
	}
	return r, validateBootstrapObservationPaths(r)
}

func parseBootstrapObservationFlags(args []string, r *bootstrapObservationCommand) (operation, member string, err error) {
	for _, arg := range args {
		if arg == "--" {
			return "", "", errors.New("observation does not accept option separators")
		}
	}
	fs := flag.NewFlagSet("bootstrap "+r.Command, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	values := map[string]*string{bootstrapObservationOperationFlag: &operation, "output": &r.Output}
	if r.Command == bootstrapObservationProbe {
		r.Encryption.flags(fs)
		values["pinned-config"], values["peer-config"], values["raft-data-dir"], values["member-id"] = &r.Config, &r.PeerConfig, &r.DataDir, &member
	} else {
		values["expected-config"] = &r.Config
		fs.Func(bootstrapObservationProbe, "signed member probe", func(value string) error { r.Probes = append(r.Probes, value); return nil })
	}
	seen := map[string]bool{}
	for name, target := range values {
		fs.Func(name, "required observation input", func(value string) error {
			if seen[name] {
				return errors.New("duplicate observation option")
			}
			seen[name], *target = true, value
			return nil
		})
	}
	if err := fs.Parse(args); err != nil {
		return "", "", errors.New("invalid observation options")
	}
	if fs.NArg() != 0 || len(seen) != len(values) {
		return "", "", errors.New("observation requires exact named inputs")
	}
	return operation, member, nil
}

func validateBootstrapObservationPaths(r bootstrapObservationCommand) error {
	if r.Command != bootstrapObservationProbe && r.Encryption != (bootstrapOfflineEncryptionCLI{}) {
		return errors.New("artifact-only manifest does not accept encryption options")
	}
	if err := r.Encryption.validate(); err != nil {
		return err
	}
	paths := []string{r.Config, r.Output}
	paths = append(paths, r.Encryption.paths()...)
	if r.Command == bootstrapObservationProbe {
		paths = append(paths, r.PeerConfig, r.DataDir)
	} else {
		if len(r.Probes) == 0 {
			return errors.New("manifest requires every signed member probe")
		}
		paths = append(paths, r.Probes...)
	}
	seen := map[string]bool{}
	for _, path := range paths {
		if !bootstrapBaselineValidPath(path) || seen[path] {
			return errors.New("observation paths must be distinct canonical absolute paths")
		}
		seen[path] = true
	}
	return nil
}

func parseBootstrapObservationPins(data []byte) (raftbootstrap.BaselineContext, error) {
	var c raftbootstrap.BaselineContext
	raw, err := decodeBootstrapObservationPins(data)
	if err != nil {
		return c, err
	}
	c.Version, c.Minimum = raw.Version, raftbootstrap.Versions{Entry: raw.Minimum.Entry, Snapshot: raw.Minimum.Snapshot, Session: raw.Minimum.Session}
	c.Configuration.State.AutoLeave = raw.AutoLeave
	c.Identity.Cluster, err = raftbootstrap.ParseUUID(raw.Cluster)
	if err != nil {
		return c, errors.WithStack(err)
	}
	c.Identity.Group, err = bootstrapBaselineUint(raw.Group)
	if err != nil {
		return c, err
	}
	if c.Version != raftbootstrap.BaselineVersion || c.Minimum != (raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}) {
		return c, errors.New("unsupported observation format")
	}
	return canonicalBootstrapObservationRoster(c, raw.Members)
}

func decodeBootstrapObservationPins(data []byte) (bootstrapObservationPinsJSON, error) {
	var raw bootstrapObservationPinsJSON
	if len(data) == 0 || len(data) > raftbootstrap.MaxFileSize || !utf8.Valid(data) {
		return raw, errors.New("invalid observation pins encoding/size")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	if err := scanBootstrapBaselineJSON(d, 0); err != nil {
		return raw, err
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) {
		return raw, errors.New("trailing observation pins")
	}
	d = json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&raw); err != nil {
		return raw, errors.New("pins must not contain a preselected cut or digest")
	}
	return raw, nil
}

func canonicalBootstrapObservationRoster(c raftbootstrap.BaselineContext, members []bootstrapBaselineMemberJSON) (raftbootstrap.BaselineContext, error) {
	if c.Configuration.State.AutoLeave {
		return c, errors.New("auto-leaving joint configuration is not a stable offline baseline")
	}
	for _, member := range members {
		if err := appendBootstrapBaselineMember(&c.Configuration, member); err != nil {
			return c, err
		}
	}
	// Index one is used only to validate a roster schema, never as native proof.
	configuration := c.Configuration
	configuration.Index = 1
	encoded, err := raftbootstrap.MarshalIntent(raftbootstrap.Intent{Identity: c.Identity, BaselineKind: raftbootstrap.Accepted, Baseline: configuration, Minimum: c.Minimum})
	if err != nil {
		return c, errors.WithStack(err)
	}
	intent, err := raftbootstrap.ParseIntent(encoded)
	if err != nil {
		return c, errors.WithStack(err)
	}
	c.Configuration = intent.Baseline
	c.Configuration.Index = 0
	return c, nil
}

func observeBootstrapOriginal(ctx context.Context, r bootstrapObservationCommand, pins raftbootstrap.BaselineContext, config *bootstrapPeerConfig) (probe raftbootstrap.BaselineProbe, err error) {
	if err := validateBootstrapPreparedSigner(config, raftbootstrap.BaselineManifest{BaselineContext: pins}, r.MemberID); err != nil {
		return probe, err
	}
	ownership, err := etcd.AcquireOfflineBaselineOwnership(r.DataDir)
	if err != nil {
		return probe, errors.WithStack(err)
	}
	defer func() {
		err = errors.Join(err, ownership.Close())
		if err != nil {
			probe = raftbootstrap.BaselineProbe{}
		}
	}()
	enc, err := r.Encryption.load(ctx, r.MemberID, pins.Identity.Group, r.DataDir)
	if err != nil {
		return probe, err
	}
	probe, err = etcd.ObserveOfflineBaseline(ctx, etcd.OfflineBaselineObservationOptions{DataDir: r.DataDir, Ownership: ownership, MemberID: r.MemberID,
		Crypto:   enc.NativeCrypto(),
		Identity: pins.Identity, OperationID: r.OperationID, Minimum: pins.Minimum, Members: pins.Configuration.Members,
		ProbeStore: func(ctx context.Context) (etcd.BaselineStoreProof, error) {
			if pins.Identity.Group == 0 {
				_, proof, err := probeOfflineTSOBaseline(ctx, r.DataDir, ownership)
				return proof, err
			}
			return bootstrapOfflineDataStoreProbe(ctx, r.DataDir, bootstrapOfflineOptionalEncryption(enc)...)
		}})
	if err != nil {
		return probe, errors.WithStack(err)
	}
	if err := matchBootstrapObservedRoster(pins.Configuration, probe.Configuration); err != nil {
		return probe, err
	}
	if err := ctx.Err(); err != nil {
		return probe, errors.WithStack(err)
	}
	signer, ok := config.clientTLS.Certificates[0].PrivateKey.(crypto.Signer)
	if !ok {
		return probe, errors.New("probe requires local TLS crypto signer")
	}
	probe, err = raftbootstrap.SignBaselineProbe(probe, signer)
	return probe, errors.WithStack(err)
}

func matchBootstrapObservedRoster(pinned, actual raftbootstrap.Configuration) error {
	pinned.Index = actual.Index
	pinnedDigest, err := raftbootstrap.ConfigurationDigest(pinned)
	if err != nil {
		return errors.WithStack(err)
	}
	actualDigest, err := raftbootstrap.ConfigurationDigest(actual)
	if err != nil {
		return errors.WithStack(err)
	}
	if actualDigest != pinnedDigest {
		return errors.New("observed membership differs from independent pins")
	}
	return nil
}

func assembleBootstrapObservation(r bootstrapObservationCommand, expectedRaw []byte, budget *bootstrapArtifactBudget) error {
	expected, err := parseBootstrapBaselineExpectedConfig(expectedRaw, r.OperationID)
	if err != nil {
		return err
	}
	if len(r.Probes) != len(expected.Configuration.Members) {
		return errors.New("manifest requires exactly all current members")
	}
	manifest := raftbootstrap.BaselineManifest{BaselineContext: expected}
	for _, path := range r.Probes {
		raw, err := budget.read(path)
		if err != nil {
			return err
		}
		probe, err := raftbootstrap.ParseBaselineProbe(raw)
		if err != nil {
			return errors.WithStack(err)
		}
		if err := raftbootstrap.VerifyBaselineProbe(probe, expected); err != nil {
			return errors.WithStack(err)
		}
		manifest.Members = append(manifest.Members, probe.Evidence)
	}
	if err := raftbootstrap.VerifyBaselineManifest(manifest, expected); err != nil {
		return errors.WithStack(err)
	}
	encoded, err := raftbootstrap.MarshalBaselineManifest(manifest)
	if err != nil {
		return errors.WithStack(err)
	}
	return writeBootstrapArtifactExclusive(r.Output, encoded)
}

func marshalBootstrapObservedConfig(c raftbootstrap.BaselineContext) ([]byte, error) {
	cluster := c.Identity.Cluster
	raw := bootstrapBaselineExpectedJSON{Version: c.Version, Cluster: fmt.Sprintf("%x-%x-%x-%x-%x", cluster[:4], cluster[4:6], cluster[6:8], cluster[8:10], cluster[10:]),
		AutoLeave: c.Configuration.State.AutoLeave,
		Group:     strconv.FormatUint(c.Identity.Group, 10), ConfigurationIndex: strconv.FormatUint(c.Configuration.Index, 10),
		Minimum:     bootstrapBaselineMinimumJSON{Entry: c.Minimum.Entry, Snapshot: c.Minimum.Snapshot, Session: c.Minimum.Session},
		CommitIndex: strconv.FormatUint(c.CommitIndex, 10), CommitTerm: strconv.FormatUint(c.CommitTerm, 10), Watermark: strconv.FormatUint(c.Watermark, 10), DataDigest: hex.EncodeToString(c.DataDigest[:])}
	for _, member := range c.Configuration.Members {
		raw.Members = append(raw.Members, bootstrapBaselineMemberJSON{ID: strconv.FormatUint(member.ID, 10), Address: member.Address, Key: hex.EncodeToString(member.PublicKeyDigest[:]), Roles: bootstrapObservedRoles(c.Configuration.State, member.ID)})
	}
	encoded, err := json.MarshalIndent(raw, "", "  ")
	return encoded, errors.WithStack(err)
}

func bootstrapObservedRoles(state raftbootstrap.ConfState, id uint64) []string {
	var roles []string
	for _, role := range []struct {
		name string
		ids  []uint64
	}{{noderuntime.VoterSuffrage, state.Voters}, {"outgoing", state.VotersOutgoing}, {bootstrapObservationLearner, state.Learners}, {"next", state.LearnersNext}} {
		for _, member := range role.ids {
			if member == id {
				roles = append(roles, role.name)
			}
		}
	}
	return roles
}
