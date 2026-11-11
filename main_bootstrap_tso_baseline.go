package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

const tsoBaselineDirectoryPerm = 0o700

type bootstrapOfflineTSOPlan struct {
	prepared kv.PreparedBootstrapActivation
	probe    []byte
}

func bootstrapOfflineTSOPreparation(prepared kv.PreparedBootstrapActivation) (*etcd.BaselinePreparationCallbacks, error) {
	if prepared.Probe.Identity.Group != dedicatedTSORaftGroupID {
		return nil, errors.WithStack(kv.ErrTSOBaseline)
	}
	encoded, err := raftbootstrap.MarshalBaselineProbe(prepared.Probe)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	probe, err := raftbootstrap.ParseBaselineProbe(encoded)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	prepared.Probe = probe
	if _, _, err := kv.PreparedBootstrapActivationMetadata(probe.Identity, prepared); err != nil {
		return nil, errors.WithStack(err)
	}
	plan := &bootstrapOfflineTSOPlan{prepared: prepared, probe: encoded}
	return &etcd.BaselinePreparationCallbacks{MutablePaths: []string{dedicatedTSOControlStoreName},
		StagePreparation: plan.stage, VerifyPreparedStore: plan.verify}, nil
}

func (p *bootstrapOfflineTSOPlan) validate(c etcd.BaselinePreparationContext) error {
	if err := raftbootstrap.VerifyBaselineManifest(c.Manifest, p.prepared.Probe.BaselineContext); err != nil {
		return errors.WithStack(err)
	}
	digest, err := raftbootstrap.BaselineManifestDigest(c.Manifest)
	if err != nil {
		return errors.WithStack(err)
	}
	if digest != p.prepared.ManifestDigest {
		return errors.WithStack(kv.ErrBootstrapActivationPreparation)
	}
	for _, evidence := range c.Manifest.Members {
		if evidence.MemberID != p.prepared.Probe.Evidence.MemberID {
			continue
		}
		encoded, err := raftbootstrap.MarshalBaselineProbe(raftbootstrap.BaselineProbe{BaselineContext: c.Manifest.BaselineContext, Evidence: evidence})
		if err != nil {
			return errors.WithStack(err)
		}
		if !bytes.Equal(encoded, p.probe) {
			return errors.WithStack(kv.ErrBootstrapActivationPreparation)
		}
		return validateTSOBackupPaths(c)
	}
	return errors.WithStack(kv.ErrBootstrapActivationPreparation)
}

func validateTSOBackupPaths(c etcd.BaselinePreparationContext) error {
	backup, err := os.Lstat(c.BackupDir)
	if err != nil {
		return errors.WithStack(err)
	}
	target, err := os.Lstat(c.DataDir)
	if err != nil {
		return errors.WithStack(err)
	}
	if !backup.IsDir() || !target.IsDir() || os.SameFile(backup, target) {
		return errors.WithStack(kv.ErrTSOBaseline)
	}
	// A legacy source has NO control DB. Never reinterpret a bound/prepared
	// target (or an alias of its mutable root) as the original backup.
	_, err = os.Lstat(filepath.Join(c.BackupDir, dedicatedTSOControlStoreName))
	if !os.IsNotExist(err) {
		return errors.Join(kv.ErrTSOBaseline, err)
	}
	return validateTSOBackupRootSeparation(c.BackupDir, filepath.Join(c.DataDir, dedicatedTSOControlStoreName))
}

func validateTSOBackupRootSeparation(backup, target string) error {
	for _, pair := range [][2]string{{backup, target}, {target, backup}} {
		rel, err := filepath.Rel(pair[0], pair[1])
		if err != nil {
			return errors.WithStack(err)
		}
		if rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return errors.WithStack(kv.ErrTSOBaseline)
		}
	}
	return nil
}

func (p *bootstrapOfflineTSOPlan) withBackup(ctx context.Context, c etcd.BaselinePreparationContext, action func(kv.TSOBaselineState, etcd.BaselineStoreProof) error) (err error) {
	if err := p.validate(c); err != nil {
		return err
	}
	ownership, err := etcd.AcquireOfflineBaselineOwnership(c.BackupDir)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { err = errors.Join(err, ownership.Close()) }()
	state, proof, err := probeOfflineTSOBaseline(ctx, c.BackupDir, ownership)
	if err != nil {
		return err
	}
	if err := validateTSOBaselinePreparation(ctx, state, p.prepared); err != nil {
		return err
	}
	if err := p.verifyOriginalNativeCut(ctx, c.BackupDir, ownership, proof); err != nil {
		return err
	}
	return action(state, proof)
}

func (p *bootstrapOfflineTSOPlan) verifyOriginalNativeCut(ctx context.Context, backup string, ownership io.Closer, proof etcd.BaselineStoreProof) error {
	inspection, err := etcd.InspectOfflineBaseline(ctx, etcd.OfflineBaselineOptions{Ownership: ownership, DataDir: backup,
		MemberID: p.prepared.Probe.Evidence.MemberID, Expected: p.prepared.Probe.BaselineContext,
		ProbeStore: func(context.Context) (etcd.BaselineStoreProof, error) { return proof, nil }})
	if err != nil {
		return errors.WithStack(err)
	}
	// Recheck the original signed native fingerprint/term/ConfState as well as
	// scalar equality. The backup's root path itself is not signing evidence.
	evidence := inspection.Evidence
	evidence.PublicKeyDER = p.prepared.Probe.Evidence.PublicKeyDER
	evidence.Signature = p.prepared.Probe.Evidence.Signature
	return errors.WithStack(raftbootstrap.VerifyBaselineProbe(raftbootstrap.BaselineProbe{
		BaselineContext: p.prepared.Probe.BaselineContext, Evidence: evidence}, p.prepared.Probe.BaselineContext))
}

func (p *bootstrapOfflineTSOPlan) stage(ctx context.Context, c etcd.BaselinePreparationContext) error {
	return p.withBackup(ctx, c, func(state kv.TSOBaselineState, _ etcd.BaselineStoreProof) error {
		_, err := initializePreparedTSOBaseline(ctx, c.DataDir, state, p.prepared)
		return err
	})
}

func (p *bootstrapOfflineTSOPlan) verify(ctx context.Context, c etcd.BaselinePreparationContext) (etcd.BaselinePreparedStoreProof, error) {
	var result etcd.BaselinePreparedStoreProof
	err := p.withBackup(ctx, c, func(state kv.TSOBaselineState, proof etcd.BaselineStoreProof) error {
		digest, err := verifyPreparedTSOBaseline(ctx, filepath.Join(c.DataDir, dedicatedTSOControlStoreName), state, p.prepared)
		if err != nil {
			return err
		}
		result = etcd.BaselinePreparedStoreProof{Baseline: proof, PreparedDataDigest: digest}
		return nil
	})
	return result, err
}

// probeOfflineTSOBaseline does not look for fsm.db or create a control DB. The
// native reader verifies selected snapshot bytes, WAL coverage and actual C;
// the legacy FSM reconstructs only scalars under that same exclusive ownership.
func probeOfflineTSOBaseline(ctx context.Context, dataDir string, ownership io.Closer) (state kv.TSOBaselineState, proof etcd.BaselineStoreProof, err error) {
	source, err := etcd.ReadOfflineBaselineWALSource(ctx, dataDir, ownership)
	if err != nil {
		return state, proof, errors.WithStack(err)
	}
	if source.Snapshot == nil {
		return state, proof, errors.WithStack(kv.ErrTSOBaseline)
	}
	defer func() { err = errors.Join(err, source.Snapshot.Close()) }()
	if source.Configuration == nil || source.Configuration.GetAutoLeave() {
		return state, proof, errors.WithStack(kv.ErrTSOBaseline)
	}
	cut := kv.TSOBaselineCut{SnapshotIndex: source.SnapshotIndex, SnapshotTerm: source.SnapshotTerm,
		CommitIndex: source.CommitIndex, CommitTerm: source.CommitTerm, LastLogIndex: source.LastLogIndex, ConfigurationIndex: source.ConfigurationIndex}
	state, err = kv.ReconstructTSOBaseline(ctx, source.Snapshot, cut, func(yield func(kv.TSOBaselineEntry) error) error {
		for _, entry := range source.Replay {
			if err := yield(kv.TSOBaselineEntry{Index: entry.Index, Term: entry.Term, Type: entry.Type, Payload: entry.Payload}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return state, proof, errors.WithStack(err)
	}
	members := append([]uint64(nil), source.Configuration.GetVoters()...)
	members = append(members, source.Configuration.GetVotersOutgoing()...)
	members = append(members, source.Configuration.GetLearners()...)
	members = append(members, source.Configuration.GetLearnersNext()...)
	slices.Sort(members)
	proof = etcd.BaselineStoreProof{AppliedIndex: state.AppliedIndex, ConfigurationIndex: state.ConfigurationIndex,
		Watermark: state.Watermark, DataDigest: state.DataDigest, ConfigurationMembers: slices.Compact(members)}
	return state, proof, nil
}

// initializePreparedTSOBaseline is an EXPLICIT migration transformation under
// native offline ownership, after its immutable source backup. Only the fixed
// new control-store root may change. Existing roots are accepted solely after
// exact full-image comparison with the expected scalar+preparation transaction.
// The native preparation callback must still verify its original source backup
// and all files outside this root before exchanging the WAL.
func initializePreparedTSOBaseline(ctx context.Context, dataDir string, state kv.TSOBaselineState, prepared kv.PreparedBootstrapActivation) (raftbootstrap.Digest, error) {
	if err := validateTSOBaselinePreparation(ctx, state, prepared); err != nil {
		return raftbootstrap.Digest{}, err
	}
	path := filepath.Join(dataDir, dedicatedTSOControlStoreName)
	if err := os.Mkdir(path, tsoBaselineDirectoryPerm); err != nil {
		if !os.IsExist(err) {
			return raftbootstrap.Digest{}, errors.WithStack(err)
		}
		return resumePreparedTSOStore(ctx, path, state, prepared)
	}
	return createPreparedTSOStore(ctx, path, state, prepared)
}

func resumePreparedTSOStore(ctx context.Context, path string, state kv.TSOBaselineState, prepared kv.PreparedBootstrapActivation) (raftbootstrap.Digest, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return raftbootstrap.Digest{}, errors.WithStack(err)
	}
	if !info.IsDir() {
		return raftbootstrap.Digest{}, errors.WithStack(kv.ErrTSOBaseline)
	}
	empty, err := emptyPreparedTSOStore(path)
	if err != nil {
		return raftbootstrap.Digest{}, err
	}
	if empty {
		return createPreparedTSOStore(ctx, path, state, prepared)
	}
	return verifyPreparedTSOBaseline(ctx, path, state, prepared)
}

// A crash before the single batch may leave an empty Pebble directory. Only
// that exact logical prefix may be completed; no metadata row is filtered out.
func emptyPreparedTSOStore(path string) (empty bool, err error) {
	db, err := pebble.Open(path, &pebble.Options{ReadOnly: true})
	if err != nil {
		return false, errors.WithStack(err)
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	iter, err := db.NewIter(nil)
	if err != nil {
		return false, errors.WithStack(err)
	}
	defer func() { err = errors.Join(err, iter.Close()) }()
	return !iter.First(), errors.WithStack(iter.Error())
}

func validateTSOBaselinePreparation(ctx context.Context, state kv.TSOBaselineState, prepared kv.PreparedBootstrapActivation) error {
	probe := prepared.Probe
	if probe.Identity.Group != dedicatedTSORaftGroupID || state.AppliedIndex != probe.CommitIndex || state.ConfigurationIndex != probe.Configuration.Index || state.Watermark != probe.Watermark || state.DataDigest != probe.DataDigest {
		return errors.WithStack(kv.ErrTSOBaseline)
	}
	if _, _, err := kv.PreparedBootstrapActivationMetadata(probe.Identity, prepared); err != nil {
		return errors.WithStack(err)
	}
	// Recompute from bytes rather than trusting the digest fields of a struct
	// returned by an earlier probe, possibly before an interrupted CLI attempt.
	verified, err := kv.ReconstructTSOBaseline(ctx, bytes.NewReader(state.ScalarSnapshot), kv.TSOBaselineCut{
		SnapshotIndex: state.AppliedIndex, SnapshotTerm: probe.CommitTerm, CommitIndex: state.AppliedIndex,
		CommitTerm: probe.CommitTerm, LastLogIndex: state.AppliedIndex, ConfigurationIndex: state.ConfigurationIndex}, nil)
	if err != nil {
		return errors.WithStack(err)
	}
	if verified.DataDigest != state.DataDigest {
		return errors.WithStack(kv.ErrTSOBaseline)
	}
	return nil
}

func createPreparedTSOStore(ctx context.Context, path string, state kv.TSOBaselineState, prepared kv.PreparedBootstrapActivation) (digest raftbootstrap.Digest, err error) {
	st, err := store.NewPebbleStore(path)
	if err != nil {
		return digest, errors.WithStack(err)
	}
	defer func() { err = errors.Join(err, st.Close()) }()
	backing, ok := st.(kv.TSODurableBacking)
	if !ok {
		return digest, errors.WithStack(kv.ErrTSODurableBacking)
	}
	fsm, err := kv.NewDurableTSOStateMachine(nil, backing)
	if err != nil {
		return digest, errors.WithStack(err)
	}
	err = fsm.InitializeLegacyControl(ctx, state.AppliedIndex, state.ConfigurationIndex, bytes.NewReader(state.ScalarSnapshot), func(tx context.Context, metadata store.RaftControlMetadataStore) error {
		return kv.StagePreparedBootstrapActivationInEntry(tx, metadata, prepared.Probe.Identity, prepared)
	})
	if err != nil {
		return digest, errors.WithStack(err)
	}
	image, err := store.ProbeBootstrapBaseline(ctx, st)
	return raftbootstrap.Digest(image.DataDigest), errors.WithStack(err)
}

// verifyPreparedTSOBaseline compares ALL target logical rows, including durable
// index and opaque metadata, with an independently constructed exact expected
// image. It does not equate the source scalar digest with the full DB digest or
// silently exclude arbitrary records. The existing target is opened read-only.
func verifyPreparedTSOBaseline(ctx context.Context, path string, state kv.TSOBaselineState, prepared kv.PreparedBootstrapActivation) (raftbootstrap.Digest, error) {
	if err := validateTSOBaselinePreparation(ctx, state, prepared); err != nil {
		return raftbootstrap.Digest{}, err
	}
	temporary, err := os.MkdirTemp("", "tso-baseline-expected-")
	if err != nil {
		return raftbootstrap.Digest{}, errors.WithStack(err)
	}
	defer func() { _ = os.RemoveAll(temporary) }()
	expected, err := createPreparedTSOStore(ctx, temporary, state, prepared)
	if err != nil {
		return raftbootstrap.Digest{}, err
	}
	actual, err := readOnlyTSOStoreDigest(ctx, path)
	if err != nil {
		return raftbootstrap.Digest{}, err
	}
	if expected != actual {
		return raftbootstrap.Digest{}, errors.WithStack(kv.ErrTSOBaseline)
	}
	return actual, nil
}

func readOnlyTSOStoreDigest(ctx context.Context, path string) (digest raftbootstrap.Digest, err error) {
	db, err := pebble.Open(path, &pebble.Options{ReadOnly: true})
	if err != nil {
		return digest, errors.WithStack(err)
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	snapshot := db.NewSnapshot()
	defer func() { err = errors.Join(err, snapshot.Close()) }()
	proof, err := store.ProbeBootstrapBaselineSnapshot(ctx, snapshot)
	return raftbootstrap.Digest(proof.DataDigest), errors.WithStack(err)
}
