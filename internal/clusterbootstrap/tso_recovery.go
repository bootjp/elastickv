package clusterbootstrap

import (
	"bufio"
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

func protectedTSONamespaceVerifier(dir string, spec *GroupBootstrapSpec, sm *protectedTSOStateMachine) etcd.NamespaceRecoveryVerifier {
	return func(ctx context.Context, source etcd.NamespaceRecoverySource) (uint64, bool, error) {
		reader := bufio.NewReader(source.Snapshot())
		magic, err := reader.Peek(len("EKVTSO06"))
		if err != nil {
			return 0, false, errors.WithStack(err)
		}
		if string(magic) == "EKVTSO06" {
			return 0, false, nil // Native will perform the complete ordinary Restore.
		}
		index, err := verifyLegacyTSONamespace(ctx, dir, spec, sm, source, reader)
		if err != nil {
			return 0, false, err
		}
		sm.verifiedLegacySnapshot = source.SnapshotIndex()
		sm.verifiedLegacyRecovery = true
		return index, true, nil
	}
}

type tsoNamespaceRecipe struct {
	manifest raftbootstrap.BaselineManifest
	prepared kv.PreparedBootstrapActivation
	native   raftengine.NativeConfiguration
	index    uint64
}

func loadTSORecoveryManifest(payload []byte) (raftbootstrap.BaselineManifest, error) {
	command, matched, err := raftbootstrap.ParseActivationCommand(payload)
	if err != nil || !matched {
		return raftbootstrap.BaselineManifest{}, errors.Join(kv.ErrTSODurableBacking, err)
	}
	return command.Manifest, nil
}

func loadTSORecoveryRecipe(spec *GroupBootstrapSpec, sm *protectedTSOStateMachine, source etcd.NamespaceRecoverySource) (tsoNamespaceRecipe, error) {
	var recipe tsoNamespaceRecipe
	manifest, err := loadTSORecoveryManifest(spec.Config.Activation)
	if err != nil {
		return recipe, err
	}
	prepared, err := BootstrapLocalPreparation(spec.Config.Local.Member, manifest)
	if err != nil {
		return recipe, err
	}
	identity := source.Identity()
	if err := ValidatePreparedBootstrapIdentity(prepared, identity); err != nil {
		return recipe, err
	}
	index, err := tsoBackingAppliedIndex(sm.backing)
	if err != nil {
		return recipe, err
	}
	if !validTSORecoveryBoundary(index, manifest.CommitIndex, source) {
		return recipe, errors.WithStack(kv.ErrTSODurableBacking)
	}
	native, err := loadTSORecoveryNative(source, prepared)
	if err != nil {
		return recipe, err
	}
	return tsoNamespaceRecipe{manifest: manifest, prepared: prepared, native: native, index: index}, nil
}

func validTSORecoveryBoundary(index, baseline uint64, source etcd.NamespaceRecoverySource) bool {
	return index >= baseline && index <= source.CommitIndex() && source.SnapshotIndex() <= baseline
}

func loadTSORecoveryNative(source etcd.NamespaceRecoverySource, prepared kv.PreparedBootstrapActivation) (raftengine.NativeConfiguration, error) {
	configuration, configurationIndex, err := source.Configuration()
	if err != nil {
		return raftengine.NativeConfiguration{}, errors.WithStack(err)
	}
	native := raftengine.NativeConfiguration{Index: configurationIndex, Voters: configuration.GetVoters(),
		VotersOutgoing: configuration.GetVotersOutgoing(), Learners: configuration.GetLearners(), LearnersNext: configuration.GetLearnersNext(), AutoLeave: configuration.GetAutoLeave()}
	for _, member := range prepared.Probe.Configuration.Members {
		native.Members = append(native.Members, raftengine.NativeConfigurationMember{ID: member.ID, Address: member.Address})
	}
	if _, err := kv.BootstrapActivationProofFromNative(source.Identity().Identity, native, prepared); err != nil {
		return native, errors.WithStack(err)
	}
	return native, nil
}

func verifyLegacyTSONamespace(ctx context.Context, dir string, spec *GroupBootstrapSpec, sm *protectedTSOStateMachine, source etcd.NamespaceRecoverySource, snapshot io.Reader) (uint64, error) {
	recipe, err := loadTSORecoveryRecipe(spec, sm, source)
	if err != nil {
		return 0, err
	}
	baseline, err := reconstructSelectedTSOBaseline(ctx, snapshot, source, recipe.manifest)
	if err != nil {
		return 0, err
	}
	plan := &bootstrapOfflineTSOPlan{prepared: recipe.prepared}
	plan.probe, err = raftbootstrap.MarshalBaselineProbe(recipe.prepared.Probe)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	err = plan.withBackup(ctx, etcd.BaselinePreparationContext{DataDir: dir,
		BackupDir: etcd.OfflineBaselineBackupDirectory(dir, recipe.manifest), Manifest: recipe.manifest}, func(original kv.TSOBaselineState, _ etcd.BaselineStoreProof) error {
		if original.DataDigest != baseline.DataDigest {
			return errors.WithStack(kv.ErrTSOBaseline)
		}
		return CompareReplayedTSOControl(ctx, sm, recipe.prepared, baseline, recipe.native, source.Replay(), recipe.index)
	})
	return recipe.index, err
}

func reconstructSelectedTSOBaseline(ctx context.Context, snapshot io.Reader, source etcd.NamespaceRecoverySource, manifest raftbootstrap.BaselineManifest) (kv.TSOBaselineState, error) {
	var entries []kv.TSOBaselineEntry
	for _, entry := range source.Replay() {
		if entry.Index <= manifest.CommitIndex {
			entries = append(entries, kv.TSOBaselineEntry{Index: entry.Index, Term: entry.Term, Type: entry.Type, Payload: entry.Payload})
		}
	}
	state, err := kv.ReconstructTSOBaseline(ctx, snapshot, kv.TSOBaselineCut{
		SnapshotIndex: source.SnapshotIndex(), SnapshotTerm: source.SnapshotTerm(), CommitIndex: manifest.CommitIndex,
		CommitTerm: manifest.CommitTerm, LastLogIndex: manifest.CommitIndex, ConfigurationIndex: manifest.Configuration.Index}, func(yield func(kv.TSOBaselineEntry) error) error {
		for _, entry := range entries {
			if err := yield(entry); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil && (state.DataDigest != manifest.DataDigest || state.Watermark != manifest.Watermark) {
		err = kv.ErrTSOBaseline
	}
	return state, errors.WithStack(err)
}

func CompareReplayedTSOControl(ctx context.Context, actual *protectedTSOStateMachine, prepared kv.PreparedBootstrapActivation, baseline kv.TSOBaselineState, native raftengine.NativeConfiguration, entries []etcd.OfflineBaselineReplayEntry, index uint64) (err error) {
	dir, err := os.MkdirTemp("", "tso-recovery-expected-")
	if err != nil {
		return errors.WithStack(err)
	}
	defer os.RemoveAll(dir)
	if _, err := InitializePreparedTSOBaseline(ctx, dir, baseline, prepared); err != nil {
		return err
	}
	st, err := store.NewPebbleStore(filepath.Join(dir, DedicatedTSOControlStoreName))
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { err = errors.Join(err, st.Close()) }()
	sm, err := NewProtectedTSOStateMachine(st, nil, prepared.Probe.Identity)
	if err != nil {
		return err
	}
	if err := replayTSOControlSuffix(ctx, sm, native, entries, baseline.AppliedIndex, index); err != nil {
		return err
	}
	return compareTSOControlImages(ctx, st, actual.backing, index)
}

func replayTSOControlSuffix(ctx context.Context, sm *protectedTSOStateMachine, native raftengine.NativeConfiguration, entries []etcd.OfflineBaselineReplayEntry, baseline, index uint64) error {
	for _, entry := range entries {
		if entry.Index <= baseline || entry.Index > index {
			continue
		}
		if err := replayTSOControlEntry(ctx, sm, native, entry); err != nil {
			return err
		}
	}
	return nil
}

func compareTSOControlImages(ctx context.Context, expectedStore, actualStore store.MVCCStore, index uint64) error {
	expected, err := store.ProbeBootstrapBaseline(ctx, expectedStore)
	if err != nil {
		return errors.WithStack(err)
	}
	observed, err := store.ProbeBootstrapBaseline(ctx, actualStore)
	if err != nil {
		return errors.WithStack(err)
	}
	if expected.AppliedIndex != index || expected.DataDigest != observed.DataDigest {
		return errors.WithStack(kv.ErrTSODurableBacking)
	}
	return nil
}

func replayTSOControlEntry(ctx context.Context, sm *protectedTSOStateMachine, native raftengine.NativeConfiguration, entry etcd.OfflineBaselineReplayEntry) error {
	if entry.Type != kv.TSOBaselineNormalEntry {
		return errors.Wrap(kv.ErrTSODurableBacking, "configuration changes after legacy baseline require a complete control snapshot")
	}
	if len(entry.Payload) == 0 {
		return errors.WithStack(sm.SetDurableAppliedIndex(entry.Index))
	}
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	sm.SetApplyNativeConfiguration(native)
	sm.SetApplyEntryIdentity(raftengine.NewAppliedEntryIdentity(entry.Index, entry.Term, entry.Type, entry.Payload))
	result := sm.Apply(entry.Payload)
	sm.ClearApplyEntryIdentity()
	if halt, ok := result.(interface{ HaltApply() error }); ok {
		if err := halt.HaltApply(); err != nil {
			return errors.WithStack(err)
		}
	}
	if err, ok := result.(error); ok {
		return errors.WithStack(err)
	}
	return nil
}
