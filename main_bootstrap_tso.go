package main

import (
	"context"
	"path/filepath"
	"slices"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

const dedicatedTSOControlStoreName = "tso-control.db"

// Proposed shared startup seam: recovery needs an identity/boundary-bound policy
// loader, not an MVCC data store. The existing shared startup remains unchanged;
// this helper uses its real Pebble control backing and adds TSO checks around it.
type tsoBootstrapRecoveryLoader func(context.Context, raftbootstrap.Identity, uint64) (raftbootstrap.PolicyRecord, error)

// buildProtectedDedicatedTSOGroup requires explicitly migrated durable state.
// A prepared group may reach native v2 verification, not active-policy readiness.
// It neither initializes a missing DB nor imports legacy snapshots.
// publicMethods belongs to the parent listener; options may carry its at-rest
// encryption wiring. The ShardGroup never exposes this backing as a data store.
func buildProtectedDedicatedTSOGroup(
	raftID string, group groupSpec, baseDir string, multi, bootstrap, joinAsLearner bool,
	bootstrapServers, bootstrapSeed []raftengine.Server, factory raftengine.Factory,
	clock *kv.HLC, proposalObserver kv.ProposalObserver,
	spec *groupBootstrapSpec, domain *bootstrapDomain, publicMethods []string,
	options ...store.PebbleStoreOption,
) (*raftGroupRuntime, *kv.ShardGroup, error) {
	dir := groupDataDir(baseDir, raftID, group.id, multi)
	native, err := validateProtectedTSOFactory(raftID, group, dir, factory, spec, domain)
	if err != nil {
		return nil, nil, err
	}
	path := filepath.Join(dir, dedicatedTSOControlStoreName)
	ownership, err := etcd.AcquireNamespaceRecoveryOwnership(dir)
	if err != nil {
		return nil, nil, errors.Wrap(err, "protected TSO requires an explicitly migrated control store and existing native WAL")
	}
	defer ownership.Close()
	description, err := pebble.Peek(path, vfs.Default)
	if err != nil {
		return nil, nil, errors.Wrap(err, "protected TSO requires an explicitly migrated control store")
	}
	if !description.Exists {
		return nil, nil, errors.Wrap(kv.ErrTSODurableBacking, "protected TSO requires an explicitly migrated control store")
	}
	st, err := store.NewPebbleStore(path, options...)
	if err != nil {
		return nil, nil, errors.Wrap(err, "open protected TSO control store")
	}
	sm, err := newProtectedTSOStateMachine(st, clock, spec.intent.Identity)
	if err != nil {
		return nil, nil, errors.Join(err, st.Close())
	}
	setup := newProtectedTSOStartup(spec, st, sm, domain, publicMethods)
	native = native.WithNamespaceRecovery(ownership, protectedTSONamespaceVerifier(dir, spec, sm))
	protected := &protectedGroupFactory{Factory: native.WithBootstrapStartup(setup), spec: spec}
	runtime, err := buildRuntimeForGroup(raftID, group, baseDir, multi, bootstrap,
		bootstrapServers, bootstrapSeed, st, sm, protected, joinAsLearner)
	if err != nil {
		return nil, nil, errors.Join(err, st.Close())
	}
	runtime.bootstrapDomain = domain
	runtime.bootstrapPeerConfig = spec.config
	sg := &kv.ShardGroup{Engine: runtime.engine, TSOState: sm.TSOStateMachine}
	sg.Txn = kv.NewLeaderProxyForShardGroup(sg, kv.WithProposalObserver(proposalObserver))
	return runtime, sg, nil
}

func validateProtectedTSOFactory(raftID string, group groupSpec, dir string, factory raftengine.Factory, spec *groupBootstrapSpec, domain *bootstrapDomain) (*etcd.Factory, error) {
	if !validProtectedTSOWiring(group, dir, spec, domain) {
		return nil, errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	native, ok := factory.(*etcd.Factory)
	if !ok || native == nil {
		return nil, errors.New("protected TSO requires the native etcd factory")
	}
	if _, err := validateBootstrapStartup(spec.config, spec.intent, raftengine.FactoryConfig{GroupID: group.id, LocalID: raftID, LocalAddress: group.address}); err != nil {
		return nil, errors.WithStack(err)
	}
	preflight := &protectedGroupFactory{spec: spec}
	if err := preflight.PreflightRaftDataDir(dir, raftEngineEtcd); err != nil {
		return nil, err
	}
	return native, nil
}

func validProtectedTSOWiring(group groupSpec, dir string, spec *groupBootstrapSpec, domain *bootstrapDomain) bool {
	return group.id == dedicatedTSORaftGroupID && spec != nil && spec.config != nil && spec.files != nil && spec.dir == dir && domain != nil && domain.admission != nil
}

type protectedTSOStateMachine struct {
	*kv.GuardedTSOStateMachine
	backing                store.MVCCStore
	identity               raftbootstrap.Identity
	verifiedLegacySnapshot uint64
	verifiedLegacyRecovery bool
}

// Protected scalar effects are part of the indexed full control image. They
// must not be reapplied without entry identity by native's legacy HLC warmup.
func (f *protectedTSOStateMachine) IsVolatileOnlyPayload([]byte) bool { return false }

func (f *protectedTSOStateMachine) ApplyChangesAdmission(payload []byte) (bool, error) {
	changed, err := kv.BootstrapApplyChangesAdmission(context.Background(), f.backing, f.identity, payload)
	return changed, errors.WithStack(err)
}

func newProtectedTSOStateMachine(st store.MVCCStore, clock *kv.HLC, identity raftbootstrap.Identity) (*protectedTSOStateMachine, error) {
	backing, ok := st.(kv.TSODurableBacking)
	if !ok {
		return nil, errors.WithStack(kv.ErrTSODurableBacking)
	}
	sm, err := kv.NewDurableTSOStateMachine(clock, backing)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	guarded, err := kv.NewGuardedTSOStateMachine(sm, identity, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	f := &protectedTSOStateMachine{GuardedTSOStateMachine: guarded, backing: st, identity: identity}
	index, err := tsoBackingAppliedIndex(st)
	if err != nil {
		return nil, err
	}
	if err := f.validateStoredTSORecovery(index); err != nil {
		return nil, err
	}
	return f, nil
}

func (f *protectedTSOStateMachine) validateStoredTSORecovery(index uint64) error {
	_, err := f.policyLoader()(context.Background(), f.identity, index)
	if !errors.Is(err, store.ErrKeyNotFound) {
		return err
	}
	// This permits Open to verify the native v2 WAL binding. It grants no
	// admission and does not reinterpret malformed or inconsistent P as absent.
	_, err = f.preparedTSOConfiguration(index)
	return err
}

func (f *protectedTSOStateMachine) preparedTSOConfiguration(applied uint64) (kv.PreparedBootstrapActivation, error) {
	var zero kv.PreparedBootstrapActivation
	if err := f.requireTSOScalarSnapshot(); err != nil {
		return zero, err
	}
	metadata, ok := f.backing.(store.RaftControlMetadataStore)
	if !ok {
		return zero, errors.WithStack(kv.ErrTSODurableBacking)
	}
	prepared, err := kv.ReadPreparedBootstrapActivation(context.Background(), metadata, f.identity)
	if err != nil {
		return zero, errors.WithStack(err)
	}
	if prepared.Probe.CommitIndex > applied || prepared.Probe.Watermark != f.AllocationFloor() || prepared.Probe.Configuration.Index != f.AppliedConfigurationIndex() {
		return zero, errors.WithStack(kv.ErrBootstrapActivationPreparation)
	}
	return prepared, nil
}

func (f *protectedTSOStateMachine) requireTSOScalarSnapshot() error {
	snapshot, err := f.Snapshot()
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(snapshot.Close())
}

// newPreparedTSOStateMachine is the explicit initial-activation constructor.
// The caller has already migrated the selected scalar snapshot and prepared
// the signed baseline while offline. No P or runtime readiness is fabricated.
// Production passes nil proof: the engine injects its pre-apply native image
// even on cold replay. A test/startup getter may supply an independent ordered
// projection, never a closure waiting on runtime.engine. The normal recovery
// factory never calls this.
func newPreparedTSOStateMachine(st store.MVCCStore, clock *kv.HLC, identity raftbootstrap.Identity, proof func(context.Context) (kv.BootstrapActivationNativeProof, error)) (*protectedTSOStateMachine, error) {
	backing, ok := st.(kv.TSODurableBacking)
	if !ok {
		return nil, errors.WithStack(kv.ErrBootstrapActivationNativeProof)
	}
	key, err := kv.BootstrapPolicyMetadataKey(identity)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if _, err := backing.ReadRaftControlMetadata(context.Background(), key); !errors.Is(err, store.ErrKeyNotFound) {
		return nil, errors.Join(kv.ErrBootstrapActivationPreparation, err)
	}
	state, err := kv.NewDurableTSOStateMachine(clock, backing)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if err := validatePreparedTSOCut(st, backing, state, identity); err != nil {
		return nil, err
	}
	guarded, err := kv.NewGuardedTSOStateMachine(state, identity, proof)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &protectedTSOStateMachine{GuardedTSOStateMachine: guarded, backing: st, identity: identity}, nil
}

func validatePreparedTSOCut(st store.MVCCStore, backing kv.TSODurableBacking, state *kv.TSOStateMachine, identity raftbootstrap.Identity) error {
	prepared, err := kv.ReadPreparedBootstrapActivation(context.Background(), backing, identity)
	if err != nil {
		return errors.WithStack(err)
	}
	if prepared.Probe.Watermark != state.AllocationFloor() || prepared.Probe.Configuration.Index != state.AppliedConfigurationIndex() {
		return errors.WithStack(kv.ErrBootstrapActivationPreparation)
	}
	index, err := tsoBackingAppliedIndex(st)
	if err != nil {
		return err
	}
	if index != prepared.Probe.CommitIndex {
		return errors.WithStack(kv.ErrBootstrapActivationPreparation)
	}
	return nil
}

func tsoBackingAppliedIndex(st store.MVCCStore) (uint64, error) {
	reader, ok := st.(raftengine.AppliedIndexReader)
	if !ok {
		return 0, errors.WithStack(kv.ErrTSODurableBacking)
	}
	index, known, err := reader.LastAppliedIndex()
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if !known || index == 0 {
		return 0, errors.WithStack(kv.ErrTSODurableBacking)
	}
	return index, nil
}

func (f *protectedTSOStateMachine) policyLoader() tsoBootstrapRecoveryLoader {
	return func(ctx context.Context, identity raftbootstrap.Identity, applied uint64) (raftbootstrap.PolicyRecord, error) {
		var zero raftbootstrap.PolicyRecord
		if identity != f.identity {
			return zero, errors.WithStack(raftbootstrap.ErrInconsistent)
		}
		// Constructor permits an empty backing for explicit migration; recovery
		// must additionally require its scalar row, not just a standalone P row.
		if err := f.requireTSOScalarSnapshot(); err != nil {
			return zero, err
		}
		policy, err := kv.LoadAppliedBootstrapPolicy(ctx, f.backing, identity, applied)
		if err != nil {
			return zero, errors.WithStack(err)
		}
		if policy.LastConfigurationIndex != f.AppliedConfigurationIndex() {
			return zero, errors.WithStack(raftbootstrap.ErrInconsistent)
		}
		return policy, nil
	}
}

func newProtectedTSOStartup(spec *groupBootstrapSpec, st store.MVCCStore, sm *protectedTSOStateMachine, domain *bootstrapDomain, publicMethods []string) bootstrapStartupFactory {
	base := newBootstrapStartupForGroup(spec.config, spec.files, spec.intent, st, publicMethods, domain)
	return func(cfg raftengine.FactoryConfig, transport *etcd.GRPCTransport) (*etcd.BootstrapStartup, error) {
		startup, err := base(cfg, transport)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		recovered := startup.RecoveredAdmission
		startup.RecoveredAdmission = func(state etcd.RecoveredState) error {
			if err := validateProtectedTSORecoveryWithArtifact(sm, *startup.NativeIdentity, state, spec.config.activation); err != nil {
				domain.admission.LatchFatal(err)
				startup.Close()
				return err
			}
			return errors.WithStack(recovered(state))
		}
		return startup, nil
	}
}

func validateProtectedTSORecovery(sm *protectedTSOStateMachine, expected etcd.WALIdentity, state etcd.RecoveredState) error {
	return validateProtectedTSORecoveryWithArtifact(sm, expected, state, nil)
}

func validateProtectedTSORecoveryWithArtifact(sm *protectedTSOStateMachine, expected etcd.WALIdentity, state etcd.RecoveredState, activation []byte) error {
	actual, verified := state.Identity()
	if !verified || actual != expected {
		return errors.WithStack(etcd.ErrWALIdentity)
	}
	policy, err := sm.policyLoader()(context.Background(), expected.Identity, state.AppliedIndex())
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return validatePreparedTSORecovery(sm, expected, state, activation)
		}
		return err
	}
	if policy.BaselineDigest != expected.BaselineDigest {
		return errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	return validateBootstrapRecoveredConfiguration(policy, state)
}

func validatePreparedTSORecovery(sm *protectedTSOStateMachine, expected etcd.WALIdentity, state etcd.RecoveredState, activation []byte) error {
	prepared, err := sm.preparedTSOConfiguration(state.AppliedIndex())
	if err != nil {
		return err
	}
	if err := state.VerifyPreparation(prepared.ManifestDigest); err != nil {
		return errors.WithStack(err)
	}
	if err := validateRuntimePreparedBootstrapIdentity(prepared, expected, activation); err != nil {
		return err
	}
	// Only C is compared here. No synthetic P is stored or published; the
	// parent's callback still verifies marker/floor zero and publishes Identity.
	configuration := raftbootstrap.PolicyRecord{Configuration: prepared.Probe.Configuration, LastConfigurationIndex: sm.AppliedConfigurationIndex()}
	return validateBootstrapRecoveredConfiguration(configuration, state)
}

// These hooks verify an already-atomic configuration recipe. They deliberately
// cannot synthesize a new P/config proof after RawNode has changed membership.
func (f *protectedTSOStateMachine) RecordAppliedNamespaceConfiguration(index uint64, members []uint64) error {
	if index != f.AppliedConfigurationIndex() {
		return errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	applied, err := tsoBackingAppliedIndex(f.backing)
	if err != nil {
		return err
	}
	return f.validateConfigurationMembers(applied, members)
}

func (f *protectedTSOStateMachine) RecoverNamespaceConfiguration(snapshotIndex uint64, members []uint64) error {
	if f.verifiedLegacyRecovery && snapshotIndex == f.verifiedLegacySnapshot {
		f.verifiedLegacyRecovery = false
		index, err := tsoBackingAppliedIndex(f.backing)
		if err != nil {
			return err
		}
		return f.validateConfigurationMembers(index, members)
	}
	if f.AppliedConfigurationIndex() == 0 || f.AppliedConfigurationIndex() > snapshotIndex {
		return errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	return f.validateConfigurationMembers(snapshotIndex, members)
}

func (f *protectedTSOStateMachine) validateConfigurationMembers(boundary uint64, members []uint64) error {
	index, err := tsoBackingAppliedIndex(f.backing)
	if err != nil {
		return err
	}
	if index > boundary {
		return errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	policy, err := f.policyLoader()(context.Background(), f.identity, index)
	if err != nil {
		if !errors.Is(err, store.ErrKeyNotFound) {
			return err
		}
		prepared, err := f.preparedTSOConfiguration(index)
		if err != nil {
			return err
		}
		return matchTSOConfigurationMembers(prepared.Probe.Configuration, members)
	}
	return matchTSOConfigurationMembers(policy.Configuration, members)
}

func matchTSOConfigurationMembers(configuration raftbootstrap.Configuration, members []uint64) error {
	want := make([]uint64, 0, len(configuration.Members))
	for _, member := range configuration.Members {
		want = append(want, member.ID)
	}
	got := slices.Clone(members)
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		return errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	return nil
}

var _ raftengine.NamespaceConfigurationAware = (*protectedTSOStateMachine)(nil)
var _ raftengine.ApplyNativeConfigurationAware = (*protectedTSOStateMachine)(nil)
var _ raftengine.ApplyAdmissionChangeAware = (*protectedTSOStateMachine)(nil)
