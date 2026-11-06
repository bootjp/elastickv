package main

import (
	"bytes"
	"context"
	"crypto"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func protectedTSOFixture(t *testing.T, dir string) (*groupBootstrapSpec, *bootstrapDomain, raftbootstrap.PolicyRecord) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0700))
	raw := bootstrapConfigFixture(t)
	raw.Local = etcd.DeriveNodeID("n1")
	raw.Members[0].ID = raw.Local
	raw.Members[0].Groups = []uint64{dedicatedTSORaftGroupID}
	config, err := buildBootstrapPeerConfig(raw)
	require.NoError(t, err)
	intent := raftbootstrap.Intent{Identity: raftbootstrap.Identity{Cluster: config.local.Cluster, Group: dedicatedTSORaftGroupID},
		BaselineKind: raftbootstrap.Accepted, Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1},
		Floor: raftbootstrap.Floor{ActivationIndex: 2, Generation: 1},
		Baseline: raftbootstrap.Configuration{Index: 1, State: raftbootstrap.ConfState{Voters: []uint64{config.local.Member}},
			Members: []raftbootstrap.Member{{ID: config.local.Member, Address: config.local.Address, PublicKeyDigest: config.local.Key}}}}
	digest, err := raftbootstrap.ConfigurationDigest(intent.Baseline)
	require.NoError(t, err)
	policy := raftbootstrap.PolicyRecord{Version: 1, Identity: intent.Identity, BaselineDigest: digest,
		Minimum: intent.Minimum, ActivationIndex: 2, Generation: 1, LastConfigurationIndex: 1, Configuration: intent.Baseline}
	files := raftbootstrap.NewStore(dir)
	projection := raftbootstrap.Policy{Identity: intent.Identity, BaselineDigest: digest, Minimum: intent.Minimum, Floor: intent.Floor}
	require.NoError(t, files.Publish(intent, raftbootstrap.Evidence{Identity: &intent.Identity, BaselineDigest: &digest, RaftState: raftbootstrap.Intact, Policy: &projection}))
	domain, err := newBootstrapDomain(intent.Identity.Cluster, []groupSpec{{id: dedicatedTSORaftGroupID}, {id: 1}})
	require.NoError(t, err)
	t.Cleanup(domain.admission.Close)
	return &groupBootstrapSpec{config: config, files: files, intent: intent, dir: dir, domain: domain}, domain, policy
}

func seedProtectedTSO(t *testing.T, st store.MVCCStore, policy raftbootstrap.PolicyRecord) *kv.TSOStateMachine {
	t.Helper()
	backing, ok := st.(kv.TSODurableBacking)
	require.True(t, ok)
	sm, err := kv.NewDurableTSOStateMachine(nil, backing)
	require.NoError(t, err)
	require.NoError(t, sm.ApplyConfigurationWithControl(t.Context(), 1, nil))
	key, err := kv.BootstrapPolicyMetadataKey(policy.Identity)
	require.NoError(t, err)
	raw, err := raftbootstrap.MarshalPolicyRecord(policy)
	require.NoError(t, err)
	require.Nil(t, sm.ApplyWithControl(t.Context(), 2, nil, func(ctx context.Context, metadata store.RaftControlMetadataStore) error {
		return metadata.StageRaftControlMetadata(ctx, key, raw)
	}))
	require.NoError(t, sm.SetDurableAppliedIndex(7))
	return sm
}

func protectedTSOStore(t *testing.T) store.MVCCStore {
	t.Helper()
	st, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	return st
}

func TestProtectedTSOPolicyRecoveryUsesDurableBacking(t *testing.T) {
	t.Parallel()
	spec, _, policy := protectedTSOFixture(t, t.TempDir())
	st := protectedTSOStore(t)
	seedProtectedTSO(t, st, policy)
	sm, err := newProtectedTSOStateMachine(st, nil, policy.Identity)
	require.NoError(t, err)
	got, err := sm.policyLoader()(t.Context(), policy.Identity, 7)
	require.NoError(t, err)
	wantBytes, err := raftbootstrap.MarshalPolicyRecord(policy)
	require.NoError(t, err)
	gotBytes, err := raftbootstrap.MarshalPolicyRecord(got)
	require.NoError(t, err)
	require.Equal(t, wantBytes, gotBytes)
	require.NoError(t, sm.RecordAppliedNamespaceConfiguration(1, []uint64{spec.config.local.Member}))
	require.NoError(t, sm.RecoverNamespaceConfiguration(7, []uint64{spec.config.local.Member}))
	require.Error(t, sm.RecoverNamespaceConfiguration(7, []uint64{spec.config.local.Member + 1}))
	require.Error(t, sm.RecordAppliedNamespaceConfiguration(8, []uint64{spec.config.local.Member}))
	require.Error(t, sm.RecoverNamespaceConfiguration(0, []uint64{spec.config.local.Member}))
	_, err = sm.policyLoader()(t.Context(), policy.Identity, 6)
	require.Error(t, err, "native boundary must equal durable applied index")
	wrong := policy.Identity
	wrong.Cluster[0]++
	_, err = sm.policyLoader()(t.Context(), wrong, 7)
	require.Error(t, err)
	expected := etcd.WALIdentity{Identity: policy.Identity, NodeID: spec.config.local.Member, BaselineDigest: policy.BaselineDigest}
	require.ErrorIs(t, validateProtectedTSORecovery(sm, expected, etcd.RecoveredState{}), etcd.ErrWALIdentity)
}

func TestProtectedTSORejectsMissingOrInconsistentState(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"empty", "policy-only", "scalars-only", "configuration-mismatch"} {
		t.Run(kind, func(t *testing.T) {
			_, _, policy := protectedTSOFixture(t, t.TempDir())
			st := protectedTSOStore(t)
			backing, ok := st.(kv.TSODurableBacking)
			require.True(t, ok)
			switch kind {
			case "empty":
			case "policy-only":
				key, err := kv.BootstrapPolicyMetadataKey(policy.Identity)
				require.NoError(t, err)
				raw, err := raftbootstrap.MarshalPolicyRecord(policy)
				require.NoError(t, err)
				require.NoError(t, backing.ApplyRaftEntry(t.Context(), 7, func(ctx context.Context) error {
					return backing.StageRaftControlMetadata(ctx, key, raw)
				}))
			case "scalars-only":
				sm, err := kv.NewDurableTSOStateMachine(nil, backing)
				require.NoError(t, err)
				require.NoError(t, sm.SetDurableAppliedIndex(7))
			case "configuration-mismatch":
				sm := seedProtectedTSO(t, st, policy)
				require.NoError(t, sm.ApplyConfigurationWithControl(t.Context(), 8, nil))
			}
			_, err := newProtectedTSOStateMachine(st, nil, policy.Identity)
			require.Error(t, err)
		})
	}
}

func TestProtectedTSOFactoryNeverAutoMigrates(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	dir := groupDataDir(base, "n1", dedicatedTSORaftGroupID, true)
	spec, domain, _ := protectedTSOFixture(t, dir)
	group := groupSpec{id: dedicatedTSORaftGroupID, address: spec.config.local.Address}
	factory := etcd.NewFactory(etcd.FactoryConfig{})
	runtime, shard, err := buildProtectedDedicatedTSOGroup("n1", group, base, true, false,
		nil, nil, factory, nil, nil, spec, domain, nil)
	require.ErrorContains(t, err, "explicitly migrated control store")
	require.Nil(t, runtime)
	require.Nil(t, shard)
	require.NoDirExists(t, filepath.Join(dir, dedicatedTSOControlStoreName))
	_, _, err = buildProtectedDedicatedTSOGroup("n1", group, base, true, false,
		nil, nil, factory, nil, nil, nil, domain, nil)
	require.Error(t, err)
	_, _, err = buildProtectedDedicatedTSOGroup("n1", group, base, true, false,
		nil, nil, factory, nil, nil, spec, nil, nil)
	require.Error(t, err)
	_, _, err = buildProtectedDedicatedTSOGroup("n1", group, base, true, false,
		nil, nil, nil, nil, nil, spec, domain, nil)
	require.Error(t, err)
	servers := []raftengine.Server{{ID: "wrong-node", Address: group.address}}
	_, _, err = buildProtectedDedicatedTSOGroup("wrong-node", group, base, false, true,
		servers, servers, factory, kv.NewHLC(), protectedTSOTestObserver{}, spec, domain,
		[]string{"/explicit/public/method"}, store.WithSSTIngestSnapshots(true))
	require.Error(t, err)
}

type protectedTSOTestObserver struct{}

func (protectedTSOTestObserver) ObserveProposalFailure() {}

func TestProtectedTSOFactoryRejectsEmptyControlDatabase(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	dir := groupDataDir(base, "n1", dedicatedTSORaftGroupID, true)
	spec, domain, _ := protectedTSOFixture(t, dir)
	path := filepath.Join(dir, dedicatedTSOControlStoreName)
	st, err := store.NewPebbleStore(path)
	require.NoError(t, err)
	require.NoError(t, st.Close())
	factory := etcd.NewFactory(etcd.FactoryConfig{})
	group := groupSpec{id: dedicatedTSORaftGroupID, address: spec.config.local.Address}
	_, _, err = buildProtectedDedicatedTSOGroup("n1", group, base, true, false,
		nil, nil, factory, nil, nil, spec, domain, nil)
	require.Error(t, err)
	// A failed constructor releases the DB lock and never imports scalar state.
	st, err = store.NewPebbleStore(path)
	require.NoError(t, err)
	require.NoError(t, st.Close())
	_, err = os.Stat(filepath.Join(dir, "wal"))
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(dir, "member", "wal"))
	require.True(t, os.IsNotExist(err))
}

func TestProtectedTSOStartupRegistersSharedDomain(t *testing.T) {
	t.Parallel()
	spec, domain, policy := protectedTSOFixture(t, t.TempDir())
	st := protectedTSOStore(t)
	seedProtectedTSO(t, st, policy)
	sm, err := newProtectedTSOStateMachine(st, nil, policy.Identity)
	require.NoError(t, err)
	transport := etcd.NewGRPCTransport([]etcd.Peer{{ID: "n1", NodeID: spec.config.local.Member, Address: spec.config.local.Address}})
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	setup := newProtectedTSOStartup(spec, st, sm, domain, nil)
	startup, err := setup(raftengine.FactoryConfig{GroupID: dedicatedTSORaftGroupID, LocalID: "n1", LocalAddress: spec.config.local.Address}, transport)
	require.NoError(t, err)
	t.Cleanup(startup.Close)
	require.Same(t, domain.admission, startup.NativeAdmission)
	require.NotNil(t, domain.owners[dedicatedTSORaftGroupID])
	require.False(t, startup.PublicReady())
	require.ErrorIs(t, startup.RecoveredAdmission(etcd.RecoveredState{}), etcd.ErrWALIdentity)
	require.False(t, startup.PublicReady())
}

func preparedTSOFixture(t *testing.T, commit uint64) (*groupBootstrapSpec, *bootstrapDomain, store.MVCCStore, kv.PreparedBootstrapActivation) {
	t.Helper()
	spec, domain, _ := protectedTSOFixture(t, t.TempDir())
	st := protectedTSOStore(t)
	backing, ok := st.(kv.TSODurableBacking)
	require.True(t, ok)
	legacy := kv.NewTSOStateMachine(nil)
	require.Nil(t, legacy.Apply(binary.BigEndian.AppendUint64([]byte("\x07TSOF\x01"), 42)))
	snapshot, err := legacy.Snapshot()
	require.NoError(t, err)
	var image bytes.Buffer
	_, err = snapshot.WriteTo(&image)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())
	state, err := kv.NewDurableTSOStateMachine(nil, backing)
	require.NoError(t, err)
	require.NoError(t, state.InitializeLegacyControl(t.Context(), commit, 1, &image, nil))
	c := raftbootstrap.BaselineContext{Version: raftbootstrap.BaselineVersion, Identity: spec.intent.Identity,
		OperationID: raftbootstrap.OperationID{9}, Configuration: spec.intent.Baseline, Minimum: spec.intent.Minimum,
		CommitIndex: commit, CommitTerm: 1, Watermark: 42, DataDigest: raftbootstrap.Digest{8}}
	configurationDigest, err := raftbootstrap.ConfigurationDigest(c.Configuration)
	require.NoError(t, err)
	signer, ok := spec.config.clientTLS.Certificates[0].PrivateKey.(crypto.Signer)
	require.True(t, ok)
	probe, err := raftbootstrap.SignBaselineProbe(raftbootstrap.BaselineProbe{BaselineContext: c,
		Evidence: raftbootstrap.BaselineMemberEvidence{MemberID: spec.config.local.Member,
			CommitIndex: commit, CommitTerm: 1, AppliedIndex: commit, LastLogIndex: commit, ConfigurationDigest: configurationDigest,
			NativeStateDigest: raftbootstrap.Digest{7}, DataDigest: c.DataDigest, Watermark: c.Watermark}}, signer)
	require.NoError(t, err)
	manifest := raftbootstrap.BaselineManifest{BaselineContext: c, Members: []raftbootstrap.BaselineMemberEvidence{probe.Evidence}}
	digest, err := raftbootstrap.BaselineManifestDigest(manifest)
	require.NoError(t, err)
	prepared := kv.PreparedBootstrapActivation{Probe: probe, ManifestDigest: digest}
	require.NoError(t, kv.StagePreparedBootstrapActivation(t.Context(), backing, c.Identity, prepared))
	return spec, domain, st, prepared
}

func TestPreparedTSOExplicitFirstActivation(t *testing.T) {
	t.Parallel()
	spec, _, st, prepared := preparedTSOFixture(t, 7)
	c := prepared.Probe.BaselineContext
	configurationDigest := prepared.Probe.Evidence.ConfigurationDigest
	manifest := raftbootstrap.BaselineManifest{BaselineContext: c, Members: []raftbootstrap.BaselineMemberEvidence{prepared.Probe.Evidence}}
	proof := func(context.Context) (kv.BootstrapActivationNativeProof, error) {
		return kv.BootstrapActivationNativeProof{Identity: c.Identity, Configuration: c.Configuration}, nil
	}
	unready, err := newProtectedTSOStateMachine(st, nil, c.Identity)
	require.NoError(t, err, "prepared state may proceed to native v2 verification")
	_, err = unready.policyLoader()(t.Context(), c.Identity, 7)
	require.ErrorIs(t, err, store.ErrKeyNotFound, "no synthetic policy is created")
	require.NoError(t, unready.RecoverNamespaceConfiguration(7, []uint64{spec.config.local.Member}))
	require.Error(t, unready.RecoverNamespaceConfiguration(7, []uint64{spec.config.local.Member + 1}))
	sm, err := newPreparedTSOStateMachine(st, nil, c.Identity, nil)
	require.NoError(t, err)
	command := raftbootstrap.ActivationCommand{Manifest: manifest, Witness: raftbootstrap.Witness{
		Cluster: c.Identity.Cluster, Group: c.Identity.Group, ConfigurationIndex: 1, ConfigurationDigest: configurationDigest}}
	preparedImage, err := store.ProbeBootstrapBaseline(t.Context(), st)
	require.NoError(t, err)
	signer, ok := spec.config.clientTLS.Certificates[0].PrivateKey.(crypto.Signer)
	require.True(t, ok)
	ack, err := raftbootstrap.SignPreparedMemberAck(raftbootstrap.PreparedMemberAck{MemberID: spec.config.local.Member,
		ManifestDigest: prepared.ManifestDigest, Minimum: c.Minimum, PreparedStateDigest: raftbootstrap.Digest(preparedImage.DataDigest)}, manifest, signer)
	require.NoError(t, err)
	command.Readiness = raftbootstrap.PreparedCertificate{ManifestDigest: prepared.ManifestDigest, Members: []raftbootstrap.PreparedMemberAck{ack}}
	payload, err := raftbootstrap.EncodeActivationCommand(command)
	require.NoError(t, err)
	changes, err := sm.ApplyChangesAdmission(payload)
	require.NoError(t, err)
	require.True(t, changes)
	sm.SetApplyNativeConfiguration(raftengine.NativeConfiguration{Index: 1, Voters: []uint64{spec.config.local.Member},
		Members: []raftengine.NativeConfigurationMember{{ID: spec.config.local.Member, Address: spec.config.local.Address}}})
	sm.SetApplyEntryIdentity(raftengine.NewAppliedEntryIdentity(8, 2, 0, payload))
	result := sm.Apply(payload)
	sm.ClearApplyEntryIdentity()
	receipt, ok := result.(raftbootstrap.ActivationReceipt)
	require.True(t, ok)
	require.Equal(t, raftbootstrap.ActivationReceiptApplied, receipt.Verdict)
	policy, err := kv.LoadAppliedBootstrapPolicy(t.Context(), st, c.Identity, 8)
	require.NoError(t, err)
	require.Equal(t, uint64(8), policy.ActivationIndex)
	require.Equal(t, uint64(42), sm.AllocationFloor())
	changes, err = sm.ApplyChangesAdmission(payload)
	require.NoError(t, err)
	require.False(t, changes, "already-active rejection must not revoke healthy sessions")
	_, err = newProtectedTSOStateMachine(st, nil, c.Identity)
	require.NoError(t, err)
	_, err = newPreparedTSOStateMachine(st, nil, c.Identity, proof)
	require.Error(t, err, "explicit preparation cannot replace active policy")
}

func TestPreparedTSORecoveryRejectsFreshNativeSource(t *testing.T) {
	t.Parallel()
	for _, version := range []string{"v1", "v2", "wrong-v2"} {
		t.Run(version, func(t *testing.T) {
			spec, domain, st, prepared := preparedTSOFixture(t, 1)
			sm, err := newProtectedTSOStateMachine(st, nil, spec.intent.Identity)
			require.NoError(t, err)
			expected := etcd.WALIdentity{Identity: spec.intent.Identity, NodeID: spec.config.local.Member,
				BaselineDigest: prepared.Probe.Evidence.ConfigurationDigest}
			if version == "v2" {
				expected.PreparationDigest = prepared.ManifestDigest
			}
			if version == "wrong-v2" {
				expected.PreparationDigest = raftbootstrap.Digest{99}
			}
			called := false
			engine, err := etcd.Open(t.Context(), etcd.OpenConfig{GroupID: dedicatedTSORaftGroupID,
				NodeID: spec.config.local.Member, LocalID: "n1", LocalAddress: spec.config.local.Address,
				DataDir: t.TempDir(), Bootstrap: true, StateMachine: sm, NativeIdentity: &expected, NativeAdmission: domain.admission,
				RecoveredAdmission: func(state etcd.RecoveredState) error {
					called = true
					return validateProtectedTSORecovery(sm, expected, state)
				}})
			if engine != nil {
				require.NoError(t, engine.Close())
			}
			require.Equal(t, version == "v1", called, "v2 must be migrated, never bootstrapped from an empty source")
			require.ErrorIs(t, err, etcd.ErrWALIdentity)
			key, keyErr := kv.BootstrapPolicyMetadataKey(spec.intent.Identity)
			require.NoError(t, keyErr)
			metadata, ok := st.(store.RaftControlMetadataStore)
			require.True(t, ok)
			_, err = metadata.ReadRaftControlMetadata(t.Context(), key)
			require.ErrorIs(t, err, store.ErrKeyNotFound, "prepared recovery must not install P")
		})
	}
}
