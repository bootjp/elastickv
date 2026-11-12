package main

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func TestNamespaceReplayMustRejectHaltBeforeNoop(t *testing.T) {
	t.Parallel()
	baseline, prepared := preparedTSOMigrationFixture(t)
	dir := t.TempDir()
	_, err := initializePreparedTSOBaseline(t.Context(), dir, baseline, prepared)
	require.NoError(t, err)
	st, err := store.NewPebbleStore(filepath.Join(dir, dedicatedTSOControlStoreName))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	sm, err := newProtectedTSOStateMachine(st, nil, prepared.Probe.Identity)
	require.NoError(t, err)
	entry := etcd.OfflineBaselineReplayEntry{Index: baseline.AppliedIndex + 1, Term: prepared.Probe.CommitTerm,
		Type: kv.TSOBaselineNormalEntry, Payload: []byte{0xff}}
	native := raftengine.NativeConfiguration{}
	sm.SetApplyEntryIdentity(raftengine.NewAppliedEntryIdentity(entry.Index, entry.Term, entry.Type, entry.Payload))
	result := sm.Apply(entry.Payload)
	sm.ClearApplyEntryIdentity()
	halt, ok := result.(interface{ HaltApply() error })
	require.True(t, ok)
	require.Error(t, halt.HaltApply(), "ordinary native replay must stop at this slot")
	index := entry.Index + 1
	require.NoError(t, sm.SetDurableAppliedIndex(index))
	entries := []etcd.OfflineBaselineReplayEntry{entry, {Index: index, Term: entry.Term, Type: kv.TSOBaselineNormalEntry}}
	err = compareReplayedTSOControl(t.Context(), sm, prepared, baseline, native, entries, index)
	require.Error(t, err, "a later no-op must not hide the earlier halting entry")
}

func TestProtectedTSOLegacyRecoveryRejectsAdditionalControlState(t *testing.T) {
	base, profile, _, configDir := prepareTSOEnablementProfile(t)
	dir := groupDataDir(base, "n1", 0, true)
	ownership, err := etcd.AcquireOfflineBaselineOwnership(dir)
	require.NoError(t, err)
	st, err := store.NewPebbleStore(filepath.Join(dir, dedicatedTSOControlStoreName))
	require.NoError(t, err)
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	require.NoError(t, metadata.ApplyRaftControlMetadata(t.Context(), func(ctx context.Context) error {
		return metadata.StageRaftControlMetadata(ctx, []byte("J/unexpected"), []byte("not in accepted legacy image"))
	}))
	require.NoError(t, st.Close())
	require.NoError(t, ownership.Close())
	assertTSOLegacyRecoveryRefused(t, base, configDir, profile)
}

func assertTSOLegacyRecoveryRefused(t *testing.T, base, peerConfigDir string, profile []groupSpec) {
	t.Helper()
	path := filepath.Join(groupDataDir(base, "n1", 0, true), dedicatedTSOControlStoreName)
	before, err := readOnlyTSOStoreDigest(t.Context(), path)
	require.NoError(t, err)
	bootstrapCfg := raftBootstrapConfig{peerConfigDir: peerConfigDir}
	specs, domain, err := loadProtectedBootstrapDomain("n1", base, profile, true, bootstrapCfg)
	require.NoError(t, err)
	t.Cleanup(domain.admission.Close)
	builder := shardGroupBuilder{raftID: "n1", raftDir: base, multi: true, factory: enablementFactory(), clock: kv.NewHLC(), bootstrapSpecs: specs, bootstrapCfg: bootstrapCfg}
	runtime, _, err := builder.build(profile[0])
	if runtime != nil {
		runtime.Close()
	}
	require.ErrorIs(t, err, kv.ErrTSODurableBacking, "a correct index and preparation cannot authorize unknown control rows")
	after, err := readOnlyTSOStoreDigest(t.Context(), path)
	require.NoError(t, err)
	require.Equal(t, before, after, "rejected startup must not import, erase or merge scalar/control state")
}

func assertTSOMissingReceiptRefusesRecovery(t *testing.T, base, configDir string, profile []groupSpec) {
	t.Helper()
	dir := groupDataDir(base, "n1", 0, true)
	path := filepath.Join(dir, dedicatedTSOControlStoreName)
	ownership, err := etcd.AcquireOfflineBaselineOwnership(dir)
	require.NoError(t, err)
	db, err := pebble.Open(path, nil)
	require.NoError(t, err)
	prefix := []byte(store.RaftControlMetadataPrefix + store.RaftControlReceiptKeyPrefix)
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix})
	require.NoError(t, err)
	require.True(t, iter.First(), "activation must have produced a durable exact receipt")
	require.True(t, bytes.HasPrefix(iter.Key(), prefix))
	key := bytes.Clone(iter.Key())
	value, err := iter.ValueAndErr()
	require.NoError(t, err)
	value = bytes.Clone(value)
	require.NoError(t, iter.Close())
	require.NoError(t, db.Delete(key, pebble.Sync))
	require.NoError(t, db.Close())
	require.NoError(t, ownership.Close())
	assertTSOLegacyRecoveryRefused(t, base, configDir, profile)
	// Restore only the exact receipt removed by this fault injection, without
	// changing P, scalars, the applied index, or any native snapshot/WAL bytes.
	ownership, err = etcd.AcquireOfflineBaselineOwnership(dir)
	require.NoError(t, err)
	db, err = pebble.Open(path, nil)
	require.NoError(t, err)
	require.NoError(t, db.Set(key, value, pebble.Sync))
	require.NoError(t, db.Close())
	require.NoError(t, ownership.Close())
}
