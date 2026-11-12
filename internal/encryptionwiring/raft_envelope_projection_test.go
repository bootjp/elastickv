package encryptionwiring

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type bootstrapProjectionFixture struct {
	in       BootstrapEncryptionProjectionInput
	sidecar  *encryption.Sidecar
	registry encryption.WriterRegistryStore
	sink     interface {
		SaveEncryptionSnapshotCheckpoint(encryption.SnapshotCheckpoint) error
		SetDurableAppliedIndex(uint64) error
	}
}

func newBootstrapProjectionFixture(t *testing.T) *bootstrapProjectionFixture {
	t.Helper()
	st, err := store.NewPebbleStore(filepath.Join(t.TempDir(), "db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	reg, err := store.WriterRegistryFor(st)
	require.NoError(t, err)
	sink, ok := st.(interface {
		SaveEncryptionSnapshotCheckpoint(encryption.SnapshotCheckpoint) error
		SetDurableAppliedIndex(uint64) error
	})
	require.True(t, ok)
	f := &bootstrapProjectionFixture{
		in: BootstrapEncryptionProjectionInput{
			RuntimeFence: func() (uint64, error) { return 1, nil },
			Configured:   true, SidecarPath: filepath.Join(t.TempDir(), "keys.json"),
			LocalStore: st, FullNodeID: 123, StorageEpoch: 7, RaftEpoch: 9,
			StorageWrapperInstalled: func(id uint32, epoch uint16) bool { return id == 1 && epoch == 7 },
			RaftWrapperInstalled:    func(id uint32, epoch uint16) bool { return id == 2 && epoch == 9 },
		},
		sidecar: &encryption.Sidecar{
			Version: encryption.SidecarVersion, RaftAppliedIndex: 30,
			StorageEnvelopeActive: true, StorageEnvelopeCutoverIndex: 10, RaftEnvelopeCutoverIndex: 20,
			Active: encryption.ActiveKeys{Storage: 1, Raft: 2},
			Keys: map[string]encryption.SidecarKey{
				"1": {Purpose: encryption.SidecarPurposeStorage, Wrapped: []byte{1}, LocalEpoch: 7},
				"2": {Purpose: encryption.SidecarPurposeRaft, Wrapped: []byte{2}, LocalEpoch: 9},
			},
		},
		registry: reg, sink: sink,
	}
	f.persist(t)
	require.NoError(t, sink.SetDurableAppliedIndex(40))
	f.register(t, 1, 123, 7)
	f.register(t, 2, 123, 9)
	return f
}

func (f *bootstrapProjectionFixture) persist(t *testing.T) {
	t.Helper()
	require.NoError(t, encryption.WriteSidecar(f.in.SidecarPath, f.sidecar))
	cp, err := encryption.SidecarCheckpoint(f.sidecar)
	require.NoError(t, err)
	require.NoError(t, f.sink.SaveEncryptionSnapshotCheckpoint(cp))
}

func (f *bootstrapProjectionFixture) register(t *testing.T, id uint32, node uint64, epoch uint16) {
	t.Helper()
	require.NoError(t, f.registry.SetRegistryRow(encryption.RegistryKey(id, encryption.NodeID16(node)),
		encryption.EncodeRegistryValue(encryption.RegistryValue{
			FullNodeID: node, FirstSeenLocalEpoch: epoch, LastSeenLocalEpoch: epoch,
		})))
}

func TestBootstrapEncryptionProjectionNotRequested(t *testing.T) {
	t.Parallel()
	got, err := ReadBootstrapEncryptionProjection(BootstrapEncryptionProjectionInput{})
	require.NoError(t, err)
	require.Equal(t, BootstrapEncryptionProjection{
		Kind: bootstrapEncryptionNotRequested, Rotation: bootstrapRotationNotRequested,
	}, got)
	for _, configured := range []bool{false, true} {
		got, err = ReadBootstrapEncryptionProjection(BootstrapEncryptionProjectionInput{
			Configured: configured, RotationRequested: true,
		})
		require.ErrorIs(t, err, ErrBootstrapEncryptionUnsupported)
		require.Equal(t, BootstrapEncryptionProjection{}, got)
	}
}

func TestBootstrapEncryptionProjectionAppliedObservation(t *testing.T) {
	t.Parallel()
	f := newBootstrapProjectionFixture(t)
	before, err := os.ReadFile(f.in.SidecarPath)
	require.NoError(t, err)
	got, err := ReadBootstrapEncryptionProjection(f.in)
	require.NoError(t, err)
	require.Equal(t, BootstrapEncryptionObserved, got.Kind)
	require.Equal(t, bootstrapRotationNotRequested, got.Rotation)
	require.Equal(t, uint64(123), got.FullNodeID)
	require.Equal(t, uint64(40), got.AppliedIndex)
	require.Equal(t, uint64(30), got.Checkpoint.Index)
	require.Equal(t, BootstrapEncryptionPurposeProjection{
		DekID: 1, Epoch: 7, Registered: true, EnvelopeActive: true, CutoverIndex: 10, WrapperInstalled: true,
		KeyDigest: sha256.Sum256([]byte{1}), Registry: encryption.RegistryValue{FullNodeID: 123, FirstSeenLocalEpoch: 7, LastSeenLocalEpoch: 7},
	}, got.Storage)
	require.Equal(t, BootstrapEncryptionPurposeProjection{
		DekID: 2, Epoch: 9, Registered: true, EnvelopeActive: true, CutoverIndex: 20, WrapperInstalled: true,
		KeyDigest: sha256.Sum256([]byte{2}), Registry: encryption.RegistryValue{FullNodeID: 123, FirstSeenLocalEpoch: 9, LastSeenLocalEpoch: 9},
	}, got.Raft)
	after, err := os.ReadFile(f.in.SidecarPath)
	require.NoError(t, err)
	require.Equal(t, before, after, "observation must not rewrite sidecar or epochs")
	registered, err := registrationCommittedAtEpoch(f.registry, 1, 123, 7)
	require.NoError(t, err)
	require.True(t, registered)
}

func TestBootstrapEncryptionProjectionRejectsIncompleteProof(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(*testing.T, *bootstrapProjectionFixture)
	}{
		{"missing path", func(_ *testing.T, f *bootstrapProjectionFixture) { f.in.SidecarPath = "" }},
		{"missing store", func(_ *testing.T, f *bootstrapProjectionFixture) { f.in.LocalStore = nil }},
		{"missing identity", func(_ *testing.T, f *bootstrapProjectionFixture) { f.in.FullNodeID = 0 }},
		{"missing sidecar", func(t *testing.T, f *bootstrapProjectionFixture) { require.NoError(t, os.Remove(f.in.SidecarPath)) }},
		{"malformed sidecar", func(t *testing.T, f *bootstrapProjectionFixture) {
			require.NoError(t, os.WriteFile(f.in.SidecarPath, []byte("{"), 0o600))
		}},
		{"checkpoint mismatch", func(t *testing.T, f *bootstrapProjectionFixture) {
			require.NoError(t, f.sink.SaveEncryptionSnapshotCheckpoint(encryption.SnapshotCheckpoint{Index: 30}))
		}},
		{"sidecar ahead of checkpoint", func(t *testing.T, f *bootstrapProjectionFixture) {
			f.sidecar.RaftAppliedIndex++
			require.NoError(t, encryption.WriteSidecar(f.in.SidecarPath, f.sidecar))
		}},
		{"checkpoint ahead of apply", func(t *testing.T, f *bootstrapProjectionFixture) {
			f.sidecar.RaftAppliedIndex = 41
			f.persist(t)
		}},
		{"storage epoch differs", func(_ *testing.T, f *bootstrapProjectionFixture) { f.in.StorageEpoch++ }},
		{"raft epoch differs", func(_ *testing.T, f *bootstrapProjectionFixture) { f.in.RaftEpoch++ }},
		{"storage registry newer", func(t *testing.T, f *bootstrapProjectionFixture) { f.register(t, 1, 123, 8) }},
		{"raft registry older", func(t *testing.T, f *bootstrapProjectionFixture) { f.register(t, 2, 123, 8) }},
		{"full identity collision", func(t *testing.T, f *bootstrapProjectionFixture) { f.register(t, 1, 123+65536, 7) }},
		{"corrupt registry", func(t *testing.T, f *bootstrapProjectionFixture) {
			require.NoError(t, f.registry.SetRegistryRow(encryption.RegistryKey(1, 123), []byte{1}))
		}},
		{"missing storage probe", func(_ *testing.T, f *bootstrapProjectionFixture) { f.in.StorageWrapperInstalled = nil }},
		{"missing raft probe", func(_ *testing.T, f *bootstrapProjectionFixture) { f.in.RaftWrapperInstalled = nil }},
		{"storage wrapper absent", func(_ *testing.T, f *bootstrapProjectionFixture) {
			f.in.StorageWrapperInstalled = func(uint32, uint16) bool { return false }
		}},
		{"raft wrapper absent", func(_ *testing.T, f *bootstrapProjectionFixture) {
			f.in.RaftWrapperInstalled = func(uint32, uint16) bool { return false }
		}},
		{"empty wrapped material", func(t *testing.T, f *bootstrapProjectionFixture) {
			key := f.sidecar.Keys["1"]
			key.Wrapped = nil
			f.sidecar.Keys["1"] = key
			f.persist(t)
		}},
		{"cutover beyond encryption apply", func(t *testing.T, f *bootstrapProjectionFixture) {
			f.sidecar.RaftEnvelopeCutoverIndex = 31
			f.persist(t)
		}},
		{"active storage missing cutover", func(t *testing.T, f *bootstrapProjectionFixture) {
			f.sidecar.StorageEnvelopeCutoverIndex = 0
			f.persist(t)
		}},
		{"unbootstrapped", func(t *testing.T, f *bootstrapProjectionFixture) {
			f.sidecar.Active = encryption.ActiveKeys{}
			f.persist(t)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newBootstrapProjectionFixture(t)
			tc.mutate(t, f)
			got, err := ReadBootstrapEncryptionProjection(f.in)
			require.Error(t, err)
			require.Equal(t, BootstrapEncryptionProjection{}, got)
		})
	}
}

func TestBootstrapEncryptionProjectionZeroEpochNeedsDurableRow(t *testing.T) {
	t.Parallel()
	f := newBootstrapProjectionFixture(t)
	f.in.StorageEpoch = 0
	f.sidecar.Active.Storage = 3
	f.sidecar.Keys["3"] = encryption.SidecarKey{Purpose: encryption.SidecarPurposeStorage, Wrapped: []byte{3}}
	f.in.StorageWrapperInstalled = func(id uint32, epoch uint16) bool { return id == 3 && epoch == 0 }
	f.persist(t)
	got, err := ReadBootstrapEncryptionProjection(f.in)
	require.Error(t, err)
	require.Equal(t, BootstrapEncryptionProjection{}, got)
	f.register(t, 3, 123, 0)
	got, err = ReadBootstrapEncryptionProjection(f.in)
	require.NoError(t, err)
	require.True(t, got.Storage.Registered)
}

func TestBootstrapEncryptionProjectionRejectsWrapperDrift(t *testing.T) {
	t.Parallel()
	f := newBootstrapProjectionFixture(t)
	calls := 0
	f.in.RaftWrapperInstalled = func(uint32, uint16) bool {
		calls++
		return calls == 1
	}
	got, err := ReadBootstrapEncryptionProjection(f.in)
	require.Error(t, err)
	require.Equal(t, BootstrapEncryptionProjection{}, got)
}

func TestBootstrapEncryptionProjectionInactiveEnvelopes(t *testing.T) {
	t.Parallel()
	f := newBootstrapProjectionFixture(t)
	f.sidecar.StorageEnvelopeActive = false
	f.sidecar.StorageEnvelopeCutoverIndex = 0
	f.sidecar.RaftEnvelopeCutoverIndex = 0
	f.in.StorageWrapperInstalled = nil
	f.in.RaftWrapperInstalled = nil
	f.persist(t)
	got, err := ReadBootstrapEncryptionProjection(f.in)
	require.NoError(t, err)
	require.False(t, got.Storage.WrapperInstalled)
	require.False(t, got.Raft.WrapperInstalled)
	require.True(t, got.Storage.Registered)
	require.True(t, got.Raft.Registered)
}

type bootstrapProjectionReadErrorStore struct {
	err               error
	applied           uint64
	appliedPresent    bool
	checkpoint        encryption.SnapshotCheckpoint
	checkpointPresent bool
	checkpointErr     error
}

func (s bootstrapProjectionReadErrorStore) LastAppliedIndex() (uint64, bool, error) {
	return s.applied, s.appliedPresent, s.err
}

func (s bootstrapProjectionReadErrorStore) LoadEncryptionSnapshotCheckpoint() (encryption.SnapshotCheckpoint, bool, error) {
	return s.checkpoint, s.checkpointPresent, s.checkpointErr
}

func TestBootstrapEncryptionProjectionPropagatesStoreError(t *testing.T) {
	t.Parallel()
	f := newBootstrapProjectionFixture(t)
	failure := errors.New("store unavailable")
	got, err := sampleBootstrapEncryptionProjection(f.in, bootstrapProjectionReadErrorStore{err: failure}, f.registry)
	require.ErrorIs(t, err, failure)
	require.Equal(t, BootstrapEncryptionProjection{}, got)
	got, err = sampleBootstrapEncryptionProjection(f.in, bootstrapProjectionReadErrorStore{
		applied: 40, appliedPresent: true, checkpointErr: failure,
	}, f.registry)
	require.ErrorIs(t, err, failure)
	require.Equal(t, BootstrapEncryptionProjection{}, got)
}

func TestBootstrapEncryptionProjectionCheckpointExcludesEpoch(t *testing.T) {
	t.Parallel()
	f := newBootstrapProjectionFixture(t)
	for id, key := range f.sidecar.Keys {
		key.LocalEpoch++
		f.sidecar.Keys[id] = key
	}
	// The durable checkpoint still matches: epoch validation is independent.
	require.NoError(t, encryption.WriteSidecar(f.in.SidecarPath, f.sidecar))
	got, err := ReadBootstrapEncryptionProjection(f.in)
	require.ErrorContains(t, err, "epoch mismatch")
	require.Equal(t, BootstrapEncryptionProjection{}, got)
}

func TestBootstrapEncryptionProjectionMissingDurableProof(t *testing.T) {
	t.Parallel()
	f := newBootstrapProjectionFixture(t)
	for _, source := range []bootstrapProjectionReadErrorStore{
		{},
		{appliedPresent: true},
		{applied: 40, appliedPresent: true},
		{applied: 40, appliedPresent: true, checkpointPresent: true},
	} {
		got, err := sampleBootstrapEncryptionProjection(f.in, source, f.registry)
		require.Error(t, err)
		require.Equal(t, BootstrapEncryptionProjection{}, got)
	}
}

func TestBootstrapEncryptionProjectionRejectsAppliedDrift(t *testing.T) {
	t.Parallel()
	f := newBootstrapProjectionFixture(t)
	f.in.RaftWrapperInstalled = func(uint32, uint16) bool {
		require.NoError(t, f.sink.SetDurableAppliedIndex(41))
		return true
	}
	got, err := ReadBootstrapEncryptionProjection(f.in)
	require.ErrorContains(t, err, "changed during observation")
	require.Equal(t, BootstrapEncryptionProjection{}, got)
}
