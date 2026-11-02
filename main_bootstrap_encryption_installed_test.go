package main

import (
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type bootstrapInstalledFixture struct {
	*bootstrapProjectionFixture
	keys    *encryption.Keystore
	runtime *raftEnvelopeRuntime
	group   *kv.ShardGroup
}

func newBootstrapInstalledFixture(t *testing.T) *bootstrapInstalledFixture {
	t.Helper()
	f := newBootstrapProjectionFixture(t)
	keys := encryption.NewKeystore()
	require.NoError(t, keys.Set(1, make([]byte, encryption.KeySize)))
	require.NoError(t, keys.Set(2, make([]byte, encryption.KeySize)))
	cipher, err := encryption.NewCipher(keys)
	require.NoError(t, err)
	st, err := store.NewPebbleStore(filepath.Join(t.TempDir(), "installed"),
		store.WithEncryption(cipher, encryption.NewDeterministicNonceFactory(123, 7), func() (uint32, bool) { return 1, true }),
		store.WithStorageEnvelopeGate(func() bool { return true }),
		store.WithStorageRegistrationGate(func() bool { return true }))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	f.in.localStore = st
	f.registry, err = store.WriterRegistryFor(st)
	require.NoError(t, err)
	sink, ok := st.(interface {
		SaveEncryptionSnapshotCheckpoint(encryption.SnapshotCheckpoint) error
		SetDurableAppliedIndex(uint64) error
	})
	require.True(t, ok)
	f.sink = sink
	f.persist(t)
	require.NoError(t, f.sink.SetDurableAppliedIndex(40))
	f.register(t, 1, 123, 7)
	f.register(t, 2, 123, 9)
	gate := &raftRegistrationGate{}
	gate.MarkRegistered(2, 9)
	runtime, err := newRaftEnvelopeRuntime(cipher, encryption.NewDeterministicNonceFactory(123, 9), 20, 2, &atomic.Uint64{}, 9, 123, gate)
	require.NoError(t, err)
	group := &kv.ShardGroup{Store: st}
	runtime.attachGroup(1, group)
	f.in = f.in.bindInstalledObservation(group, encryptionWriteWiring{raftEnvelope: runtime}, f.in.runtimeFence)
	f.in.rotationRequested = true
	return &bootstrapInstalledFixture{bootstrapProjectionFixture: f, keys: keys, runtime: runtime, group: group}
}

func TestBootstrapEncryptionInstalledRotationIsNotCompletion(t *testing.T) {
	f := newBootstrapInstalledFixture(t)
	got, err := readBootstrapEncryptionProjection(f.in)
	require.NoError(t, err)
	require.Equal(t, bootstrapRotationRequestedUnproven, got.rotation)
	require.True(t, got.installedEvidence)
	require.Equal(t, uint64(1), got.generation)
	second, err := readBootstrapEncryptionProjection(f.in)
	require.NoError(t, err)
	require.Equal(t, got, second)
	require.Equal(t, got.digest(), second.digest())
	f.in.rotationRequested = false
	second, err = readBootstrapEncryptionProjection(f.in)
	require.NoError(t, err)
	require.Equal(t, bootstrapRotationNotRequested, second.rotation)
	require.NotEqual(t, got.digest(), second.digest())
}

func TestBootstrapEncryptionInstalledRejectsMissingEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *bootstrapInstalledFixture)
	}{
		{"storage key missing", func(_ *testing.T, f *bootstrapInstalledFixture) { f.keys.Delete(1) }},
		{"raft key missing", func(_ *testing.T, f *bootstrapInstalledFixture) { f.keys.Delete(2) }},
		{"raft wrapper removed", func(_ *testing.T, f *bootstrapInstalledFixture) { f.group.SetRaftPayloadWrap(nil) }},
		{"raft wrapper replaced", func(_ *testing.T, f *bootstrapInstalledFixture) {
			f.group.SetRaftPayloadWrap(func(p []byte) ([]byte, error) { return p, nil })
		}},
		{"same closure republished", func(_ *testing.T, f *bootstrapInstalledFixture) {
			f.group.SetRaftPayloadWrap(f.group.RaftPayloadWrap())
		}},
		{"raft registration gate closed", func(_ *testing.T, f *bootstrapInstalledFixture) { f.runtime.registration.MarkRegistered(3, 9) }},
		{"raft nonce epoch mismatch", func(_ *testing.T, f *bootstrapInstalledFixture) {
			f.runtime.nonceFactory = encryption.NewDeterministicNonceFactory(123, 10)
		}},
		{"raft nonce node mismatch", func(_ *testing.T, f *bootstrapInstalledFixture) {
			f.runtime.nonceFactory = encryption.NewDeterministicNonceFactory(124, 9)
		}},
		{"raft full node collision", func(_ *testing.T, f *bootstrapInstalledFixture) { f.runtime.fullNodeID += 65536 }},
		{"raft wrapper epoch mismatch", func(_ *testing.T, f *bootstrapInstalledFixture) { f.runtime.raftEpoch++ }},
		{"raft cutover mismatch", func(_ *testing.T, f *bootstrapInstalledFixture) { f.runtime.cutoverIndex.Store(19) }},
		{"registry missing", func(t *testing.T, f *bootstrapInstalledFixture) {
			f.sidecar.Active.Storage = 3
			f.sidecar.Keys["3"] = encryption.SidecarKey{Purpose: encryption.SidecarPurposeStorage, Wrapped: []byte{3}, LocalEpoch: 7}
			f.persist(t)
		}},
		{"registry first epoch invalid", func(t *testing.T, f *bootstrapInstalledFixture) {
			require.NoError(t, f.registry.SetRegistryRow(encryption.RegistryKey(1, 123), encryption.EncodeRegistryValue(encryption.RegistryValue{FullNodeID: 123, FirstSeenLocalEpoch: 8, LastSeenLocalEpoch: 7})))
		}},
		{"generation missing", func(_ *testing.T, f *bootstrapInstalledFixture) { f.in.runtimeFence = nil }},
		{"generation zero", func(_ *testing.T, f *bootstrapInstalledFixture) {
			f.in.runtimeFence = func() (uint64, error) { return 0, nil }
		}},
		{"actual probe missing", func(_ *testing.T, f *bootstrapInstalledFixture) { f.in.installed = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newBootstrapInstalledFixture(t)
			tc.mutate(t, f)
			got, err := readBootstrapEncryptionProjection(f.in)
			require.Error(t, err)
			require.Equal(t, bootstrapEncryptionProjection{}, got)
		})
	}
}

func TestBootstrapEncryptionInstalledRotationRefreshesActualWrapper(t *testing.T) {
	f := newBootstrapInstalledFixture(t)
	before, err := readBootstrapEncryptionProjection(f.in)
	require.NoError(t, err)
	require.NoError(t, f.keys.Set(3, make([]byte, encryption.KeySize)))
	f.sidecar.Active.Raft = 3
	f.sidecar.RaftAppliedIndex = 31
	f.sidecar.Keys["3"] = encryption.SidecarKey{Purpose: encryption.SidecarPurposeRaft, Wrapped: []byte{3}, LocalEpoch: 9}
	f.persist(t)
	f.register(t, 3, 123, 9)
	_, err = readBootstrapEncryptionProjection(f.in)
	require.Error(t, err, "sidecar and registry are not actual wrapper installation")
	require.NoError(t, f.runtime.installRotatedRaftDEK(3))
	_, err = readBootstrapEncryptionProjection(f.in)
	require.Error(t, err, "durable row is not an open actual wrapper registration gate")
	f.runtime.registration.MarkRegistered(3, 9)
	f.in.runtimeFence = func() (uint64, error) { return 2, nil }
	after, err := readBootstrapEncryptionProjection(f.in)
	require.NoError(t, err)
	require.Equal(t, uint32(3), after.raft.dekID)
	require.True(t, after.raft.wrapperInstalled)
	require.Equal(t, bootstrapRotationRequestedUnproven, after.rotation)
	require.NotEqual(t, before.digest(), after.digest())
	f.runtime.reinstallActiveWrap()
	reinstalled, err := readBootstrapEncryptionProjection(f.in)
	require.NoError(t, err)
	require.NotEqual(t, after.raft.wrapperIdentity, reinstalled.raft.wrapperIdentity)
	require.NotEqual(t, after.digest(), reinstalled.digest())
}

func TestBootstrapEncryptionProjectionGenerationAndFatalFence(t *testing.T) {
	for _, fatal := range []bool{false, true} {
		t.Run(map[bool]string{false: "drift", true: "fatal"}[fatal], func(t *testing.T) {
			f := newBootstrapInstalledFixture(t)
			failure := errors.New("native apply halted")
			calls := 0
			f.in.runtimeFence = func() (uint64, error) {
				calls++
				if calls > 1 && fatal {
					return 0, failure
				}
				return uint64(calls), nil
			}
			got, err := readBootstrapEncryptionProjection(f.in)
			require.Error(t, err)
			if fatal {
				require.ErrorIs(t, err, failure)
			}
			require.Equal(t, bootstrapEncryptionProjection{}, got)
		})
	}
}

func TestBootstrapEncryptionProjectionDigestTracksRegistryAndGeneration(t *testing.T) {
	f := newBootstrapInstalledFixture(t)
	first, err := readBootstrapEncryptionProjection(f.in)
	require.NoError(t, err)
	f.in.runtimeFence = func() (uint64, error) { return 2, nil }
	second, err := readBootstrapEncryptionProjection(f.in)
	require.NoError(t, err)
	require.Equal(t, first.digest(), second.digest(), "observation generation is not encryption authority")
	require.NoError(t, f.registry.SetRegistryRow(encryption.RegistryKey(1, 123), encryption.EncodeRegistryValue(encryption.RegistryValue{FullNodeID: 123, FirstSeenLocalEpoch: 6, LastSeenLocalEpoch: 7})))
	third, err := readBootstrapEncryptionProjection(f.in)
	require.NoError(t, err)
	require.NotEqual(t, second.digest(), third.digest())
	require.Equal(t, uint16(7), first.storage.registry.FirstSeenLocalEpoch, "old observation cannot alias mutable registry")
}

func TestBootstrapEncryptionProjectionDigestExcludesApplyProgress(t *testing.T) {
	f := newBootstrapInstalledFixture(t)
	before, err := readBootstrapEncryptionProjection(f.in)
	require.NoError(t, err)
	f.sidecar.RaftAppliedIndex++
	f.persist(t)
	require.NoError(t, f.sink.SetDurableAppliedIndex(41))
	after, err := readBootstrapEncryptionProjection(f.in)
	require.NoError(t, err)
	require.NotEqual(t, before.appliedIndex, after.appliedIndex)
	require.NotEqual(t, before.checkpoint, after.checkpoint)
	require.Equal(t, before.digest(), after.digest(), "ordinary advancing applied/checkpoint boundaries are not authority")
}

func TestBootstrapEncryptionRuntimeInvalidatesBeforeMutation(t *testing.T) {
	f := newBootstrapInstalledFixture(t)
	calls := 0
	ended := 0
	f.runtime.setBeforeEncryptionChange(func() func() {
		calls++
		require.True(t, f.runtime.mu.TryLock(), "begin must be outside wrapper mutex")
		f.runtime.mu.Unlock()
		return func() {
			ended++
			require.True(t, f.runtime.mu.TryLock(), "end must be outside wrapper mutex")
			f.runtime.mu.Unlock()
		}
	})
	f.runtime.registration.MarkRegistered(2, 9)
	require.Equal(t, 1, calls)
	f.group.SetRaftPayloadWrap(nil)
	require.Equal(t, 2, calls)
	f.runtime.reinstallActiveWrap()
	require.Equal(t, 3, calls, "runtime uses one paired capability across actual publication")
	require.Equal(t, calls, ended)
	_, err := readBootstrapEncryptionProjection(f.in)
	require.NoError(t, err)
}

func TestBootstrapEncryptionProjectionTypedPendingAndFatal(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pending bool
		mutate  func(*testing.T, *bootstrapInstalledFixture)
	}{
		{"older local registration", true, func(t *testing.T, f *bootstrapInstalledFixture) { f.register(t, 1, 123, 6) }},
		{"missing local registration", true, func(t *testing.T, f *bootstrapInstalledFixture) {
			f.sidecar.Active.Storage = 3
			f.sidecar.Keys["3"] = encryption.SidecarKey{Purpose: encryption.SidecarPurposeStorage, Wrapped: []byte{3}, LocalEpoch: 7}
			f.persist(t)
		}},
		{"missing wrapper", true, func(_ *testing.T, f *bootstrapInstalledFixture) { f.group.SetRaftPayloadWrap(nil) }},
		{"checkpoint drift", true, func(t *testing.T, f *bootstrapInstalledFixture) {
			f.sidecar.RaftAppliedIndex++
			require.NoError(t, encryption.WriteSidecar(f.in.sidecarPath, f.sidecar))
		}},
		{"registry rollback", false, func(t *testing.T, f *bootstrapInstalledFixture) { f.register(t, 1, 123, 8) }},
		{"malformed key", false, func(t *testing.T, f *bootstrapInstalledFixture) {
			key := f.sidecar.Keys["1"]
			key.Wrapped = nil
			f.sidecar.Keys["1"] = key
			f.persist(t)
		}},
		{"missing loaded key", false, func(_ *testing.T, f *bootstrapInstalledFixture) { f.keys.Delete(1) }},
		{"wrong nonce epoch", false, func(_ *testing.T, f *bootstrapInstalledFixture) {
			f.runtime.nonceFactory = encryption.NewDeterministicNonceFactory(123, 10)
		}},
		{"native fatal", false, func(_ *testing.T, f *bootstrapInstalledFixture) {
			f.in.runtimeFence = func() (uint64, error) { return 0, encryption.ErrEncryptionApply }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newBootstrapInstalledFixture(t)
			tc.mutate(t, f)
			got, err := readBootstrapEncryptionProjection(f.in)
			require.Error(t, err)
			require.Equal(t, tc.pending, errors.Is(err, errBootstrapEncryptionProjectionPending))
			require.Zero(t, got)
			if tc.name == "registry rollback" {
				require.ErrorIs(t, err, encryption.ErrLocalEpochRollback)
			}
		})
	}
}

func TestNewBootstrapEncryptionProjectionInputBindsActualWiring(t *testing.T) {
	f := newBootstrapInstalledFixture(t)
	w := encryptionWriteWiring{cipher: f.runtime.cipher, raftEnvelope: f.runtime, epoch: 7, raftEpoch: 9}
	in, err := newBootstrapEncryptionProjectionInput(true, true, f.in.sidecarPath, f.group, w, 123, f.in.runtimeFence)
	require.NoError(t, err)
	got, err := readBootstrapEncryptionProjection(in)
	require.NoError(t, err)
	require.True(t, got.installedEvidence)
	require.Equal(t, bootstrapRotationRequestedUnproven, got.rotation)
	_, err = newBootstrapEncryptionProjectionInput(true, true, f.in.sidecarPath, f.group, encryptionWriteWiring{}, 123, f.in.runtimeFence)
	require.ErrorIs(t, err, errBootstrapEncryptionUnsupported)
	in, err = newBootstrapEncryptionProjectionInput(false, false, "", nil, encryptionWriteWiring{}, 0, nil)
	require.NoError(t, err)
	got, err = readBootstrapEncryptionProjection(in)
	require.NoError(t, err)
	require.Equal(t, bootstrapEncryptionNotRequested, got.kind)
}

func TestBootstrapEncryptionPairedMutationClosesOldCoherentImage(t *testing.T) {
	f := newBootstrapInstalledFixture(t)
	var mu sync.Mutex
	generation := uint64(1)
	unsettled := 0
	f.in.runtimeFence = func() (uint64, error) {
		mu.Lock()
		defer mu.Unlock()
		if unsettled != 0 {
			return 0, errBootstrapEncryptionProjectionPending
		}
		return generation, nil
	}
	_, err := readBootstrapEncryptionProjection(f.in)
	require.NoError(t, err)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	f.runtime.setBeforeEncryptionChange(func() func() {
		mu.Lock()
		unsettled++
		mu.Unlock()
		if !first.Swap(true) {
			close(entered)
			<-release
		}
		return func() { mu.Lock(); unsettled--; generation++; mu.Unlock() }
	})
	go func() { defer close(done); f.group.SetRaftPayloadWrap(nil) }()
	<-entered
	// Actual old wrapper is still coherent here. A before-only generation bump
	// would let it be published, but the paired fence must reject the sample.
	got, err := readBootstrapEncryptionProjection(f.in)
	require.ErrorIs(t, err, errBootstrapEncryptionProjectionPending)
	require.Zero(t, got)
	close(release)
	<-done
	_, err = readBootstrapEncryptionProjection(f.in)
	require.ErrorIs(t, err, errBootstrapEncryptionProjectionPending, "settling never repairs a missing actual wrapper")
	f.runtime.reinstallActiveWrap()
	_, err = readBootstrapEncryptionProjection(f.in)
	require.NoError(t, err)
}
