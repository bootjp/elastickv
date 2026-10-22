package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestEncryptionMutatorsEnabledUsesLoadedKEKState(t *testing.T) {
	previousEnabled := *encryptionEnabled
	previousSidecar := *encryptionSidecarPath
	t.Cleanup(func() {
		*encryptionEnabled = previousEnabled
		*encryptionSidecarPath = previousSidecar
	})

	*encryptionEnabled = true
	*encryptionSidecarPath = "/data/encryption/keys.json"
	require.True(t, encryptionMutatorsEnabled(true))
	require.False(t, encryptionMutatorsEnabled(false))
	*encryptionEnabled = false
	require.False(t, encryptionMutatorsEnabled(true))
	*encryptionEnabled = true
	*encryptionSidecarPath = ""
	require.False(t, encryptionMutatorsEnabled(true))
}

type failingStartupKEK struct{}

func (failingStartupKEK) Wrap([]byte) ([]byte, error) { return nil, errors.New("provider unavailable") }
func (failingStartupKEK) Unwrap([]byte) ([]byte, error) {
	return nil, errors.New("provider unavailable")
}
func (failingStartupKEK) Name() string { return "failing" }

func TestVerifyKEKBeforeMutators(t *testing.T) {
	previousEnabled := *encryptionEnabled
	previousSidecar := *encryptionSidecarPath
	t.Cleanup(func() {
		*encryptionEnabled = previousEnabled
		*encryptionSidecarPath = previousSidecar
	})

	*encryptionEnabled = true
	*encryptionSidecarPath = "/data/encryption/keys.json"
	require.ErrorIs(t, verifyKEKBeforeMutators(failingStartupKEK{}), kek.ErrKEKPreflightFailed)

	*encryptionEnabled = false
	require.NoError(t, verifyKEKBeforeMutators(failingStartupKEK{}))
	*encryptionEnabled = true
	*encryptionSidecarPath = ""
	require.NoError(t, verifyKEKBeforeMutators(failingStartupKEK{}))
}

func TestStartupKEKSourceWiringCachesGuardUnwrapThroughHydration(t *testing.T) {
	previousEnabled := *encryptionEnabled
	previousSidecar := *encryptionSidecarPath
	previousFile := *kekFile
	previousURI := *kekURI
	t.Cleanup(func() {
		*encryptionEnabled = previousEnabled
		*encryptionSidecarPath = previousSidecar
		*kekFile = previousFile
		*kekURI = previousURI
	})

	staticKEK := bytes.Repeat([]byte{0x71}, encryption.KeySize)
	*kekFile = filepath.Join(t.TempDir(), "kek")
	require.NoError(t, os.WriteFile(*kekFile, staticKEK, 0o600))
	*kekURI = ""
	*encryptionEnabled = true
	*encryptionSidecarPath = filepath.Join(t.TempDir(), encryption.SidecarFilename)

	source, err := kek.NewWrapperFromSources(context.Background(), *kekFile, "")
	require.NoError(t, err)
	dek := bytes.Repeat([]byte{0x72}, encryption.KeySize)
	wrapped, err := source.Wrap(dek)
	require.NoError(t, err)
	sidecar := &encryption.Sidecar{
		Version: encryption.SidecarVersion,
		Keys: map[string]encryption.SidecarKey{
			"7": {Purpose: encryption.SidecarPurposeStorage, Wrapped: wrapped},
		},
	}
	require.NoError(t, encryption.WriteSidecar(*encryptionSidecarPath, sidecar))

	observer := &countingKEKObserver{}
	wrapper, err := loadKEKAndRunStartupGuards(observer)
	require.NoError(t, err)
	require.IsType(t, &encryption.StartupUnwrapCache{}, wrapper,
		"production source wiring must retain the startup cache")
	guardUnwraps := observer.total()
	require.GreaterOrEqual(t, guardUnwraps, 2, "preflight and sidecar guards must reach the provider")

	keystore := encryption.NewKeystore()
	require.NoError(t, encryption.HydrateKeystoreFromSidecar(keystore, wrapper, sidecar))
	require.Equal(t, guardUnwraps, observer.total(),
		"hydration must reuse the sidecar unwrap from the guard")

	encryption.SealStartupUnwrapCache(wrapper)
	_, err = wrapper.Unwrap(wrapped)
	require.NoError(t, err)
	require.Equal(t, guardUnwraps+1, observer.total(),
		"the retained wrapper must stop caching after startup")
}

func TestEnvKEKBootstrapCutoverSnapshotRestore(t *testing.T) {
	staticKEK := bytes.Repeat([]byte{0x71}, encryption.KeySize)
	t.Setenv(kek.EnvVar, base64.StdEncoding.EncodeToString(staticKEK))
	wrapper, err := kek.NewWrapperFromSources(context.Background(), "", "")
	require.NoError(t, err)
	require.NoError(t, kek.VerifyWrapper(wrapper))

	storageDEK := bytes.Repeat([]byte{0x72}, encryption.KeySize)
	raftDEK := bytes.Repeat([]byte{0x73}, encryption.KeySize)
	wrappedStorage, err := wrapper.Wrap(storageDEK)
	require.NoError(t, err)
	wrappedRaft, err := wrapper.Wrap(raftDEK)
	require.NoError(t, err)

	dir := t.TempDir()
	sidecarPath := dir + "/keys.json"
	keystore := encryption.NewKeystore()
	wiring, err := buildEncryptionWriteWiring(true, "n1", sidecarPath, wrapper, keystore, []groupSpec{{id: 1}}, nil)
	require.NoError(t, err)
	source, err := store.NewPebbleStore(dir+"/source", wiring.pebbleOptions()...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })
	registry, err := store.WriterRegistryFor(source)
	require.NoError(t, err)
	applier, err := encryption.NewApplier(
		registry,
		encryption.WithKEK(wrapper),
		encryption.WithKeystore(keystore),
		encryption.WithSidecarPath(sidecarPath),
		encryption.WithStateCache(wiring.cache),
	)
	require.NoError(t, err)

	require.NoError(t, applier.ApplyBootstrap(1, fsmwire.BootstrapPayload{
		StorageDEKID:   e2eStorageDEKID,
		WrappedStorage: wrappedStorage,
		RaftDEKID:      e2eRaftDEKID,
		WrappedRaft:    wrappedRaft,
		BatchRegistry: []fsmwire.RegistrationPayload{
			{DEKID: e2eStorageDEKID, FullNodeID: e2eNodeID, LocalEpoch: 0},
		},
	}))
	require.NoError(t, applier.ApplyRotation(2, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableStorageEnvelope,
		DEKID:                e2eStorageDEKID,
		Purpose:              fsmwire.PurposeStorage,
		Wrapped:              []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: e2eStorageDEKID, FullNodeID: e2eNodeID, LocalEpoch: 1},
	}))
	wiring.cache.MarkRegistered(e2eStorageDEKID)
	require.NoError(t, source.PutAt(context.Background(), []byte("env-kek"), []byte("secret"), 100, 0))

	snapshot, err := source.Snapshot()
	require.NoError(t, err)
	var snapshotBytes bytes.Buffer
	_, err = snapshot.WriteTo(&snapshotBytes)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())

	target, err := store.NewPebbleStore(dir+"/target", wiring.pebbleOptions()...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = target.Close() })
	require.NoError(t, target.Restore(bytes.NewReader(snapshotBytes.Bytes())))
	got, err := target.GetAt(context.Background(), []byte("env-kek"), 100)
	require.NoError(t, err)
	require.Equal(t, []byte("secret"), got)
}
