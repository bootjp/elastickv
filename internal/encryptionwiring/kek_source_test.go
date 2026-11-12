package encryptionwiring

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
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestEncryptionMutatorsEnabledUsesLoadedKEKState(t *testing.T) {
	t.Parallel()
	f := Flags{Enabled: true, SidecarPath: "/data/encryption/keys.json"}
	require.True(t, MutatorsEnabled(f, true))
	require.False(t, MutatorsEnabled(f, false))
	f.Enabled = false
	require.False(t, MutatorsEnabled(f, true))
	f.Enabled = true
	f.SidecarPath = ""
	require.False(t, MutatorsEnabled(f, true))
}

type failingStartupKEK struct{}

func (failingStartupKEK) Wrap([]byte) ([]byte, error) { return nil, errors.New("provider unavailable") }

func (failingStartupKEK) Unwrap([]byte) ([]byte, error) {
	return nil, errors.New("provider unavailable")
}

func (failingStartupKEK) Name() string { return "failing" }

func TestVerifyKEKBeforeMutators(t *testing.T) {
	t.Parallel()
	f := Flags{Enabled: true, SidecarPath: "/data/encryption/keys.json"}
	require.ErrorIs(t, verifyKEKBeforeMutators(f, failingStartupKEK{}), kek.ErrKEKPreflightFailed)

	f.Enabled = false
	require.NoError(t, verifyKEKBeforeMutators(f, failingStartupKEK{}))
	f.Enabled = true
	f.SidecarPath = ""
	require.NoError(t, verifyKEKBeforeMutators(f, failingStartupKEK{}))
}

func TestStartupKEKSourceWiringCachesGuardUnwrapThroughHydration(t *testing.T) {
	staticKEK := bytes.Repeat([]byte{0x71}, encryption.KeySize)
	f := Flags{
		Enabled:     true,
		kekFile:     filepath.Join(t.TempDir(), "kek"),
		SidecarPath: filepath.Join(t.TempDir(), encryption.SidecarFilename),
	}
	require.NoError(t, os.WriteFile(f.kekFile, staticKEK, 0o600))

	source, err := kek.NewWrapperFromSources(context.Background(), f.kekFile, "")
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
	require.NoError(t, encryption.WriteSidecar(f.SidecarPath, sidecar))

	observer := &countingKEKObserver{}
	wrapper, err := loadKEKAndRunStartupGuards(f, observer)
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
	wiring, err := BuildWriteWiring(true, "n1", sidecarPath, wrapper, keystore, []noderuntime.GroupSpec{{ID: 1}}, nil)
	require.NoError(t, err)
	source, err := store.NewPebbleStore(dir+"/source", wiring.PebbleOptions()...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })
	registry, err := store.WriterRegistryFor(source)
	require.NoError(t, err)
	applier, err := encryption.NewApplier(
		registry,
		encryption.WithKEK(wrapper),
		encryption.WithKeystore(keystore),
		encryption.WithSidecarPath(sidecarPath),
		encryption.WithStateCache(wiring.Cache),
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
	wiring.Cache.MarkRegistered(e2eStorageDEKID)
	require.NoError(t, source.PutAt(context.Background(), []byte("env-kek"), []byte("secret"), 100, 0))

	snapshot, err := source.Snapshot()
	require.NoError(t, err)
	var snapshotBytes bytes.Buffer
	_, err = snapshot.WriteTo(&snapshotBytes)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())

	target, err := store.NewPebbleStore(dir+"/target", wiring.PebbleOptions()...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = target.Close() })
	require.NoError(t, target.Restore(bytes.NewReader(snapshotBytes.Bytes())))
	got, err := target.GetAt(context.Background(), []byte("env-kek"), 100)
	require.NoError(t, err)
	require.Equal(t, []byte("secret"), got)
}

func TestEncryptionFlagsBootstrapRequest(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		flags Flags
		want  noderuntime.BootstrapEncryptionRequest
	}{
		{"off", Flags{}, noderuntime.BootstrapEncryptionRequest{}},
		{"kek only", Flags{kekFile: "kek"}, noderuntime.BootstrapEncryptionRequest{}},
		{"enabled", Flags{Enabled: true}, noderuntime.BootstrapEncryptionRequest{Configured: true}},
		{"sidecar", Flags{SidecarPath: "keys.json"}, noderuntime.BootstrapEncryptionRequest{Configured: true}},
		{"rotation", Flags{RotateOnStartup: true}, noderuntime.BootstrapEncryptionRequest{RotateOnStartup: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tc.flags.BootstrapRequest()
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.want != noderuntime.BootstrapEncryptionRequest{}, got.Requested())
		})
	}
}
