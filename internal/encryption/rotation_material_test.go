package encryption_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/stretchr/testify/require"
)

func rotationMaterialSidecar() *encryption.Sidecar {
	return &encryption.Sidecar{Active: encryption.ActiveKeys{Storage: 1, Raft: 2}, Keys: map[string]encryption.SidecarKey{
		"1": {Purpose: encryption.SidecarPurposeStorage, Wrapped: []byte("storage")},
		"2": {Purpose: encryption.SidecarPurposeRaft, Wrapped: []byte("raft")},
		"3": {Purpose: encryption.SidecarPurposeStorage, Wrapped: []byte("historical")},
	}}
}

func TestRotationMaterialRejectsAliasedDomains(t *testing.T) {
	t.Parallel()
	sc := rotationMaterialSidecar()
	for _, tc := range []struct {
		name, purpose, wrapped string
		id                     uint32
		valid                  bool
	}{
		{"fresh storage", encryption.SidecarPurposeStorage, "fresh", 4, true},
		{"fresh raft", encryption.SidecarPurposeRaft, "fresh", 4, true},
		{"same identity replay", encryption.SidecarPurposeStorage, "storage", 1, true},
		{"same purpose new ID", encryption.SidecarPurposeStorage, "storage", 4, false},
		{"cross purpose new ID", encryption.SidecarPurposeRaft, "storage", 4, false},
		{"cross purpose same ID", encryption.SidecarPurposeRaft, "storage", 1, false},
		{"same ID replaced", encryption.SidecarPurposeStorage, "fresh", 1, false},
		{"inactive history", encryption.SidecarPurposeRaft, "historical", 4, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			unwrap := &fakeKEK{}
			material, err := unwrap.Unwrap([]byte(tc.wrapped))
			require.NoError(t, err)
			err = encryption.ValidateRotationDEKMaterial(sc, unwrap, tc.id, tc.purpose, material)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, encryption.ErrDEKMaterialReuse)
			}
		})
	}
}

type rotationMaterialWrapper struct{ buffers [][]byte }

func (w *rotationMaterialWrapper) Unwrap([]byte) ([]byte, error) {
	key := bytes.Repeat([]byte{7}, encryption.KeySize)
	w.buffers = append(w.buffers, key)
	return key, nil
}

func TestRotationMaterialDifferentWrapAndZeroing(t *testing.T) {
	t.Parallel()
	unwrap := &rotationMaterialWrapper{}
	sc := &encryption.Sidecar{Keys: map[string]encryption.SidecarKey{"1": {Purpose: encryption.SidecarPurposeStorage, Wrapped: []byte("randomized-envelope-one")}}}
	material, err := unwrap.Unwrap([]byte("randomized-envelope-two"))
	require.NoError(t, err)
	err = encryption.ValidateRotationDEKMaterial(sc, unwrap, 2, encryption.SidecarPurposeRaft, material)
	require.ErrorIs(t, err, encryption.ErrDEKMaterialReuse)
	require.Equal(t, bytes.Repeat([]byte{7}, encryption.KeySize), material, "caller material is not modified")
	require.Equal(t, make([]byte, encryption.KeySize), unwrap.buffers[1], "temporary historical material must be erased")
	bad := &fakeKEK{failOn: []byte("randomized-envelope-one"), failErr: errors.New("unavailable historical key")}
	require.Error(t, encryption.ValidateRotationDEKMaterial(sc, bad, 2, encryption.SidecarPurposeRaft, material))
}

func TestApplyRotationMaterialRejectsBeforeMutation(t *testing.T) {
	t.Parallel()
	reg := newMapRegistryStore()
	ks := encryption.NewKeystore()
	path := t.TempDir() + "/keys.json"
	app, err := encryption.NewApplier(reg, encryption.WithKEK(&fakeKEK{}), encryption.WithKeystore(ks), encryption.WithSidecarPath(path))
	require.NoError(t, err)
	require.NoError(t, app.ApplyBootstrap(1, fsmwire.BootstrapPayload{StorageDEKID: 1, WrappedStorage: []byte("storage"), RaftDEKID: 2, WrappedRaft: []byte("raft")}))
	before, err := encryption.ReadSidecar(path)
	require.NoError(t, err)
	for _, purpose := range []fsmwire.Purpose{fsmwire.PurposeStorage, fsmwire.PurposeRaft} {
		err = app.ApplyRotation(2, fsmwire.RotationPayload{SubTag: fsmwire.RotateSubRotateDEK, DEKID: 4, Purpose: purpose, Wrapped: []byte("storage"), ProposerRegistration: fsmwire.RegistrationPayload{DEKID: 4, FullNodeID: 42}})
		require.ErrorIs(t, err, encryption.ErrDEKMaterialReuse)
		require.False(t, ks.Has(4))
		after, err := encryption.ReadSidecar(path)
		require.NoError(t, err)
		require.Equal(t, before, after)
		_, ok, err := reg.GetRegistryRow(encryption.RegistryKey(4, 42))
		require.NoError(t, err)
		require.False(t, ok)
	}
	fresh := fsmwire.RotationPayload{SubTag: fsmwire.RotateSubRotateDEK, DEKID: 4, Purpose: fsmwire.PurposeStorage, Wrapped: []byte("independent fresh material"), ProposerRegistration: fsmwire.RegistrationPayload{DEKID: 4, FullNodeID: 42}}
	require.NoError(t, app.ApplyRotation(2, fresh), "a fresh material control must still rotate")
	require.True(t, ks.Has(4))
	require.NoError(t, app.ApplyRotation(2, fresh), "same-domain exact replay remains idempotent")
}
