package main

import (
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestMigrationNamespaceProductionFSMInstalledWithEncryptionOptions(t *testing.T) {
	t.Parallel()
	for _, encrypted := range []bool{false, true} {
		wiring := encryptionWriteWiring{}
		if encrypted {
			cipher, err := encryption.NewCipher(encryption.NewKeystore())
			require.NoError(t, err)
			wiring = encryptionWriteWiring{cache: encryption.NewStateCache(), cipher: cipher, nonceFactory: encryption.NewDeterministicNonceFactory(1, 1)}
		}
		st, err := store.NewPebbleStore(t.TempDir(), wiring.pebbleOptions()...)
		require.NoError(t, err)
		registry, err := store.WriterRegistryFor(st)
		require.NoError(t, err)
		applier, err := encryption.NewApplier(registry)
		require.NoError(t, err)
		fsm := kv.NewKvFSMWithHLCAndTracker(st, kv.NewHLC(), nil, fsmOptionsForGroup(applier, nil, 1, wiring)...)
		require.Equal(t, kv.MigrationNamespaceVersion, kv.MigrationNamespaceInstalledVersion(st, fsm))
		require.Zero(t, kv.MigrationNamespaceInstalledVersion(st, namespaceHiddenFSM{fsm}))
		require.Zero(t, kv.MigrationNamespaceInstalledVersion(st, kv.NewTSOStateMachine(kv.NewHLC())))
		require.NoError(t, st.Close())
	}
}
