package snapshotoffload

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// The M3 corruption drills. Each plants one specific defect in a
// published artifact and asserts restore fails closed AND leaves the
// destination data dir absent — a half-restored dir is worse than no
// restore, because the node would come up serving truncated state.

// publishForRestoreDrill publishes a snapshot and returns the store and
// its manifest.
func publishForRestoreDrill(t *testing.T, root string, payload []byte) (*LocalStore, Manifest) {
	t.Helper()
	sourceDataDir := seedPhysicalSnapshot(t, root, payload, 21, 5, singlePeer())
	store := newTestLocalStore(t, filepath.Join(root, "objects"))
	manifest, err := PublishPersistedSnapshot(context.Background(), PublishOptions{
		Store:         store,
		DataDir:       sourceDataDir,
		Prefix:        "cluster-a",
		GroupID:       1,
		SourceCluster: "cluster-a",
	})
	require.NoError(t, err)
	return store, *manifest
}

// requireRestoreFailsClosed runs a restore and asserts it failed and
// left no data dir behind.
func requireRestoreFailsClosed(t *testing.T, store *LocalStore, manifestKey, dataDir string) error {
	t.Helper()
	_, err := RestorePhysicalSnapshot(context.Background(), RestoreOptions{
		Store:       store,
		ManifestKey: manifestKey,
		DataDir:     dataDir,
		Peers:       singlePeer(),
	})
	require.Error(t, err)
	_, statErr := os.Stat(dataDir)
	require.True(t, os.IsNotExist(statErr),
		"a failed restore must leave the destination absent, not half-written")
	return err
}

// TestRestoreRejectsTruncatedPayload covers a partial upload or a
// truncating filesystem: the bytes hash differently AND are short. The
// length check must fire before any content is trusted.
func TestRestoreRejectsTruncatedPayload(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	payload := []byte("EKVTHLC1a-payload-long-enough-to-truncate-meaningfully")
	store, manifest := publishForRestoreDrill(t, root, payload)

	payloadPath, err := store.pathForKey(manifest.Payload.Key)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(payloadPath, payload[:len(payload)/2], 0o600))

	err = requireRestoreFailsClosed(t, store, manifest.ManifestKey, filepath.Join(root, "restored"))
	require.True(t, errors.Is(err, ErrIntegrity))
}

// TestRestoreRejectsPayloadGrownBeyondItsDeclaredLength is the
// complement: extra bytes appended to the object.
func TestRestoreRejectsPayloadGrownBeyondItsDeclaredLength(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	payload := []byte("EKVTHLC1a-payload-to-be-extended")
	store, manifest := publishForRestoreDrill(t, root, payload)

	payloadPath, err := store.pathForKey(manifest.Payload.Key)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(payloadPath, append(payload, []byte("extra")...), 0o600))

	err = requireRestoreFailsClosed(t, store, manifest.ManifestKey, filepath.Join(root, "restored"))
	require.True(t, errors.Is(err, ErrIntegrity))
}

// TestRestoreRejectsAManifestWhosePayloadIsMissing is the dangling
// reference case — precisely the state a retention bug would leave
// behind if it reclaimed a payload a committed manifest still names.
// Restore must fail cleanly rather than produce an empty data dir.
func TestRestoreRejectsAManifestWhosePayloadIsMissing(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	payload := []byte("EKVTHLC1payload-that-gets-reclaimed")
	store, manifest := publishForRestoreDrill(t, root, payload)

	require.NoError(t, store.DeleteObject(context.Background(), manifest.Payload.Key))

	err := requireRestoreFailsClosed(t, store, manifest.ManifestKey, filepath.Join(root, "restored"))
	require.True(t, errors.Is(err, ErrObjectNotFound),
		"a manifest naming an absent payload must report the payload as missing")
}

// TestRestoreRejectsAManifestWithATamperedPayloadDescriptor covers a
// manifest edited to name a different payload SHA, in both shapes an
// attacker can produce.
//
// Leaving the self-hash stale is caught by the manifest's own
// integrity check. Refreshing the self-hash defeats that check but is
// still caught downstream, because the payload OBJECT KEY encodes the
// content hash: a descriptor claiming a different SHA points at a key
// whose bytes cannot hash to it. Both must fail closed.
func TestRestoreRejectsAManifestWithATamperedPayloadDescriptor(t *testing.T) {
	t.Parallel()

	falseSHA := hexSHA256Bytes([]byte("a completely different payload"))

	tests := []struct {
		name            string
		refreshSelfHash bool
	}{
		{name: "stale self hash", refreshSelfHash: false},
		{name: "self hash refreshed to match the edit", refreshSelfHash: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			payload := []byte("EKVTHLC1payload-with-an-edited-descriptor")
			store, manifest := publishForRestoreDrill(t, root, payload)

			tampered := manifest
			tampered.Payload.SHA256 = falseSHA
			encoded, _, err := tampered.MarshalCanonical()
			require.NoError(t, err)
			if !tc.refreshSelfHash {
				encoded = bytes.Replace(encoded,
					[]byte(tampered.ManifestSHA256), []byte(manifest.ManifestSHA256), 1)
			}
			manifestPath, err := store.pathForKey(manifest.ManifestKey)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(manifestPath, encoded, 0o600))

			err = requireRestoreFailsClosed(t, store, manifest.ManifestKey,
				filepath.Join(root, "restored"))
			require.True(t, errors.Is(err, ErrIntegrity),
				"an edited payload descriptor must fail the integrity contract, got %v", err)
		})
	}
}

// TestRestoreDrillSucceedsIntoAFreshDirectory is the positive drill:
// the artifact published above restores cleanly into an absent data
// dir, which is the operation the corruption cases must not
// half-perform.
func TestRestoreDrillSucceedsIntoAFreshDirectory(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	payload := []byte("EKVTHLC1a-healthy-payload-for-the-drill")
	store, manifest := publishForRestoreDrill(t, root, payload)

	restoreDataDir := filepath.Join(root, "restored")
	result, err := RestorePhysicalSnapshot(context.Background(), RestoreOptions{
		Store:       store,
		ManifestKey: manifest.ManifestKey,
		DataDir:     restoreDataDir,
		Peers:       singlePeer(),
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.DirExists(t, restoreDataDir)

	// A second restore into the now-populated dir must refuse rather
	// than overwrite: the preflight is what protects an operator who
	// re-runs the drill against a live node's data dir.
	_, err = RestorePhysicalSnapshot(context.Background(), RestoreOptions{
		Store:       store,
		ManifestKey: manifest.ManifestKey,
		DataDir:     restoreDataDir,
		Peers:       singlePeer(),
	})
	require.Error(t, err, "restore must refuse a destination that already exists")
}
