package etcd

import (
	"context"
	"io"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type namespaceRecoveryRestoreProbe struct {
	StateMachine
	restored bool
}

func (p *namespaceRecoveryRestoreProbe) Restore(io.Reader) error {
	p.restored = true
	return nil
}

func namespaceRecoveryFixture(t *testing.T) (OfflineBaselineOptions, *WALIdentity, io.Closer) {
	t.Helper()
	o, signer := offlineBaselineFixture(t, true)
	manifest := signedOfflineBaseline(t, o, signer)
	_, err := PrepareOfflineBaseline(t.Context(), o, manifest)
	require.NoError(t, err)
	migration, err := ResumeOfflineBaseline(t.Context(), o, manifest)
	require.NoError(t, err)
	require.Equal(t, "bound", migration.Phase)
	digest, err := raftbootstrap.BaselineManifestDigest(manifest)
	require.NoError(t, err)
	identity, err := baselineDestinationIdentity(o, digest)
	require.NoError(t, err)
	guard, err := claimNamespaceRecoveryOwnership(OpenConfig{DataDir: o.DataDir, namespaceOwnership: o.Ownership})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, guard.Close()) })
	return o, identity, guard
}

func TestNativeNamespaceRecoveryChecksProvenanceAndBounds(t *testing.T) {
	for _, mode := range []string{"verified", "reject", "out-of-range", "ordinary-restore", "unbound"} {
		t.Run(mode, func(t *testing.T) {
			o, identity, _ := namespaceRecoveryFixture(t)
			probe := &namespaceRecoveryRestoreProbe{}
			called := false
			rejected := errors.New("complete namespace differs")
			verify := func(_ context.Context, source NamespaceRecoverySource) (uint64, bool, error) {
				called = true
				require.Equal(t, *identity, source.Identity())
				require.Equal(t, uint64(3), source.SnapshotIndex())
				require.Equal(t, uint64(3), source.CommitIndex())
				payload, err := io.ReadAll(source.Snapshot())
				require.NoError(t, err)
				require.Equal(t, "FSM snapshot", string(payload))
				switch mode {
				case "reject":
					return 0, false, rejected
				case "out-of-range":
					return 4, true, nil
				case "ordinary-restore":
					return 0, false, nil
				default:
					return 3, true, nil
				}
			}
			if mode == "unbound" {
				identity = nil
			}
			disk, err := loadWalStateRecovery(zap.NewNop(), filepath.Join(o.DataDir, walDirName),
				filepath.Join(o.DataDir, snapDirName), filepath.Join(o.DataDir, fsmSnapDirName), probe, nil, verify, identity)
			if disk != nil {
				require.NoError(t, disk.Persist.Close())
			}
			assertNamespaceRecoveryResult(t, mode, disk, err, rejected)
			require.Equal(t, mode != "unbound", called)
			require.Equal(t, mode == "ordinary-restore", probe.restored)
		})
	}
}

func assertNamespaceRecoveryResult(t *testing.T, mode string, disk *diskState, err, rejected error) {
	t.Helper()
	switch mode {
	case "reject":
		require.ErrorIs(t, err, rejected)
	case "out-of-range", "unbound":
		require.Error(t, err)
	default:
		require.NoError(t, err)
		require.Equal(t, uint64(3), disk.EffectiveApplied)
	}
}

func TestNativeNamespaceRecoveryOwnershipHandoff(t *testing.T) {
	o, _, guard := namespaceRecoveryFixture(t)
	require.NoError(t, o.Ownership.Close(), "closing the transferred offline token must not unlock the engine root")
	other, err := AcquireOfflineBaselineOwnership(o.DataDir)
	if other != nil {
		require.NoError(t, other.Close())
	}
	require.Error(t, err, "directory ownership must be continuous through handoff")
	require.NoError(t, guard.Close())
	other, err = AcquireOfflineBaselineOwnership(o.DataDir)
	require.NoError(t, err)
	require.NoError(t, other.Close())
}
