//go:build unix

package etcd

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrepareExternalSnapshotRestoreRejectsFIFOInput(t *testing.T) {
	root := t.TempDir()
	input := filepath.Join(root, "encoded.fsm")
	require.NoError(t, syscall.Mkfifo(input, 0o600))

	_, err := PrepareExternalSnapshotRestore(ExternalSnapshotRestoreOptions{
		InputFSMPath: input,
		DataDir:      filepath.Join(root, "raft"),
		Index:        1,
		Term:         1,
		Peers:        []Peer{{NodeID: 1, ID: "n1", Address: "127.0.0.1:12001"}},
	})
	require.ErrorIs(t, err, ErrExternalSnapshotRestoreInvalid)
}

func TestPrepareExternalSnapshotRestoreRejectsSymlinkInput(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target.fsm")
	require.NoError(t, os.WriteFile(target, []byte("payload"), 0o600))
	input := filepath.Join(root, "encoded.fsm")
	require.NoError(t, os.Symlink(target, input))

	_, err := PrepareExternalSnapshotRestore(ExternalSnapshotRestoreOptions{
		InputFSMPath: input,
		DataDir:      filepath.Join(root, "raft"),
		Index:        1,
		Term:         1,
		Peers:        []Peer{{NodeID: 1, ID: "n1", Address: "127.0.0.1:12001"}},
	})
	require.ErrorIs(t, err, ErrExternalSnapshotRestoreInvalid)
}
