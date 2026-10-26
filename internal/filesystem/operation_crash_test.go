package filesystem

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

const operationCrashDirectory = "ELASTICKV_OPERATION_CRASH_DIRECTORY"

type operationCrashEvidence struct {
	IDs   []OperationID `json:"ids"`
	Inode uint64        `json:"inode"`
}

func TestJournalReceiptsSurviveProcessKill(t *testing.T) {
	if dir := os.Getenv(operationCrashDirectory); dir != "" {
		runOperationCrashHelper(t, dir)
		return
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestJournalReceiptsSurviveProcessKill$")
	cmd.Env = append(os.Environ(), operationCrashDirectory+"="+dir)
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit, string(output))
	status, ok := exit.Sys().(syscall.WaitStatus)
	require.True(t, ok, string(output))
	require.Equal(t, syscall.SIGKILL, status.Signal(), string(output))
	raw, err := os.ReadFile(filepath.Join(dir, "evidence.json"))
	require.NoError(t, err)
	var evidence operationCrashEvidence
	require.NoError(t, json.Unmarshal(raw, &evidence))
	require.Len(t, evidence.IDs, 4)

	st, err := store.NewPebbleStore(filepath.Join(dir, "pebble"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	svc, err := NewService(st, &testCoordinator{st: st})
	require.NoError(t, err)
	for _, id := range evidence.IDs {
		require.NoError(t, svc.VerifyOperationReceipt(t.Context(), id))
	}
	inode, err := svc.Resolve(t.Context(), RootInode, []byte("target"))
	require.NoError(t, err)
	require.Equal(t, evidence.Inode, inode)
	data, err := svc.Read(t.Context(), inode, 0, 0, 3)
	require.NoError(t, err)
	require.Equal(t, []byte("dur"), data)
}

func runOperationCrashHelper(t *testing.T, dir string) {
	ctx := context.Background()
	st, err := store.NewPebbleStore(filepath.Join(dir, "pebble"))
	require.NoError(t, err)
	svc, err := NewService(st, &testCoordinator{st: st})
	require.NoError(t, err)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	firstID := testWriteOperationID(t, svc, 1)
	evidence := operationCrashEvidence{IDs: make([]OperationID, 4)}
	for i := range evidence.IDs {
		evidence.IDs[i] = firstID
		evidence.IDs[i].Sequence = uint64(i + 1)
	}
	file, err := svc.CreateWithOperation(ctx, evidence.IDs[0], RootInode, []byte("source"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	evidence.Inode = file.Inode
	_, err = svc.WriteWithOperation(ctx, evidence.IDs[1], file.Inode, 0, []byte("durable"))
	require.NoError(t, err)
	_, err = svc.SetAttrWithOperation(ctx, evidence.IDs[2], file.Inode, SetAttrMask{Size: true}, SetAttr{Size: 3})
	require.NoError(t, err)
	require.NoError(t, svc.RenameWithOperation(ctx, evidence.IDs[3], RootInode, []byte("source"), RootInode, []byte("target")))
	for _, id := range evidence.IDs {
		require.NoError(t, svc.VerifyOperationReceipt(ctx, id))
	}
	raw, err := json.Marshal(evidence)
	require.NoError(t, err)
	fileOnDisk, err := os.OpenFile(filepath.Join(dir, "evidence.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	require.NoError(t, err)
	_, err = fileOnDisk.Write(raw)
	require.NoError(t, err)
	require.NoError(t, fileOnDisk.Sync())
	require.NoError(t, fileOnDisk.Close())
	// Kill without closing Pebble or running test cleanup.
	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGKILL))
}
