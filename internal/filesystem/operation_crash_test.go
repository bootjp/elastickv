package filesystem

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

const operationCrashDirectory = "ELASTICKV_OPERATION_CRASH_DIRECTORY"
const operationFenceCrashDirectory = "ELASTICKV_OPERATION_FENCE_CRASH_DIRECTORY"
const operationDecisionCrashDirectory = "ELASTICKV_OPERATION_DECISION_CRASH_DIRECTORY"

type operationCrashEvidence struct {
	IDs   []OperationID `json:"ids"`
	Inode uint64        `json:"inode"`
}

type operationFenceCrashEvidence struct {
	ID      OperationID `json:"id"`
	Attempt [16]byte    `json:"attempt"`
}

type crashReplayCoordinator struct {
	*testCoordinator
	dispatches int
}

func (c *crashReplayCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	c.dispatches++
	return c.testCoordinator.Dispatch(ctx, req)
}

func TestJournalCommittedAttemptDecisionSurvivesProcessKill(t *testing.T) {
	if dir := os.Getenv(operationDecisionCrashDirectory); dir != "" {
		runOperationDecisionCrashHelper(t, dir)
		return
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestJournalCommittedAttemptDecisionSurvivesProcessKill$")
	cmd.Env = append(os.Environ(), operationDecisionCrashDirectory+"="+dir)
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit, string(output))
	status, ok := exit.Sys().(syscall.WaitStatus)
	require.True(t, ok, string(output))
	require.Equal(t, syscall.SIGKILL, status.Signal(), string(output))
	raw, err := os.ReadFile(filepath.Join(dir, "decision-evidence.json"))
	require.NoError(t, err)
	var evidence operationFenceCrashEvidence
	require.NoError(t, json.Unmarshal(raw, &evidence))

	st, err := store.NewPebbleStore(filepath.Join(dir, "pebble"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	coordinator := &crashReplayCoordinator{testCoordinator: &testCoordinator{st: st}}
	svc, err := NewService(st, coordinator)
	require.NoError(t, err)
	journal, err := NewOperationJournal(svc)
	require.NoError(t, err)
	outcome, record, err := journal.ResolveAttempt(t.Context(), evidence.ID, evidence.Attempt)
	require.NoError(t, err)
	require.Equal(t, AttemptCommitted, outcome)
	require.Equal(t, []byte("done"), record.Result)
	beforeTS := st.LastCommitTS()
	beforeVersion, exists, err := st.LatestCommitTS(t.Context(), []byte("data"))
	require.NoError(t, err)
	require.True(t, exists)
	beforeValue, err := st.GetAt(t.Context(), []byte("data"), beforeTS)
	require.NoError(t, err)
	require.Equal(t, []byte("new"), beforeValue)
	duplicate, err := journal.Publish(t.Context(), evidence.ID, evidence.Attempt, operationAttemptPlan{
		Intent: []byte("write"), Effects: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("data"), Value: []byte("new")}}, Result: []byte("done"),
	})
	require.NoError(t, err)
	require.Equal(t, record, duplicate)
	require.Zero(t, coordinator.dispatches, "replay must not dispatch another mutation")
	require.Equal(t, beforeTS, st.LastCommitTS(), "replay must not publish another mutation")
	afterVersion, exists, err := st.LatestCommitTS(t.Context(), []byte("data"))
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, beforeVersion, afterVersion)
	value, err := st.GetAt(t.Context(), []byte("data"), st.LastCommitTS())
	require.NoError(t, err)
	require.Equal(t, []byte("new"), value)
}

func runOperationDecisionCrashHelper(t *testing.T, dir string) {
	ctx := context.Background()
	st, err := store.NewPebbleStore(filepath.Join(dir, "pebble"))
	require.NoError(t, err)
	svc, err := NewService(st, &testCoordinator{st: st})
	require.NoError(t, err)
	journal, err := NewOperationJournal(svc)
	require.NoError(t, err)
	id := testOperationID(t, journal, 1)
	_, err = journal.Admit(ctx, id, []byte("write"), 128, 1024)
	require.NoError(t, err)
	attempt, err := journal.BeginAttempt(ctx, id)
	require.NoError(t, err)
	_, err = journal.Publish(ctx, id, attempt.AttemptID, operationAttemptPlan{
		Intent: []byte("write"), Effects: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("data"), Value: []byte("new")}}, Result: []byte("done"),
	})
	require.NoError(t, err)
	raw, err := json.Marshal(operationFenceCrashEvidence{ID: id, Attempt: attempt.AttemptID})
	require.NoError(t, err)
	fileOnDisk, err := os.OpenFile(filepath.Join(dir, "decision-evidence.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	require.NoError(t, err)
	_, err = fileOnDisk.Write(raw)
	require.NoError(t, err)
	require.NoError(t, fileOnDisk.Sync())
	require.NoError(t, fileOnDisk.Close())
	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGKILL))
}

func TestJournalGenerationFenceSurvivesProcessKill(t *testing.T) {
	if dir := os.Getenv(operationFenceCrashDirectory); dir != "" {
		runOperationFenceCrashHelper(t, dir)
		return
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestJournalGenerationFenceSurvivesProcessKill$")
	cmd.Env = append(os.Environ(), operationFenceCrashDirectory+"="+dir)
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit, string(output))
	status, ok := exit.Sys().(syscall.WaitStatus)
	require.True(t, ok, string(output))
	require.Equal(t, syscall.SIGKILL, status.Signal(), string(output))
	raw, err := os.ReadFile(filepath.Join(dir, "fence-evidence.json"))
	require.NoError(t, err)
	var evidence operationFenceCrashEvidence
	require.NoError(t, json.Unmarshal(raw, &evidence))

	st, err := store.NewPebbleStore(filepath.Join(dir, "pebble"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	svc, err := NewService(st, &testCoordinator{st: st})
	require.NoError(t, err)
	journal, err := NewOperationJournal(svc)
	require.NoError(t, err)
	outcome, record, err := journal.ResolveAttempt(t.Context(), evidence.ID, evidence.Attempt)
	require.NoError(t, err)
	require.Equal(t, AttemptAborted, outcome)
	require.Contains(t, record.FencedAttempts, evidence.Attempt)
	_, err = journal.Publish(t.Context(), evidence.ID, evidence.Attempt, operationAttemptPlan{
		Intent: []byte("write"), Effects: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("data"), Value: []byte("old")}},
	})
	require.ErrorIs(t, err, ErrOperationGeneration)
	_, err = st.GetAt(t.Context(), []byte("data"), st.LastCommitTS())
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	_, err = journal.Publish(t.Context(), evidence.ID, record.AttemptID, operationAttemptPlan{
		Intent: []byte("write"), Effects: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("data"), Value: []byte("new")}},
	})
	require.NoError(t, err)
	value, err := st.GetAt(t.Context(), []byte("data"), st.LastCommitTS())
	require.NoError(t, err)
	require.Equal(t, []byte("new"), value)
}

func runOperationFenceCrashHelper(t *testing.T, dir string) {
	ctx := context.Background()
	st, err := store.NewPebbleStore(filepath.Join(dir, "pebble"))
	require.NoError(t, err)
	svc, err := NewService(st, &testCoordinator{st: st})
	require.NoError(t, err)
	journal, err := NewOperationJournal(svc)
	require.NoError(t, err)
	id := testOperationID(t, journal, 1)
	_, err = journal.Admit(ctx, id, []byte("write"), 128, 1024)
	require.NoError(t, err)
	attempt, err := journal.BeginAttempt(ctx, id)
	require.NoError(t, err)
	_, err = journal.AdvanceGeneration(ctx, id, attempt.AttemptID)
	require.NoError(t, err)
	raw, err := json.Marshal(operationFenceCrashEvidence{ID: id, Attempt: attempt.AttemptID})
	require.NoError(t, err)
	fileOnDisk, err := os.OpenFile(filepath.Join(dir, "fence-evidence.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	require.NoError(t, err)
	_, err = fileOnDisk.Write(raw)
	require.NoError(t, err)
	require.NoError(t, fileOnDisk.Sync())
	require.NoError(t, fileOnDisk.Close())
	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGKILL))
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
