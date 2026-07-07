package main

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	crdberrors "github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeRotateStartupController struct {
	mu        sync.Mutex
	state     raftengine.State
	callbacks []func()
}

func (f *fakeRotateStartupController) State() raftengine.State {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.state
}

func (f *fakeRotateStartupController) Leader() raftengine.LeaderInfo { return raftengine.LeaderInfo{} }
func (f *fakeRotateStartupController) VerifyLeader(context.Context) error {
	return nil
}
func (f *fakeRotateStartupController) LinearizableRead(context.Context) (uint64, error) {
	return 0, nil
}

func (f *fakeRotateStartupController) RegisterLeaderAcquiredCallback(fn func()) func() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.callbacks = append(f.callbacks, fn)
	idx := len(f.callbacks) - 1
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		if idx >= 0 && idx < len(f.callbacks) {
			f.callbacks[idx] = nil
		}
	}
}

func (f *fakeRotateStartupController) becomeLeader() {
	f.mu.Lock()
	f.state = raftengine.StateLeader
	callbacks := append([]func(){}, f.callbacks...)
	f.mu.Unlock()
	for _, cb := range callbacks {
		if cb != nil {
			cb()
		}
	}
}

type fakeRotateStartupRunner struct {
	runs atomic.Int32
	done atomic.Bool
	err  error
}

func (f *fakeRotateStartupRunner) Run(context.Context) error {
	f.runs.Add(1)
	if f.err == nil {
		f.done.Store(true)
	}
	return f.err
}

func (f *fakeRotateStartupRunner) Done() bool { return f.done.Load() }

type retryOnceRotateStartupRunner struct {
	runs atomic.Int32
	done atomic.Bool
	err  error
}

func (r *retryOnceRotateStartupRunner) Run(context.Context) error {
	if r.runs.Add(1) == 1 {
		return r.err
	}
	r.done.Store(true)
	return nil
}

func (r *retryOnceRotateStartupRunner) Done() bool { return r.done.Load() }

type blockingDoneRotateStartupRunner struct {
	doneEntered chan struct{}
	releaseDone chan struct{}
	once        sync.Once
}

func (b *blockingDoneRotateStartupRunner) Run(context.Context) error { return nil }

func (b *blockingDoneRotateStartupRunner) Done() bool {
	b.once.Do(func() { close(b.doneEntered) })
	<-b.releaseDone
	return false
}

type blockingRunRotateStartupRunner struct {
	started chan struct{}
	release chan struct{}
	done    atomic.Bool
	once    sync.Once
}

func (b *blockingRunRotateStartupRunner) Run(context.Context) error {
	b.once.Do(func() { close(b.started) })
	<-b.release
	b.done.Store(true)
	return nil
}

func (b *blockingRunRotateStartupRunner) Done() bool { return b.done.Load() }

func TestInstallEncryptionRotateOnStartupRequest_ImmediateLeaderBlocks(t *testing.T) {
	t.Parallel()
	controller := &fakeRotateStartupController{state: raftengine.StateLeader}
	runner := &fakeRotateStartupRunner{}
	deregister := installEncryptionRotateOnStartupRequest(context.Background(), controller, runner, slog.Default())
	defer deregister()
	if got := runner.runs.Load(); got != 1 {
		t.Fatalf("immediate leader must run rotation synchronously before returning; runs=%d", got)
	}
}

func TestInstallEncryptionRotateOnStartupRequest_FollowerFiresOnLeadership(t *testing.T) {
	t.Parallel()
	controller := &fakeRotateStartupController{state: raftengine.StateFollower}
	runner := &fakeRotateStartupRunner{}
	deregister := installEncryptionRotateOnStartupRequest(context.Background(), controller, runner, nil)
	defer deregister()
	if got := runner.runs.Load(); got != 0 {
		t.Fatalf("follower install must not run immediately; runs=%d", got)
	}
	controller.becomeLeader()
	deadline := time.Now().Add(2 * time.Second)
	for runner.runs.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := runner.runs.Load(); got != 1 {
		t.Fatalf("leader-acquired callback must run rotation once; runs=%d", got)
	}
}

func TestInstallEncryptionRotateOnStartupRequest_RetriesTransientWhileLeader(t *testing.T) {
	oldDelay := encryptionRotateOnStartupRetryDelay
	encryptionRotateOnStartupRetryDelay = time.Millisecond
	defer func() { encryptionRotateOnStartupRetryDelay = oldDelay }()

	controller := &fakeRotateStartupController{state: raftengine.StateLeader}
	runner := &retryOnceRotateStartupRunner{err: status.Error(codes.Unavailable, "try again")}
	deregister := installEncryptionRotateOnStartupRequest(context.Background(), controller, runner, nil)
	defer deregister()
	if got := runner.runs.Load(); got != 2 {
		t.Fatalf("transient startup rotation must retry before returning; runs=%d", got)
	}
}

func TestInstallEncryptionRotateOnStartupRequest_CallbackDoesNotBlockOnDone(t *testing.T) {
	controller := &fakeRotateStartupController{state: raftengine.StateFollower}
	runner := &blockingDoneRotateStartupRunner{
		doneEntered: make(chan struct{}),
		releaseDone: make(chan struct{}),
	}
	deregister := installEncryptionRotateOnStartupRequest(context.Background(), controller, runner, nil)
	defer deregister()
	defer close(runner.releaseDone)

	returned := make(chan struct{})
	go func() {
		controller.becomeLeader()
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("leader-acquired callback blocked before starting async rotation")
	}
	select {
	case <-runner.doneEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("async rotation did not reach Done")
	}
}

func TestInstallEncryptionRotateOnStartupRequest_WaitTracksAsyncFlight(t *testing.T) {
	controller := &fakeRotateStartupController{state: raftengine.StateFollower}
	runner := &blockingRunRotateStartupRunner{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	deregister, waitStartup := installEncryptionRotateOnStartupRequestWithWait(context.Background(), controller, runner, nil)
	defer deregister()

	controller.becomeLeader()
	select {
	case <-runner.started:
	case <-time.After(2 * time.Second):
		t.Fatal("async rotation did not start")
	}

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- waitStartup(context.Background())
	}()
	select {
	case err := <-waitDone:
		t.Fatalf("startup wait returned before async rotation finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(runner.release)
	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("startup wait returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("startup wait did not finish after rotation completed")
	}
}

func TestInstallEncryptionRotateOnStartupRequest_WaitReturnsPermanentError(t *testing.T) {
	t.Parallel()
	controller := &fakeRotateStartupController{state: raftengine.StateLeader}
	runner := &fakeRotateStartupRunner{err: errors.New("bad sidecar")}
	_, waitStartup := installEncryptionRotateOnStartupRequestWithWait(context.Background(), controller, runner, nil)

	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := waitStartup(waitCtx)
	if err == nil || !strings.Contains(err.Error(), "bad sidecar") {
		t.Fatalf("startup wait err=%v, want bad sidecar", err)
	}
	if got := runner.runs.Load(); got != 1 {
		t.Fatalf("permanent error must not be retried; runs=%d", got)
	}
}

type fakeStartupKEK struct{}

func (fakeStartupKEK) Wrap(dek []byte) ([]byte, error) {
	if len(dek) != encryption.KeySize {
		return nil, errors.New("bad dek length")
	}
	out := make([]byte, 1, 1+len(dek))
	out[0] = 0xAA
	out = append(out, dek...)
	return out, nil
}
func (fakeStartupKEK) Unwrap(wrapped []byte) ([]byte, error) { return wrapped, nil }
func (fakeStartupKEK) Name() string                          { return "fake" }

type recordingRotateProposer struct {
	proposals [][]byte
	err       error
}

func (r *recordingRotateProposer) Propose(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return nil, errors.New("unexpected Propose")
}

func (r *recordingRotateProposer) ProposeAdmin(_ context.Context, data []byte) (*raftengine.ProposalResult, error) {
	if r.err != nil {
		return nil, r.err
	}
	cp := append([]byte(nil), data...)
	r.proposals = append(r.proposals, cp)
	return &raftengine.ProposalResult{CommitIndex: uint64(len(r.proposals))}, nil
}

type rotateStartupLeader struct {
	*recordingRotateProposer
}

func (rotateStartupLeader) State() raftengine.State            { return raftengine.StateLeader }
func (rotateStartupLeader) Leader() raftengine.LeaderInfo      { return raftengine.LeaderInfo{} }
func (rotateStartupLeader) VerifyLeader(context.Context) error { return nil }
func (rotateStartupLeader) LinearizableRead(context.Context) (uint64, error) {
	return 0, nil
}

func TestEncryptionRotateOnStartupTask_RunRotatesStorageAndRaft(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	sidecarPath := filepath.Join(dir, encryption.SidecarFilename)
	sc := &encryption.Sidecar{
		Version:          encryption.SidecarVersion,
		RaftAppliedIndex: 1,
		Active:           encryption.ActiveKeys{Storage: 7, Raft: 8},
		Keys: map[string]encryption.SidecarKey{
			"7": {Purpose: encryption.SidecarPurposeStorage, Wrapped: []byte("storage"), Created: "2026-07-07T00:00:00Z", LocalEpoch: 2},
			"8": {Purpose: encryption.SidecarPurposeRaft, Wrapped: []byte("raft"), Created: "2026-07-07T00:00:00Z", LocalEpoch: 3},
		},
	}
	if err := encryption.WriteSidecar(sidecarPath, sc); err != nil {
		t.Fatalf("WriteSidecar: %v", err)
	}
	rec := &recordingRotateProposer{}
	server := adapterServerForRotateStartup(t, sidecarPath, rec)
	task := &encryptionRotateOnStartupTask{
		server:       server,
		sidecarPath:  sidecarPath,
		kekWrapper:   fakeStartupKEK{},
		fullNodeID:   0xCAFE,
		storageEpoch: 11,
		raftEpoch:    12,
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !task.Done() {
		t.Fatal("task must be done after both active purposes rotate")
	}
	if got := len(rec.proposals); got != 2 {
		t.Fatalf("proposals=%d, want 2", got)
	}
	assertRotateProposal(t, rec.proposals[0], pb.RotateDEKRequest_PURPOSE_STORAGE, 9, 11)
	assertRotateProposal(t, rec.proposals[1], pb.RotateDEKRequest_PURPOSE_RAFT, 10, 12)
}

func TestEncryptionRotateOnStartupTask_RunFencesBeforeSidecarRead(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	sidecarPath := filepath.Join(dir, encryption.SidecarFilename)
	sc := &encryption.Sidecar{
		Version:          encryption.SidecarVersion,
		RaftAppliedIndex: 10,
		Active:           encryption.ActiveKeys{Storage: 7},
		Keys: map[string]encryption.SidecarKey{
			"7": {Purpose: encryption.SidecarPurposeStorage, Wrapped: []byte("storage"), Created: "2026-07-07T00:00:00Z"},
		},
	}
	rec := &recordingRotateProposer{}
	var fenced atomic.Bool
	task := &encryptionRotateOnStartupTask{
		server:      adapterServerForRotateStartup(t, sidecarPath, rec),
		sidecarPath: sidecarPath,
		kekWrapper:  fakeStartupKEK{},
		fullNodeID:  0xCAFE,
		readFence: func(context.Context) (uint64, error) {
			fenced.Store(true)
			if err := encryption.WriteSidecar(sidecarPath, sc); err != nil {
				return 0, err
			}
			return sc.RaftAppliedIndex, nil
		},
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !fenced.Load() {
		t.Fatal("read fence was not called")
	}
	if got := len(rec.proposals); got != 1 {
		t.Fatalf("proposals=%d, want 1 after fenced sidecar read", got)
	}
	assertRotateProposal(t, rec.proposals[0], pb.RotateDEKRequest_PURPOSE_STORAGE, 8, 0)
}

func TestEncryptionRotateOnStartupTask_RetryRereadsSidecarKeyID(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	sidecarPath := filepath.Join(dir, encryption.SidecarFilename)
	sc := &encryption.Sidecar{
		Version:          encryption.SidecarVersion,
		RaftAppliedIndex: 1,
		Active:           encryption.ActiveKeys{Storage: 7},
		Keys: map[string]encryption.SidecarKey{
			"7": {Purpose: encryption.SidecarPurposeStorage, Wrapped: []byte("storage"), Created: "2026-07-07T00:00:00Z"},
		},
	}
	if err := encryption.WriteSidecar(sidecarPath, sc); err != nil {
		t.Fatalf("WriteSidecar: %v", err)
	}
	rec := &recordingRotateProposer{err: status.Error(codes.Unavailable, "commit result uncertain")}
	task := &encryptionRotateOnStartupTask{
		server:      adapterServerForRotateStartup(t, sidecarPath, rec),
		sidecarPath: sidecarPath,
		kekWrapper:  fakeStartupKEK{},
		fullNodeID:  0xCAFE,
	}
	if err := task.Run(context.Background()); err == nil {
		t.Fatal("Run returned nil, want retryable rotate error")
	}
	sc.Active.Storage = 8
	sc.Keys["8"] = encryption.SidecarKey{
		Purpose: encryption.SidecarPurposeStorage,
		Wrapped: []byte("storage-v2"),
		Created: "2026-07-07T00:00:01Z",
	}
	if err := encryption.WriteSidecar(sidecarPath, sc); err != nil {
		t.Fatalf("WriteSidecar retry fixture: %v", err)
	}
	rec.err = nil
	if err := task.Run(context.Background()); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if got := len(rec.proposals); got != 1 {
		t.Fatalf("successful proposals=%d, want 1", got)
	}
	assertRotateProposal(t, rec.proposals[0], pb.RotateDEKRequest_PURPOSE_STORAGE, 9, 0)
}

func TestEncryptionRotateOnStartupTask_KeyIDExhaustionDoesNotMarkDone(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	sidecarPath := filepath.Join(dir, encryption.SidecarFilename)
	sc := &encryption.Sidecar{
		Version:          encryption.SidecarVersion,
		RaftAppliedIndex: 1,
		Active:           encryption.ActiveKeys{Storage: 7},
		Keys: map[string]encryption.SidecarKey{
			"7":          {Purpose: encryption.SidecarPurposeStorage, Wrapped: []byte("storage")},
			"4294967295": {Purpose: encryption.SidecarPurposeRaft, Wrapped: []byte("raft")},
		},
	}
	if err := encryption.WriteSidecar(sidecarPath, sc); err != nil {
		t.Fatalf("WriteSidecar: %v", err)
	}
	task := &encryptionRotateOnStartupTask{
		server:      adapterServerForRotateStartup(t, sidecarPath, &recordingRotateProposer{}),
		sidecarPath: sidecarPath,
		kekWrapper:  fakeStartupKEK{},
		fullNodeID:  0xCAFE,
	}
	if err := task.Run(context.Background()); err == nil {
		t.Fatal("Run returned nil, want key-id exhaustion error")
	}
	if task.Done() {
		t.Fatal("key-id exhaustion must not mark rotation done")
	}
}

func TestRetryableEncryptionRotateOnStartupError_UnwrapsStatus(t *testing.T) {
	t.Parallel()
	err := crdberrors.Wrap(status.Error(codes.Unavailable, "try later"), "wrapped")
	if !retryableEncryptionRotateOnStartupError(err) {
		t.Fatal("wrapped Unavailable status must be retryable")
	}
}

func adapterServerForRotateStartup(t *testing.T, sidecarPath string, rec *recordingRotateProposer) *adapter.EncryptionAdminServer {
	t.Helper()
	leader := rotateStartupLeader{recordingRotateProposer: rec}
	server := adapter.NewEncryptionAdminServer(
		adapter.WithEncryptionAdminSidecarPath(sidecarPath),
		adapter.WithEncryptionAdminFullNodeID(0xCAFE),
		adapter.WithEncryptionAdminProposer(leader),
		adapter.WithEncryptionAdminLeaderView(leader),
	)
	if err := server.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	return server
}

func assertRotateProposal(t *testing.T, raw []byte, purpose pb.RotateDEKRequest_Purpose, wantDEKID uint32, wantEpoch uint16) {
	t.Helper()
	if len(raw) == 0 {
		t.Fatal("empty proposal")
	}
	if raw[0] != fsmwire.OpRotation {
		t.Fatalf("proposal tag=%#v, want OpRotation", raw)
	}
	got, err := fsmwire.DecodeRotation(raw[1:])
	if err != nil {
		t.Fatalf("DecodeRotation: %v", err)
	}
	wantPurpose := rotateStartupWirePurpose(purpose)
	if got.SubTag != fsmwire.RotateSubRotateDEK || got.Purpose != wantPurpose || got.DEKID != wantDEKID {
		t.Fatalf("rotation payload=%+v, want subtag rotate purpose=%v dek=%d", got, wantPurpose, wantDEKID)
	}
	if got.ProposerRegistration.FullNodeID != 0xCAFE ||
		got.ProposerRegistration.DEKID != wantDEKID ||
		got.ProposerRegistration.LocalEpoch != wantEpoch {
		t.Fatalf("registration=%+v, want node=0xCAFE dek=%d epoch=%d",
			got.ProposerRegistration, wantDEKID, wantEpoch)
	}
	assertRotateWrappedDEK(t, got.Wrapped)
}

func rotateStartupWirePurpose(purpose pb.RotateDEKRequest_Purpose) fsmwire.Purpose {
	if purpose == pb.RotateDEKRequest_PURPOSE_RAFT {
		return fsmwire.PurposeRaft
	}
	return fsmwire.PurposeStorage
}

func assertRotateWrappedDEK(t *testing.T, wrapped []byte) {
	t.Helper()
	if len(wrapped) != 1+encryption.KeySize {
		t.Fatalf("wrapped len=%d, want %d", len(wrapped), 1+encryption.KeySize)
	}
	if wrapped[0] != 0xAA {
		t.Fatalf("wrapped len/prefix=(%d,%#x), want (%d,0xAA)",
			len(wrapped), wrapped[0], 1+encryption.KeySize)
	}
}

func TestNextEncryptionRotateOnStartupKeyID_Exhausted(t *testing.T) {
	t.Parallel()
	_, err := nextEncryptionRotateOnStartupKeyID(&encryption.Sidecar{
		Keys: map[string]encryption.SidecarKey{
			"4294967295": {Purpose: encryption.SidecarPurposeStorage},
		},
	})
	if err == nil {
		t.Fatal("max key id must refuse rotation")
	}
}
