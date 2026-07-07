package main

import (
	"context"
	"errors"
	"path/filepath"
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

func TestInstallEncryptionRotateOnStartupRequest_ImmediateLeaderBlocks(t *testing.T) {
	t.Parallel()
	controller := &fakeRotateStartupController{state: raftengine.StateLeader}
	runner := &fakeRotateStartupRunner{}
	deregister := installEncryptionRotateOnStartupRequest(context.Background(), controller, runner, nil)
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

func TestInstallEncryptionRotateOnStartupRequest_RechecksLeaderAfterRegister(t *testing.T) {
	t.Parallel()
	controller := &fakeRotateStartupController{state: raftengine.StateLeader}
	runner := &fakeRotateStartupRunner{err: raftengine.ErrLeadershipLost}
	deregister := installEncryptionRotateOnStartupRequest(context.Background(), controller, runner, nil)
	defer deregister()
	deadline := time.Now().Add(2 * time.Second)
	for runner.runs.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := runner.runs.Load(); got < 2 {
		t.Fatalf("leader recheck after callback registration did not retry pending rotation; runs=%d", got)
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

func TestEncryptionRotateOnStartupTask_RetryableRotateErrorSkipsUncertainKeyID(t *testing.T) {
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
	if got := task.nextKeyID; got != 9 {
		t.Fatalf("nextKeyID=%d, want 9 after treating key 8 as uncertain/committed", got)
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
