package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
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
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	pb "github.com/bootjp/elastickv/proto"
	crdberrors "github.com/cockroachdb/errors"
	"google.golang.org/grpc"
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

type raceFireRotateStartupController struct {
	fakeRotateStartupController
	stateCalls atomic.Int32
	runner     *blockingRunRotateStartupRunner
}

func (r *raceFireRotateStartupController) State() raftengine.State {
	r.mu.Lock()
	state := r.state
	callbacks := append([]func(){}, r.callbacks...)
	r.mu.Unlock()
	if r.stateCalls.Add(1) == 2 {
		go func() {
			for _, cb := range callbacks {
				if cb != nil {
					cb()
				}
			}
		}()
		select {
		case <-r.runner.started:
		case <-time.After(2 * time.Second):
		}
	}
	return state
}

type stubStartupCoordinator struct {
	dispatches   atomic.Int32
	nextCalls    atomic.Int32
	nextAfter    atomic.Int32
	recoverCalls atomic.Int32
	clock        *kv.HLC
	nextErr      error
	nextAfterErr error
	recoverErr   error
}

func (s *stubStartupCoordinator) Dispatch(context.Context, *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	s.dispatches.Add(1)
	return &kv.CoordinateResponse{}, nil
}

func (s *stubStartupCoordinator) IsLeader() bool { return true }

func (s *stubStartupCoordinator) VerifyLeader(context.Context) error { return nil }

func (s *stubStartupCoordinator) LinearizableRead(context.Context) (uint64, error) { return 1, nil }

func (s *stubStartupCoordinator) RaftLeader() string { return "n1" }

func (s *stubStartupCoordinator) IsLeaderForKey([]byte) bool { return true }

func (s *stubStartupCoordinator) VerifyLeaderForKey(context.Context, []byte) error { return nil }

func (s *stubStartupCoordinator) RaftLeaderForKey([]byte) string { return "n1" }

func (s *stubStartupCoordinator) Clock() *kv.HLC {
	if s.clock == nil {
		s.clock = kv.NewHLC()
	}
	return s.clock
}

func (s *stubStartupCoordinator) Next(context.Context) (uint64, error) {
	s.nextCalls.Add(1)
	return 101, s.nextErr
}

func (s *stubStartupCoordinator) NextAfter(_ context.Context, min uint64) (uint64, error) {
	s.nextAfter.Add(1)
	return min + 1, s.nextAfterErr
}

func (s *stubStartupCoordinator) RecoverHLCLease(context.Context) error {
	s.recoverCalls.Add(1)
	return s.recoverErr
}

func TestInstallEncryptionRotateOnStartupRequest_ImmediateLeaderWaitRuns(t *testing.T) {
	t.Parallel()
	controller := &fakeRotateStartupController{state: raftengine.StateLeader}
	runner := &fakeRotateStartupRunner{}
	deregister, waitStartup := installEncryptionRotateOnStartupRequestWithWait(context.Background(), controller, runner, slog.Default())
	defer deregister()
	if got := runner.runs.Load(); got != 0 {
		t.Fatalf("install must defer immediate-leader rotation until startup wait; runs=%d", got)
	}
	if err := waitStartup.Wait(context.Background()); err != nil {
		t.Fatalf("waitStartup: %v", err)
	}
	if got := runner.runs.Load(); got != 1 {
		t.Fatalf("startup wait must run immediate-leader rotation once; runs=%d", got)
	}
}

func TestInstallEncryptionRotateOnStartupRequest_FollowerDefersUntilStartupWait(t *testing.T) {
	t.Parallel()
	controller := &fakeRotateStartupController{state: raftengine.StateFollower}
	runner := &fakeRotateStartupRunner{}
	deregister, waitStartup := installEncryptionRotateOnStartupRequestWithWait(context.Background(), controller, runner, nil)
	defer deregister()
	if got := runner.runs.Load(); got != 0 {
		t.Fatalf("follower install must not run immediately; runs=%d", got)
	}
	controller.becomeLeader()
	time.Sleep(25 * time.Millisecond)
	if got := runner.runs.Load(); got != 0 {
		t.Fatalf("leader callback before startup wait must not run rotation; runs=%d", got)
	}
	if err := waitStartup.Wait(context.Background()); err != nil {
		t.Fatalf("waitStartup: %v", err)
	}
	if got := runner.runs.Load(); got != 1 {
		t.Fatalf("startup wait must run deferred leader rotation once; runs=%d", got)
	}
}

func TestInstallEncryptionRotateOnStartupRequest_RetriesTransientWhileLeader(t *testing.T) {
	oldDelay := encryptionRotateOnStartupRetryDelay
	encryptionRotateOnStartupRetryDelay = time.Millisecond
	defer func() { encryptionRotateOnStartupRetryDelay = oldDelay }()

	controller := &fakeRotateStartupController{state: raftengine.StateLeader}
	runner := &retryOnceRotateStartupRunner{err: status.Error(codes.Unavailable, "try again")}
	deregister, waitStartup := installEncryptionRotateOnStartupRequestWithWait(context.Background(), controller, runner, nil)
	defer deregister()
	if got := runner.runs.Load(); got != 0 {
		t.Fatalf("install must defer immediate-leader retry loop until startup wait; runs=%d", got)
	}
	if err := waitStartup.Wait(context.Background()); err != nil {
		t.Fatalf("waitStartup: %v", err)
	}
	if got := runner.runs.Load(); got != 2 {
		t.Fatalf("transient startup rotation must retry before startup wait returns; runs=%d", got)
	}
}

func TestInstallEncryptionRotateOnStartupRequest_CallbackDoesNotBlockOnDone(t *testing.T) {
	controller := &fakeRotateStartupController{state: raftengine.StateFollower}
	runner := &blockingDoneRotateStartupRunner{
		doneEntered: make(chan struct{}),
		releaseDone: make(chan struct{}),
	}
	deregister, waitStartup := installEncryptionRotateOnStartupRequestWithWait(context.Background(), controller, runner, nil)
	defer deregister()
	defer close(runner.releaseDone)

	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := waitStartup.Wait(waitCtx); err != nil {
		t.Fatalf("startup wait while follower: %v", err)
	}

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

	if err := waitStartup.Wait(context.Background()); err != nil {
		t.Fatalf("initial follower wait: %v", err)
	}
	controller.becomeLeader()
	select {
	case <-runner.started:
	case <-time.After(2 * time.Second):
		t.Fatal("async rotation did not start")
	}

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- waitStartup.Wait(context.Background())
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

func TestInstallEncryptionRotateOnStartupRequest_WaitContinuesAfterConcurrentFire(t *testing.T) {
	runner := &blockingRunRotateStartupRunner{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	controller := &raceFireRotateStartupController{
		fakeRotateStartupController: fakeRotateStartupController{state: raftengine.StateLeader},
		runner:                      runner,
	}
	deregister, waitStartup := installEncryptionRotateOnStartupRequestWithWait(context.Background(), controller, runner, nil)
	defer deregister()

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- waitStartup.Wait(context.Background())
	}()
	select {
	case <-runner.started:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent startup rotation did not start")
	}
	select {
	case err := <-waitDone:
		t.Fatalf("startup wait returned before concurrent rotation finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(runner.release)
	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("startup wait returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("startup wait did not finish after concurrent rotation completed")
	}
}

func TestInstallEncryptionRotateOnStartupRequest_BlocksMutatorsDuringAsyncStartupFlight(t *testing.T) {
	controller := &fakeRotateStartupController{state: raftengine.StateFollower}
	runner := &blockingRunRotateStartupRunner{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	deregister, waitStartup := installEncryptionRotateOnStartupRequestWithWait(context.Background(), controller, runner, nil)
	defer deregister()

	if err := waitStartup.Wait(context.Background()); err != nil {
		t.Fatalf("initial follower wait: %v", err)
	}
	controller.becomeLeader()
	select {
	case <-runner.started:
	case <-time.After(2 * time.Second):
		t.Fatal("async startup rotation did not start")
	}
	if !waitStartup.BlockMutators() {
		t.Fatal("startup mutator gate must remain closed while async rotation is in flight")
	}

	close(runner.release)
	deadline := time.After(2 * time.Second)
	for waitStartup.BlockMutators() {
		select {
		case <-deadline:
			t.Fatal("startup mutator gate did not open after async rotation finished")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestInstallEncryptionRotateOnStartupRequest_WaitReturnsPermanentError(t *testing.T) {
	t.Parallel()
	controller := &fakeRotateStartupController{state: raftengine.StateLeader}
	runner := &fakeRotateStartupRunner{err: errors.New("bad sidecar")}
	_, waitStartup := installEncryptionRotateOnStartupRequestWithWait(context.Background(), controller, runner, nil)

	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := waitStartup.Wait(waitCtx)
	if err == nil || !strings.Contains(err.Error(), "bad sidecar") {
		t.Fatalf("startup wait err=%v, want bad sidecar", err)
	}
	if got := runner.runs.Load(); got != 1 {
		t.Fatalf("permanent error must not be retried; runs=%d", got)
	}
}

func TestStartupPublicKVGate_BlocksMutatorsUntilReady(t *testing.T) {
	t.Parallel()
	gate := &startupPublicKVGate{}

	blockedMethods := []string{
		pb.RawKV_RawGet_FullMethodName,
		pb.TransactionalKV_Get_FullMethodName,
		pb.Internal_Forward_FullMethodName,
		pb.AdminForward_Forward_FullMethodName,
		pb.Distribution_SplitRange_FullMethodName,
		pb.RaftAdmin_AddVoter_FullMethodName,
		pb.RaftAdmin_AddLearner_FullMethodName,
		pb.RaftAdmin_PromoteLearner_FullMethodName,
		pb.RaftAdmin_RemoveServer_FullMethodName,
		pb.RaftAdmin_TransferLeadership_FullMethodName,
		pb.EncryptionAdmin_BootstrapEncryption_FullMethodName,
		pb.EncryptionAdmin_RotateDEK_FullMethodName,
		pb.EncryptionAdmin_RegisterEncryptionWriter_FullMethodName,
		pb.EncryptionAdmin_ResyncSidecar_FullMethodName,
		pb.EncryptionAdmin_EnableStorageEnvelope_FullMethodName,
		pb.EncryptionAdmin_EnableRaftEnvelope_FullMethodName,
		pb.S3BlobFetch_PushChunkBlob_FullMethodName,
	}

	for _, method := range blockedMethods {
		t.Run("blocked/"+method, func(t *testing.T) {
			handlerCalled := false
			handler := func(context.Context, interface{}) (interface{}, error) {
				handlerCalled = true
				return "ok", nil
			}
			if _, err := gate.unaryInterceptor(
				context.Background(), nil,
				&grpc.UnaryServerInfo{FullMethod: method},
				handler,
			); status.Code(err) != codes.Unavailable {
				t.Fatalf("%s before ready err=%v, want Unavailable", method, err)
			}
			if handlerCalled {
				t.Fatalf("%s handler ran before startup gate opened", method)
			}
		})
	}

	allowedMethods := []string{
		pb.Internal_RelayPublish_FullMethodName,
		pb.RaftAdmin_Status_FullMethodName,
		pb.RaftAdmin_Configuration_FullMethodName,
		pb.EncryptionAdmin_GetCapability_FullMethodName,
		pb.EncryptionAdmin_GetSidecarState_FullMethodName,
	}
	for _, method := range allowedMethods {
		t.Run("allowed/"+method, func(t *testing.T) {
			handlerCalled := false
			handler := func(context.Context, interface{}) (interface{}, error) {
				handlerCalled = true
				return "ok", nil
			}
			if _, err := gate.unaryInterceptor(
				context.Background(), nil,
				&grpc.UnaryServerInfo{FullMethod: method},
				handler,
			); err != nil {
				t.Fatalf("%s before ready must pass: %v", method, err)
			}
			if !handlerCalled {
				t.Fatalf("%s handler did not run", method)
			}
		})
	}

	gate.markReady()
	for _, method := range blockedMethods {
		t.Run("ready/"+method, func(t *testing.T) {
			handlerCalled := false
			handler := func(context.Context, interface{}) (interface{}, error) {
				handlerCalled = true
				return "ok", nil
			}
			if _, err := gate.unaryInterceptor(
				context.Background(), nil,
				&grpc.UnaryServerInfo{FullMethod: method},
				handler,
			); err != nil {
				t.Fatalf("%s after ready err=%v", method, err)
			}
			if !handlerCalled {
				t.Fatalf("%s handler did not run after startup gate opened", method)
			}
		})
	}
}

func TestStartupPublicKVGate_BlocksMutatorStreamsUntilReady(t *testing.T) {
	t.Parallel()
	gate := &startupPublicKVGate{}

	handlerCalled := false
	handler := func(interface{}, grpc.ServerStream) error {
		handlerCalled = true
		return nil
	}
	err := gate.streamInterceptor(
		nil,
		nil,
		&grpc.StreamServerInfo{FullMethod: pb.S3BlobFetch_PushChunkBlob_FullMethodName},
		handler,
	)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("PushChunkBlob before ready err=%v, want Unavailable", err)
	}
	if handlerCalled {
		t.Fatal("stream handler ran before startup gate opened")
	}

	gate.markReady()
	if err := gate.streamInterceptor(
		nil,
		nil,
		&grpc.StreamServerInfo{FullMethod: pb.S3BlobFetch_PushChunkBlob_FullMethodName},
		handler,
	); err != nil {
		t.Fatalf("PushChunkBlob after ready err=%v", err)
	}
	if !handlerCalled {
		t.Fatal("stream handler did not run after startup gate opened")
	}
}

func TestStartupPublicKVGate_BlocksAfterReadyWhenStartupRotationPending(t *testing.T) {
	t.Parallel()
	blocked := true
	gate := &startupPublicKVGate{blockMutator: func() bool { return blocked }}
	gate.markReady()

	handlerCalled := false
	handler := func(context.Context, interface{}) (interface{}, error) {
		handlerCalled = true
		return "ok", nil
	}
	if _, err := gate.unaryInterceptor(
		context.Background(), nil,
		&grpc.UnaryServerInfo{FullMethod: pb.RaftAdmin_AddVoter_FullMethodName},
		handler,
	); status.Code(err) != codes.Unavailable {
		t.Fatalf("ready gate with pending startup rotation err=%v, want Unavailable", err)
	}
	if handlerCalled {
		t.Fatal("gated handler ran while startup rotation blocker was active")
	}

	blocked = false
	if _, err := gate.unaryInterceptor(
		context.Background(), nil,
		&grpc.UnaryServerInfo{FullMethod: pb.RaftAdmin_AddVoter_FullMethodName},
		handler,
	); err != nil {
		t.Fatalf("gate after blocker cleared: %v", err)
	}
	if !handlerCalled {
		t.Fatal("gated handler did not run after blocker cleared")
	}
}

func TestStartupGatedCoordinator_BlocksAdapterDispatchDuringAsyncRotation(t *testing.T) {
	t.Parallel()
	inner := &stubStartupCoordinator{clock: kv.NewHLC()}
	blocked := true
	gate := &startupPublicKVGate{blockMutator: func() bool { return blocked }}
	gate.markReady()
	coord := startupGatedCoordinator{inner: inner, gate: gate}

	if _, err := coord.Dispatch(context.Background(), nil); status.Code(err) != codes.Unavailable {
		t.Fatalf("blocked Dispatch err=%v, want Unavailable", err)
	}
	if got := inner.dispatches.Load(); got != 0 {
		t.Fatalf("inner Dispatch calls while blocked=%d, want 0", got)
	}
	if _, err := coord.LinearizableRead(context.Background()); err != nil {
		t.Fatalf("LinearizableRead should pass through while Dispatch is blocked: %v", err)
	}

	blocked = false
	if _, err := coord.Dispatch(context.Background(), nil); err != nil {
		t.Fatalf("Dispatch after blocker cleared: %v", err)
	}
	if got := inner.dispatches.Load(); got != 1 {
		t.Fatalf("inner Dispatch calls after unblock=%d, want 1", got)
	}
}

func TestStartupGatedCoordinator_ForwardsTimestampCapabilities(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("recover failed")
	inner := &stubStartupCoordinator{clock: kv.NewHLC(), recoverErr: sentinel}
	coord := startupGatedCoordinator{
		inner: inner,
		gate:  &startupPublicKVGate{blockMutator: func() bool { return true }},
	}

	ts, err := coord.Next(context.Background())
	if err != nil || ts != 101 {
		t.Fatalf("Next() = (%d, %v), want (101, nil)", ts, err)
	}
	next, err := coord.NextAfter(context.Background(), 200)
	if err != nil || next != 201 {
		t.Fatalf("NextAfter() = (%d, %v), want (201, nil)", next, err)
	}
	if err := coord.RecoverHLCLease(context.Background()); !errors.Is(err, sentinel) {
		t.Fatalf("RecoverHLCLease() err=%v, want sentinel", err)
	}
	if got := inner.nextCalls.Load(); got != 1 {
		t.Fatalf("inner Next calls=%d, want 1", got)
	}
	if got := inner.nextAfter.Load(); got != 1 {
		t.Fatalf("inner NextAfter calls=%d, want 1", got)
	}
	if got := inner.recoverCalls.Load(); got != 1 {
		t.Fatalf("inner RecoverHLCLease calls=%d, want 1", got)
	}
	if got := inner.dispatches.Load(); got != 0 {
		t.Fatalf("timestamp capability forwarding called Dispatch %d times, want 0", got)
	}
}

func TestRuntimeServerRunner_PrepareAdminForwardServersDoesNotBindPublicListeners(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	lc := &net.ListenConfig{}
	heldDynamo, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen held dynamo: %v", err)
	}
	defer heldDynamo.Close()
	heldS3, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen held s3: %v", err)
	}
	defer heldS3.Close()

	runner := runtimeServerRunner{
		ctx:             ctx,
		lc:              lc,
		dynamoAddress:   heldDynamo.Addr().String(),
		s3Address:       heldS3.Addr().String(),
		s3PathStyleOnly: true,
		metricsRegistry: monitoring.NewRegistry("test-node", "127.0.0.1:0"),
		coordinate: startupGatedCoordinator{
			inner: &stubStartupCoordinator{clock: kv.NewHLC()},
		},
	}
	if err := runner.prepareAdminForwardServers(); err != nil {
		t.Fatalf("prepareAdminForwardServers: %v", err)
	}
	if runner.dynamoServer == nil {
		t.Fatal("dynamo admin-forward server was not prepared")
	}
	if runner.s3Server == nil {
		t.Fatal("s3 admin-forward server was not prepared")
	}
	if runner.dynamoListener != nil || runner.s3Listener != nil {
		t.Fatalf("prepareAdminForwardServers bound listeners: dynamo=%v s3=%v", runner.dynamoListener, runner.s3Listener)
	}
}

func TestRuntimeServerRunner_PreparePublicServicesBindsBeforeRotation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	lc := &net.ListenConfig{}
	heldDynamo, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen held dynamo: %v", err)
	}
	defer heldDynamo.Close()

	runner := runtimeServerRunner{
		ctx:             ctx,
		lc:              lc,
		redisAddress:    "127.0.0.1:0",
		dynamoAddress:   heldDynamo.Addr().String(),
		s3PathStyleOnly: true,
		metricsAddress:  "",
		pprofAddress:    "",
		pubsubRelay:     adapter.NewRedisPubSubRelay(),
		metricsRegistry: monitoring.NewRegistry("test-node", "127.0.0.1:0"),
		coordinate:      &stubStartupCoordinator{clock: kv.NewHLC()},
	}
	if err := runner.prepareAdminForwardServers(); err != nil {
		t.Fatalf("prepareAdminForwardServers: %v", err)
	}
	err = runner.preparePublicServices()
	if err == nil || !strings.Contains(err.Error(), "failed to listen") {
		t.Fatalf("preparePublicServices err=%v, want occupied listener error before rotation", err)
	}
	runner.closePreparedExternalListeners()
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

type blockingRotateProposer struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingRotateProposer) Propose(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return nil, errors.New("unexpected Propose")
}

func (b *blockingRotateProposer) ProposeAdmin(context.Context, []byte) (*raftengine.ProposalResult, error) {
	b.once.Do(func() { close(b.started) })
	<-b.release
	return &raftengine.ProposalResult{CommitIndex: 1}, nil
}

type rotateStartupLeader struct {
	raftengine.Proposer
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

func TestEncryptionRotateOnStartupTask_DoneDoesNotWaitForRotateDEK(t *testing.T) {
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
	blocking := &blockingRotateProposer{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	task := &encryptionRotateOnStartupTask{
		server:      adapterServerForRotateStartupProposer(t, sidecarPath, blocking),
		sidecarPath: sidecarPath,
		kekWrapper:  fakeStartupKEK{},
		fullNodeID:  0xCAFE,
	}
	runDone := make(chan error, 1)
	go func() {
		runDone <- task.Run(context.Background())
	}()
	select {
	case <-blocking.started:
	case <-time.After(2 * time.Second):
		t.Fatal("RotateDEK proposal did not start")
	}

	doneReturned := make(chan bool, 1)
	go func() { doneReturned <- task.Done() }()
	select {
	case got := <-doneReturned:
		if got {
			t.Fatal("task must not be done while RotateDEK is still in flight")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Done blocked behind in-flight RotateDEK")
	}

	close(blocking.release)
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run after release: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not finish after RotateDEK release")
	}
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

func TestEncryptionRotateOnStartupTask_PreflightsKeyIDCapacity(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	sidecarPath := filepath.Join(dir, encryption.SidecarFilename)
	sc := &encryption.Sidecar{
		Version:          encryption.SidecarVersion,
		RaftAppliedIndex: 1,
		Active:           encryption.ActiveKeys{Storage: 7, Raft: 8},
		Keys: map[string]encryption.SidecarKey{
			"7":          {Purpose: encryption.SidecarPurposeStorage, Wrapped: []byte("storage")},
			"8":          {Purpose: encryption.SidecarPurposeRaft, Wrapped: []byte("raft")},
			"4294967294": {Purpose: encryption.SidecarPurposeStorage, Wrapped: []byte("near-max")},
		},
	}
	if err := encryption.WriteSidecar(sidecarPath, sc); err != nil {
		t.Fatalf("WriteSidecar: %v", err)
	}
	rec := &recordingRotateProposer{}
	task := &encryptionRotateOnStartupTask{
		server:      adapterServerForRotateStartup(t, sidecarPath, rec),
		sidecarPath: sidecarPath,
		kekWrapper:  fakeStartupKEK{},
		fullNodeID:  0xCAFE,
	}
	if err := task.Run(context.Background()); err == nil {
		t.Fatal("Run returned nil, want preflight key-id exhaustion error")
	}
	if len(rec.proposals) != 0 {
		t.Fatalf("proposals=%d, want 0 before capacity preflight passes", len(rec.proposals))
	}
	if task.Done() {
		t.Fatal("key-id preflight exhaustion must not mark rotation done")
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
	return adapterServerForRotateStartupProposer(t, sidecarPath, rec)
}

func adapterServerForRotateStartupProposer(t *testing.T, sidecarPath string, proposer raftengine.Proposer) *adapter.EncryptionAdminServer {
	t.Helper()
	leader := rotateStartupLeader{Proposer: proposer}
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
