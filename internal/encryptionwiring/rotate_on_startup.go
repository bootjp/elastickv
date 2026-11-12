package encryptionwiring

import (
	"context"
	"crypto/rand"
	"log/slog"
	"math"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var encryptionRotateOnStartupRetryDelay = 200 * time.Millisecond
var encryptionRotateOnStartupFlightPublishPoll = 10 * time.Millisecond

type encryptionRotateOnStartupController interface {
	raftengine.LeaderView
	RegisterLeaderAcquiredCallback(fn func()) (deregister func())
}

type encryptionRotateOnStartupRunner interface {
	Run(context.Context) error
	Done() bool
}

type encryptionRotateOnStartupFlightState struct {
	inFlight atomic.Bool

	Mu          sync.Mutex
	done        <-chan struct{}
	terminalErr error
}

type encryptionRotateOnStartupTask struct {
	server       *adapter.EncryptionAdminServer
	SidecarPath  string
	kekWrapper   kek.Wrapper
	RaftEnvelope *RaftEnvelopeRuntime
	readFence    func(context.Context) (uint64, error)
	FullNodeID   uint64
	StorageEpoch uint16
	RaftEpoch    uint16

	Mu          sync.Mutex
	nextKeyID   uint32
	storageDone bool
	raftDone    bool
}

func InstallRotateOnStartup(
	ctx context.Context,
	requested bool,
	rt *noderuntime.GroupRuntime,
	postCutoverProposer raftengine.Proposer,
	sidecarPath string,
	kekWrapper kek.Wrapper,
	raftEnvelope *RaftEnvelopeRuntime,
	fullNodeID uint64,
	storageEpoch uint16,
	raftEpoch uint16,
	logger *slog.Logger,
) (func(), StartupRotationWaiter) {
	if !requested {
		return func() {}, StartupRotationWaiter{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	if rt == nil || rt.Engine == nil {
		logger.Warn("encryption rotate-on-startup skipped: default raft group is not available")
		return func() {}, StartupRotationWaiter{}
	}
	controller, ok := rt.Engine.(encryptionRotateOnStartupController)
	if !ok {
		logger.Warn("encryption rotate-on-startup skipped: engine does not implement leader-acquired observer")
		return func() {}, StartupRotationWaiter{}
	}
	if sidecarPath == "" || kekWrapper == nil {
		logger.Warn("encryption rotate-on-startup skipped: encryption sidecar or KEK is not configured")
		return func() {}, StartupRotationWaiter{}
	}
	server := adapter.NewEncryptionAdminServer(
		adapter.WithEncryptionAdminSidecarPath(sidecarPath),
		adapter.WithEncryptionAdminFullNodeID(fullNodeID),
		adapter.WithEncryptionAdminProposer(rt.Engine),
		adapter.WithEncryptionAdminLeaderView(rt.Engine),
		adapter.WithEncryptionAdminPostCutoverProposer(postCutoverProposer),
	)
	if err := server.Validate(); err != nil {
		logger.Warn("encryption rotate-on-startup skipped: admin server wiring invalid", "err", err)
		return func() {}, StartupRotationWaiter{}
	}
	task := &encryptionRotateOnStartupTask{
		server:       server,
		SidecarPath:  sidecarPath,
		kekWrapper:   kekWrapper,
		RaftEnvelope: raftEnvelope,
		readFence:    controller.LinearizableRead,
		FullNodeID:   fullNodeID,
		StorageEpoch: storageEpoch,
		RaftEpoch:    raftEpoch,
	}
	return installEncryptionRotateOnStartupRequestWithWait(ctx, controller, task, logger)
}

type StartupRotationWaiter struct {
	WaitFunc     func(context.Context) error
	BlockMutator func() bool
}

func (w StartupRotationWaiter) Wait(ctx context.Context) error {
	if w.WaitFunc == nil {
		return nil
	}
	return w.WaitFunc(ctx)
}

func (w StartupRotationWaiter) BlockMutators() bool {
	return w.BlockMutator != nil && w.BlockMutator()
}

func installEncryptionRotateOnStartupRequestWithWait(
	ctx context.Context,
	controller encryptionRotateOnStartupController,
	task encryptionRotateOnStartupRunner,
	logger *slog.Logger,
) (func(), StartupRotationWaiter) {
	if controller == nil || task == nil {
		return func() {}, StartupRotationWaiter{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	var flight encryptionRotateOnStartupFlightState
	var callbacksEnabled atomic.Bool
	fire := func(block bool) {
		_ = fireEncryptionRotateOnStartup(ctx, controller, task, logger, &flight, block)
	}
	deregister := controller.RegisterLeaderAcquiredCallback(func() {
		if !callbacksEnabled.Load() {
			return
		}
		fire(false)
	})
	waitStartup := func(waitCtx context.Context) error {
		callbacksEnabled.Store(true)
		return waitEncryptionRotateOnStartupIdle(waitCtx, controller, task, logger, &flight)
	}
	blockMutators := func() bool {
		if flight.terminalError() != nil {
			return true
		}
		return controller.State() == raftengine.StateLeader && !task.Done()
	}
	return deregister, StartupRotationWaiter{WaitFunc: waitStartup, BlockMutator: blockMutators}
}

func (s *encryptionRotateOnStartupFlightState) publishDone(done <-chan struct{}) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.done = done
}

func (s *encryptionRotateOnStartupFlightState) currentDone() <-chan struct{} {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	return s.done
}

func (s *encryptionRotateOnStartupFlightState) clearDone(done <-chan struct{}) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if s.done == done {
		s.done = nil
	}
}

func (s *encryptionRotateOnStartupFlightState) setTerminalError(err error) {
	if err == nil {
		return
	}
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if s.terminalErr == nil {
		s.terminalErr = err
	}
}

func (s *encryptionRotateOnStartupFlightState) terminalError() error {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	return s.terminalErr
}

func fireEncryptionRotateOnStartup(
	ctx context.Context,
	controller encryptionRotateOnStartupController,
	task encryptionRotateOnStartupRunner,
	logger *slog.Logger,
	flight *encryptionRotateOnStartupFlightState,
	block bool,
) <-chan struct{} {
	if flight.terminalError() != nil {
		return nil
	}
	if controller.State() != raftengine.StateLeader {
		return nil
	}
	if !flight.inFlight.CompareAndSwap(false, true) {
		return nil
	}
	done := make(chan struct{})
	flight.publishDone(done)
	run := func() {
		defer close(done)
		flight.setTerminalError(runEncryptionRotateOnStartup(ctx, controller, task, logger, &flight.inFlight))
	}
	if block {
		run()
		return done
	}
	go run()
	return done
}

func runEncryptionRotateOnStartup(
	ctx context.Context,
	controller encryptionRotateOnStartupController,
	task encryptionRotateOnStartupRunner,
	logger *slog.Logger,
	inFlight *atomic.Bool,
) error {
	defer inFlight.Store(false)
	for {
		if controller.State() != raftengine.StateLeader || task.Done() {
			return nil
		}
		err := task.Run(ctx)
		if err == nil || controller.State() != raftengine.StateLeader || task.Done() {
			return nil
		}
		logEncryptionRotateOnStartupError(logger, err)
		if !RetryableRotateOnStartupError(err) {
			return errors.Wrap(err, "encryption rotate-on-startup: permanent failure")
		}
		timer := time.NewTimer(encryptionRotateOnStartupRetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Wrap(ctx.Err(), "encryption rotate-on-startup: retry wait")
		case <-timer.C:
		}
	}
}

func waitEncryptionRotateOnStartupIdle(
	ctx context.Context,
	controller encryptionRotateOnStartupController,
	task encryptionRotateOnStartupRunner,
	logger *slog.Logger,
	flight *encryptionRotateOnStartupFlightState,
) error {
	for {
		if err := flight.terminalError(); err != nil {
			return err
		}
		waited, err := waitCurrentEncryptionRotateOnStartupFlight(ctx, flight)
		if err != nil {
			return err
		}
		if waited {
			continue
		}
		if controller.State() != raftengine.StateLeader || task.Done() {
			return nil
		}
		if flight.inFlight.Load() {
			if err := waitEncryptionRotateOnStartupFlightPublish(ctx); err != nil {
				return err
			}
			continue
		}
		done := fireEncryptionRotateOnStartup(ctx, controller, task, logger, flight, true)
		if done == nil {
			continue
		}
	}
}

func waitCurrentEncryptionRotateOnStartupFlight(ctx context.Context, flight *encryptionRotateOnStartupFlightState) (bool, error) {
	done := flight.currentDone()
	if done == nil {
		return false, nil
	}
	select {
	case <-ctx.Done():
		return true, errors.Wrap(ctx.Err(), "encryption rotate-on-startup: wait for in-flight attempt")
	case <-done:
	}
	flight.clearDone(done)
	return true, nil
}

func waitEncryptionRotateOnStartupFlightPublish(ctx context.Context) error {
	timer := time.NewTimer(encryptionRotateOnStartupFlightPublishPoll)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "encryption rotate-on-startup: wait for in-flight publication")
	case <-timer.C:
		return nil
	}
}

func logEncryptionRotateOnStartupError(logger *slog.Logger, err error) {
	if RetryableRotateOnStartupError(err) {
		logger.Warn("encryption rotate-on-startup deferred after transient error", "err", err)
		return
	}
	logger.Warn("encryption rotate-on-startup gave up after permanent error", "err", err)
}

func (t *encryptionRotateOnStartupTask) Done() bool {
	t.Mu.Lock()
	defer t.Mu.Unlock()
	return t.storageDone && t.raftDone
}

func (t *encryptionRotateOnStartupTask) Run(ctx context.Context) error {
	if err := t.fenceSidecarRead(ctx); err != nil {
		return err
	}
	sc, err := encryption.ReadSidecar(t.SidecarPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || encryption.IsNotExist(err) {
			t.markAllDone()
			return nil
		}
		return errors.Wrap(err, "encryption rotate-on-startup: read sidecar")
	}

	if err := t.prepareNextKeyID(sc); err != nil {
		return err
	}
	if err := t.rotatePurposeIfActive(
		ctx, sc.Active.Storage,
		pb.RotateDEKRequest_PURPOSE_STORAGE, t.StorageEpoch,
	); err != nil {
		return err
	}
	if err := t.rotatePurposeIfActive(
		ctx, sc.Active.Raft,
		pb.RotateDEKRequest_PURPOSE_RAFT, t.RaftEpoch,
	); err != nil {
		return err
	}
	return nil
}

func (t *encryptionRotateOnStartupTask) fenceSidecarRead(ctx context.Context) error {
	if t.readFence == nil {
		return nil
	}
	if _, err := t.readFence(ctx); err != nil {
		return errors.Wrap(err, "encryption rotate-on-startup: sidecar read fence")
	}
	return nil
}

func (t *encryptionRotateOnStartupTask) markAllDone() {
	t.Mu.Lock()
	defer t.Mu.Unlock()
	t.storageDone = true
	t.raftDone = true
}

func (t *encryptionRotateOnStartupTask) prepareNextKeyID(sc *encryption.Sidecar) error {
	t.Mu.Lock()
	defer t.Mu.Unlock()
	return t.prepareNextKeyIDLocked(sc)
}

func (t *encryptionRotateOnStartupTask) prepareNextKeyIDLocked(sc *encryption.Sidecar) error {
	next, err := NextRotateOnStartupKeyID(sc)
	if err != nil {
		return err
	}
	pending := t.pendingRotatePurposeCountLocked(sc)
	remaining := uint64(math.MaxUint32) - uint64(next) + 1
	if pending > remaining {
		return errors.New("encryption rotate-on-startup: key_id space exhausted")
	}
	t.nextKeyID = next
	return nil
}

func (t *encryptionRotateOnStartupTask) pendingRotatePurposeCountLocked(sc *encryption.Sidecar) uint64 {
	var pending uint64
	if sc.Active.Storage != encryption.ReservedKeyID && !t.storageDone {
		pending++
	}
	if sc.Active.Raft != encryption.ReservedKeyID && !t.raftDone {
		pending++
	}
	return pending
}

func (t *encryptionRotateOnStartupTask) rotatePurposeIfActive(
	ctx context.Context,
	activeID uint32,
	purpose pb.RotateDEKRequest_Purpose,
	localEpoch uint16,
) error {
	keyID, rotate, err := t.reserveRotateKey(activeID, purpose)
	if err != nil || !rotate {
		return err
	}
	if err := t.rotateWithKeyID(ctx, purpose, localEpoch, keyID); err != nil {
		return err
	}
	t.Mu.Lock()
	t.advanceNextKeyIDLocked(keyID)
	t.Mu.Unlock()
	if purpose == pb.RotateDEKRequest_PURPOSE_RAFT && t.RaftEnvelope != nil {
		if err := t.RaftEnvelope.installRotatedRaftDEK(keyID); err != nil {
			return errors.Wrap(err, "encryption rotate-on-startup: install rotated raft envelope wrapper")
		}
	}
	t.markPurposeDone(purpose)
	return nil
}

func (t *encryptionRotateOnStartupTask) reserveRotateKey(
	activeID uint32,
	purpose pb.RotateDEKRequest_Purpose,
) (uint32, bool, error) {
	t.Mu.Lock()
	defer t.Mu.Unlock()
	done := t.purposeDoneLocked(purpose)
	if activeID == encryption.ReservedKeyID {
		t.markPurposeDoneLocked(purpose)
		return 0, false, nil
	}
	if done {
		return 0, false, nil
	}
	if t.nextKeyID == encryption.ReservedKeyID {
		return 0, false, errors.New("encryption rotate-on-startup: key_id space exhausted")
	}
	return t.nextKeyID, true, nil
}

func (t *encryptionRotateOnStartupTask) purposeDoneLocked(purpose pb.RotateDEKRequest_Purpose) bool {
	if purpose == pb.RotateDEKRequest_PURPOSE_RAFT {
		return t.raftDone
	}
	return t.storageDone
}

func (t *encryptionRotateOnStartupTask) markPurposeDone(purpose pb.RotateDEKRequest_Purpose) {
	t.Mu.Lock()
	defer t.Mu.Unlock()
	t.markPurposeDoneLocked(purpose)
}

func (t *encryptionRotateOnStartupTask) markPurposeDoneLocked(purpose pb.RotateDEKRequest_Purpose) {
	if purpose == pb.RotateDEKRequest_PURPOSE_RAFT {
		t.raftDone = true
		return
	}
	t.storageDone = true
}

func (t *encryptionRotateOnStartupTask) rotateWithKeyID(ctx context.Context, purpose pb.RotateDEKRequest_Purpose, localEpoch uint16, keyID uint32) error {
	if keyID == encryption.ReservedKeyID {
		return errors.New("encryption rotate-on-startup: key_id space exhausted")
	}
	wrapped, err := WrapFreshStartupDEK(t.kekWrapper)
	if err != nil {
		return err
	}
	_, err = t.server.RotateDEK(ctx, &pb.RotateDEKRequest{
		Purpose:            purpose,
		NewDekId:           keyID,
		WrappedNewDek:      wrapped,
		ProposerNodeId:     t.FullNodeID,
		ProposerLocalEpoch: uint32(localEpoch),
	})
	if err != nil {
		return errors.Wrap(err, "encryption rotate-on-startup: RotateDEK")
	}
	return nil
}

func (t *encryptionRotateOnStartupTask) advanceNextKeyIDLocked(keyID uint32) {
	if keyID == math.MaxUint32 {
		t.nextKeyID = encryption.ReservedKeyID
	} else {
		t.nextKeyID = keyID + 1
	}
}

func NextRotateOnStartupKeyID(sc *encryption.Sidecar) (uint32, error) {
	var maxID uint32
	for raw := range sc.Keys {
		id64, err := strconv.ParseUint(raw, 10, 32)
		if err != nil {
			return 0, errors.Wrapf(err, "encryption rotate-on-startup: parse sidecar key_id %q", raw)
		}
		if uint32(id64) > maxID {
			maxID = uint32(id64)
		}
	}
	if maxID == math.MaxUint32 {
		return 0, errors.New("encryption rotate-on-startup: key_id space exhausted")
	}
	return maxID + 1, nil
}

func WrapFreshStartupDEK(wrapper kek.Wrapper) ([]byte, error) {
	if wrapper == nil {
		return nil, errors.New("encryption rotate-on-startup: KEK wrapper is nil")
	}
	dek := make([]byte, encryption.KeySize)
	if _, err := rand.Read(dek); err != nil {
		return nil, errors.Wrap(err, "encryption rotate-on-startup: generate DEK")
	}
	defer ZeroBytes(dek)
	wrapped, err := wrapper.Wrap(dek)
	if err != nil {
		return nil, errors.Wrap(err, "encryption rotate-on-startup: wrap DEK")
	}
	return wrapped, nil
}

func ZeroBytes(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

func RetryableRotateOnStartupError(err error) bool {
	switch {
	case errors.Is(err, raftengine.ErrNotLeader),
		errors.Is(err, raftengine.ErrLeadershipLost),
		errors.Is(err, raftengine.ErrLeadershipTransferInProgress):
		return true
	case errors.Is(err, context.Canceled):
		return false
	}
	code := status.Code(errors.Cause(err))
	return code == codes.FailedPrecondition ||
		code == codes.Unavailable ||
		code == codes.DeadlineExceeded
}
