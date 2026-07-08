package main

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

	mu          sync.Mutex
	done        <-chan struct{}
	terminalErr error
}

type encryptionRotateOnStartupTask struct {
	server       *adapter.EncryptionAdminServer
	sidecarPath  string
	kekWrapper   kek.Wrapper
	raftEnvelope *raftEnvelopeRuntime
	readFence    func(context.Context) (uint64, error)
	fullNodeID   uint64
	storageEpoch uint16
	raftEpoch    uint16

	mu          sync.Mutex
	nextKeyID   uint32
	storageDone bool
	raftDone    bool
}

func installEncryptionRotateOnStartup(
	ctx context.Context,
	requested bool,
	rt *raftGroupRuntime,
	postCutoverProposer raftengine.Proposer,
	sidecarPath string,
	kekWrapper kek.Wrapper,
	raftEnvelope *raftEnvelopeRuntime,
	fullNodeID uint64,
	storageEpoch uint16,
	raftEpoch uint16,
	logger *slog.Logger,
) (func(), func(context.Context) error) {
	if !requested {
		return func() {}, encryptionRotateOnStartupNoopWait
	}
	if logger == nil {
		logger = slog.Default()
	}
	if rt == nil || rt.engine == nil {
		logger.Warn("encryption rotate-on-startup skipped: default raft group is not available")
		return func() {}, encryptionRotateOnStartupNoopWait
	}
	controller, ok := rt.engine.(encryptionRotateOnStartupController)
	if !ok {
		logger.Warn("encryption rotate-on-startup skipped: engine does not implement leader-acquired observer")
		return func() {}, encryptionRotateOnStartupNoopWait
	}
	if sidecarPath == "" || kekWrapper == nil {
		logger.Warn("encryption rotate-on-startup skipped: encryption sidecar or KEK is not configured")
		return func() {}, encryptionRotateOnStartupNoopWait
	}
	server := adapter.NewEncryptionAdminServer(
		adapter.WithEncryptionAdminSidecarPath(sidecarPath),
		adapter.WithEncryptionAdminFullNodeID(fullNodeID),
		adapter.WithEncryptionAdminProposer(rt.engine),
		adapter.WithEncryptionAdminLeaderView(rt.engine),
		adapter.WithEncryptionAdminPostCutoverProposer(postCutoverProposer),
	)
	if err := server.Validate(); err != nil {
		logger.Warn("encryption rotate-on-startup skipped: admin server wiring invalid", "err", err)
		return func() {}, encryptionRotateOnStartupNoopWait
	}
	task := &encryptionRotateOnStartupTask{
		server:       server,
		sidecarPath:  sidecarPath,
		kekWrapper:   kekWrapper,
		raftEnvelope: raftEnvelope,
		readFence:    controller.LinearizableRead,
		fullNodeID:   fullNodeID,
		storageEpoch: storageEpoch,
		raftEpoch:    raftEpoch,
	}
	return installEncryptionRotateOnStartupRequestWithWait(ctx, controller, task, logger)
}

func installEncryptionRotateOnStartupRequestWithWait(
	ctx context.Context,
	controller encryptionRotateOnStartupController,
	task encryptionRotateOnStartupRunner,
	logger *slog.Logger,
) (func(), func(context.Context) error) {
	if controller == nil || task == nil {
		return func() {}, encryptionRotateOnStartupNoopWait
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
	return deregister, waitStartup
}

func (s *encryptionRotateOnStartupFlightState) publishDone(done <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.done = done
}

func (s *encryptionRotateOnStartupFlightState) currentDone() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done
}

func (s *encryptionRotateOnStartupFlightState) clearDone(done <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done == done {
		s.done = nil
	}
}

func (s *encryptionRotateOnStartupFlightState) setTerminalError(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminalErr == nil {
		s.terminalErr = err
	}
}

func (s *encryptionRotateOnStartupFlightState) terminalError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
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
		if !retryableEncryptionRotateOnStartupError(err) {
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
			return nil
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

func encryptionRotateOnStartupNoopWait(context.Context) error { return nil }

func logEncryptionRotateOnStartupError(logger *slog.Logger, err error) {
	if retryableEncryptionRotateOnStartupError(err) {
		logger.Warn("encryption rotate-on-startup deferred after transient error", "err", err)
		return
	}
	logger.Warn("encryption rotate-on-startup gave up after permanent error", "err", err)
}

func (t *encryptionRotateOnStartupTask) Done() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.storageDone && t.raftDone
}

func (t *encryptionRotateOnStartupTask) Run(ctx context.Context) error {
	if err := t.fenceSidecarRead(ctx); err != nil {
		return err
	}
	sc, err := encryption.ReadSidecar(t.sidecarPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || encryption.IsNotExist(err) {
			t.markAllDone()
			return nil
		}
		return errors.Wrap(err, "encryption rotate-on-startup: read sidecar")
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.prepareNextKeyIDLocked(sc); err != nil {
		return err
	}
	if err := t.rotatePurposeIfActiveLocked(
		ctx, &t.storageDone, sc.Active.Storage,
		pb.RotateDEKRequest_PURPOSE_STORAGE, t.storageEpoch,
	); err != nil {
		return err
	}
	if err := t.rotatePurposeIfActiveLocked(
		ctx, &t.raftDone, sc.Active.Raft,
		pb.RotateDEKRequest_PURPOSE_RAFT, t.raftEpoch,
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
	t.mu.Lock()
	defer t.mu.Unlock()
	t.storageDone = true
	t.raftDone = true
}

func (t *encryptionRotateOnStartupTask) prepareNextKeyIDLocked(sc *encryption.Sidecar) error {
	next, err := nextEncryptionRotateOnStartupKeyID(sc)
	if err != nil {
		return err
	}
	t.nextKeyID = next
	return nil
}

func (t *encryptionRotateOnStartupTask) rotatePurposeIfActiveLocked(
	ctx context.Context,
	done *bool,
	activeID uint32,
	purpose pb.RotateDEKRequest_Purpose,
	localEpoch uint16,
) error {
	if activeID == encryption.ReservedKeyID {
		*done = true
		return nil
	}
	if *done {
		return nil
	}
	if err := t.rotateLocked(ctx, purpose, localEpoch); err != nil {
		return err
	}
	*done = true
	return nil
}

func (t *encryptionRotateOnStartupTask) rotateLocked(ctx context.Context, purpose pb.RotateDEKRequest_Purpose, localEpoch uint16) error {
	if t.nextKeyID == encryption.ReservedKeyID {
		return errors.New("encryption rotate-on-startup: key_id space exhausted")
	}
	keyID := t.nextKeyID
	wrapped, err := wrapFreshStartupDEK(t.kekWrapper)
	if err != nil {
		return err
	}
	_, err = t.server.RotateDEK(ctx, &pb.RotateDEKRequest{
		Purpose:            purpose,
		NewDekId:           keyID,
		WrappedNewDek:      wrapped,
		ProposerNodeId:     t.fullNodeID,
		ProposerLocalEpoch: uint32(localEpoch),
	})
	if err != nil {
		return errors.Wrap(err, "encryption rotate-on-startup: RotateDEK")
	}
	t.advanceNextKeyIDLocked(keyID)
	if purpose == pb.RotateDEKRequest_PURPOSE_RAFT && t.raftEnvelope != nil {
		if err := t.raftEnvelope.installRotatedRaftDEK(keyID); err != nil {
			return errors.Wrap(err, "encryption rotate-on-startup: install rotated raft envelope wrapper")
		}
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

func nextEncryptionRotateOnStartupKeyID(sc *encryption.Sidecar) (uint32, error) {
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

func wrapFreshStartupDEK(wrapper kek.Wrapper) ([]byte, error) {
	if wrapper == nil {
		return nil, errors.New("encryption rotate-on-startup: KEK wrapper is nil")
	}
	dek := make([]byte, encryption.KeySize)
	if _, err := rand.Read(dek); err != nil {
		return nil, errors.Wrap(err, "encryption rotate-on-startup: generate DEK")
	}
	defer zeroBytes(dek)
	wrapped, err := wrapper.Wrap(dek)
	if err != nil {
		return nil, errors.Wrap(err, "encryption rotate-on-startup: wrap DEK")
	}
	return wrapped, nil
}

func zeroBytes(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

func retryableEncryptionRotateOnStartupError(err error) bool {
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
