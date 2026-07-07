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

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type encryptionRotateOnStartupController interface {
	raftengine.LeaderView
	RegisterLeaderAcquiredCallback(fn func()) (deregister func())
}

type encryptionRotateOnStartupRunner interface {
	Run(context.Context) error
	Done() bool
}

type encryptionRotateOnStartupTask struct {
	server       *adapter.EncryptionAdminServer
	sidecarPath  string
	kekWrapper   kek.Wrapper
	raftEnvelope *raftEnvelopeRuntime
	fullNodeID   uint64
	storageEpoch uint16
	raftEpoch    uint16

	mu          sync.Mutex
	nextReady   bool
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
) func() {
	if !requested {
		return func() {}
	}
	if logger == nil {
		logger = slog.Default()
	}
	if rt == nil || rt.engine == nil {
		logger.Warn("encryption rotate-on-startup skipped: default raft group is not available")
		return func() {}
	}
	controller, ok := rt.engine.(encryptionRotateOnStartupController)
	if !ok {
		logger.Warn("encryption rotate-on-startup skipped: engine does not implement leader-acquired observer")
		return func() {}
	}
	if sidecarPath == "" || kekWrapper == nil {
		logger.Warn("encryption rotate-on-startup skipped: encryption sidecar or KEK is not configured")
		return func() {}
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
		return func() {}
	}
	task := &encryptionRotateOnStartupTask{
		server:       server,
		sidecarPath:  sidecarPath,
		kekWrapper:   kekWrapper,
		raftEnvelope: raftEnvelope,
		fullNodeID:   fullNodeID,
		storageEpoch: storageEpoch,
		raftEpoch:    raftEpoch,
	}
	return installEncryptionRotateOnStartupRequest(ctx, controller, task, logger)
}

func installEncryptionRotateOnStartupRequest(
	ctx context.Context,
	controller encryptionRotateOnStartupController,
	task encryptionRotateOnStartupRunner,
	logger *slog.Logger,
) func() {
	if controller == nil || task == nil {
		return func() {}
	}
	if logger == nil {
		logger = slog.Default()
	}
	var inFlight atomic.Bool
	fire := func(block bool) {
		fireEncryptionRotateOnStartup(ctx, controller, task, logger, &inFlight, block)
	}
	wasLeaderBefore := controller.State() == raftengine.StateLeader
	if wasLeaderBefore {
		fire(true)
	}
	deregister := controller.RegisterLeaderAcquiredCallback(func() {
		fire(false)
	})
	if controller.State() == raftengine.StateLeader && !task.Done() {
		fire(false)
	}
	return deregister
}

func fireEncryptionRotateOnStartup(
	ctx context.Context,
	controller encryptionRotateOnStartupController,
	task encryptionRotateOnStartupRunner,
	logger *slog.Logger,
	inFlight *atomic.Bool,
	block bool,
) {
	if task.Done() || controller.State() != raftengine.StateLeader {
		return
	}
	run := func() {
		if !inFlight.CompareAndSwap(false, true) {
			return
		}
		defer inFlight.Store(false)
		if task.Done() || controller.State() != raftengine.StateLeader {
			return
		}
		if err := task.Run(ctx); err != nil {
			logEncryptionRotateOnStartupError(logger, err)
		}
	}
	if block {
		run()
		return
	}
	go run()
}

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
		t.storageDone = true
		t.raftDone = true
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

func (t *encryptionRotateOnStartupTask) markAllDone() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.storageDone = true
	t.raftDone = true
}

func (t *encryptionRotateOnStartupTask) prepareNextKeyIDLocked(sc *encryption.Sidecar) error {
	if t.nextReady {
		return nil
	}
	next, err := nextEncryptionRotateOnStartupKeyID(sc)
	if err != nil {
		return err
	}
	t.nextKeyID = next
	t.nextReady = true
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
		if retryableEncryptionRotateOnStartupError(err) {
			t.advanceNextKeyIDLocked(keyID)
		}
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
