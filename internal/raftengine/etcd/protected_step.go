package etcd

import (
	"context"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/cockroachdb/errors"
	"go.etcd.io/raft/v3/raftpb"
)

type protectedStep struct {
	message raftpb.Message
	receipt *peersession.Receipt
}

func (e *Engine) enqueueProtectedStep(ctx context.Context, msg raftpb.Message, receipt *peersession.Receipt) error {
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	if err := receipt.Check(ctx); err != nil {
		return errors.WithStack(err)
	}
	queue := e.protectedStepCh
	if isInboundPriorityMsg(msg.GetType()) {
		queue = e.protectedPriorityStepCh
	}
	request := protectedStep{message: msg, receipt: receipt}
	select {
	case queue <- request:
		return nil
	default:
		e.stepQueueFullCount.Add(1)
	}
	if !isBlockingInboundStepMsg(msg.GetType()) {
		return errors.WithStack(errStepQueueFull)
	}
	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-e.doneCh:
		return e.currentErrorOrClosed()
	case queue <- request:
		return nil
	}
}

func (e *Engine) handleProtectedStep(request protectedStep) {
	// The apply loop owns Step and invalidation ordering. The RPC may already
	// have completed; retain session generation, not the RPC's short lifetime.
	e.handleStepWithAdmission(request.message, func(action func() error) error {
		return request.receipt.WithAdmission(context.Background(), action)
	})
}
