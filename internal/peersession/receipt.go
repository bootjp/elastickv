package peersession

import (
	"context"
	"sync"

	"google.golang.org/grpc"
)

// Receipt seals the decoded routing/traffic classification of ONE successfully
// received stream message. It is not a replicated apply receipt or a grant.
// Capture immediately after RecvMsg, before the next receive; Check revalidates
// at install/enqueue/result without re-consuming a stateful metadata decoder.
type Receipt struct {
	handle  *Handle
	message Message
	method  string
}

func (r *Receipt) Check(ctx context.Context) error {
	if r == nil || r.handle == nil {
		return ErrDenied
	}
	return r.handle.validate(ctx, r.message, false)
}
func (r *Receipt) Message() Message {
	if r == nil {
		return Message{}
	}
	return r.message
}

type receiptKey struct{}
type receiptSlot struct {
	mu    sync.Mutex
	value *Receipt
}

func (s *receiptSlot) set(r *Receipt) { s.mu.Lock(); defer s.mu.Unlock(); s.value = r }
func (s *receiptSlot) get() *Receipt  { s.mu.Lock(); defer s.mu.Unlock(); return s.value }

// Received requires BOTH the native authenticated server context and this
// interceptor's private per-stream slot; no caller-provided receipt is accepted.
func (o *Owner) Received(ctx context.Context) (*Receipt, error) {
	method, ok := grpc.Method(ctx)
	if !ok {
		return nil, ErrDenied
	}
	h, err := o.incoming(ctx, method)
	if err != nil {
		return nil, err
	}
	slot, ok := ctx.Value(receiptKey{}).(*receiptSlot)
	if !ok || slot == nil {
		return nil, ErrDenied
	}
	r := slot.get()
	if r == nil || r.method != method || r.handle.c != h.c || r.handle.s != h.s {
		return nil, ErrDenied
	}
	if err := r.Check(ctx); err != nil {
		return nil, err
	}
	return r, nil
}
