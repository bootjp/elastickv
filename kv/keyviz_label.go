package kv

import (
	"context"

	"github.com/bootjp/elastickv/keyviz"
	"github.com/cockroachdb/errors"
)

type keyVizLabelContextKey struct{}

func contextWithKeyVizLabel(ctx context.Context, label keyviz.Label) context.Context {
	if label == keyviz.LabelLegacy {
		return ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, keyVizLabelContextKey{}, label)
}

func keyVizLabelFromContext(ctx context.Context) keyviz.Label {
	if ctx == nil {
		return keyviz.LabelLegacy
	}
	label, _ := ctx.Value(keyVizLabelContextKey{}).(keyviz.Label)
	return label
}

// WithKeyVizLabel returns a Coordinator wrapper that stamps all dispatches and
// key-routed lease reads with the supplied adapter label.
func WithKeyVizLabel(c Coordinator, label keyviz.Label) Coordinator {
	if c == nil || label == keyviz.LabelLegacy {
		return c
	}
	return keyVizLabeledCoordinator{inner: c, label: label}
}

type keyVizLabeledCoordinator struct {
	inner Coordinator
	label keyviz.Label
}

func (c keyVizLabeledCoordinator) Dispatch(ctx context.Context, reqs *OperationGroup[OP]) (*CoordinateResponse, error) {
	if reqs != nil && reqs.KeyVizLabel == keyviz.LabelLegacy {
		copied := *reqs
		copied.KeyVizLabel = c.label
		reqs = &copied
	}
	resp, err := c.inner.Dispatch(ctx, reqs)
	return resp, errors.WithStack(err)
}

func (c keyVizLabeledCoordinator) IsLeader() bool { return c.inner.IsLeader() }

func (c keyVizLabeledCoordinator) VerifyLeader(ctx context.Context) error {
	return errors.WithStack(c.inner.VerifyLeader(ctx))
}

func (c keyVizLabeledCoordinator) LinearizableRead(ctx context.Context) (uint64, error) {
	idx, err := c.inner.LinearizableRead(ctx)
	return idx, errors.WithStack(err)
}

func (c keyVizLabeledCoordinator) LinearizableReadForKey(ctx context.Context, key []byte) (uint64, error) {
	ctx = contextWithKeyVizLabel(ctx, c.label)
	if lr, ok := c.inner.(interface {
		LinearizableReadForKey(context.Context, []byte) (uint64, error)
	}); ok {
		idx, err := lr.LinearizableReadForKey(ctx, key)
		return idx, errors.WithStack(err)
	}
	idx, err := c.inner.LinearizableRead(ctx)
	return idx, errors.WithStack(err)
}

func (c keyVizLabeledCoordinator) RaftLeader() string { return c.inner.RaftLeader() }

func (c keyVizLabeledCoordinator) IsLeaderForKey(key []byte) bool {
	return c.inner.IsLeaderForKey(key)
}

func (c keyVizLabeledCoordinator) VerifyLeaderForKey(ctx context.Context, key []byte) error {
	return errors.WithStack(c.inner.VerifyLeaderForKey(ctx, key))
}

func (c keyVizLabeledCoordinator) RaftLeaderForKey(key []byte) string {
	return c.inner.RaftLeaderForKey(key)
}

func (c keyVizLabeledCoordinator) Clock() *HLC { return c.inner.Clock() }

func (c keyVizLabeledCoordinator) LeaseRead(ctx context.Context) (uint64, error) {
	if lr, ok := c.inner.(LeaseReadableCoordinator); ok {
		idx, err := lr.LeaseRead(ctx)
		return idx, errors.WithStack(err)
	}
	idx, err := c.inner.LinearizableRead(ctx)
	return idx, errors.WithStack(err)
}

func (c keyVizLabeledCoordinator) LeaseReadForKey(ctx context.Context, key []byte) (uint64, error) {
	ctx = contextWithKeyVizLabel(ctx, c.label)
	if lr, ok := c.inner.(LeaseReadableCoordinator); ok {
		idx, err := lr.LeaseReadForKey(ctx, key)
		return idx, errors.WithStack(err)
	}
	idx, err := c.inner.LinearizableRead(ctx)
	return idx, errors.WithStack(err)
}

func (c keyVizLabeledCoordinator) LeaseReadAllGroups(ctx context.Context) error {
	if ag, ok := c.inner.(AllGroupsLeaseReadableCoordinator); ok {
		return errors.WithStack(ag.LeaseReadAllGroups(ctx))
	}
	_, err := LeaseReadThrough(c.inner, ctx)
	return errors.WithStack(err)
}

func (c keyVizLabeledCoordinator) LeaseReadAllGroupsTimestamp(ctx context.Context) (uint64, error) {
	return LeaseReadAllGroupsTimestampThrough(c.inner, ctx)
}

func (c keyVizLabeledCoordinator) EngineGroupIDForKey(key []byte) uint64 {
	if gr, ok := c.inner.(GroupRoutableCoordinator); ok {
		return gr.EngineGroupIDForKey(key)
	}
	return 0
}
