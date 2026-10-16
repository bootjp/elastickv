package kv

import (
	"context"

	"github.com/bootjp/elastickv/keyviz"
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
// key-routed lease reads with the supplied adapter label. Every other call and
// capability is forwarded unchanged by the embedded CoordinatorForwarder.
func WithKeyVizLabel(c Coordinator, label keyviz.Label) Coordinator {
	if c == nil || label == keyviz.LabelLegacy {
		return c
	}
	return keyVizLabeledCoordinator{CoordinatorForwarder: CoordinatorForwarder{Inner: c}, label: label}
}

type keyVizLabeledCoordinator struct {
	CoordinatorForwarder

	label keyviz.Label
}

func (c keyVizLabeledCoordinator) Dispatch(ctx context.Context, reqs *OperationGroup[OP]) (*CoordinateResponse, error) {
	if reqs != nil && reqs.KeyVizLabel == keyviz.LabelLegacy {
		copied := *reqs
		copied.KeyVizLabel = c.label
		reqs = &copied
	}
	return c.CoordinatorForwarder.Dispatch(ctx, reqs)
}

func (c keyVizLabeledCoordinator) LinearizableReadForKey(ctx context.Context, key []byte) (uint64, error) {
	return c.CoordinatorForwarder.LinearizableReadForKey(contextWithKeyVizLabel(ctx, c.label), key)
}

func (c keyVizLabeledCoordinator) Next(ctx context.Context) (uint64, error) {
	return c.CoordinatorForwarder.Next(contextWithKeyVizLabel(ctx, c.label))
}

func (c keyVizLabeledCoordinator) NextAfter(ctx context.Context, min uint64) (uint64, error) {
	return c.CoordinatorForwarder.NextAfter(contextWithKeyVizLabel(ctx, c.label), min)
}

func (c keyVizLabeledCoordinator) RecoverHLCLease(ctx context.Context) error {
	return c.CoordinatorForwarder.RecoverHLCLease(contextWithKeyVizLabel(ctx, c.label))
}

func (c keyVizLabeledCoordinator) LeaseReadForKey(ctx context.Context, key []byte) (uint64, error) {
	return c.CoordinatorForwarder.LeaseReadForKey(contextWithKeyVizLabel(ctx, c.label), key)
}

// LeaseReadForGroup is part of the group-keyed read-fence path; the Redis
// adapter's fence type-asserts this wrapper, the outermost one it holds.
func (c keyVizLabeledCoordinator) LeaseReadForGroup(ctx context.Context, groupID uint64) (uint64, error) {
	return c.CoordinatorForwarder.LeaseReadForGroup(contextWithKeyVizLabel(ctx, c.label), groupID)
}
