package kv

import (
	"context"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

var ErrTimestampBindingStoreUnsupported = errors.New("data-group store cannot atomically apply timestamp bindings")

// ApplyTimestampBoundCreation commits a binding and its write in one indexed
// data-group Raft entry. An exact retry does not invoke apply again.
func ApplyTimestampBoundCreation(ctx context.Context, st store.MVCCStore, index uint64, binding TimestampBinding, apply func(context.Context) error) (alreadyApplied bool, err error) {
	if st == nil || index == 0 || apply == nil || !binding.valid() {
		return false, errors.WithStack(ErrTimestampBindingInvalid)
	}
	metadata, ok := st.(store.RaftControlMetadataStore)
	if !ok {
		return false, errors.WithStack(ErrTimestampBindingStoreUnsupported)
	}
	applier, ok := st.(store.RaftEntryApplier)
	if !ok {
		return false, errors.WithStack(ErrTimestampBindingStoreUnsupported)
	}
	var already bool
	err = applier.ApplyRaftEntry(ctx, index, func(entryCtx context.Context) error {
		var stageErr error
		already, stageErr = StageTimestampBinding(entryCtx, metadata, binding)
		if stageErr != nil || already {
			return stageErr
		}
		return apply(entryCtx)
	})
	if err != nil {
		return false, errors.Wrap(err, "apply timestamp-bound creation")
	}
	return already, nil
}
