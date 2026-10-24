package kv

import (
	"context"
	"io"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

var ErrExternalSnapshotRestoreUnsupported = errors.New("store lacks external snapshot validation")

var _ raftengine.ContextSnapshotRestorer = (*kvFSM)(nil)

func (f *kvFSM) RestoreWithContext(ctx context.Context, r io.Reader, opts raftengine.SnapshotRestoreOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	switch opts.Origin {
	case raftengine.SnapshotRestoreRaft:
		return f.Restore(r)
	case raftengine.SnapshotRestoreExternalRebase:
		st, ok := f.store.(store.ExternalSnapshotRestorer)
		if !ok {
			return errors.WithStack(ErrExternalSnapshotRestoreUnsupported)
		}
		return f.restoreSnapshot(r, func(body io.Reader) error {
			if err := ctx.Err(); err != nil {
				return errors.WithStack(err)
			}
			return st.RestoreExternal(ctx, body)
		})
	default:
		return errors.New("unknown snapshot restore origin")
	}
}
