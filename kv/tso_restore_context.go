package kv

import (
	"bufio"
	"context"
	"io"
	"os"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

var _ raftengine.ContextSnapshotRestorer = (*TSOStateMachine)(nil)

var ErrTSOExternalLegacySnapshotUnsupported = errors.New("tso fsm: external legacy KV snapshot requires decoded store validation")

type tsoExternalStoreRestorer interface {
	RestoreExternal(context.Context, io.Reader) error
}

func (f *TSOStateMachine) RestoreWithContext(ctx context.Context, r io.Reader, opts raftengine.SnapshotRestoreOptions) error {
	if err := validateTSORestoreContext(ctx, r, opts.Origin); err != nil {
		return err
	}
	return f.restoreWithValidatedTSOContext(ctx, r, opts.Origin)
}

func validateTSORestoreContext(ctx context.Context, r io.Reader, origin raftengine.SnapshotRestoreOrigin) error {
	if ctx == nil || r == nil {
		return errors.Wrap(ErrTSOStateMachineInvalidEntry, "restore context and reader are required")
	}
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	if origin != raftengine.SnapshotRestoreRaft && origin != raftengine.SnapshotRestoreExternalRebase {
		return errors.Wrap(ErrTSOStateMachineInvalidEntry, "unknown snapshot restore origin")
	}
	return nil
}

func (f *TSOStateMachine) restoreWithValidatedTSOContext(ctx context.Context, r io.Reader, origin raftengine.SnapshotRestoreOrigin) error {
	br := tsoSnapshotReader(tsoContextRestoreReader{ctx: ctx, r: r})
	legacy, err := hasLegacyKVFSMSnapshotHeader(br)
	if err != nil {
		return err
	}
	if legacy {
		return f.restoreLegacyTSOWithContext(ctx, br, origin)
	}
	ceiling, allocation, cutover, active, floor, pending, err := readTSOSnapshotState(br)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	if f != nil {
		if err := f.restorePendingPhaseDFloor(pending, allocation, active, floor); err != nil {
			return err
		}
		f.restoreSnapshotState(ceiling, allocation, cutover, active, floor)
	}
	return nil
}

func (f *TSOStateMachine) restoreLegacyTSOWithContext(ctx context.Context, br *bufio.Reader, origin raftengine.SnapshotRestoreOrigin) error {
	staged := NewTSOStateMachine(nil)
	if origin == raftengine.SnapshotRestoreExternalRebase {
		ceiling, err := validateExternalLegacyTSO(ctx, br)
		if err != nil {
			return err
		}
		staged.restoreSnapshotState(ceiling, tsoLeaseAllocationFloor(ceiling), false, false, 0)
	} else if err := staged.Restore(br); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	if f != nil && staged.ceilingMs.Load() > 0 {
		f.restoreSnapshotState(staged.ceilingMs.Load(), staged.AllocationFloor(), false, false, 0)
	}
	return nil
}

// Pebble's existing restore dispatcher handles native, streaming MVCC and SST
// formats. No external KV payload may reach the legacy io.Discard bridge.
func validateExternalLegacyTSO(ctx context.Context, br *bufio.Reader) (int64, error) {
	ceiling, _, err := ReadSnapshotHeader(br)
	if err != nil {
		return 0, errors.Wrap(err, "read external legacy TSO header")
	}
	ceilingMs, err := decodeTSOCeiling(ceiling, "external legacy KV snapshot")
	if err != nil {
		return 0, err
	}
	dir, err := os.MkdirTemp("", "elastickv-tso-restore-*")
	if err != nil {
		return 0, errors.WithStack(err)
	}
	defer os.RemoveAll(dir)
	st, err := store.NewPebbleStore(dir)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	err = validateTSOLegacyStore(ctx, br, st)
	if err := errors.Join(err, st.Close()); err != nil {
		return 0, errors.WithStack(err)
	}
	return ceilingMs, nil
}

func validateTSOLegacyStore(ctx context.Context, r io.Reader, st store.MVCCStore) error {
	gate, ok := st.(tsoExternalStoreRestorer)
	if !ok {
		return errors.WithStack(ErrTSOExternalLegacySnapshotUnsupported)
	}
	return errors.WithStack(gate.RestoreExternal(ctx, r))
}

type tsoContextRestoreReader struct {
	ctx context.Context
	r   io.Reader
}

func (r tsoContextRestoreReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, errors.WithStack(err)
	}
	n, err := r.r.Read(p)
	if canceled := r.ctx.Err(); canceled != nil {
		return n, errors.WithStack(canceled)
	}
	if errors.Is(err, io.EOF) {
		return n, io.EOF
	}
	return n, errors.WithStack(err)
}
