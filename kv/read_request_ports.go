package kv

import (
	"context"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// Only typed read methods may cross this internal seam into raw MVCC methods.
func withReadSnapshot[T any](r *ReadRequest, ctx context.Context, snapshot ReadSnapshot, scopeCheck func() error, read func(uint64) (T, error)) (T, error) {
	var zero T
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.validateSnapshot(ctx, snapshot); err != nil {
		return zero, err
	}
	if err := scopeCheck(); err != nil {
		return zero, err
	}
	value, err := read(snapshot.Timestamp())
	if scopeErr := r.validate(ctx); scopeErr != nil {
		return zero, scopeErr
	}
	return value, errors.WithStack(err)
}

func (r *ReadRequest) Exists(ctx context.Context, snapshot ReadSnapshot, key []byte) (bool, error) {
	return withReadSnapshot(r, ctx, snapshot, func() error { return r.validateKeyScope(snapshot, key) }, func(timestamp uint64) (bool, error) {
		reader, ok := r.st.(interface {
			ExistsAt(context.Context, []byte, uint64) (bool, error)
		})
		if !ok {
			return false, errors.New("snapshot store does not support ExistsAt")
		}
		return reader.ExistsAt(ctx, key, timestamp)
	})
}

func (r *ReadRequest) ScanKeys(ctx context.Context, snapshot ReadSnapshot, start []byte, end []byte, limit int) ([][]byte, error) {
	return withReadSnapshot(r, ctx, snapshot, func() error { return r.validateRangeScope(snapshot, start, end, 0) }, func(timestamp uint64) ([][]byte, error) {
		reader, ok := r.st.(interface {
			ScanKeysAt(context.Context, []byte, []byte, int, uint64) ([][]byte, error)
		})
		if !ok {
			return nil, errors.New("snapshot store does not support ScanKeysAt")
		}
		return reader.ScanKeysAt(ctx, start, end, limit, timestamp)
	})
}

func (r *ReadRequest) ReverseScan(ctx context.Context, snapshot ReadSnapshot, start []byte, end []byte, limit int) ([]*store.KVPair, error) {
	return withReadSnapshot(r, ctx, snapshot, func() error { return r.validateRangeScope(snapshot, start, end, 0) }, func(timestamp uint64) ([]*store.KVPair, error) {
		reader, ok := r.st.(interface {
			ReverseScanAt(context.Context, []byte, []byte, int, uint64) ([]*store.KVPair, error)
		})
		if !ok {
			return nil, errors.New("snapshot store does not support ReverseScanAt")
		}
		return reader.ReverseScanAt(ctx, start, end, limit, timestamp)
	})
}

func (r *ReadRequest) GetGroup(ctx context.Context, snapshot ReadSnapshot, groupID uint64, key []byte) ([]byte, error) {
	return withReadSnapshot(r, ctx, snapshot, func() error { return r.validateGroupScope(snapshot, groupID, key) }, func(timestamp uint64) ([]byte, error) {
		reader, ok := r.st.(interface {
			GetGroupAt(context.Context, uint64, []byte, uint64) ([]byte, error)
		})
		if !ok {
			return nil, errors.New("snapshot store does not support GetGroupAt")
		}
		return reader.GetGroupAt(ctx, groupID, key, timestamp)
	})
}

func (r *ReadRequest) ScanGroup(ctx context.Context, snapshot ReadSnapshot, groupID uint64, start []byte, end []byte, limit int) ([]*store.KVPair, error) {
	return withReadSnapshot(r, ctx, snapshot, func() error { return r.validateRangeScope(snapshot, start, end, groupID) }, func(timestamp uint64) ([]*store.KVPair, error) {
		reader, ok := r.st.(interface {
			ScanGroupAt(context.Context, uint64, []byte, []byte, int, uint64) ([]*store.KVPair, error)
		})
		if !ok {
			return nil, errors.New("snapshot store does not support ScanGroupAt")
		}
		return reader.ScanGroupAt(ctx, groupID, start, end, limit, timestamp)
	})
}

func (r *ReadRequest) ReverseScanGroup(ctx context.Context, snapshot ReadSnapshot, groupID uint64, start []byte, end []byte, limit int) ([]*store.KVPair, error) {
	return withReadSnapshot(r, ctx, snapshot, func() error { return r.validateRangeScope(snapshot, start, end, groupID) }, func(timestamp uint64) ([]*store.KVPair, error) {
		reader, ok := r.st.(interface {
			ReverseScanGroupAt(context.Context, uint64, []byte, []byte, int, uint64) ([]*store.KVPair, error)
		})
		if !ok {
			return nil, errors.New("snapshot store does not support ReverseScanGroupAt")
		}
		return reader.ReverseScanGroupAt(ctx, groupID, start, end, limit, timestamp)
	})
}

func (r *ReadRequest) ScanGroupKeys(ctx context.Context, snapshot ReadSnapshot, groupID uint64, start []byte, end []byte, limit int) ([][]byte, error) {
	return withReadSnapshot(r, ctx, snapshot, func() error { return r.validateRangeScope(snapshot, start, end, groupID) }, func(timestamp uint64) ([][]byte, error) {
		reader, ok := r.st.(interface {
			ScanGroupKeysAt(context.Context, uint64, []byte, []byte, int, uint64) ([][]byte, error)
		})
		if !ok {
			return nil, errors.New("snapshot store does not support ScanGroupKeysAt")
		}
		return reader.ScanGroupKeysAt(ctx, groupID, start, end, limit, timestamp)
	})
}

func (r *ReadRequest) GetWithFence(ctx context.Context, snapshot ReadSnapshot, key []byte, groupID uint64, readRouteVersion uint64) ([]byte, error) {
	return withReadSnapshot(r, ctx, snapshot, func() error { return r.validateGroupScope(snapshot, groupID, key) }, func(timestamp uint64) ([]byte, error) {
		reader, ok := r.st.(interface {
			GetAtWithReadFence(context.Context, []byte, uint64, uint64, uint64) ([]byte, error)
		})
		if !ok {
			return nil, errors.New("snapshot store does not support GetAtWithReadFence")
		}
		return reader.GetAtWithReadFence(ctx, key, timestamp, groupID, readRouteVersion)
	})
}

func (r *ReadRequest) ScanKeysWithFence(ctx context.Context, snapshot ReadSnapshot, start []byte, end []byte, limit int, groupID uint64, readRouteVersion uint64) ([][]byte, error) {
	return withReadSnapshot(r, ctx, snapshot, func() error { return r.validateRangeScope(snapshot, start, end, groupID) }, func(timestamp uint64) ([][]byte, error) {
		reader, ok := r.st.(interface {
			ScanKeysAtWithReadFence(context.Context, []byte, []byte, int, uint64, uint64, uint64) ([][]byte, error)
		})
		if !ok {
			return nil, errors.New("snapshot store does not support ScanKeysAtWithReadFence")
		}
		return reader.ScanKeysAtWithReadFence(ctx, start, end, limit, timestamp, groupID, readRouteVersion)
	})
}

func (r *ReadRequest) ScanWithFence(ctx context.Context, snapshot ReadSnapshot, start []byte, end []byte, limit int, reverse bool, groupID uint64, readRouteVersion uint64, routeStart []byte, routeEnd []byte) ([]*store.KVPair, error) {
	return withReadSnapshot(r, ctx, snapshot, func() error { return r.validateRangeScope(snapshot, start, end, groupID) }, func(timestamp uint64) ([]*store.KVPair, error) {
		reader, ok := r.st.(interface {
			ScanAtWithReadFence(context.Context, []byte, []byte, int, uint64, bool, uint64, uint64, []byte, []byte) ([]*store.KVPair, error)
		})
		if !ok {
			return nil, errors.New("snapshot store does not support ScanAtWithReadFence")
		}
		return reader.ScanAtWithReadFence(ctx, start, end, limit, timestamp, reverse, groupID, readRouteVersion, routeStart, routeEnd)
	})
}

type ownedReadPage struct {
	rows   []*store.KVPair
	resume []byte
}

func (r *ReadRequest) ScanGroupOwned(ctx context.Context, snapshot ReadSnapshot, groupID uint64, start, end []byte, limit int) ([]*store.KVPair, []byte, error) {
	page, err := withReadSnapshot(r, ctx, snapshot, func() error { return r.validateRangeScope(snapshot, start, end, groupID) }, func(timestamp uint64) (ownedReadPage, error) {
		reader, ok := r.st.(interface {
			ScanGroupOwnedAt(context.Context, uint64, []byte, []byte, int, uint64) ([]*store.KVPair, []byte, error)
		})
		if !ok {
			return ownedReadPage{}, errors.New("snapshot store does not support owned-group scans")
		}
		rows, resume, readErr := reader.ScanGroupOwnedAt(ctx, groupID, start, end, limit, timestamp)
		return ownedReadPage{rows: rows, resume: resume}, errors.WithStack(readErr)
	})
	return page.rows, page.resume, err
}

type physicalReadPage struct {
	rows    []*store.KVPair
	limited bool
}

// ScanPhysical applies the same capability checks to a scan with a physical
// visit budget. Direction and both budgets are preserved at the store seam.
func (r *ReadRequest) ScanPhysical(ctx context.Context, snapshot ReadSnapshot, start, end []byte, visibleLimit, physicalLimit int, reverse bool) ([]*store.KVPair, bool, error) {
	page, err := withReadSnapshot(r, ctx, snapshot, func() error { return r.validateRangeScope(snapshot, start, end, 0) }, func(ts uint64) (physicalReadPage, error) {
		reader, ok := r.st.(interface {
			ScanAtPhysicalLimit(context.Context, []byte, []byte, int, int, uint64) ([]*store.KVPair, bool, error)
			ReverseScanAtPhysicalLimit(context.Context, []byte, []byte, int, int, uint64) ([]*store.KVPair, bool, error)
		})
		if !ok {
			return physicalReadPage{}, errors.New("snapshot store does not support physical-limit scans")
		}
		var rows []*store.KVPair
		var limited bool
		var readErr error
		if reverse {
			rows, limited, readErr = reader.ReverseScanAtPhysicalLimit(ctx, start, end, visibleLimit, physicalLimit, ts)
		} else {
			rows, limited, readErr = reader.ScanAtPhysicalLimit(ctx, start, end, visibleLimit, physicalLimit, ts)
		}
		return physicalReadPage{rows: rows, limited: limited}, errors.WithStack(readErr)
	})
	return page.rows, page.limited, err
}
