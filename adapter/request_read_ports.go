package adapter

import (
	"context"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

func requestExistsAt(ctx context.Context, backend any, key []byte, ts uint64) (bool, error) {
	request, snapshot, closeRead, err := requestReadSnapshot(ctx, backend, ts)
	if err != nil {
		return false, err
	}
	defer closeRead()
	return wrappedReadResult(request.Exists(requestReadContext(ctx, request), snapshot, key))
}

func requestScanKeysAt(ctx context.Context, backend any, start []byte, end []byte, limit int, ts uint64) ([][]byte, error) {
	request, snapshot, closeRead, err := requestReadSnapshot(ctx, backend, ts)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer closeRead()
	return wrappedReadResult(request.ScanKeys(requestReadContext(ctx, request), snapshot, start, end, limit))
}

func requestReverseScanAt(ctx context.Context, backend any, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	request, snapshot, closeRead, err := requestReadSnapshot(ctx, backend, ts)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer closeRead()
	return wrappedReadResult(request.ReverseScan(requestReadContext(ctx, request), snapshot, start, end, limit))
}

func requestGetGroupAt(ctx context.Context, backend any, groupID uint64, key []byte, ts uint64) ([]byte, error) {
	request, snapshot, closeRead, err := requestReadSnapshot(ctx, backend, ts)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer closeRead()
	return wrappedReadResult(request.GetGroup(requestReadContext(ctx, request), snapshot, groupID, key))
}

func requestScanGroupAt(ctx context.Context, backend any, groupID uint64, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	request, snapshot, closeRead, err := requestReadSnapshot(ctx, backend, ts)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer closeRead()
	return wrappedReadResult(request.ScanGroup(requestReadContext(ctx, request), snapshot, groupID, start, end, limit))
}

func requestReverseScanGroupAt(ctx context.Context, backend any, groupID uint64, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	request, snapshot, closeRead, err := requestReadSnapshot(ctx, backend, ts)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer closeRead()
	return wrappedReadResult(request.ReverseScanGroup(requestReadContext(ctx, request), snapshot, groupID, start, end, limit))
}

func requestScanGroupKeysAt(ctx context.Context, backend any, groupID uint64, start []byte, end []byte, limit int, ts uint64) ([][]byte, error) {
	request, snapshot, closeRead, err := requestReadSnapshot(ctx, backend, ts)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer closeRead()
	return wrappedReadResult(request.ScanGroupKeys(requestReadContext(ctx, request), snapshot, groupID, start, end, limit))
}

func requestGetAtWithReadFence(ctx context.Context, backend any, key []byte, ts uint64, groupID uint64, readRouteVersion uint64) ([]byte, error) {
	request, snapshot, closeRead, err := requestReadSnapshot(ctx, backend, ts)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer closeRead()
	return wrappedReadResult(request.GetWithFence(requestReadContext(ctx, request), snapshot, key, groupID, readRouteVersion))
}

func requestScanKeysAtWithReadFence(ctx context.Context, backend any, start []byte, end []byte, limit int, ts uint64, groupID uint64, readRouteVersion uint64) ([][]byte, error) {
	request, snapshot, closeRead, err := requestReadSnapshot(ctx, backend, ts)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer closeRead()
	return wrappedReadResult(request.ScanKeysWithFence(requestReadContext(ctx, request), snapshot, start, end, limit, groupID, readRouteVersion))
}

func requestScanAtWithReadFence(ctx context.Context, backend any, start []byte, end []byte, limit int, ts uint64, reverse bool, groupID uint64, readRouteVersion uint64, routeStart []byte, routeEnd []byte) ([]*store.KVPair, error) {
	request, snapshot, closeRead, err := requestReadSnapshot(ctx, backend, ts)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer closeRead()
	return wrappedReadResult(request.ScanWithFence(requestReadContext(ctx, request), snapshot, start, end, limit, reverse, groupID, readRouteVersion, routeStart, routeEnd))
}

func requestScanGroupOwnedAt(ctx context.Context, backend any, groupID uint64, start, end []byte, limit int, ts uint64) ([]*store.KVPair, []byte, error) {
	request, snapshot, closeRead, err := requestReadSnapshot(ctx, backend, ts)
	if err != nil {
		return nil, nil, err
	}
	defer closeRead()
	rows, resume, err := request.ScanGroupOwned(requestReadContext(ctx, request), snapshot, groupID, start, end, limit)
	return rows, resume, errors.WithStack(err)
}

func requestScanPhysicalAt(ctx context.Context, backend any, start, end []byte, visibleLimit, physicalLimit int, ts uint64, reverse bool) ([]*store.KVPair, bool, error) {
	request, snapshot, closeRead, err := requestReadSnapshot(ctx, backend, ts)
	if err != nil {
		return nil, false, err
	}
	defer closeRead()
	rows, limited, err := request.ScanPhysical(requestReadContext(ctx, request), snapshot, start, end, visibleLimit, physicalLimit, reverse)
	return rows, limited, errors.WithStack(err)
}
