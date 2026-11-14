package adapter

import (
	"context"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// Legacy At helpers carry MVCC numbers for transaction planning and explicit
// historical protocols. Inside a client request a number only resolves an
// already-issued capability; it never grants permission to mint one.
func requestReadSnapshot(ctx context.Context, backend any, ts uint64) (*kv.ReadRequest, kv.ReadSnapshot, func(), error) {
	st, ok := backend.(store.MVCCStore)
	if !ok {
		return nil, kv.ReadSnapshot{}, func() {}, kv.ErrReadSnapshotScope
	}
	if request, ok := kv.ReadRequestFromContext(ctx); ok {
		if !request.MatchesStore(st) {
			return nil, kv.ReadSnapshot{}, func() {}, kv.ErrReadSnapshotScope
		}
		snapshot, err := request.Issued(ctx, ts)
		return request, snapshot, func() {}, errors.WithStack(err)
	}
	// Unscoped calls are the existing internal historical At contract. Public
	// protocol ingress always installs a fresh latest-read request. The source
	// architecture guard confines this bridge to existing version-aware helpers.
	request := kv.NewReadRequest(ctx, nil, st, nil)
	snapshot, err := request.Historical(request.Context(), ts)
	return request, snapshot, request.Close, errors.WithStack(err)
}

func requestGetAt(ctx context.Context, st store.MVCCStore, key []byte, ts uint64) ([]byte, error) {
	request, snapshot, closeRead, err := requestReadSnapshot(ctx, st, ts)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer closeRead()
	return wrappedReadResult(request.Get(requestReadContext(ctx, request), snapshot, key))
}

func requestScanAt(ctx context.Context, st store.MVCCStore, start, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	request, snapshot, closeRead, err := requestReadSnapshot(ctx, st, ts)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer closeRead()
	return wrappedReadResult(request.Scan(requestReadContext(ctx, request), snapshot, start, end, limit))
}

func requestReadContext(ctx context.Context, request *kv.ReadRequest) context.Context {
	if _, scoped := kv.ReadRequestFromContext(ctx); !scoped {
		return request.Context()
	}
	return ctx
}

// historicalGetAt is reserved for protocol-authorized versions, e.g. an
// immutable content-addressed blob or an exact committed idempotency probe.
// It creates a fresh, store-bound child capability rather than borrowing the
// metadata request's token for another store.
func historicalGetAt(ctx context.Context, st store.MVCCStore, key []byte, ts uint64) ([]byte, error) {
	request := kv.NewReadRequest(ctx, nil, st, nil)
	defer request.Close()
	snapshot, err := request.Historical(request.Context(), ts)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return wrappedReadResult(request.Get(request.Context(), snapshot, key))
}

func historicalScanAt(ctx context.Context, st store.MVCCStore, start, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	request := kv.NewReadRequest(ctx, nil, st, nil)
	defer request.Close()
	snapshot, err := request.Historical(request.Context(), ts)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return wrappedReadResult(request.Scan(request.Context(), snapshot, start, end, limit))
}

// authorizeHistoricalRead is used only at an explicit historical/eventual
// protocol choice. Resolution in requestReadSnapshot must never call it.
func authorizeHistoricalRead(ctx context.Context, st store.MVCCStore, ts uint64) error {
	request, ok := kv.ReadRequestFromContext(ctx)
	if !ok {
		return nil
	}
	if !request.MatchesStore(st) {
		return kv.ErrReadSnapshotScope
	}
	_, err := request.Historical(ctx, ts)
	return errors.WithStack(err)
}

func checkIssuedGet(ctx context.Context, st store.MVCCStore, key []byte, ts uint64) error {
	request, ok := kv.ReadRequestFromContext(ctx)
	if !ok {
		return nil
	}
	if !request.MatchesStore(st) {
		return kv.ErrReadSnapshotScope
	}
	snapshot, err := request.Issued(ctx, ts)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(request.CheckGet(ctx, snapshot, key))
}

func wrappedReadResult[T any](value T, err error) (T, error) { return value, errors.WithStack(err) }

func (s *S3Server) newReadRequest(ctx context.Context) *kv.ReadRequest {
	if s == nil {
		return kv.NewReadRequest(ctx, nil, nil, nil)
	}
	return kv.NewReadRequest(ctx, s.coordinator, s.store, s.readTracker)
}
func (s *SQSServer) newReadRequest(ctx context.Context) *kv.ReadRequest {
	if s == nil {
		return kv.NewReadRequest(ctx, nil, nil, nil)
	}
	return kv.NewReadRequest(ctx, s.coordinator, s.store, s.readTracker)
}
func (d *DynamoDBServer) newReadRequest(ctx context.Context) *kv.ReadRequest {
	if d == nil {
		return kv.NewReadRequest(ctx, nil, nil, nil)
	}
	return kv.NewReadRequest(ctx, d.coordinator, d.store, d.readTracker)
}
