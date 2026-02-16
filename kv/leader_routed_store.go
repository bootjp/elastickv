package kv

import (
	"bytes"
	"context"
	"io"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
)

// LeaderRoutedStore is an MVCCStore wrapper that serves reads from the local
// store only when leadership is verified; otherwise it proxies reads to the
// current leader via gRPC.
//
// This is intended for single-raft-group deployments where the underlying
// store itself is not leader-aware (e.g. *store.MVCCStore).
//
// Writes and maintenance operations are delegated to the local store.
type LeaderRoutedStore struct {
	local       store.MVCCStore
	coordinator Coordinator

	connCache GRPCConnCache
}

func NewLeaderRoutedStore(local store.MVCCStore, coordinator Coordinator) *LeaderRoutedStore {
	return &LeaderRoutedStore{
		local:       local,
		coordinator: coordinator,
	}
}

func (s *LeaderRoutedStore) leaderOKForKey(key []byte) bool {
	if s.coordinator == nil {
		return true
	}
	if !s.coordinator.IsLeaderForKey(key) {
		return false
	}
	return s.coordinator.VerifyLeaderForKey(key) == nil
}

func (s *LeaderRoutedStore) leaderAddrForKey(key []byte) raft.ServerAddress {
	if s.coordinator == nil {
		return ""
	}
	return s.coordinator.RaftLeaderForKey(key)
}

func (s *LeaderRoutedStore) proxyRawGet(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	addr := s.leaderAddrForKey(key)
	if addr == "" {
		return nil, errors.WithStack(ErrLeaderNotFound)
	}

	conn, err := s.connCache.ConnFor(addr)
	if err != nil {
		return nil, err
	}

	cli := pb.NewRawKVClient(conn)
	resp, err := cli.RawGet(ctx, &pb.RawGetRequest{Key: key, Ts: ts})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !resp.GetExists() {
		return nil, store.ErrKeyNotFound
	}
	return resp.Value, nil
}

func (s *LeaderRoutedStore) proxyRawLatestCommitTS(ctx context.Context, key []byte) (uint64, bool, error) {
	addr := s.leaderAddrForKey(key)
	if addr == "" {
		return 0, false, errors.WithStack(ErrLeaderNotFound)
	}

	conn, err := s.connCache.ConnFor(addr)
	if err != nil {
		return 0, false, err
	}

	cli := pb.NewRawKVClient(conn)
	resp, err := cli.RawLatestCommitTS(ctx, &pb.RawLatestCommitTSRequest{Key: key})
	if err != nil {
		return 0, false, errors.WithStack(err)
	}
	return resp.Ts, resp.Exists, nil
}

func (s *LeaderRoutedStore) proxyRawScanAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	addr := s.leaderAddrForKey(start)
	if addr == "" {
		return nil, errors.WithStack(ErrLeaderNotFound)
	}

	conn, err := s.connCache.ConnFor(addr)
	if err != nil {
		return nil, err
	}

	cli := pb.NewRawKVClient(conn)
	resp, err := cli.RawScanAt(ctx, &pb.RawScanAtRequest{
		StartKey: start,
		EndKey:   end,
		Limit:    int64(limit),
		Ts:       ts,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	out := make([]*store.KVPair, 0, len(resp.Kv))
	for _, kvp := range resp.Kv {
		out = append(out, &store.KVPair{
			Key:   bytes.Clone(kvp.Key),
			Value: bytes.Clone(kvp.Value),
		})
	}
	return out, nil
}

func (s *LeaderRoutedStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	if s == nil || s.local == nil {
		return nil, store.ErrKeyNotFound
	}
	if s.leaderOKForKey(key) {
		val, err := s.local.GetAt(ctx, key, ts)
		return val, errors.WithStack(err)
	}
	return s.proxyRawGet(ctx, key, ts)
}

func (s *LeaderRoutedStore) ExistsAt(ctx context.Context, key []byte, ts uint64) (bool, error) {
	v, err := s.GetAt(ctx, key, ts)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return v != nil, nil
}

func (s *LeaderRoutedStore) ScanAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	if s == nil || s.local == nil {
		return []*store.KVPair{}, nil
	}
	if limit <= 0 {
		return []*store.KVPair{}, nil
	}
	if s.leaderOKForKey(start) {
		kvs, err := s.local.ScanAt(ctx, start, end, limit, ts)
		return kvs, errors.WithStack(err)
	}
	return s.proxyRawScanAt(ctx, start, end, limit, ts)
}

func (s *LeaderRoutedStore) PutAt(ctx context.Context, key []byte, value []byte, commitTS uint64, expireAt uint64) error {
	if s == nil || s.local == nil {
		return errors.WithStack(store.ErrNotSupported)
	}
	return errors.WithStack(s.local.PutAt(ctx, key, value, commitTS, expireAt))
}

func (s *LeaderRoutedStore) DeleteAt(ctx context.Context, key []byte, commitTS uint64) error {
	if s == nil || s.local == nil {
		return errors.WithStack(store.ErrNotSupported)
	}
	return errors.WithStack(s.local.DeleteAt(ctx, key, commitTS))
}

func (s *LeaderRoutedStore) PutWithTTLAt(ctx context.Context, key []byte, value []byte, commitTS uint64, expireAt uint64) error {
	if s == nil || s.local == nil {
		return errors.WithStack(store.ErrNotSupported)
	}
	return errors.WithStack(s.local.PutWithTTLAt(ctx, key, value, commitTS, expireAt))
}

func (s *LeaderRoutedStore) ExpireAt(ctx context.Context, key []byte, expireAt uint64, commitTS uint64) error {
	if s == nil || s.local == nil {
		return errors.WithStack(store.ErrNotSupported)
	}
	return errors.WithStack(s.local.ExpireAt(ctx, key, expireAt, commitTS))
}

func (s *LeaderRoutedStore) LatestCommitTS(ctx context.Context, key []byte) (uint64, bool, error) {
	if s == nil || s.local == nil {
		return 0, false, nil
	}
	if s.leaderOKForKey(key) {
		ts, exists, err := s.local.LatestCommitTS(ctx, key)
		return ts, exists, errors.WithStack(err)
	}
	return s.proxyRawLatestCommitTS(ctx, key)
}

func (s *LeaderRoutedStore) ApplyMutations(ctx context.Context, mutations []*store.KVPairMutation, startTS, commitTS uint64) error {
	if s == nil || s.local == nil {
		return errors.WithStack(store.ErrNotSupported)
	}
	return errors.WithStack(s.local.ApplyMutations(ctx, mutations, startTS, commitTS))
}

func (s *LeaderRoutedStore) LastCommitTS() uint64 {
	if s == nil || s.local == nil {
		return 0
	}
	return s.local.LastCommitTS()
}

func (s *LeaderRoutedStore) Compact(ctx context.Context, minTS uint64) error {
	if s == nil || s.local == nil {
		return errors.WithStack(store.ErrNotSupported)
	}
	return errors.WithStack(s.local.Compact(ctx, minTS))
}

func (s *LeaderRoutedStore) Snapshot() (io.ReadWriter, error) {
	if s == nil || s.local == nil {
		return nil, errors.WithStack(store.ErrNotSupported)
	}
	snap, err := s.local.Snapshot()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return snap, nil
}

func (s *LeaderRoutedStore) Restore(buf io.Reader) error {
	if s == nil || s.local == nil {
		return errors.WithStack(store.ErrNotSupported)
	}
	return errors.WithStack(s.local.Restore(buf))
}

func (s *LeaderRoutedStore) Close() error {
	if s == nil {
		return nil
	}
	// LeaderRoutedStore is a routing wrapper; it does not own the underlying
	// store's lifecycle. Close only releases resources owned by the wrapper.
	return s.connCache.Close()
}

var _ store.MVCCStore = (*LeaderRoutedStore)(nil)
