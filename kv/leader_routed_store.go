package kv

import (
	"bytes"
	"context"
	"io"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
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

type linearizableKeyCoordinator interface {
	LinearizableReadForKey(ctx context.Context, key []byte) (uint64, error)
}

func NewLeaderRoutedStore(local store.MVCCStore, coordinator Coordinator) *LeaderRoutedStore {
	return &LeaderRoutedStore{
		local:       local,
		coordinator: coordinator,
	}
}

// leaderFenceTS prefers a linearizable read fence when the coordinator
// exposes one. Returns (localOK, fenceTS): when localOK is true the caller
// should read from the local store at max(callerTS, fenceTS) so the snapshot
// is at least as fresh as the fence point. Older coordinators fall back to the
// legacy quorum verify path (fenceTS is 0 in that case).
func (s *LeaderRoutedStore) leaderFenceTS(ctx context.Context, key []byte) (bool, uint64) {
	if s.coordinator == nil {
		return true, 0
	}
	if reader, ok := s.coordinator.(linearizableKeyCoordinator); ok {
		fenceTS, err := reader.LinearizableReadForKey(ctx, key)
		return err == nil, fenceTS
	}
	if !s.coordinator.IsLeaderForKey(key) {
		return false, 0
	}
	return s.coordinator.VerifyLeaderForKey(ctx, key) == nil, 0
}

// leaderOKForKey returns whether the local store is authoritative for key.
// Use leaderFenceTS when the read timestamp must be updated after the fence.
func (s *LeaderRoutedStore) leaderOKForKey(ctx context.Context, key []byte) bool {
	ok, _ := s.leaderFenceTS(ctx, key)
	return ok
}

func (s *LeaderRoutedStore) leaderAddrForKey(key []byte) string {
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
	// Compatibility with older nodes that don't set RawGetResponse.exists:
	// treat any non-nil payload as found even when exists=false.
	if !resp.GetExists() && resp.GetValue() == nil {
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

func (s *LeaderRoutedStore) proxyRawScanAt(
	ctx context.Context,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
	reverse bool,
) ([]*store.KVPair, error) {
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
		Reverse:  reverse,
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

func (s *LeaderRoutedStore) proxyRawScanKeysAt(
	ctx context.Context,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
) ([][]byte, error) {
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
		KeysOnly: true,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	out := make([][]byte, 0, len(resp.Kv))
	for _, kvp := range resp.Kv {
		if kvp == nil {
			continue
		}
		out = append(out, bytes.Clone(kvp.Key))
	}
	return out, nil
}

func (s *LeaderRoutedStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	if s == nil || s.local == nil {
		return nil, store.ErrKeyNotFound
	}
	ok, fenceTS := s.leaderFenceTS(ctx, key)
	if ok {
		val, err := s.local.GetAt(ctx, key, max(ts, fenceTS))
		return val, errors.WithStack(err)
	}
	return s.proxyRawGet(ctx, key, ts)
}

func (s *LeaderRoutedStore) ExistsAt(ctx context.Context, key []byte, ts uint64) (bool, error) {
	if s == nil || s.local == nil {
		return false, nil
	}
	ok, fenceTS := s.leaderFenceTS(ctx, key)
	if ok {
		// Use max(ts, fenceTS) so the snapshot is at least as fresh as the
		// linearizable fence point. Without this, a timestamp acquired before the
		// fence completes could cause the read to miss writes the fence was meant
		// to make visible.
		exists, err := s.local.ExistsAt(ctx, key, max(ts, fenceTS))
		return exists, errors.WithStack(err)
	}
	// Via proxy path: RawGet returns a nil Value for a key that exists with an
	// empty value because proto3 strips zero-valued bytes fields on the wire.
	// Determine existence from the error alone, not from whether Value is non-nil.
	_, err := s.proxyRawGet(ctx, key, ts)
	if errors.Is(err, store.ErrKeyNotFound) {
		return false, nil
	}
	return err == nil, errors.WithStack(err)
}

// CommittedVersionAt gates the exact-timestamp existence probe so client
// reads through this wrapper get a fresh authoritative answer even on a
// deposed leader. The FSM apply path is NOT affected — it holds the raw
// local store (not a LeaderRoutedStore), so its deterministic probe never
// goes through this method. The option-2 reuse path
// (RedisServer.resolveReuseLength) DOES call this and needs the
// authoritative answer to preserve the pending.length fast-path
// (returning the per-our-commit length rather than the leader's current
// Len) when our prior attempt actually committed.
//
// Two-path strategy, mirroring how LatestCommitTS uses a lease fast-path
// and a proxy slow-path:
//
//   - We are the leader with a valid lease (leaderOKForKey is true): the
//     local replica is up-to-date by the lease invariant; read local.
//   - Not leader (deposed or never): there is no RawCommittedVersionAt
//     RPC to proxy to, so use the coordinator's LinearizableRead to
//     submit a Raft ReadIndex — that protocol forwards to the current
//     leader and waits until our local applied index has caught up to
//     the leader's commit point. After that, a local probe sees every
//     committed version of this key (including any landed at commitTS).
//     If the read-index fails (no leader reachable, ctx canceled), fall
//     back to (false, nil); the adapter's resolveReuseLength then re-reads
//     via the already-leader-fenced ScanAt/GetAt, returning the leader's
//     current Len — a valid serialization, just not the per-our-commit
//     value.
func (s *LeaderRoutedStore) CommittedVersionAt(ctx context.Context, key []byte, commitTS uint64) (bool, error) {
	if s == nil || s.local == nil {
		return false, nil
	}
	if !s.leaderOKForKey(ctx, key) && !s.tryLinearizableFence(ctx) {
		// Not leader and ReadIndex failed (no coordinator wired, no leader
		// reachable, ctx canceled). Report (false, nil) so the adapter's
		// resolveReuseLength falls through to the leader-fenced ScanAt/GetAt
		// path, which returns a valid current-Len serialization.
		return false, nil
	}
	exists, err := s.local.CommittedVersionAt(ctx, key, commitTS)
	return exists, errors.WithStack(err)
}

// tryLinearizableFence submits a Raft ReadIndex via the coordinator and
// reports whether it succeeded. After a successful ReadIndex the local
// applied index is caught up to the current leader's commit point, so a
// subsequent local read sees every committed version. The error from the
// underlying call is intentionally not surfaced — callers that need the
// authoritative answer treat a failed fence as "couldn't verify, fall back
// to the leader-routed slow path." Structured to avoid the nilerr
// false positive at the call site.
func (s *LeaderRoutedStore) tryLinearizableFence(ctx context.Context) bool {
	if s == nil || s.coordinator == nil {
		return false
	}
	_, err := s.coordinator.LinearizableRead(ctx)
	return err == nil
}

func (s *LeaderRoutedStore) ScanAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	if s == nil || s.local == nil {
		return []*store.KVPair{}, nil
	}
	if limit <= 0 {
		return []*store.KVPair{}, nil
	}
	ok, fenceTS := s.leaderFenceTS(ctx, start)
	if ok {
		kvs, err := s.local.ScanAt(ctx, start, end, limit, max(ts, fenceTS))
		return kvs, errors.WithStack(err)
	}
	return s.proxyRawScanAt(ctx, start, end, limit, ts, false)
}

func (s *LeaderRoutedStore) ScanKeysAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([][]byte, error) {
	if s == nil || s.local == nil {
		return [][]byte{}, nil
	}
	if limit <= 0 {
		return [][]byte{}, nil
	}
	ok, fenceTS := s.leaderFenceTS(ctx, start)
	if ok {
		keys, err := s.local.ScanKeysAt(ctx, start, end, limit, max(ts, fenceTS))
		return keys, errors.WithStack(err)
	}
	return s.proxyRawScanKeysAt(ctx, start, end, limit, ts)
}

func (s *LeaderRoutedStore) ScanAtPhysicalLimit(ctx context.Context, start []byte, end []byte, visibleLimit, physicalLimit int, ts uint64) ([]*store.KVPair, bool, error) {
	return s.scanAtPhysicalLimit(ctx, start, end, visibleLimit, physicalLimit, ts, false)
}

func (s *LeaderRoutedStore) ReverseScanAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	if s == nil || s.local == nil {
		return []*store.KVPair{}, nil
	}
	if limit <= 0 {
		return []*store.KVPair{}, nil
	}
	ok, fenceTS := s.leaderFenceTS(ctx, start)
	if ok {
		kvs, err := s.local.ReverseScanAt(ctx, start, end, limit, max(ts, fenceTS))
		return kvs, errors.WithStack(err)
	}
	return s.proxyRawScanAt(ctx, start, end, limit, ts, true)
}

func (s *LeaderRoutedStore) ReverseScanAtPhysicalLimit(ctx context.Context, start []byte, end []byte, visibleLimit, physicalLimit int, ts uint64) ([]*store.KVPair, bool, error) {
	return s.scanAtPhysicalLimit(ctx, start, end, visibleLimit, physicalLimit, ts, true)
}

func (s *LeaderRoutedStore) scanAtPhysicalLimit(ctx context.Context, start []byte, end []byte, visibleLimit, physicalLimit int, ts uint64, reverse bool) ([]*store.KVPair, bool, error) {
	if s == nil || s.local == nil {
		return []*store.KVPair{}, false, nil
	}
	if visibleLimit <= 0 || physicalLimit <= 0 {
		return []*store.KVPair{}, false, nil
	}
	ok, fenceTS := s.leaderFenceTS(ctx, start)
	if ok {
		scanner, hasPhysicalLimit := s.local.(physicalLimitedStore)
		if hasPhysicalLimit {
			return scanPhysicalLimitLocal(ctx, scanner, start, end, visibleLimit, physicalLimit, max(ts, fenceTS), reverse)
		}
		if !reverse {
			kvs, err := s.local.ScanAt(ctx, start, end, visibleLimit, max(ts, fenceTS))
			return kvs, false, errors.WithStack(err)
		}
		kvs, err := s.local.ReverseScanAt(ctx, start, end, visibleLimit, max(ts, fenceTS))
		return kvs, false, errors.WithStack(err)
	}
	// RawScanAt cannot enforce physicalLimit, so report truncation and let
	// callers fail closed instead of proxying an unbounded physical scan.
	return nil, true, nil
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
	if s.leaderOKForKey(ctx, key) {
		ts, exists, err := s.local.LatestCommitTS(ctx, key)
		return ts, exists, errors.WithStack(err)
	}
	return s.proxyRawLatestCommitTS(ctx, key)
}

func (s *LeaderRoutedStore) ApplyMutations(ctx context.Context, mutations []*store.KVPairMutation, readKeys [][]byte, startTS, commitTS uint64) error {
	if s == nil || s.local == nil {
		return errors.WithStack(store.ErrNotSupported)
	}
	return errors.WithStack(s.local.ApplyMutations(ctx, mutations, readKeys, startTS, commitTS))
}

// ApplyMutationsRaft forwards to the local store's raft-apply variant. See
// store.MVCCStore for the durability contract.
func (s *LeaderRoutedStore) ApplyMutationsRaft(ctx context.Context, mutations []*store.KVPairMutation, readKeys [][]byte, startTS, commitTS uint64) error {
	if s == nil || s.local == nil {
		return errors.WithStack(store.ErrNotSupported)
	}
	return errors.WithStack(s.local.ApplyMutationsRaft(ctx, mutations, readKeys, startTS, commitTS))
}

// ApplyMutationsRaftAt forwards to the local store's raft-entry-index-
// aware variant so the underlying pebbleStore can bundle
// metaAppliedIndex with the mutation. See PR #910 design §2.
func (s *LeaderRoutedStore) ApplyMutationsRaftAt(ctx context.Context, mutations []*store.KVPairMutation, readKeys [][]byte, startTS, commitTS, appliedIndex uint64) error {
	if s == nil || s.local == nil {
		return errors.WithStack(store.ErrNotSupported)
	}
	return errors.WithStack(s.local.ApplyMutationsRaftAt(ctx, mutations, readKeys, startTS, commitTS, appliedIndex))
}

func (s *LeaderRoutedStore) DeletePrefixAt(ctx context.Context, prefix []byte, excludePrefix []byte, commitTS uint64) error {
	if s == nil || s.local == nil {
		return errors.WithStack(store.ErrNotSupported)
	}
	return errors.WithStack(s.local.DeletePrefixAt(ctx, prefix, excludePrefix, commitTS))
}

// DeletePrefixAtRaft forwards to the local store's raft-apply variant.
func (s *LeaderRoutedStore) DeletePrefixAtRaft(ctx context.Context, prefix []byte, excludePrefix []byte, commitTS uint64) error {
	if s == nil || s.local == nil {
		return errors.WithStack(store.ErrNotSupported)
	}
	return errors.WithStack(s.local.DeletePrefixAtRaft(ctx, prefix, excludePrefix, commitTS))
}

// DeletePrefixAtRaftAt forwards to the local store's raft-entry-
// index-aware variant. See PR #910 design §2 "why both leaves".
func (s *LeaderRoutedStore) DeletePrefixAtRaftAt(ctx context.Context, prefix []byte, excludePrefix []byte, commitTS, appliedIndex uint64) error {
	if s == nil || s.local == nil {
		return errors.WithStack(store.ErrNotSupported)
	}
	return errors.WithStack(s.local.DeletePrefixAtRaftAt(ctx, prefix, excludePrefix, commitTS, appliedIndex))
}

// LastAppliedIndex forwards to the local store when it implements
// raftengine.AppliedIndexReader. Defensive: in production today the
// kvFSM holds a *pebbleStore directly (not a LeaderRoutedStore — that
// wrapper is used by adapter/server code for read routing, not by
// the FSM apply path); so this forward is currently dead code for
// the cold-start skip optimisation. We add it anyway because future
// refactors might wrap the FSM's store, and a silent no-op there
// would degrade the optimisation to full-restore-always with no
// failure signal.
//
// (0, false, nil) returns are the strictly-additive fallback —
// either the wrapper has no local, the local does not implement
// the reader, or the local reports missing/truncated. The caller in
// internal/raftengine/etcd/wal_store.go (Branch 3) treats all of
// these as "fall back to full restore", which is correct.
func (s *LeaderRoutedStore) LastAppliedIndex() (uint64, bool, error) {
	if s == nil || s.local == nil {
		return 0, false, nil
	}
	reader, ok := s.local.(interface {
		LastAppliedIndex() (uint64, bool, error)
	})
	if !ok {
		return 0, false, nil
	}
	idx, present, err := reader.LastAppliedIndex()
	if err != nil {
		return 0, false, errors.WithStack(err)
	}
	return idx, present, nil
}

// SetDurableAppliedIndex forwards to the local store when it
// implements raftengine.AppliedIndexWriter. Symmetric defensive
// no-op when the local store does not expose the writer seam — see
// LastAppliedIndex doc-comment.
func (s *LeaderRoutedStore) SetDurableAppliedIndex(idx uint64) error {
	if s == nil || s.local == nil {
		return nil
	}
	writer, ok := s.local.(interface {
		SetDurableAppliedIndex(idx uint64) error
	})
	if !ok {
		return nil
	}
	return errors.WithStack(writer.SetDurableAppliedIndex(idx))
}

func (s *LeaderRoutedStore) LastCommitTS() uint64 {
	if s == nil || s.local == nil {
		return 0
	}
	return s.local.LastCommitTS()
}

// WriteConflictCountsByPrefix delegates to the local MVCC store. The
// leader-routed wrapper does not add cross-group conflict detection of
// its own, so the node-local view IS the authoritative view.
func (s *LeaderRoutedStore) WriteConflictCountsByPrefix() map[string]uint64 {
	if s == nil || s.local == nil {
		return map[string]uint64{}
	}
	return s.local.WriteConflictCountsByPrefix()
}

const globalLastCommitTSTimeout = 200 * time.Millisecond

// GlobalLastCommitTS returns the most recently committed HLC timestamp from
// the authoritative leader.  On the leader this is the local LastCommitTS.
// On a follower the method issues a lightweight RPC (RawLatestCommitTS with
// an empty key) so callers obtain a non-stale snapshot — critical for
// ConsistentRead semantics where followers must not serve reads at a stale
// local watermark.  Falls back to the local LastCommitTS on any error.
func (s *LeaderRoutedStore) GlobalLastCommitTS(ctx context.Context) uint64 {
	if s == nil || s.local == nil {
		return 0
	}
	if s.coordinator == nil || s.coordinator.IsLeader() {
		return s.local.LastCommitTS()
	}
	addr := s.coordinator.RaftLeader()
	if addr == "" {
		return s.local.LastCommitTS()
	}
	conn, err := s.connCache.ConnFor(addr)
	if err != nil {
		return s.local.LastCommitTS()
	}
	proxyCtx, cancel := context.WithTimeout(ctx, globalLastCommitTSTimeout)
	defer cancel()
	cli := pb.NewRawKVClient(conn)
	resp, err := cli.RawLatestCommitTS(proxyCtx, &pb.RawLatestCommitTSRequest{})
	if err != nil || resp.GetTs() == 0 {
		return s.local.LastCommitTS()
	}
	return resp.GetTs()
}

func (s *LeaderRoutedStore) Compact(ctx context.Context, minTS uint64) error {
	if s == nil || s.local == nil {
		return errors.WithStack(store.ErrNotSupported)
	}
	return errors.WithStack(s.local.Compact(ctx, minTS))
}

func (s *LeaderRoutedStore) Snapshot() (store.Snapshot, error) {
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
