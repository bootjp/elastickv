package kv

import (
	"bytes"
	"context"
	"sort"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const defaultBackupScanPageSize = 1024

// BackupScanner pages through ShardStore.ScanKeysAt without holding store locks
// across pages, then materializes each key at the pinned read timestamp.
type BackupScanner interface {
	Next(ctx context.Context) (*store.KVPair, bool, error)
	Close() error
}

// BackupKeyFilter decides whether a key should be materialized by a value
// scanner. It runs after route ownership filtering but before reading values.
type BackupKeyFilter func(key []byte) (bool, error)

// BackupKeyScanner is the count-only counterpart to BackupScanner. It pages
// through the same captured route set without materializing values.
type BackupKeyScanner interface {
	Next(ctx context.Context) ([]byte, bool, error)
	Close() error
}

// BackupRouteSnapshot is an immutable route view shared by every scan in one
// logical backup. Keeping it separate from a scanner lets BeginBackup count
// keys and StreamBackup materialize values from the same ownership view even
// when the live route catalog changes between those RPCs.
type BackupRouteSnapshot struct {
	routes        []distribution.Route
	scanGroups    []uint64
	clampToRoutes bool
	start         []byte
	end           []byte
}

type backupScanner struct {
	store         *ShardStore
	routes        []distribution.Route
	scanGroups    []uint64
	clampToRoutes bool
	end           []byte
	ts            uint64
	pageSize      int
	cursor        []byte
	page          []*store.KVPair
	index         int
	keyFilter     BackupKeyFilter
	closed        bool
	exhausted     bool
}

type routedScanKey struct {
	key           []byte
	route         distribution.Route
	partitionOnly bool
}

type backupKeyScanner struct {
	store         *ShardStore
	routes        []distribution.Route
	scanGroups    []uint64
	clampToRoutes bool
	end           []byte
	ts            uint64
	pageSize      int
	cursor        []byte
	page          []routedScanKey
	index         int
	closed        bool
	exhausted     bool
}

func NewBackupScanner(st *ShardStore, start []byte, end []byte, ts uint64, pageSize int) BackupScanner {
	snapshot := st.CaptureBackupRouteSnapshot(start, end)
	return NewBackupScannerAtSnapshot(st, snapshot, ts, pageSize)
}

// CaptureBackupRouteSnapshot captures route ownership and scan bounds once.
func (s *ShardStore) CaptureBackupRouteSnapshot(start []byte, end []byte) BackupRouteSnapshot {
	if s == nil {
		return BackupRouteSnapshot{start: bytes.Clone(start), end: bytes.Clone(end)}
	}
	routes, clampToRoutes := s.routesForForwardScan(start, end)
	return BackupRouteSnapshot{
		routes:        cloneBackupRoutes(routes),
		scanGroups:    backupRouteGroupIDs(routes),
		clampToRoutes: clampToRoutes,
		start:         bytes.Clone(start),
		end:           bytes.Clone(end),
	}
}

// CaptureBackupRouteSnapshotAt reads the durable distribution catalog at ts.
// The caller must pass the CatalogStore bound to the catalog owner group;
// using a normally routed ShardStore can split the version read from the
// route-row scan when those reserved prefixes route to different groups.
func CaptureBackupRouteSnapshotAt(ctx context.Context, catalog *distribution.CatalogStore, ts uint64) (BackupRouteSnapshot, error) {
	if catalog == nil {
		return BackupRouteSnapshot{}, errors.New("backup route store is unavailable")
	}
	snapshot, err := catalog.SnapshotAt(ctx, ts)
	if err != nil {
		return BackupRouteSnapshot{}, errors.Wrap(err, "read distribution catalog at backup timestamp")
	}
	routes, err := distribution.RoutesFromCatalogSnapshot(snapshot)
	if err != nil {
		return BackupRouteSnapshot{}, errors.Wrap(err, "materialize backup route snapshot")
	}
	return BackupRouteSnapshot{routes: cloneBackupRoutes(routes), scanGroups: backupRouteGroupIDs(routes)}, nil
}

// BackupRouteSnapshotWithScanGroups returns a snapshot that also scans the
// supplied Raft groups for resolver-owned partitioned keys even when the durable
// byte-range catalog has no route for those groups.
func BackupRouteSnapshotWithScanGroups(snapshot BackupRouteSnapshot, groupIDs []uint64) BackupRouteSnapshot {
	out := cloneBackupRouteSnapshot(snapshot)
	out.scanGroups = appendUniqueBackupGroups(out.scanGroups, groupIDs)
	return out
}

// ValidateBackupSnapshotAt resolves committed or rolled-back transaction
// locks and fails closed while any prepared transaction remains pending at the
// backup cut. The scan covers lock-only inserts that have no visible user key.
func (s *ShardStore) ValidateBackupSnapshotAt(ctx context.Context, snapshot BackupRouteSnapshot, ts uint64, pageSize int) error {
	if s == nil {
		return errors.New("backup store is unavailable")
	}
	if pageSize <= 0 {
		pageSize = defaultBackupScanPageSize
	}
	for _, groupID := range backupSnapshotGroupIDs(snapshot) {
		group, ok := s.groupForID(groupID)
		if !ok || group == nil || group.Store == nil {
			return errors.Wrapf(ErrLeaderNotFound, "backup lock validation group %d is unavailable", groupID)
		}
		if err := s.validateBackupGroupLocksAt(ctx, group, ts, pageSize); err != nil {
			return errors.Wrapf(err, "validate backup locks for group %d", groupID)
		}
	}
	return nil
}

func (s *ShardStore) validateBackupGroupLocksAt(ctx context.Context, group *ShardGroup, ts uint64, pageSize int) error {
	cursor := txnLockKey(nil)
	end := prefixScanEnd([]byte(txnLockPrefix))
	for {
		locks, err := group.Store.ScanAt(ctx, cursor, end, pageSize, ts)
		if err != nil {
			return errors.WithStack(err)
		}
		if len(locks) == 0 {
			return nil
		}
		plan, err := s.planBackupLockResolutionsAt(ctx, locks, ts)
		if err != nil {
			return err
		}
		if err := applyScanLockResolutions(ctx, group, plan); err != nil {
			return err
		}
		last := locks[len(locks)-1]
		if len(locks) < pageSize {
			return nil
		}
		if last == nil || len(last.Key) == 0 {
			return errors.New("backup lock scan returned an invalid cursor")
		}
		cursor = nextScanCursor(last.Key)
		if bytes.Compare(cursor, end) >= 0 {
			return nil
		}
	}
}

func (s *ShardStore) planBackupLockResolutionsAt(ctx context.Context, lockKVs []*store.KVPair, ts uint64) (*scanLockPlan, error) {
	plan := newScanLockPlan(len(lockKVs))
	for _, kvp := range lockKVs {
		if err := s.planBackupLockResolutionAt(ctx, plan, kvp, ts); err != nil {
			return nil, err
		}
	}
	return plan, nil
}

func (s *ShardStore) planBackupLockResolutionAt(ctx context.Context, plan *scanLockPlan, kvp *store.KVPair, ts uint64) error {
	if kvp == nil {
		return nil
	}
	userKey, ok := txnUserKeyFromLockKey(kvp.Key)
	if !ok {
		return nil
	}
	lock, err := decodeTxnLock(kvp.Value)
	if err != nil {
		return errors.WithStack(err)
	}
	if len(lock.PrimaryKey) == 0 {
		return errors.Wrapf(ErrTxnInvalidMeta, "missing txn primary key for key %s", string(userKey))
	}
	txnKey := lockTxnKey{startTS: lock.StartTS, primary: string(lock.PrimaryKey)}
	state, err := s.cachedBackupLockTxnStatusAt(ctx, plan, lock, txnKey, ts)
	if err != nil {
		return err
	}
	phase, resolveTS, err := lockResolutionForStatus(state, lock, userKey, plan.cleanupNow)
	if err != nil {
		return err
	}
	appendScanLockResolutionBatch(plan, txnKey, phase, resolveTS, lock, userKey)
	return nil
}

func (s *ShardStore) cachedBackupLockTxnStatusAt(
	ctx context.Context,
	plan *scanLockPlan,
	lock txnLock,
	txnKey lockTxnKey,
	ts uint64,
) (lockTxnStatus, error) {
	if state, ok := plan.statusCache[txnKey]; ok {
		return state, nil
	}
	status, commitTS, err := s.primaryTxnRecordedStatusAt(ctx, lock.PrimaryKey, lock.StartTS, ts)
	if err != nil {
		return lockTxnStatus{}, err
	}
	state := lockTxnStatus{status: status, commitTS: commitTS}
	plan.statusCache[txnKey] = state
	return state, nil
}

func (s *ShardStore) primaryTxnRecordedStatusAt(ctx context.Context, primaryKey []byte, startTS uint64, ts uint64) (txnStatus, uint64, error) {
	commitTS, committed, err := s.txnCommitTSAt(ctx, primaryKey, startTS, ts)
	if err != nil {
		return txnStatusPending, 0, err
	}
	if committed && commitTS <= ts {
		return txnStatusCommitted, commitTS, nil
	}
	rolledBack, err := s.hasTxnRollbackAt(ctx, primaryKey, startTS, ts)
	if err != nil {
		return txnStatusPending, 0, err
	}
	if rolledBack {
		return txnStatusRolledBack, 0, nil
	}
	return txnStatusPending, 0, nil
}

func (s *ShardStore) txnCommitTSAt(ctx context.Context, primaryKey []byte, startTS uint64, ts uint64) (uint64, bool, error) {
	b, err := s.GetAt(ctx, txnCommitKey(primaryKey, startTS), ts)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return 0, false, nil
		}
		return 0, false, err
	}
	cts, derr := decodeTxnCommitRecord(b)
	if derr != nil {
		return 0, false, errors.WithStack(derr)
	}
	return cts, true, nil
}

func (s *ShardStore) hasTxnRollbackAt(ctx context.Context, primaryKey []byte, startTS uint64, ts uint64) (bool, error) {
	_, err := s.GetAt(ctx, txnRollbackKey(primaryKey, startTS), ts)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// NewBackupScannerAtSnapshot creates a value scanner from a captured route view.
func NewBackupScannerAtSnapshot(st *ShardStore, snapshot BackupRouteSnapshot, ts uint64, pageSize int) BackupScanner {
	return NewFilteredBackupScannerAtSnapshot(st, snapshot, ts, pageSize, nil)
}

// NewFilteredBackupScannerAtSnapshot creates a value scanner that skips
// filtered-out keys before materializing values.
func NewFilteredBackupScannerAtSnapshot(
	st *ShardStore,
	snapshot BackupRouteSnapshot,
	ts uint64,
	pageSize int,
	keyFilter BackupKeyFilter,
) BackupScanner {
	if pageSize <= 0 {
		pageSize = defaultBackupScanPageSize
	}
	snapshot = cloneBackupRouteSnapshot(snapshot)
	return &backupScanner{
		store:         st,
		routes:        snapshot.routes,
		scanGroups:    snapshot.scanGroups,
		clampToRoutes: snapshot.clampToRoutes,
		cursor:        snapshot.start,
		end:           snapshot.end,
		ts:            ts,
		pageSize:      pageSize,
		keyFilter:     keyFilter,
	}
}

func (s *ShardStore) NewBackupScanner(start []byte, end []byte, ts uint64, pageSize int) BackupScanner {
	return NewBackupScanner(s, start, end, ts, pageSize)
}

func (s *ShardStore) NewBackupScannerAtSnapshot(snapshot BackupRouteSnapshot, ts uint64, pageSize int) BackupScanner {
	return NewBackupScannerAtSnapshot(s, snapshot, ts, pageSize)
}

func (s *ShardStore) NewFilteredBackupScannerAtSnapshot(
	snapshot BackupRouteSnapshot,
	ts uint64,
	pageSize int,
	keyFilter BackupKeyFilter,
) BackupScanner {
	return NewFilteredBackupScannerAtSnapshot(s, snapshot, ts, pageSize, keyFilter)
}

func NewBackupKeyScanner(st *ShardStore, start []byte, end []byte, ts uint64, pageSize int) BackupKeyScanner {
	snapshot := st.CaptureBackupRouteSnapshot(start, end)
	return NewBackupKeyScannerAtSnapshot(st, snapshot, ts, pageSize)
}

// NewBackupKeyScannerAtSnapshot creates a key-only scanner from a captured route view.
func NewBackupKeyScannerAtSnapshot(st *ShardStore, snapshot BackupRouteSnapshot, ts uint64, pageSize int) BackupKeyScanner {
	if pageSize <= 0 {
		pageSize = defaultBackupScanPageSize
	}
	snapshot = cloneBackupRouteSnapshot(snapshot)
	return &backupKeyScanner{
		store:         st,
		routes:        snapshot.routes,
		scanGroups:    snapshot.scanGroups,
		clampToRoutes: snapshot.clampToRoutes,
		cursor:        snapshot.start,
		end:           snapshot.end,
		ts:            ts,
		pageSize:      pageSize,
	}
}

func (s *ShardStore) NewBackupKeyScanner(start []byte, end []byte, ts uint64, pageSize int) BackupKeyScanner {
	return NewBackupKeyScanner(s, start, end, ts, pageSize)
}

func (s *ShardStore) NewBackupKeyScannerAtSnapshot(snapshot BackupRouteSnapshot, ts uint64, pageSize int) BackupKeyScanner {
	return NewBackupKeyScannerAtSnapshot(s, snapshot, ts, pageSize)
}

func (s *backupScanner) Next(ctx context.Context) (*store.KVPair, bool, error) {
	if s.closed || s.store == nil {
		return nil, false, nil
	}
	for s.index >= len(s.page) {
		if err := s.loadNextPage(ctx); err != nil {
			return nil, false, err
		}
		if len(s.page) == 0 {
			if s.exhausted {
				return nil, false, nil
			}
			continue
		}
	}
	kvp := s.page[s.index]
	s.index++
	if kvp == nil {
		return nil, true, nil
	}
	return kvp, true, nil
}

func (s *backupScanner) Close() error {
	s.closed = true
	s.exhausted = true
	s.page = nil
	return nil
}

func (s *backupKeyScanner) Next(ctx context.Context) ([]byte, bool, error) {
	if s.closed || s.store == nil {
		return nil, false, nil
	}
	for s.index >= len(s.page) {
		if err := s.loadNextPage(ctx); err != nil {
			return nil, false, err
		}
		if len(s.page) == 0 {
			if s.exhausted {
				return nil, false, nil
			}
			continue
		}
	}
	key := bytes.Clone(s.page[s.index].key)
	s.index++
	return key, true, nil
}

func (s *backupKeyScanner) Close() error {
	s.closed = true
	s.exhausted = true
	s.page = nil
	return nil
}

func (s *backupKeyScanner) loadNextPage(ctx context.Context) error {
	if s.exhausted {
		s.page = nil
		s.index = 0
		return nil
	}
	keys, err := s.store.scanKeyRoutesWithSourceAt(
		ctx, s.routes, s.scanGroups, s.cursor, s.end, s.pageSize, s.ts, s.clampToRoutes,
	)
	if err != nil {
		return err
	}
	s.page = s.page[:0]
	for _, item := range keys {
		if _, ok, err := s.store.routeForRoutedKey(item, s.routes); err != nil {
			return err
		} else if ok {
			s.page = append(s.page, item)
		}
	}
	s.index = 0
	if len(keys) == 0 {
		s.exhausted = true
		return nil
	}
	last := lastRoutedScanKey(keys)
	if last == nil {
		s.exhausted = true
		return nil
	}
	s.cursor = nextScanCursor(last)
	return nil
}

func (s *backupScanner) loadNextPage(ctx context.Context) error {
	if s.exhausted {
		s.page = nil
		s.index = 0
		return nil
	}
	keys, err := s.store.scanKeyRoutesWithSourceAt(
		ctx, s.routes, s.scanGroups, s.cursor, s.end, s.pageSize, s.ts, s.clampToRoutes,
	)
	if err != nil {
		return err
	}
	s.page = s.page[:0]
	for _, item := range keys {
		kvp, ok, err := s.materializeBackupKey(ctx, item)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		s.page = append(s.page, kvp)
	}
	s.index = 0
	if len(keys) == 0 {
		s.exhausted = true
		return nil
	}
	last := lastRoutedScanKey(keys)
	if last == nil {
		s.exhausted = true
		return nil
	}
	s.cursor = nextScanCursor(last)
	return nil
}

func (s *backupScanner) materializeBackupKey(ctx context.Context, item routedScanKey) (*store.KVPair, bool, error) {
	route, ok, err := s.materializeRouteForKey(item)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	if s.keyFilter != nil {
		selected, err := s.keyFilter(item.key)
		if err != nil {
			return nil, false, err
		}
		if !selected {
			return nil, false, nil
		}
	}
	val, err := s.store.getRouteAt(ctx, route, item.key, s.ts)
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &store.KVPair{Key: bytes.Clone(item.key), Value: bytes.Clone(val)}, true, nil
}

func (s *backupScanner) materializeRouteForKey(item routedScanKey) (distribution.Route, bool, error) {
	return s.store.routeForRoutedKey(item, s.routes)
}

func (s *ShardStore) routeForRoutedKey(item routedScanKey, routes []distribution.Route) (distribution.Route, bool, error) {
	if route, ok, handled, err := s.partitionRouteForRoutedKey(item); handled || err != nil {
		return route, ok, err
	}
	if item.partitionOnly {
		return distribution.Route{}, false, nil
	}
	return byteRangeRouteForRoutedKey(item, routes)
}

func (s *ShardStore) partitionRouteForRoutedKey(item routedScanKey) (distribution.Route, bool, bool, error) {
	if s != nil && s.partitionResolver != nil {
		groupID, ok := s.partitionResolver.ResolveGroup(item.key)
		if ok {
			if groupID == item.route.GroupID {
				return distribution.Route{GroupID: groupID}, true, true, nil
			}
			return distribution.Route{}, false, true, nil
		}
		if s.partitionResolver.RecognisesPartitionedKey(item.key) {
			return distribution.Route{}, false, true, errors.Wrapf(ErrInvalidRequest, "no partition route for backup key %q", item.key)
		}
	}
	return distribution.Route{}, false, false, nil
}

func byteRangeRouteForRoutedKey(item routedScanKey, routes []distribution.Route) (distribution.Route, bool, error) {
	key := routeKey(item.key)
	if routeContainsKey(item.route, key) {
		return item.route, true, nil
	}
	for _, route := range routes {
		if route.GroupID != item.route.GroupID {
			continue
		}
		if routeContainsKey(route, key) {
			return route, true, nil
		}
	}
	return distribution.Route{}, false, nil
}

func cloneBackupRouteSnapshot(snapshot BackupRouteSnapshot) BackupRouteSnapshot {
	return BackupRouteSnapshot{
		routes:        cloneBackupRoutes(snapshot.routes),
		scanGroups:    appendUniqueBackupGroups(nil, snapshot.scanGroups),
		clampToRoutes: snapshot.clampToRoutes,
		start:         bytes.Clone(snapshot.start),
		end:           bytes.Clone(snapshot.end),
	}
}

func cloneBackupRoutes(routes []distribution.Route) []distribution.Route {
	out := make([]distribution.Route, len(routes))
	for i, route := range routes {
		out[i] = route
		out[i].Start = bytes.Clone(route.Start)
		out[i].End = bytes.Clone(route.End)
	}
	return out
}

func backupRouteGroupIDs(routes []distribution.Route) []uint64 {
	groups := make([]uint64, 0, len(routes))
	for _, route := range routes {
		groups = appendUniqueBackupGroups(groups, []uint64{route.GroupID})
	}
	return groups
}

func backupSnapshotGroupIDs(snapshot BackupRouteSnapshot) []uint64 {
	groups := backupRouteGroupIDs(snapshot.routes)
	return appendUniqueBackupGroups(groups, snapshot.scanGroups)
}

func appendUniqueBackupGroups(dst []uint64, src []uint64) []uint64 {
	if len(src) == 0 {
		return dst
	}
	seen := make(map[uint64]struct{}, len(dst)+len(src))
	for _, groupID := range dst {
		seen[groupID] = struct{}{}
	}
	for _, groupID := range src {
		if _, ok := seen[groupID]; ok {
			continue
		}
		seen[groupID] = struct{}{}
		dst = append(dst, groupID)
	}
	return dst
}

func (s *ShardStore) scanKeyRoutesWithSourceAt(
	ctx context.Context,
	routes []distribution.Route,
	scanGroups []uint64,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
	clampToRoutes bool,
) ([]routedScanKey, error) {
	out := make([]routedScanKey, 0)
	seenGroups := make(map[uint64]struct{})
	out, err := s.scanSnapshotKeyRoutesWithSourceAt(ctx, out, routes, start, end, limit, ts, clampToRoutes, seenGroups)
	if err != nil {
		return nil, err
	}
	return s.scanPartitionOnlyKeyGroupsAt(ctx, out, routes, scanGroups, seenGroups, start, end, limit, ts)
}

func (s *ShardStore) scanSnapshotKeyRoutesWithSourceAt(
	ctx context.Context,
	out []routedScanKey,
	routes []distribution.Route,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
	clampToRoutes bool,
	seenGroups map[uint64]struct{},
) ([]routedScanKey, error) {
	for _, route := range routes {
		scanStart, scanEnd, skip := backupRouteScanBounds(route, start, end, clampToRoutes, seenGroups)
		if skip {
			continue
		}
		next, err := s.appendBackupScanRouteKeysAt(ctx, out, routes, route, scanStart, scanEnd, limit, ts, false)
		if err != nil {
			return nil, err
		}
		out = next
		if clampToRoutes && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func backupRouteScanBounds(
	route distribution.Route,
	start []byte,
	end []byte,
	clampToRoutes bool,
	seenGroups map[uint64]struct{},
) (scanStart []byte, scanEnd []byte, skip bool) {
	if clampToRoutes {
		seenGroups[route.GroupID] = struct{}{}
		return clampScanStart(start, route.Start), clampScanEnd(end, route.End), false
	}
	if _, seen := seenGroups[route.GroupID]; seen {
		return nil, nil, true
	}
	seenGroups[route.GroupID] = struct{}{}
	return start, end, false
}

func (s *ShardStore) scanPartitionOnlyKeyGroupsAt(
	ctx context.Context,
	out []routedScanKey,
	routes []distribution.Route,
	scanGroups []uint64,
	seenGroups map[uint64]struct{},
	start []byte,
	end []byte,
	limit int,
	ts uint64,
) ([]routedScanKey, error) {
	for _, groupID := range scanGroups {
		if _, seen := seenGroups[groupID]; seen {
			continue
		}
		seenGroups[groupID] = struct{}{}
		route := distribution.Route{GroupID: groupID}
		next, err := s.appendBackupScanRouteKeysAt(ctx, out, routes, route, start, end, limit, ts, true)
		if err != nil {
			return nil, err
		}
		out = next
	}
	return out, nil
}

func (s *ShardStore) appendBackupScanRouteKeysAt(
	ctx context.Context,
	out []routedScanKey,
	routes []distribution.Route,
	route distribution.Route,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
	partitionOnly bool,
) ([]routedScanKey, error) {
	keys, err := s.scanKeyRouteAt(ctx, route, start, end, limit, ts)
	if err != nil {
		return nil, err
	}
	return s.mergeAndTrimRoutedScanKeys(out, routedScanKeys(route, keys, partitionOnly), routes, limit)
}

func routedScanKeys(route distribution.Route, keys [][]byte, partitionOnly bool) []routedScanKey {
	items := make([]routedScanKey, 0, len(keys))
	for _, key := range keys {
		if key == nil {
			continue
		}
		items = append(items, routedScanKey{key: key, route: route, partitionOnly: partitionOnly})
	}
	return items
}

func (s *ShardStore) mergeAndTrimRoutedScanKeys(
	out []routedScanKey,
	keys []routedScanKey,
	routes []distribution.Route,
	limit int,
) ([]routedScanKey, error) {
	if len(keys) == 0 {
		return out, nil
	}
	out = append(out, keys...)
	sort.SliceStable(out, func(i, j int) bool {
		return bytes.Compare(out[i].key, out[j].key) < 0
	})
	write := 0
	for _, item := range out {
		if item.key == nil {
			continue
		}
		if write > 0 && bytes.Equal(out[write-1].key, item.key) {
			preferred, err := s.preferredRoutedScanKey(out[write-1], item, routes)
			if err != nil {
				return nil, err
			}
			out[write-1] = preferred
			continue
		}
		out[write] = item
		write++
	}
	clear(out[write:])
	out = out[:write]
	if len(out) <= limit {
		return out, nil
	}
	clear(out[limit:])
	return out[:limit], nil
}

func (s *ShardStore) preferredRoutedScanKey(current, candidate routedScanKey, routes []distribution.Route) (routedScanKey, error) {
	_, currentOwned, err := s.routeForRoutedKey(current, routes)
	if err != nil {
		return routedScanKey{}, err
	}
	_, candidateOwned, err := s.routeForRoutedKey(candidate, routes)
	if err != nil {
		return routedScanKey{}, err
	}
	if candidateOwned && !currentOwned {
		return candidate, nil
	}
	return current, nil
}

func lastRoutedScanKey(keys []routedScanKey) []byte {
	for i := len(keys) - 1; i >= 0; i-- {
		if keys[i].key != nil {
			return keys[i].key
		}
	}
	return nil
}

func routeContainsKey(route distribution.Route, key []byte) bool {
	if bytes.Compare(key, route.Start) < 0 {
		return false
	}
	return route.End == nil || bytes.Compare(key, route.End) < 0
}
