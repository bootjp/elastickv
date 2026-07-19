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
	clampToRoutes bool
	start         []byte
	end           []byte
}

type backupScanner struct {
	store         *ShardStore
	routes        []distribution.Route
	clampToRoutes bool
	end           []byte
	ts            uint64
	pageSize      int
	cursor        []byte
	page          []*store.KVPair
	index         int
	closed        bool
	exhausted     bool
}

type routedScanKey struct {
	key   []byte
	route distribution.Route
}

type backupKeyScanner struct {
	store         *ShardStore
	routes        []distribution.Route
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
	return BackupRouteSnapshot{routes: cloneBackupRoutes(routes)}, nil
}

// NewBackupScannerAtSnapshot creates a value scanner from a captured route view.
func NewBackupScannerAtSnapshot(st *ShardStore, snapshot BackupRouteSnapshot, ts uint64, pageSize int) BackupScanner {
	if pageSize <= 0 {
		pageSize = defaultBackupScanPageSize
	}
	snapshot = cloneBackupRouteSnapshot(snapshot)
	return &backupScanner{
		store:         st,
		routes:        snapshot.routes,
		clampToRoutes: snapshot.clampToRoutes,
		cursor:        snapshot.start,
		end:           snapshot.end,
		ts:            ts,
		pageSize:      pageSize,
	}
}

func (s *ShardStore) NewBackupScanner(start []byte, end []byte, ts uint64, pageSize int) BackupScanner {
	return NewBackupScanner(s, start, end, ts, pageSize)
}

func (s *ShardStore) NewBackupScannerAtSnapshot(snapshot BackupRouteSnapshot, ts uint64, pageSize int) BackupScanner {
	return NewBackupScannerAtSnapshot(s, snapshot, ts, pageSize)
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
	keys, err := s.store.scanKeyRoutesWithSourceAt(ctx, s.routes, s.cursor, s.end, s.pageSize, s.ts, s.clampToRoutes)
	if err != nil {
		return err
	}
	s.page = s.page[:0]
	for _, item := range keys {
		if _, ok := routeForRoutedKey(item, s.routes); ok {
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
	keys, err := s.store.scanKeyRoutesWithSourceAt(ctx, s.routes, s.cursor, s.end, s.pageSize, s.ts, s.clampToRoutes)
	if err != nil {
		return err
	}
	s.page = s.page[:0]
	for _, item := range keys {
		route, ok := s.materializeRouteForKey(item)
		if !ok {
			continue
		}
		val, err := s.store.getRouteAt(ctx, route, item.key, s.ts)
		if errors.Is(err, store.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		s.page = append(s.page, &store.KVPair{Key: bytes.Clone(item.key), Value: bytes.Clone(val)})
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

func (s *backupScanner) materializeRouteForKey(item routedScanKey) (distribution.Route, bool) {
	return routeForRoutedKey(item, s.routes)
}

func routeForRoutedKey(item routedScanKey, routes []distribution.Route) (distribution.Route, bool) {
	key := routeKey(item.key)
	if routeContainsKey(item.route, key) {
		return item.route, true
	}
	for _, route := range routes {
		if route.GroupID != item.route.GroupID {
			continue
		}
		if routeContainsKey(route, key) {
			return route, true
		}
	}
	return distribution.Route{}, false
}

func cloneBackupRouteSnapshot(snapshot BackupRouteSnapshot) BackupRouteSnapshot {
	return BackupRouteSnapshot{
		routes:        cloneBackupRoutes(snapshot.routes),
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

func (s *ShardStore) scanKeyRoutesWithSourceAt(
	ctx context.Context,
	routes []distribution.Route,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
	clampToRoutes bool,
) ([]routedScanKey, error) {
	out := make([]routedScanKey, 0)
	seenGroups := make(map[uint64]struct{})
	for _, route := range routes {
		scanStart := start
		scanEnd := end
		if clampToRoutes {
			scanStart = clampScanStart(start, route.Start)
			scanEnd = clampScanEnd(end, route.End)
		} else {
			if _, seen := seenGroups[route.GroupID]; seen {
				continue
			}
			seenGroups[route.GroupID] = struct{}{}
		}

		keys, err := s.scanKeyRouteAt(ctx, route, scanStart, scanEnd, limit, ts)
		if err != nil {
			return nil, err
		}
		out = mergeAndTrimRoutedScanKeys(out, routedScanKeys(route, keys), routes, limit)
		if clampToRoutes && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func routedScanKeys(route distribution.Route, keys [][]byte) []routedScanKey {
	items := make([]routedScanKey, 0, len(keys))
	for _, key := range keys {
		if key == nil {
			continue
		}
		items = append(items, routedScanKey{key: key, route: route})
	}
	return items
}

func mergeAndTrimRoutedScanKeys(
	out []routedScanKey,
	keys []routedScanKey,
	routes []distribution.Route,
	limit int,
) []routedScanKey {
	if len(keys) == 0 {
		return out
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
			out[write-1] = preferredRoutedScanKey(out[write-1], item, routes)
			continue
		}
		out[write] = item
		write++
	}
	clear(out[write:])
	out = out[:write]
	if len(out) <= limit {
		return out
	}
	clear(out[limit:])
	return out[:limit]
}

func preferredRoutedScanKey(current, candidate routedScanKey, routes []distribution.Route) routedScanKey {
	_, currentOwned := routeForRoutedKey(current, routes)
	_, candidateOwned := routeForRoutedKey(candidate, routes)
	if candidateOwned && !currentOwned {
		return candidate
	}
	return current
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
