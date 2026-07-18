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

func NewBackupScanner(st *ShardStore, start []byte, end []byte, ts uint64, pageSize int) BackupScanner {
	if pageSize <= 0 {
		pageSize = defaultBackupScanPageSize
	}
	var routes []distribution.Route
	var clampToRoutes bool
	if st != nil {
		routes, clampToRoutes = st.routesForForwardScan(start, end)
		routes = append([]distribution.Route(nil), routes...)
	}
	return &backupScanner{
		store:         st,
		routes:        routes,
		clampToRoutes: clampToRoutes,
		cursor:        bytes.Clone(start),
		end:           bytes.Clone(end),
		ts:            ts,
		pageSize:      pageSize,
	}
}

func (s *ShardStore) NewBackupScanner(start []byte, end []byte, ts uint64, pageSize int) BackupScanner {
	return NewBackupScanner(s, start, end, ts, pageSize)
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
	key := routeKey(item.key)
	if routeContainsKey(item.route, key) {
		return item.route, true
	}
	for _, route := range s.routes {
		if route.GroupID != item.route.GroupID {
			continue
		}
		if routeContainsKey(route, key) {
			return route, true
		}
	}
	return distribution.Route{}, false
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
		out = mergeAndTrimRoutedScanKeys(out, routedScanKeys(route, keys), limit)
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

func mergeAndTrimRoutedScanKeys(out []routedScanKey, keys []routedScanKey, limit int) []routedScanKey {
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
			out[write-1] = item
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
