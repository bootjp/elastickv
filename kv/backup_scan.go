package kv

import (
	"bytes"
	"context"

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

func NewBackupScanner(st *ShardStore, start []byte, end []byte, ts uint64, pageSize int) BackupScanner {
	if pageSize <= 0 {
		pageSize = defaultBackupScanPageSize
	}
	var routes []distribution.Route
	var clampToRoutes bool
	if st != nil {
		routes, clampToRoutes = st.routesForScan(start, end)
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
			return nil, false, nil
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
	keys, err := s.store.scanKeyRoutesAt(ctx, s.routes, s.cursor, s.end, s.pageSize, s.ts, s.clampToRoutes)
	if err != nil {
		return err
	}
	s.page = s.page[:0]
	for _, key := range keys {
		val, err := s.getAtCapturedRoute(ctx, key)
		if errors.Is(err, store.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		s.page = append(s.page, &store.KVPair{Key: bytes.Clone(key), Value: bytes.Clone(val)})
	}
	s.index = 0
	if len(keys) == 0 {
		s.exhausted = true
		return nil
	}
	last := lastScanKey(keys)
	if last == nil {
		s.exhausted = true
		return nil
	}
	s.cursor = nextScanCursor(last)
	return nil
}

func (s *backupScanner) getAtCapturedRoute(ctx context.Context, key []byte) ([]byte, error) {
	normalized := routeKey(key)
	for _, route := range s.routes {
		if routeContainsKey(route, normalized) {
			return s.store.getRouteAt(ctx, route, key, s.ts)
		}
	}
	return nil, store.ErrKeyNotFound
}

func routeContainsKey(route distribution.Route, key []byte) bool {
	if bytes.Compare(key, route.Start) < 0 {
		return false
	}
	return route.End == nil || bytes.Compare(key, route.End) < 0
}
