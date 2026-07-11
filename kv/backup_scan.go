package kv

import (
	"bytes"
	"context"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
)

const defaultBackupScanPageSize = 1024

// BackupScanner pages through ShardStore.ScanAt without holding store locks
// across pages.
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
	page, err := s.store.scanRoutesAtSorted(ctx, s.routes, s.cursor, s.end, s.pageSize, s.ts, s.clampToRoutes)
	if err != nil {
		return err
	}
	s.page = page
	s.index = 0
	if len(page) == 0 {
		s.exhausted = true
		return nil
	}
	last := page[len(page)-1]
	if last == nil {
		s.exhausted = true
		return nil
	}
	s.cursor = nextScanCursor(last.Key)
	return nil
}
