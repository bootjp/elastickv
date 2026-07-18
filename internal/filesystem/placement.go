package filesystem

import (
	"context"
	"slices"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

type PlacementStats struct {
	FilesByGroup     map[uint64]uint64
	MultiShardFiles  uint64
	MultiShardInodes []uint64
	MoveInflight     uint64
	OpenHandleLeases uint64
	OrphanedInodes   uint64
}

type filePlacementGroupScanner interface {
	FilesystemGroupIDs() []uint64
	ScanGroupAt(context.Context, uint64, []byte, []byte, int, uint64) ([]*store.KVPair, error)
}

// ListFilePlacementStats computes operator-facing placement state at one
// fenced snapshot and publishes it to the configured observer.
func (s *Service) ListFilePlacementStats(ctx context.Context) (PlacementStats, error) {
	resolver, ok := s.store.(filePlacementResolver)
	if !ok {
		return PlacementStats{}, ErrPlacementRequired
	}
	ts, err := s.readTS(ctx)
	if err != nil {
		return PlacementStats{}, err
	}
	stats := PlacementStats{FilesByGroup: make(map[uint64]uint64)}
	homes, err := s.scanPlacementHomes(ctx, ts)
	if err != nil {
		return PlacementStats{}, err
	}
	metas, err := s.scanPlacementInodes(ctx, ts, resolver, &stats)
	if err != nil {
		return PlacementStats{}, err
	}
	chunkGroups, err := s.scanPlacementChunkGroups(ctx, ts, resolver)
	if err != nil {
		return PlacementStats{}, err
	}
	addMultiShardPlacementStats(&stats, metas, homes, chunkGroups)
	stats.OpenHandleLeases, err = s.countVisiblePrefix(ctx, fskeys.RefAllPrefix(), ts)
	if err != nil {
		return PlacementStats{}, errors.Wrap(err, "filesystem scan placement leases")
	}
	stats.MoveInflight, err = s.countInflightMoveJobs(ctx, ts)
	if err != nil {
		return PlacementStats{}, err
	}
	if s.observer != nil {
		s.observer.ObservePlacementStats(stats)
	}
	return stats, nil
}

func (s *Service) scanPlacementHomes(ctx context.Context, ts uint64) (map[uint64]Home, error) {
	homes := make(map[uint64]Home)
	if err := s.scanVisiblePrefix(ctx, fskeys.HomeAllPrefix(), ts, func(pair *store.KVPair) error {
		inode, ok := fskeys.HomeInodeFromKey(pair.Key)
		if !ok {
			return nil
		}
		home, err := decodeJSON[Home](pair.Value)
		if err != nil {
			return err
		}
		homes[inode] = home
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "filesystem scan placement homes")
	}
	return homes, nil
}

func (s *Service) scanPlacementInodes(
	ctx context.Context,
	ts uint64,
	resolver filePlacementResolver,
	stats *PlacementStats,
) (map[uint64]InodeMeta, error) {
	metas := make(map[uint64]InodeMeta)
	if err := s.scanVisiblePrefix(ctx, fskeys.InodeAllPrefix(), ts, func(pair *store.KVPair) error {
		meta, err := decodeJSON[InodeMeta](pair.Value)
		if err != nil {
			return err
		}
		metas[meta.Inode] = meta
		if meta.Orphaned || meta.Nlink == 0 {
			stats.OrphanedInodes++
		}
		if meta.Type != TypeFile || meta.Orphaned || meta.Nlink == 0 {
			return nil
		}
		if groupID, found := resolver.FilesystemGroupForHome(meta.HomeSlot, meta.Inode); found {
			stats.FilesByGroup[groupID]++
		}
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "filesystem scan placement inodes")
	}
	return metas, nil
}

func (s *Service) scanPlacementChunkGroups(
	ctx context.Context,
	ts uint64,
	resolver filePlacementResolver,
) (map[uint64]map[uint64]struct{}, error) {
	if scanner, ok := s.store.(filePlacementGroupScanner); ok {
		return scanPhysicalPlacementChunkGroups(ctx, scanner, ts)
	}
	chunkGroups := make(map[uint64]map[uint64]struct{})
	if err := s.scanVisiblePrefix(ctx, fskeys.ChunkAllPrefix(), ts, func(pair *store.KVPair) error {
		home, inode, _, ok := fskeys.ChunkPartsFromKey(pair.Key)
		if !ok {
			return nil
		}
		groupID, found := resolver.FilesystemGroupForHome(home, inode)
		if !found {
			return nil
		}
		groups := chunkGroups[inode]
		if groups == nil {
			groups = make(map[uint64]struct{})
			chunkGroups[inode] = groups
		}
		groups[groupID] = struct{}{}
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "filesystem scan placement chunks")
	}
	return chunkGroups, nil
}

func scanPhysicalPlacementChunkGroups(
	ctx context.Context,
	scanner filePlacementGroupScanner,
	ts uint64,
) (map[uint64]map[uint64]struct{}, error) {
	chunkGroups := make(map[uint64]map[uint64]struct{})
	prefix := fskeys.ChunkAllPrefix()
	end := prefixEnd(prefix)
	for _, groupID := range scanner.FilesystemGroupIDs() {
		start := append([]byte(nil), prefix...)
		for {
			page, err := scanner.ScanGroupAt(ctx, groupID, start, end, statFSScanPageSize, ts)
			if err != nil {
				return nil, errors.Wrapf(err, "filesystem scan placement chunks for group %d", groupID)
			}
			for _, pair := range page {
				if pair == nil {
					continue
				}
				_, inode, _, ok := fskeys.ChunkPartsFromKey(pair.Key)
				if !ok {
					continue
				}
				groups := chunkGroups[inode]
				if groups == nil {
					groups = make(map[uint64]struct{})
					chunkGroups[inode] = groups
				}
				groups[groupID] = struct{}{}
			}
			if len(page) < statFSScanPageSize {
				break
			}
			start = scanCursorAfter(page[len(page)-1].Key)
		}
	}
	return chunkGroups, nil
}

func addMultiShardPlacementStats(
	stats *PlacementStats,
	metas map[uint64]InodeMeta,
	homes map[uint64]Home,
	chunkGroups map[uint64]map[uint64]struct{},
) {
	for inode, groups := range chunkGroups {
		meta, found := metas[inode]
		if !found || meta.Orphaned || meta.Nlink == 0 || len(groups) <= 1 {
			continue
		}
		if home, found := homes[inode]; found && home.State == HomeStateMigrating {
			continue
		}
		stats.MultiShardFiles++
		stats.MultiShardInodes = append(stats.MultiShardInodes, inode)
	}
	slices.Sort(stats.MultiShardInodes)
}

func (s *Service) countInflightMoveJobs(ctx context.Context, ts uint64) (uint64, error) {
	var inflight uint64
	if err := s.scanVisiblePrefix(ctx, fskeys.MoveJobAllPrefix(), ts, func(pair *store.KVPair) error {
		job, err := decodeJSON[MoveJob](pair.Value)
		if err != nil {
			return err
		}
		if job.Phase != MovePhaseCompleted {
			inflight++
		}
		return nil
	}); err != nil {
		return 0, errors.Wrap(err, "filesystem scan placement jobs")
	}
	return inflight, nil
}
