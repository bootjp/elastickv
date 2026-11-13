package adapter

import (
	"context"
	"log/slog"
	"sync"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

type s3BlobGCService struct {
	mu             sync.Mutex
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	started        bool
	workersHealthy bool
	generation     uint64
}

type s3LocalStoreLister interface {
	LocalStores() []store.MVCCStore
}

// StartBlobGC starts one sweeper and one orphan scanner for every local
// physical store. It runs before the S3 listener opens; only a fully
// configured service opens the local readiness gate.
func (s *S3Server) StartBlobGC(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("s3 blob gc requires a context")
	}
	if s.store == nil || s.coordinator == nil {
		return errors.New("s3 bucket cleanup recovery requires a store and coordinator")
	}
	s.blobGC.mu.Lock()
	defer s.blobGC.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	if s.blobGC.started {
		return nil
	}
	var sweepers []*s3keys.ChunkBlobSweeper
	var orphans []*s3keys.ChunkBlobOrphanScanner
	if s.blobOffloadEnabled {
		var err error
		sweepers, orphans, err = s.prepareBlobGCWorkers()
		if err != nil {
			return err
		}
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.blobGC.cancel = cancel
	s.blobGC.started = true
	s.blobGC.workersHealthy = true
	s.blobGC.generation++
	generation := s.blobGC.generation
	s.launchBlobGCWorkers(runCtx, generation, sweepers, orphans)
	s.blobGC.wg.Add(1)
	go func() {
		defer s.blobGC.wg.Done()
		s.runBucketCleanupRecovery(runCtx, generation)
	}()
	context.AfterFunc(runCtx, func() {
		s.blobGC.mu.Lock()
		defer s.blobGC.mu.Unlock()
		if s.blobGC.generation == generation {
			s.blobOffloadGCReady.Store(false)
		}
	})
	return nil
}

func (s *S3Server) prepareBlobGCWorkers() ([]*s3keys.ChunkBlobSweeper, []*s3keys.ChunkBlobOrphanScanner, error) {
	clock := s.clock()
	if clock == nil {
		return nil, nil, errors.New("s3 blob gc requires a coordinator clock")
	}
	locals, err := s.chunkBlobGCLocalStores()
	if err != nil {
		return nil, nil, err
	}
	return s.buildBlobGCWorkers(locals, clock.Next)
}

func (s *S3Server) setBlobGCReadyIfCurrent(ctx context.Context, generation uint64, ready bool) {
	s.blobGC.mu.Lock()
	defer s.blobGC.mu.Unlock()
	if s.blobGC.generation == generation && ctx.Err() == nil {
		s.blobOffloadGCReady.Store(ready && s.blobOffloadEnabled && s.blobGC.workersHealthy)
	}
}

func (s *S3Server) buildBlobGCWorkers(locals []store.ChunkBlobLocalStore, nowTS func() uint64) ([]*s3keys.ChunkBlobSweeper, []*s3keys.ChunkBlobOrphanScanner, error) {
	sweepers := make([]*s3keys.ChunkBlobSweeper, 0, len(locals))
	orphans := make([]*s3keys.ChunkBlobOrphanScanner, 0, len(locals))
	for _, local := range locals {
		sweeper, sweepErr := s3keys.NewChunkBlobSweeper(s3keys.ChunkBlobSweeperOptions{
			Store: s, Local: local, NowTS: nowTS,
		})
		if sweepErr != nil {
			return nil, nil, errors.WithStack(sweepErr)
		}
		orphan, orphanErr := s3keys.NewChunkBlobOrphanScanner(s3keys.ChunkBlobOrphanScannerOptions{
			Store: s, Local: local, NowTS: nowTS,
		})
		if orphanErr != nil {
			return nil, nil, errors.WithStack(orphanErr)
		}
		sweepers = append(sweepers, sweeper)
		orphans = append(orphans, orphan)
	}
	return sweepers, orphans, nil
}

func (s *S3Server) launchBlobGCWorkers(runCtx context.Context, generation uint64, sweepers []*s3keys.ChunkBlobSweeper, orphans []*s3keys.ChunkBlobOrphanScanner) {
	for _, sweeper := range sweepers {
		s.blobGC.wg.Add(1)
		go func() {
			defer s.blobGC.wg.Done()
			s.blobGCWorkerResult(runCtx, generation, "sweeper", sweeper.Run(runCtx))
		}()
	}
	for _, orphan := range orphans {
		s.blobGC.wg.Add(1)
		go func() {
			defer s.blobGC.wg.Done()
			s.blobGCWorkerResult(runCtx, generation, "orphan scanner", orphan.Run(runCtx))
		}()
	}
}

func (s *S3Server) blobGCWorkerResult(ctx context.Context, generation uint64, worker string, err error) {
	if ctx.Err() != nil {
		return
	}
	s.blobGC.mu.Lock()
	if s.blobGC.generation == generation {
		s.blobGC.workersHealthy = false
		s.blobOffloadGCReady.Store(false)
	}
	s.blobGC.mu.Unlock()
	slog.ErrorContext(ctx, "s3 blob gc worker stopped", "worker", worker, "err", err)
}

func (s *S3Server) chunkBlobGCLocalStores() ([]store.ChunkBlobLocalStore, error) {
	var physical []store.MVCCStore
	if lister, ok := s.store.(s3LocalStoreLister); ok {
		physical = lister.LocalStores()
	} else if s.store != nil {
		physical = []store.MVCCStore{s.store}
	}
	if len(physical) == 0 {
		return nil, errors.New("s3 blob gc requires a local store")
	}
	locals := make([]store.ChunkBlobLocalStore, 0, len(physical))
	for _, st := range physical {
		local, err := store.NewChunkBlobLocalStore(st)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		locals = append(locals, local)
	}
	return locals, nil
}

func (s *S3Server) stopBlobGC() {
	s.blobGC.mu.Lock()
	cancel := s.blobGC.cancel
	s.blobGC.cancel = nil
	s.blobGC.started = false
	s.blobGC.workersHealthy = false
	s.blobGC.generation++
	s.blobOffloadGCReady.Store(false)
	s.blobGC.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	s.blobGC.wg.Wait()
}
