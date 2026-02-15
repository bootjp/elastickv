package kv

import (
	"context"
	"sync"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

const maxLatestCommitTSConcurrency = 16

// MaxLatestCommitTS returns the maximum commit timestamp for the provided keys.
//
// Missing keys are ignored. If any LatestCommitTS lookup returns an error, the
// error is returned to the caller.
func MaxLatestCommitTS(ctx context.Context, st store.MVCCStore, keys [][]byte) (uint64, error) {
	if st == nil || len(keys) == 0 {
		return 0, nil
	}

	uniq := uniqueKeys(keys)
	if len(uniq) == 0 {
		return 0, nil
	}

	// Avoid goroutine overhead for tiny inputs.
	if len(uniq) == 1 {
		return maxLatestCommitTSSequential(ctx, st, uniq)
	}

	limit := maxLatestCommitTSConcurrency
	if limit < 1 {
		limit = 1
	}
	if limit > len(uniq) {
		limit = len(uniq)
	}

	return maxLatestCommitTSParallel(ctx, st, uniq, limit)
}

func uniqueKeys(keys [][]byte) [][]byte {
	seen := make(map[string]struct{}, len(keys))
	uniq := make([][]byte, 0, len(keys))
	for _, key := range keys {
		if len(key) == 0 {
			continue
		}
		k := string(key)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		uniq = append(uniq, key)
	}
	return uniq
}

func maxLatestCommitTSSequential(ctx context.Context, st store.MVCCStore, keys [][]byte) (uint64, error) {
	var maxTS uint64
	for _, key := range keys {
		ts, exists, err := st.LatestCommitTS(ctx, key)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		if !exists {
			continue
		}
		if ts > maxTS {
			maxTS = ts
		}
	}
	return maxTS, nil
}

func maxLatestCommitTSParallel(ctx context.Context, st store.MVCCStore, keys [][]byte, limit int) (uint64, error) {
	eg, egctx := errgroup.WithContext(ctx)
	eg.SetLimit(limit)

	var mu sync.Mutex
	var maxTS uint64
	for i := range keys {
		key := keys[i]
		eg.Go(func() error {
			ts, exists, err := st.LatestCommitTS(egctx, key)
			if err != nil {
				return errors.WithStack(err)
			}
			if !exists {
				return nil
			}
			mu.Lock()
			if ts > maxTS {
				maxTS = ts
			}
			mu.Unlock()
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return 0, err
	}
	return maxTS, nil
}
