package loadtest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	pb "github.com/bootjp/elastickv/proto"
)

var ErrMismatch = errors.New("loadtest: value mismatch")

func (r *Runner) key(kind string, id uint64) []byte {
	side := keyDomains[id%uint64(len(keyDomains))]
	return fmt.Appendf(nil, "%s/%s/%s/%016x", side, r.config.Prefix, kind, id)
}

func (r *Runner) value(kind string, id uint64) []byte {
	v := bytes.Repeat([]byte{'v'}, r.config.ValueBytes)
	copy(v, fmt.Sprintf("%s/%s/%016x", r.config.Prefix, kind, id))
	return v
}

func (r *Runner) put(ctx context.Context, client pb.RawKVClient, kind string, id uint64) error {
	ctx, cancel := context.WithTimeout(ctx, r.config.RequestTimeout)
	defer cancel()
	response, err := client.RawPut(ctx, &pb.RawPutRequest{Key: r.key(kind, id), Value: r.value(kind, id)})
	if err == nil && !response.GetSuccess() {
		return errors.New("loadtest: RawPut unsuccessful")
	}
	return err
}

func (r *Runner) get(ctx context.Context, client pb.RawKVClient, kind string, id uint64) error {
	ctx, cancel := context.WithTimeout(ctx, r.config.RequestTimeout)
	defer cancel()
	response, err := client.RawGet(ctx, &pb.RawGetRequest{Key: r.key(kind, id)})
	if err != nil {
		return err
	}
	if !response.GetExists() || !bytes.Equal(response.GetValue(), r.value(kind, id)) {
		return fmt.Errorf("%w: %s", ErrMismatch, r.key(kind, id))
	}
	return nil
}

// Seed writes only the immutable read dataset. Measured writes use unique keys.
func (r *Runner) Seed(ctx context.Context) error {
	if r.seeded || r.started {
		return errors.New("loadtest: prefix already initialized")
	}
	probeCtx, cancel := context.WithTimeout(ctx, r.config.RequestTimeout)
	response, err := r.clients[0].RawGet(probeCtx, &pb.RawGetRequest{Key: r.key("seed", 0)})
	cancel()
	if err != nil {
		return fmt.Errorf("probe unused prefix: %w", err)
	}
	if response.GetExists() {
		return errors.New("loadtest: prefix exists; choose an unused prefix")
	}
	for id := range uint64(r.config.Keys) {
		client := r.clients[int(id%uint64(len(r.clients)))]
		if err := r.put(ctx, client, "seed", id); err != nil {
			return fmt.Errorf("seed %d: %w", id, err)
		}
	}
	r.seeded = true
	return nil
}

type workerResult struct {
	read, write, verification, logical, logicalRead, logicalWrite samples
	acknowledged                                                  []uint64
}

func (r *Runner) execute(ctx context.Context, job request, result *workerResult) {
	client := r.clients[int((job.id/uint64(len(keyDomains)))%uint64(len(r.clients)))]
	started := time.Now()
	var err error
	if (job.id*operationStride)%operationCycle < uint64(r.config.ReadPercent) {
		err = r.get(ctx, client, "seed", job.id%uint64(r.config.Keys))
		result.read.record(time.Since(started), err)
	} else {
		err = r.put(ctx, client, "write", job.id)
		result.write.record(time.Since(started), err)
		if err == nil {
			result.acknowledged = append(result.acknowledged, job.id)
			if r.config.VerifyWrites {
				verifyStart := time.Now()
				err = r.get(ctx, client, "write", job.id)
				result.verification.record(time.Since(verifyStart), err)
			}
		}
	}
	// Includes scheduler delay and optional write verification, unlike RPC latency.
	elapsed := time.Since(job.scheduled)
	result.logical.record(elapsed, err)
	if (job.id*operationStride)%operationCycle < uint64(r.config.ReadPercent) {
		result.logicalRead.record(elapsed, err)
	} else {
		result.logicalWrite.record(elapsed, err)
	}
}

// Audit reads every acknowledged unique write through every supplied endpoint.
// No operation is retried; an ambiguous mutation is never replayed.
func (r *Runner) Audit(ctx context.Context) Metric {
	jobs := make(chan uint64)
	results := make(chan samples, r.config.Concurrency)
	for range r.config.Concurrency {
		go func() {
			var result samples
			for id := range jobs {
				if ctx.Err() != nil {
					break
				}
				for _, client := range r.clients {
					if ctx.Err() != nil {
						break
					}
					started := time.Now()
					err := r.get(ctx, client, "write", id)
					result.record(time.Since(started), err)
				}
			}
			results <- result
		}()
	}
auditJobs:
	for _, id := range r.acknowledged {
		select {
		case <-ctx.Done():
			break auditJobs
		case jobs <- id:
		}
	}
	close(jobs)
	var total samples
	for range r.config.Concurrency {
		total.merge(<-results)
	}
	return total.metric()
}
