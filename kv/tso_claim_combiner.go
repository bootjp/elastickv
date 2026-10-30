package kv

import (
	"context"
	stderrors "errors"
	"time"

	"github.com/cockroachdb/errors"
)

const (
	tsoClaimQueueCapacity = 4 * MaxTSOClaimsPerBatch
	tsoClaimCollectDelay  = 200 * time.Microsecond
	tsoClaimCommitBudget  = 5 * time.Second
)

type tsoClaimCommitter interface {
	commitClaimBatch(context.Context, uint64, []TSOClaimRecord) error
}

type tsoClaimSubmission struct {
	ctx    context.Context
	term   uint64
	proof  TSOClaimProof
	result chan error
}

// This combiner accepts already fixed candidates, not uint64 timestamps. A
// caller must retain the candidate before submission; cancellation never
// authorizes choosing another slot for that ID. It is not yet production-wired.
type tsoClaimCombiner struct {
	issuer tsoClaimCommitter
	queue  chan *tsoClaimSubmission
	done   chan struct{}
	cancel context.CancelFunc
}

func newTSOClaimCombiner(ctx context.Context, issuer tsoClaimCommitter) *tsoClaimCombiner {
	ctx, cancel := context.WithCancel(nonNilTSOContext(ctx))
	c := &tsoClaimCombiner{
		issuer: issuer, queue: make(chan *tsoClaimSubmission, tsoClaimQueueCapacity),
		done: make(chan struct{}), cancel: cancel,
	}
	go c.run(ctx)
	return c
}

func (c *tsoClaimCombiner) close() {
	c.cancel()
	<-c.done
}

func (c *tsoClaimCombiner) claim(ctx context.Context, term uint64, proof TSOClaimProof) (TSOClaimProof, error) {
	ctx = nonNilTSOContext(ctx)
	if term == 0 || !proof.matches(proof.Record.Binding, claimRolePhase(proof.Record.Binding.Role)) {
		return TSOClaimProof{}, errors.WithStack(ErrTSOClaimInvalid)
	}
	if err := ctx.Err(); err != nil {
		return TSOClaimProof{}, errors.WithStack(err)
	}
	request := &tsoClaimSubmission{ctx: ctx, term: term, proof: proof, result: make(chan error, 1)}
	select {
	case <-c.done:
		return TSOClaimProof{}, errors.WithStack(ErrTSOClaimClosed)
	case c.queue <- request:
	default:
		return TSOClaimProof{}, errors.WithStack(ErrTSOClaimCapacity)
	}
	return c.await(ctx, request)
}

func (c *tsoClaimCombiner) await(ctx context.Context, request *tsoClaimSubmission) (TSOClaimProof, error) {
	select {
	case err := <-request.result:
		if err != nil {
			return TSOClaimProof{}, err
		}
		return request.proof, nil
	case <-ctx.Done():
		return TSOClaimProof{}, errors.WithStack(stderrors.Join(ErrTSOClaimUnknown, ctx.Err()))
	case <-c.done:
		return TSOClaimProof{}, errors.WithStack(ErrTSOClaimUnknown)
	}
}

func claimRolePhase(role TSOClaimRole) TSOClaimPhase {
	if role != TSOClaimCommit {
		return TSOClaimRead
	}
	return TSOClaimOnePhase
}

func (c *tsoClaimCombiner) run(ctx context.Context) {
	defer close(c.done)
	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case first := <-c.queue:
			batch := c.collect(ctx, first)
			c.commit(ctx, batch)
		}
	}
}

func (c *tsoClaimCombiner) collect(ctx context.Context, first *tsoClaimSubmission) []*tsoClaimSubmission {
	batch := make([]*tsoClaimSubmission, 0, MaxTSOClaimsPerBatch)
	batch = append(batch, first)
	deadline := time.Now().Add(tsoClaimCollectDelay)
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	for len(batch) < MaxTSOClaimsPerBatch {
		if ctx.Err() != nil || !time.Now().Before(deadline) {
			return batch
		}
		select {
		case <-ctx.Done():
			return batch
		case <-timer.C:
			return batch
		case request := <-c.queue:
			batch = append(batch, request)
		}
	}
	return batch
}

func (c *tsoClaimCombiner) commit(ctx context.Context, batch []*tsoClaimSubmission) {
	term, records, active := collectTSOClaimRecords(batch)
	if len(active) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, tsoClaimCommitBudget)
	defer cancel()
	err := c.issuer.commitClaimBatch(ctx, term, records)
	for _, request := range active {
		request.result <- err
	}
}

func collectTSOClaimRecords(batch []*tsoClaimSubmission) (uint64, []TSOClaimRecord, []*tsoClaimSubmission) {
	var term uint64
	records := make([]TSOClaimRecord, 0, len(batch))
	active := make([]*tsoClaimSubmission, 0, len(batch))
	seen := make(map[TSOClaimID]TSOClaimRecord, len(batch))
	for _, request := range batch {
		if err := request.ctx.Err(); err != nil {
			request.result <- errors.WithStack(stderrors.Join(ErrTSOClaimUnknown, err))
			continue
		}
		if term == 0 {
			term = request.term
		}
		if request.term != term {
			request.result <- errors.WithStack(ErrTSOClaimClosed)
			continue
		}
		r := request.proof.Record
		if prior, exists := seen[r.ID]; exists {
			if prior != r {
				request.result <- errors.WithStack(ErrTSOClaimConflict)
				continue
			}
		} else {
			records = append(records, r)
			seen[r.ID] = r
		}
		active = append(active, request)
	}
	return term, records, active
}
