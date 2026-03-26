package kv

import (
	"sync"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

type TransactionManager struct {
	raft *raft.Raft

	mu          sync.Mutex
	rawPending  []*rawCommitItem
	rawFlushing bool
	closeCh     chan struct{}
	closeOnce   sync.Once
}

type rawCommitItem struct {
	reqs []*pb.Request
	done chan rawCommitResult
}

type rawCommitResult struct {
	commitIndex uint64
	err         error
}

const maxRawBatchRequests = 64
const maxRawPendingItems = 4096

// maxMarshaledCommandSize is the upper bound on a marshaled Raft command.
// Protects against integer overflow when computing allocation sizes.
const maxMarshaledCommandSize = 64 * 1024 * 1024 // 64 MiB

var rawBatchWindow = 500 * time.Microsecond

var errRawQueueFull = errors.New("raw commit queue is full; try again later")

func NewTransaction(raft *raft.Raft) *TransactionManager {
	return &TransactionManager{
		raft:    raft,
		closeCh: make(chan struct{}),
	}
}

var errShuttingDown = errors.New("transaction manager is shutting down")

// Close signals the TransactionManager to stop and drains any pending raw
// commit items, sending each an error so callers are not blocked forever.
func (t *TransactionManager) Close() {
	t.closeOnce.Do(func() {
		close(t.closeCh)

		t.mu.Lock()
		pending := t.rawPending
		t.rawPending = nil
		t.rawFlushing = false
		t.mu.Unlock()

		for _, item := range pending {
			item.done <- rawCommitResult{err: errShuttingDown}
		}
	})
}

type Transactional interface {
	Commit(reqs []*pb.Request) (*TransactionResponse, error)
	Abort(reqs []*pb.Request) (*TransactionResponse, error)
}

type TransactionResponse struct {
	CommitIndex uint64
}

func marshalRaftCommand(reqs []*pb.Request) ([]byte, error) {
	if len(reqs) == 1 {
		b, err := proto.Marshal(reqs[0])
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if len(b) >= maxMarshaledCommandSize {
			return nil, errors.New("marshaled request too large")
		}
		return prependByte(raftEncodeSingle, b), nil
	}
	b, err := proto.Marshal(&pb.RaftCommand{Requests: reqs})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(b) >= maxMarshaledCommandSize {
		return nil, errors.New("marshaled request batch too large")
	}
	return prependByte(raftEncodeBatch, b), nil
}

// prependByte returns a new slice with prefix followed by data.
func prependByte(prefix byte, data []byte) []byte {
	out := make([]byte, len(data)+1)
	out[0] = prefix
	copy(out[1:], data)
	return out
}

// applyRequests submits one raft command and returns per-request FSM results.
// HashiCorp Raft delivers FSM responses via ApplyFuture.Response(), not Error(),
// so we must inspect the response to avoid silently treating failed writes as
// successes.
func applyRequests(r *raft.Raft, reqs []*pb.Request) (uint64, []error, error) {
	b, err := marshalRaftCommand(reqs)
	if err != nil {
		return 0, nil, errors.WithStack(err)
	}

	af := r.Apply(b, time.Second)
	if err := af.Error(); err != nil {
		return 0, nil, errors.WithStack(err)
	}

	switch resp := af.Response().(type) {
	case nil:
		return af.Index(), make([]error, len(reqs)), nil
	case error:
		if len(reqs) != 1 {
			return 0, nil, errors.WithStack(resp)
		}
		return af.Index(), []error{errors.WithStack(resp)}, nil
	case *fsmApplyResponse:
		if len(resp.results) != len(reqs) {
			return 0, nil, errors.WithStack(errors.Newf("unexpected apply response size: got %d want %d", len(resp.results), len(reqs)))
		}
		return af.Index(), resp.results, nil
	default:
		return 0, nil, errors.WithStack(errors.Newf("unexpected apply response type %T", resp))
	}
}

func (t *TransactionManager) Commit(reqs []*pb.Request) (*TransactionResponse, error) {
	if len(reqs) == 0 {
		return &TransactionResponse{}, nil
	}
	if hasTxnRequests(reqs) {
		return t.commitSequential(reqs)
	}
	return t.commitRaw(reqs)
}

func hasTxnRequests(reqs []*pb.Request) bool {
	for _, req := range reqs {
		if req != nil && req.IsTxn {
			return true
		}
	}
	return false
}

func (t *TransactionManager) commitSequential(reqs []*pb.Request) (*TransactionResponse, error) {
	commitIndex, err := func() (uint64, error) {
		commitIndex := uint64(0)
		for _, req := range reqs {
			idx, results, err := applyRequests(t.raft, []*pb.Request{req})
			if err != nil {
				return 0, err
			}
			if len(results) > 0 && results[0] != nil {
				return 0, results[0]
			}
			commitIndex = idx
		}

		return commitIndex, nil
	}()

	if err != nil {
		// Only attempt transactional cleanup for transactional batches. Raw request
		// batches may partially succeed across shards by design. One-phase
		// transactional requests do not leave intents behind, so they do not need
		// abort cleanup on failure.
		if needsTxnCleanup(reqs) {
			_, _err := t.Abort(reqs)
			if _err != nil {
				return nil, errors.WithStack(errors.CombineErrors(err, _err))
			}
		}
		return nil, errors.WithStack(err)
	}

	return &TransactionResponse{
		CommitIndex: commitIndex,
	}, nil
}

func needsTxnCleanup(reqs []*pb.Request) bool {
	for _, req := range reqs {
		if req != nil && req.IsTxn && req.Phase != pb.Phase_NONE {
			return true
		}
	}
	return false
}

func (t *TransactionManager) commitRaw(reqs []*pb.Request) (*TransactionResponse, error) {
	item := &rawCommitItem{
		reqs: reqs,
		done: make(chan rawCommitResult, 1),
	}

	shouldFlush := false
	t.mu.Lock()
	select {
	case <-t.closeCh:
		t.mu.Unlock()
		return nil, errShuttingDown
	default:
	}
	if len(t.rawPending) >= maxRawPendingItems {
		t.mu.Unlock()
		return nil, errRawQueueFull
	}
	t.rawPending = append(t.rawPending, item)
	if !t.rawFlushing {
		t.rawFlushing = true
		shouldFlush = true
	}
	t.mu.Unlock()

	if shouldFlush {
		go t.flushRawPending()
	}

	res := <-item.done
	if res.err != nil {
		return nil, res.err
	}
	return &TransactionResponse{CommitIndex: res.commitIndex}, nil
}

func (t *TransactionManager) flushRawPending() {
	timer := time.NewTimer(rawBatchWindow)
	select {
	case <-timer.C:
	case <-t.closeCh:
		timer.Stop()
		return
	}

	for {
		select {
		case <-t.closeCh:
			return
		default:
		}

		batch := t.takeRawBatch()
		if len(batch) == 0 {
			t.mu.Lock()
			if len(t.rawPending) > 0 {
				t.mu.Unlock()
				continue
			}
			t.rawFlushing = false
			t.mu.Unlock()
			return
		}

		t.applyRawBatch(batch)

		t.mu.Lock()
		if len(t.rawPending) == 0 {
			t.rawFlushing = false
			t.mu.Unlock()
			return
		}
		t.mu.Unlock()
	}
}

func (t *TransactionManager) takeRawBatch() []*rawCommitItem {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.rawPending) == 0 {
		return nil
	}

	count := 0
	n := 0
	for n < len(t.rawPending) {
		next := count + len(t.rawPending[n].reqs)
		if n > 0 && next > maxRawBatchRequests {
			break
		}
		count = next
		n++
	}

	batch := append([]*rawCommitItem(nil), t.rawPending[:n]...)
	t.rawPending = t.rawPending[n:]
	return batch
}

func (t *TransactionManager) applyRawBatch(batch []*rawCommitItem) {
	reqs := make([]*pb.Request, 0, maxRawBatchRequests)
	offsets := make([]int, 0, len(batch)+1)
	for _, item := range batch {
		offsets = append(offsets, len(reqs))
		reqs = append(reqs, item.reqs...)
	}
	offsets = append(offsets, len(reqs))

	idx, results, err := applyRequests(t.raft, reqs)
	if err != nil {
		for _, item := range batch {
			item.done <- rawCommitResult{err: err}
		}
		return
	}

	for i, item := range batch {
		itemErr := combineApplyErrors(results[offsets[i]:offsets[i+1]])
		if itemErr != nil {
			item.done <- rawCommitResult{err: itemErr}
			continue
		}
		item.done <- rawCommitResult{commitIndex: idx}
	}
}

func combineApplyErrors(errs []error) error {
	var combined error
	for _, err := range errs {
		if err == nil {
			continue
		}
		combined = errors.CombineErrors(combined, err)
	}
	return errors.WithStack(combined)
}

func (t *TransactionManager) Abort(reqs []*pb.Request) (*TransactionResponse, error) {
	var abortReqs []*pb.Request
	for _, req := range reqs {
		if abortReq := abortRequestFor(req); abortReq != nil {
			abortReqs = append(abortReqs, abortReq)
		}
	}

	var commitIndex uint64
	for _, req := range abortReqs {
		idx, results, err := applyRequests(t.raft, []*pb.Request{req})
		if err != nil {
			return nil, err
		}
		if len(results) > 0 && results[0] != nil {
			return nil, results[0]
		}
		commitIndex = idx
	}

	return &TransactionResponse{
		CommitIndex: commitIndex,
	}, nil
}

func abortRequestFor(req *pb.Request) *pb.Request {
	if req == nil || !req.IsTxn || req.Phase == pb.Phase_NONE {
		return nil
	}
	meta, muts, err := extractTxnMeta(req.Mutations)
	if err != nil {
		// Best-effort cleanup; skip requests we can't interpret.
		return nil
	}
	startTS := req.Ts
	abortTS := abortTSFrom(startTS, startTS)
	if abortTS <= startTS {
		// Overflow: can't choose an abort timestamp strictly greater than startTS.
		return nil
	}
	meta.CommitTS = abortTS

	return &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_ABORT,
		Ts:    startTS,
		Mutations: append([]*pb.Mutation{
			{
				Op:    pb.Op_PUT,
				Key:   []byte(txnMetaPrefix),
				Value: EncodeTxnMeta(meta),
			},
		}, muts...),
	}
}

func extractTxnMeta(muts []*pb.Mutation) (TxnMeta, []*pb.Mutation, error) {
	if len(muts) == 0 || muts[0] == nil || !isTxnMetaKey(muts[0].Key) {
		return TxnMeta{}, nil, errors.WithStack(ErrTxnMetaMissing)
	}
	meta, err := DecodeTxnMeta(muts[0].Value)
	if err != nil {
		return TxnMeta{}, nil, errors.WithStack(errors.Wrap(err, "decode txn meta"))
	}
	return meta, muts[1:], nil
}
