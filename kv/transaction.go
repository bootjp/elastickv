package kv

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

type TransactionManager struct {
	proposer raftengine.Proposer

	versionCASAdmission VersionCASAdmission
	proposalObserver    ProposalObserver
	timestampAdmission  atomic.Pointer[transactionTimestampAdmission]
	migrationAdmission  atomic.Pointer[transactionMigrationAdmission]

	mu          sync.Mutex
	rawPending  []*rawCommitItem
	rawFlushing bool
	closeCh     chan struct{}
	closeOnce   sync.Once
}

type rawCommitItem struct {
	reqs         []*pb.Request
	done         chan rawCommitResult
	encodedBytes int
}

type rawCommitResult struct {
	commitIndex uint64
	err         error
}

// The single flusher waits for each proposal to apply before taking the next
// batch. A larger batch drains more of an existing backlog after slow syncs.
// Bound coalesced commands below the Raft append target of 1 MiB.
const maxRawBatchRequests = 256
const maxRawBatchBytes = 512 * 1024
const maxRawPendingItems = 4096

// maxMarshaledCommandSize is the upper bound on a marshaled Raft command.
// Protects against integer overflow when computing allocation sizes.
const maxMarshaledCommandSize = 64 * 1024 * 1024 // 64 MiB

var rawBatchWindow = 500 * time.Microsecond

var errRawQueueFull = errors.New("raw commit queue is full; try again later")

// ProposalObserver records raft proposal failures for operational metrics.
type ProposalObserver interface {
	ObserveProposalFailure()
}

type TransactionOption func(*TransactionManager)

// WithProposalObserver records raft.Apply failures without coupling kv to a
// concrete monitoring backend.
func WithProposalObserver(observer ProposalObserver) TransactionOption {
	return func(t *TransactionManager) {
		t.proposalObserver = observer
	}
}

func NewTransactionWithProposer(proposer raftengine.Proposer, opts ...TransactionOption) *TransactionManager {
	t := &TransactionManager{
		proposer: proposer,
		closeCh:  make(chan struct{}),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(t)
		}
	}
	return t
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

// Transactional is the kv-internal interface that fronts the raft propose
// path. Implementations (TransactionManager, LeaderProxy, ShardRouter,
// leaseRefreshingTxn) thread the caller's context end-to-end so a Redis /
// gRPC / S3 / SQS handler's deadline reaches Propose / VerifyLeader without
// being silently dropped to context.Background. See PR #748 / design doc
// 2026_05_10_implemented_kv_ctx_plumbing.md for the rationale; the prior
// signatures relied on a 5 s safety bound (#745), which verifyLeaderEngineCtx
// preserves as the fallback for contexts without a deadline.
type Transactional interface {
	Commit(ctx context.Context, reqs []*pb.Request) (*TransactionResponse, error)
	Abort(ctx context.Context, reqs []*pb.Request) (*TransactionResponse, error)
}

type TransactionResponse struct {
	CommitIndex uint64
	CommitTS    uint64
}

// proposalConditions classifies caller-supplied mutations in one pass. In
// particular, require_absent must stay out of the log while a replica could
// still decode it as an unknown field and apply an unconditional PUT.
type proposalConditions struct {
	cas    bool
	absent bool
}

func classifyProposalConditions(reqs []*pb.Request) proposalConditions {
	var conditions proposalConditions
	for _, req := range reqs {
		for _, mutation := range req.GetMutations() {
			conditions.cas = conditions.cas || usesCAS(mutation)
			conditions.absent = conditions.absent || mutation.GetRequireAbsent()
		}
	}
	return conditions
}

func (c proposalConditions) validate(reqs []*pb.Request) error {
	if c.cas {
		// The standalone CAS shape also forbids require_absent.
		return validateCASProposalShape(reqs)
	}
	if c.absent {
		return errors.WithStack(ErrTTLInlineCapabilityRequired)
	}
	return nil
}

func marshalRaftCommand(reqs []*pb.Request) ([]byte, error) {
	conditions := classifyProposalConditions(reqs)
	if err := conditions.validate(reqs); err != nil {
		return nil, err
	}
	return marshalClassifiedRaftCommand(reqs, conditions.cas)
}

func marshalClassifiedRaftCommand(reqs []*pb.Request, cas bool) ([]byte, error) {
	if len(reqs) == 1 {
		return marshalSingleRaftRequest(reqs[0], cas)
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

func marshalSingleRaftRequest(req *pb.Request, cas bool) ([]byte, error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(b) >= maxMarshaledCommandSize {
		return nil, errors.New("marshaled request too large")
	}
	if !cas {
		return prependByte(raftEncodeSingle, b), nil
	}
	if len(b) > maxMarshaledCommandSize-len(versionCASFrame) {
		return nil, errors.New("marshaled version CAS request too large")
	}
	return append([]byte(versionCASFrame), b...), nil
}

// prependByte returns a new slice with prefix followed by data.
func prependByte(prefix byte, data []byte) []byte {
	if len(data) >= math.MaxInt {
		panic("prependByte: data too large")
	}
	out := make([]byte, len(data)+1)
	out[0] = prefix
	copy(out[1:], data)
	return out
}

// applyRequests submits one raft command and returns per-request FSM results.
// HashiCorp Raft delivers FSM responses via ApplyFuture.Response(), not Error(),
// so we must inspect the response to avoid silently treating failed writes as
// successes.
func applyRequests(ctx context.Context, proposer raftengine.Proposer, reqs []*pb.Request, observer ProposalObserver, admission ...*transactionMigrationAdmission) (uint64, []error, error) {
	conditions := classifyProposalConditions(reqs)
	if err := conditions.validate(reqs); err != nil {
		return 0, nil, err
	}
	var migration *transactionMigrationAdmission
	if len(admission) > 0 {
		migration = admission[0]
	}
	return applyClassifiedRequests(ctx, proposer, reqs, observer, migration, conditions.cas)
}

// applyClassifiedRequests is called after all mutations have been checked.
// A CAS command is always standalone, including through migration admission.
func applyClassifiedRequests(ctx context.Context, proposer raftengine.Proposer, reqs []*pb.Request, observer ProposalObserver, migration *transactionMigrationAdmission, cas bool) (uint64, []error, error) {
	if migration != nil {
		return migration.apply(ctx, proposer, reqs, observer, cas)
	}
	b, err := marshalClassifiedRaftCommand(reqs, cas)
	if err != nil {
		return 0, nil, errors.WithStack(err)
	}

	if proposer == nil {
		recordProposalFailure(observer)
		return 0, nil, errors.WithStack(ErrLeaderNotFound)
	}

	if raftengine.IsVersionCASRaftPayload(b) {
		if _, ok := raftengine.VersionCASProposalProof(ctx); !ok {
			return 0, nil, errors.WithStack(ErrVersionCASUnavailable)
		}
	}
	result, err := proposer.Propose(ctx, b)
	if err != nil {
		recordProposalFailure(observer)
		if rejectedBeforeProposal(err) {
			// The etcd engine returns these before appending the entry.
			return 0, nil, errors.WithStack(err)
		}
		// Once Propose has been called, a timeout or leadership change does
		// not prove that the entry was absent from the replicated log.
		return 0, nil, &unknownTransactionFailure{cause: err}
	}
	if result == nil {
		recordProposalFailure(observer)
		return 0, nil, &unknownTransactionFailure{cause: errors.New("raft proposal returned nil result")}
	}

	return interpretProposalResult(result, reqs, observer)
}

func rejectedBeforeProposal(err error) bool {
	return errors.Is(err, raftengine.ErrNotLeader) || errors.Is(err, raftengine.ErrLeadershipTransferInProgress) ||
		errors.Is(err, ErrVersionCASUnavailable)
}

func interpretProposalResult(result *raftengine.ProposalResult, reqs []*pb.Request, proposalObserver ProposalObserver) (uint64, []error, error) {
	switch resp := result.Response.(type) {
	case nil:
		return result.CommitIndex, make([]error, len(reqs)), nil
	case error:
		if len(reqs) != 1 {
			recordProposalFailure(proposalObserver)
			return 0, nil, errors.WithStack(resp)
		}
		return result.CommitIndex, []error{errors.WithStack(resp)}, nil
	case *fsmApplyResponse:
		if len(resp.results) != len(reqs) {
			recordProposalFailure(proposalObserver)
			return 0, nil, errors.WithStack(errors.Newf("unexpected apply response size: got %d want %d", len(resp.results), len(reqs)))
		}
		return result.CommitIndex, resp.results, nil
	default:
		recordProposalFailure(proposalObserver)
		return 0, nil, errors.WithStack(errors.Newf("unexpected apply response type %T", resp))
	}
}

func recordProposalFailure(observer ProposalObserver) {
	if observer != nil {
		observer.ObserveProposalFailure()
	}
}

func (t *TransactionManager) Commit(ctx context.Context, reqs []*pb.Request) (*TransactionResponse, error) {
	return t.commitClassified(ctx, reqs, classifyProposalConditions(reqs))
}

func (t *TransactionManager) commitClassified(ctx context.Context, reqs []*pb.Request, conditions proposalConditions) (*TransactionResponse, error) {
	if len(reqs) == 0 {
		return &TransactionResponse{}, nil
	}
	if conditions.cas && t.versionCASAdmission == nil {
		return nil, errors.WithStack(ErrVersionCASUnavailable)
	}
	if err := conditions.validate(reqs); err != nil {
		return nil, err
	}
	owned, err := t.timestampAdmission.Load().prepare(ctx, reqs)
	if err != nil {
		return nil, err
	}
	reqs = owned
	if conditions.cas {
		req, err := t.admitVersionCASRequest(ctx, reqs[0])
		if err != nil {
			return nil, err
		}
		ctx, err = t.versionCASAdmission(ctx)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return t.commitSequential(ctx, []*pb.Request{req}, true)
	}
	if hasTxnRequests(reqs) {
		return t.commitSequential(ctx, reqs, false)
	}
	return t.commitRaw(ctx, reqs)
}

func hasTxnRequests(reqs []*pb.Request) bool {
	for _, req := range reqs {
		if req != nil && req.IsTxn {
			return true
		}
	}
	return false
}

func (t *TransactionManager) commitSequential(ctx context.Context, reqs []*pb.Request, cas bool) (*TransactionResponse, error) {
	commitIndex, err := t.applySequentialRequests(ctx, reqs, cas)

	if err != nil {
		// Only the coordinator can decide whether earlier participants or an
		// earlier attempt with this identity permit an ABORT.
		return nil, errors.WithStack(err)
	}

	return &TransactionResponse{
		CommitIndex: commitIndex,
	}, nil
}

func (t *TransactionManager) applySequentialRequests(ctx context.Context, reqs []*pb.Request, cas bool) (uint64, error) {
	commitIndex := uint64(0)
	for _, req := range reqs {
		idx, results, err := applyClassifiedRequests(ctx, t.proposer, []*pb.Request{req}, t.proposalObserver, t.migrationAdmission.Load(), cas)
		if err != nil {
			return 0, err
		}
		if len(results) > 0 && results[0] != nil {
			if singleRequestApplyVerdict(req, len(reqs), idx, results[0]) {
				return 0, &AppliedRequestRejection{commitIndex: idx, cause: results[0]}
			}
			return 0, &unknownTransactionFailure{cause: results[0]}
		}
		commitIndex = idx
	}
	return commitIndex, nil
}

func (t *TransactionManager) commitRaw(ctx context.Context, reqs []*pb.Request) (*TransactionResponse, error) {
	if requiresOwnedProposalContext(t.proposer) {
		return t.commitRawOwned(ctx, reqs)
	}
	item := &rawCommitItem{
		reqs:         reqs,
		done:         make(chan rawCommitResult, 1),
		encodedBytes: proto.Size(&pb.RaftCommand{Requests: reqs}),
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

	// Wait under the caller's ctx so a deadline expiring while batched
	// commits are pending lets the caller exit without blocking on a
	// busy queue. The proposal itself is driven by flushRawPending in
	// a separate goroutine using context.Background — that is the
	// intentional batched-commit model: many callers' ctxs map to one
	// batch propose, so no single ctx can bound it. Caller cancellation
	// here only abandons the wait; the propose still completes and
	// other waiters in the same batch get their results normally.
	select {
	case res := <-item.done:
		if res.err != nil {
			return nil, res.err
		}
		return &TransactionResponse{CommitIndex: res.commitIndex}, nil
	case <-ctx.Done():
		return nil, errors.WithStack(ctx.Err())
	}
}

// A future cross-request batch needs a composite of ALL original owned handles
// and one atomic terminal check. Until then, retain this request's entire raw
// command in one entry without detaching it from its receiver's context.
func (t *TransactionManager) commitRawOwned(ctx context.Context, reqs []*pb.Request) (*TransactionResponse, error) {
	select {
	case <-t.closeCh:
		return nil, errShuttingDown
	default:
	}
	index, results, err := applyRequests(ctx, t.proposer, reqs, t.proposalObserver, t.migrationAdmission.Load())
	if err != nil {
		return nil, err
	}
	if err := combineApplyErrors(results); err != nil {
		return nil, err
	}
	return &TransactionResponse{CommitIndex: index}, nil
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

	count, encodedBytes := 0, 1 // The command encoding prefix occupies one byte.
	n := 0
	for n < len(t.rawPending) {
		item := t.rawPending[n]
		nextCount := count + len(item.reqs)
		// Preserve an individual caller's request group as one command, even
		// when it alone exceeds the coalescing limits.
		if n > 0 && (nextCount > maxRawBatchRequests || item.encodedBytes > maxRawBatchBytes-encodedBytes) {
			break
		}
		count = nextCount
		encodedBytes += item.encodedBytes
		n++
	}

	batch := append([]*rawCommitItem(nil), t.rawPending[:n]...)
	clear(t.rawPending[:n])
	t.rawPending = t.rawPending[n:]
	return batch
}

func (t *TransactionManager) applyRawBatch(batch []*rawCommitItem) {
	reqs := make([]*pb.Request, 0, len(batch))
	offsets := make([]int, 0, len(batch)+1)
	for _, item := range batch {
		offsets = append(offsets, len(reqs))
		reqs = append(reqs, item.reqs...)
	}
	offsets = append(offsets, len(reqs))

	// Batched-commit goroutine cannot inherit any single caller's ctx —
	// see the commitRaw comment. Use Background here; per-caller
	// cancellation is honoured at the wait site in commitRaw via select
	// on item.done vs ctx.Done.
	idx, results, err := applyRequests(context.Background(), t.proposer, reqs, t.proposalObserver, t.migrationAdmission.Load())
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

func (t *TransactionManager) Abort(ctx context.Context, reqs []*pb.Request) (*TransactionResponse, error) {
	if HasCASMutation(reqs) {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
	var abortReqs []*pb.Request
	for _, req := range reqs {
		if abortReq := abortRequestFor(req); abortReq != nil {
			abortReqs = append(abortReqs, abortReq)
		}
	}

	var commitIndex uint64
	for _, req := range abortReqs {
		idx, results, err := applyRequests(ctx, t.proposer, []*pb.Request{req}, t.proposalObserver, t.migrationAdmission.Load())
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
