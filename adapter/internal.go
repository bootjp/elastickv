package adapter

import (
	"bytes"
	"context"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

type InternalOption func(*Internal)

// WithInternalWriteGate re-applies the coordinator's route-floor check to
// writes that were forwarded here from a follower.
func WithInternalWriteGate(gate kv.MutationWriteGate) InternalOption {
	return func(i *Internal) {
		i.writeGate = gate
	}
}

func WithInternalTimestampAllocator(alloc kv.TimestampAllocator) InternalOption {
	return func(i *Internal) {
		i.tsAllocator = alloc
	}
}

// ForwardWriteObserver observes successfully committed leader-side forwarded
// writes. It lets the autosplit sampler account for writes that entered through
// followers without coupling adapter.Internal to keyviz.
type ForwardWriteObserver func([]*pb.Request)

func WithInternalForwardWriteObserver(observer ForwardWriteObserver) InternalOption {
	return func(i *Internal) {
		i.forwardWriteObserver = observer
	}
}

func NewInternalWithEngine(txm kv.Transactional, leader raftengine.LeaderView, clock *kv.HLC, relay *RedisPubSubRelay, opts ...InternalOption) *Internal {
	i := &Internal{
		leader:             leader,
		transactionManager: txm,
		clock:              clock,
		relay:              relay,
	}
	for _, opt := range opts {
		opt(i)
	}
	return i
}

type Internal struct {
	leader               raftengine.LeaderView
	transactionManager   kv.Transactional
	clock                *kv.HLC
	tsAllocator          kv.TimestampAllocator
	relay                *RedisPubSubRelay
	writeGate            kv.MutationWriteGate
	forwardWriteObserver ForwardWriteObserver

	pb.UnimplementedInternalServer
}

var _ pb.InternalServer = (*Internal)(nil)

var ErrNotLeader = errors.New("not leader")
var ErrLeaderNotFound = errors.New("leader not found")
var ErrTxnTimestampOverflow = errors.New("txn timestamp overflow")

func (i *Internal) Forward(ctx context.Context, req *pb.ForwardRequest) (*pb.ForwardResponse, error) {
	if i.leader == nil || i.leader.State() != raftengine.StateLeader {
		return nil, errors.WithStack(ErrNotLeader)
	}
	if err := i.leader.VerifyLeader(ctx); err != nil {
		return nil, errors.WithStack(ErrNotLeader)
	}

	commitTS, err := i.stampTimestamps(ctx, req)
	if err != nil {
		return &pb.ForwardResponse{
			Success:     false,
			CommitIndex: 0,
		}, errors.WithStack(err)
	}

	r, err := i.transactionManager.Commit(ctx, req.Requests)
	if err != nil {
		return &pb.ForwardResponse{
			Success:     false,
			CommitIndex: 0,
		}, errors.WithStack(err)
	}
	if i.forwardWriteObserver != nil {
		i.forwardWriteObserver(req.GetRequests())
	}

	return &pb.ForwardResponse{
		Success:     true,
		CommitIndex: r.CommitIndex,
		CommitTs:    commitTS,
	}, nil
}

func (i *Internal) RelayPublish(_ context.Context, req *pb.RelayPublishRequest) (*pb.RelayPublishResponse, error) {
	if req == nil || i.relay == nil {
		return &pb.RelayPublishResponse{}, nil
	}
	return &pb.RelayPublishResponse{
		Subscribers: i.relay.Publish(req.Channel, req.Message),
	}, nil
}

func (i *Internal) stampTimestamps(ctx context.Context, req *pb.ForwardRequest) (uint64, error) {
	if req == nil {
		return 0, nil
	}
	if req.IsTxn {
		return i.stampTxnTimestamps(ctx, req.Requests)
	}

	return 0, i.stampRawTimestamps(ctx, req.Requests)
}

func (i *Internal) nextTimestamp(ctx context.Context, label string) (uint64, error) {
	if i.tsAllocator != nil {
		ts, err := i.tsAllocator.Next(ctx)
		if err != nil {
			return 0, errors.Wrap(err, label)
		}
		return ts, nil
	}
	if i.clock == nil {
		return 1, nil
	}
	ts, err := i.clock.NextFenced()
	if err != nil {
		return 0, errors.Wrap(err, label)
	}
	return ts, nil
}

func (i *Internal) nextTimestampAfter(ctx context.Context, min uint64, label string) (uint64, error) {
	if min == ^uint64(0) {
		return 0, errors.Wrap(kv.ErrTxnCommitTSRequired, label)
	}
	if i.tsAllocator != nil {
		return i.nextTimestampAfterFromAllocator(ctx, min, label)
	}
	if i.clock == nil {
		return min + 1, nil
	}
	if i.clock != nil {
		i.clock.Observe(min)
	}
	ts, err := i.nextTimestamp(ctx, label)
	if err != nil {
		return 0, err
	}
	if ts <= min {
		return 0, errors.Wrap(kv.ErrTxnCommitTSRequired, label)
	}
	return ts, nil
}

func (i *Internal) nextTimestampAfterFromAllocator(ctx context.Context, min uint64, label string) (uint64, error) {
	if after, ok := i.tsAllocator.(kv.TimestampAfterAllocator); ok {
		ts, err := after.NextAfter(ctx, min)
		if err != nil {
			return 0, errors.Wrap(err, label)
		}
		return ts, nil
	}
	ts, err := i.tsAllocator.Next(ctx)
	if err != nil {
		return 0, errors.Wrap(err, label)
	}
	if ts <= min {
		return 0, errors.Wrap(kv.ErrTxnCommitTSRequired, label)
	}
	return ts, nil
}

func (i *Internal) stampRawTimestamps(ctx context.Context, reqs []*pb.Request) error {
	for _, r := range reqs {
		if r == nil {
			continue
		}
		if r.Ts == 0 {
			ts, err := i.nextTimestamp(ctx, "stampRawTimestamps")
			if err != nil {
				return err
			}
			r.Ts = ts
		}
		// The follower that forwarded this write could not run the route-floor
		// check: its own stamping path bails out when the group engine is not
		// leader, so the timestamp only exists once we assign it here. Without
		// this the write would reach Raft through the bare TransactionManager
		// below with no MinWriteTSExclusive check at all. Already-stamped
		// requests are re-checked too, because the floor may have advanced
		// since the sender stamped them.
		if err := i.ensureRawWriteAllowed(r); err != nil {
			return err
		}
	}
	return nil
}

func (i *Internal) ensureRawWriteAllowed(r *pb.Request) error {
	if i.writeGate == nil || r == nil {
		return nil
	}
	return errors.WithStack(i.writeGate.EnsureMutationsWriteAllowed(r.Mutations, r.Ts))
}

func (i *Internal) stampTxnTimestamps(ctx context.Context, reqs []*pb.Request) (uint64, error) {
	startTS := forwardedTxnStartTS(reqs)
	if startTS == 0 {
		ts, err := i.nextTimestamp(ctx, "stampTxnTimestamps startTS")
		if err != nil {
			return 0, err
		}
		startTS = ts
	}
	if startTS == ^uint64(0) {
		return 0, errors.WithStack(ErrTxnTimestampOverflow)
	}

	// Assign the unified timestamp to all requests in the transaction.
	for _, r := range reqs {
		if r != nil {
			r.Ts = startTS
		}
	}

	commitTS, err := i.fillForwardedTxnCommitTS(ctx, reqs, startTS)
	if err != nil {
		return 0, err
	}
	if err := i.ensureTxnWritesAllowed(reqs, commitTS); err != nil {
		return 0, err
	}
	return commitTS, nil
}

func (i *Internal) ensureTxnWritesAllowed(reqs []*pb.Request, commitTS uint64) error {
	if i.writeGate == nil || commitTS == 0 {
		return nil
	}
	for _, r := range reqs {
		if r == nil || !r.IsTxn {
			continue
		}
		if r.Phase != pb.Phase_COMMIT && r.Phase != pb.Phase_NONE {
			continue
		}
		muts := forwardedTxnUserMutations(r.Mutations)
		if len(muts) == 0 {
			continue
		}
		if err := i.writeGate.EnsureMutationsWriteAllowed(muts, commitTS); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func forwardedTxnUserMutations(muts []*pb.Mutation) []*pb.Mutation {
	out := make([]*pb.Mutation, 0, len(muts))
	metaPrefix := []byte(kv.TxnMetaPrefix)
	for _, mut := range muts {
		if mut == nil || bytes.HasPrefix(mut.Key, metaPrefix) {
			continue
		}
		out = append(out, mut)
	}
	return out
}

func forwardedTxnStartTS(reqs []*pb.Request) uint64 {
	for _, r := range reqs {
		if r != nil && r.Ts != 0 {
			return r.Ts
		}
	}
	return 0
}

func forwardedTxnMetaMutation(r *pb.Request, metaPrefix []byte) (*pb.Mutation, bool) {
	if r == nil || !r.IsTxn {
		return nil, false
	}
	if r.Phase != pb.Phase_COMMIT && r.Phase != pb.Phase_ABORT && r.Phase != pb.Phase_NONE {
		return nil, false
	}
	if len(r.Mutations) == 0 || r.Mutations[0] == nil {
		return nil, false
	}
	if !bytes.HasPrefix(r.Mutations[0].Key, metaPrefix) {
		return nil, false
	}
	return r.Mutations[0], true
}

type forwardedTxnMetaToUpdate struct {
	m    *pb.Mutation
	meta kv.TxnMeta
}

func (i *Internal) fillForwardedTxnCommitTS(ctx context.Context, reqs []*pb.Request, startTS uint64) (uint64, error) {
	metaMutations, commitTS, err := collectForwardedTxnMetas(reqs)
	if err != nil {
		return 0, err
	}

	if commitTS == 0 && len(metaMutations) > 0 {
		commitTS, err = i.forwardedTxnCommitTS(ctx, startTS)
		if err != nil {
			return 0, err
		}
	}

	for _, item := range metaMutations {
		item.meta.CommitTS = commitTS
		item.m.Value = kv.EncodeTxnMeta(item.meta)
	}
	if err := stampRequestMutationCommitTS(reqs, commitTS); err != nil {
		return 0, err
	}
	return commitTS, nil
}

func stampRequestMutationCommitTS(reqs []*pb.Request, commitTS uint64) error {
	for _, r := range reqs {
		if r == nil {
			continue
		}
		if err := kv.StampMutationCommitTS(r.Mutations, commitTS); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func collectForwardedTxnMetas(reqs []*pb.Request) ([]forwardedTxnMetaToUpdate, uint64, error) {
	metaMutations := make([]forwardedTxnMetaToUpdate, 0, len(reqs))
	var commitTS uint64
	prefix := []byte(kv.TxnMetaPrefix)
	for _, r := range reqs {
		m, ok := forwardedTxnMetaMutation(r, prefix)
		if !ok {
			continue
		}
		meta, err := kv.DecodeTxnMeta(m.Value)
		if err != nil {
			continue
		}
		if meta.CommitTS != 0 {
			if commitTS != 0 && commitTS != meta.CommitTS {
				return nil, 0, errors.WithStack(kv.ErrInvalidRequest)
			}
			commitTS = meta.CommitTS
			continue
		}
		metaMutations = append(metaMutations, forwardedTxnMetaToUpdate{m: m, meta: meta})
	}
	return metaMutations, commitTS, nil
}

// forwardedTxnCommitTS allocates a commit timestamp for a forwarded
// transaction, strictly greater than startTS. Pulled out of
// fillForwardedTxnCommitTS so that function stays under cyclop.
func (i *Internal) forwardedTxnCommitTS(ctx context.Context, startTS uint64) (uint64, error) {
	commitTS := startTS + 1
	if commitTS == 0 {
		// Overflow: can't choose a commit timestamp strictly greater than startTS.
		return 0, errors.WithStack(ErrTxnTimestampOverflow)
	}
	if i.clock != nil {
		i.clock.Observe(startTS)
	}
	if i.tsAllocator != nil || i.clock != nil {
		ts, err := i.nextTimestampAfter(ctx, startTS, "fillForwardedTxnCommitTS")
		if err != nil {
			return 0, err
		}
		commitTS = ts
	}
	if commitTS <= startTS {
		// Defensive: avoid writing an invalid CommitTS.
		return 0, errors.WithStack(ErrTxnTimestampOverflow)
	}
	return commitTS, nil
}
