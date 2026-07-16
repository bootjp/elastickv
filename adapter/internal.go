package adapter

import (
	"bytes"
	"context"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type InternalOption func(*Internal)

func WithInternalTimestampAllocator(alloc kv.TimestampAllocator) InternalOption {
	return func(i *Internal) {
		i.tsAllocator = alloc
	}
}

func WithInternalStore(st store.MVCCStore) InternalOption {
	return func(i *Internal) {
		i.store = st
	}
}

func WithInternalMigrationProposer(proposer raftengine.Proposer) InternalOption {
	return func(i *Internal) {
		i.migrationProposer = proposer
	}
}

func WithInternalRouteEngine(engine *distribution.Engine) InternalOption {
	return func(i *Internal) {
		i.routeEngine = engine
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
	leader             raftengine.LeaderView
	transactionManager kv.Transactional
	clock              *kv.HLC
	tsAllocator        kv.TimestampAllocator
	relay              *RedisPubSubRelay
	store              store.MVCCStore
	migrationProposer  raftengine.Proposer
	routeEngine        *distribution.Engine

	pb.UnimplementedInternalServer
}

var _ pb.InternalServer = (*Internal)(nil)

var ErrNotLeader = errors.New("not leader")
var ErrLeaderNotFound = errors.New("leader not found")
var ErrTxnTimestampOverflow = errors.New("txn timestamp overflow")

const (
	defaultMigrationExportChunkBytes  = 4 << 20
	defaultMigrationExportScanFactor  = 4
	defaultMigrationExportMaxVersions = 1024
)

func (i *Internal) Forward(ctx context.Context, req *pb.ForwardRequest) (*pb.ForwardResponse, error) {
	if i.leader == nil || i.leader.State() != raftengine.StateLeader {
		return nil, errors.WithStack(ErrNotLeader)
	}
	if err := i.leader.VerifyLeader(ctx); err != nil {
		return nil, errors.WithStack(ErrNotLeader)
	}

	if err := i.stampTimestamps(ctx, req); err != nil {
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

	return &pb.ForwardResponse{
		Success:     true,
		CommitIndex: r.CommitIndex,
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

func (i *Internal) ExportRangeVersions(req *pb.ExportRangeVersionsRequest, stream pb.Internal_ExportRangeVersionsServer) error {
	if err := i.validateExportRangeVersionsRequest(req); err != nil {
		return err
	}
	if err := i.verifyInternalLeaderApplied(stream.Context()); err != nil {
		return err
	}
	return i.streamExportRangeVersions(req, stream)
}

func (i *Internal) validateExportRangeVersionsRequest(req *pb.ExportRangeVersionsRequest) error {
	if req == nil {
		return errors.WithStack(status.Error(codes.InvalidArgument, "export range versions request is nil"))
	}
	if i.store == nil {
		return errors.WithStack(status.Error(codes.FailedPrecondition, "migration export store is not configured"))
	}
	if req.GetMaxCommitTs() == 0 {
		return errors.WithStack(status.Error(codes.InvalidArgument, "migration export max_commit_ts is required"))
	}
	if req.GetKeyFamily() == 0 {
		return errors.WithStack(status.Error(codes.InvalidArgument, "migration export key_family is required"))
	}
	if exportRangeVersionsRequestFullyUnbounded(req) {
		return errors.WithStack(status.Error(codes.InvalidArgument, "migration export requires a raw or route bound"))
	}
	return nil
}

func exportRangeVersionsRequestFullyUnbounded(req *pb.ExportRangeVersionsRequest) bool {
	return len(req.GetRangeStart()) == 0 &&
		len(req.GetRangeEnd()) == 0 &&
		len(req.GetRouteStart()) == 0 &&
		len(req.GetRouteEnd()) == 0
}

func (i *Internal) streamExportRangeVersions(req *pb.ExportRangeVersionsRequest, stream pb.Internal_ExportRangeVersionsServer) error {
	opts := exportRangeVersionsOptions(req)
	for {
		result, err := i.store.ExportVersions(stream.Context(), opts)
		if err != nil {
			return errors.WithStack(err)
		}
		if !result.Done && bytes.Equal(opts.Cursor, result.NextCursor) {
			return errors.WithStack(status.Error(codes.Internal, "migration export cursor did not progress"))
		}
		if err := stream.Send(&pb.ExportRangeVersionsResponse{
			Versions:   protoMVCCVersionsFromStore(result.Versions),
			NextCursor: result.NextCursor,
			Done:       result.Done,
		}); err != nil {
			return errors.WithStack(err)
		}
		if result.Done {
			return nil
		}
		opts.Cursor = result.NextCursor
	}
}

func (i *Internal) ImportRangeVersions(ctx context.Context, req *pb.ImportRangeVersionsRequest) (*pb.ImportRangeVersionsResponse, error) {
	if req == nil {
		return nil, errors.WithStack(status.Error(codes.InvalidArgument, "import range versions request is nil"))
	}
	if err := validateImportRangeVersionsRequest(req); err != nil {
		return nil, err
	}
	if i.migrationProposer == nil {
		return nil, errors.WithStack(status.Error(codes.FailedPrecondition, "migration import proposer is not configured"))
	}
	if err := i.verifyInternalLeader(ctx); err != nil {
		return nil, err
	}
	result, err := i.proposeMigrationImport(ctx, req)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if i.clock != nil && result.MaxImportedTS > 0 {
		i.clock.Observe(result.MaxImportedTS)
	}
	return &pb.ImportRangeVersionsResponse{AckedCursor: result.AckedCursor}, nil
}

func validateImportRangeVersionsRequest(req *pb.ImportRangeVersionsRequest) error {
	if req.GetJobId() == 0 {
		return errors.WithStack(status.Error(codes.InvalidArgument, "import range versions job_id is required"))
	}
	if req.GetBracketId() == 0 {
		return errors.WithStack(status.Error(codes.InvalidArgument, "import range versions bracket_id is required"))
	}
	return nil
}

func (i *Internal) verifyInternalLeader(ctx context.Context) error {
	if i.leader == nil {
		return errors.WithStack(ErrNotLeader)
	}
	if i.leader.State() != raftengine.StateLeader {
		return errors.WithStack(ErrNotLeader)
	}
	if err := i.leader.VerifyLeader(ctx); err != nil {
		return errors.WithStack(ErrNotLeader)
	}
	return nil
}

func (i *Internal) verifyInternalLeaderApplied(ctx context.Context) error {
	if i.leader == nil {
		return errors.WithStack(ErrNotLeader)
	}
	if i.leader.State() != raftengine.StateLeader {
		return errors.WithStack(ErrNotLeader)
	}
	_, err := i.leader.LinearizableRead(ctx)
	return errors.WithStack(err)
}

func (i *Internal) proposeMigrationImport(ctx context.Context, req *pb.ImportRangeVersionsRequest) (store.ImportVersionsResult, error) {
	cmd, err := kv.MarshalMigrationImportCommand(req)
	if err != nil {
		return store.ImportVersionsResult{}, errors.WithStack(err)
	}
	result, err := i.migrationProposer.Propose(ctx, cmd)
	if err != nil {
		return store.ImportVersionsResult{}, errors.WithStack(err)
	}
	if result == nil {
		return store.ImportVersionsResult{}, errors.New("migration import proposal returned nil result")
	}
	switch resp := result.Response.(type) {
	case store.ImportVersionsResult:
		return resp, nil
	case *store.ImportVersionsResult:
		if resp == nil {
			return store.ImportVersionsResult{}, errors.New("migration import apply returned nil result")
		}
		return *resp, nil
	case error:
		return store.ImportVersionsResult{}, errors.WithStack(resp)
	default:
		return store.ImportVersionsResult{}, errors.WithStack(errors.Newf("unexpected migration import apply response type %T", resp))
	}
}

func exportRangeVersionsOptions(req *pb.ExportRangeVersionsRequest) store.ExportVersionsOptions {
	chunkBytes := uint64(req.GetChunkBytes())
	if chunkBytes == 0 {
		chunkBytes = defaultMigrationExportChunkBytes
	}
	maxScannedBytes := req.GetMaxScannedBytes()
	if maxScannedBytes == 0 {
		maxScannedBytes = chunkBytes * defaultMigrationExportScanFactor
	}
	opts := store.ExportVersionsOptions{
		StartKey:             req.GetRangeStart(),
		EndKey:               req.GetRangeEnd(),
		MinCommitTSExclusive: req.GetMinCommitTs(),
		MaxCommitTSInclusive: req.GetMaxCommitTs(),
		Cursor:               req.GetCursor(),
		MaxVersions:          defaultMigrationExportMaxVersions,
		MaxBytes:             chunkBytes,
		MaxScannedBytes:      maxScannedBytes,
		KeyFamily:            req.GetKeyFamily(),
		AcceptKey:            migrationExportFilter(req),
	}
	return opts
}

func migrationExportFilter(req *pb.ExportRangeVersionsRequest) func([]byte) bool {
	routeFilter := kv.RouteKeyFilter(req.GetRouteStart(), req.GetRouteEnd())
	if migrationFamilyRequiresDecodedS3(req.GetKeyFamily()) {
		routeFilter = decodedS3BucketRouteFilter(req.GetKeyFamily(), req.GetRouteStart(), req.GetRouteEnd())
	}
	excludeKnownInternal := req.GetExcludeKnownInternal() || req.GetKeyFamily() == distribution.MigrationFamilyUser
	bracket := distribution.MigrationBracket{
		Family:               req.GetKeyFamily(),
		Start:                bytes.Clone(req.GetRangeStart()),
		End:                  bytes.Clone(req.GetRangeEnd()),
		ExcludeKnownInternal: excludeKnownInternal,
		ExcludePrefixes:      cloneByteSlices(req.GetExcludePrefixes()),
	}
	return func(rawKey []byte) bool {
		return bracket.ContainsRawKey(rawKey) && routeFilter(rawKey)
	}
}

func migrationFamilyRequiresDecodedS3(family uint32) bool {
	return family == distribution.MigrationFamilyS3BucketMeta ||
		family == distribution.MigrationFamilyS3BucketGeneration
}

func decodedS3BucketRouteFilter(family uint32, routeStart, routeEnd []byte) func([]byte) bool {
	allowRawRouteMatch := !s3BucketRouteBounds(routeStart, routeEnd)
	return func(rawKey []byte) bool {
		bucket, ok := decodedS3BucketName(family, rawKey)
		if !ok {
			return false
		}
		if allowRawRouteMatch && kv.RouteKeyFilter(routeStart, routeEnd)(rawKey) {
			return true
		}
		return decodedS3BucketRouteIntersects(bucket, routeStart, routeEnd)
	}
}

func s3BucketRouteBounds(routeStart, routeEnd []byte) bool {
	return bytes.HasPrefix(routeStart, []byte(s3keys.RoutePrefix)) ||
		bytes.HasPrefix(routeEnd, []byte(s3keys.RoutePrefix))
}

func decodedS3BucketRouteIntersects(bucket string, routeStart, routeEnd []byte) bool {
	bucketRouteStart := s3keys.RoutePrefixForBucketAnyGeneration(bucket)
	return rangesIntersect(routeStart, routeEnd, bucketRouteStart, prefixScanEnd(bucketRouteStart))
}

func rangesIntersect(aStart, aEnd, bStart, bEnd []byte) bool {
	if len(aEnd) > 0 && bytes.Compare(aEnd, bStart) <= 0 {
		return false
	}
	if len(bEnd) > 0 && bytes.Compare(bEnd, aStart) <= 0 {
		return false
	}
	return true
}

func decodedS3BucketName(family uint32, rawKey []byte) (string, bool) {
	switch family {
	case distribution.MigrationFamilyS3BucketMeta:
		return s3keys.ParseBucketMetaKey(rawKey)
	case distribution.MigrationFamilyS3BucketGeneration:
		return s3keys.ParseBucketGenerationKey(rawKey)
	default:
		return "", false
	}
}

func cloneByteSlices(in [][]byte) [][]byte {
	if len(in) == 0 {
		return nil
	}
	out := make([][]byte, len(in))
	for i := range in {
		out[i] = bytes.Clone(in[i])
	}
	return out
}

func protoMVCCVersionsFromStore(in []store.MVCCVersion) []*pb.MVCCVersion {
	out := make([]*pb.MVCCVersion, 0, len(in))
	for _, version := range in {
		out = append(out, &pb.MVCCVersion{
			Key:       bytes.Clone(version.Key),
			CommitTs:  version.CommitTS,
			Tombstone: version.Tombstone,
			Value:     bytes.Clone(version.Value),
			KeyFamily: version.KeyFamily,
			ExpireAt:  version.ExpireAt,
		})
	}
	return out
}

func (i *Internal) stampTimestamps(ctx context.Context, req *pb.ForwardRequest) error {
	if req == nil {
		return nil
	}
	if req.IsTxn {
		return i.stampTxnTimestamps(ctx, req.Requests)
	}

	return i.stampRawTimestamps(ctx, req.Requests)
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
		if err := i.rejectWriteTimestampFloorMutations(r.Mutations, r.Ts); err != nil {
			return err
		}
	}
	return nil
}

func (i *Internal) rejectWriteTimestampFloorMutations(muts []*pb.Mutation, commitTS uint64) error {
	if i == nil || i.routeEngine == nil || commitTS == 0 {
		return nil
	}
	routes := i.routeEngine.Stats()
	for _, mut := range muts {
		if mut == nil || forwardedTxnControlMutation(mut) || (len(mut.Key) == 0 && mut.GetOp() != pb.Op_DEL_PREFIX) {
			continue
		}
		if err := rejectMutationBelowRouteWriteFloor(routes, mut, commitTS); err != nil {
			return err
		}
	}
	return nil
}

func rejectMutationBelowRouteWriteFloor(routes []distribution.Route, mut *pb.Mutation, commitTS uint64) error {
	for _, route := range routes {
		if routeWriteTimestampFloorApplies(route, mut, commitTS) {
			return errors.Wrapf(kv.ErrRouteWriteTimestampTooLow, "key %q commit_ts=%d floor=%d", mut.Key, commitTS, route.MinWriteTSExclusive)
		}
	}
	return nil
}

func forwardedTxnControlMutation(mut *pb.Mutation) bool {
	return mut != nil && bytes.HasPrefix(mut.GetKey(), []byte(kv.TxnKeyPrefix))
}

func routeWriteTimestampFloorApplies(route distribution.Route, mut *pb.Mutation, commitTS uint64) bool {
	if route.MinWriteTSExclusive == 0 || commitTS > route.MinWriteTSExclusive {
		return false
	}
	if mut.GetOp() == pb.Op_DEL_PREFIX {
		start, end := kv.RoutePrefixRange(mut.GetKey())
		return rangesIntersect(route.Start, route.End, start, end)
	}
	return kv.RouteKeyFilter(route.Start, route.End)(mut.GetKey())
}

func (i *Internal) stampTxnTimestamps(ctx context.Context, reqs []*pb.Request) error {
	startTS := forwardedTxnStartTS(reqs)
	if startTS == 0 {
		ts, err := i.nextTimestamp(ctx, "stampTxnTimestamps startTS")
		if err != nil {
			return err
		}
		startTS = ts
	}
	if startTS == ^uint64(0) {
		return errors.WithStack(ErrTxnTimestampOverflow)
	}

	// Assign the unified timestamp to all requests in the transaction.
	for _, r := range reqs {
		if r != nil {
			r.Ts = startTS
		}
	}

	if err := i.fillForwardedTxnCommitTS(ctx, reqs, startTS); err != nil {
		return err
	}
	return i.rejectWriteTimestampFloorTxnRequests(reqs)
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

func (i *Internal) fillForwardedTxnCommitTS(ctx context.Context, reqs []*pb.Request, startTS uint64) error {
	type metaToUpdate struct {
		m    *pb.Mutation
		meta kv.TxnMeta
	}

	metaMutations := make([]metaToUpdate, 0, len(reqs))
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
			continue
		}
		metaMutations = append(metaMutations, metaToUpdate{m: m, meta: meta})
	}
	if len(metaMutations) == 0 {
		return nil
	}

	commitTS, err := i.forwardedTxnCommitTS(ctx, startTS)
	if err != nil {
		return err
	}

	for _, item := range metaMutations {
		item.meta.CommitTS = commitTS
		item.m.Value = kv.EncodeTxnMeta(item.meta)
	}
	return nil
}

func (i *Internal) rejectWriteTimestampFloorTxnRequests(reqs []*pb.Request) error {
	for _, r := range reqs {
		commitTS, ok := forwardedTxnRequestCommitTS(r)
		if !ok {
			continue
		}
		if err := i.rejectWriteTimestampFloorMutations(r.Mutations, commitTS); err != nil {
			return err
		}
	}
	return nil
}

func forwardedTxnRequestCommitTS(r *pb.Request) (uint64, bool) {
	if r == nil || (r.Phase != pb.Phase_COMMIT && r.Phase != pb.Phase_NONE) {
		return 0, false
	}
	m, ok := forwardedTxnMetaMutation(r, []byte(kv.TxnMetaPrefix))
	if !ok {
		return 0, false
	}
	meta, err := kv.DecodeTxnMeta(m.Value)
	if err != nil || meta.CommitTS == 0 {
		return 0, false
	}
	return meta.CommitTS, true
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
