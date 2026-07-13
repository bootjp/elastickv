package adapter

import (
	"bytes"
	"context"
	"os"
	"strings"

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

func WithInternalMigrationPromoteGate(gate func(context.Context) error) InternalOption {
	return func(i *Internal) {
		i.migrationPromoteGate = gate
	}
}

func NewInternalWithEngine(txm kv.Transactional, leader raftengine.LeaderView, clock *kv.HLC, relay *RedisPubSubRelay, opts ...InternalOption) *Internal {
	i := &Internal{
		leader:               leader,
		transactionManager:   txm,
		clock:                clock,
		relay:                relay,
		migrationPromoteGate: defaultMigrationPromoteGate,
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
	store                store.MVCCStore
	migrationProposer    raftengine.Proposer
	migrationPromoteGate func(context.Context) error

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
	if err := i.verifyInternalLeader(stream.Context()); err != nil {
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

func (i *Internal) PromoteStagedVersions(ctx context.Context, req *pb.PromoteStagedVersionsRequest) (*pb.PromoteStagedVersionsResponse, error) {
	if req == nil {
		return nil, errors.WithStack(status.Error(codes.InvalidArgument, "promote staged versions request is nil"))
	}
	if req.GetJobId() == 0 {
		return nil, errors.WithStack(status.Error(codes.InvalidArgument, "promote staged versions job_id is required"))
	}
	if i.migrationProposer == nil {
		return nil, errors.WithStack(status.Error(codes.FailedPrecondition, "migration promote proposer is not configured"))
	}
	if err := i.verifyInternalLeader(ctx); err != nil {
		return nil, err
	}
	if err := i.verifyMigrationPromoteEnabled(ctx); err != nil {
		return nil, err
	}
	if err := validatePromoteStagedVersionsRequest(req); err != nil {
		return nil, errors.WithStack(err)
	}
	result, err := i.proposeMigrationPromote(ctx, req)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if i.clock != nil && result.MaxPromotedTS > 0 {
		i.clock.Observe(result.MaxPromotedTS)
	}
	return &pb.PromoteStagedVersionsResponse{
		NextCursor:    result.NextCursor,
		Done:          result.Done,
		PromotedRows:  result.PromotedRows,
		MaxPromotedTs: result.MaxPromotedTS,
	}, nil
}

func (i *Internal) verifyMigrationPromoteEnabled(ctx context.Context) error {
	if i.migrationPromoteGate == nil {
		return nil
	}
	if err := i.migrationPromoteGate(ctx); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func validatePromoteStagedVersionsRequest(req *pb.PromoteStagedVersionsRequest) error {
	prefix := distribution.MigrationStagedDataKeyPrefix(req.GetJobId())
	if err := store.ValidateExportCursorForRange(req.GetCursor(), prefix, store.PrefixScanEnd(prefix)); err != nil {
		if errors.Is(err, store.ErrInvalidExportCursor) {
			return errors.WithStack(status.Error(codes.InvalidArgument, store.ErrInvalidExportCursor.Error()))
		}
		return errors.WithStack(status.Errorf(codes.Internal, "validate promote cursor: %v", err))
	}
	return nil
}

func defaultMigrationPromoteGate(context.Context) error {
	if migrationPromoteOpcodeEnabledFromEnv() {
		return nil
	}
	return errors.WithStack(status.Error(codes.FailedPrecondition, "migration promote opcode is disabled; enable after every voter is running a build that supports migration promotion"))
}

func migrationPromoteOpcodeEnabledFromEnv() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("ELASTICKV_ENABLE_MIGRATION_PROMOTE_OPCODE"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func (i *Internal) verifyInternalLeader(ctx context.Context) error {
	if i.leader.State() != raftengine.StateLeader {
		return errors.WithStack(ErrNotLeader)
	}
	if err := i.leader.VerifyLeader(ctx); err != nil {
		return errors.WithStack(ErrNotLeader)
	}
	return nil
}

func (i *Internal) proposeMigrationImport(ctx context.Context, req *pb.ImportRangeVersionsRequest) (store.ImportVersionsResult, error) {
	cmd, err := kv.MarshalMigrationImportCommand(req)
	if err != nil {
		return store.ImportVersionsResult{}, errors.WithStack(err)
	}
	resp, err := i.proposeMigrationCommand(ctx, cmd, "migration import")
	if err != nil {
		return store.ImportVersionsResult{}, errors.WithStack(err)
	}
	switch resp := resp.(type) {
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

func (i *Internal) proposeMigrationPromote(ctx context.Context, req *pb.PromoteStagedVersionsRequest) (store.PromoteVersionsResult, error) {
	cmd, err := kv.MarshalMigrationPromoteCommand(req)
	if err != nil {
		return store.PromoteVersionsResult{}, errors.WithStack(err)
	}
	resp, err := i.proposeMigrationCommand(ctx, cmd, "migration promote")
	if err != nil {
		return store.PromoteVersionsResult{}, errors.WithStack(err)
	}
	switch resp := resp.(type) {
	case store.PromoteVersionsResult:
		return resp, nil
	case *store.PromoteVersionsResult:
		if resp == nil {
			return store.PromoteVersionsResult{}, errors.New("migration promote apply returned nil result")
		}
		return *resp, nil
	case error:
		return store.PromoteVersionsResult{}, errors.WithStack(resp)
	default:
		return store.PromoteVersionsResult{}, errors.WithStack(errors.Newf("unexpected migration promote apply response type %T", resp))
	}
}

func (i *Internal) proposeMigrationCommand(ctx context.Context, cmd []byte, label string) (any, error) {
	result, err := i.migrationProposer.Propose(ctx, cmd)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if result == nil {
		return nil, errors.WithStack(errors.Newf("%s proposal returned nil result", label))
	}
	return result.Response, nil
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
	return func(rawKey []byte) bool {
		bucket, ok := decodedS3BucketName(family, rawKey)
		if !ok {
			return false
		}
		routeKey := s3keys.RouteKey(bucket, 0, "")
		if len(routeStart) > 0 && bytes.Compare(routeKey, routeStart) < 0 {
			return false
		}
		return len(routeEnd) == 0 || bytes.Compare(routeKey, routeEnd) < 0
	}
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
		if r.Ts != 0 {
			continue
		}
		ts, err := i.nextTimestamp(ctx, "stampRawTimestamps")
		if err != nil {
			return err
		}
		r.Ts = ts
	}
	return nil
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

	return i.fillForwardedTxnCommitTS(ctx, reqs, startTS)
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
