package kv

import (
	"context"
	"crypto/rand"
	"sort"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

func bindParticipantTimestampIntent(requests []*pb.Request, retained *pb.TimestampClaimIntent) (*pb.TimestampClaimIntent, error) {
	if len(requests) == 0 {
		return nil, errors.WithStack(ErrTSOClaimInvalid)
	}
	first := requests[0]
	root := &pb.TimestampClaimIntent{IsTxn: first.IsTxn, ObservedRouteVersion: first.ObservedRouteVersion}
	if err := initializeParticipantClaimOperation(root, retained); err != nil {
		return nil, err
	}
	for _, request := range requests {
		if err := appendParticipantClaimIntent(root, request); err != nil {
			return nil, err
		}
	}
	canonical, err := canonicalTimestampIntent(root)
	if err != nil {
		return nil, err
	}
	if err := compareRetainedParticipantIntent(canonical, retained); err != nil {
		return nil, err
	}
	for _, request := range requests {
		request.TimestampIntent = canonical
		if err := ValidateRequestTimestampIntent(request); err != nil {
			return nil, err
		}
	}
	return canonical, nil
}

func initializeParticipantClaimOperation(root, retained *pb.TimestampClaimIntent) error {
	if retained != nil {
		root.OperationId = append([]byte(nil), retained.OperationId...)
	} else {
		root.OperationId = make([]byte, len(TSOOperationID{}))
		if _, err := rand.Read(root.OperationId); err != nil {
			return errors.Wrap(err, "create participant operation ID")
		}
	}
	return nil
}

func appendParticipantClaimIntent(root *pb.TimestampClaimIntent, request *pb.Request) error {
	if request.IsTxn != root.IsTxn || request.ObservedRouteVersion != root.ObservedRouteVersion {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	muts := request.Mutations
	if request.IsTxn {
		meta, data, err := extractTxnMeta(muts)
		if err != nil {
			return err
		}
		root.ReadTimestamp, root.PrimaryKey, root.PreviousCommitTimestamp = request.Ts, meta.PrimaryKey, meta.PrevCommitTS
		root.LockTtlMs = meta.LockTTLms
		muts = data
	}
	root.Mutations = append(root.Mutations, muts...)
	root.ReadKeys = append(root.ReadKeys, request.ReadKeys...)
	root.WriteFenceBypassKeys = append(root.WriteFenceBypassKeys, request.WriteFenceBypassKeys...)
	root.Participants = append(root.Participants, &pb.TimestampClaimParticipant{GroupId: request.TimestampClaimGroupId, Mutations: muts, ReadKeys: request.ReadKeys, WriteFenceBypassKeys: request.WriteFenceBypassKeys})
	return nil
}

func compareRetainedParticipantIntent(canonical, retained *pb.TimestampClaimIntent) error {
	if retained != nil {
		prior, err := canonicalTimestampIntent(retained)
		if err != nil {
			return err
		}
		if !proto.Equal(prior, canonical) {
			return errors.WithStack(ErrTSOClaimConflict)
		}
	}
	return nil
}

func bindClaimToParticipantRequests(ctx context.Context, allocator TimestampAllocator, requests []*pb.Request, group *OperationGroup[OP]) (uint64, error) {
	root, err := bindParticipantTimestampIntent(requests, group.TimestampIntent)
	if err != nil {
		return 0, err
	}
	group.TimestampIntent = root
	first := requests[0]
	first.TimestampClaim = group.TimestampClaim
	if err := restoreRetainedClaimTimestamp(first, group.CommitTS); err != nil {
		return 0, err
	}
	if err := StampRequestTimestampClaim(ctx, allocator, first); err != nil {
		return 0, err
	}
	claim, err := DecodeRequestTimestampClaim(first.TimestampClaim)
	if err != nil {
		return 0, err
	}
	timestamp := claim.Proof.Record.Timestamp
	group.TimestampClaim, group.CommitTS = first.TimestampClaim, timestamp
	for _, request := range requests {
		request.TimestampIntent, request.TimestampClaim = root, first.TimestampClaim
		if err := stampClaimPersistenceTimestamp(request, timestamp); err != nil {
			return 0, err
		}
	}
	return timestamp, nil
}

func restoreRetainedClaimTimestamp(request *pb.Request, retainedTimestamp uint64) error {
	if len(request.TimestampClaim) == 0 {
		return nil
	}
	claim, err := DecodeRequestTimestampClaim(request.TimestampClaim)
	if err != nil {
		return err
	}
	if retainedTimestamp != 0 && retainedTimestamp != claim.Proof.Record.Timestamp {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	return stampClaimPersistenceTimestamp(request, claim.Proof.Record.Timestamp)
}

func (c *ShardedCoordinator) claimRawLogs(ctx context.Context, group *OperationGroup[OP], requests []*pb.Request, gids []uint64) error {
	if !TimestampClaimsRequired(c.tsAllocator) {
		return nil
	}
	for i, request := range requests {
		request.TimestampClaimGroupId = gids[i]
	}
	_, err := bindClaimToParticipantRequests(ctx, c.tsAllocator, requests, group)
	return err
}

func (c *ShardedCoordinator) dispatchClaimedTransaction(ctx context.Context, group *OperationGroup[OP], callerSuppliedStart bool) (*CoordinateResponse, error) {
	grouped, gids, primary, err := c.claimedTxnMutationGroups(group)
	if err != nil {
		return nil, err
	}
	single := len(gids) == 1 && c.allReadKeysInShard(group.ReadKeys, gids[0])
	if err := c.validateCallerSuppliedTxnStart(ctx, group.StartTS, single, callerSuppliedStart); err != nil {
		return nil, err
	}
	reads, err := c.groupReadKeysByShardID(group.ReadKeys)
	if err != nil {
		return nil, err
	}
	reads = c.groupedReadKeysWithStagedVisibilityMutationAliases(reads, grouped)
	if groupedReadKeyCount(reads) > maxReadKeys {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
	requests, intentRequests, err := c.claimedTxnRequests(ctx, group, grouped, gids, reads, primary, single)
	if err != nil {
		return nil, err
	}
	commitTS, err := bindClaimToParticipantRequests(ctx, c.tsAllocator, intentRequests, group)
	if err != nil {
		return nil, err
	}
	if err := c.ensureGroupedMutationsWriteAllowedWithBypass(grouped, commitTS, c.writeFenceBypassKeysByGroup(group.Elems)); err != nil {
		return nil, err
	}
	if single {
		return c.commitClaimedSingle(ctx, gids[0], requests[gids[0]], commitTS)
	}
	return c.commitClaimedParticipants(ctx, group, grouped, gids, reads, requests, primary, commitTS)
}

func (c *ShardedCoordinator) claimedTxnMutationGroups(group *OperationGroup[OP]) (map[uint64][]*pb.Mutation, []uint64, []byte, error) {
	if len(group.ReadKeys) > maxReadKeys {
		return nil, nil, nil, errors.WithStack(ErrInvalidRequest)
	}
	grouped, gids, err := c.groupTxnMutations(group.Elems, group.KeyVizLabel, group.ObservedRouteVersion, group.RequireAllParticipants)
	if err != nil {
		return nil, nil, nil, err
	}
	if group.PrevCommitTS != 0 && len(gids) != 1 {
		return nil, nil, nil, errors.WithStack(ErrTxnDedupRequiresSingleShard)
	}
	primary := transactionPrimaryKey(group.Elems, group.PrimaryKey)
	if len(primary) == 0 {
		return nil, nil, nil, errors.WithStack(ErrTxnPrimaryKeyRequired)
	}
	return grouped, gids, primary, nil
}

func (c *ShardedCoordinator) commitClaimedSingle(ctx context.Context, gid uint64, request *pb.Request, commitTS uint64) (*CoordinateResponse, error) {
	target, err := c.txnGroupForID(gid)
	if err != nil {
		return nil, err
	}
	response, err := target.Txn.Commit(ctx, []*pb.Request{request})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &CoordinateResponse{CommitIndex: response.CommitIndex, CommitTS: commitTS}, nil
}

func (c *ShardedCoordinator) claimedTxnRequests(ctx context.Context, group *OperationGroup[OP], grouped map[uint64][]*pb.Mutation, gids []uint64, reads map[uint64][][]byte, primary []byte, single bool) (map[uint64]*pb.Request, []*pb.Request, error) {
	bypass := c.writeFenceBypassKeysByGroup(group.Elems)
	requests := make(map[uint64]*pb.Request, len(gids))
	intentRequests := make([]*pb.Request, 0, len(gids)+len(reads))
	for _, gid := range gids {
		if !single {
			if err := c.requireClaimPrepareEvidence(ctx, gid); err != nil {
				return nil, nil, err
			}
		}
		phase := pb.Phase_PREPARE
		if single {
			phase = pb.Phase_NONE
		}
		meta := TxnMeta{PrimaryKey: primary, LockTTLms: defaultTxnLockTTLms, PrevCommitTS: group.PrevCommitTS}
		if group.CommitTS != 0 {
			if single {
				meta.CommitTS = group.CommitTS
			} else {
				meta.PrepareCommitTS = group.CommitTS
			}
		}
		request := &pb.Request{IsTxn: true, Phase: phase, Ts: group.StartTS, ObservedRouteVersion: group.ObservedRouteVersion, TimestampClaimGroupId: gid,
			Mutations: append([]*pb.Mutation{{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(meta)}}, grouped[gid]...),
			ReadKeys:  reads[gid], WriteFenceBypassKeys: bypass[gid]}
		requests[gid] = request
		intentRequests = append(intentRequests, request)
	}
	for gid, keys := range reads {
		if _, written := requests[gid]; written {
			continue
		}
		intentRequests = append(intentRequests, &pb.Request{IsTxn: true, Phase: pb.Phase_PREPARE, Ts: group.StartTS, ObservedRouteVersion: group.ObservedRouteVersion, TimestampClaimGroupId: gid, ReadKeys: keys,
			Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primary, LockTTLms: defaultTxnLockTTLms})}}})
	}
	return requests, intentRequests, nil
}

func (c *ShardedCoordinator) requireClaimPrepareEvidence(ctx context.Context, gid uint64) error {
	group, err := c.txnGroupForID(gid)
	if err != nil {
		return err
	}
	enabled, err := PrepareEvidenceEnabled(ctx, group.Store)
	if err != nil {
		return err
	}
	if !enabled {
		return errors.WithStack(ErrTSOProtocolUnsupported)
	}
	if _, ok := group.Store.(PreparedClaimMutationApplier); !ok {
		return errors.Wrap(ErrTSOProtocolUnsupported, "store lacks atomic prepared-claim mutation apply")
	}
	return nil
}

func (c *ShardedCoordinator) commitClaimedParticipants(ctx context.Context, group *OperationGroup[OP], grouped map[uint64][]*pb.Mutation, gids []uint64, reads map[uint64][][]byte, requests map[uint64]*pb.Request, primary []byte, commitTS uint64) (*CoordinateResponse, error) {
	prepared, err := c.prepareClaimedParticipants(ctx, group, gids, requests, primary)
	if err != nil {
		return nil, err
	}
	if err := c.validateReadOnlyShards(ctx, reads, gids, group.StartTS); err != nil {
		c.abortClaimedParticipants(context.WithoutCancel(ctx), group, prepared, primary)
		return nil, err
	}
	primaryGID, ok := primaryGroupIDForKey(primary, grouped, gids)
	if !ok {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
	index, err := c.commitClaimedGroup(ctx, group, primaryGID, primary, pb.Phase_COMMIT)
	if err != nil {
		if primaryCommitDefinitelyRejected(err) {
			c.abortClaimedParticipants(context.WithoutCancel(ctx), group, prepared, primary)
			return nil, errors.WithStack(err)
		}
		return nil, errors.WithStack(errors.Join(ErrTransactionOutcomeUnknown, err))
	}
	for _, gid := range gids {
		if gid == primaryGID {
			continue
		}
		secondaryIndex, err := c.commitClaimedSecondary(ctx, group, gid, primary)
		if err != nil {
			return nil, err
		}
		index = max(index, secondaryIndex)
	}
	return &CoordinateResponse{CommitIndex: index, CommitTS: commitTS}, nil
}

func (c *ShardedCoordinator) prepareClaimedParticipants(ctx context.Context, group *OperationGroup[OP], gids []uint64, requests map[uint64]*pb.Request, primary []byte) ([]uint64, error) {
	prepared := make([]uint64, 0, len(gids))
	for _, gid := range gids {
		target, err := c.txnGroupForID(gid)
		if err != nil {
			c.abortClaimedParticipants(context.WithoutCancel(ctx), group, prepared, primary)
			return nil, err
		}
		if _, err := target.Txn.Commit(ctx, []*pb.Request{requests[gid]}); err != nil {
			c.abortClaimedParticipants(context.WithoutCancel(ctx), group, prepared, primary)
			return nil, errors.WithStack(err)
		}
		prepared = append(prepared, gid)
	}
	return prepared, nil
}

func (c *ShardedCoordinator) commitClaimedGroup(ctx context.Context, group *OperationGroup[OP], gid uint64, primary []byte, phase pb.Phase) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, verifyLeaderTimeout)
	defer cancel()
	target, err := c.txnGroupForID(gid)
	if err != nil {
		return 0, err
	}
	request, err := claimedResolutionRequest(group, gid, primary, phase)
	if err != nil {
		return 0, err
	}
	response, err := target.Txn.Commit(ctx, []*pb.Request{request})
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if response == nil {
		return 0, errors.WithStack(ErrTransactionOutcomeUnknown)
	}
	return response.CommitIndex, nil
}

func claimedResolutionRequest(group *OperationGroup[OP], gid uint64, primary []byte, phase pb.Phase) (*pb.Request, error) {
	var participant *pb.TimestampClaimParticipant
	for _, part := range group.TimestampIntent.Participants {
		if part.GroupId == gid {
			participant = part
		}
	}
	if participant == nil {
		return nil, errors.WithStack(ErrTSOClaimInvalid)
	}
	request := &pb.Request{IsTxn: true, Phase: phase, Ts: group.StartTS, ObservedRouteVersion: group.ObservedRouteVersion, TimestampClaimGroupId: gid, TimestampIntent: group.TimestampIntent, TimestampClaim: group.TimestampClaim,
		Mutations: append([]*pb.Mutation{txnMetaMutation(primary, group.TimestampIntent.LockTtlMs, group.CommitTS)}, keyMutations(participant.Mutations)...), WriteFenceBypassKeys: participant.WriteFenceBypassKeys}
	return request, nil
}

func (c *ShardedCoordinator) commitClaimedSecondary(ctx context.Context, group *OperationGroup[OP], gid uint64, primary []byte) (uint64, error) {
	target, err := c.txnGroupForID(gid)
	if err != nil {
		return 0, err
	}
	request, err := claimedResolutionRequest(group, gid, primary, pb.Phase_COMMIT)
	if err != nil {
		return 0, err
	}
	response, err := commitSecondaryWithRetry(ctx, target, request)
	if err != nil {
		return 0, c.secondaryCommitFailure(err, gid, group.StartTS, group.CommitTS, primary, group.RequireAllParticipants)
	}
	if response == nil {
		return 0, errors.WithStack(ErrTransactionOutcomeUnknown)
	}
	return response.CommitIndex, nil
}

func (c *ShardedCoordinator) abortClaimedParticipants(ctx context.Context, group *OperationGroup[OP], gids []uint64, primary []byte) {
	for _, gid := range gids {
		_, _ = c.commitClaimedGroup(ctx, group, gid, primary, pb.Phase_ABORT)
	}
}

func (c *ShardedCoordinator) claimedBroadcastIDs() ([]uint64, error) {
	groups, err := c.allShardGroups()
	if err != nil {
		return nil, err
	}
	ids := append([]uint64(nil), c.allShardGroupIDs...)
	if !c.allShardGroupsConfigured {
		ids = nil
		for id, group := range c.groups {
			if id != 0 && group.TSOState == nil {
				ids = append(ids, id)
			}
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	}
	if len(ids) != len(groups) {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
	return ids, nil
}
