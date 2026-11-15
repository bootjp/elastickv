package kv

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"slices"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

// WithTwoGroupPrepareAdmission is a trusted-runtime opt-in. Production must
// not call it until every replica supports the command and the activation gate
// is replicated. Public Forward never accepts a lookup or observation.
func (c *ShardedCoordinator) WithTwoGroupPrepareAdmission(lookup func(uint64) (TwoGroupHomeLookup, error)) *ShardedCoordinator {
	c.twoGroupHomeLookup = lookup
	for _, group := range c.groups {
		if group != nil {
			group.twoGroupTerminalResolver = c.DecideTwoGroup
		}
	}
	return c
}

// TwoGroupRemotePreparer is a trusted private peer route. It accepts only a
// manifest; the destination leader constructs any home observation locally.
type TwoGroupRemotePreparer interface {
	PrepareTwoGroupOnLeader(context.Context, uint64, *pb.TwoGroupPrepareManifest) error
}

func (c *ShardedCoordinator) WithTwoGroupRemotePrepare(resolve func(uint64) (TwoGroupRemotePreparer, error)) *ShardedCoordinator {
	c.twoGroupRemotePrepare = resolve
	return c
}

// prepareTwoGroup is the trusted in-process PREPARE-only entry. It never
// accepts an observation from its caller and cannot commit or abort the txn.
// The normal Dispatch route remains fenced until terminal evidence is wired.
func (c *ShardedCoordinator) prepareTwoGroup(ctx context.Context, startTS, commitTS uint64,
	primaryKey []byte, grouped map[uint64][]*pb.Mutation, groupedReadKeys map[uint64][][]byte,
	routeVersion uint64, bypassKeys map[uint64][][]byte) (bool, error) {
	if c == nil || c.twoGroupHomeLookup == nil || len(grouped) != 2 {
		return false, errors.WithStack(ErrTwoGroupAdmission)
	}
	gids := make([]uint64, 0, len(grouped))
	for gid := range grouped {
		gids = append(gids, gid)
	}
	unresolved, err := c.prewriteTwoGroupTxn(ctx, startTS, commitTS, primaryKey, grouped, gids,
		groupedReadKeys, routeVersion, bypassKeys)
	return unresolved, err
}

func twoGroupOperationID(primaryKey []byte, startTS uint64) [16]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("elastickv/two-group-prepare/id/v1\x00"))
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(primaryKey)))
	_, _ = h.Write(size[:])
	_, _ = h.Write(primaryKey)
	binary.BigEndian.PutUint64(size[:], startTS)
	_, _ = h.Write(size[:])
	var id [16]byte
	copy(id[:], h.Sum(nil))
	return id
}

func (c *ShardedCoordinator) prewriteTwoGroupTxn(ctx context.Context, startTS, commitTS uint64, primaryKey []byte,
	grouped map[uint64][]*pb.Mutation, gids []uint64, groupedReadKeys map[uint64][][]byte,
	routeVersion uint64, bypassKeys map[uint64][][]byte) (bool, error) {
	manifest, ids, err := c.buildTwoGroupManifest(ctx, startTS, commitTS, primaryKey, grouped, gids,
		groupedReadKeys, routeVersion, bypassKeys)
	if err != nil {
		return false, err
	}
	return c.prepareTwoGroupOrdered(ctx, manifest, ids, groupedReadKeys, startTS)
}

func (c *ShardedCoordinator) buildTwoGroupManifest(ctx context.Context, startTS, commitTS uint64, primaryKey []byte,
	grouped map[uint64][]*pb.Mutation, gids []uint64, groupedReadKeys map[uint64][][]byte,
	routeVersion uint64, bypassKeys map[uint64][][]byte) (*pb.TwoGroupPrepareManifest, []uint64, error) {
	primary, ids, err := c.twoGroupPrimaryAndIDs(primaryKey, grouped, gids)
	if err != nil {
		return nil, nil, err
	}
	metas, err := c.prepareEvidenceMetas(ctx, primaryKey, commitTS, ids)
	if err != nil {
		return nil, nil, err
	}
	id := twoGroupOperationID(primaryKey, startTS)
	manifest := &pb.TwoGroupPrepareManifest{Version: twoGroupManifestVersion, OperationId: id[:], PersistenceTimestamp: commitTS, PrimaryGroupId: primary}
	for _, gid := range ids {
		meta, _, err := extractTxnMeta([]*pb.Mutation{metas[gid]})
		if err != nil || meta.PrepareCommitTS != commitTS {
			return nil, nil, errors.Join(ErrTwoGroupAdmission, err)
		}
		manifest.Participants = append(manifest.Participants, &pb.TwoGroupPrepareParticipant{GroupId: gid, Prepare: &pb.Request{
			IsTxn: true, Phase: pb.Phase_PREPARE, Ts: startTS,
			Mutations: append([]*pb.Mutation{metas[gid]}, grouped[gid]...),
			ReadKeys:  groupedReadKeys[gid], ObservedRouteVersion: routeVersion,
			WriteFenceBypassKeys: bypassKeys[gid],
		}})
	}
	if _, _, _, err := canonicalTwoGroupManifest(manifest); err != nil {
		return nil, nil, err
	}
	return manifest, ids, nil
}

func (c *ShardedCoordinator) twoGroupPrimaryAndIDs(primaryKey []byte, grouped map[uint64][]*pb.Mutation, gids []uint64) (uint64, []uint64, error) {
	primary, ok := primaryGroupIDForKey(primaryKey, grouped, gids)
	if !ok || len(gids) != 2 || c.twoGroupHomeLookup == nil {
		return 0, nil, errors.WithStack(ErrTwoGroupAdmission)
	}
	ids := slices.Clone(gids)
	slices.Sort(ids)
	if ids[0] == ids[1] || ids[0] == 0 {
		return 0, nil, errors.WithStack(ErrTwoGroupAdmission)
	}
	return primary, ids, nil
}

func (c *ShardedCoordinator) prepareTwoGroupOrdered(ctx context.Context, manifest *pb.TwoGroupPrepareManifest,
	ids []uint64, groupedReadKeys map[uint64][][]byte, startTS uint64) (bool, error) {
	primary := manifest.GetPrimaryGroupId()
	lookup, err := c.twoGroupHomeLookup(primary)
	if err != nil || lookup == nil {
		return false, errors.Join(ErrTwoGroupAdmission, err)
	}
	if err := c.proposeOrReconcileHome(ctx, primary, manifest, lookup); err != nil {
		return true, err
	}
	observed, err := ObserveTwoGroupHome(ctx, lookup, manifest)
	if err != nil {
		return true, err
	}
	secondary := ids[0]
	if secondary == primary {
		secondary = ids[1]
	}
	if err := c.proposeTwoGroupPrepare(ctx, secondary, manifest, observed); err != nil {
		return true, err
	}
	if err := c.validateReadOnlyShards(ctx, groupedReadKeys, ids, startTS); err != nil {
		return true, err
	}
	return false, nil
}

func (c *ShardedCoordinator) proposeOrReconcileHome(ctx context.Context, primary uint64,
	manifest *pb.TwoGroupPrepareManifest, lookup TwoGroupHomeLookup) error {
	err := c.proposeTwoGroupPrepare(ctx, primary, manifest, TwoGroupAdmissionObservation{})
	if err == nil {
		return nil
	}
	// A lost reply may conceal an applied home PREPARE. Only the current
	// authenticated home leader can reconcile that outcome.
	if _, lookupErr := ObserveTwoGroupHome(ctx, lookup, manifest); lookupErr != nil {
		return errors.Join(err, lookupErr)
	}
	return nil
}

func (c *ShardedCoordinator) proposeTwoGroupPrepare(ctx context.Context, gid uint64,
	manifest *pb.TwoGroupPrepareManifest, observed TwoGroupAdmissionObservation) error {
	proposer, term, err := c.localTwoGroupProposer(ctx, gid)
	if err != nil {
		if !errors.Is(err, raftengine.ErrNotLeader) || c.twoGroupRemotePrepare == nil {
			return err
		}
		remote, resolveErr := c.twoGroupRemotePrepare(gid)
		if resolveErr != nil || remote == nil {
			return errors.Join(ErrTwoGroupAdmission, resolveErr)
		}
		return errors.Wrap(remote.PrepareTwoGroupOnLeader(ctx, gid, manifest), "remote two-group PREPARE")
	}
	_, err = proposeTwoGroupPreparedEntry(ctx, proposer, term, gid, manifest, observed)
	return err
}

// ProposeTwoGroupPrepareOnLeader is called only after private peer-session
// authentication. The secondary performs its own authenticated home lookup;
// the RPC caller never supplies an observation or a Raft command.
func ProposeTwoGroupPrepareOnLeader(ctx context.Context, gid uint64, manifest *pb.TwoGroupPrepareManifest,
	leader raftengine.LeaderView, proposer raftengine.TermFencedProposer, lookup TwoGroupHomeLookup,
	checkSession func(context.Context) error) (uint64, uint64, error) {
	reader, term, err := twoGroupLeaderTerm(ctx, gid, manifest, leader, proposer, checkSession)
	if err != nil {
		return 0, 0, err
	}
	observed, err := twoGroupLeaderObservation(ctx, gid, manifest, lookup)
	if err != nil {
		return 0, 0, err
	}
	if err := checkSession(ctx); err != nil {
		return 0, 0, err
	}
	if current := reader.Status(); current.State != raftengine.StateLeader || current.Term != term {
		return 0, 0, errors.WithStack(raftengine.ErrStaleAdmissionTerm)
	}
	index, err := proposeTwoGroupPreparedEntry(ctx, proposer, term, gid, manifest, observed)
	return index, term, err
}

func twoGroupLeaderTerm(ctx context.Context, gid uint64, manifest *pb.TwoGroupPrepareManifest,
	leader raftengine.LeaderView, proposer raftengine.TermFencedProposer,
	checkSession func(context.Context) error) (raftengine.StatusReader, uint64, error) {
	if _, _, _, err := canonicalTwoGroupManifest(manifest); err != nil || gid == 0 || leader == nil || proposer == nil || checkSession == nil {
		return nil, 0, errors.Join(ErrTwoGroupAdmission, err)
	}
	reader, ok := leader.(raftengine.StatusReader)
	if !ok || leader.State() != raftengine.StateLeader {
		return nil, 0, errors.WithStack(raftengine.ErrNotLeader)
	}
	term := reader.Status().Term
	if term == 0 {
		return nil, 0, errors.WithStack(raftengine.ErrStaleAdmissionTerm)
	}
	if err := leader.VerifyLeader(ctx); err != nil {
		return nil, 0, errors.Wrap(err, "verify two-group leader")
	}
	return reader, term, nil
}

func twoGroupLeaderObservation(ctx context.Context, gid uint64, manifest *pb.TwoGroupPrepareManifest,
	lookup TwoGroupHomeLookup) (TwoGroupAdmissionObservation, error) {
	if gid == manifest.GetPrimaryGroupId() {
		return TwoGroupAdmissionObservation{}, nil
	}
	if lookup == nil {
		return TwoGroupAdmissionObservation{}, errors.WithStack(ErrTwoGroupAdmission)
	}
	return ObserveTwoGroupHome(ctx, lookup, manifest)
}

func proposeTwoGroupPreparedEntry(ctx context.Context, proposer raftengine.TermFencedProposer, term, gid uint64,
	manifest *pb.TwoGroupPrepareManifest, observed TwoGroupAdmissionObservation) (uint64, error) {
	data, err := marshalTwoGroupPrepareEntry(gid, manifest, observed)
	if err != nil {
		return 0, err
	}
	result, err := proposer.ProposeIfTerm(ctx, term, data)
	if err != nil || result == nil {
		return 0, errors.Join(ErrTransactionOutcomeUnknown, err)
	}
	if result.CommitIndex == 0 {
		return 0, errors.WithStack(ErrTransactionOutcomeUnknown)
	}
	if result.Response != nil {
		if applyErr, ok := result.Response.(error); ok {
			return 0, errors.WithStack(applyErr)
		}
		return 0, errors.WithStack(ErrRaftEntryApply)
	}
	return result.CommitIndex, nil
}

func (c *ShardedCoordinator) localTwoGroupProposer(ctx context.Context, gid uint64) (raftengine.TermFencedProposer, uint64, error) {
	g, err := c.txnGroupForID(gid)
	if err != nil {
		return nil, 0, err
	}
	if g.Engine == nil || g.Engine.State() != raftengine.StateLeader {
		return nil, 0, errors.WithStack(raftengine.ErrNotLeader)
	}
	reader, ok := g.Engine.(raftengine.StatusReader)
	if !ok {
		return nil, 0, errors.WithStack(raftengine.ErrStaleAdmissionTerm)
	}
	term := reader.Status().Term
	if term == 0 {
		return nil, 0, errors.WithStack(raftengine.ErrStaleAdmissionTerm)
	}
	if err := g.Engine.VerifyLeader(ctx); err != nil {
		return nil, 0, errors.Wrap(err, "verify two-group leader")
	}
	proposer, ok := g.Proposer().(raftengine.TermFencedProposer)
	if !ok {
		return nil, 0, errors.WithStack(raftengine.ErrStaleAdmissionTerm)
	}
	return proposer, term, nil
}
