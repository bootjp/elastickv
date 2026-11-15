package kv

import (
	"context"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// decideTwoGroup is a trusted, opt-in local-leader terminal path. Both data
// group leaders must be local; a remote leader leaves the transaction pending.
// It does not install a public Forward route or relax the legacy terminal fence.
func (c *ShardedCoordinator) decideTwoGroup(ctx context.Context, manifest *pb.TwoGroupPrepareManifest, decision pb.Phase) error {
	primary, secondary, lookup, err := c.twoGroupDecisionInputs(manifest, decision)
	if err != nil {
		return err
	}
	if err := c.requireLocalTwoGroupPrepare(ctx, primary, manifest); err != nil {
		return err
	}
	// No primary COMMIT may become durable before the secondary PREPARE.
	if decision == pb.Phase_COMMIT {
		if err := c.requireLocalTwoGroupPrepare(ctx, secondary, manifest); err != nil {
			return err
		}
	}
	if err := c.proposeOrReconcileTwoGroupDecision(ctx, primary, manifest, decision,
		TwoGroupDecisionObservation{}, lookup); err != nil {
		return err
	}
	observed, err := ObserveTwoGroupDecision(ctx, lookup, manifest, decision)
	if err != nil {
		return errors.Join(ErrTransactionOutcomeUnknown, err)
	}
	if err := c.requireLocalTwoGroupPrepare(ctx, secondary, manifest); err != nil {
		return errors.Join(ErrTransactionOutcomeUnknown, err)
	}
	return c.proposeOrReconcileTwoGroupDecision(ctx, secondary, manifest, decision, observed, lookup)
}

func (c *ShardedCoordinator) twoGroupDecisionInputs(manifest *pb.TwoGroupPrepareManifest,
	decision pb.Phase) (uint64, uint64, TwoGroupDecisionLookup, error) {
	if c == nil || c.twoGroupHomeLookup == nil || !validTwoGroupDecision(decision) {
		return 0, 0, nil, errors.WithStack(ErrTwoGroupAdmission)
	}
	if _, _, _, err := canonicalTwoGroupManifest(manifest); err != nil {
		return 0, 0, nil, err
	}
	primary := manifest.GetPrimaryGroupId()
	secondary := manifest.GetParticipants()[0].GetGroupId()
	if secondary == primary {
		secondary = manifest.GetParticipants()[1].GetGroupId()
	}
	lookup, err := c.twoGroupHomeLookup(primary)
	if err != nil {
		return 0, 0, nil, errors.WithStack(err)
	}
	decisionLookup, ok := lookup.(TwoGroupDecisionLookup)
	if !ok {
		return 0, 0, nil, errors.WithStack(ErrTwoGroupAdmission)
	}
	return primary, secondary, decisionLookup, nil
}

func (c *ShardedCoordinator) requireLocalTwoGroupPrepare(ctx context.Context, group uint64, manifest *pb.TwoGroupPrepareManifest) error {
	metadata, err := c.localTwoGroupMetadataAfterBarrier(ctx, group)
	if err != nil {
		return err
	}
	return lookupTwoGroupLocalPrepare(ctx, metadata, manifest, group)
}

func (c *ShardedCoordinator) localTwoGroupMetadataAfterBarrier(ctx context.Context, group uint64) (store.RaftControlMetadataStore, error) {
	g, err := c.txnGroupForID(group)
	if err != nil || g.Store == nil || g.Engine == nil {
		return nil, errors.Join(ErrTwoGroupAdmission, err)
	}
	metadata, ok := g.Store.(store.RaftControlMetadataStore)
	if !ok {
		return nil, errors.WithStack(ErrTwoGroupAdmission)
	}
	reader, term, err := localTwoGroupLeaderTerm(g.Engine)
	if err != nil {
		return nil, err
	}
	if _, err := g.Engine.LinearizableRead(ctx); err != nil {
		return nil, errors.Wrap(err, "read two-group barrier")
	}
	if reader.Status().Term != term || g.Engine.State() != raftengine.StateLeader {
		return nil, errors.WithStack(raftengine.ErrStaleAdmissionTerm)
	}
	return metadata, nil
}

func localTwoGroupLeaderTerm(engine raftengine.Engine) (raftengine.StatusReader, uint64, error) {
	reader, ok := engine.(raftengine.StatusReader)
	if !ok || engine.State() != raftengine.StateLeader {
		return nil, 0, errors.WithStack(raftengine.ErrNotLeader)
	}
	term := reader.Status().Term
	if term == 0 {
		return nil, 0, errors.WithStack(raftengine.ErrStaleAdmissionTerm)
	}
	return reader, term, nil
}

func (c *ShardedCoordinator) proposeOrReconcileTwoGroupDecision(ctx context.Context, group uint64,
	manifest *pb.TwoGroupPrepareManifest, decision pb.Phase, observed TwoGroupDecisionObservation,
	lookup TwoGroupDecisionLookup) error {
	var err error
	if group == manifest.GetPrimaryGroupId() {
		err = c.proposeTwoGroupTerminal(ctx, group, manifest, decision, observed)
		if err == nil {
			return nil
		}
		if _, lookupErr := ObserveTwoGroupDecision(ctx, lookup, manifest, decision); lookupErr == nil {
			return nil
		} else {
			return errors.Join(ErrTransactionOutcomeUnknown, err, lookupErr)
		}
	}
	err = c.proposeTwoGroupTerminal(ctx, group, manifest, decision, observed)
	if err == nil {
		return nil
	}
	if lookupErr := c.requireLocalTwoGroupTerminal(ctx, group, manifest, decision); lookupErr == nil {
		return nil
	} else {
		return errors.Join(ErrTransactionOutcomeUnknown, err, lookupErr)
	}
}

func (c *ShardedCoordinator) requireLocalTwoGroupTerminal(ctx context.Context, group uint64,
	manifest *pb.TwoGroupPrepareManifest, decision pb.Phase) error {
	metadata, err := c.localTwoGroupMetadataAfterBarrier(ctx, group)
	if err != nil {
		return err
	}
	var id [16]byte
	copy(id[:], manifest.GetOperationId())
	raw, err := metadata.ReadRaftControlMetadata(ctx, twoGroupTerminalKey(id))
	if err != nil {
		return errors.Wrap(err, "read local terminal record")
	}
	record, err := parseTwoGroupTerminalRecord(raw)
	if err != nil || !sameTwoGroupDecision(record, manifest, decision) {
		return errors.Join(ErrTwoGroupAdmission, err)
	}
	return nil
}

func (c *ShardedCoordinator) proposeTwoGroupTerminal(ctx context.Context, group uint64,
	manifest *pb.TwoGroupPrepareManifest, decision pb.Phase, observed TwoGroupDecisionObservation) error {
	proposer, term, err := c.localTwoGroupProposer(ctx, group)
	if err != nil {
		return err
	}
	data, err := marshalTwoGroupTerminalEntry(group, manifest, decision, observed)
	if err != nil {
		return err
	}
	result, err := proposer.ProposeIfTerm(ctx, term, data)
	if err != nil || result == nil || result.CommitIndex == 0 {
		return errors.Join(ErrTransactionOutcomeUnknown, err)
	}
	if result.Response != nil {
		if applyErr, ok := result.Response.(error); ok {
			return applyErr
		}
		return errors.WithStack(ErrRaftEntryApply)
	}
	return nil
}
