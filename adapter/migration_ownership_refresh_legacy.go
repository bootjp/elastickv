package adapter

import (
	"bytes"
	"context"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

// Legacy records with unchanged geometry keep their immutable per-job ledger.
// This path never manufactures a partition ID, clips a proof or trusts a cache.
func (s *DistributionServer) reconcileLegacyOwnershipPartition(ctx context.Context, id uint64, g *kv.ShardGroup, index uint64, members []SplitMigrationVoter, before store.MigrationOwnershipState) error {
	proof, err := s.fetchUnchangedLegacyPartition(ctx, before)
	if err != nil {
		return err
	}
	term := g.Engine.Status().Term
	if err := verifyPartitionConsumerEpoch(ctx, g, index, term); err != nil {
		return err
	}
	if proof.GetCatalogVersion() != before.CatalogVersion || before.ConfigurationIndex != index {
		if err := s.replicateOwnershipRefresh(ctx, before.JobID, id, index, proof); err != nil {
			return err
		}
	}
	for _, member := range members {
		if err := probeOwnershipRefreshVoter(ctx, member, before.JobID, index, proof, true); err != nil {
			return err
		}
	}
	if err := verifyLegacyPartitionLocalRecord(ctx, g.Store, before, proof, index); err != nil {
		return err
	}
	if err := verifyPartitionConsumerEpoch(ctx, g, index, term); err != nil {
		return err
	}
	return s.verifyMigrationPartitionMemberSnapshot(ctx, id, members)
}

func (s *DistributionServer) fetchUnchangedLegacyPartition(ctx context.Context, before store.MigrationOwnershipState) (*pb.MigrationOwnershipProof, error) {
	if _, err := store.MigrationOwnershipLegacyReferenceFor(before); err != nil {
		return nil, errors.WithStack(err)
	}
	client, err := s.ownershipRefresh.root(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if client == nil {
		return nil, errors.WithStack(store.ErrNotSupported)
	}
	prior := kv.MigrationOwnershipProofFromState(before)
	resp, err := client.AttestMigrationOwnership(ctx, &pb.AttestMigrationOwnershipRequest{JobId: before.JobID, Prior: prior})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if resp.GetOwnershipProtocol() != migrationOwnershipProtocol || resp.GetOwnership() == nil {
		return nil, errors.WithStack(store.ErrNotSupported)
	}
	proof := resp.GetOwnership()
	expected := proto.CloneOf(prior)
	expected.CatalogVersion = proof.GetCatalogVersion()
	if proof.GetCatalogVersion() < prior.GetCatalogVersion() || !proto.Equal(expected, proof) {
		return nil, errors.WithStack(store.ErrMigrationOwnership)
	}
	return proof, nil
}

func verifyLegacyPartitionLocalRecord(ctx context.Context, st store.MVCCStore, before store.MigrationOwnershipState, proof *pb.MigrationOwnershipProof, index uint64) error {
	reader, ok := st.(store.MigrationOwnershipStore)
	if !ok {
		return errors.WithStack(store.ErrNotSupported)
	}
	view, err := reader.MigrationServingState(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, actual := range view.Ownership {
		if actual.JobID != before.JobID {
			continue
		}
		if !legacyPartitionRecordMatches(actual, proof, index) {
			return errors.WithStack(store.ErrMigrationOwnership)
		}
		x, err := kv.EncodeMigrationPartitionJSON(before.Receipts)
		if err != nil {
			return errors.WithStack(err)
		}
		y, err := kv.EncodeMigrationPartitionJSON(actual.Receipts)
		if err != nil {
			return errors.WithStack(err)
		}
		if !bytes.Equal(x, y) {
			return errors.WithStack(store.ErrMigrationOwnership)
		}
		return nil
	}
	return errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
}

func legacyPartitionRecordMatches(actual store.MigrationOwnershipState, proof *pb.MigrationOwnershipProof, index uint64) bool {
	return store.MigrationOwnershipEvidenceValid(actual) && actual.ConfigurationIndex == index && proto.Equal(kv.MigrationOwnershipProofFromState(actual), proof)
}
