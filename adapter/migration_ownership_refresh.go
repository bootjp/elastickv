package adapter

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const (
	ownershipRefreshInterval = 5 * time.Second
	ownershipRefreshBudget   = 5 * time.Second
	ownershipRefreshBatch    = 4
	ownershipRefreshRetry    = time.Minute
)

type MigrationOwnershipAttestor interface {
	AttestMigrationOwnership(context.Context, *pb.AttestMigrationOwnershipRequest, ...grpc.CallOption) (*pb.AttestMigrationOwnershipResponse, error)
}

type MigrationOwnershipAttestorFactory func(context.Context) (MigrationOwnershipAttestor, error)

type migrationOwnershipRefresh struct {
	mu               sync.Mutex
	groups           map[uint64]*kv.ShardGroup
	root             MigrationOwnershipAttestorFactory
	next             map[uint64]int
	retry            map[ownershipRefreshKey]ownershipRefreshFailure
	acked            map[ownershipRefreshKey]ownershipRefreshAck
	groupNext        int
	partitionRuntime *migrationPartitionRuntime
}

type ownershipRefreshKey struct{ group, job uint64 }

type ownershipRefreshFailure struct {
	prior  *pb.MigrationOwnershipProof
	mirror uint64
	after  time.Time
}

// ACK caching suppresses redundant probes only. No serving/proposal verdict
// consumes this process-local scheduling state; restart probes the proof again.
type ownershipRefreshAck struct {
	proof       *pb.MigrationOwnershipProof
	index, term uint64
}

// WithMigrationOwnershipRefresh installs leader-local enumeration, not a root
// proof registry. Native proofs survive both job GC and guard retirement.
func WithMigrationOwnershipRefresh(groups map[uint64]*kv.ShardGroup, root MigrationOwnershipAttestorFactory) DistributionServerOption {
	return func(s *DistributionServer) {
		local := make(map[uint64]*kv.ShardGroup, len(groups))
		for id, group := range groups {
			local[id] = group
		}
		s.ownershipRefresh = &migrationOwnershipRefresh{groups: local, root: root, next: make(map[uint64]int), retry: make(map[ownershipRefreshKey]ownershipRefreshFailure), acked: make(map[ownershipRefreshKey]ownershipRefreshAck)}
	}
}

// MigrationOwnershipRefreshConfigured reports installation only, not a serving
// verdict. Runtime capability must also check the actual read/proposal hooks.
func (s *DistributionServer) MigrationOwnershipRefreshConfigured(groupID uint64) bool {
	if s == nil || groupID == 0 || s.ownershipRefresh == nil || s.ownershipRefresh.root == nil {
		return false
	}
	group := s.ownershipRefresh.groups[groupID]
	return group != nil && group.Store != nil && group.Engine != nil && s.splitMigrationGroupFactory != nil && s.splitMigrationVoterFactory != nil
}

func (s *DistributionServer) RunMigrationOwnershipRefresh(ctx context.Context) error {
	if s.ownershipRefresh == nil {
		return nil
	}
	ticker := time.NewTicker(ownershipRefreshInterval)
	defer ticker.Stop()
	for {
		if err := s.RunMigrationOwnershipRefreshOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Debug("migration ownership refresh deferred", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (s *DistributionServer) RunMigrationOwnershipRefreshOnce(ctx context.Context) error {
	if s.ownershipRefresh != nil && s.ownershipRefresh.partitionRuntime != nil {
		return s.runMigrationPartitionRefreshOnce(ctx)
	}
	if s.ownershipRefresh == nil || s.ownershipRefresh.root == nil || s.splitJobRunnerReadinessGate == nil {
		return errors.WithStack(store.ErrNotSupported)
	}
	if err := s.requireReadReady(); err != nil {
		return err
	}
	if err := s.splitJobRunnerReadinessGate(ctx); err != nil {
		return err
	}
	attempt, cancel := context.WithTimeout(ctx, ownershipRefreshBudget)
	defer cancel()
	r := s.ownershipRefresh
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]uint64, 0, len(r.groups))
	for id := range r.groups {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return s.refreshMigrationOwnershipGroups(attempt, ids)
}

func (s *DistributionServer) refreshMigrationOwnershipGroups(ctx context.Context, ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}
	r := s.ownershipRefresh
	start := r.groupNext % len(ids)
	var result error
	for n := range len(ids) {
		at := (start + n) % len(ids)
		r.groupNext = (at + 1) % len(ids)
		id := ids[at]
		if err := s.refreshMigrationOwnershipGroup(ctx, id, r.groups[id]); err != nil {
			result = errors.CombineErrors(result, err)
		}
		if ctx.Err() != nil {
			break
		}
	}
	return errors.WithStack(result)
}

func migrationOwnershipRefreshLeader(id uint64, group *kv.ShardGroup) bool {
	return id != 0 && group != nil && group.Engine != nil && group.Store != nil && group.Engine.State() == raftengine.StateLeader
}

func (s *DistributionServer) refreshMigrationOwnershipGroup(ctx context.Context, id uint64, group *kv.ShardGroup) error {
	if !migrationOwnershipRefreshLeader(id, group) {
		return nil
	}
	reader, ok := group.Store.(store.MigrationOwnershipStore)
	if !ok {
		return errors.WithStack(store.ErrNotSupported)
	}
	if _, err := group.Engine.LinearizableRead(ctx); err != nil {
		return errors.WithStack(err)
	}
	view, err := reader.MigrationServingState(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	if len(view.Ownership) == 0 {
		return nil
	}
	if _, valid := migrationOwnershipGeneration(view.Ownership); !valid {
		return errors.WithStack(store.ErrMigrationOwnership)
	}
	index, err := ownershipRefreshConfiguration(ctx, group)
	if err != nil {
		return err
	}
	return s.refreshMigrationOwnershipBatch(ctx, id, group, view.Ownership, index)
}

func (s *DistributionServer) refreshMigrationOwnershipBatch(ctx context.Context, id uint64, group *kv.ShardGroup, states []store.MigrationOwnershipState, index uint64) error {
	r := s.ownershipRefresh
	start := r.next[id] % len(states)
	var result error
	for n := range min(ownershipRefreshBatch, len(states)) {
		at := (start + n) % len(states)
		r.next[id] = (at + 1) % len(states)
		if err := s.refreshMigrationOwnershipProof(ctx, id, group, states[at], index); err != nil {
			result = errors.CombineErrors(result, err)
		}
		if ctx.Err() != nil {
			break
		}
	}
	return errors.WithStack(result)
}

func ownershipRefreshConfiguration(ctx context.Context, group *kv.ShardGroup) (uint64, error) {
	reader, ok := group.Engine.(raftengine.AppliedConfigurationReader)
	if !ok {
		return 0, errors.WithStack(store.ErrNotSupported)
	}
	index, supported := reader.AppliedConfigurationIndex()
	if !supported || index == 0 {
		return 0, errors.WithStack(kv.ErrMigrationConfigurationChanged)
	}
	durable, err := kv.MigrationConfigurationIndex(ctx, group.Store)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if durable != index {
		return 0, errors.WithStack(kv.ErrMigrationConfigurationChanged)
	}
	return index, nil
}

func (s *DistributionServer) refreshMigrationOwnershipProof(ctx context.Context, id uint64, group *kv.ShardGroup, state store.MigrationOwnershipState, index uint64) error {
	if state.LocalGroupID != id {
		return errors.WithStack(store.ErrMigrationOwnership)
	}
	prior := kv.MigrationOwnershipProofFromState(state)
	var mirror uint64
	if s.engine != nil {
		mirror = s.engine.Version()
	}
	key := ownershipRefreshKey{group: id, job: state.JobID}
	term := group.Engine.Status().Term
	if s.ownershipRefresh.deferred(key, prior, mirror) || s.ownershipRefresh.alreadyAcknowledged(key, prior, mirror, index, term) {
		return nil
	}
	if err := s.verifyOwnershipRefreshVoters(ctx, state.JobID, id, index, prior, false); err != nil {
		return err
	}
	// A mirror can trigger an attempt, but cannot authorize changing any field.
	if mirror <= state.CatalogVersion {
		return s.acknowledgeOwnershipRefresh(ctx, key, group, index, term, prior)
	}
	proof, err := s.fetchOwnershipRefreshAttempt(ctx, key, prior, mirror)
	if err != nil {
		return err
	}
	return s.finishOwnershipRefresh(ctx, key, group, index, term, proof)
}

func (s *DistributionServer) finishOwnershipRefresh(ctx context.Context, key ownershipRefreshKey, group *kv.ShardGroup, index, term uint64, proof *pb.MigrationOwnershipProof) error {
	if err := group.Engine.VerifyLeader(ctx); err != nil {
		return errors.WithStack(err)
	}
	current, err := ownershipRefreshConfiguration(ctx, group)
	if err != nil {
		return err
	}
	if current != index {
		return errors.WithStack(kv.ErrMigrationConfigurationChanged)
	}
	if err := s.replicateOwnershipRefresh(ctx, key.job, key.group, index, proof); err != nil {
		return err
	}
	return s.acknowledgeOwnershipRefresh(ctx, key, group, index, term, proof)
}

func (s *DistributionServer) fetchOwnershipRefreshAttempt(ctx context.Context, key ownershipRefreshKey, prior *pb.MigrationOwnershipProof, mirror uint64) (*pb.MigrationOwnershipProof, error) {
	proof, err := s.fetchOwnershipRefresh(ctx, key.job, prior)
	if err != nil {
		s.ownershipRefresh.retry[key] = ownershipRefreshFailure{prior: proto.CloneOf(prior), mirror: mirror, after: time.Now().Add(ownershipRefreshRetry)}
		return nil, err
	}
	delete(s.ownershipRefresh.retry, key)
	return proof, nil
}

func (s *DistributionServer) acknowledgeOwnershipRefresh(ctx context.Context, key ownershipRefreshKey, group *kv.ShardGroup, index, term uint64, proof *pb.MigrationOwnershipProof) error {
	if err := s.verifyOwnershipRefreshVoters(ctx, key.job, key.group, index, proof, true); err != nil {
		return err
	}
	current, err := ownershipRefreshConfiguration(ctx, group)
	if err != nil {
		return err
	}
	if current != index || group.Engine.Status().Term != term {
		return errors.WithStack(kv.ErrMigrationConfigurationChanged)
	}
	s.ownershipRefresh.acked[key] = ownershipRefreshAck{proof: proto.CloneOf(proof), index: index, term: term}
	return nil
}

func (r *migrationOwnershipRefresh) deferred(key ownershipRefreshKey, prior *pb.MigrationOwnershipProof, mirror uint64) bool {
	failed, found := r.retry[key]
	return found && failed.mirror == mirror && proto.Equal(failed.prior, prior) && time.Now().Before(failed.after)
}

func (r *migrationOwnershipRefresh) alreadyAcknowledged(key ownershipRefreshKey, proof *pb.MigrationOwnershipProof, mirror, index, term uint64) bool {
	ack, found := r.acked[key]
	return found && mirror <= proof.GetCatalogVersion() && ack.index == index && ack.term == term && proto.Equal(ack.proof, proof)
}

func (s *DistributionServer) fetchOwnershipRefresh(ctx context.Context, jobID uint64, prior *pb.MigrationOwnershipProof) (*pb.MigrationOwnershipProof, error) {
	client, err := s.ownershipRefresh.root(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if client == nil {
		return nil, errors.WithStack(store.ErrNotSupported)
	}
	attempt, cancel := context.WithTimeout(ctx, defaultSplitMigrationAttemptTimeout)
	defer cancel()
	resp, err := client.AttestMigrationOwnership(attempt, &pb.AttestMigrationOwnershipRequest{JobId: jobID, Prior: prior})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if resp.GetOwnershipProtocol() != migrationOwnershipProtocol || resp.GetOwnership() == nil {
		return nil, errors.WithStack(store.ErrNotSupported)
	}
	proof := resp.GetOwnership()
	expected := proto.CloneOf(prior)
	expected.CatalogVersion = proof.GetCatalogVersion()
	if proof.GetCatalogVersion() <= prior.GetCatalogVersion() || !proto.Equal(expected, proof) {
		return nil, errors.WithStack(store.ErrMigrationOwnership)
	}
	return proof, nil
}

func (s *DistributionServer) replicateOwnershipRefresh(ctx context.Context, jobID, groupID, index uint64, proof *pb.MigrationOwnershipProof) error {
	if s.splitMigrationGroupFactory == nil {
		return errors.WithStack(store.ErrNotSupported)
	}
	client, err := s.splitMigrationGroupFactory(ctx, groupID)
	if err != nil {
		return err
	}
	if client == nil {
		return errors.WithStack(store.ErrNotSupported)
	}
	resp, err := cleanupSplitMigration(ctx, client, &pb.CleanupMigrationRequest{JobId: jobID,
		Mode: pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_REFRESH, Ownership: proof,
		RequireConfigurationIndex: true, ExpectedConfigurationIndex: index})
	if err != nil {
		return err
	}
	if !resp.GetDone() {
		return errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
	}
	return nil
}

func (s *DistributionServer) verifyOwnershipRefreshVoters(ctx context.Context, jobID, groupID, index uint64, proof *pb.MigrationOwnershipProof, applied bool) error {
	if s.splitMigrationVoterFactory == nil {
		return errors.WithStack(store.ErrNotSupported)
	}
	voters, err := s.splitMigrationVoters(ctx, groupID, nil)
	if err != nil {
		return err
	}
	for _, voter := range voters {
		if err := probeOwnershipRefreshVoter(ctx, voter, jobID, index, proof, applied); err != nil {
			return err
		}
	}
	return nil
}

func probeOwnershipRefreshVoter(ctx context.Context, voter SplitMigrationVoter, jobID, index uint64, proof *pb.MigrationOwnershipProof, applied bool) error {
	kind := pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_CAPABILITY
	if applied {
		kind = pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_APPLIED
	}
	req := &pb.ProbeMigrationStateRequest{JobId: jobID, Kind: kind, RequireConfigurationIndex: true, ExpectedConfigurationIndex: index}
	if applied {
		req.Ownership = proof
	}
	attempt, cancel := context.WithTimeout(ctx, defaultSplitMigrationVoterProbeTimeout)
	defer cancel()
	resp, err := voter.Client.ProbeMigrationState(attempt, req)
	if err != nil {
		return errors.WithStack(err)
	}
	if !resp.GetReady() || resp.GetOwnershipProtocol() != migrationOwnershipProtocol ||
		resp.GetAppliedConfigurationIndex() != index || resp.GetOwnershipGeneration() != proof.GetGeneration() {
		return errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
	}
	if applied && !proto.Equal(resp.GetOwnership(), proof) {
		return errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
	}
	return nil
}
