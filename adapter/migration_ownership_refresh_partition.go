package adapter

import (
	"bytes"
	"context"
	"slices"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type migrationPartitionRuntime struct {
	reads   *kv.ShardStore
	members SplitMigrationVoterFactory
	next    map[uint64]int
}

// WithMigrationPartitionRuntime requires the actual public read/store pairing
// and apply-member discovery (voters AND learners). Install after refresh.
// No boolean activation flag can stand in for these installed integrations.
func WithMigrationPartitionRuntime(reads *kv.ShardStore, members SplitMigrationVoterFactory) DistributionServerOption {
	return func(s *DistributionServer) {
		if s.ownershipRefresh == nil {
			return
		}
		s.ownershipRefresh.partitionRuntime = &migrationPartitionRuntime{reads: reads, members: members, next: make(map[uint64]int)}
	}
}

func (s *DistributionServer) MigrationPartitionRuntimeConfigured(groupID uint64) bool {
	if !s.MigrationOwnershipRefreshConfigured(groupID) {
		return false
	}
	r := s.ownershipRefresh.partitionRuntime
	if r == nil || r.reads == nil || r.members == nil || !r.reads.MigrationPartitionReadAdmissionReady(groupID) {
		return false
	}
	g := s.ownershipRefresh.groups[groupID]
	if !kv.MigrationPartitionRequestAdmissionReady(g.Txn) {
		return false
	}
	_, storeOK := g.Store.(store.MigrationOwnershipPartitionStore)
	_, engineOK := g.Engine.(raftengine.AppliedConfigurationReader)
	return storeOK && engineOK
}

// reconcileMigrationPartitions is the committed-root-ACK -> SOURCE handoff.
// It never assumes this server is root leader. Success includes exact receipt
// ACKs from every current apply member; a changed epoch requires a fresh retry.
func (s *DistributionServer) reconcileMigrationPartitions(ctx context.Context, groupID uint64) error {
	if !s.MigrationPartitionRuntimeConfigured(groupID) {
		return errors.WithStack(store.ErrNotSupported)
	}
	attempt, cancel := context.WithTimeout(ctx, ownershipRefreshBudget)
	defer cancel()
	r := s.ownershipRefresh
	if !r.mu.TryLock() {
		return errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
	}
	defer r.mu.Unlock()
	reader, ok := r.groups[groupID].Store.(store.MigrationOwnershipPartitionStore)
	if !ok {
		return errors.WithStack(store.ErrNotSupported)
	}
	// Offline groups may need several retained manifests. Do not ACK a single
	// intermediate generation as reconciled with the current root geometry.
	for attempt.Err() == nil {
		before, err := reader.MigrationOwnershipPartitions(attempt)
		if err != nil {
			return errors.WithStack(err)
		}
		if _, err := s.reconcileMigrationPartitionGroup(attempt, groupID, 0); err != nil {
			return errors.WithStack(err)
		}
		after, err := reader.MigrationOwnershipPartitions(attempt)
		if err != nil {
			return errors.WithStack(err)
		}
		same, err := sameMigrationPartitionFrontier(before, after)
		if err != nil {
			return err
		}
		if same {
			return nil
		}
	}
	return errors.WithStack(attempt.Err())
}

func sameMigrationPartitionFrontier(before, after store.MigrationOwnershipPartitionState) (bool, error) {
	x, err := kv.EncodeMigrationPartitionJSON(before.Partitions)
	if err != nil {
		return false, errors.WithStack(err)
	}
	y, err := kv.EncodeMigrationPartitionJSON(after.Partitions)
	return bytes.Equal(x, y), errors.WithStack(err)
}

// Background work has its own frontier cursor, independent of immutable job
// enumeration and retry/ACK caches. Job GC cannot remove this obligation.
func (s *DistributionServer) runMigrationPartitionRefreshOnce(ctx context.Context) error {
	attempt, cancel := context.WithTimeout(ctx, ownershipRefreshBudget)
	defer cancel()
	r := s.ownershipRefresh
	if !r.mu.TryLock() {
		return errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
	}
	defer r.mu.Unlock()
	ids := make([]uint64, 0, len(r.groups))
	for id := range r.groups {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	if len(ids) == 0 {
		return nil
	}
	start, remaining := r.groupNext%len(ids), ownershipRefreshBatch
	var result error
	for n := 0; n < len(ids) && remaining > 0 && attempt.Err() == nil; n++ {
		at := (start + n) % len(ids)
		r.groupNext = (at + 1) % len(ids)
		id := ids[at]
		if !migrationOwnershipRefreshLeader(id, r.groups[id]) {
			continue
		}
		if !s.MigrationPartitionRuntimeConfigured(id) {
			result = errors.CombineErrors(result, store.ErrNotSupported)
			continue
		}
		used, err := s.reconcileMigrationPartitionGroup(attempt, id, remaining)
		remaining -= used
		result = errors.CombineErrors(result, err)
	}
	return errors.WithStack(errors.CombineErrors(result, attempt.Err()))
}

type migrationPartitionPrior struct {
	partition *store.MigrationOwnershipPartition
	legacy    *store.MigrationOwnershipState
}

func (p migrationPartitionPrior) geometry() store.MigrationOwnershipPartition {
	if p.partition != nil {
		return *p.partition
	}
	return kv.MigrationPartitionFromLegacy(*p.legacy)
}

func migrationPartitionCandidates(view store.MigrationServingState, state store.MigrationOwnershipPartitionState) []migrationPartitionPrior {
	var result []migrationPartitionPrior
	consumed := make(map[uint64]bool)
	for _, r := range state.Receipts {
		for _, ref := range r.Transition.Before {
			if ref.LegacyJobID != 0 {
				consumed[ref.LegacyJobID] = true
			}
		}
		consumeMigrationPartitionJobOperation(state.Generation, r, consumed)
	}
	for i := range state.Partitions {
		result = append(result, migrationPartitionPrior{partition: &state.Partitions[i]})
	}
	for i := range view.Ownership {
		if !consumed[view.Ownership[i].JobID] || !store.MigrationOwnershipEvidenceValid(view.Ownership[i]) {
			result = append(result, migrationPartitionPrior{legacy: &view.Ownership[i]})
		}
	}
	return result
}

func consumeMigrationPartitionJobOperation(generation string, receipt store.MigrationOwnershipPartitionReceipt, consumed map[uint64]bool) {
	if receipt.Transition.Operation != store.MigrationOwnershipPartitionJobOperation {
		return
	}
	// Validate historical outputs against their own immutable receipt, not the
	// current frontier: a later split may already have replaced these children.
	historical := store.MigrationOwnershipPartitionState{Generation: generation, Partitions: receipt.Transition.After, Receipts: []store.MigrationOwnershipPartitionReceipt{receipt}}
	for _, p := range receipt.Transition.After {
		if store.MigrationOwnershipPartitionEvidenceValid(historical, p) {
			consumed[p.MigrationJobID] = true
		}
	}
}

func partitionRangesOverlap(a, b store.MigrationOwnershipPartition) bool {
	return (len(a.RouteEnd) == 0 || bytes.Compare(b.RouteStart, a.RouteEnd) < 0) && (len(b.RouteEnd) == 0 || bytes.Compare(a.RouteStart, b.RouteEnd) < 0)
}

func migrationPartitionPriorClosure(candidates []migrationPartitionPrior, at int) ([]store.MigrationOwnershipPartition, []store.MigrationOwnershipState, []int) {
	selected := map[int]bool{at: true}
	for changed := true; changed; {
		changed = false
		for i, p := range candidates {
			if selected[i] {
				continue
			}
			for j := range selected {
				if partitionRangesOverlap(p.geometry(), candidates[j].geometry()) {
					selected[i], changed = true, true
					break
				}
			}
		}
	}
	var partitions []store.MigrationOwnershipPartition
	var legacy []store.MigrationOwnershipState
	var indices []int
	for i, p := range candidates {
		if !selected[i] {
			continue
		}
		indices = append(indices, i)
		if p.partition != nil {
			partitions = append(partitions, *p.partition)
		} else {
			legacy = append(legacy, *p.legacy)
		}
	}
	return partitions, legacy, indices
}

func (s *DistributionServer) reconcileMigrationPartitionGroup(ctx context.Context, id uint64, limit int) (int, error) {
	g := s.ownershipRefresh.groups[id]
	if !migrationPartitionLocalGroupPresent(id, g) {
		return 0, errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
	}
	view, state, index, err := readMigrationPartitionGroup(ctx, g)
	if err != nil {
		return 0, err
	}
	candidates := migrationPartitionCandidates(view, state)
	if len(candidates) == 0 {
		return 0, s.verifyEmptyMigrationPartitionGroup(ctx, id, index)
	}
	r := s.ownershipRefresh.partitionRuntime
	start := r.next[id] % len(candidates)
	background := limit > 0
	if limit == 0 {
		limit = len(candidates)
	}
	visited := make(map[int]bool)
	used := 0
	var result error
	for n := 0; n < len(candidates) && used < limit; n++ {
		at := (start + n) % len(candidates)
		r.next[id] = (at + 1) % len(candidates)
		if visited[at] {
			continue
		}
		partitions, legacy, indices := migrationPartitionPriorClosure(candidates, at)
		for _, i := range indices {
			visited[i] = true
		}
		used++
		err := s.reconcileMigrationPartitionPrior(ctx, id, g, index, state, partitions, legacy)
		result = errors.CombineErrors(result, migrationPartitionBackgroundResult(err, background))
		if ctx.Err() != nil {
			break
		}
	}
	return used, errors.WithStack(errors.CombineErrors(result, ctx.Err()))
}

func migrationPartitionLocalGroupPresent(id uint64, g *kv.ShardGroup) bool {
	return id != 0 && g != nil && g.Store != nil && g.Engine != nil
}

func readMigrationPartitionGroup(ctx context.Context, g *kv.ShardGroup) (store.MigrationServingState, store.MigrationOwnershipPartitionState, uint64, error) {
	var view store.MigrationServingState
	var state store.MigrationOwnershipPartitionState
	if g.Engine.State() == raftengine.StateLeader {
		if _, err := g.Engine.LinearizableRead(ctx); err != nil {
			return view, state, 0, errors.WithStack(err)
		}
	}
	index, err := ownershipRefreshConfiguration(ctx, g)
	if err != nil {
		return view, state, 0, err
	}
	reader, ok := g.Store.(store.MigrationOwnershipStore)
	if !ok {
		return view, state, 0, errors.WithStack(store.ErrNotSupported)
	}
	view, err = reader.MigrationServingState(ctx)
	if err != nil {
		return view, state, 0, errors.WithStack(err)
	}
	partitionReader, ok := g.Store.(store.MigrationOwnershipPartitionStore)
	if !ok {
		return view, state, 0, errors.WithStack(store.ErrNotSupported)
	}
	state, err = partitionReader.MigrationOwnershipPartitions(ctx)
	return view, state, index, errors.WithStack(err)
}

func (s *DistributionServer) partitionApplyMembers(ctx context.Context, id uint64) ([]SplitMigrationVoter, error) {
	r := s.ownershipRefresh.partitionRuntime
	if r == nil || r.members == nil {
		return nil, errors.WithStack(store.ErrNotSupported)
	}
	members, err := r.members(ctx, id)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	members = slices.Clone(members)
	if len(members) == 0 {
		return nil, errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
	}
	if err := validateSplitMigrationVoters(members); err != nil {
		return nil, errors.WithStack(err)
	}
	return members, nil
}

func (s *DistributionServer) reconcileMigrationPartitionPrior(ctx context.Context, id uint64, g *kv.ShardGroup, index uint64, state store.MigrationOwnershipPartitionState, partitions []store.MigrationOwnershipPartition, legacy []store.MigrationOwnershipState) error {
	var prior store.MigrationOwnershipPartition
	if len(partitions) > 0 {
		prior = partitions[0]
	} else {
		prior = kv.MigrationPartitionFromLegacy(legacy[0])
	}
	if prior.LocalGroupID != id {
		return errors.WithStack(store.ErrMigrationOwnership)
	}
	members, err := s.partitionMembersWithNamespace(ctx, id)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := verifyPartitionApplyCapabilities(ctx, members, prior.MigrationJobID, index, prior.Generation); err != nil {
		return err
	}
	c, err := s.fetchMigrationPartitionAttestation(ctx, partitions, legacy, index)
	if err != nil {
		return s.reconcileMigrationPartitionFailure(ctx, id, g, index, members, legacy, err, partitions)
	}
	term := g.Engine.Status().Term
	transition := c.Transition
	unchanged := migrationPartitionControlUnchanged(c, len(partitions), len(legacy), prior, index)
	if unchanged {
		transition, err = currentMigrationPartitionReceipt(state, prior.ReceiptID)
		if err != nil {
			return err
		}
	} else {
		if err := verifyPartitionConsumerEpoch(ctx, g, index, term); err != nil {
			return errors.WithStack(err)
		}
		if err := s.replicateMigrationPartition(ctx, id, index, prior.MigrationJobID, c); err != nil {
			return err
		}
	}
	return s.finishMigrationPartitionAcknowledgement(ctx, id, g, index, term, members, prior.MigrationJobID, transition)
}

func (s *DistributionServer) reconcileMigrationPartitionFailure(ctx context.Context, id uint64, g *kv.ShardGroup, index uint64, members []SplitMigrationVoter, legacy []store.MigrationOwnershipState, err error, partitions []store.MigrationOwnershipPartition) error {
	if deferredMigrationNegativePartition(err, partitions, legacy) {
		return errors.WithStack(ErrMigrationNegativePartitionDeferred)
	}
	if unchangedLegacyPartitionFallback(err, len(partitions), len(legacy)) {
		return s.reconcileLegacyOwnershipPartition(ctx, id, g, index, members, legacy[0])
	}
	return errors.WithStack(err)
}

func unchangedLegacyPartitionFallback(err error, partitions, legacy int) bool {
	return partitions == 0 && legacy == 1 && status.Code(err) == codes.Unimplemented
}

func migrationPartitionControlUnchanged(c kv.MigrationPartitionControl, partitions, legacy int, prior store.MigrationOwnershipPartition, index uint64) bool {
	return c.UnchangedDescriptor != nil && partitions == 1 && legacy == 0 && prior.CatalogVersion == c.Transition.CatalogVersion && prior.ConfigurationIndex == index
}

func (s *DistributionServer) finishMigrationPartitionAcknowledgement(ctx context.Context, id uint64, g *kv.ShardGroup, index, term uint64, members []SplitMigrationVoter, job uint64, transition store.MigrationOwnershipPartitionTransition) error {
	if err := s.acknowledgeMigrationPartition(ctx, members, index, job, transition); err != nil {
		return errors.WithStack(err)
	}
	if err := verifyLocalMigrationPartitionReceipt(ctx, g.Store, transition); err != nil {
		return errors.WithStack(err)
	}
	if err := verifyPartitionConsumerEpoch(ctx, g, index, term); err != nil {
		return errors.WithStack(err)
	}
	return s.verifyMigrationPartitionMemberSnapshot(ctx, id, members)
}

func (s *DistributionServer) verifyMigrationPartitionMemberSnapshot(ctx context.Context, id uint64, members []SplitMigrationVoter) error {
	current, err := s.partitionApplyMembers(ctx, id)
	if err != nil {
		return errors.WithStack(err)
	}
	if len(members) != len(current) {
		return errors.WithStack(kv.ErrMigrationConfigurationChanged)
	}
	for i := range members {
		if members[i].ID != current[i].ID || members[i].Address != current[i].Address {
			return errors.WithStack(kv.ErrMigrationConfigurationChanged)
		}
	}
	return nil
}

func currentMigrationPartitionReceipt(state store.MigrationOwnershipPartitionState, id []byte) (store.MigrationOwnershipPartitionTransition, error) {
	for _, receipt := range state.Receipts {
		if bytes.Equal(receipt.ReceiptID, id) {
			return receipt.Transition, nil
		}
	}
	return store.MigrationOwnershipPartitionTransition{}, errors.WithStack(store.ErrMigrationOwnership)
}

func verifyPartitionApplyCapabilities(ctx context.Context, members []SplitMigrationVoter, job, index uint64, generation string) error {
	for _, member := range members {
		resp, err := probeSplitOwnershipCapability(ctx, member, job)
		if err != nil {
			return err
		}
		if resp.GetOwnershipPartitionProtocol() != MigrationOwnershipPartitionProtocolVersion || resp.GetAppliedConfigurationIndex() != index || resp.GetOwnershipGeneration() != generation {
			return errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
		}
	}
	return nil
}

func (s *DistributionServer) replicateMigrationPartition(ctx context.Context, id, index, job uint64, c kv.MigrationPartitionControl) error {
	client, err := s.splitMigrationGroupFactory(ctx, id)
	if err != nil {
		return err
	}
	if client == nil {
		return errors.WithStack(store.ErrNotSupported)
	}
	raw, err := kv.EncodeMigrationPartitionControl(c)
	if err != nil {
		return errors.WithStack(err)
	}
	resp, err := cleanupSplitMigration(ctx, client, &pb.CleanupMigrationRequest{JobId: job, Mode: pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_PARTITION,
		RequireConfigurationIndex: true, ExpectedConfigurationIndex: index, PartitionTransition: raw})
	if err != nil {
		return err
	}
	if !resp.GetDone() {
		return errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
	}
	return nil
}

func verifyLocalMigrationPartitionReceipt(ctx context.Context, st store.MVCCStore, t store.MigrationOwnershipPartitionTransition) error {
	reader, ok := st.(store.MigrationOwnershipPartitionStore)
	if !ok {
		return errors.WithStack(store.ErrNotSupported)
	}
	state, err := reader.MigrationOwnershipPartitions(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	id, err := store.MigrationOwnershipPartitionReceiptIDFor(t)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, receipt := range state.Receipts {
		if receipt.AppliedIndex != 0 && bytes.Equal(receipt.ReceiptID, id) {
			return nil
		}
	}
	return errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
}

func verifyPartitionConsumerEpoch(ctx context.Context, g *kv.ShardGroup, index, term uint64) error {
	if g.Engine.State() == raftengine.StateLeader {
		if err := g.Engine.VerifyLeader(ctx); err != nil {
			return errors.WithStack(err)
		}
	}
	actual, err := ownershipRefreshConfiguration(ctx, g)
	if err != nil {
		return errors.WithStack(err)
	}
	if actual != index || g.Engine.Status().Term != term {
		return errors.WithStack(kv.ErrMigrationConfigurationChanged)
	}
	return nil
}

func (s *DistributionServer) verifyEmptyMigrationPartitionGroup(ctx context.Context, id, index uint64) error {
	members, err := s.partitionMembersWithNamespace(ctx, id)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, member := range members {
		resp, err := probeSplitOwnershipCapability(ctx, member, 1)
		if err != nil {
			return errors.WithStack(err)
		}
		if resp.GetOwnershipPartitionProtocol() != MigrationOwnershipPartitionProtocolVersion || resp.GetAppliedConfigurationIndex() != index || resp.GetOwnershipGeneration() != "" {
			return errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
		}
	}
	return nil
}

func (s *DistributionServer) partitionMembersWithNamespace(ctx context.Context, id uint64) ([]SplitMigrationVoter, error) {
	members, err := s.partitionApplyMembers(ctx, id)
	if err != nil {
		return nil, err
	}
	client, err := s.splitMigrationGroupFactory(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := EnsureMigrationNamespaceAdmission(ctx, client, members); err != nil {
		return nil, err
	}
	return members, nil
}

func (s *DistributionServer) fetchMigrationPartitionAttestation(ctx context.Context, partitions []store.MigrationOwnershipPartition, legacy []store.MigrationOwnershipState, index uint64) (kv.MigrationPartitionControl, error) {
	var zero kv.MigrationPartitionControl
	client, err := s.ownershipRefresh.root(ctx)
	if err != nil {
		return zero, errors.WithStack(err)
	}
	if client == nil {
		return zero, errors.WithStack(store.ErrNotSupported)
	}
	req, err := migrationPartitionAttestationRequest(partitions, legacy)
	if err != nil {
		return zero, err
	}
	resp, err := client.AttestMigrationOwnership(ctx, req)
	if err != nil {
		return zero, errors.WithStack(err)
	}
	if resp.GetOwnershipProtocol() != migrationOwnershipProtocol || resp.GetOwnership() != nil {
		return zero, errors.WithStack(store.ErrNotSupported)
	}
	c, err := kv.DecodeMigrationPartitionControl(resp.GetPartitionTransition())
	if err != nil {
		return zero, errors.WithStack(err)
	}
	if err := matchMigrationPartitionAttestationPriors(req, c); err != nil {
		return zero, err
	}
	result, err := kv.BuildMigrationPartitionControl(partitions, legacy, c.Manifest, c.UnchangedDescriptor, c.Transition.CatalogVersion, index)
	return result, errors.WithStack(err)
}

func migrationPartitionAttestationRequest(partitions []store.MigrationOwnershipPartition, legacy []store.MigrationOwnershipState) (*pb.AttestMigrationOwnershipRequest, error) {
	p, err := kv.EncodeMigrationPartitionJSON(partitions)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	l, err := kv.EncodeMigrationPartitionJSON(legacy)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &pb.AttestMigrationOwnershipRequest{PriorPartition: p, PriorOwnershipState: l}, nil
}

func matchMigrationPartitionAttestationPriors(req *pb.AttestMigrationOwnershipRequest, c kv.MigrationPartitionControl) error {
	actual, err := migrationPartitionAttestationRequest(c.PriorPartitions, c.PriorOwnership)
	if err != nil {
		return err
	}
	if !bytes.Equal(req.GetPriorPartition(), actual.GetPriorPartition()) || !bytes.Equal(req.GetPriorOwnershipState(), actual.GetPriorOwnershipState()) {
		return errors.WithStack(store.ErrMigrationOwnership)
	}
	return nil
}

func (s *DistributionServer) acknowledgeMigrationPartition(ctx context.Context, members []SplitMigrationVoter, index, job uint64, t store.MigrationOwnershipPartitionTransition) error {
	raw, err := kv.EncodeMigrationPartitionJSON(t)
	if err != nil {
		return errors.WithStack(err)
	}
	id, err := store.MigrationOwnershipPartitionReceiptIDFor(t)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, member := range members {
		attempt, cancel := context.WithTimeout(ctx, defaultSplitMigrationVoterProbeTimeout)
		resp, probeErr := member.Client.ProbeMigrationState(attempt, &pb.ProbeMigrationStateRequest{JobId: job, Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_PARTITION_APPLIED,
			RequireConfigurationIndex: true, ExpectedConfigurationIndex: index, PartitionTransition: raw})
		cancel()
		if probeErr != nil {
			return errors.WithStack(probeErr)
		}
		if !migrationPartitionAcknowledged(resp, index, t.Generation, id) {
			return errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
		}
	}
	return nil
}

func migrationPartitionAcknowledged(resp *pb.ProbeMigrationStateResponse, index uint64, generation string, id []byte) bool {
	return resp.GetReady() && resp.GetOwnershipPartitionProtocol() == MigrationOwnershipPartitionProtocolVersion && resp.GetOwnershipProtocol() == migrationOwnershipProtocol &&
		resp.GetAppliedConfigurationIndex() == index && resp.GetOwnershipGeneration() == generation && bytes.Equal(resp.GetPartitionReceiptId(), id)
}
