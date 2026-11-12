package main

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// Only the catalog reader uses this decorator. The native FSM, encryption
// verifier and registration receiver keep the actual PebbleStore unchanged.
type bootstrapRecoveryCatalogRead struct {
	store.MVCCStore
	onVersion  func()
	corrupt    bool
	extraRoute []byte
	once       sync.Once
}

func (s *bootstrapRecoveryCatalogRead) ScanAt(ctx context.Context, start, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	page, err := s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
	if err != nil {
		return nil, err
	}
	key := distribution.CatalogRouteKey(2)
	if len(s.extraRoute) != 0 && bytes.Compare(start, key) <= 0 && (end == nil || bytes.Compare(key, end) < 0) {
		page = append(page, &store.KVPair{Key: key, Value: bytes.Clone(s.extraRoute)})
	}
	return page, nil
}

func (s *bootstrapRecoveryCatalogRead) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	if bytes.Equal(key, distribution.CatalogVersionKey()) {
		if s.onVersion != nil {
			s.once.Do(s.onVersion)
		}
		if s.corrupt {
			return []byte{255}, nil
		}
	}
	return s.MVCCStore.GetAt(ctx, key, ts)
}

func bootstrapRecoveryCatalogView(node *bootstrapRotationE2ENode, reader store.MVCCStore) []*raftGroupRuntime {
	return []*raftGroupRuntime{{
		GroupRuntime: &noderuntime.GroupRuntime{
			Spec:        node.runtime.Spec,
			Engine:      node.runtime.Engine,
			Store:       reader,
			PublicReady: node.runtime.PublicReady,
		},
		bootstrapDomain: node.domain,
	}}
}

type bootstrapRecoveryOverlap struct {
	reader   *bootstrapRecoveryCatalogRead
	runtimes []*raftGroupRuntime
	routes   *distribution.Engine
	catalog  *distribution.CatalogStore
}

func forceBootstrapRecoveryOverlap(t *testing.T, f *bootstrapRotationE2EFixture, node *bootstrapRotationE2ENode, duringRead func(*distribution.Engine)) bootstrapRecoveryOverlap {
	t.Helper()
	// Hold catalog publication under this test's control, not native apply or
	// registration. The normal production reconciler is invoked once below.
	node.stopReadiness()
	old, err := node.domain.admission.Ticket(1)
	require.NoError(t, err)
	var overlapped bool
	routes := distribution.NewEngine()
	routes.UpdateRoute(nil, nil, 1)
	reader := &bootstrapRecoveryCatalogRead{MVCCStore: node.runtime.Store, onVersion: func() {
		source := bootstrapRegistrationFromContext(node.registrationCtx)
		require.NotNil(t, source)
		sc, err := encryption.ReadSidecar(f.sidecarPath)
		require.NoError(t, err)
		before := node.runtime.Engine.Status().AppliedIndex
		ctx, cancel := context.WithTimeout(node.registrationCtx, enablementE2EWait)
		defer cancel()
		entry := registrationEntry(sc.Active.Raft, f.config.local.Member, node.wiring.raftEpoch)
		req := registrationRequest(sc.Active.Raft, f.config.local.Member, node.wiring.raftEpoch)
		err = source.Propose(ctx, node.group.Proposer(), entry, req)
		require.True(t, err == nil || errors.Is(err, raftadmission.ErrStale) || errors.Is(err, kv.ErrTransactionOutcomeUnknown) || errors.Is(err, context.Canceled) || errors.Is(err, raftengine.ErrLeadershipLost), "registration may lose its reply during native recovery: %v", err)
		require.Eventually(t, func() bool { return node.runtime.Engine.Status().AppliedIndex > before }, enablementE2EWait, time.Millisecond)
		require.Eventually(t, func() bool { return node.domain.admission.Stage(1) == peersession.ControlReady }, enablementE2EWait, time.Millisecond)
		fresh, err := node.domain.admission.Ticket(1)
		require.NoError(t, err)
		// An unknown reply does not prove this duplicate registration committed;
		// the applied prefix and fresh ticket prove actual native recovery.
		require.NotEqual(t, old, fresh, "actual registration or native prefix/term recovery must invalidate the catalog ticket")
		overlapped = true
		if duringRead != nil {
			duringRead(routes)
		}
	}}
	runtimes := bootstrapRecoveryCatalogView(node, reader)
	catalog, err := setupDistributionCatalog(t.Context(), runtimes, routes)
	require.True(t, overlapped)
	require.NoError(t, err, "initial startup must defer stale publication to the production reconciler, not fail the process")
	require.NotNil(t, catalog)
	require.False(t, node.runtime.PublicReady(), "a discarded ticket cannot mark catalog loaded")
	_, err = node.domain.admission.RotationStatus()
	require.NoError(t, err, "stale initial catalog publication must not latch fatal")
	require.ErrorIs(t, node.domain.admission.MarkCatalogLoaded(old), raftadmission.ErrStale)
	return bootstrapRecoveryOverlap{reader: reader, runtimes: runtimes, routes: routes, catalog: catalog}
}

func TestBootstrapRecoveryCatalogSnapshotRegistrationOverlap(t *testing.T) {
	t.Setenv("ELASTICKV_RAFT_SNAPSHOT_COUNT", "1")
	f := newBootstrapRotationE2EFixture(t, true)
	node := f.protected(t)
	overlap := forceBootstrapRecoveryOverlap(t, f, node, nil)
	require.NoError(t, reconcileBootstrapReadiness(t.Context(), node.domain, overlap.runtimes, overlap.catalog, overlap.routes))
	require.True(t, node.runtime.PublicReady())
	ctx, cancel := context.WithTimeout(t.Context(), enablementE2EWait)
	defer cancel()
	_, err := node.coordinate.Dispatch(ctx, &kv.OperationGroup[kv.OP]{Elems: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("recovery-overlap"), Value: []byte("durable-after-race")}}})
	require.NoError(t, err)
	node.read(t, "recovery-overlap", "durable-after-race")
	node.close()
	restarted := f.protected(t)
	restarted.read(t, "recovery-overlap", "durable-after-race")
}

func TestBootstrapRecoveryCatalogSameVersionMalformedReload(t *testing.T) {
	t.Setenv("ELASTICKV_RAFT_SNAPSHOT_COUNT", "1")
	f := newBootstrapRotationE2EFixture(t, true)
	node := f.protected(t)
	overlap := forceBootstrapRecoveryOverlap(t, f, node, nil)
	version, err := overlap.catalog.Version(t.Context())
	require.NoError(t, err)
	require.Equal(t, version, overlap.routes.AppliedCatalogSnapshot().Version, "the stale initial read already installed this engine version")
	overlap.reader.extraRoute, err = distribution.EncodeRouteDescriptor(distribution.RouteDescriptor{RouteID: 2, Start: []byte("m"), GroupID: 1, State: distribution.RouteStateActive})
	require.NoError(t, err)
	// Each record has valid canonical bytes and a distinct ID, but the new
	// route overlaps the existing full-range route at the SAME version.
	require.ErrorIs(t, reconcileBootstrapReadiness(t.Context(), node.domain, overlap.runtimes, overlap.catalog, overlap.routes), distribution.ErrEngineSnapshotRouteOrder)
	require.False(t, node.runtime.PublicReady())
}

func TestBootstrapRecoveryCatalogNewerWatcherVersionDefersUntilLocalCommit(t *testing.T) {
	t.Setenv("ELASTICKV_RAFT_SNAPSHOT_COUNT", "1")
	f := newBootstrapRotationE2EFixture(t, true)
	node := f.protected(t)
	overlap := forceBootstrapRecoveryOverlap(t, f, node, func(routes *distribution.Engine) {
		latest, err := distribution.NewCatalogStore(node.runtime.Store).Snapshot(t.Context())
		require.NoError(t, err)
		latest.Version++
		require.NoError(t, routes.ApplySnapshot(latest), "model an independently validated watcher publication, not local durable catchup")
	})
	require.NoError(t, reconcileBootstrapReadiness(t.Context(), node.domain, overlap.runtimes, overlap.catalog, overlap.routes))
	require.False(t, node.runtime.PublicReady(), "a deferred older local read cannot mint a loaded receipt")
	target := overlap.routes.AppliedCatalogSnapshot().Version
	commitBootstrapRecoveryCatalogVersion(t, f, node, target)
	require.NoError(t, reconcileBootstrapReadiness(t.Context(), node.domain, overlap.runtimes, overlap.catalog, overlap.routes))
	require.True(t, node.runtime.PublicReady(), "fresh read of ACTUALLY committed local catalog completes readiness")
	version, err := distribution.NewCatalogStore(node.runtime.Store).Version(t.Context())
	require.NoError(t, err)
	require.Equal(t, target, version)
}

func commitBootstrapRecoveryCatalogVersion(t *testing.T, f *bootstrapRotationE2EFixture, node *bootstrapRotationE2ENode, version uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), enablementE2EWait)
	defer cancel()
	owned, op, err := node.domain.admission.Begin(ctx, raftadmission.Catalog)
	require.NoError(t, err)
	defer op.CancelAndJoin()
	barrier, err := bootstrapEncryptionRecoveryBarrier(node.domain, node.runtime)
	require.NoError(t, err)
	index, err := barrier(owned)
	require.NoError(t, err)
	policy, err := kv.LoadAppliedBootstrapPolicy(owned, node.runtime.Store, f.manifest.Identity, index)
	require.NoError(t, err)
	witness, err := policy.Witness()
	require.NoError(t, err)
	start := node.runtime.Store.LastCommitTS() + 1
	commit := start + 1
	key := distribution.CatalogVersionKey()
	request := &pb.Request{IsTxn: true, Ts: start, ReadKeys: [][]byte{key}, Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: key, CommitTS: commit})},
		{Op: pb.Op_PUT, Key: key, Value: distribution.EncodeCatalogVersion(version)},
	}}
	require.NoError(t, kv.StampMutationCommitTS(request.Mutations, commit))
	inner, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	require.NoError(t, err)
	payload, err := kv.EncodeGuardedBootstrapCommand(raftbootstrap.GuardedCommand{Kind: raftbootstrap.ControlGuardedCatalog, Witness: witness,
		OperationID: raftbootstrap.OperationID{98}, Inner: append([]byte{bootstrapPayloadSingle}, inner...)})
	require.NoError(t, err)
	result, err := node.group.Proposer().ProposeAdmin(owned, payload)
	require.NoError(t, finishBootstrapIngress(op, err))
	require.NotNil(t, result)
	receipt, ok := result.Response.(raftbootstrap.GuardedReceipt)
	require.True(t, ok)
	require.Equal(t, raftbootstrap.GuardedReceiptApplied, receipt.Verdict)
}

func TestBootstrapRecoveryCatalogCorruptionRemainsTerminal(t *testing.T) {
	t.Setenv("ELASTICKV_RAFT_SNAPSHOT_COUNT", "1")
	f := newBootstrapRotationE2EFixture(t, true)
	node := f.protected(t)
	node.stopReadiness()
	reader := &bootstrapRecoveryCatalogRead{MVCCStore: node.runtime.Store, corrupt: true}
	routes := distribution.NewEngine()
	routes.UpdateRoute(nil, nil, 1)
	_, err := setupDistributionCatalog(t.Context(), bootstrapRecoveryCatalogView(node, reader), routes)
	require.ErrorIs(t, err, distribution.ErrCatalogInvalidVersionRecord, "only a healthy stale ticket may be deferred")
}
