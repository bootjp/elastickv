package kv

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func guardedNewApplyStores() []struct {
	name string
	new  func(*testing.T) store.MVCCStore
} {
	return []struct {
		name string
		new  func(*testing.T) store.MVCCStore
	}{
		{name: "memory", new: func(t *testing.T) store.MVCCStore {
			t.Helper()
			st := store.NewMVCCStore()
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			return st
		}},
		{name: "pebble", new: func(t *testing.T) store.MVCCStore {
			t.Helper()
			st, err := store.NewPebbleStore(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			return st
		}},
	}
}

func guardedNewPolicy() raftbootstrap.PolicyRecord {
	return raftbootstrap.PolicyRecord{Version: 1, Identity: raftbootstrap.Identity{Cluster: raftbootstrap.UUID{1}, Group: 7},
		BaselineDigest: raftbootstrap.Digest{2}, Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1},
		ActivationIndex: 7, Generation: 1, LastConfigurationIndex: 5,
		Configuration: raftbootstrap.Configuration{Index: 5, State: raftbootstrap.ConfState{Voters: []uint64{1}},
			Members: []raftbootstrap.Member{{ID: 1, Address: "node1:9000", PublicKeyDigest: raftbootstrap.Digest{3}}}}}
}

func stageGuardedNewPolicy(t *testing.T, ctx context.Context, st store.MVCCStore, policy raftbootstrap.PolicyRecord) {
	t.Helper()
	key, err := BootstrapPolicyMetadataKey(policy.Identity)
	require.NoError(t, err)
	raw, err := raftbootstrap.MarshalPolicyRecord(policy)
	require.NoError(t, err)
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	require.NoError(t, metadata.StageRaftControlMetadata(ctx, key, raw))
}

func seedGuardedNewPolicy(t *testing.T, st store.MVCCStore, policy raftbootstrap.PolicyRecord, index uint64) {
	t.Helper()
	applier, ok := st.(store.RaftEntryApplier)
	require.True(t, ok)
	require.NoError(t, applier.ApplyRaftEntry(t.Context(), index, func(ctx context.Context) error {
		stageGuardedNewPolicy(t, ctx, st, policy)
		return nil
	}))
}

func newGuardedCatalogRequest(t *testing.T) *pb.Request {
	t.Helper()
	return onePhaseTxnRequestWithPrevCommit(20, 30, 0, distribution.CatalogVersionKey(),
		[]*Elem[OP]{{Op: Put, Key: distribution.CatalogVersionKey(), Value: distribution.EncodeCatalogVersion(1)}},
		[][]byte{distribution.CatalogVersionKey()}, 0, nil)
}

func encodeGuardedCatalogRequest(t *testing.T, request *pb.Request) []byte {
	t.Helper()
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	require.NoError(t, err)
	return prependByte(raftEncodeSingle, raw)
}

func guardedNewCommand(t *testing.T, policy raftbootstrap.PolicyRecord, kind raftbootstrap.ControlKind) raftbootstrap.GuardedCommand {
	t.Helper()
	witness, err := policy.Witness()
	require.NoError(t, err)
	inner := EncodeWatermarkAdvance(100 << hlcLogicalBits)
	if kind == raftbootstrap.ControlGuardedCatalog {
		inner = encodeGuardedCatalogRequest(t, newGuardedCatalogRequest(t))
	}
	return raftbootstrap.GuardedCommand{Kind: kind, Witness: witness, OperationID: raftbootstrap.OperationID{4}, Inner: inner}
}

func newGuardedApplyFSM(t *testing.T, st store.MVCCStore) *kvFSM {
	t.Helper()
	policy := guardedNewPolicy()
	fsm, ok := NewKvFSMWithHLC(st, NewHLC(), WithRaftBootstrapIdentity(policy.Identity.Cluster, policy.Identity.Group)).(*kvFSM)
	require.True(t, ok)
	require.Equal(t, policy.Identity, fsm.NativeGroupIdentity())
	return fsm
}

func applyGuardedNewCommand(t *testing.T, fsm *kvFSM, command raftbootstrap.GuardedCommand, index uint64) any {
	t.Helper()
	payload, err := EncodeGuardedBootstrapCommand(command)
	require.NoError(t, err)
	fsm.SetApplyEntryIdentity(raftengine.NewAppliedEntryIdentity(index, 3, 0, payload))
	return fsm.Apply(payload)
}

func requireGuardedNewIndex(t *testing.T, st store.MVCCStore, index uint64) {
	t.Helper()
	reader, ok := st.(durableBootstrapIndex)
	if !ok {
		// Memory stores intentionally expose no durable-index API. Verify the
		// protected snapshot's index through the production Pebble restore path.
		snapshot, err := st.Snapshot()
		require.NoError(t, err)
		var raw bytes.Buffer
		_, err = snapshot.WriteTo(&raw)
		require.NoError(t, err)
		require.NoError(t, snapshot.Close())
		clone, err := store.NewPebbleStore(t.TempDir())
		require.NoError(t, err)
		defer clone.Close()
		require.NoError(t, clone.Restore(&raw))
		reader, ok = clone.(durableBootstrapIndex)
		require.True(t, ok)
	}
	got, known, err := reader.LastAppliedIndex()
	require.NoError(t, err)
	require.True(t, known)
	require.Equal(t, index, got)
}

func TestGuardedBootstrapNewAtomicApply(t *testing.T) {
	t.Parallel()
	for _, backend := range guardedNewApplyStores() {
		t.Run(backend.name, func(t *testing.T) {
			for _, kind := range []raftbootstrap.ControlKind{raftbootstrap.ControlGuardedWatermark, raftbootstrap.ControlGuardedCatalog} {
				t.Run(map[raftbootstrap.ControlKind]string{raftbootstrap.ControlGuardedWatermark: "watermark", raftbootstrap.ControlGuardedCatalog: "catalog"}[kind], func(t *testing.T) {
					st := backend.new(t)
					policy := guardedNewPolicy()
					seedGuardedNewPolicy(t, st, policy, 10)
					fsm := newGuardedApplyFSM(t, st)
					command := guardedNewCommand(t, policy, kind)
					receipt, ok := applyGuardedNewCommand(t, fsm, command, 11).(raftbootstrap.GuardedReceipt)
					require.True(t, ok)
					require.Equal(t, raftbootstrap.GuardedReceiptApplied, receipt.Verdict)
					requireGuardedNewIndex(t, st, 11)
					stored, found, err := fsm.readGuardedBootstrapReceipt(t.Context(), receipt.Entry)
					require.NoError(t, err)
					require.True(t, found)
					require.Equal(t, receipt, stored)
					requireGuardedNewEffects(t, fsm, receipt)
					policy.Generation++
					seedGuardedNewPolicy(t, st, policy, 12)
					require.Equal(t, receipt, applyGuardedNewCommand(t, fsm, command, 11), "exact replay bypasses current policy")
					requireGuardedNewIndex(t, st, 12)
				})
			}
		})
	}
}

func requireGuardedNewEffects(t *testing.T, fsm *kvFSM, receipt raftbootstrap.GuardedReceipt) {
	t.Helper()
	if receipt.Kind == raftbootstrap.ControlGuardedWatermark {
		require.Equal(t, uint64(100<<hlcLogicalBits), fsm.store.LastCommitTS())
		require.Equal(t, fsm.store.LastCommitTS(), receipt.ReturnedWatermark)
		require.Equal(t, int64(100), hlcCeilingFromHLC(fsm.hlc))
		return
	}
	value, err := fsm.store.GetAt(t.Context(), distribution.CatalogVersionKey(), 30)
	require.NoError(t, err)
	require.Equal(t, distribution.EncodeCatalogVersion(1), value)
	require.Equal(t, uint64(30), fsm.store.LastCommitTS())
}

func TestGuardedBootstrapNewStaleWitnessReceipt(t *testing.T) {
	t.Parallel()
	for _, backend := range guardedNewApplyStores() {
		t.Run(backend.name, func(t *testing.T) {
			st := backend.new(t)
			policy := guardedNewPolicy()
			seedGuardedNewPolicy(t, st, policy, 10)
			fsm := newGuardedApplyFSM(t, st)
			for i, kind := range []raftbootstrap.ControlKind{raftbootstrap.ControlGuardedWatermark, raftbootstrap.ControlGuardedCatalog} {
				command := guardedNewCommand(t, policy, kind)
				command.Witness.Generation++
				receipt, ok := applyGuardedNewCommand(t, fsm, command, uint64(11+i)).(raftbootstrap.GuardedReceipt)
				require.True(t, ok)
				require.Equal(t, raftbootstrap.GuardedReceiptRejectedStaleWitness, receipt.Verdict)
				require.Zero(t, receipt.ReturnedWatermark)
				require.Zero(t, st.LastCommitTS())
				requireGuardedNewIndex(t, st, uint64(11+i))
				require.Equal(t, receipt, applyGuardedNewCommand(t, fsm, command, uint64(11+i)))
			}
		})
	}
}

type guardedNewFaultStore struct {
	store.MVCCStore
	store.RaftControlMetadataStore
	store.RaftEntryApplier
	fault  string
	cause  error
	before func(context.Context)
}

func (s *guardedNewFaultStore) ReadRaftControlMetadata(ctx context.Context, key []byte) ([]byte, error) {
	if s.fault == "policy-read" && bytes.HasPrefix(key, []byte("P/")) {
		return nil, s.cause
	}
	return s.RaftControlMetadataStore.ReadRaftControlMetadata(ctx, key)
}

func (s *guardedNewFaultStore) StageRaftControlMetadata(ctx context.Context, key, value []byte) error {
	if s.fault == "receipt-stage" && bytes.HasPrefix(key, []byte("R/")) {
		return s.cause
	}
	return s.RaftControlMetadataStore.StageRaftControlMetadata(ctx, key, value)
}

func (s *guardedNewFaultStore) ApplyRaftEntry(ctx context.Context, index uint64, fn func(context.Context) error) error {
	return s.RaftEntryApplier.ApplyRaftEntry(ctx, index, func(tx context.Context) error {
		if s.before != nil {
			s.before(tx)
		}
		if err := fn(tx); err != nil {
			return err
		}
		if s.fault == "commit" {
			return s.cause
		}
		return nil
	})
}

func newGuardedFaultStore(t *testing.T, st store.MVCCStore) *guardedNewFaultStore {
	t.Helper()
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	applier, ok := st.(store.RaftEntryApplier)
	require.True(t, ok)
	return &guardedNewFaultStore{MVCCStore: st, RaftControlMetadataStore: metadata, RaftEntryApplier: applier, cause: errors.New("injected guarded apply failure")}
}

func TestGuardedBootstrapNewOperationalFailureAtomicAbort(t *testing.T) {
	t.Parallel()
	for _, backend := range guardedNewApplyStores() {
		t.Run(backend.name, func(t *testing.T) {
			for _, fault := range []string{"policy-read", "receipt-stage", "commit"} {
				t.Run(fault, func(t *testing.T) { testGuardedNewAtomicAbort(t, backend.new(t), fault) })
			}
		})
	}
}

func testGuardedNewAtomicAbort(t *testing.T, st store.MVCCStore, fault string) {
	t.Helper()
	policy := guardedNewPolicy()
	seedGuardedNewPolicy(t, st, policy, 10)
	for _, kind := range []raftbootstrap.ControlKind{raftbootstrap.ControlGuardedWatermark, raftbootstrap.ControlGuardedCatalog} {
		wrapped := newGuardedFaultStore(t, st)
		wrapped.fault = fault
		fsm := newGuardedApplyFSM(t, wrapped)
		notices := 0
		fsm.applyObservers = append(fsm.applyObservers, guardedNewApplyObserver{notify: func(pb.Op, []byte) { notices++ }})
		beforeCeiling := hlcCeilingFromHLC(fsm.hlc)
		command := guardedNewCommand(t, policy, kind)
		err := haltApplyError(t, applyGuardedNewCommand(t, fsm, command, 11))
		require.ErrorIs(t, err, wrapped.cause)
		requireGuardedNewIndex(t, st, 10)
		require.Zero(t, st.LastCommitTS())
		require.Equal(t, beforeCeiling, hlcCeilingFromHLC(fsm.hlc))
		require.Empty(t, fsm.deferredApplyNotices)
		require.Zero(t, notices)
		_, found, err := fsm.readGuardedBootstrapReceipt(t.Context(), guardedNewCommandIdentity(t, command, 11))
		require.NoError(t, err)
		require.False(t, found)
		_, err = st.GetAt(t.Context(), distribution.CatalogVersionKey(), 30)
		require.ErrorIs(t, err, store.ErrKeyNotFound)
	}
}

func guardedNewCommandIdentity(t *testing.T, command raftbootstrap.GuardedCommand, index uint64) raftbootstrap.EntryIdentity {
	t.Helper()
	payload, err := EncodeGuardedBootstrapCommand(command)
	require.NoError(t, err)
	entry := raftengine.NewAppliedEntryIdentity(index, 3, 0, payload)
	return raftbootstrap.EntryIdentity{Cluster: command.Witness.Cluster, Group: command.Witness.Group,
		Index: index, Term: 3, Digest: raftbootstrap.Digest(entry.Digest)}
}

type guardedNewApplyObserver struct{ notify func(pb.Op, []byte) }

func (o guardedNewApplyObserver) OnApply(op pb.Op, key []byte) { o.notify(op, key) }

func TestGuardedBootstrapNewWitnessReadsIndexedPolicy(t *testing.T) {
	t.Parallel()
	for _, backend := range guardedNewApplyStores() {
		t.Run(backend.name, func(t *testing.T) {
			st := backend.new(t)
			policy := guardedNewPolicy()
			seedGuardedNewPolicy(t, st, policy, 10)
			command := guardedNewCommand(t, policy, raftbootstrap.ControlGuardedWatermark)
			wrapped := newGuardedFaultStore(t, st)
			wrapped.before = func(ctx context.Context) {
				next := policy
				next.Generation++
				stageGuardedNewPolicy(t, ctx, st, next)
			}
			fsm := newGuardedApplyFSM(t, wrapped)
			receipt, ok := applyGuardedNewCommand(t, fsm, command, 11).(raftbootstrap.GuardedReceipt)
			require.True(t, ok)
			require.Equal(t, raftbootstrap.GuardedReceiptRejectedStaleWitness, receipt.Verdict)
			require.Zero(t, st.LastCommitTS())
		})
	}
}

func TestGuardedBootstrapNewSnapshotParity(t *testing.T) {
	t.Parallel()
	for _, backend := range guardedNewApplyStores() {
		t.Run(backend.name, func(t *testing.T) {
			st := backend.new(t)
			policy := guardedNewPolicy()
			seedGuardedNewPolicy(t, st, policy, 10)
			fsm := newGuardedApplyFSM(t, st)
			catalog := guardedNewCommand(t, policy, raftbootstrap.ControlGuardedCatalog)
			receipt := applyGuardedNewCommand(t, fsm, catalog, 11)
			watermark := guardedNewCommand(t, policy, raftbootstrap.ControlGuardedWatermark)
			watermarkReceipt := applyGuardedNewCommand(t, fsm, watermark, 12)
			snapshot, err := fsm.Snapshot()
			require.NoError(t, err)
			var raw bytes.Buffer
			_, err = snapshot.WriteTo(&raw)
			require.NoError(t, err)
			require.NoError(t, snapshot.Close())
			clone := newGuardedApplyFSM(t, backend.new(t))
			require.NoError(t, clone.Restore(&raw))
			requireGuardedNewIndex(t, clone.store, 12)
			require.Equal(t, receipt, applyGuardedNewCommand(t, clone, catalog, 11))
			require.Equal(t, watermarkReceipt, applyGuardedNewCommand(t, clone, watermark, 12))
			require.Equal(t, st.LastCommitTS(), clone.store.LastCommitTS())
			metadata, ok := clone.store.(store.RaftControlMetadataStore)
			require.True(t, ok)
			loaded, err := readAppliedBootstrapPolicy(t.Context(), metadata, policy.Identity, 12)
			require.NoError(t, err)
			before, err := raftbootstrap.MarshalPolicyRecord(policy)
			require.NoError(t, err)
			after, err := raftbootstrap.MarshalPolicyRecord(loaded)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func guardedNewReservedPolicy(t *testing.T) raftbootstrap.PolicyRecord {
	t.Helper()
	policy := guardedNewPolicy()
	expected, err := policy.Witness()
	require.NoError(t, err)
	policy.Generation++
	candidate := raftbootstrap.Member{ID: 1, Address: "node1-new:9000", PublicKeyDigest: raftbootstrap.Digest{3}}
	policy.Reservation = &raftbootstrap.MembershipReservation{
		ID: raftbootstrap.ReservationID{ReserveIndex: 9, OperationID: raftbootstrap.OperationID{5}}, Expected: expected,
		Transition: raftbootstrap.TransitionReplaceAddress, Candidate: candidate, EvidenceDigest: raftbootstrap.Digest{6},
		Target: raftbootstrap.Configuration{Index: 6, State: raftbootstrap.ConfState{Voters: []uint64{1}}, Members: []raftbootstrap.Member{candidate}},
	}
	return policy
}

func TestGuardedBootstrapNewReservationReceiptedWithoutEffects(t *testing.T) {
	t.Parallel()
	for _, backend := range guardedNewApplyStores() {
		t.Run(backend.name, func(t *testing.T) {
			st := backend.new(t)
			policy := guardedNewReservedPolicy(t)
			seedGuardedNewPolicy(t, st, policy, 10)
			fsm := newGuardedApplyFSM(t, st)
			for i, kind := range []raftbootstrap.ControlKind{raftbootstrap.ControlGuardedWatermark, raftbootstrap.ControlGuardedCatalog} {
				command := guardedNewCommand(t, policy, kind)
				receipt, ok := applyGuardedNewCommand(t, fsm, command, uint64(11+i)).(raftbootstrap.GuardedReceipt)
				require.True(t, ok)
				require.Equal(t, raftbootstrap.GuardedReceiptRejectedReservation, receipt.Verdict)
				require.Zero(t, receipt.ReturnedWatermark)
				require.Zero(t, st.LastCommitTS())
				requireGuardedNewIndex(t, st, uint64(11+i))
			}
		})
	}
}

func TestGuardedBootstrapNewCoveredWatermarkPersistsExactResult(t *testing.T) {
	t.Parallel()
	for _, backend := range guardedNewApplyStores() {
		t.Run(backend.name, func(t *testing.T) {
			st := backend.new(t)
			policy := guardedNewPolicy()
			seedGuardedNewPolicy(t, st, policy, 10)
			fsm := newGuardedApplyFSM(t, st)
			command := guardedNewCommand(t, policy, raftbootstrap.ControlGuardedWatermark)
			require.IsType(t, raftbootstrap.GuardedReceipt{}, applyGuardedNewCommand(t, fsm, command, 11))
			command.Inner = EncodeWatermarkAdvance(99 << hlcLogicalBits)
			receipt, ok := applyGuardedNewCommand(t, fsm, command, 12).(raftbootstrap.GuardedReceipt)
			require.True(t, ok)
			require.Equal(t, uint64(99<<hlcLogicalBits), receipt.RequestedWatermark)
			require.Equal(t, uint64(100<<hlcLogicalBits), receipt.ReturnedWatermark)
			requireGuardedNewIndex(t, st, 12)
		})
	}
}

func TestGuardedBootstrapNewCatalogTypedCommitValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func(*pb.Request)
	}{
		{name: "malformed_value", mutate: func(r *pb.Request) { r.Mutations[1].Value = []byte{1} }},
		{name: "unknown_catalog_family", mutate: func(r *pb.Request) { r.Mutations[1].Key = []byte("!dist|unknown|1") }},
		{name: "route_key_mismatch", mutate: func(r *pb.Request) {
			raw, err := distribution.EncodeRouteDescriptor(distribution.RouteDescriptor{RouteID: 1, GroupID: 7})
			require.NoError(t, err)
			r.Mutations[1].Key, r.Mutations[1].Value = distribution.CatalogRouteKey(2), raw
		}},
		{name: "duplicate_mutation", mutate: func(r *pb.Request) { r.Mutations = append(r.Mutations, r.Mutations[1]) }},
		{name: "prefix_delete", mutate: func(r *pb.Request) { r.Mutations[1].Op = pb.Op_DEL_PREFIX }},
		{name: "mutation_unknown_fields", mutate: func(r *pb.Request) { r.Mutations[1].ProtoReflect().SetUnknown([]byte{0x78, 0x00}) }},
		{name: "unstamped_patch", mutate: func(r *pb.Request) { r.Mutations[1].CommitTsValueOffset = 1 }},
		{name: "absence_precondition", mutate: func(r *pb.Request) { r.Mutations[1].RequireAbsent = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := store.NewMVCCStore()
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			policy := guardedNewPolicy()
			seedGuardedNewPolicy(t, st, policy, 10)
			fsm := newGuardedApplyFSM(t, st)
			request := newGuardedCatalogRequest(t)
			tc.mutate(request)
			command := guardedNewCommand(t, policy, raftbootstrap.ControlGuardedCatalog)
			command.Inner = encodeGuardedCatalogRequest(t, request)
			err := haltApplyError(t, applyGuardedNewCommand(t, fsm, command, 11))
			require.ErrorIs(t, err, ErrInvalidGuardedBootstrapInner)
			require.Zero(t, st.LastCommitTS())
			requireGuardedNewIndex(t, st, 10)
		})
	}
}

func TestGuardedBootstrapNewCatalogConflictDoesNotPartiallyCommit(t *testing.T) {
	t.Parallel()
	for _, backend := range guardedNewApplyStores() {
		t.Run(backend.name, func(t *testing.T) {
			st := backend.new(t)
			policy := guardedNewPolicy()
			seedGuardedNewPolicy(t, st, policy, 10)
			require.NoError(t, st.PutAt(t.Context(), distribution.CatalogVersionKey(), distribution.EncodeCatalogVersion(2), 25, 0))
			fsm := newGuardedApplyFSM(t, st)
			err := haltApplyError(t, applyGuardedNewCommand(t, fsm, guardedNewCommand(t, policy, raftbootstrap.ControlGuardedCatalog), 11))
			require.ErrorIs(t, err, store.ErrWriteConflict)
			requireGuardedNewIndex(t, st, 10)
			require.Equal(t, uint64(25), st.LastCommitTS())
		})
	}
}

func TestGuardedBootstrapNewMissingReceiptAtOldIndexHalts(t *testing.T) {
	t.Parallel()
	st, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	policy := guardedNewPolicy()
	seedGuardedNewPolicy(t, st, policy, 12)
	fsm := newGuardedApplyFSM(t, st)
	err = haltApplyError(t, applyGuardedNewCommand(t, fsm, guardedNewCommand(t, policy, raftbootstrap.ControlGuardedWatermark), 11))
	require.ErrorIs(t, err, ErrGuardedBootstrapReceiptConflict)
	requireGuardedNewIndex(t, st, 12)
	require.Zero(t, st.LastCommitTS())
}

func TestGuardedBootstrapNewCatalogObserversAfterReceiptCommit(t *testing.T) {
	t.Parallel()
	for _, backend := range guardedNewApplyStores() {
		t.Run(backend.name, func(t *testing.T) {
			st := backend.new(t)
			policy := guardedNewPolicy()
			seedGuardedNewPolicy(t, st, policy, 10)
			fsm := newGuardedApplyFSM(t, st)
			fsm.routes = forbiddenApplyWatcher{}
			fsm.shardGroupID = policy.Identity.Group
			request := newGuardedCatalogRequest(t)
			route, err := distribution.EncodeRouteDescriptor(distribution.RouteDescriptor{RouteID: 1, GroupID: 7})
			require.NoError(t, err)
			request.Mutations = append(request.Mutations,
				&pb.Mutation{Op: pb.Op_PUT, Key: distribution.CatalogNextRouteIDKey(), Value: distribution.EncodeCatalogNextRouteID(2)},
				&pb.Mutation{Op: pb.Op_PUT, Key: distribution.CatalogRouteKey(1), Value: route})
			command := guardedNewCommand(t, policy, raftbootstrap.ControlGuardedCatalog)
			command.Inner = encodeGuardedCatalogRequest(t, request)
			identity := guardedNewCommandIdentity(t, command, 11)
			notices := 0
			fsm.applyObservers = append(fsm.applyObservers, guardedNewApplyObserver{notify: func(_ pb.Op, _ []byte) {
				notices++
				receipt, found, err := fsm.readGuardedBootstrapReceipt(t.Context(), identity)
				require.NoError(t, err)
				require.True(t, found, "observer cannot run ahead of receipt commit")
				require.Equal(t, raftbootstrap.GuardedReceiptApplied, receipt.Verdict)
				require.Equal(t, uint64(30), st.LastCommitTS())
			}})
			require.IsType(t, raftbootstrap.GuardedReceipt{}, applyGuardedNewCommand(t, fsm, command, 11))
			require.Equal(t, 3, notices)
			requireGuardedNewIndex(t, st, 11)
			got, err := st.GetAt(t.Context(), distribution.CatalogRouteKey(1), 30)
			require.NoError(t, err)
			require.Equal(t, route, got)
		})
	}
}

func TestGuardedBootstrapNewActivateRemainsClosed(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	policy := guardedNewPolicy()
	seedGuardedNewPolicy(t, st, policy, 10)
	fsm := newGuardedApplyFSM(t, st)
	payload, err := EncodeGuardedBootstrapCommand(guardedNewCommand(t, policy, raftbootstrap.ControlGuardedWatermark))
	require.NoError(t, err)
	payload[4] = byte(raftbootstrap.ControlActivate)
	fsm.SetApplyEntryIdentity(raftengine.NewAppliedEntryIdentity(11, 3, 0, payload))
	err = haltApplyError(t, fsm.Apply(payload))
	require.ErrorIs(t, err, ErrGuardedBootstrapApply)
	requireGuardedNewIndex(t, st, 10)
	require.Zero(t, st.LastCommitTS())
}
