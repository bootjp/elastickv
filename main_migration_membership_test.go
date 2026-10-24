package main

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/raftadmin"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type migrationMembershipStore struct {
	store.MVCCStore
	store.MigrationOwnershipStore
	view store.MigrationServingState
	err  error
}

func (s migrationMembershipStore) MigrationServingState(context.Context) (store.MigrationServingState, error) {
	return s.view, s.err
}

type migrationMembershipNext struct {
	add     func()
	promote func()
	err     error
}

func (n migrationMembershipNext) PreAddMember(context.Context, string, string) error {
	n.add()
	return n.err
}

func (n migrationMembershipNext) PrePromoteMember(context.Context, string, string) error {
	n.promote()
	return n.err
}

func TestMigrationMembershipRequiresSupportBeforeNextHook(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		enabled bool
		view    store.MigrationServingState
	}{
		{name: "enabled-before-first-control", enabled: true},
		{name: "durable-ownership", view: store.MigrationServingState{Ownership: []store.MigrationOwnershipState{{JobID: 7}}}},
		{name: "durable-guard", view: store.MigrationServingState{Readiness: []store.TargetStagedReadinessState{{JobID: 7, Armed: true}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var order []string
			next := migrationMembershipNext{add: func() { order = append(order, "add") }, promote: func() { order = append(order, "promote") }}
			gate := newMigrationMembershipGate(migrationMembershipStore{view: test.view}, test.enabled,
				func(_ context.Context, address string) error {
					require.Equal(t, "candidate:50051", address)
					order = append(order, "probe")
					return nil
				}, next)
			require.NoError(t, gate.PreAddMember(t.Context(), "candidate", "candidate:50051"))
			require.NoError(t, gate.PrePromoteMember(t.Context(), "candidate", "candidate:50051"))
			require.Equal(t, []string{"probe", "add", "probe", "promote"}, order)
		})
	}
}

func TestMigrationMembershipRefusalStopsNextHook(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("unsupported protocol")
	next := migrationMembershipNext{add: func() { t.Fatal("add hook ran") }, promote: func() { t.Fatal("promotion hook ran") }}
	gate := newMigrationMembershipGate(nil, true, func(context.Context, string) error { return sentinel }, next)
	require.ErrorIs(t, gate.PreAddMember(t.Context(), "candidate", "candidate:50051"), sentinel)
	require.ErrorIs(t, gate.PrePromoteMember(t.Context(), "candidate", "candidate:50051"), sentinel)
	gate.probe = nil
	require.Error(t, gate.PreAddMember(t.Context(), "candidate", "candidate:50051"))
}

func TestMigrationMembershipPreservesUnconfiguredClusters(t *testing.T) {
	t.Parallel()
	for _, st := range []store.MVCCStore{nil, store.NewMVCCStore(), migrationMembershipStore{}} {
		calls := 0
		var next raftadmin.MembershipChangeInterceptor = migrationMembershipNext{add: func() { calls++ }}
		gate := newMigrationMembershipGate(st, false, func(context.Context, string) error {
			t.Fatal("ordinary cluster must not probe migration support")
			return nil
		}, next)
		require.NoError(t, gate.PreAddMember(t.Context(), "legacy", "legacy:50051"))
		require.Equal(t, 1, calls)
	}
}

func TestMigrationMembershipMetadataErrorAndCancellationStopAdmission(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("ownership metadata unavailable")
	gate := newMigrationMembershipGate(migrationMembershipStore{err: sentinel}, false, nil, nil)
	require.ErrorIs(t, gate.PreAddMember(t.Context(), "candidate", "candidate:50051"), sentinel)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, gate.PrePromoteMember(ctx, "candidate", "candidate:50051"), context.Canceled)
}

func TestMigrationMembershipPreservesNextHookErrors(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("prepare evidence protocol unsupported")
	next := migrationMembershipNext{add: func() {}, promote: func() {}, err: sentinel}
	gate := newMigrationMembershipGate(nil, false, nil, next)
	require.ErrorIs(t, gate.PreAddMember(t.Context(), "candidate", "candidate:50051"), sentinel)
	require.ErrorIs(t, gate.PrePromoteMember(t.Context(), "candidate", "candidate:50051"), sentinel)
}
