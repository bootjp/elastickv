package adapter

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type handoffTestStore struct {
	store.MVCCStore
	onHint func()
}

func (s *handoffTestStore) OnlineMigrationHandoffActive(context.Context, []byte) bool {
	if s.onHint != nil {
		s.onHint()
	}
	return true
}

type handoffTestCoordinator struct {
	kv.Coordinator
	dispatch func(context.Context) (*kv.CoordinateResponse, error)
}

func (c *handoffTestCoordinator) Dispatch(ctx context.Context, _ *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	return c.dispatch(ctx)
}

func handoffNonadmissionProof(t *testing.T) error {
	t.Helper()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{Version: 1, Routes: []distribution.RouteDescriptor{{RouteID: 1, GroupID: 1, State: distribution.RouteStateWriteFenced}}}))
	coordinator := kv.NewShardedCoordinator(engine, nil, 1, kv.NewHLC(), nil)
	_, err := coordinator.Dispatch(t.Context(), &kv.OperationGroup[kv.OP]{Elems: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("n"), Value: []byte("v")}}})
	require.True(t, kv.IsMutationNotAdmitted(err))
	return err
}

func handoffTestBuild() (*kv.OperationGroup[kv.OP], error) {
	return &kv.OperationGroup[kv.OP]{Elems: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("n"), Value: []byte("v")}}}, nil
}

func TestRawHandoffCancelWhileWaitingKeepsProof(t *testing.T) {
	t.Parallel()
	proof := handoffNonadmissionProof(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	coordinator := &handoffTestCoordinator{dispatch: func(context.Context) (*kv.CoordinateResponse, error) { calls++; return nil, proof }}
	server := NewGRPCServer(&handoffTestStore{MVCCStore: store.NewMVCCStore(), onHint: cancel}, coordinator)
	_, err := server.dispatchRawHandoff(ctx, []byte("n"), 1, handoffTestBuild)
	require.True(t, kv.IsMutationNotAdmitted(err), "no new dispatch started, so the known noncommit result remains authoritative")
	require.Equal(t, 1, calls)
}

func TestRawHandoffRedispatchHasFiniteDeadline(t *testing.T) {
	t.Parallel()
	proof := handoffNonadmissionProof(t)
	calls := 0
	coordinator := &handoffTestCoordinator{dispatch: func(ctx context.Context) (*kv.CoordinateResponse, error) {
		calls++
		if calls == 1 {
			return nil, proof
		}
		deadline, present := ctx.Deadline()
		require.True(t, present, "redispatch must not retain a wait slot under an unbounded caller context")
		if present {
			require.LessOrEqual(t, time.Until(deadline), rawHandoffMaxWait)
		}
		return nil, kv.ErrTransactionOutcomeUnknown
	}}
	server := NewGRPCServer(&handoffTestStore{MVCCStore: store.NewMVCCStore()}, coordinator)
	_, err := server.dispatchRawHandoff(t.Context(), []byte("n"), 1, handoffTestBuild)
	require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
	require.Equal(t, 2, calls, "unknown outcomes are never retried")
}
