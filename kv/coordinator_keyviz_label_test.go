package kv

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/keyviz"
	"github.com/stretchr/testify/require"
)

// Adapters wrap their coordinator with WithKeyVizLabel, which stamps the
// adapter label on dispatches and puts it on the context of key-routed reads.
// The single-group Coordinate (the cmd/server demo's coordinator) must hand
// that label to the sampler, which keeps or collapses it according to its own
// KeyVizLabelsEnabled option. Coordinate used to pass LabelLegacy, so the
// demo's --keyvizLabelsEnabled never produced per-adapter rows (CodeRabbit on
// PR #1277).
func TestCoordinateSamplerReceivesAdapterLabel(t *testing.T) {
	t.Parallel()

	sampler := &recordingSampler{}
	clock := NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
	c := (&Coordinate{
		transactionManager: &stubTransactional{},
		engine:             stubLeaderEngine{},
		clock:              clock,
	}).WithSamplerRouteResolver(sampler, func([]byte) (uint64, bool) { return 7, true })
	labeled := WithKeyVizLabel(c, keyviz.LabelRedis)
	ctx := context.Background()

	_, err := labeled.Dispatch(ctx, &OperationGroup[OP]{
		Elems: []*Elem[OP]{{Op: Put, Key: []byte("raw"), Value: []byte("value")}},
	})
	require.NoError(t, err)
	startTS, err := c.nextStartTS(ctx)
	require.NoError(t, err)
	_, err = labeled.Dispatch(ctx, &OperationGroup[OP]{
		IsTxn:   true,
		StartTS: startTS,
		Elems:   []*Elem[OP]{{Op: Put, Key: []byte("txn"), Value: []byte("value")}},
	})
	require.NoError(t, err)
	reader, ok := labeled.(LeaseReadableCoordinator)
	require.True(t, ok)
	_, err = reader.LeaseReadForKey(ctx, []byte("read"))
	require.NoError(t, err)

	calls := sampler.snapshot()
	require.Len(t, calls, 3)
	for _, call := range calls {
		require.Equal(t, keyviz.LabelRedis, call.label, "op %v key %q", call.op, call.key)
	}
}

// Without an adapter label the observation stays on the legacy row.
func TestCoordinateSamplerDefaultsToLegacyLabel(t *testing.T) {
	t.Parallel()

	sampler := &recordingSampler{}
	clock := NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
	c := (&Coordinate{
		transactionManager: &stubTransactional{},
		engine:             stubLeaderEngine{},
		clock:              clock,
	}).WithSamplerRouteResolver(sampler, func([]byte) (uint64, bool) { return 7, true })

	_, err := c.Dispatch(context.Background(), &OperationGroup[OP]{
		Elems: []*Elem[OP]{{Op: Put, Key: []byte("raw"), Value: []byte("value")}},
	})
	require.NoError(t, err)
	_, err = c.LeaseReadForKey(context.Background(), []byte("read"))
	require.NoError(t, err)

	calls := sampler.snapshot()
	require.Len(t, calls, 2)
	for _, call := range calls {
		require.Equal(t, keyviz.LabelLegacy, call.label)
	}
}
