package peersession

import (
	"context"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/internal/peersession/wire"
	"github.com/stretchr/testify/require"
)

type revokingAuthorizer struct {
	owner *Owner
	once  sync.Once
}

func (a *revokingAuthorizer) Recheck(_ context.Context, b Binding) (CatchupStatus, error) {
	a.once.Do(func() { a.owner.RevokeGroup(b.Group) })
	return CatchupStatus{Binding: b, Pending: PendingClear, Phase: GrantReciprocallyAcknowledged}, nil
}
func TestNativeAuthorizerMayRevokeWithoutSessionLockCycle(t *testing.T) {
	a := &revokingAuthorizer{}
	f := setupNative(t, fixtureOptions{clientState: Prejoin, learner: true, authorizer: a})
	a.owner = f.client
	cc := f.dial(t)
	h := f.handshake(t, cc)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	done := make(chan error, 1)
	go func() { var reply wire.Frame; done <- h.Invoke(ctx, cc, sendMethod, request(Catchup), &reply) }()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-ctx.Done():
		t.Fatal("authorizer/Owner lock cycle")
	}
	require.Zero(t, f.service.delivered.Load())
}

type blockingAuthorizer struct {
	entered chan struct{}
	once    sync.Once
}

func (a *blockingAuthorizer) Recheck(ctx context.Context, _ Binding) (CatchupStatus, error) {
	a.once.Do(func() { close(a.entered) })
	<-ctx.Done()
	return CatchupStatus{}, ctx.Err()
}
func TestNativeRevocationCancelsAndJoinsOwnedAuthorizerCall(t *testing.T) {
	a := &blockingAuthorizer{entered: make(chan struct{})}
	f := setupNative(t, fixtureOptions{clientState: Prejoin, learner: true, authorizer: a})
	cc := f.dial(t)
	h := f.handshake(t, cc)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	done := make(chan error, 1)
	go func() { var reply wire.Frame; done <- h.Invoke(ctx, cc, sendMethod, request(Catchup), &reply) }()
	select {
	case <-a.entered:
	case <-ctx.Done():
		t.Fatal("authorizer did not start")
	}
	f.client.RevokeGroup(testGroup)
	select {
	case err := <-done:
		require.Error(t, err)
	case <-ctx.Done():
		t.Fatal("authorizer call outlived session revocation")
	}
	require.Zero(t, f.service.delivered.Load())
	require.Equal(t, Counts{}, f.client.Counts())
}
