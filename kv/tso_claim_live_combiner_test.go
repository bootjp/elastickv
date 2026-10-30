package kv

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTSOProductionClaimsConcurrentPublicNext(t *testing.T) {
	allocator, batch, state := newProductionClaimFixture(t)
	const workers, perWorker = 8, 64
	values := make([]uint64, workers*perWorker)
	failures := make([]error, len(values))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Go(func() {
			<-start
			for j := range perWorker {
				i := worker*perWorker + j
				values[i], failures[i] = batch.Next(t.Context())
				if failures[i] == nil {
					failures[i] = allocator.ValidateDurableTimestamp(t.Context(), values[i])
				}
			}
		})
	}
	close(start)
	wg.Wait()
	seen := make(map[uint64]bool, len(values))
	for i, value := range values {
		require.NoError(t, failures[i])
		require.False(t, seen[value], "duplicate public timestamp")
		seen[value] = true
	}
	require.Len(t, state.claims.Claims, len(values))
	var total, batches, largest int
	for _, payload := range claimRecordingEngine(t, allocator).proposedPayloads() {
		if !bytes.HasPrefix(payload, []byte(tsoClaimIntentEnvelope)) {
			continue
		}
		require.LessOrEqual(t, len(payload), tsoIntentBatchHeaderBytes+MaxTSOClaimsPerBatch*tsoSelectedIntentBytes)
		_, intents, err := decodeTSOClaimIntents(payload)
		require.NoError(t, err)
		require.LessOrEqual(t, len(intents), MaxTSOClaimsPerBatch)
		total += len(intents)
		batches++
		largest = max(largest, len(intents))
	}
	require.Equal(t, len(values), total)
	require.Greater(t, largest, 1, "actual public Next calls must combine, not only the fixed-candidate kernel")
	t.Logf("public Next: %d claims / %d proposals, largest batch %d", total, batches, largest)
}

type contextClaimSigner struct {
	key [ed25519.PublicKeySize]byte
}

type claimPeerKey struct{}

func (p contextClaimSigner) AuthorizeTimestampClaimLookup(ctx context.Context) error {
	return ctx.Err()
}

func (p contextClaimSigner) AuthorizeTimestampClaim(ctx context.Context, key [ed25519.PublicKeySize]byte) error {
	peer, ok := ctx.Value(claimPeerKey{}).([ed25519.PublicKeySize]byte)
	if !ok || peer != key || key != p.key {
		return ErrTSOClaimAuthorityRequired
	}
	return ctx.Err()
}

func TestTSOProductionClaimsCombinerPreservesEachAuthorityContext(t *testing.T) {
	allocator, batch, state := newProductionClaimFixture(t)
	key := allocator.claimIdentity.PublicKey()
	allocator.claimAuthority = contextClaimSigner{key: key}
	contexts := []context.Context{
		context.WithValue(t.Context(), claimPeerKey{}, key),
		context.WithValue(t.Context(), claimPeerKey{}, [ed25519.PublicKeySize]byte{99}),
	}
	results := make([]error, len(contexts))
	var wg sync.WaitGroup
	for i, ctx := range contexts {
		wg.Go(func() { _, results[i] = batch.Next(ctx) })
	}
	wg.Wait()
	require.NoError(t, results[0])
	require.ErrorIs(t, results[1], ErrTSOClaimAuthorityRequired)
	require.Len(t, state.claims.Claims, 1, "an authorized peer must not authorize its combined neighbour")
}

type blockedClaimSigner struct {
	pinnedClaimSigner
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *blockedClaimSigner) AuthorizeTimestampClaim(ctx context.Context, key [ed25519.PublicKeySize]byte) error {
	p.once.Do(func() {
		close(p.entered)
		select {
		case <-p.release:
		case <-ctx.Done():
		}
	})
	return p.pinnedClaimSigner.AuthorizeTimestampClaim(ctx, key)
}

func TestTSOProductionClaimsPublicQueueBound(t *testing.T) {
	allocator, batch, _ := newProductionClaimFixture(t)
	policy := &blockedClaimSigner{pinnedClaimSigner: pinnedClaimSigner{key: allocator.claimIdentity.PublicKey()}, entered: make(chan struct{}), release: make(chan struct{})}
	allocator.claimAuthority = policy
	var release sync.Once
	defer release.Do(func() { close(policy.release) })
	failures := make([]error, tsoClaimQueueCapacity+1)
	var wg sync.WaitGroup
	wg.Go(func() { _, failures[0] = batch.Next(t.Context()) })
	select {
	case <-policy.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("public claim did not reach authority")
	}
	for i := range tsoClaimQueueCapacity {
		wg.Go(func() { _, failures[i+1] = batch.Next(t.Context()) })
	}
	require.Eventually(t, func() bool {
		allocator.claimQueueMu.Lock()
		defer allocator.claimQueueMu.Unlock()
		return len(allocator.claimQueue) == tsoClaimQueueCapacity
	}, 2*time.Second, time.Millisecond)
	_, err := batch.Next(t.Context())
	require.ErrorIs(t, err, ErrTSOClaimCapacity)
	release.Do(func() { close(policy.release) })
	wg.Wait()
	for _, err := range failures {
		require.NoError(t, err)
	}
}

func TestTSOProductionClaimsNewTermPreservesOnlyCommittedSlots(t *testing.T) {
	allocator, batch, state := newProductionClaimFixture(t)
	first, err := batch.Next(t.Context())
	require.NoError(t, err)
	oldFloor := state.AllocationFloor()
	claimRecordingEngine(t, allocator).setTerm(2)
	second, err := batch.Next(t.Context())
	require.NoError(t, err)
	require.Greater(t, second, oldFloor)
	require.NoError(t, allocator.ValidateDurableTimestamp(t.Context(), first))
	for slot := first + 1; slot <= oldFloor; slot++ {
		require.ErrorIs(t, allocator.ValidateDurableTimestamp(t.Context(), slot), ErrTSOTimestampInvalid)
	}
	for _, retained := range state.claims.Grants {
		if retained.Grant.Term == 1 {
			require.True(t, retained.Closed)
		}
	}
}

func TestTSOProductionClaimsLookupHonorsBudgetDuringReservation(t *testing.T) {
	allocator, _, _ := newProductionClaimFixture(t)
	identity := allocator.TimestampClaimIdentity()
	signed, _, err := identity.signClaim(TSOClaimBinding{Owner: identity.owner(), Operation: TSOOperationID{1}, Intent: TSOIntentDigest{2}, Role: TSOClaimCommit}, 0, 16)
	require.NoError(t, err)
	allocator.mu.Lock()
	defer allocator.mu.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := allocator.LookupTimestampClaim(ctx, signed); done <- err }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.ErrorIs(t, err, ErrTSOClaimUnknown)
	case <-time.After(time.Second):
		t.Fatal("public lookup ignored its caller budget while waiting for a reservation")
	}
}
