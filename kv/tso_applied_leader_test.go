package kv

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTSOLeaderWaitsForAppliedPendingState(t *testing.T) {
	for _, operation := range []string{"reserve", "validate", "resolve"} {
		t.Run(operation, func(t *testing.T) {
			allocator, state, engine, _ := pendingTestAllocator(t, &testPhaseDGate{})
			require.Nil(t, state.Apply(marshalTSOCutover()))
			base, err := allocator.clock.NextFenced()
			require.NoError(t, err)
			require.Nil(t, state.Apply(marshalTSOAllocationFloor(base)))
			floor := base + 100
			entered := make(chan struct{})
			applied := make(chan struct{})
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			engine.readIndex = func(ctx context.Context) (uint64, error) {
				close(entered)
				select {
				case <-applied:
					return 3, nil
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			}
			done := make(chan error, 1)
			go func() {
				done <- runAppliedPendingOperation(ctx, allocator, operation, base, floor)
			}()
			select {
			case <-entered:
			case <-done:
				t.Fatal("TSO operation inspected state without an applied leader fence")
			case <-ctx.Done():
				t.Fatal("TSO operation did not enter its applied leader fence")
			}
			require.Empty(t, engine.proposedPayloads())
			require.Nil(t, state.Apply(marshalTSOPendingPhaseD(floor)))
			close(applied)
			err = <-done
			switch operation {
			case "reserve":
				require.NoError(t, err)
				require.Equal(t, floor, state.PhaseDFloor(), "the committed pending floor must not be replaced")
			case "validate":
				require.ErrorIs(t, err, ErrTSOPhaseDInactive)
			case "resolve":
				require.ErrorIs(t, err, ErrTSOTimestampInvalid, "a legacy resolution cannot escape the applied pending bound")
			}
		})
	}
}

func runAppliedPendingOperation(ctx context.Context, allocator *RaftTSOAllocator, operation string, base, floor uint64) error {
	switch operation {
	case "reserve":
		_, err := allocator.ReserveBatchAfter(ctx, 1, 0, true, true)
		return err
	case "validate":
		return allocator.ValidateDurableTimestamp(ctx, base+1)
	case "resolve":
		return allocator.ValidateLegacyResolutionTimestamps(ctx, base+1, floor+1)
	default:
		return fmt.Errorf("unknown TSO operation %q", operation)
	}
}
