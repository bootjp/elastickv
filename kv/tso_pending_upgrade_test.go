package kv

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTSOPendingHistoricalCheckFailureKeepsIssuingWithBoundedRetry(t *testing.T) {
	for _, checkErr := range []error{ErrTSOProtocolUnsupported, ErrTSOPhaseDActivationPending, context.DeadlineExceeded} {
		t.Run(checkErr.Error(), func(t *testing.T) {
			gate := &testPhaseDGate{checkErr: checkErr}
			a, fsm, engine, _ := pendingTestAllocator(t, gate)
			require.Nil(t, fsm.Apply(marshalTSOCutover()))
			require.Nil(t, fsm.Apply(marshalTSOPhaseD(100)))
			for range 2 {
				ts, err := a.Next(context.Background())
				require.NoError(t, err)
				require.Greater(t, ts, uint64(100))
			}
			require.Equal(t, 1, gate.checks, "ordinary requests must not repeatedly probe an incomplete rolling upgrade")
			require.Zero(t, fsm.PrepareEvidenceProtocolFloor())
			require.Empty(t, gate.floors)
			require.Empty(t, gate.finished)
			engine.setTerm(2)
			_, err := a.Next(context.Background())
			require.NoError(t, err)
			require.Equal(t, 2, gate.checks, "a new term must not inherit the old term's probe cooldown")
			gate.checkErr = nil
			a.phaseDBootstrapRetryAt = time.Time{}
			_, err = a.Next(context.Background())
			require.NoError(t, err)
			require.Equal(t, uint64(100), fsm.PrepareEvidenceProtocolFloor())
		})
	}
}

func TestTSOPendingHistoricalMarkerPrecedesSealAndPermanentlyDisablesFallback(t *testing.T) {
	gate := &testPhaseDGate{sealErr: ErrTSOPhaseDActivationPending}
	a, fsm, engine, _ := pendingTestAllocator(t, gate)
	require.Nil(t, fsm.Apply(marshalTSOCutover()))
	require.Nil(t, fsm.Apply(marshalTSOPhaseD(100)))
	require.Nil(t, fsm.Apply(marshalTSOAllocationFloor(120)))
	gate.seal = func() { require.Equal(t, uint64(100), fsm.PrepareEvidenceProtocolFloor()) }
	_, err := a.Next(context.Background())
	require.ErrorIs(t, err, ErrTSOPhaseDActivationPending)
	require.Equal(t, [][]byte{marshalTSOPendingPhaseD(100)}, engine.proposedPayloads())
	require.Equal(t, uint64(120), fsm.AllocationFloor())
	engine.setTerm(2)
	gate.checkErr = ErrTSOProtocolUnsupported
	checks := gate.checks
	_, err = a.Next(context.Background())
	require.ErrorIs(t, err, ErrTSOProtocolUnsupported)
	require.Equal(t, checks+1, gate.checks, "durable pending retries must recheck support without historical fallback")
	gate.checkErr = nil
	gate.sealErr = nil
	gate.prepareErr = ErrTSOPhaseDActivationPending
	_, err = a.Next(context.Background())
	require.ErrorIs(t, err, ErrTSOPhaseDActivationPending)
	gate.prepareErr = nil
	gate.finishErr = ErrTSOPhaseDActivationPending
	_, err = a.Next(context.Background())
	require.ErrorIs(t, err, ErrTSOPhaseDActivationPending)
	require.Equal(t, uint64(120), fsm.AllocationFloor())
	gate.finishErr = nil
	ts, err := a.Next(context.Background())
	require.NoError(t, err)
	require.Greater(t, ts, uint64(120))
	require.Equal(t, uint64(100), fsm.PrepareEvidenceProtocolFloor())
}

func TestTSOPendingHistoricalFailedCheckStillFencesLeadership(t *testing.T) {
	gate := &testPhaseDGate{checkErr: ErrTSOProtocolUnsupported}
	a, fsm, engine, _ := pendingTestAllocator(t, gate)
	require.Nil(t, fsm.Apply(marshalTSOCutover()))
	require.Nil(t, fsm.Apply(marshalTSOPhaseD(100)))
	gate.check = func() { engine.setTerm(2) }
	_, err := a.Next(context.Background())
	require.ErrorIs(t, err, ErrTSONotLeader)
	require.Empty(t, engine.proposedPayloads())
	require.Zero(t, fsm.AllocationFloor())
}

func TestTSOPendingHistoricalLateAppliedMarkerCannotFallBack(t *testing.T) {
	gate := &testPhaseDGate{checkErr: ErrTSOProtocolUnsupported, sealErr: ErrTSOPhaseDActivationPending}
	a, fsm, _, _ := pendingTestAllocator(t, gate)
	require.Nil(t, fsm.Apply(marshalTSOCutover()))
	require.Nil(t, fsm.Apply(marshalTSOPhaseD(100)))
	gate.check = func() { require.Nil(t, fsm.Apply(marshalTSOPendingPhaseD(100))) }
	_, err := a.Next(context.Background())
	require.ErrorIs(t, err, ErrTSOProtocolUnsupported)
	require.Zero(t, fsm.AllocationFloor())
}

func TestTSOPendingActiveResumeEnablesNewGroupBeforeSeal(t *testing.T) {
	gate := &testPhaseDGate{checkErr: ErrTSOProtocolUnsupported}
	a, fsm, _, _ := pendingTestAllocator(t, gate)
	require.Nil(t, fsm.Apply(marshalTSOCutover()))
	require.Nil(t, fsm.Apply(marshalTSOPendingPhaseD(100)))
	require.Nil(t, fsm.Apply(marshalTSOPhaseD(100)))
	require.Nil(t, fsm.Apply(marshalTSOAllocationFloor(120)))
	_, err := a.Next(context.Background())
	require.ErrorIs(t, err, ErrTSOProtocolUnsupported)
	require.Equal(t, uint64(120), fsm.AllocationFloor())
	gate.checkErr = nil
	var steps []string
	enabled := false
	gate.check = func() {
		enabled = true
		steps = append(steps, "check")
	}
	gate.seal = func() {
		require.True(t, enabled, "a newly configured data group must be enabled before seal")
		steps = append(steps, "seal")
	}
	gate.prepare = func(uint64) { steps = append(steps, "prepare") }
	gate.finish = func(uint64) { steps = append(steps, "finish") }
	_, err = a.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"check", "seal", "prepare", "finish"}, steps)
}
