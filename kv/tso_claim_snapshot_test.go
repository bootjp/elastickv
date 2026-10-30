package kv

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func rewriteClaimSnapshotHeader(t *testing.T, raw []byte, change func(*tsoClaimSnapshotHeader)) []byte {
	t.Helper()
	var header tsoClaimSnapshotHeader
	require.NoError(t, binary.Read(bytes.NewReader(raw), binary.BigEndian, &header))
	change(&header)
	var out bytes.Buffer
	require.NoError(t, binary.Write(&out, binary.BigEndian, header))
	out.Write(raw[tsoClaimSnapshotHeaderBytes:])
	return out.Bytes()
}

func TestTSOClaimSnapshotStrictRestorePreservesStateOnError(t *testing.T) {
	_, state := newSlotClaimTSOFixture(t)
	first := claimTestGrant(t, state, 1)
	p := claimTestRecord(first, 10, 0)
	require.Nil(t, applyTestClaims(t, state, 1, p.Record))
	second := claimTestGrant(t, state, 2)
	q := claimTestRecord(second, 11, 0)
	require.Nil(t, applyTestClaims(t, state, 2, q.Record))
	valid := claimTestSnapshot(t, state)
	var header tsoClaimSnapshotHeader
	require.NoError(t, binary.Read(bytes.NewReader(valid), binary.BigEndian, &header))
	grantStart := tsoClaimSnapshotHeaderBytes + int(header.ScalarBytes)
	claimStart := grantStart + int(header.Grants)*(tsoClaimGrantBytes+1)
	copyWithChange := func(change func([]byte)) []byte {
		out := append([]byte(nil), valid...)
		change(out)
		return out
	}
	malformed := map[string][]byte{
		"trailing": append(append([]byte(nil), valid...), 0),
		"version":  copyWithChange(func(b []byte) { b[7]++ }),
		"unknown_closed_flag": copyWithChange(func(b []byte) {
			b[grantStart+tsoClaimGrantBytes] = 2
		}),
		"unknown_role": copyWithChange(func(b []byte) {
			const roleOffset = 16 + 16 + 8 + 32 + 16 + 32
			b[claimStart+roleOffset] = 255
		}),
		"duplicate_grant": copyWithChange(func(b []byte) {
			copy(b[grantStart+tsoClaimGrantBytes+1:], b[grantStart:grantStart+len(TSOClaimID{})])
		}),
		"duplicate_claim": copyWithChange(func(b []byte) {
			copy(b[claimStart+tsoClaimRecordBytes:], b[claimStart:claimStart+len(TSOClaimID{})])
		}),
		"regressed_grant_term": copyWithChange(func(b []byte) {
			const termOffset = 16 + 32
			binary.BigEndian.PutUint64(b[grantStart+termOffset:], 2)
			b[grantStart+tsoClaimGrantBytes] = 0
			secondStart := grantStart + tsoClaimGrantBytes + 1
			binary.BigEndian.PutUint64(b[secondStart+termOffset:], 1)
			b[secondStart+tsoClaimGrantBytes] = 1
		}),
		"too_many_claims": rewriteClaimSnapshotHeader(t, valid, func(h *tsoClaimSnapshotHeader) { h.Claims = ^uint32(0) }),
		"too_many_grants": rewriteClaimSnapshotHeader(t, valid, func(h *tsoClaimSnapshotHeader) { h.Grants = maxTSOClaimGrants + 1 }),
		"unsupported_gc":  rewriteClaimSnapshotHeader(t, valid, func(h *tsoClaimSnapshotHeader) { h.RetiredBefore = 1 }),
	}
	for name, raw := range malformed {
		t.Run(name, func(t *testing.T) {
			require.Error(t, state.Restore(bytes.NewReader(raw)))
			require.Equal(t, valid, claimTestSnapshot(t, state))
		})
	}
	for n := range len(valid) {
		require.Error(t, state.Restore(bytes.NewReader(valid[:n])), "truncated at %d", n)
	}
	require.Equal(t, valid, claimTestSnapshot(t, state))
}

func TestTSOClaimScalarSnapshotCannotCreateOrDiscardClaims(t *testing.T) {
	alloc, state := newSlotClaimTSOFixture(t)
	scalar := claimTestSnapshot(t, state)
	g := claimTestGrant(t, state, 1)
	p := claimTestRecord(g, 10, 0)
	require.Nil(t, applyTestClaims(t, state, 1, p.Record))
	populated := claimTestSnapshot(t, state)
	require.ErrorIs(t, state.Restore(bytes.NewReader(scalar)), ErrTSOClaimInvalid)
	require.Equal(t, populated, claimTestSnapshot(t, state))
	restored := NewTSOStateMachine(NewHLC())
	require.NoError(t, restored.Restore(bytes.NewReader(scalar)))
	require.ErrorIs(t, restored.validateClaim(p, p.Record.Binding, TSOClaimPrimary), ErrTSOClaimUnknown)
	require.NoError(t, alloc.validateTimestampClaim(t.Context(), p, p.Record.Binding, TSOClaimPrimary))
}

func TestTSOClaimSnapshotCannotReopenGrantOrDropClaim(t *testing.T) {
	_, state := newSlotClaimTSOFixture(t)
	g := claimTestGrant(t, state, 1)
	beforeClaim := claimTestSnapshot(t, state)
	p := claimTestRecord(g, 10, 0)
	require.Nil(t, applyTestClaims(t, state, 1, p.Record))
	beforeClose := claimTestSnapshot(t, state)
	claimTestGrant(t, state, 2)
	after := claimTestSnapshot(t, state)
	for _, raw := range [][]byte{beforeClaim, beforeClose} {
		require.ErrorIs(t, state.Restore(bytes.NewReader(raw)), ErrTSOClaimInvalid)
		require.Equal(t, after, claimTestSnapshot(t, state))
	}
}

func TestTSOClaimCredentialSecretIsNotReplicated(t *testing.T) {
	_, state := newSlotClaimTSOFixture(t)
	g := claimTestGrant(t, state, 1)
	c, err := NewTSOClaimCredential()
	require.NoError(t, err)
	p := claimTestRecord(g, 10, 0)
	p.Credential = c
	p.Record.ID = c.ID
	p.Record.SecretHash = claimCredentialHash(c)
	payload, err := marshalTSOClaimBatch(1, []TSOClaimRecord{p.Record})
	require.NoError(t, err)
	require.NotContains(t, string(payload), string(c.Secret[:]))
	require.Nil(t, state.Apply(payload))
	require.NotContains(t, string(claimTestSnapshot(t, state)), string(c.Secret[:]))
}
