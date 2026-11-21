package raftbootstrap

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func membershipContextFixture(t *testing.T) MembershipChangeContext {
	t.Helper()
	candidate := Member{ID: 8, Address: "127.0.0.1:50051", PublicKeyDigest: Digest{9}}
	target, err := ConfigurationDigest(Configuration{Index: 7,
		State:   ConfState{Voters: []uint64{1}, Learners: []uint64{candidate.ID}},
		Members: []Member{{ID: 1, Address: "127.0.0.1:50050", PublicKeyDigest: Digest{1}}, candidate}})
	require.NoError(t, err)
	return MembershipChangeContext{
		ID:           ReservationID{ReserveIndex: 42, OperationID: OperationID{1}},
		Expected:     Witness{Cluster: UUID{2}, Group: 3, ActivationIndex: 4, Generation: 5, ConfigurationIndex: 6, ConfigurationDigest: Digest{7}},
		Transition:   TransitionAddLearner,
		TargetDigest: target,
		Candidate:    candidate,
		PeerID:       "n8",
	}
}

func TestMembershipChangeContextRoundTrip(t *testing.T) {
	context := membershipContextFixture(t)
	encoded, err := EncodeMembershipChangeContext(context)
	require.NoError(t, err)
	require.Equal(t, byte(4), encoded[0])
	decoded, err := ParseMembershipChangeContext(encoded)
	require.NoError(t, err)
	require.Equal(t, context, decoded)
	reservation := MembershipReservation{ID: context.ID, Expected: context.Expected,
		Transition: context.Transition, Candidate: context.Candidate,
		Target: Configuration{Index: 7, State: ConfState{Voters: []uint64{1}, Learners: []uint64{context.Candidate.ID}},
			Members: []Member{{ID: 1, Address: "127.0.0.1:50050", PublicKeyDigest: Digest{1}}, context.Candidate}}}
	require.True(t, decoded.MatchesReservation(reservation, "n8", 8))
	reservation.ID.ReserveIndex++
	require.False(t, decoded.MatchesReservation(reservation, "n8", 8))
	reservation.ID.ReserveIndex--
	require.False(t, decoded.MatchesReservation(reservation, "n9", 8))
	require.False(t, decoded.MatchesReservation(reservation, "n8", 9))
	reservation.Target.Index++
	require.False(t, decoded.MatchesReservation(reservation, "n8", 8))
}

func TestMembershipChangeContextRejectsMalformedFrames(t *testing.T) {
	context := membershipContextFixture(t)
	valid, err := EncodeMembershipChangeContext(context)
	require.NoError(t, err)
	for _, tc := range []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"truncated", func(b []byte) []byte { return b[:len(b)-1] }},
		{"trailing", func(b []byte) []byte { return append(b, 0) }},
		{"old version", func(b []byte) []byte { b[0] = 3; return b }},
		{"duplicated operation mismatch", func(b []byte) []byte { b[25]++; return b }},
		{"empty peer id", func(b []byte) []byte { binary.BigEndian.PutUint32(b[membershipContextFixedSize-8:], 0); return b }},
		{"oversized peer id", func(b []byte) []byte {
			binary.BigEndian.PutUint32(b[membershipContextFixedSize-8:], ^uint32(0))
			return b
		}},
		{"unknown transition", func(b []byte) []byte { binary.BigEndian.PutUint32(b[41+WitnessSize:], 99); return b }},
		{"zero target digest", func(b []byte) []byte {
			clear(b[membershipContextPrefixSize+WitnessSize+4 : membershipContextPrefixSize+WitnessSize+4+checksumSize])
			return b
		}},
		{"zero identity digest", func(b []byte) []byte { b[membershipContextPrefixSize+WitnessSize+4+checksumSize] = 0; return b }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseMembershipChangeContext(tc.mutate(append([]byte(nil), valid...)))
			require.Error(t, err)
		})
	}
}
