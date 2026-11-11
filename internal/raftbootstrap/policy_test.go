package raftbootstrap

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func policyFixture(t testing.TB) PolicyRecord {
	t.Helper()
	in, _ := fixture(t)
	digest, err := ConfigurationDigest(in.Baseline)
	require.NoError(t, err)
	policy := PolicyRecord{
		Version: PolicyVersion, Identity: in.Identity, BaselineDigest: digest,
		Minimum: in.Minimum, ActivationIndex: 11, Generation: 3,
		LastConfigurationIndex: in.Baseline.Index, Configuration: in.Baseline,
		RetiredMemberIDs: []uint64{9, 8},
		TerminalOperations: []TerminalMembershipOperation{
			{ID: ReservationID{ReserveIndex: 18, OperationID: OperationID{2}}, Outcome: MembershipAborted, Member: 8},
			{ID: ReservationID{ReserveIndex: 17, OperationID: OperationID{1}}, Outcome: MembershipCompleted, Member: 9},
		},
	}
	configurationDigest, err := ConfigurationDigest(policy.Configuration)
	require.NoError(t, err)
	w := Witness{
		Cluster: policy.Identity.Cluster, Group: policy.Identity.Group,
		ActivationIndex: policy.ActivationIndex, Generation: policy.Generation - 1,
		ConfigurationIndex: policy.LastConfigurationIndex, ConfigurationDigest: configurationDigest,
	}
	policy.Reservation = &MembershipReservation{
		ID: ReservationID{ReserveIndex: 21, OperationID: OperationID{3}}, Expected: w,
		Transition: TransitionReplaceAddress,
		Candidate:  Member{ID: 2, Address: "node2-new:9000", PublicKeyDigest: sha256.Sum256([]byte("key2"))},
		Target:     in.Baseline, EvidenceDigest: sha256.Sum256([]byte("evidence")),
	}
	policy.Reservation.Target, err = canonicalConfiguration(policy.Reservation.Target)
	require.NoError(t, err)
	for i := range policy.Reservation.Target.Members {
		if policy.Reservation.Target.Members[i].ID == policy.Reservation.Candidate.ID {
			policy.Reservation.Target.Members[i] = policy.Reservation.Candidate
		}
	}
	policy.Reservation.Target.Index = policy.Configuration.Index + 1
	return policy
}

func TestPolicyRecordCanonicalRoundTrip(t *testing.T) {
	t.Parallel()
	policy := policyFixture(t)
	encoded, err := MarshalPolicyRecord(policy)
	require.NoError(t, err)
	require.LessOrEqual(t, len(encoded), MaxFileSize)
	parsed, err := ParsePolicyRecord(encoded)
	require.NoError(t, err)
	require.Equal(t, []uint64{8, 9}, parsed.RetiredMemberIDs)
	require.Equal(t, uint64(17), parsed.TerminalOperations[0].ID.ReserveIndex)
	require.Equal(t, []uint64{3, 1}, policy.Configuration.State.Voters, "must not mutate input")
	again, err := MarshalPolicyRecord(parsed)
	require.NoErrorf(t, err, "parsed policy=%+v reservation=%+v", parsed, parsed.Reservation)
	require.Equal(t, encoded, again)

	digestA, err := PolicyDigest(policy)
	require.NoError(t, err)
	digestB, err := PolicyDigest(parsed)
	require.NoError(t, err)
	require.Equal(t, digestA, digestB)
	witness, err := parsed.Witness()
	require.NoError(t, err)
	require.Equal(t, parsed.Identity.Cluster, witness.Cluster)
	require.Equal(t, parsed.Generation, witness.Generation)
	require.Equal(t, parsed.LastConfigurationIndex, witness.ConfigurationIndex)
}

func TestPolicyRecordStrictFrame(t *testing.T) {
	t.Parallel()
	valid, err := MarshalPolicyRecord(policyFixture(t))
	require.NoError(t, err)
	for n := 0; n < len(valid); n++ {
		_, err := ParsePolicyRecord(valid[:n])
		require.Error(t, err, "truncation at %d", n)
	}
	for n := range valid {
		corrupt := bytes.Clone(valid)
		corrupt[n] ^= 1
		_, err := ParsePolicyRecord(corrupt)
		require.Error(t, err, "corruption at %d", n)
	}

	trailing := append(bytes.Clone(valid), 0)
	_, err = ParsePolicyRecord(trailing)
	require.ErrorIs(t, err, ErrInvalid)
	_, err = ParsePolicyRecord(make([]byte, MaxFileSize+1))
	require.ErrorIs(t, err, ErrInvalid)

	unknown := bytes.Clone(valid)
	binary.BigEndian.PutUint32(unknown[len(policyMagic):], 2)
	rechecksumPolicy(unknown)
	_, err = ParsePolicyRecord(unknown)
	require.ErrorIs(t, err, ErrUnsupported)

	badLength := bytes.Clone(valid)
	binary.BigEndian.PutUint32(badLength[len(policyMagic)+4:], ^uint32(0))
	rechecksumPolicy(badLength)
	_, err = ParsePolicyRecord(badLength)
	require.ErrorIs(t, err, ErrInvalid)
}

func rechecksumPolicy(data []byte) {
	sum := sha256.Sum256(data[:len(data)-checksumSize])
	copy(data[len(data)-checksumSize:], sum[:])
}

func TestPolicyRecordValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		change func(*PolicyRecord)
		want   error
	}{
		{"unknown-version", func(p *PolicyRecord) { p.Version = 2 }, ErrUnsupported},
		{"zero-cluster", func(p *PolicyRecord) { p.Identity.Cluster = UUID{} }, ErrInvalid},
		{"zero-baseline", func(p *PolicyRecord) { p.BaselineDigest = Digest{} }, ErrInvalid},
		{"zero-minimum", func(p *PolicyRecord) { p.Minimum.Entry = 0 }, ErrInvalid},
		{"future-minimum", func(p *PolicyRecord) { p.Minimum.Entry = 2 }, ErrUnsupported},
		{"zero-activation", func(p *PolicyRecord) { p.ActivationIndex = 0 }, ErrInvalid},
		{"zero-generation", func(p *PolicyRecord) { p.Generation = 0 }, ErrInvalid},
		{"config-index", func(p *PolicyRecord) { p.LastConfigurationIndex++ }, ErrInvalid},
		{"auto-leave", func(p *PolicyRecord) { p.Configuration.State.AutoLeave = true }, ErrInvalid},
		{"reservation-auto-leave", func(p *PolicyRecord) { p.Reservation.Target.State.AutoLeave = true }, ErrInvalid},
		{"duplicate-retired", func(p *PolicyRecord) { p.RetiredMemberIDs = []uint64{8, 8} }, ErrInvalid},
		{"current-retired", func(p *PolicyRecord) { p.RetiredMemberIDs = []uint64{1} }, ErrInvalid},
		{"unknown-transition", func(p *PolicyRecord) { p.Reservation.Transition = 99 }, ErrInvalid},
		{"zero-reservation-id", func(p *PolicyRecord) { p.Reservation.ID.OperationID = OperationID{} }, ErrInvalid},
		{"stale-reservation-witness", func(p *PolicyRecord) { p.Reservation.Expected.Generation-- }, ErrInvalid},
		{"retired-candidate", func(p *PolicyRecord) { p.Reservation.Candidate.ID = 8 }, ErrInvalid},
		{"add-learner-missing-target", func(p *PolicyRecord) { p.Reservation.Transition = TransitionAddLearner }, ErrInvalid},
		{"replace-address-unrelated-target", func(p *PolicyRecord) {
			p.Reservation.Transition = TransitionReplaceAddress
			p.Reservation.Candidate.Address = "unrelated:9000"
		}, ErrInvalid},
		{"completed-member-not-accounted", func(p *PolicyRecord) { p.TerminalOperations[1].Member = 99 }, ErrInvalid},
		{"zero-evidence", func(p *PolicyRecord) { p.Reservation.EvidenceDigest = Digest{} }, ErrInvalid},
		{"unknown-outcome", func(p *PolicyRecord) { p.TerminalOperations[0].Outcome = 99 }, ErrInvalid},
		{"duplicate-terminal", func(p *PolicyRecord) { p.TerminalOperations[1].ID = p.TerminalOperations[0].ID }, ErrInvalid},
		{"duplicate-terminal-operation-id", func(p *PolicyRecord) {
			p.TerminalOperations[1].ID.OperationID = p.TerminalOperations[0].ID.OperationID
		}, ErrInvalid},
		{"duplicate-terminal-reserve-index", func(p *PolicyRecord) {
			p.TerminalOperations[1].ID.ReserveIndex = p.TerminalOperations[0].ID.ReserveIndex
		}, ErrInvalid},
		{"reused-active-operation-id", func(p *PolicyRecord) {
			p.Reservation.ID.OperationID = p.TerminalOperations[0].ID.OperationID
		}, ErrInvalid},
		{"active-terminal", func(p *PolicyRecord) { p.TerminalOperations[0].ID = p.Reservation.ID }, ErrInvalid},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			policy := policyFixture(t)
			tc.change(&policy)
			_, err := MarshalPolicyRecord(policy)
			require.ErrorIs(t, err, tc.want)
		})
	}
}

func TestReservationMembershipTransitionTargets(t *testing.T) {
	t.Parallel()
	t.Run("remove permits complete roster removal", func(t *testing.T) {
		policy := policyFixture(t)
		policy.Reservation.Transition = TransitionRemove
		policy.Reservation.Candidate = policy.Configuration.Members[0]
		policy.Reservation.Target = policy.Configuration
		policy.Reservation.Target.Members = slices.Clone(policy.Configuration.Members)
		policy.Reservation.Target.State.Voters = slices.Clone(policy.Configuration.State.Voters)
		policy.Reservation.Target.State.Learners = slices.Clone(policy.Configuration.State.Learners)
		policy.Reservation.Target.Index++
		policy.Reservation.Target.Members = slices.DeleteFunc(policy.Reservation.Target.Members, func(m Member) bool { return m.ID == policy.Reservation.Candidate.ID })
		policy.Reservation.Target.State.Voters = slices.DeleteFunc(policy.Reservation.Target.State.Voters, func(id uint64) bool { return id == policy.Reservation.Candidate.ID })
		_, err := canonicalPolicy(policy)
		require.NoError(t, err)
	})

	t.Run("joint enter rejects unrelated voter changes", func(t *testing.T) {
		policy := jointTransitionFixture(t, TransitionEnterJoint)
		policy.Reservation.Target.State.Voters = append(policy.Reservation.Target.State.Voters, 99)
		policy.Reservation.Target.Members = append(policy.Reservation.Target.Members, Member{ID: 99, Address: "node99:9000", PublicKeyDigest: sha256.Sum256([]byte("key99"))})
		_, err := canonicalPolicy(policy)
		require.ErrorIs(t, err, ErrInvalid)
	})

	t.Run("joint leave preserves incoming voter set", func(t *testing.T) {
		policy := jointTransitionFixture(t, TransitionLeaveJoint)
		policy.Reservation.Target.State.Voters = append(policy.Reservation.Target.State.Voters, 99)
		policy.Reservation.Target.Members = append(policy.Reservation.Target.Members, Member{ID: 99, Address: "node99:9000", PublicKeyDigest: sha256.Sum256([]byte("key99"))})
		_, err := canonicalPolicy(policy)
		require.ErrorIs(t, err, ErrInvalid)
	})

	t.Run("joint leave cannot also change learners or peer metadata", func(t *testing.T) {
		policy := jointTransitionFixture(t, TransitionLeaveJoint)
		policy.Reservation.Candidate = policy.Configuration.Members[0]
		policy.Reservation.Target.Members = slices.Clone(policy.Configuration.Members)
		policy.Reservation.Target.Members[0].Address = "changed-during-leave:9000"
		_, err := canonicalPolicy(policy)
		require.ErrorIs(t, err, ErrInvalid)
	})
}

func TestValidatePolicyTransitionConsumesExactReservation(t *testing.T) {
	t.Parallel()
	previous := policyFixture(t)
	next := previous
	next.Reservation = nil
	next.Generation++
	next.Configuration = previous.Reservation.Target
	next.LastConfigurationIndex = next.Configuration.Index
	next.TerminalOperations = append(slices.Clone(previous.TerminalOperations), TerminalMembershipOperation{
		ID: previous.Reservation.ID, Outcome: MembershipCompleted, Member: previous.Reservation.Candidate.ID,
	})
	require.NoError(t, ValidatePolicyTransition(previous, next))

	wrongID := next
	wrongID.TerminalOperations = slices.Clone(next.TerminalOperations)
	wrongID.TerminalOperations[len(wrongID.TerminalOperations)-1].ID.OperationID[0]++
	require.ErrorIs(t, ValidatePolicyTransition(previous, wrongID), ErrInvalid)

	wrongConfig := next
	wrongConfig.Configuration.Members = slices.Clone(next.Configuration.Members)
	wrongConfig.Configuration.Members[0].Address = "other:9000"
	require.ErrorIs(t, ValidatePolicyTransition(previous, wrongConfig), ErrInvalid)

	aborted := previous
	aborted.Reservation = nil
	aborted.Generation++
	aborted.TerminalOperations = append(slices.Clone(previous.TerminalOperations), TerminalMembershipOperation{
		ID: previous.Reservation.ID, Outcome: MembershipAborted, Member: previous.Reservation.Candidate.ID,
	})
	require.NoError(t, ValidatePolicyTransition(previous, aborted))

	aborted.Configuration = next.Configuration
	require.ErrorIs(t, ValidatePolicyTransition(previous, aborted), ErrInvalid)
}

func TestCompletedRemovalMustRetireMemberID(t *testing.T) {
	t.Parallel()
	previous := policyFixture(t)
	reservation := previous.Reservation
	reservation.Transition = TransitionRemove
	reservation.Candidate = previous.Configuration.Members[0]
	reservation.Target = previous.Configuration
	reservation.Target.Members = slices.Clone(previous.Configuration.Members)
	reservation.Target.State.Voters = slices.Clone(previous.Configuration.State.Voters)
	reservation.Target.State.Learners = slices.Clone(previous.Configuration.State.Learners)
	reservation.Target.Index++
	reservation.Target.Members = slices.DeleteFunc(reservation.Target.Members, func(member Member) bool {
		return member.ID == reservation.Candidate.ID
	})
	reservation.Target.State.Voters = slices.DeleteFunc(reservation.Target.State.Voters, func(id uint64) bool {
		return id == reservation.Candidate.ID
	})

	next := previous
	next.Reservation = nil
	next.Generation++
	next.Configuration = reservation.Target
	next.LastConfigurationIndex = next.Configuration.Index
	next.TerminalOperations = append(slices.Clone(previous.TerminalOperations), TerminalMembershipOperation{
		ID: reservation.ID, Outcome: MembershipCompleted, Member: reservation.Candidate.ID,
	})
	require.ErrorIs(t, ValidatePolicyTransition(previous, next), ErrInvalid, "removed IDs cannot be reused")

	next.RetiredMemberIDs = append(slices.Clone(previous.RetiredMemberIDs), reservation.Candidate.ID)
	slices.Sort(next.RetiredMemberIDs)
	require.NoError(t, ValidatePolicyTransition(previous, next))
}

func TestAbortedLearnerAdditionMustRetireCandidateID(t *testing.T) {
	t.Parallel()
	previous := policyFixture(t)
	reservation := previous.Reservation
	reservation.Transition = TransitionAddLearner
	reservation.Candidate = Member{ID: 4, Address: "node4:9000", PublicKeyDigest: Digest{4}}
	reservation.Target = previous.Configuration
	reservation.Target.Index++
	reservation.Target.Members = append(slices.Clone(previous.Configuration.Members), reservation.Candidate)
	reservation.Target.State.Learners = append(slices.Clone(previous.Configuration.State.Learners), reservation.Candidate.ID)
	next := previous
	next.Generation++
	next.Reservation = nil
	next.TerminalOperations = append(slices.Clone(previous.TerminalOperations), TerminalMembershipOperation{
		ID: reservation.ID, Outcome: MembershipAborted, Member: reservation.Candidate.ID,
	})
	require.ErrorIs(t, ValidatePolicyTransition(previous, next), ErrInvalid)
	next.RetiredMemberIDs = append(slices.Clone(previous.RetiredMemberIDs), reservation.Candidate.ID)
	require.NoError(t, ValidatePolicyTransition(previous, next))
}

func jointTransitionFixture(t *testing.T, transition MembershipTransition) PolicyRecord {
	t.Helper()
	policy := policyFixture(t)
	policy.Reservation.Transition = transition
	policy.Reservation.Candidate = policy.Configuration.Members[0]
	policy.Reservation.Target = policy.Configuration
	policy.Reservation.Target.Index++
	if transition == TransitionEnterJoint {
		policy.Reservation.Target.State.Voters = []uint64{1}
		policy.Reservation.Target.State.VotersOutgoing = []uint64{1, 3}
	} else {
		policy.Configuration.State.VotersOutgoing = []uint64{1, 3}
		digest, err := ConfigurationDigest(policy.Configuration)
		require.NoError(t, err)
		policy.Reservation.Expected.ConfigurationDigest = digest
		policy.Reservation.Target = policy.Configuration
		policy.Reservation.Target.Index++
		policy.Reservation.Target.State.VotersOutgoing = nil
	}
	policy.LastConfigurationIndex = policy.Configuration.Index
	return policy
}

func TestPolicyRecordRejectsNonCanonicalWire(t *testing.T) {
	t.Parallel()
	policy := policyFixture(t)
	valid, err := MarshalPolicyRecord(policy)
	require.NoError(t, err)

	// Body offsets before the variable Configuration are fixed. Swap the two
	// first voter IDs to make an otherwise valid record non-canonical.
	const bodyPrefix = 16 + 8 + 32 + 12 + 8 + 8 + 8 + 32
	configurationOffset := len(policyMagic) + 8 + bodyPrefix
	firstVoter := configurationOffset + 8 + 1 + 4
	noncanonical := bytes.Clone(valid)
	a := binary.BigEndian.Uint64(noncanonical[firstVoter:])
	b := binary.BigEndian.Uint64(noncanonical[firstVoter+8:])
	binary.BigEndian.PutUint64(noncanonical[firstVoter:], b)
	binary.BigEndian.PutUint64(noncanonical[firstVoter+8:], a)
	rechecksumPolicy(noncanonical)
	_, err = ParsePolicyRecord(noncanonical)
	require.ErrorIs(t, err, ErrInvalid)
}

func TestPolicyRecordMaximumBound(t *testing.T) {
	t.Parallel()
	policy := policyFixture(t)
	policy.Reservation = nil
	policy.TerminalOperations = nil
	policy.Configuration.Members[0].Address = "[" + string(bytes.Repeat([]byte{'a'}, MaxFileSize)) + "]:1"
	_, err := MarshalPolicyRecord(policy)
	require.ErrorIs(t, err, ErrInvalid)
}

func TestPolicyDigestCoversAuthorityState(t *testing.T) {
	t.Parallel()
	base := policyFixture(t)
	digest, err := PolicyDigest(base)
	require.NoError(t, err)
	mutations := []func(*PolicyRecord){
		func(p *PolicyRecord) { p.Generation++; p.Reservation.Expected.Generation++ },
		func(p *PolicyRecord) { p.RetiredMemberIDs = append(p.RetiredMemberIDs, 10) },
		func(p *PolicyRecord) { p.TerminalOperations[0].Outcome = MembershipCompleted },
	}
	for _, mutate := range mutations {
		changed := policyFixture(t)
		mutate(&changed)
		other, err := PolicyDigest(changed)
		require.NoError(t, err)
		require.NotEqual(t, digest, other)
	}
}

func FuzzParsePolicyRecord(f *testing.F) {
	valid, err := MarshalPolicyRecord(policyFixture(f))
	require.NoError(f, err)
	f.Add(valid)
	f.Add([]byte(policyMagic))
	f.Fuzz(func(t *testing.T, data []byte) {
		policy, err := ParsePolicyRecord(data)
		if err == nil {
			encoded, err := MarshalPolicyRecord(policy)
			require.NoError(t, err)
			require.Equal(t, data, encoded)
		}
	})
}
