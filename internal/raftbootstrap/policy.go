package raftbootstrap

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"slices"

	"github.com/cockroachdb/errors"
)

const (
	policyMagic                     = "EKVBP001"
	PolicyVersion            uint32 = 1
	policyFrameOverhead             = len(policyMagic) + 4 + 4 + checksumSize
	policyHeaderSize                = len(policyMagic) + 4 + 4
	policyGenerationMinimum         = 2
	policyTerminalRecordSize        = 8 + operationIDSize + 4 + 8
)

type MembershipTransition uint32

const (
	TransitionAddLearner MembershipTransition = iota + 1
	TransitionPromote
	TransitionRemove
	TransitionReplaceAddress
	TransitionReplaceKey
	TransitionEnterJoint
	TransitionLeaveJoint
)

type MembershipTerminalOutcome uint32

const (
	MembershipCompleted MembershipTerminalOutcome = iota + 1
	MembershipAborted
)

type ReservationID struct {
	ReserveIndex uint64
	OperationID  OperationID
}

type MembershipReservation struct {
	ID             ReservationID
	Expected       Witness
	Transition     MembershipTransition
	Candidate      Member
	Target         Configuration
	EvidenceDigest Digest
}

type TerminalMembershipOperation struct {
	ID      ReservationID
	Outcome MembershipTerminalOutcome
	Member  uint64
}

// PolicyRecord is the authoritative replicated bootstrap and membership
// policy. Session and process-local state must not be stored here.
type PolicyRecord struct {
	Version                uint32
	Identity               Identity
	BaselineDigest         Digest
	Minimum                Versions
	ActivationIndex        uint64
	Generation             uint64
	LastConfigurationIndex uint64
	Configuration          Configuration
	RetiredMemberIDs       []uint64
	Reservation            *MembershipReservation
	TerminalOperations     []TerminalMembershipOperation
}

func validMembershipTransition(v MembershipTransition) bool {
	return v >= TransitionAddLearner && v <= TransitionLeaveJoint
}

func validTerminalOutcome(v MembershipTerminalOutcome) bool {
	return v == MembershipCompleted || v == MembershipAborted
}

func validReservationID(id ReservationID) bool {
	return id.ReserveIndex != 0 && id.OperationID != (OperationID{})
}

func compareOperationID(a, b OperationID) int { return bytes.Compare(a[:], b[:]) }

func compareReservationID(a, b ReservationID) int {
	if c := compareID(a.ReserveIndex, b.ReserveIndex); c != 0 {
		return c
	}
	return compareOperationID(a.OperationID, b.OperationID)
}

func canonicalPolicy(in PolicyRecord) (PolicyRecord, error) {
	if err := validatePolicyHeader(in); err != nil {
		return PolicyRecord{}, err
	}
	configuration, err := canonicalConfiguration(in.Configuration)
	if err != nil {
		return PolicyRecord{}, err
	}
	if configuration.Index != in.LastConfigurationIndex || configuration.State.AutoLeave {
		return PolicyRecord{}, errors.WithStack(ErrInvalid)
	}
	in.Configuration = configuration
	if err := canonicalRetiredMembers(&in); err != nil {
		return PolicyRecord{}, err
	}
	if err := canonicalPolicyReservation(&in); err != nil {
		return PolicyRecord{}, err
	}
	if err := canonicalTerminalOperations(&in); err != nil {
		return PolicyRecord{}, err
	}
	return in, nil
}

func validatePolicyHeader(in PolicyRecord) error {
	if in.Version != PolicyVersion {
		return errors.WithStack(ErrUnsupported)
	}
	if in.Identity.Cluster == (UUID{}) || in.BaselineDigest == (Digest{}) ||
		in.ActivationIndex == 0 || in.Generation == 0 || in.LastConfigurationIndex == 0 {
		return errors.WithStack(ErrInvalid)
	}
	return validateVersions(in.Minimum)
}

func canonicalRetiredMembers(in *PolicyRecord) error {
	in.RetiredMemberIDs = slices.Clone(in.RetiredMemberIDs)
	slices.Sort(in.RetiredMemberIDs)
	if err := validateIDs(in.RetiredMemberIDs); err != nil {
		return err
	}
	for _, id := range in.RetiredMemberIDs {
		if memberInConfiguration(in.Configuration, id) {
			return errors.WithStack(ErrInvalid)
		}
	}
	return nil
}

func canonicalPolicyReservation(in *PolicyRecord) error {
	if in.Reservation == nil {
		return nil
	}
	reservation := *in.Reservation
	if err := canonicalReservation(*in, &reservation); err != nil {
		return err
	}
	in.Reservation = &reservation
	return nil
}

func canonicalTerminalOperations(in *PolicyRecord) error {
	in.TerminalOperations = slices.Clone(in.TerminalOperations)
	slices.SortFunc(in.TerminalOperations, func(a, b TerminalMembershipOperation) int {
		return compareReservationID(a.ID, b.ID)
	})
	operationIDs := make(map[OperationID]struct{}, len(in.TerminalOperations))
	for i, operation := range in.TerminalOperations {
		if err := validateTerminalOperation(*in, i, operation); err != nil {
			return err
		}
		if _, duplicate := operationIDs[operation.ID.OperationID]; duplicate {
			return errors.WithStack(ErrInvalid)
		}
		operationIDs[operation.ID.OperationID] = struct{}{}
	}
	return nil
}

func validateTerminalOperation(policy PolicyRecord, index int, operation TerminalMembershipOperation) error {
	if !validReservationID(operation.ID) || !validTerminalOutcome(operation.Outcome) || operation.Member == 0 {
		return errors.WithStack(ErrInvalid)
	}
	if terminalOperationIDConflict(policy, index, operation) {
		return errors.WithStack(ErrInvalid)
	}
	if operation.Outcome == MembershipCompleted && !memberInConfiguration(policy.Configuration, operation.Member) &&
		!slices.Contains(policy.RetiredMemberIDs, operation.Member) {
		return errors.WithStack(ErrInvalid)
	}
	return nil
}

// ValidatePolicyTransition binds terminal receipts to the reservation they
// consume. A standalone policy frame cannot prove how it was reached; callers
// applying replicated policy entries must validate the previous and next
// records together before publishing the next record.
func ValidatePolicyTransition(previous, next PolicyRecord) error {
	old, err := canonicalPolicy(previous)
	if err != nil {
		return err
	}
	updated, err := canonicalPolicy(next)
	if err != nil {
		return err
	}
	if !samePolicyLineage(old, updated) {
		return errors.WithStack(ErrInvalid)
	}
	if !consumesReservation(old, updated) {
		return errors.WithStack(ErrInvalid)
	}
	terminal := updated.TerminalOperations[len(updated.TerminalOperations)-1]
	if terminal.ID != old.Reservation.ID || terminal.Member != old.Reservation.Candidate.ID {
		return errors.WithStack(ErrInvalid)
	}
	return validateTerminalTransition(old, updated, terminal)
}

func validateTerminalTransition(old, updated PolicyRecord, terminal TerminalMembershipOperation) error {
	switch terminal.Outcome {
	case MembershipCompleted:
		target, err := canonicalConfiguration(old.Reservation.Target)
		if err != nil || !sameConfiguration(updated.Configuration, target) {
			return errors.WithStack(ErrInvalid)
		}
		return validateTerminalRetiredMembers(old, updated, terminal.Outcome)
	case MembershipAborted:
		if !sameConfiguration(updated.Configuration, old.Configuration) {
			return errors.WithStack(ErrInvalid)
		}
		return validateTerminalRetiredMembers(old, updated, terminal.Outcome)
	default:
		return errors.WithStack(ErrInvalid)
	}
}

func validateTerminalRetiredMembers(old, updated PolicyRecord, outcome MembershipTerminalOutcome) error {
	expected := slices.Clone(old.RetiredMemberIDs)
	if outcome == MembershipCompleted && old.Reservation.Transition == TransitionRemove ||
		outcome == MembershipAborted && old.Reservation.Transition == TransitionAddLearner {
		expected = append(expected, old.Reservation.Candidate.ID)
		slices.Sort(expected)
	}
	if !slices.Equal(updated.RetiredMemberIDs, expected) {
		return errors.WithStack(ErrInvalid)
	}
	return nil
}

func samePolicyLineage(old, updated PolicyRecord) bool {
	return old.Identity == updated.Identity && old.BaselineDigest == updated.BaselineDigest &&
		old.Minimum == updated.Minimum && old.ActivationIndex == updated.ActivationIndex &&
		updated.Generation == old.Generation+1
}

func consumesReservation(old, updated PolicyRecord) bool {
	if old.Reservation == nil || updated.Reservation != nil ||
		len(updated.TerminalOperations) != len(old.TerminalOperations)+1 {
		return false
	}
	for i := range old.TerminalOperations {
		if old.TerminalOperations[i] != updated.TerminalOperations[i] {
			return false
		}
	}
	return true
}

func sameConfiguration(a, b Configuration) bool {
	digestA, errA := ConfigurationDigest(a)
	digestB, errB := ConfigurationDigest(b)
	return errA == nil && errB == nil && digestA == digestB
}

func terminalOperationIDConflict(policy PolicyRecord, index int, operation TerminalMembershipOperation) bool {
	if index > 0 {
		previous := policy.TerminalOperations[index-1].ID
		if previous.ReserveIndex >= operation.ID.ReserveIndex {
			return true
		}
	}
	return policy.Reservation != nil && (policy.Reservation.ID.ReserveIndex == operation.ID.ReserveIndex ||
		policy.Reservation.ID.OperationID == operation.ID.OperationID)
}

func canonicalReservation(policy PolicyRecord, reservation *MembershipReservation) error {
	if err := validateReservationHeader(*reservation); err != nil {
		return err
	}
	if policy.Generation < policyGenerationMinimum {
		return errors.WithStack(ErrInvalid)
	}
	if err := validateReservationWitness(policy, *reservation); err != nil {
		return err
	}
	if err := canonicalizeReservationTarget(policy, reservation); err != nil {
		return err
	}
	return validateReservationTarget(policy.Configuration, *reservation)
}

func validateReservationWitness(policy PolicyRecord, reservation MembershipReservation) error {
	digest, err := ConfigurationDigest(policy.Configuration)
	if err != nil {
		return err
	}
	expected := Witness{
		Cluster:             policy.Identity.Cluster,
		Group:               policy.Identity.Group,
		ActivationIndex:     policy.ActivationIndex,
		Generation:          policy.Generation - 1,
		ConfigurationIndex:  policy.LastConfigurationIndex,
		ConfigurationDigest: digest,
	}
	if reservation.Expected != expected {
		return errors.WithStack(ErrInvalid)
	}
	return nil
}

func canonicalizeReservationTarget(policy PolicyRecord, reservation *MembershipReservation) error {
	target, err := canonicalConfiguration(reservation.Target)
	if err != nil {
		return errors.WithStack(err)
	}
	if target.State.AutoLeave {
		return errors.WithStack(ErrInvalid)
	}
	reservation.Target = target
	for _, id := range policy.RetiredMemberIDs {
		if reservation.Candidate.ID == id || memberInConfiguration(target, id) {
			return errors.WithStack(ErrInvalid)
		}
	}
	return nil
}

func validateReservationHeader(reservation MembershipReservation) error {
	if !validReservationID(reservation.ID) || !validMembershipTransition(reservation.Transition) ||
		reservation.Candidate.ID == 0 || reservation.Candidate.PublicKeyDigest == (Digest{}) ||
		!validAddress(reservation.Candidate.Address) || reservation.EvidenceDigest == (Digest{}) {
		return errors.WithStack(ErrInvalid)
	}
	return nil
}

func validateReservationTarget(current Configuration, reservation MembershipReservation) error {
	if reservation.Target.Index <= current.Index {
		return errors.WithStack(ErrInvalid)
	}
	if err := validateTargetDiff(current, reservation.Target, reservation.Candidate.ID); err != nil {
		return err
	}
	validators := map[MembershipTransition]func(Configuration, MembershipReservation) error{
		TransitionAddLearner:     validateAddLearnerTarget,
		TransitionPromote:        validatePromoteTarget,
		TransitionRemove:         validateRemoveTarget,
		TransitionReplaceAddress: validateReplaceAddressTarget,
		TransitionReplaceKey:     validateReplaceKeyTarget,
		TransitionEnterJoint:     validateEnterJointTarget,
		TransitionLeaveJoint:     validateLeaveJointTarget,
	}
	validate := validators[reservation.Transition]
	if validate == nil {
		return errors.WithStack(ErrInvalid)
	}
	return validate(current, reservation)
}

func validateTargetDiff(current, target Configuration, candidateID uint64) error {
	if !sameIDsExcept(current.State.Voters, target.State.Voters, candidateID) ||
		!sameIDsExcept(current.State.VotersOutgoing, target.State.VotersOutgoing, candidateID) ||
		!sameIDsExcept(current.State.Learners, target.State.Learners, candidateID) ||
		!sameIDsExcept(current.State.LearnersNext, target.State.LearnersNext, candidateID) ||
		!sameConfigurationMembersExcept(current, target, candidateID) {
		return errors.WithStack(ErrInvalid)
	}
	return nil
}

func sameIDsExcept(a, b []uint64, except uint64) bool {
	filteredA := filterID(a, except)
	filteredB := filterID(b, except)
	return slices.Equal(filteredA, filteredB)
}

func filterID(ids []uint64, except uint64) []uint64 {
	filtered := make([]uint64, 0, len(ids))
	for _, id := range ids {
		if id != except {
			filtered = append(filtered, id)
		}
	}
	return filtered
}

func sameConfigurationMembersExcept(current, target Configuration, except uint64) bool {
	currentMembers := make([]Member, 0, len(current.Members))
	targetMembers := make([]Member, 0, len(target.Members))
	for _, member := range current.Members {
		if member.ID != except {
			currentMembers = append(currentMembers, member)
		}
	}
	for _, member := range target.Members {
		if member.ID != except {
			targetMembers = append(targetMembers, member)
		}
	}
	return slices.Equal(currentMembers, targetMembers)
}

func validateAddLearnerTarget(current Configuration, reservation MembershipReservation) error {
	candidate := reservation.Candidate
	_, currentlyMember := configurationMember(current, candidate.ID)
	targetMember, targetHasMember := configurationMember(reservation.Target, candidate.ID)
	if currentlyMember || !targetHasMember || targetMember != candidate ||
		!slices.Contains(reservation.Target.State.Learners, candidate.ID) {
		return errors.WithStack(ErrInvalid)
	}
	return nil
}

func validatePromoteTarget(current Configuration, reservation MembershipReservation) error {
	candidate := reservation.Candidate
	currentMember, currentlyMember := configurationMember(current, candidate.ID)
	targetMember, targetHasMember := configurationMember(reservation.Target, candidate.ID)
	if !currentlyMember || currentMember != candidate || !targetHasMember || targetMember != candidate ||
		!slices.Contains(current.State.Learners, candidate.ID) ||
		!slices.Contains(reservation.Target.State.Voters, candidate.ID) {
		return errors.WithStack(ErrInvalid)
	}
	return nil
}

func validateRemoveTarget(current Configuration, reservation MembershipReservation) error {
	candidate := reservation.Candidate
	currentMember, currentlyMember := configurationMember(current, candidate.ID)
	_, targetHasMember := configurationMember(reservation.Target, candidate.ID)
	if !currentlyMember || currentMember != candidate || targetHasMember {
		return errors.WithStack(ErrInvalid)
	}
	return nil
}

func validateReplaceAddressTarget(current Configuration, reservation MembershipReservation) error {
	return validateReplaceMemberTarget(current, reservation, true)
}

func validateReplaceKeyTarget(current Configuration, reservation MembershipReservation) error {
	return validateReplaceMemberTarget(current, reservation, false)
}

func validateReplaceMemberTarget(current Configuration, reservation MembershipReservation, address bool) error {
	currentMember, currentlyMember := configurationMember(current, reservation.Candidate.ID)
	targetMember, targetHasMember := configurationMember(reservation.Target, reservation.Candidate.ID)
	if !currentlyMember || !targetHasMember || currentMember.ID != targetMember.ID || targetMember != reservation.Candidate {
		return errors.WithStack(ErrInvalid)
	}
	if err := validateReplacementMode(currentMember, targetMember, address); err != nil {
		return err
	}
	return nil
}

func validateReplacementMode(current, target Member, address bool) error {
	if address {
		if current.PublicKeyDigest != target.PublicKeyDigest || current.Address == target.Address {
			return errors.WithStack(ErrInvalid)
		}
		return nil
	}
	if current.Address != target.Address || current.PublicKeyDigest == target.PublicKeyDigest {
		return errors.WithStack(ErrInvalid)
	}
	return nil
}

func validateEnterJointTarget(current Configuration, reservation MembershipReservation) error {
	target := reservation.Target
	candidateID := reservation.Candidate.ID
	_, currentHasCandidate := configurationMember(current, candidateID)
	_, targetHasCandidate := configurationMember(target, candidateID)
	if current.State.AutoLeave || len(current.State.VotersOutgoing) != 0 || !currentHasCandidate || !targetHasCandidate ||
		!slices.Equal(target.State.VotersOutgoing, current.State.Voters) ||
		!validJointEntryVoters(current.State.Voters, target.State.Voters, candidateID) ||
		!slices.Equal(current.State.Learners, target.State.Learners) || !slices.Equal(current.State.LearnersNext, target.State.LearnersNext) ||
		!slices.Equal(current.Members, target.Members) {
		return errors.WithStack(ErrInvalid)
	}
	return nil
}

func validJointEntryVoters(current, target []uint64, candidateID uint64) bool {
	return sameIDsExcept(current, target, candidateID) && !slices.Equal(current, target)
}

func validateLeaveJointTarget(current Configuration, reservation MembershipReservation) error {
	target := reservation.Target
	if len(current.State.VotersOutgoing) == 0 || len(target.State.VotersOutgoing) != 0 ||
		!slices.Equal(current.State.Voters, target.State.Voters) ||
		!slices.Equal(current.State.Learners, target.State.Learners) ||
		!slices.Equal(current.State.LearnersNext, target.State.LearnersNext) ||
		!slices.Equal(current.Members, target.Members) {
		return errors.WithStack(ErrInvalid)
	}
	return nil
}

func configurationMember(configuration Configuration, id uint64) (Member, bool) {
	index, found := slices.BinarySearchFunc(configuration.Members, id, func(member Member, id uint64) int {
		return compareID(member.ID, id)
	})
	if !found {
		return Member{}, false
	}
	return configuration.Members[index], true
}

func memberInConfiguration(configuration Configuration, id uint64) bool {
	_, found := slices.BinarySearchFunc(configuration.Members, id, func(member Member, id uint64) int {
		return compareID(member.ID, id)
	})
	return found
}

func encodeReservationID(w *encoder, id ReservationID) {
	w.fixed(id.ReserveIndex)
	w.fixed(id.OperationID)
}

func encodePolicyBody(w *encoder, policy PolicyRecord) {
	w.fixed(policy.Identity.Cluster)
	w.fixed(policy.Identity.Group)
	w.fixed(policy.BaselineDigest)
	w.fixed(policy.Minimum)
	w.fixed(policy.ActivationIndex)
	w.fixed(policy.Generation)
	w.fixed(policy.LastConfigurationIndex)
	encodeConfiguration(w, policy.Configuration)
	w.length(len(policy.RetiredMemberIDs))
	for _, id := range policy.RetiredMemberIDs {
		w.fixed(id)
	}
	if policy.Reservation == nil {
		w.fixed(uint8(0))
	} else {
		w.fixed(uint8(1))
		reservation := policy.Reservation
		encodeReservationID(w, reservation.ID)
		w.fixed(reservation.Expected)
		w.fixed(reservation.Transition)
		w.fixed(reservation.Candidate.ID)
		w.length(len(reservation.Candidate.Address))
		w.appendString(reservation.Candidate.Address)
		w.fixed(reservation.Candidate.PublicKeyDigest)
		encodeConfiguration(w, reservation.Target)
		w.fixed(reservation.EvidenceDigest)
	}
	w.length(len(policy.TerminalOperations))
	for _, operation := range policy.TerminalOperations {
		encodeReservationID(w, operation.ID)
		w.fixed(operation.Outcome)
		w.fixed(operation.Member)
	}
}

func canonicalPolicyBody(policy PolicyRecord) ([]byte, error) {
	policy, err := canonicalPolicy(policy)
	if err != nil {
		return nil, err
	}
	w := &encoder{limit: MaxFileSize - policyFrameOverhead}
	encodePolicyBody(w, policy)
	if w.err != nil {
		return nil, w.err
	}
	return bytes.Clone(w.Bytes()), nil
}

// MarshalPolicyRecord emits a canonical, checksummed record no larger than
// MaxFileSize. The checksum detects corruption; it is not authentication.
func MarshalPolicyRecord(policy PolicyRecord) ([]byte, error) {
	body, err := canonicalPolicyBody(policy)
	if err != nil {
		return nil, err
	}
	w := &encoder{limit: MaxFileSize}
	w.appendString(policyMagic)
	w.fixed(PolicyVersion)
	w.length(len(body))
	w.appendBytes(body)
	sum := sha256.Sum256(w.Bytes())
	w.appendBytes(sum[:])
	if w.err != nil || w.Len() > MaxFileSize {
		return nil, errors.WithStack(ErrInvalid)
	}
	return bytes.Clone(w.Bytes()), nil
}

func decodeReservationID(r *decoder) ReservationID {
	var id ReservationID
	r.fixed(&id.ReserveIndex)
	r.fixed(&id.OperationID)
	return id
}

func decodePolicyBody(r *decoder, version uint32) PolicyRecord {
	policy := PolicyRecord{Version: version}
	r.fixed(&policy.Identity.Cluster)
	r.fixed(&policy.Identity.Group)
	r.fixed(&policy.BaselineDigest)
	r.fixed(&policy.Minimum)
	r.fixed(&policy.ActivationIndex)
	r.fixed(&policy.Generation)
	r.fixed(&policy.LastConfigurationIndex)
	policy.Configuration = decodeConfiguration(r)
	n := r.count(memberIDSize)
	policy.RetiredMemberIDs = make([]uint64, n)
	for i := range policy.RetiredMemberIDs {
		r.fixed(&policy.RetiredMemberIDs[i])
	}
	var present uint8
	r.fixed(&present)
	if present == 1 {
		reservation := &MembershipReservation{}
		reservation.ID = decodeReservationID(r)
		r.fixed(&reservation.Expected)
		r.fixed(&reservation.Transition)
		r.fixed(&reservation.Candidate.ID)
		size := r.count(1)
		address := make([]byte, size)
		if r.err == nil {
			_, r.err = io.ReadFull(r.Reader, address)
		}
		reservation.Candidate.Address = string(address)
		r.fixed(&reservation.Candidate.PublicKeyDigest)
		reservation.Target = decodeConfiguration(r)
		r.fixed(&reservation.EvidenceDigest)
		policy.Reservation = reservation
	} else if present != 0 {
		r.err = errors.WithStack(ErrInvalid)
	}
	n = r.count(policyTerminalRecordSize)
	policy.TerminalOperations = make([]TerminalMembershipOperation, n)
	for i := range policy.TerminalOperations {
		operation := &policy.TerminalOperations[i]
		operation.ID = decodeReservationID(r)
		r.fixed(&operation.Outcome)
		r.fixed(&operation.Member)
	}
	return policy
}

// ParsePolicyRecord accepts canonical v1 records only.
func ParsePolicyRecord(data []byte) (PolicyRecord, error) {
	if len(data) > MaxFileSize || len(data) < policyFrameOverhead {
		return PolicyRecord{}, errors.WithStack(ErrInvalid)
	}
	version, payload, err := parsePolicyFrame(data)
	if err != nil {
		return PolicyRecord{}, err
	}
	r := &decoder{Reader: bytes.NewReader(payload)}
	policy := decodePolicyBody(r, version)
	if r.err != nil || r.Len() != 0 {
		return PolicyRecord{}, errors.WithStack(ErrInvalid)
	}
	canonical, err := MarshalPolicyRecord(policy)
	if err != nil {
		return PolicyRecord{}, err
	}
	if !bytes.Equal(canonical, data) {
		return PolicyRecord{}, errors.WithStack(ErrInvalid)
	}
	return policy, nil
}

func parsePolicyFrame(data []byte) (uint32, []byte, error) {
	if string(data[:len(policyMagic)]) != policyMagic {
		return 0, nil, errors.WithStack(ErrUnsupported)
	}
	version := binary.BigEndian.Uint32(data[len(policyMagic):])
	if version != PolicyVersion {
		return 0, nil, errors.WithStack(ErrUnsupported)
	}
	bodyLen := binary.BigEndian.Uint32(data[len(policyMagic)+4:])
	if int64(bodyLen) != int64(len(data))-int64(policyFrameOverhead) {
		return 0, nil, errors.WithStack(ErrInvalid)
	}
	checksumOffset := len(data) - checksumSize
	sum := sha256.Sum256(data[:checksumOffset])
	if !bytes.Equal(sum[:], data[checksumOffset:]) {
		return 0, nil, errors.WithStack(ErrInvalid)
	}
	return version, data[policyHeaderSize:checksumOffset], nil
}

// PolicyDigest binds the complete canonical policy, including reservation
// and terminal membership state, without the outer checksum frame.
func PolicyDigest(policy PolicyRecord) (Digest, error) {
	body, err := canonicalPolicyBody(policy)
	if err != nil {
		return Digest{}, err
	}
	return sha256.Sum256(body), nil
}

// Witness derives the exact active policy/configuration witness.
func (policy PolicyRecord) Witness() (Witness, error) {
	policy, err := canonicalPolicy(policy)
	if err != nil {
		return Witness{}, err
	}
	digest, err := ConfigurationDigest(policy.Configuration)
	if err != nil {
		return Witness{}, err
	}
	return Witness{
		Cluster:             policy.Identity.Cluster,
		Group:               policy.Identity.Group,
		ActivationIndex:     policy.ActivationIndex,
		Generation:          policy.Generation,
		ConfigurationIndex:  policy.LastConfigurationIndex,
		ConfigurationDigest: digest,
	}, nil
}
