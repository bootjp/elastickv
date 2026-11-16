package raftbootstrap

import (
	"bytes"
	"encoding/binary"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
)

const (
	membershipContextVersion    byte = 4
	membershipContextPrefixSize      = 1 + operationIDSize + 8 + operationIDSize
	membershipContextFixedSize       = membershipContextPrefixSize + WitnessSize + 4 + checksumSize + checksumSize + 8 + 4 + 4
)

// MembershipChangeContext binds a native ConfChange to one replicated
// reservation. The native change type and resulting configuration must still
// be checked against the reserved transition before RawNode applies it.
type MembershipChangeContext struct {
	ID           ReservationID
	Expected     Witness
	Transition   MembershipTransition
	TargetDigest Digest
	Candidate    Member
	PeerID       string
}

func EncodeMembershipChangeContext(context MembershipChangeContext) ([]byte, error) {
	if err := validateMembershipChangeContext(context); err != nil {
		return nil, err
	}
	length := membershipContextFixedSize + len(context.PeerID) + len(context.Candidate.Address)
	out := make([]byte, length)
	out[0] = membershipContextVersion
	copy(out[1:17], context.ID.OperationID[:])
	binary.BigEndian.PutUint64(out[17:25], context.ID.ReserveIndex)
	copy(out[25:41], context.ID.OperationID[:])
	encodeWitness(out[membershipContextPrefixSize:membershipContextPrefixSize+WitnessSize], context.Expected)
	offset := membershipContextPrefixSize + WitnessSize
	binary.BigEndian.PutUint32(out[offset:offset+4], uint32(context.Transition))
	offset += 4
	copy(out[offset:offset+checksumSize], context.TargetDigest[:])
	offset += checksumSize
	copy(out[offset:offset+checksumSize], context.Candidate.PublicKeyDigest[:])
	offset += checksumSize
	binary.BigEndian.PutUint64(out[offset:offset+8], context.Candidate.ID)
	offset += 8
	binary.BigEndian.PutUint32(out[offset:offset+4], uint32(len(context.PeerID))) //nolint:gosec // The complete context was bounded above.
	offset += 4
	binary.BigEndian.PutUint32(out[offset:offset+4], uint32(len(context.Candidate.Address))) //nolint:gosec // The complete context was bounded above.
	offset += 4
	copy(out[offset:], context.PeerID)
	copy(out[offset+len(context.PeerID):], context.Candidate.Address)
	return out, nil
}

func ParseMembershipChangeContext(data []byte) (MembershipChangeContext, error) {
	if len(data) < membershipContextFixedSize || len(data) > MaxFileSize {
		return MembershipChangeContext{}, errors.WithStack(ErrInvalid)
	}
	if data[0] != membershipContextVersion {
		return MembershipChangeContext{}, errors.WithStack(ErrUnsupported)
	}
	var context MembershipChangeContext
	copy(context.ID.OperationID[:], data[1:17])
	context.ID.ReserveIndex = binary.BigEndian.Uint64(data[17:25])
	if !bytes.Equal(data[25:41], context.ID.OperationID[:]) {
		return MembershipChangeContext{}, errors.WithStack(ErrInvalid)
	}
	context.Expected = decodeWitness(data[membershipContextPrefixSize : membershipContextPrefixSize+WitnessSize])
	offset := membershipContextPrefixSize + WitnessSize
	context.Transition = MembershipTransition(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4
	copy(context.TargetDigest[:], data[offset:offset+checksumSize])
	offset += checksumSize
	copy(context.Candidate.PublicKeyDigest[:], data[offset:offset+checksumSize])
	offset += checksumSize
	context.Candidate.ID = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8
	peerIDLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4
	addressLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4
	if peerIDLen == 0 || peerIDLen > len(data)-offset || addressLen != len(data)-offset-peerIDLen {
		return MembershipChangeContext{}, errors.WithStack(ErrInvalid)
	}
	context.PeerID = string(data[offset : offset+peerIDLen])
	context.Candidate.Address = string(data[offset+peerIDLen:])
	if err := validateMembershipChangeContext(context); err != nil {
		return MembershipChangeContext{}, err
	}
	return context, nil
}

func (context MembershipChangeContext) MatchesReservation(reservation MembershipReservation, peerID string) bool {
	targetDigest, err := ConfigurationDigest(reservation.Target)
	if err != nil {
		return false
	}
	return context.ID == reservation.ID && context.Expected == reservation.Expected &&
		context.Transition == reservation.Transition && context.TargetDigest == targetDigest &&
		context.Candidate == reservation.Candidate &&
		context.PeerID == peerID
}

func validateMembershipChangeContext(context MembershipChangeContext) error {
	if !validReservationID(context.ID) || !validActiveWitness(context.Expected) ||
		!validMembershipTransition(context.Transition) || context.TargetDigest == (Digest{}) || context.Candidate.ID == 0 ||
		context.Candidate.PublicKeyDigest == (Digest{}) || !validAddress(context.Candidate.Address) {
		return errors.WithStack(ErrInvalid)
	}
	if !validMembershipContextPeer(context.PeerID, context.Candidate.Address) {
		return errors.WithStack(ErrInvalid)
	}
	return nil
}

func validMembershipContextPeer(peerID, address string) bool {
	return peerID != "" && utf8.ValidString(peerID) && bytes.IndexByte([]byte(peerID), 0) < 0 &&
		len(peerID) <= MaxFileSize-membershipContextFixedSize-len(address)
}
