package kv

import (
	"bytes"
	"encoding/binary"

	"github.com/cockroachdb/errors"
)

const TSOClaimProtocolVersion uint32 = 2

type TSORequestClaim struct {
	Signed TSOSignedClaimIntent
	Proof  TSOClaimProof
}

func marshalClaimWire(magic string, value any) ([]byte, error) {
	var out bytes.Buffer
	out.WriteString(magic)
	if err := binary.Write(&out, binary.BigEndian, value); err != nil {
		return nil, errors.Wrap(err, "encode timestamp claim wire")
	}
	return out.Bytes(), nil
}

func decodeClaimWire(raw []byte, magic string, value any) error {
	if len(raw) != len(magic)+binary.Size(value) || !bytes.HasPrefix(raw, []byte(magic)) {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	return errors.Wrap(binary.Read(bytes.NewReader(raw[len(magic):]), binary.BigEndian, value), "decode timestamp claim wire")
}

func EncodeSignedTimestampClaim(s TSOSignedClaimIntent) ([]byte, error) {
	if err := s.verify(); err != nil {
		return nil, err
	}
	return marshalClaimWire("TSOSIG01", s)
}

func DecodeSignedTimestampClaim(raw []byte) (TSOSignedClaimIntent, error) {
	var s TSOSignedClaimIntent
	if err := decodeClaimWire(raw, "TSOSIG01", &s); err != nil {
		return s, err
	}
	return s, s.verify()
}

func EncodeTimestampClaimRecord(r TSOClaimRecord) ([]byte, error) {
	return marshalClaimWire("TSOREC01", r)
}

func DecodeTimestampClaimRecord(raw []byte) (TSOClaimRecord, error) {
	var r TSOClaimRecord
	err := decodeClaimWire(raw, "TSOREC01", &r)
	return r, err
}

func EncodeRequestTimestampClaim(p TSORequestClaim) ([]byte, error) {
	if err := p.Signed.verify(); err != nil {
		return nil, err
	}
	if !p.Signed.matchesRecord(p.Proof.Record) || !p.Proof.matches(p.Signed.Binding, claimRolePhase(p.Signed.Binding.Role)) {
		return nil, errors.WithStack(ErrTSOClaimInvalid)
	}
	return marshalClaimWire("TSOPRF01", p)
}

func DecodeRequestTimestampClaim(raw []byte) (TSORequestClaim, error) {
	var p TSORequestClaim
	if err := decodeClaimWire(raw, "TSOPRF01", &p); err != nil {
		return p, err
	}
	if _, err := EncodeRequestTimestampClaim(p); err != nil {
		return p, err
	}
	return p, nil
}
