package kv

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"sort"

	"github.com/cockroachdb/errors"
)

// This component format does not consume EKVTSO06, reserved for the full
// control manifest. A future manifest must include this entire component.
const tsoClaimSnapshotMagic = "EKVCLM01"

type tsoClaimSnapshotHeader struct {
	Magic         [8]byte
	ScalarBytes   uint32
	Term          uint64
	RetiredBefore uint64
	Grants        uint32
	Claims        uint32
}

type tsoClaimGrantWire struct {
	Grant  TSOClaimGrant
	Closed uint8
}

const (
	tsoClaimSnapshotHeaderBytes = 36
	maxTSOClaimSnapshotBytes    = tsoClaimSnapshotHeaderBytes + tsoSnapshotV5Len +
		maxTSOClaimGrants*(tsoClaimGrantBytes+1) + maxTSOClaimRecords*tsoClaimRecordBytes
)

func encodeTSOClaimSnapshot(scalar []byte, l *tsoClaimLedger) ([]byte, error) {
	if l == nil || l.Term == 0 || len(l.Grants) > maxTSOClaimGrants || len(l.Claims) > maxTSOClaimRecords {
		return nil, errors.WithStack(ErrTSOClaimInvalid)
	}
	header := tsoClaimSnapshotHeader{
		ScalarBytes: uint32(len(scalar)), Term: l.Term, RetiredBefore: l.RetiredBefore, // #nosec G115 -- scalar layouts are at most tsoSnapshotV5Len bytes.
		Grants: uint32(len(l.Grants)), Claims: uint32(len(l.Claims)), // #nosec G115 -- bounded by retention caps above.
	}
	copy(header.Magic[:], tsoClaimSnapshotMagic)
	var out bytes.Buffer
	if err := binary.Write(&out, binary.BigEndian, header); err != nil {
		return nil, errors.Wrap(err, "encode TSO claim snapshot header")
	}
	out.Write(scalar)
	grants := sortedTSOClaimGrants(l)
	if err := binary.Write(&out, binary.BigEndian, grants); err != nil {
		return nil, errors.Wrap(err, "encode TSO claim snapshot grants")
	}
	records := sortedTSOClaimRecords(l)
	if err := binary.Write(&out, binary.BigEndian, records); err != nil {
		return nil, errors.Wrap(err, "encode TSO claim snapshot records")
	}
	return out.Bytes(), nil
}

func sortedTSOClaimGrants(l *tsoClaimLedger) []tsoClaimGrantWire {
	grants := make([]tsoClaimGrantWire, 0, len(l.Grants))
	for _, g := range l.Grants {
		wire := tsoClaimGrantWire{Grant: g.Grant}
		if g.Closed {
			wire.Closed = 1
		}
		grants = append(grants, wire)
	}
	sort.Slice(grants, func(i, j int) bool { return grants[i].Grant.Base < grants[j].Grant.Base })
	return grants
}

func sortedTSOClaimRecords(l *tsoClaimLedger) []TSOClaimRecord {
	records := make([]TSOClaimRecord, 0, len(l.Claims))
	for _, r := range l.Claims {
		records = append(records, r)
	}
	sort.Slice(records, func(i, j int) bool { return bytes.Compare(records[i].ID[:], records[j].ID[:]) < 0 })
	return records
}

func readTSOClaimSnapshot(br *bufio.Reader) (*tsoFSMSnapshot, error) {
	payload, err := io.ReadAll(io.LimitReader(br, maxTSOClaimSnapshotBytes+1))
	if err != nil {
		return nil, errors.Wrap(err, "read TSO claim snapshot")
	}
	r := bytes.NewReader(payload)
	var header tsoClaimSnapshotHeader
	if err := binary.Read(r, binary.BigEndian, &header); err != nil {
		return nil, errors.Wrap(err, "decode TSO claim snapshot header")
	}
	if err := validateTSOClaimSnapshotHeader(header, len(payload)); err != nil {
		return nil, err
	}
	scalar := make([]byte, int(header.ScalarBytes))
	if _, err := io.ReadFull(r, scalar); err != nil {
		return nil, errors.Wrap(err, "decode TSO claim snapshot scalar")
	}
	s := &tsoFSMSnapshot{}
	s.ceilingMs, s.allocationFloor, s.cutoverActive, s.phaseDActive, s.phaseDFloor, s.pendingFloor, err =
		readTSOSnapshotState(bufio.NewReader(bytes.NewReader(scalar)))
	if err != nil {
		return nil, err
	}
	if !s.phaseDActive {
		return nil, errors.WithStack(ErrTSOClaimInvalid)
	}
	s.claims, err = decodeTSOClaimSnapshotLedger(r, header, s.phaseDFloor, s.allocationFloor)
	return s, err
}

func validateTSOClaimSnapshotHeader(h tsoClaimSnapshotHeader, size int) error {
	if size < 0 {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	if !h.valid() {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	expected := uint64(tsoClaimSnapshotHeaderBytes) + uint64(h.ScalarBytes) +
		uint64(h.Grants)*(tsoClaimGrantBytes+1) + uint64(h.Claims)*tsoClaimRecordBytes
	if uint64(size) != expected {
		return errors.Wrap(ErrTSOClaimInvalid, "claim snapshot length or trailing bytes")
	}
	return nil
}

func (h tsoClaimSnapshotHeader) valid() bool {
	return string(h.Magic[:]) == tsoClaimSnapshotMagic && h.ScalarBytes <= tsoSnapshotV5Len &&
		h.ScalarBytes >= tsoSnapshotV4Len && h.Term != 0 && h.RetiredBefore == 0 &&
		h.Grants != 0 && h.Grants <= maxTSOClaimGrants && h.Claims <= maxTSOClaimRecords
}

func decodeTSOClaimSnapshotLedger(r *bytes.Reader, h tsoClaimSnapshotHeader, floor, end uint64) (*tsoClaimLedger, error) {
	l := newTSOClaimLedger()
	l.Term, l.RetiredBefore = h.Term, h.RetiredBefore
	grants := make([]tsoClaimGrantWire, int(h.Grants))
	if err := binary.Read(r, binary.BigEndian, grants); err != nil {
		return nil, errors.Wrap(err, "decode retained TSO grants")
	}
	if err := l.restoreGrants(grants, floor, end); err != nil {
		return nil, err
	}
	records := make([]TSOClaimRecord, int(h.Claims))
	if err := binary.Read(r, binary.BigEndian, records); err != nil {
		return nil, errors.Wrap(err, "decode retained TSO claims")
	}
	if err := l.restoreClaims(records); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *tsoClaimLedger) restoreGrants(grants []tsoClaimGrantWire, floor, end uint64) error {
	previousEnd := floor
	var maxTerm uint64
	for _, retained := range grants {
		g := retained.Grant
		if !retained.validFor(l.Term, maxTerm, previousEnd, end) {
			return errors.WithStack(ErrTSOClaimInvalid)
		}
		if _, exists := l.Grants[g.ID]; exists {
			return errors.WithStack(ErrTSOClaimConflict)
		}
		l.Grants[g.ID] = tsoRetainedGrant{Grant: g, Closed: retained.Closed == 1}
		previousEnd = g.end()
		maxTerm = max(maxTerm, g.Term)
	}
	if maxTerm != l.Term {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	return nil
}

func (g tsoClaimGrantWire) validFor(term, previousTerm, floor, end uint64) bool {
	return g.Grant.valid() && g.Grant.Term <= term && g.Grant.Term >= previousTerm &&
		g.Grant.Base > floor && g.Grant.end() <= end && g.Closed <= 1 &&
		(g.Closed == 1) == (g.Grant.Term < term)
}

func (l *tsoClaimLedger) restoreClaims(records []TSOClaimRecord) error {
	for i, r := range records {
		if i > 0 && bytes.Compare(records[i-1].ID[:], r.ID[:]) >= 0 {
			return errors.WithStack(ErrTSOClaimConflict)
		}
		if err := l.checkRecord(r, true); err != nil {
			return err
		}
		l.insertRecord(r)
	}
	return nil
}

// Restore cannot silently discard claims already known here, reopen a grant,
// or infer claims from a scalar range. GC is deliberately unsupported until
// transaction/resolver retention has a durable cross-group proof.
func (f *TSOStateMachine) checkClaimRestore(s *tsoFSMSnapshot) error {
	if f.claims == nil || f.claims.Term == 0 {
		return nil
	}
	l := s.claims
	if l == nil || l.Term < f.claims.Term || l.RetiredBefore != f.claims.RetiredBefore ||
		s.phaseDFloor != f.PhaseDFloor() {
		return errors.Wrap(ErrTSOClaimInvalid, "snapshot would discard claim history")
	}
	return l.requireHistory(f.claims)
}

func (l *tsoClaimLedger) requireHistory(priorLedger *tsoClaimLedger) error {
	for id, prior := range priorLedger.Grants {
		next, exists := l.Grants[id]
		if !exists || next.Grant != prior.Grant || prior.Closed && !next.Closed {
			return errors.Wrap(ErrTSOClaimInvalid, "snapshot would discard or reopen a grant")
		}
	}
	for id, prior := range priorLedger.Claims {
		if l.Claims[id] != prior {
			return errors.Wrap(ErrTSOClaimInvalid, "snapshot would discard or rebind a claim")
		}
	}
	return nil
}

func (f *TSOStateMachine) restoreClaimSnapshot(br *bufio.Reader) error {
	s, err := readTSOClaimSnapshot(br)
	if err != nil {
		return err
	}
	if f == nil {
		return nil
	}
	if f.PhaseDActive() && f.PhaseDFloor() != s.phaseDFloor {
		return errors.Wrap(ErrTSOClaimInvalid, "claim snapshot has a different phase-D boundary")
	}
	if err := f.checkClaimRestore(s); err != nil {
		return err
	}
	if err := f.restorePendingPhaseDFloor(s.pendingFloor, s.allocationFloor, s.phaseDActive, s.phaseDFloor); err != nil {
		return err
	}
	f.restoreSnapshotState(s.ceilingMs, s.allocationFloor, s.cutoverActive, s.phaseDActive, s.phaseDFloor)
	f.claims = s.claims
	return nil
}
