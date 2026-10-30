package kv

import (
	"bytes"
	"encoding/binary"

	"github.com/cockroachdb/errors"
)

const (
	tsoClaimGrantEnvelope = "\x07TSOG\x01"
	tsoClaimBatchEnvelope = "\x07TSOQ\x01"
	tsoClaimRecordBytes   = 153
	tsoClaimGrantBytes    = 68
	maxTSOClaimBatchBytes = len(tsoClaimBatchEnvelope) + 8 + 4 + MaxTSOClaimsPerBatch*tsoClaimRecordBytes
)

func (f *TSOStateMachine) applyClaimEnvelope(data []byte) (any, bool) {
	if bytes.HasPrefix(data, []byte(tsoClaimIntentEnvelope)) {
		return f.applyClaimIntents(data), true
	}
	if !bytes.HasPrefix(data, []byte(tsoClaimGrantEnvelope)) && !bytes.HasPrefix(data, []byte(tsoClaimBatchEnvelope)) {
		return nil, false
	}
	if f == nil {
		return haltErr(errors.WithStack(ErrTSOStateMachineInvalidEntry)), true
	}
	if bytes.HasPrefix(data, []byte(tsoClaimGrantEnvelope)) {
		return f.applyClaimGrant(data), true
	}
	return f.applyClaimBatch(data), true
}

func isTSOClaimPayload(data []byte) bool {
	if bytes.HasPrefix(data, []byte(tsoClaimIntentEnvelope)) {
		_, _, err := decodeTSOClaimIntents(data)
		return err == nil
	}
	if bytes.HasPrefix(data, []byte(tsoClaimGrantEnvelope)) {
		return len(data) == len(tsoClaimGrantEnvelope)+tsoClaimGrantBytes
	}
	if bytes.HasPrefix(data, []byte(tsoClaimBatchEnvelope)) {
		_, _, err := decodeTSOClaimBatch(data)
		return err == nil
	}
	return false
}

func marshalTSOClaimGrant(g TSOClaimGrant) ([]byte, error) {
	if !g.valid() {
		return nil, errors.WithStack(ErrTSOClaimInvalid)
	}
	var out bytes.Buffer
	out.WriteString(tsoClaimGrantEnvelope)
	if err := binary.Write(&out, binary.BigEndian, g); err != nil {
		return nil, errors.Wrap(err, "encode TSO claim grant")
	}
	return out.Bytes(), nil
}

func marshalTSOClaimBatch(term uint64, records []TSOClaimRecord) ([]byte, error) {
	if term == 0 || len(records) == 0 || len(records) > MaxTSOClaimsPerBatch {
		return nil, errors.WithStack(ErrTSOClaimInvalid)
	}
	var out bytes.Buffer
	out.WriteString(tsoClaimBatchEnvelope)
	if err := binary.Write(&out, binary.BigEndian, term); err != nil {
		return nil, errors.Wrap(err, "encode TSO claim term")
	}
	count := uint32(len(records)) // #nosec G115 -- bounded by MaxTSOClaimsPerBatch.
	if err := binary.Write(&out, binary.BigEndian, count); err != nil {
		return nil, errors.Wrap(err, "encode TSO claim count")
	}
	if err := binary.Write(&out, binary.BigEndian, records); err != nil {
		return nil, errors.Wrap(err, "encode TSO claim batch")
	}
	return out.Bytes(), nil
}

func (f *TSOStateMachine) applyClaimGrant(data []byte) any {
	if len(data) != len(tsoClaimGrantEnvelope)+tsoClaimGrantBytes {
		return haltErr(errors.Wrap(ErrTSOStateMachineInvalidEntry, "invalid claim grant length"))
	}
	var g TSOClaimGrant
	if err := binary.Read(bytes.NewReader(data[len(tsoClaimGrantEnvelope):]), binary.BigEndian, &g); err != nil {
		return haltErr(errors.Wrap(err, "decode TSO claim grant"))
	}
	if !g.valid() {
		return haltErr(errors.WithStack(ErrTSOStateMachineInvalidEntry))
	}
	if !f.PhaseDActive() || g.Base <= f.PhaseDFloor() {
		return errors.WithStack(ErrTSOPhaseDInactive)
	}
	if f.claims == nil {
		f.claims = newTSOClaimLedger()
	}
	if err := f.claims.addGrant(g, f.AllocationFloor()); err != nil {
		return err
	}
	f.applyAllocationFloor(g.end())
	return nil
}

func (l *tsoClaimLedger) addGrant(g TSOClaimGrant, floor uint64) error {
	if prior, exists := l.Grants[g.ID]; exists {
		if prior.Grant != g {
			return errors.WithStack(ErrTSOClaimConflict)
		}
		return nil
	}
	if g.Term < l.Term {
		return errors.WithStack(ErrTSOClaimClosed)
	}
	if g.Base <= floor {
		return errors.WithStack(ErrTSOClaimConflict)
	}
	if len(l.Grants) >= maxTSOClaimGrants {
		return errors.WithStack(ErrTSOClaimCapacity)
	}
	if g.Term > l.Term {
		for id, prior := range l.Grants {
			prior.Closed = true
			l.Grants[id] = prior
		}
		l.Term = g.Term
	}
	l.Grants[g.ID] = tsoRetainedGrant{Grant: g}
	return nil
}

func decodeTSOClaimBatch(data []byte) (uint64, []TSOClaimRecord, error) {
	const headerBytes = len(tsoClaimBatchEnvelope) + 8 + 4
	if len(data) < headerBytes || len(data) > maxTSOClaimBatchBytes {
		return 0, nil, errors.WithStack(ErrTSOStateMachineInvalidEntry)
	}
	r := bytes.NewReader(data[len(tsoClaimBatchEnvelope):])
	var term uint64
	var count uint32
	if err := binary.Read(r, binary.BigEndian, &term); err != nil {
		return 0, nil, errors.Wrap(err, "decode TSO claim term")
	}
	if err := binary.Read(r, binary.BigEndian, &count); err != nil {
		return 0, nil, errors.Wrap(err, "decode TSO claim count")
	}
	if term == 0 || count == 0 || count > MaxTSOClaimsPerBatch || r.Len() != int(count)*tsoClaimRecordBytes {
		return 0, nil, errors.WithStack(ErrTSOStateMachineInvalidEntry)
	}
	records := make([]TSOClaimRecord, int(count))
	if err := binary.Read(r, binary.BigEndian, records); err != nil {
		return 0, nil, errors.Wrap(err, "decode TSO claim records")
	}
	return term, records, nil
}

func (f *TSOStateMachine) applyClaimBatch(data []byte) any {
	term, records, err := decodeTSOClaimBatch(data)
	if err != nil {
		return haltErr(err)
	}
	if f.claims == nil || term != f.claims.Term {
		return errors.WithStack(ErrTSOClaimClosed)
	}
	return f.claims.addBatch(records)
}

func (l *tsoClaimLedger) addBatch(records []TSOClaimRecord) error {
	seen := newTSOClaimLedger()
	newClaims := 0
	for _, r := range records {
		if err := l.checkRecord(r, false); err != nil {
			return err
		}
		if _, duplicate := seen.Claims[r.ID]; duplicate {
			return errors.WithStack(ErrTSOClaimConflict)
		}
		if _, duplicate := seen.Slots[r.Timestamp]; duplicate {
			return errors.WithStack(ErrTSOClaimConflict)
		}
		if _, duplicate := seen.Operations[r.Binding.key()]; duplicate {
			return errors.WithStack(ErrTSOClaimConflict)
		}
		seen.insertRecord(r)
		if _, exists := l.Claims[r.ID]; !exists {
			newClaims++
		}
	}
	if len(l.Claims)+newClaims > maxTSOClaimRecords {
		return errors.WithStack(ErrTSOClaimCapacity)
	}
	for _, r := range records {
		l.insertRecord(r)
	}
	return nil
}

func (f *TSOStateMachine) lookupClaim(credential TSOClaimCredential, expected TSOClaimBinding) (TSOClaimProof, error) {
	if f == nil {
		return TSOClaimProof{}, errors.WithStack(ErrTSOClaimUnknown)
	}
	f.claimMu.RLock()
	defer f.claimMu.RUnlock()
	if f.claims == nil {
		return TSOClaimProof{}, errors.WithStack(ErrTSOClaimUnknown)
	}
	r, ok := f.claims.Claims[credential.ID]
	if !ok {
		return TSOClaimProof{}, errors.WithStack(ErrTSOClaimUnknown)
	}
	p := TSOClaimProof{Record: r, Credential: credential}
	phase := TSOClaimOnePhase
	if expected.Role != TSOClaimCommit {
		phase = TSOClaimRead
	}
	if !p.matches(expected, phase) {
		return TSOClaimProof{}, errors.WithStack(ErrTSOClaimInvalid)
	}
	return p, nil
}

func (f *TSOStateMachine) validateClaim(p TSOClaimProof, expected TSOClaimBinding, phase TSOClaimPhase) error {
	if !p.matches(expected, phase) {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	durable, err := f.lookupClaim(p.Credential, expected)
	if err != nil {
		return err
	}
	if durable.Record != p.Record {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	return nil
}
