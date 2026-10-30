package kv

import (
	"bytes"
	"encoding/binary"

	"github.com/cockroachdb/errors"
)

const (
	tsoClaimIntentEnvelope    = "\x07TSOI\x01"
	tsoSelectedIntentBytes    = 253
	tsoIntentBatchHeaderBytes = len(tsoClaimIntentEnvelope) + 8 + 4
)

var ErrTSOClaimGrantExhausted = errors.New("tso: concrete claim grant exhausted")

type tsoSelectedClaimIntent struct {
	Grant  TSOClaimID
	Intent TSOSignedClaimIntent
}

func marshalTSOClaimIntents(term uint64, intents []tsoSelectedClaimIntent) ([]byte, error) {
	if term == 0 || len(intents) == 0 || len(intents) > MaxTSOClaimsPerBatch {
		return nil, errors.WithStack(ErrTSOClaimInvalid)
	}
	var out bytes.Buffer
	out.WriteString(tsoClaimIntentEnvelope)
	if err := binary.Write(&out, binary.BigEndian, term); err != nil {
		return nil, errors.WithStack(err)
	}
	count := uint32(len(intents)) // #nosec G115 -- bounded by MaxTSOClaimsPerBatch.
	if err := binary.Write(&out, binary.BigEndian, count); err != nil {
		return nil, errors.WithStack(err)
	}
	if err := binary.Write(&out, binary.BigEndian, intents); err != nil {
		return nil, errors.WithStack(err)
	}
	return out.Bytes(), nil
}

func decodeTSOClaimIntents(data []byte) (uint64, []tsoSelectedClaimIntent, error) {
	if len(data) < tsoIntentBatchHeaderBytes || len(data) > tsoIntentBatchHeaderBytes+MaxTSOClaimsPerBatch*tsoSelectedIntentBytes {
		return 0, nil, errors.WithStack(ErrTSOStateMachineInvalidEntry)
	}
	r := bytes.NewReader(data[len(tsoClaimIntentEnvelope):])
	var term uint64
	var count uint32
	if err := binary.Read(r, binary.BigEndian, &term); err != nil {
		return 0, nil, errors.WithStack(err)
	}
	if err := binary.Read(r, binary.BigEndian, &count); err != nil {
		return 0, nil, errors.WithStack(err)
	}
	if term == 0 || count == 0 || count > MaxTSOClaimsPerBatch || r.Len() != int(count)*tsoSelectedIntentBytes {
		return 0, nil, errors.WithStack(ErrTSOStateMachineInvalidEntry)
	}
	intents := make([]tsoSelectedClaimIntent, int(count))
	if err := binary.Read(r, binary.BigEndian, intents); err != nil {
		return 0, nil, errors.WithStack(err)
	}
	return term, intents, nil
}

func (f *TSOStateMachine) applyClaimIntents(data []byte) any {
	term, intents, err := decodeTSOClaimIntents(data)
	if err != nil {
		return haltErr(err)
	}
	if f == nil || f.claims == nil || term != f.claims.Term {
		return errors.WithStack(ErrTSOClaimClosed)
	}
	staged := newTSOClaimLedger()
	records := make([]TSOClaimRecord, 0, len(intents))
	for _, selected := range intents {
		s := selected.Intent
		if err := s.verify(); err != nil {
			return err
		}
		if _, duplicate := staged.Claims[s.ID]; duplicate {
			return errors.WithStack(ErrTSOClaimConflict)
		}
		r, err := f.claims.selectClaim(selected, staged)
		if err != nil {
			return err
		}
		staged.insertRecord(r)
		records = append(records, r)
	}
	return f.claims.addBatch(records)
}

func (l *tsoClaimLedger) selectClaim(selected tsoSelectedClaimIntent, staged *tsoClaimLedger) (TSOClaimRecord, error) {
	s := selected.Intent
	if prior, exists := l.Claims[s.ID]; exists {
		if !s.matchesRecord(prior) {
			return TSOClaimRecord{}, errors.WithStack(ErrTSOClaimConflict)
		}
		return prior, nil
	}
	g, exists := l.Grants[selected.Grant]
	if !exists || g.Closed || g.Grant.Term != l.Term || g.Grant.Owner != s.Binding.Owner {
		return TSOClaimRecord{}, errors.WithStack(ErrTSOClaimClosed)
	}
	return l.selectClaimOffset(selected, staged, g.Grant)
}

func (l *tsoClaimLedger) selectClaimOffset(selected tsoSelectedClaimIntent, staged *tsoClaimLedger, grant TSOClaimGrant) (TSOClaimRecord, error) {
	s := selected.Intent
	last := max(l.Cursors[selected.Grant], staged.Cursors[selected.Grant], s.Minimum)
	if last == ^uint64(0) {
		return TSOClaimRecord{}, errors.WithStack(ErrTSOClaimGrantExhausted)
	}
	timestamp := max(grant.Base, last+1)
	if timestamp > grant.end() {
		return TSOClaimRecord{}, errors.WithStack(ErrTSOClaimGrantExhausted)
	}
	r := TSOClaimRecord{ID: s.ID, Grant: selected.Grant, Timestamp: timestamp, Binding: s.Binding, SecretHash: s.SecretHash}
	if err := l.checkRecord(r, false); err != nil {
		return TSOClaimRecord{}, err
	}
	if _, duplicate := staged.Operations[r.Binding.key()]; duplicate {
		return TSOClaimRecord{}, errors.WithStack(ErrTSOClaimConflict)
	}
	return r, nil
}

func (f *TSOStateMachine) recordForSignedClaim(s TSOSignedClaimIntent) (TSOClaimRecord, error) {
	f.claimMu.RLock()
	defer f.claimMu.RUnlock()
	if f.claims == nil {
		return TSOClaimRecord{}, errors.WithStack(ErrTSOClaimUnknown)
	}
	r, exists := f.claims.Claims[s.ID]
	if !exists {
		return TSOClaimRecord{}, errors.WithStack(ErrTSOClaimUnknown)
	}
	if !s.matchesRecord(r) {
		return TSOClaimRecord{}, errors.WithStack(ErrTSOClaimConflict)
	}
	return r, nil
}
