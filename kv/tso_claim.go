package kv

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"

	"github.com/cockroachdb/errors"
)

const (
	MaxTSOClaimsPerBatch = 64
	maxTSOClaimRecords   = 65536
	maxTSOClaimGrants    = 4096
)

var (
	ErrTSOClaimInvalid  = errors.New("tso: invalid concrete slot claim")
	ErrTSOClaimConflict = errors.New("tso: claim binding conflicts with durable state")
	ErrTSOClaimClosed   = errors.New("tso: claim grant is closed")
	ErrTSOClaimCapacity = errors.New("tso: claim retention capacity reached")
	ErrTSOClaimUnknown  = errors.New("tso: claim outcome is unknown; reconcile the same claim ID")
)

type TSOClaimID [16]byte
type TSOClaimOwner [32]byte
type TSOOperationID [16]byte
type TSOIntentDigest [32]byte

type TSOClaimRole uint8

const (
	TSOClaimStart TSOClaimRole = iota + 1
	TSOClaimCommit
	TSOClaimObservation
)

type TSOClaimPhase uint8

const (
	TSOClaimRead TSOClaimPhase = iota + 1
	TSOClaimPrepare
	TSOClaimOnePhase
	TSOClaimPrepareEvidence
	TSOClaimPrimary
	TSOClaimSecondary
	TSOClaimResolution
)

// TSOClaimBinding is data, not authenticated identity. An ingress must derive
// Owner from its authenticated principal and Intent from the actual operation,
// not copy either from an untrusted proof. No production ingress uses this API
// until the transport, decoder and creation-fence gates are integrated.
type TSOClaimBinding struct {
	Owner     TSOClaimOwner
	Operation TSOOperationID
	Intent    TSOIntentDigest
	Role      TSOClaimRole
}

// TSOClaimCredential must be durably retained by the caller BEFORE issuance.
// An unknown response is retried using this exact credential and binding.
// The secret never enters the replicated log or a snapshot.
type TSOClaimCredential struct {
	ID     TSOClaimID
	Secret [32]byte
}

func NewTSOClaimCredential() (TSOClaimCredential, error) {
	var c TSOClaimCredential
	if _, err := rand.Read(c.ID[:]); err != nil {
		return c, errors.Wrap(err, "create TSO claim ID")
	}
	if _, err := rand.Read(c.Secret[:]); err != nil {
		return c, errors.Wrap(err, "create TSO claim credential")
	}
	return c, nil
}

type TSOClaimGrant struct {
	ID    TSOClaimID
	Owner TSOClaimOwner
	Term  uint64
	Base  uint64
	Count uint32
}

// TSOClaimRecord is the immutable, committed binding of ONE concrete slot.
// Neither a reservation nor a per-owner watermark is a claim.
type TSOClaimRecord struct {
	ID         TSOClaimID
	Grant      TSOClaimID
	Timestamp  uint64
	Binding    TSOClaimBinding
	SecretHash [32]byte
}

type TSOClaimProof struct {
	Record     TSOClaimRecord
	Credential TSOClaimCredential
}

type tsoClaimOperationKey struct {
	owner     TSOClaimOwner
	operation TSOOperationID
	role      TSOClaimRole
}

func (b TSOClaimBinding) key() tsoClaimOperationKey {
	return tsoClaimOperationKey{owner: b.Owner, operation: b.Operation, role: b.Role}
}

func (b TSOClaimBinding) valid() bool {
	return b.Owner != (TSOClaimOwner{}) && b.Operation != (TSOOperationID{}) &&
		b.Intent != (TSOIntentDigest{}) && validTSOClaimRole(b.Role)
}

func validTSOClaimRole(role TSOClaimRole) bool {
	return role >= TSOClaimStart && role <= TSOClaimObservation
}

func (b TSOClaimBinding) allows(phase TSOClaimPhase) bool {
	switch phase {
	case TSOClaimRead:
		return b.Role == TSOClaimStart || b.Role == TSOClaimObservation
	case TSOClaimPrepare:
		return b.Role == TSOClaimStart
	case TSOClaimOnePhase, TSOClaimPrepareEvidence, TSOClaimPrimary, TSOClaimSecondary, TSOClaimResolution:
		return b.Role == TSOClaimCommit
	default:
		return false
	}
}

func (c TSOClaimCredential) valid() bool {
	return c.ID != (TSOClaimID{}) && c.Secret != ([32]byte{})
}

func claimCredentialHash(c TSOClaimCredential) [32]byte {
	return sha256.Sum256(c.Secret[:])
}

func (p TSOClaimProof) matches(expected TSOClaimBinding, phase TSOClaimPhase) bool {
	hash := claimCredentialHash(p.Credential)
	return p.Credential.valid() && p.Credential.ID == p.Record.ID &&
		p.Record.Binding == expected && expected.valid() && expected.allows(phase) &&
		subtle.ConstantTimeCompare(hash[:], p.Record.SecretHash[:]) == 1
}

func (g TSOClaimGrant) valid() bool {
	return g.ID != (TSOClaimID{}) && g.Owner != (TSOClaimOwner{}) &&
		g.Term != 0 && g.Base != 0 && g.Count != 0 && g.Count <= MaxTSOBatchSize &&
		g.Base <= ^uint64(0)-uint64(g.Count)+1
}

func (g TSOClaimGrant) end() uint64 {
	return g.Base + uint64(g.Count) - 1
}

type tsoRetainedGrant struct {
	Grant  TSOClaimGrant
	Closed bool
}

type tsoClaimLedger struct {
	Term          uint64
	RetiredBefore uint64
	Grants        map[TSOClaimID]tsoRetainedGrant
	Claims        map[TSOClaimID]TSOClaimRecord
	Slots         map[uint64]TSOClaimID
	Operations    map[tsoClaimOperationKey]TSOClaimID
	Cursors       map[TSOClaimID]uint64
}

func newTSOClaimLedger() *tsoClaimLedger {
	return &tsoClaimLedger{
		Grants:     make(map[TSOClaimID]tsoRetainedGrant),
		Claims:     make(map[TSOClaimID]TSOClaimRecord),
		Slots:      make(map[uint64]TSOClaimID),
		Operations: make(map[tsoClaimOperationKey]TSOClaimID),
		Cursors:    make(map[TSOClaimID]uint64),
	}
}

func (l *tsoClaimLedger) clone() *tsoClaimLedger {
	if l == nil {
		return nil
	}
	c := newTSOClaimLedger()
	c.Term, c.RetiredBefore = l.Term, l.RetiredBefore
	for id, g := range l.Grants {
		c.Grants[id] = g
	}
	for id, r := range l.Claims {
		c.Claims[id] = r
		c.Slots[r.Timestamp] = id
		c.Operations[r.Binding.key()] = id
		c.Cursors[r.Grant] = max(c.Cursors[r.Grant], r.Timestamp)
	}
	return c
}

func (l *tsoClaimLedger) checkRecord(r TSOClaimRecord, restoring bool) error {
	g, err := l.recordGrant(r)
	if err != nil {
		return err
	}
	if prior, exists := l.Claims[r.ID]; exists {
		if prior != r {
			return errors.WithStack(ErrTSOClaimConflict)
		}
		return nil
	}
	return l.checkNewRecord(r, g, restoring)
}

func (l *tsoClaimLedger) recordGrant(r TSOClaimRecord) (tsoRetainedGrant, error) {
	if r.ID == (TSOClaimID{}) || !r.Binding.valid() || r.SecretHash == ([32]byte{}) {
		return tsoRetainedGrant{}, errors.WithStack(ErrTSOClaimInvalid)
	}
	g, ok := l.Grants[r.Grant]
	if !ok || r.Binding.Owner != g.Grant.Owner || r.Timestamp < g.Grant.Base || r.Timestamp > g.Grant.end() {
		return tsoRetainedGrant{}, errors.WithStack(ErrTSOClaimInvalid)
	}
	return g, nil
}

func (l *tsoClaimLedger) checkNewRecord(r TSOClaimRecord, g tsoRetainedGrant, restoring bool) error {
	if !restoring && (g.Closed || g.Grant.Term != l.Term) {
		return errors.WithStack(ErrTSOClaimClosed)
	}
	if _, exists := l.Slots[r.Timestamp]; exists {
		return errors.WithStack(ErrTSOClaimConflict)
	}
	if _, exists := l.Operations[r.Binding.key()]; exists {
		return errors.WithStack(ErrTSOClaimConflict)
	}
	if len(l.Claims) >= maxTSOClaimRecords {
		return errors.WithStack(ErrTSOClaimCapacity)
	}
	return nil
}

func (l *tsoClaimLedger) insertRecord(r TSOClaimRecord) {
	l.Claims[r.ID] = r
	l.Slots[r.Timestamp] = r.ID
	l.Operations[r.Binding.key()] = r.ID
	l.Cursors[r.Grant] = max(l.Cursors[r.Grant], r.Timestamp)
}
