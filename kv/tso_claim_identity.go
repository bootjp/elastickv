package kv

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"

	"github.com/cockroachdb/errors"
)

const tsoSignedClaimDomain = "elastickv/timestamp-claim/v1"

// The seed must come from durable node identity storage. It is never generated
// on demand by the allocator: restart must derive the SAME attempt credential.
type TSOClaimIdentity struct {
	seed [ed25519.SeedSize]byte
}

func NewTSOClaimIdentity(seed []byte) (*TSOClaimIdentity, error) {
	if len(seed) != ed25519.SeedSize || bytes.Equal(seed, make([]byte, ed25519.SeedSize)) {
		return nil, errors.WithStack(ErrTSOClaimInvalid)
	}
	i := &TSOClaimIdentity{}
	copy(i.seed[:], seed)
	return i, nil
}

func (i *TSOClaimIdentity) publicKey() [ed25519.PublicKeySize]byte {
	var key [ed25519.PublicKeySize]byte
	private := ed25519.NewKeyFromSeed(i.seed[:])
	copy(key[:], private[ed25519.SeedSize:])
	return key
}

func (i *TSOClaimIdentity) PublicKey() [ed25519.PublicKeySize]byte { return i.publicKey() }

func (i *TSOClaimIdentity) owner() TSOClaimOwner {
	key := i.publicKey()
	return TSOClaimOwner(sha256.Sum256(key[:]))
}

type TSOSignedClaimIntent struct {
	Binding    TSOClaimBinding
	ID         TSOClaimID
	SecretHash [32]byte
	Minimum    uint64
	GrantSize  uint32
	PublicKey  [ed25519.PublicKeySize]byte
	Signature  [ed25519.SignatureSize]byte
}

// Authentication/rolling-cutover ownership is external to this package. The
// implementation must authorize this signing key using independently verified
// peer identity and the active decoder/creation-fence epoch, not a request flag.
// The allocator calls it before proposing; FSM replay never calls it.
type TSOClaimAuthority interface {
	AuthorizeTimestampClaim(context.Context, [ed25519.PublicKeySize]byte) error
	AuthorizeTimestampClaimLookup(context.Context) error
}

type TSOClaimIssuer interface {
	ClaimTimestamp(context.Context, TSOSignedClaimIntent) (TSOClaimRecord, error)
	LookupTimestampClaim(context.Context, TSOSignedClaimIntent) (TSOClaimRecord, error)
}

func (i *TSOClaimIdentity) credential(binding TSOClaimBinding) TSOClaimCredential {
	idMAC := hmac.New(sha256.New, i.seed[:])
	idMAC.Write([]byte(tsoSignedClaimDomain + "/id"))
	idMAC.Write(binding.Operation[:])
	idMAC.Write([]byte{byte(binding.Role)})
	var c TSOClaimCredential
	copy(c.ID[:], idMAC.Sum(nil))
	secretMAC := hmac.New(sha256.New, i.seed[:])
	secretMAC.Write([]byte(tsoSignedClaimDomain + "/credential"))
	secretMAC.Write(c.ID[:])
	secretMAC.Write(binding.Intent[:])
	copy(c.Secret[:], secretMAC.Sum(nil))
	return c
}

func (i *TSOClaimIdentity) signClaim(binding TSOClaimBinding, minimum uint64, grantSize int) (TSOSignedClaimIntent, TSOClaimCredential, error) {
	if i == nil || !binding.valid() || binding.Owner != i.owner() || grantSize <= 0 || grantSize > MaxTSOBatchSize {
		return TSOSignedClaimIntent{}, TSOClaimCredential{}, errors.WithStack(ErrTSOClaimInvalid)
	}
	c := i.credential(binding)
	s := TSOSignedClaimIntent{
		Binding: binding, ID: c.ID, SecretHash: claimCredentialHash(c), Minimum: minimum,
		GrantSize: uint32(grantSize), PublicKey: i.publicKey(), // #nosec G115 -- grantSize bounded above.
	}
	copy(s.Signature[:], ed25519.Sign(ed25519.NewKeyFromSeed(i.seed[:]), s.signingBytes()))
	return s, c, nil
}

func (s TSOSignedClaimIntent) signingBytes() []byte {
	s.Signature = [ed25519.SignatureSize]byte{}
	var out bytes.Buffer
	out.WriteString(tsoSignedClaimDomain)
	// Fixed-width fields only; bytes.Buffer cannot fail these writes.
	_ = binary.Write(&out, binary.BigEndian, s)
	return out.Bytes()
}

func (s TSOSignedClaimIntent) verify() error {
	if !s.Binding.valid() || s.ID == (TSOClaimID{}) || s.SecretHash == ([32]byte{}) ||
		s.GrantSize == 0 || s.GrantSize > MaxTSOBatchSize || s.Minimum == ^uint64(0) {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	if s.Binding.Owner != TSOClaimOwner(sha256.Sum256(s.PublicKey[:])) ||
		!ed25519.Verify(s.PublicKey[:], s.signingBytes(), s.Signature[:]) {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	return nil
}

func (s TSOSignedClaimIntent) matchesRecord(r TSOClaimRecord) bool {
	return r.ID == s.ID && r.Binding == s.Binding && r.SecretHash == s.SecretHash && r.Timestamp > s.Minimum
}
