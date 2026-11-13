package clusterbootstrap

import (
	"crypto/sha256"
	"slices"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
)

var ErrBootstrapPayloadProjection = errors.New("bootstrap payload encryption projection is absent or inconsistent")
var ErrBootstrapRotationRecipe = errors.New("bootstrap rotation payload does not match an authorized recipe")

// Published only by the local installed-state/controller owner. Generation
// must be fenced by A before keys/wrappers/recipes change, then checked again
// at enqueue/result. This image and its validation result grant no authority.
type BootstrapPayloadProjection struct {
	Group              uint64
	Generation         uint64
	RaftEnvelopeActive bool
	RaftKeyID          uint32
	RaftCipher         *encryption.Cipher
	RotationRecipes    []BootstrapRotationPayloadGrant
}

type BootstrapRotationPayloadGrant struct {
	Group  uint64
	digest [sha256.Size]byte
}

// Recipe creation is local controller work, never a network-derived grant.
// The complete canonical plaintext includes the wrapped DEK and registration;
// KEK unwrap/apply-state checks remain in the existing controller/FSM path.
func NewBootstrapRotationPayloadGrant(group uint64, payload []byte) (BootstrapRotationPayloadGrant, error) {
	if err := ValidateBootstrapRotationPayload(payload); err != nil {
		return BootstrapRotationPayloadGrant{}, err
	}
	return BootstrapRotationPayloadGrant{Group: group, digest: sha256.Sum256(payload)}, nil
}

func NewBootstrapPayloadProjection(group, generation uint64, cipher *encryption.Cipher, keyID uint32, active bool, recipes ...BootstrapRotationPayloadGrant) (BootstrapPayloadProjection, error) {
	projection := BootstrapPayloadProjection{Group: group, Generation: generation, RaftEnvelopeActive: active,
		RaftCipher: cipher, RaftKeyID: keyID, RotationRecipes: slices.Clone(recipes)}
	if err := projection.validate(group); err != nil {
		return BootstrapPayloadProjection{}, err
	}
	for _, recipe := range projection.RotationRecipes {
		if recipe.Group != group || recipe.digest == ([sha256.Size]byte{}) {
			return BootstrapPayloadProjection{}, errors.WithStack(ErrBootstrapRotationRecipe)
		}
	}
	return projection, nil
}

func (p BootstrapPayloadProjection) validate(group uint64) error {
	if p.Generation == 0 || p.Group != group {
		return errors.WithStack(ErrBootstrapPayloadProjection)
	}
	if p.RaftEnvelopeActive && (p.RaftCipher == nil || p.RaftKeyID == encryption.ReservedKeyID) {
		return errors.WithStack(ErrBootstrapPayloadProjection)
	}
	return nil
}

type bootstrapPayloadValidation struct {
	FinalDigest          [sha256.Size]byte
	PlaintextDigest      [sha256.Size]byte
	ProjectionGeneration uint64
}

// Validate OUTSIDE A against a pinned installed projection. Seal the ACTUAL
// final bytes, not the decoded plaintext. The caller must bookend this work
// with A's generation checks and use FinalDigest at the terminal native send.
// Envelope mode is explicit: its version byte overlaps the plaintext batch
// tag, so neither prefix guessing nor plaintext fallback on AEAD error is safe.
func ValidateBootstrapFinalPayload(cluster raftbootstrap.UUID, kind raftadmission.Kind, group uint64, final []byte, projection BootstrapPayloadProjection) (bootstrapPayloadValidation, error) {
	var zero bootstrapPayloadValidation
	if err := projection.validate(group); err != nil {
		return zero, errors.Join(ErrBootstrapAdmissionPayload, err)
	}
	if len(final) == 0 || len(final) > bootstrapPayloadMaximumSize+encryption.EnvelopeOverhead {
		return zero, errors.WithStack(ErrBootstrapAdmissionPayload)
	}
	final = slices.Clone(final)
	plain, err := projection.Plaintext(final)
	if err != nil {
		return zero, errors.Join(ErrBootstrapAdmissionPayload, err)
	}
	if err := ValidateBootstrapAdmissionPayload(kind, plain); err != nil {
		return zero, err
	}
	if err := validateBootstrapControlScope(cluster, group, plain); err != nil {
		return zero, err
	}
	if kind == raftadmission.Rotation && !projection.authorizesRotation(plain) {
		return zero, errors.Join(ErrBootstrapAdmissionPayload, ErrBootstrapRotationRecipe)
	}
	return bootstrapPayloadValidation{FinalDigest: sha256.Sum256(final), PlaintextDigest: sha256.Sum256(plain), ProjectionGeneration: projection.Generation}, nil
}

func (p BootstrapPayloadProjection) Plaintext(final []byte) ([]byte, error) {
	if !p.RaftEnvelopeActive {
		return final, nil
	}
	envelope, err := encryption.DecodeEnvelope(final)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if envelope.KeyID != p.RaftKeyID {
		return nil, errors.WithStack(ErrBootstrapPayloadProjection)
	}
	plain, err := encryption.UnwrapRaftPayload(p.RaftCipher, final)
	return plain, errors.WithStack(err)
}

func (p BootstrapPayloadProjection) authorizesRotation(plain []byte) bool {
	grant := BootstrapRotationPayloadGrant{Group: p.Group, digest: sha256.Sum256(plain)}
	return slices.Contains(p.RotationRecipes, grant)
}
