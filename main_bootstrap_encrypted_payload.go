package main

import (
	"crypto/sha256"
	"slices"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
)

var errBootstrapPayloadProjection = errors.New("bootstrap payload encryption projection is absent or inconsistent")
var errBootstrapRotationRecipe = errors.New("bootstrap rotation payload does not match an authorized recipe")

// Published only by the local installed-state/controller owner. Generation
// must be fenced by A before keys/wrappers/recipes change, then checked again
// at enqueue/result. This image and its validation result grant no authority.
type bootstrapPayloadProjection struct {
	Group              uint64
	Generation         uint64
	RaftEnvelopeActive bool
	RaftKeyID          uint32
	RaftCipher         *encryption.Cipher
	rotationRecipes    []bootstrapRotationPayloadGrant
}

type bootstrapRotationPayloadGrant struct {
	group  uint64
	digest [sha256.Size]byte
}

// Recipe creation is local controller work, never a network-derived grant.
// The complete canonical plaintext includes the wrapped DEK and registration;
// KEK unwrap/apply-state checks remain in the existing controller/FSM path.
func newBootstrapRotationPayloadGrant(group uint64, payload []byte) (bootstrapRotationPayloadGrant, error) {
	if err := validateBootstrapRotationPayload(payload); err != nil {
		return bootstrapRotationPayloadGrant{}, err
	}
	return bootstrapRotationPayloadGrant{group: group, digest: sha256.Sum256(payload)}, nil
}

func newBootstrapPayloadProjection(group, generation uint64, cipher *encryption.Cipher, keyID uint32, active bool, recipes ...bootstrapRotationPayloadGrant) (bootstrapPayloadProjection, error) {
	projection := bootstrapPayloadProjection{Group: group, Generation: generation, RaftEnvelopeActive: active,
		RaftCipher: cipher, RaftKeyID: keyID, rotationRecipes: slices.Clone(recipes)}
	if err := projection.validate(group); err != nil {
		return bootstrapPayloadProjection{}, err
	}
	for _, recipe := range projection.rotationRecipes {
		if recipe.group != group || recipe.digest == ([sha256.Size]byte{}) {
			return bootstrapPayloadProjection{}, errors.WithStack(errBootstrapRotationRecipe)
		}
	}
	return projection, nil
}

func (p bootstrapPayloadProjection) validate(group uint64) error {
	if p.Generation == 0 || p.Group != group {
		return errors.WithStack(errBootstrapPayloadProjection)
	}
	if p.RaftEnvelopeActive && (p.RaftCipher == nil || p.RaftKeyID == encryption.ReservedKeyID) {
		return errors.WithStack(errBootstrapPayloadProjection)
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
func validateBootstrapFinalPayload(cluster raftbootstrap.UUID, kind raftadmission.Kind, group uint64, final []byte, projection bootstrapPayloadProjection) (bootstrapPayloadValidation, error) {
	var zero bootstrapPayloadValidation
	if err := projection.validate(group); err != nil {
		return zero, errors.Join(errBootstrapAdmissionPayload, err)
	}
	if len(final) == 0 || len(final) > bootstrapPayloadMaximumSize+encryption.EnvelopeOverhead {
		return zero, errors.WithStack(errBootstrapAdmissionPayload)
	}
	final = slices.Clone(final)
	plain, err := projection.plaintext(final)
	if err != nil {
		return zero, errors.Join(errBootstrapAdmissionPayload, err)
	}
	if err := validateBootstrapAdmissionPayload(kind, plain); err != nil {
		return zero, err
	}
	if err := validateBootstrapControlScope(cluster, group, plain); err != nil {
		return zero, err
	}
	if kind == raftadmission.Rotation && !projection.authorizesRotation(plain) {
		return zero, errors.Join(errBootstrapAdmissionPayload, errBootstrapRotationRecipe)
	}
	return bootstrapPayloadValidation{FinalDigest: sha256.Sum256(final), PlaintextDigest: sha256.Sum256(plain), ProjectionGeneration: projection.Generation}, nil
}

func (p bootstrapPayloadProjection) plaintext(final []byte) ([]byte, error) {
	if !p.RaftEnvelopeActive {
		return final, nil
	}
	envelope, err := encryption.DecodeEnvelope(final)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if envelope.KeyID != p.RaftKeyID {
		return nil, errors.WithStack(errBootstrapPayloadProjection)
	}
	plain, err := encryption.UnwrapRaftPayload(p.RaftCipher, final)
	return plain, errors.WithStack(err)
}

func (p bootstrapPayloadProjection) authorizesRotation(plain []byte) bool {
	grant := bootstrapRotationPayloadGrant{group: p.Group, digest: sha256.Sum256(plain)}
	return slices.Contains(p.rotationRecipes, grant)
}
