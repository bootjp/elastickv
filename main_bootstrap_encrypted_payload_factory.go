package main

import (
	"slices"

	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
)

type bootstrapPayloadProjectionLoader func(group uint64) (bootstrapPayloadProjection, error)
type bootstrapFinalPayloadValidator func(raftadmission.Kind, uint64, []byte) (bootstrapPayloadValidation, error)

// The loader is a domain-local settled projection, never a process-global
// decoder. Its fence must fail during installed-state changes. A still owns
// the generation check before/after this callback and at final native enqueue;
// neither this callback nor its double observation can replace that boundary.
func newBootstrapFinalPayloadValidator(cluster raftbootstrap.UUID, load bootstrapPayloadProjectionLoader) (bootstrapFinalPayloadValidator, error) {
	if cluster == (raftbootstrap.UUID{}) || load == nil {
		return nil, errors.WithStack(errBootstrapPayloadProjection)
	}
	return func(kind raftadmission.Kind, group uint64, final []byte) (bootstrapPayloadValidation, error) {
		var zero bootstrapPayloadValidation
		before, err := load(group)
		if err != nil {
			return zero, errors.Join(errBootstrapAdmissionPayload, err)
		}
		if before.RaftCipher == nil {
			return zero, errors.Join(errBootstrapAdmissionPayload, errBootstrapPayloadProjection)
		}
		validated, err := validateBootstrapFinalPayload(cluster, kind, group, final, before)
		if err != nil {
			return zero, err
		}
		after, err := load(group)
		if err != nil {
			return zero, errors.Join(errBootstrapAdmissionPayload, err)
		}
		if !before.samePublication(after) {
			return zero, errors.Join(errBootstrapAdmissionPayload, raftadmission.ErrStale)
		}
		return validated, nil
	}, nil
}

// Config.ValidatePayload compatibility adapter. The arbiter's existing
// BindProposal must continue hashing final ciphertext, not PlaintextDigest.
// Call the proof-returning factory above when binding an exact request
// declaration or checking the projection generation in an extended A API.
func newBootstrapEncryptedPayloadValidator(cluster raftbootstrap.UUID, load bootstrapPayloadProjectionLoader) (func(raftadmission.Kind, uint64, []byte) error, error) {
	validate, err := newBootstrapFinalPayloadValidator(cluster, load)
	if err != nil {
		return nil, err
	}
	return func(kind raftadmission.Kind, group uint64, final []byte) error {
		_, err := validate(kind, group, final)
		return err
	}, nil
}

func (p bootstrapPayloadProjection) samePublication(other bootstrapPayloadProjection) bool {
	return p.Group == other.Group && p.Generation == other.Generation && p.RaftEnvelopeActive == other.RaftEnvelopeActive &&
		p.RaftKeyID == other.RaftKeyID && p.RaftCipher == other.RaftCipher && slices.Equal(p.rotationRecipes, other.rotationRecipes)
}
