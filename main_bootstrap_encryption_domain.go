package main

import (
	"crypto/sha256"
	"slices"

	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

// Protected by the enclosing domain mutex. The serial supervisor retains only
// the current immutable recipe commitment, never a union or unbounded registry.
type bootstrapEncryptionDomain struct {
	wiring       encryptionWriteWiring
	nodeID       uint64
	defaultGroup uint64
	allowed      map[uint64]bool
	groups       map[uint64]*kv.ShardGroup
	recipe       raftbootstrap.Digest
	grants       []bootstrapRotationPayloadGrant
}

func newBootstrapEncryptionDomain(groups []noderuntime.GroupSpec, request noderuntime.BootstrapEncryptionRequest, optional []encryptionWriteWiring) (*bootstrapEncryptionDomain, error) {
	if len(optional) > 1 {
		return nil, errors.Wrap(errBootstrapEncryptionUnsupported, "multiple encryption wirings")
	}
	var w encryptionWriteWiring
	if len(optional) == 1 {
		w = optional[0]
	}
	if !request.Requested() && !w.encryptionConfigured() {
		return nil, nil
	}
	if err := validateBootstrapDomainEncryptionWiring(w, groups); err != nil {
		return nil, err
	}
	allowed := make(map[uint64]bool, len(groups))
	for _, group := range groups {
		allowed[group.ID] = true
	}
	return &bootstrapEncryptionDomain{wiring: w, nodeID: w.raftEnvelope.fullNodeID, defaultGroup: noderuntime.DefaultGroupID(groups), allowed: allowed,
		groups: make(map[uint64]*kv.ShardGroup)}, nil
}

// Call under startup exclusivity after SG construction and before any async
// registration/proposals. Reattaching the same group is a no-op, not a new wrap.
func (d *bootstrapDomain) attachEncryptionGroup(id uint64, group *kv.ShardGroup) error {
	if d.encryption == nil {
		return nil
	}
	if err := validateBootstrapEncryptionGroup(group); err != nil {
		return err
	}
	d.mu.Lock()
	enc := d.encryption
	previous, allowed := enc.groups[id], enc.allowed[id]
	d.mu.Unlock()
	if !allowed {
		return errors.WithStack(errBootstrapPayloadProjection)
	}
	if previous != nil {
		if previous == group {
			return nil
		}
		return errors.Wrap(errBootstrapEncryptionUnsupported, "duplicate encryption group")
	}
	end := d.admission.BeginEncryptionMutation()
	defer end()
	r := enc.wiring.raftEnvelope
	r.mu.Lock()
	attached := r.groups[id]
	r.mu.Unlock()
	if attached != nil && attached != group {
		return errors.Wrap(errBootstrapEncryptionUnsupported, "foreign raft wrapper group")
	}
	if attached == nil {
		r.attachGroup(id, group)
	}
	d.mu.Lock()
	enc.groups[id] = group
	d.mu.Unlock()
	return nil
}

func validateBootstrapEncryptionGroup(group *kv.ShardGroup) error {
	if group == nil || group.Store == nil || group.Engine == nil {
		return errors.Wrap(errBootstrapEncryptionUnsupported, "missing actual encryption group")
	}
	if group.Proposer() == group.Engine {
		return errors.Wrap(errBootstrapEncryptionUnsupported, "unwrapped encryption group proposer")
	}
	return nil
}

func (d *bootstrapDomain) authorizeRotationRecipe(recipe *bootstrapRotationRecipe) error {
	if d.encryption == nil || recipe == nil {
		return errors.WithStack(errBootstrapRotationRecipe)
	}
	enc := d.encryption
	if err := enc.validateRecipeIdentity(recipe); err != nil {
		return err
	}
	requests := recipe.Requests()
	grants, err := bootstrapRotationPayloadGrants(enc.defaultGroup, requests...)
	if err != nil {
		return err
	}
	copyRecipe := *recipe
	copyRecipe.requests = requests
	if err := copyRecipe.hash(); err != nil {
		return err
	}
	digest := raftbootstrap.Digest(recipe.Digest())
	if digest == (raftbootstrap.Digest{}) || copyRecipe.Digest() != recipe.Digest() {
		return errors.WithStack(errBootstrapRotationRecipe)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if enc.recipe == digest {
		if !slices.Equal(enc.grants, grants) {
			return errors.WithStack(errBootstrapRotationRecipe)
		}
		return nil
	}
	enc.recipe = digest
	enc.grants = slices.Clone(grants)
	return nil
}

func (enc *bootstrapEncryptionDomain) validateRecipeIdentity(recipe *bootstrapRotationRecipe) error {
	if recipe.config.NodeID != enc.nodeID || recipe.config.StorageEpoch != enc.wiring.epoch || recipe.config.RaftEpoch != enc.wiring.raftEpoch {
		return errors.WithStack(errBootstrapRotationRecipe)
	}
	for _, req := range recipe.requests {
		if req == nil || req.ProposerNodeId != enc.nodeID {
			return errors.WithStack(errBootstrapRotationRecipe)
		}
		epoch := enc.wiring.epoch
		if req.Purpose == pb.RotateDEKRequest_PURPOSE_RAFT {
			epoch = enc.wiring.raftEpoch
		}
		if req.ProposerLocalEpoch != uint32(epoch) {
			return errors.WithStack(errBootstrapRotationRecipe)
		}
	}
	return nil
}

func (d *bootstrapDomain) inspectPayload(kind raftadmission.Kind, group uint64, final []byte, recipe raftbootstrap.Digest) (raftadmission.PayloadInspection, error) {
	var zero raftadmission.PayloadInspection
	if d.encryption == nil {
		if err := bootstrapPayloadValidator(d.cluster)(kind, group, final); err != nil {
			return zero, err
		}
		return raftadmission.PayloadInspection{PlaintextDigest: sha256.Sum256(final)}, nil
	}
	if kind == raftadmission.Rotation && recipe == (raftbootstrap.Digest{}) {
		return zero, errors.WithStack(errBootstrapRotationRecipe)
	}
	validate, err := newBootstrapFinalPayloadValidator(d.cluster, func(id uint64) (bootstrapPayloadProjection, error) {
		return d.loadEncryptionPayloadProjection(id, recipe)
	})
	if err != nil {
		return zero, err
	}
	proof, err := validate(kind, group, final)
	if err != nil {
		return zero, err
	}
	return raftadmission.PayloadInspection{PlaintextDigest: proof.PlaintextDigest, ProjectionGeneration: proof.ProjectionGeneration}, nil
}

func (d *bootstrapDomain) loadEncryptionPayloadProjection(id uint64, recipe raftbootstrap.Digest) (bootstrapPayloadProjection, error) {
	var zero bootstrapPayloadProjection
	before, err := d.admission.EncryptionFence()
	if err != nil {
		return zero, errors.WithStack(err)
	}
	d.mu.Lock()
	enc := d.encryption
	group := enc.groups[id]
	var grants []bootstrapRotationPayloadGrant
	if recipe != (raftbootstrap.Digest{}) && enc.recipe == recipe {
		grants = slices.Clone(enc.grants)
	}
	d.mu.Unlock()
	if group == nil {
		return zero, errors.Wrap(errBootstrapEncryptionProjectionPending, "actual encrypted group is not attached")
	}
	// This endpoint-only view does not create IO/admin authority. It reuses the
	// actual atomic-cell and pinned-nonce probes, including registration-pending
	// wire encoding, and never reads a sidecar active-ID as write authority.
	installed := bootstrapEncryptionRuntimeIO{config: bootstrapEncryptionRuntimeIOConfig{Group: group, Wiring: enc.wiring, NodeID: enc.nodeID}}
	cipher, key, active, err := installed.installedPayload()
	if err != nil {
		return zero, err
	}
	after, err := d.admission.EncryptionFence()
	if err != nil {
		return zero, errors.WithStack(err)
	}
	if before != after {
		return zero, errors.WithStack(raftadmission.ErrStale)
	}
	return newBootstrapPayloadProjection(id, before, cipher, key, active, grants...)
}
