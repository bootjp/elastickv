package clusterbootstrap

import (
	"crypto/sha256"
	"slices"

	"github.com/bootjp/elastickv/internal/encryptionwiring"
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
	wiring       encryptionwiring.WriteWiring
	nodeID       uint64
	defaultGroup uint64
	allowed      map[uint64]bool
	groups       map[uint64]*kv.ShardGroup
	Recipe       raftbootstrap.Digest
	Grants       []BootstrapRotationPayloadGrant
}

func newBootstrapEncryptionDomain(groups []noderuntime.GroupSpec, request noderuntime.BootstrapEncryptionRequest, optional []encryptionwiring.WriteWiring) (*bootstrapEncryptionDomain, error) {
	if len(optional) > 1 {
		return nil, errors.Wrap(encryptionwiring.ErrBootstrapEncryptionUnsupported, "multiple encryption wirings")
	}
	var w encryptionwiring.WriteWiring
	if len(optional) == 1 {
		w = optional[0]
	}
	if !request.Requested() && !w.EncryptionConfigured() {
		return nil, nil
	}
	if err := encryptionwiring.ValidateBootstrapDomainEncryptionWiring(w, groups); err != nil {
		return nil, errors.WithStack(err)
	}
	allowed := make(map[uint64]bool, len(groups))
	for _, group := range groups {
		allowed[group.ID] = true
	}
	return &bootstrapEncryptionDomain{wiring: w, nodeID: w.RaftEnvelope.FullNodeID, defaultGroup: noderuntime.DefaultGroupID(groups), allowed: allowed,
		groups: make(map[uint64]*kv.ShardGroup)}, nil
}

// Call under startup exclusivity after SG construction and before any async
// registration/proposals. Reattaching the same group is a no-op, not a new wrap.
func (d *BootstrapDomain) AttachEncryptionGroup(id uint64, group *kv.ShardGroup) error {
	if d.Encryption == nil {
		return nil
	}
	if err := validateBootstrapEncryptionGroup(group); err != nil {
		return err
	}
	d.mu.Lock()
	enc := d.Encryption
	previous, allowed := enc.groups[id], enc.allowed[id]
	d.mu.Unlock()
	if !allowed {
		return errors.WithStack(ErrBootstrapPayloadProjection)
	}
	if previous != nil {
		if previous == group {
			return nil
		}
		return errors.Wrap(encryptionwiring.ErrBootstrapEncryptionUnsupported, "duplicate encryption group")
	}
	end := d.Admission.BeginEncryptionMutation()
	defer end()
	r := enc.wiring.RaftEnvelope
	r.Mu.Lock()
	attached := r.Groups[id]
	r.Mu.Unlock()
	if attached != nil && attached != group {
		return errors.Wrap(encryptionwiring.ErrBootstrapEncryptionUnsupported, "foreign raft wrapper group")
	}
	if attached == nil {
		r.AttachGroup(id, group)
	}
	d.mu.Lock()
	enc.groups[id] = group
	d.mu.Unlock()
	return nil
}

func validateBootstrapEncryptionGroup(group *kv.ShardGroup) error {
	if group == nil || group.Store == nil || group.Engine == nil {
		return errors.Wrap(encryptionwiring.ErrBootstrapEncryptionUnsupported, "missing actual encryption group")
	}
	if group.Proposer() == group.Engine {
		return errors.Wrap(encryptionwiring.ErrBootstrapEncryptionUnsupported, "unwrapped encryption group proposer")
	}
	return nil
}

func (d *BootstrapDomain) AuthorizeRotationRecipe(recipe *BootstrapRotationRecipe) error {
	if d.Encryption == nil || recipe == nil {
		return errors.WithStack(ErrBootstrapRotationRecipe)
	}
	enc := d.Encryption
	if err := enc.validateRecipeIdentity(recipe); err != nil {
		return err
	}
	requests := recipe.Requests()
	grants, err := BootstrapRotationPayloadGrants(enc.defaultGroup, requests...)
	if err != nil {
		return err
	}
	copyRecipe := *recipe
	copyRecipe.RequestsList = requests
	if err := copyRecipe.Hash(); err != nil {
		return err
	}
	digest := raftbootstrap.Digest(recipe.Digest())
	if digest == (raftbootstrap.Digest{}) || copyRecipe.Digest() != recipe.Digest() {
		return errors.WithStack(ErrBootstrapRotationRecipe)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if enc.Recipe == digest {
		if !slices.Equal(enc.Grants, grants) {
			return errors.WithStack(ErrBootstrapRotationRecipe)
		}
		return nil
	}
	enc.Recipe = digest
	enc.Grants = slices.Clone(grants)
	return nil
}

func (enc *bootstrapEncryptionDomain) validateRecipeIdentity(recipe *BootstrapRotationRecipe) error {
	if recipe.Config.NodeID != enc.nodeID || recipe.Config.StorageEpoch != enc.wiring.Epoch || recipe.Config.RaftEpoch != enc.wiring.RaftEpoch {
		return errors.WithStack(ErrBootstrapRotationRecipe)
	}
	for _, req := range recipe.RequestsList {
		if req == nil || req.ProposerNodeId != enc.nodeID {
			return errors.WithStack(ErrBootstrapRotationRecipe)
		}
		epoch := enc.wiring.Epoch
		if req.Purpose == pb.RotateDEKRequest_PURPOSE_RAFT {
			epoch = enc.wiring.RaftEpoch
		}
		if req.ProposerLocalEpoch != uint32(epoch) {
			return errors.WithStack(ErrBootstrapRotationRecipe)
		}
	}
	return nil
}

func (d *BootstrapDomain) InspectPayload(kind raftadmission.Kind, group uint64, final []byte, recipe raftbootstrap.Digest) (raftadmission.PayloadInspection, error) {
	var zero raftadmission.PayloadInspection
	if d.Encryption == nil {
		if err := BootstrapPayloadValidator(d.cluster)(kind, group, final); err != nil {
			return zero, err
		}
		return raftadmission.PayloadInspection{PlaintextDigest: sha256.Sum256(final)}, nil
	}
	if kind == raftadmission.Rotation && recipe == (raftbootstrap.Digest{}) {
		return zero, errors.WithStack(ErrBootstrapRotationRecipe)
	}
	validate, err := NewBootstrapFinalPayloadValidator(d.cluster, func(id uint64) (BootstrapPayloadProjection, error) {
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

func (d *BootstrapDomain) loadEncryptionPayloadProjection(id uint64, recipe raftbootstrap.Digest) (BootstrapPayloadProjection, error) {
	var zero BootstrapPayloadProjection
	before, err := d.Admission.EncryptionFence()
	if err != nil {
		return zero, errors.WithStack(err)
	}
	d.mu.Lock()
	enc := d.Encryption
	group := enc.groups[id]
	var grants []BootstrapRotationPayloadGrant
	if recipe != (raftbootstrap.Digest{}) && enc.Recipe == recipe {
		grants = slices.Clone(enc.Grants)
	}
	d.mu.Unlock()
	if group == nil {
		return zero, errors.Wrap(encryptionwiring.ErrBootstrapEncryptionProjectionPending, "actual encrypted group is not attached")
	}
	// This endpoint-only view does not create IO/admin authority. It reuses the
	// actual atomic-cell and pinned-nonce probes, including registration-pending
	// wire encoding, and never reads a sidecar active-ID as write authority.
	installed := BootstrapEncryptionRuntimeIO{Config: BootstrapEncryptionRuntimeIOConfig{Group: group, Wiring: enc.wiring, NodeID: enc.nodeID}}
	cipher, key, active, err := installed.installedPayload()
	if err != nil {
		return zero, err
	}
	after, err := d.Admission.EncryptionFence()
	if err != nil {
		return zero, errors.WithStack(err)
	}
	if before != after {
		return zero, errors.WithStack(raftadmission.ErrStale)
	}
	return NewBootstrapPayloadProjection(id, before, cipher, key, active, grants...)
}
