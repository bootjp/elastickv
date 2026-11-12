package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/bootjp/elastickv/internal/encryptionwiring"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

var errBootstrapRotationInvalidEvidence = errors.New("rotation recipe: invalid durable evidence")

type bootstrapRotationAdmin interface {
	RotateDEK(context.Context, *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error)
}

type bootstrapRotationRegistry interface {
	GetRegistryRow([]byte) ([]byte, bool, error)
}

// Barrier must fence against the current leader and wait for LOCAL apply.
// Followers supply the read-only recovery lane, never a proposal exception.
// Validate checks the caller-supplied Rot/recovery generation; this module
// neither acquires authority nor publishes completion/responsibility changes.
type bootstrapRotationRecipeConfig struct {
	Admin                   bootstrapRotationAdmin
	Barrier                 func(context.Context) (uint64, error)
	Validate                func(context.Context) error
	IsLeader                func() bool
	ReadSidecar             func() (*encryption.Sidecar, error)
	Registry                bootstrapRotationRegistry
	Wrapper                 kek.Wrapper
	NodeID                  uint64
	StorageEpoch, RaftEpoch uint16
	// InstallAndVerify must verify exact material and the installed active
	// wrapper/nonce epoch, not just that some wrapper is present.
	InstallAndVerify func(context.Context, uint32, encryption.SidecarKey) error
}

type bootstrapRotationOutcome uint8

const (
	bootstrapRotationPending bootstrapRotationOutcome = iota
	bootstrapRotationReconciled
	bootstrapRotationSuperseded
)

type bootstrapRotationReceipt struct {
	Outcome bootstrapRotationOutcome
	// Reconciled without Complete resolves old responsibility, not the
	// requested rotation. Only an owning leader may publish Complete.
	Complete     bool
	RecipeDigest [sha256.Size]byte
	AppliedIndex uint64
	Active       encryption.ActiveKeys
}

// Immutable after construction. The controller serializes Run/Reconcile and
// retains the recipe across term changes until its responsibility is resolved.
type bootstrapRotationRecipe struct {
	config   bootstrapRotationRecipeConfig
	baseline encryption.ActiveKeys
	requests []*pb.RotateDEKRequest
	digest   [sha256.Size]byte
}

func newBootstrapRotationRecipe(ctx context.Context, cfg bootstrapRotationRecipeConfig) (*bootstrapRotationRecipe, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if !cfg.IsLeader() {
		return nil, fmt.Errorf("rotation recipe: only a leader may plan: %w", raftengine.ErrNotLeader)
	}
	r := &bootstrapRotationRecipe{config: cfg}
	sc, _, err := r.read(ctx)
	if err != nil {
		return nil, err
	}
	r.baseline = sc.Active
	if err := r.plan(sc); err != nil {
		return nil, err
	}
	if err := r.hash(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

func (cfg bootstrapRotationRecipeConfig) validate() error {
	if cfg.Admin == nil || cfg.Barrier == nil || cfg.Validate == nil || cfg.IsLeader == nil {
		return errors.New("rotation recipe: incomplete authority configuration")
	}
	return cfg.validateIO()
}

func (cfg bootstrapRotationRecipeConfig) validateIO() error {
	if cfg.ReadSidecar == nil || cfg.Registry == nil || cfg.Wrapper == nil || cfg.NodeID == 0 || cfg.InstallAndVerify == nil {
		return errors.New("rotation recipe: incomplete IO configuration")
	}
	return nil
}

func (r *bootstrapRotationRecipe) plan(sc *encryption.Sidecar) error {
	next, err := encryptionwiring.NextRotateOnStartupKeyID(sc)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, purpose := range []pb.RotateDEKRequest_Purpose{pb.RotateDEKRequest_PURPOSE_STORAGE, pb.RotateDEKRequest_PURPOSE_RAFT} {
		active, epoch := sc.Active.Storage, r.config.StorageEpoch
		if purpose == pb.RotateDEKRequest_PURPOSE_RAFT {
			active, epoch = sc.Active.Raft, r.config.RaftEpoch
		}
		if active == 0 {
			continue
		}
		if next == 0 {
			return errors.New("rotation recipe: key ID space exhausted")
		}
		wrapped, err := encryptionwiring.WrapFreshStartupDEK(r.config.Wrapper)
		if err != nil {
			return errors.WithStack(err)
		}
		r.requests = append(r.requests, &pb.RotateDEKRequest{Purpose: purpose, NewDekId: next, WrappedNewDek: bytes.Clone(wrapped), ProposerNodeId: r.config.NodeID, ProposerLocalEpoch: uint32(epoch)})
		if next == math.MaxUint32 {
			next = 0
		} else {
			next++
		}
	}
	return nil
}

func (r *bootstrapRotationRecipe) hash() error {
	h := sha256.New()
	_, _ = h.Write([]byte("elastickv/bootstrap-rotation-recipe/v1\x00"))
	_, _ = h.Write(binary.BigEndian.AppendUint64(nil, r.config.NodeID))
	_, _ = h.Write(binary.BigEndian.AppendUint32(nil, r.baseline.Storage))
	_, _ = h.Write(binary.BigEndian.AppendUint32(nil, r.baseline.Raft))
	for _, req := range r.requests {
		data, err := proto.MarshalOptions{Deterministic: true}.Marshal(req)
		if err != nil {
			return fmt.Errorf("rotation recipe: hash request: %w", err)
		}
		_, _ = h.Write(binary.BigEndian.AppendUint64(nil, uint64(len(data))))
		_, _ = h.Write(data)
	}
	copy(r.digest[:], h.Sum(nil))
	return nil
}

func (r *bootstrapRotationRecipe) Digest() [sha256.Size]byte { return r.digest }

// Requests returns copies for binding exact encrypted-payload grants to Rot.
func (r *bootstrapRotationRecipe) Requests() []*pb.RotateDEKRequest {
	result := make([]*pb.RotateDEKRequest, 0, len(r.requests))
	for _, req := range r.requests {
		result = append(result, cloneBootstrapRotationRequest(req))
	}
	return result
}

func cloneBootstrapRotationRequest(req *pb.RotateDEKRequest) *pb.RotateDEKRequest {
	return &pb.RotateDEKRequest{Purpose: req.Purpose, NewDekId: req.NewDekId, WrappedNewDek: bytes.Clone(req.WrappedNewDek), ProposerNodeId: req.ProposerNodeId, ProposerLocalEpoch: req.ProposerLocalEpoch}
}

func (r *bootstrapRotationRecipe) read(ctx context.Context) (*encryption.Sidecar, uint64, error) {
	if err := r.config.Validate(ctx); err != nil {
		return nil, 0, err
	}
	index, err := r.config.Barrier(ctx)
	if err != nil {
		return nil, 0, err
	}
	sc, err := r.config.ReadSidecar()
	if err != nil {
		return nil, 0, bootstrapRotationEvidenceError(err)
	}
	if sc == nil || index == 0 {
		return nil, 0, errBootstrapRotationInvalidEvidence
	}
	if sc.RaftAppliedIndex > index {
		return nil, 0, fmt.Errorf("rotation recipe: sidecar advanced after barrier: %w", encryptionwiring.ErrBootstrapEncryptionProjectionPending)
	}
	if err := r.config.Validate(ctx); err != nil {
		return nil, 0, err
	}
	return sc, index, nil
}

// Run performs at most one proposal per purpose. Every retry uses the exact
// planned bytes, and even an OK admin response requires durable reconciliation.
func (r *bootstrapRotationRecipe) Run(ctx context.Context) (bootstrapRotationReceipt, error) {
	for _, req := range r.requests {
		sc, _, err := r.read(ctx)
		if err != nil {
			return r.pending(), err
		}
		state, err := r.classify(sc)
		if err != nil {
			return r.pending(), bootstrapRotationEvidenceError(err)
		}
		if state == bootstrapRotationSuperseded {
			return r.Reconcile(ctx)
		}
		if r.matches(sc, req) {
			if err := r.verifyActive(ctx, sc); err != nil {
				return r.pending(), bootstrapRotationEvidenceError(err)
			}
			continue
		}
		if result, err := r.attempt(ctx, req); err != nil || result.Outcome == bootstrapRotationSuperseded {
			return result, err
		}
	}
	return r.Reconcile(ctx)
}

func (r *bootstrapRotationRecipe) attempt(ctx context.Context, req *pb.RotateDEKRequest) (bootstrapRotationReceipt, error) {
	if err := r.config.Validate(ctx); err != nil {
		return r.pending(), err
	}
	if !r.config.IsLeader() {
		return r.pending(), fmt.Errorf("rotation recipe: follower cannot rotate: %w", raftengine.ErrNotLeader)
	}
	_, proposalErr := r.config.Admin.RotateDEK(ctx, cloneBootstrapRotationRequest(req))
	// A failed/cancelled result never means absent. The next owned run
	// can reconcile if this context has already been revoked.
	observed, _, readErr := r.read(ctx)
	if readErr != nil {
		return r.pending(), errors.Join(proposalErr, readErr)
	}
	if !r.matches(observed, req) {
		result, reconcileErr := r.Reconcile(ctx)
		if reconcileErr == nil && result.Outcome == bootstrapRotationSuperseded {
			return result, nil
		}
		if reconcileErr != nil {
			return r.pending(), errors.Join(proposalErr, bootstrapRotationEvidenceError(reconcileErr))
		}
		return r.pending(), errors.Join(proposalErr, fmt.Errorf("rotation recipe: proposal outcome unresolved: %w", kv.ErrTransactionOutcomeUnknown))
	}
	if err := r.verifyActive(ctx, observed); err != nil {
		return r.pending(), errors.Join(proposalErr, bootstrapRotationEvidenceError(err))
	}
	return r.pending(), nil
}

func (r *bootstrapRotationRecipe) pending() bootstrapRotationReceipt {
	return bootstrapRotationReceipt{RecipeDigest: r.digest, Outcome: bootstrapRotationPending}
}

func (r *bootstrapRotationRecipe) matches(sc *encryption.Sidecar, req *pb.RotateDEKRequest) bool {
	k, ok := sc.Keys[strconv.FormatUint(uint64(req.NewDekId), 10)]
	purpose, active := encryption.SidecarPurposeStorage, sc.Active.Storage
	if req.Purpose == pb.RotateDEKRequest_PURPOSE_RAFT {
		purpose, active = encryption.SidecarPurposeRaft, sc.Active.Raft
	}
	return ok && active == req.NewDekId && k.Purpose == purpose && bytes.Equal(k.Wrapped, req.WrappedNewDek) && uint32(k.LocalEpoch) == req.ProposerLocalEpoch
}

func (r *bootstrapRotationRecipe) classify(sc *encryption.Sidecar) (bootstrapRotationOutcome, error) {
	if (r.baseline.Storage == 0 && sc.Active.Storage != 0) || (r.baseline.Raft == 0 && sc.Active.Raft != 0) {
		return bootstrapRotationSuperseded, nil
	}
	result := bootstrapRotationReconciled
	for _, req := range r.requests {
		state, err := r.classifyRequest(sc, req)
		if err != nil || state == bootstrapRotationSuperseded {
			return state, err
		}
		if state == bootstrapRotationPending {
			result = state
		}
	}
	return result, nil
}

func (r *bootstrapRotationRecipe) classifyRequest(sc *encryption.Sidecar, req *pb.RotateDEKRequest) (bootstrapRotationOutcome, error) {
	if r.matches(sc, req) {
		return bootstrapRotationReconciled, nil
	}
	active, baseline := sc.Active.Storage, r.baseline.Storage
	if req.Purpose == pb.RotateDEKRequest_PURPOSE_RAFT {
		active, baseline = sc.Active.Raft, r.baseline.Raft
	}
	if active == 0 {
		return bootstrapRotationPending, errors.New("rotation recipe: active purpose disappeared")
	}
	_, occupied := sc.Keys[strconv.FormatUint(uint64(req.NewDekId), 10)]
	if active != baseline || occupied {
		return bootstrapRotationSuperseded, nil
	}
	return bootstrapRotationPending, nil
}

// Reconcile is strictly read-only with respect to Raft. It may repair local
// wrappers, then fences/rechecks the exact durable image before returning.
// A receipt is an observation, not permission to clear responsibility under A.
func (r *bootstrapRotationRecipe) Reconcile(ctx context.Context) (bootstrapRotationReceipt, error) {
	sc, _, err := r.read(ctx)
	if err != nil {
		return r.pending(), err
	}
	outcome, err := r.classify(sc)
	if err != nil {
		return r.pending(), bootstrapRotationEvidenceError(err)
	}
	complete := outcome == bootstrapRotationReconciled
	if outcome == bootstrapRotationPending {
		outcome = bootstrapRotationReconciled
	}
	if err := r.verifyActive(ctx, sc); err != nil {
		return r.pending(), bootstrapRotationEvidenceError(err)
	}
	after, index, err := r.read(ctx)
	if err != nil {
		return r.pending(), err
	}
	if !rotationActiveEqual(sc, after) {
		return r.pending(), fmt.Errorf("rotation recipe: durable projection changed during wrapper installation: %w", encryptionwiring.ErrBootstrapEncryptionProjectionPending)
	}
	if err := r.verifyActive(ctx, after); err != nil {
		return r.pending(), bootstrapRotationEvidenceError(err)
	}
	if err := r.config.Validate(ctx); err != nil {
		return r.pending(), err
	}
	return bootstrapRotationReceipt{Outcome: outcome, Complete: complete, RecipeDigest: r.digest, AppliedIndex: index, Active: after.Active}, nil
}

func bootstrapRotationEvidenceError(err error) error {
	if err == nil || bootstrapRotationTransient(err) {
		return err
	}
	return errors.Join(errBootstrapRotationInvalidEvidence, err)
}

func rotationActiveEqual(a, b *encryption.Sidecar) bool {
	if a.Active != b.Active || a.RaftAppliedIndex != b.RaftAppliedIndex {
		return false
	}
	for _, id := range []uint32{a.Active.Storage, a.Active.Raft} {
		key := strconv.FormatUint(uint64(id), 10)
		x, y := a.Keys[key], b.Keys[key]
		if x.Purpose != y.Purpose || x.LocalEpoch != y.LocalEpoch || !bytes.Equal(x.Wrapped, y.Wrapped) {
			return false
		}
	}
	return true
}

func (r *bootstrapRotationRecipe) verifyActive(ctx context.Context, sc *encryption.Sidecar) error {
	for _, purpose := range []string{encryption.SidecarPurposeStorage, encryption.SidecarPurposeRaft} {
		id, epoch := sc.Active.Storage, r.config.StorageEpoch
		if purpose == encryption.SidecarPurposeRaft {
			id, epoch = sc.Active.Raft, r.config.RaftEpoch
		}
		if id == 0 {
			continue
		}
		if err := r.verifyPurpose(ctx, sc, id, epoch, purpose); err != nil {
			return err
		}
	}
	return nil
}

func (r *bootstrapRotationRecipe) verifyPurpose(ctx context.Context, sc *encryption.Sidecar, id uint32, epoch uint16, purpose string) error {
	k, ok := sc.Keys[strconv.FormatUint(uint64(id), 10)]
	if !ok || k.Purpose != purpose || len(k.Wrapped) == 0 || k.LocalEpoch != epoch {
		return errors.New("rotation recipe: active key metadata mismatch")
	}
	if err := r.verifyRegistry(id, epoch); err != nil {
		return err
	}
	if err := r.config.Validate(ctx); err != nil {
		return err
	}
	if err := r.config.InstallAndVerify(ctx, id, k); err != nil {
		return err
	}
	return nil
}

func (r *bootstrapRotationRecipe) verifyRegistry(id uint32, epoch uint16) error {
	raw, present, err := r.config.Registry.GetRegistryRow(encryption.RegistryKey(id, encryption.NodeID16(r.config.NodeID)))
	if err != nil {
		return fmt.Errorf("rotation recipe: registry read: %w", err)
	}
	if !present {
		return fmt.Errorf("rotation recipe: missing registry for key %d: %w", id, encryptionwiring.ErrBootstrapEncryptionProjectionPending)
	}
	row, err := encryption.DecodeRegistryValue(raw)
	if err != nil {
		return fmt.Errorf("rotation recipe: registry decode: %w", err)
	}
	if row.FullNodeID != r.config.NodeID || row.FirstSeenLocalEpoch > row.LastSeenLocalEpoch {
		return errors.New("rotation recipe: registry identity/epoch mismatch")
	}
	if row.LastSeenLocalEpoch > epoch {
		return fmt.Errorf("rotation recipe: registry epoch rollback: %w", encryption.ErrLocalEpochRollback)
	}
	if row.LastSeenLocalEpoch < epoch {
		return fmt.Errorf("rotation recipe: local epoch not registered: %w", encryptionwiring.ErrBootstrapEncryptionProjectionPending)
	}
	return nil
}
