package clusterbootstrap

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

var ErrBootstrapRotationInvalidEvidence = errors.New("rotation recipe: invalid durable evidence")

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
type BootstrapRotationRecipeConfig struct {
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
	BootstrapRotationPending bootstrapRotationOutcome = iota
	BootstrapRotationReconciled
	BootstrapRotationSuperseded
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
type BootstrapRotationRecipe struct {
	Config       BootstrapRotationRecipeConfig
	Baseline     encryption.ActiveKeys
	RequestsList []*pb.RotateDEKRequest
	digest       [sha256.Size]byte
}

func NewBootstrapRotationRecipe(ctx context.Context, cfg BootstrapRotationRecipeConfig) (*BootstrapRotationRecipe, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if !cfg.IsLeader() {
		return nil, fmt.Errorf("rotation recipe: only a leader may plan: %w", raftengine.ErrNotLeader)
	}
	r := &BootstrapRotationRecipe{Config: cfg}
	sc, _, err := r.Read(ctx)
	if err != nil {
		return nil, err
	}
	r.Baseline = sc.Active
	if err := r.plan(sc); err != nil {
		return nil, err
	}
	if err := r.Hash(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

func (cfg BootstrapRotationRecipeConfig) validate() error {
	if cfg.Admin == nil || cfg.Barrier == nil || cfg.Validate == nil || cfg.IsLeader == nil {
		return errors.New("rotation recipe: incomplete authority configuration")
	}
	return cfg.validateIO()
}

func (cfg BootstrapRotationRecipeConfig) validateIO() error {
	if cfg.ReadSidecar == nil || cfg.Registry == nil || cfg.Wrapper == nil || cfg.NodeID == 0 || cfg.InstallAndVerify == nil {
		return errors.New("rotation recipe: incomplete IO configuration")
	}
	return nil
}

func (r *BootstrapRotationRecipe) plan(sc *encryption.Sidecar) error {
	next, err := encryptionwiring.NextRotateOnStartupKeyID(sc)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, purpose := range []pb.RotateDEKRequest_Purpose{pb.RotateDEKRequest_PURPOSE_STORAGE, pb.RotateDEKRequest_PURPOSE_RAFT} {
		active, epoch := sc.Active.Storage, r.Config.StorageEpoch
		if purpose == pb.RotateDEKRequest_PURPOSE_RAFT {
			active, epoch = sc.Active.Raft, r.Config.RaftEpoch
		}
		if active == 0 {
			continue
		}
		if next == 0 {
			return errors.New("rotation recipe: key ID space exhausted")
		}
		wrapped, err := encryptionwiring.WrapFreshStartupDEK(r.Config.Wrapper)
		if err != nil {
			return errors.WithStack(err)
		}
		r.RequestsList = append(r.RequestsList, &pb.RotateDEKRequest{Purpose: purpose, NewDekId: next, WrappedNewDek: bytes.Clone(wrapped), ProposerNodeId: r.Config.NodeID, ProposerLocalEpoch: uint32(epoch)})
		if next == math.MaxUint32 {
			next = 0
		} else {
			next++
		}
	}
	return nil
}

func (r *BootstrapRotationRecipe) Hash() error {
	h := sha256.New()
	_, _ = h.Write([]byte("elastickv/bootstrap-rotation-recipe/v1\x00"))
	_, _ = h.Write(binary.BigEndian.AppendUint64(nil, r.Config.NodeID))
	_, _ = h.Write(binary.BigEndian.AppendUint32(nil, r.Baseline.Storage))
	_, _ = h.Write(binary.BigEndian.AppendUint32(nil, r.Baseline.Raft))
	for _, req := range r.RequestsList {
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

func (r *BootstrapRotationRecipe) Digest() [sha256.Size]byte { return r.digest }

// Requests returns copies for binding exact encrypted-payload grants to Rot.
func (r *BootstrapRotationRecipe) Requests() []*pb.RotateDEKRequest {
	result := make([]*pb.RotateDEKRequest, 0, len(r.RequestsList))
	for _, req := range r.RequestsList {
		result = append(result, CloneBootstrapRotationRequest(req))
	}
	return result
}

func CloneBootstrapRotationRequest(req *pb.RotateDEKRequest) *pb.RotateDEKRequest {
	return &pb.RotateDEKRequest{Purpose: req.Purpose, NewDekId: req.NewDekId, WrappedNewDek: bytes.Clone(req.WrappedNewDek), ProposerNodeId: req.ProposerNodeId, ProposerLocalEpoch: req.ProposerLocalEpoch}
}

func (r *BootstrapRotationRecipe) Read(ctx context.Context) (*encryption.Sidecar, uint64, error) {
	if err := r.Config.Validate(ctx); err != nil {
		return nil, 0, err
	}
	index, err := r.Config.Barrier(ctx)
	if err != nil {
		return nil, 0, err
	}
	sc, err := r.Config.ReadSidecar()
	if err != nil {
		return nil, 0, bootstrapRotationEvidenceError(err)
	}
	if sc == nil || index == 0 {
		return nil, 0, ErrBootstrapRotationInvalidEvidence
	}
	if sc.RaftAppliedIndex > index {
		return nil, 0, fmt.Errorf("rotation recipe: sidecar advanced after barrier: %w", encryptionwiring.ErrBootstrapEncryptionProjectionPending)
	}
	if err := r.Config.Validate(ctx); err != nil {
		return nil, 0, err
	}
	return sc, index, nil
}

// Run performs at most one proposal per purpose. Every retry uses the exact
// planned bytes, and even an OK admin response requires durable reconciliation.
func (r *BootstrapRotationRecipe) Run(ctx context.Context) (bootstrapRotationReceipt, error) {
	for _, req := range r.RequestsList {
		sc, _, err := r.Read(ctx)
		if err != nil {
			return r.pending(), err
		}
		state, err := r.classify(sc)
		if err != nil {
			return r.pending(), bootstrapRotationEvidenceError(err)
		}
		if state == BootstrapRotationSuperseded {
			return r.Reconcile(ctx)
		}
		if r.matches(sc, req) {
			if err := r.VerifyActive(ctx, sc); err != nil {
				return r.pending(), bootstrapRotationEvidenceError(err)
			}
			continue
		}
		if result, err := r.attempt(ctx, req); err != nil || result.Outcome == BootstrapRotationSuperseded {
			return result, err
		}
	}
	return r.Reconcile(ctx)
}

func (r *BootstrapRotationRecipe) attempt(ctx context.Context, req *pb.RotateDEKRequest) (bootstrapRotationReceipt, error) {
	if err := r.Config.Validate(ctx); err != nil {
		return r.pending(), err
	}
	if !r.Config.IsLeader() {
		return r.pending(), fmt.Errorf("rotation recipe: follower cannot rotate: %w", raftengine.ErrNotLeader)
	}
	_, proposalErr := r.Config.Admin.RotateDEK(ctx, CloneBootstrapRotationRequest(req))
	// A failed/cancelled result never means absent. The next owned run
	// can reconcile if this context has already been revoked.
	observed, _, readErr := r.Read(ctx)
	if readErr != nil {
		return r.pending(), errors.Join(proposalErr, readErr)
	}
	if !r.matches(observed, req) {
		result, reconcileErr := r.Reconcile(ctx)
		if reconcileErr == nil && result.Outcome == BootstrapRotationSuperseded {
			return result, nil
		}
		if reconcileErr != nil {
			return r.pending(), errors.Join(proposalErr, bootstrapRotationEvidenceError(reconcileErr))
		}
		return r.pending(), errors.Join(proposalErr, fmt.Errorf("rotation recipe: proposal outcome unresolved: %w", kv.ErrTransactionOutcomeUnknown))
	}
	if err := r.VerifyActive(ctx, observed); err != nil {
		return r.pending(), errors.Join(proposalErr, bootstrapRotationEvidenceError(err))
	}
	return r.pending(), nil
}

func (r *BootstrapRotationRecipe) pending() bootstrapRotationReceipt {
	return bootstrapRotationReceipt{RecipeDigest: r.digest, Outcome: BootstrapRotationPending}
}

func (r *BootstrapRotationRecipe) matches(sc *encryption.Sidecar, req *pb.RotateDEKRequest) bool {
	k, ok := sc.Keys[strconv.FormatUint(uint64(req.NewDekId), 10)]
	purpose, active := encryption.SidecarPurposeStorage, sc.Active.Storage
	if req.Purpose == pb.RotateDEKRequest_PURPOSE_RAFT {
		purpose, active = encryption.SidecarPurposeRaft, sc.Active.Raft
	}
	return ok && active == req.NewDekId && k.Purpose == purpose && bytes.Equal(k.Wrapped, req.WrappedNewDek) && uint32(k.LocalEpoch) == req.ProposerLocalEpoch
}

func (r *BootstrapRotationRecipe) classify(sc *encryption.Sidecar) (bootstrapRotationOutcome, error) {
	if (r.Baseline.Storage == 0 && sc.Active.Storage != 0) || (r.Baseline.Raft == 0 && sc.Active.Raft != 0) {
		return BootstrapRotationSuperseded, nil
	}
	result := BootstrapRotationReconciled
	for _, req := range r.RequestsList {
		state, err := r.classifyRequest(sc, req)
		if err != nil || state == BootstrapRotationSuperseded {
			return state, err
		}
		if state == BootstrapRotationPending {
			result = state
		}
	}
	return result, nil
}

func (r *BootstrapRotationRecipe) classifyRequest(sc *encryption.Sidecar, req *pb.RotateDEKRequest) (bootstrapRotationOutcome, error) {
	if r.matches(sc, req) {
		return BootstrapRotationReconciled, nil
	}
	active, baseline := sc.Active.Storage, r.Baseline.Storage
	if req.Purpose == pb.RotateDEKRequest_PURPOSE_RAFT {
		active, baseline = sc.Active.Raft, r.Baseline.Raft
	}
	if active == 0 {
		return BootstrapRotationPending, errors.New("rotation recipe: active purpose disappeared")
	}
	_, occupied := sc.Keys[strconv.FormatUint(uint64(req.NewDekId), 10)]
	if active != baseline || occupied {
		return BootstrapRotationSuperseded, nil
	}
	return BootstrapRotationPending, nil
}

// Reconcile is strictly read-only with respect to Raft. It may repair local
// wrappers, then fences/rechecks the exact durable image before returning.
// A receipt is an observation, not permission to clear responsibility under A.
func (r *BootstrapRotationRecipe) Reconcile(ctx context.Context) (bootstrapRotationReceipt, error) {
	sc, _, err := r.Read(ctx)
	if err != nil {
		return r.pending(), err
	}
	outcome, err := r.classify(sc)
	if err != nil {
		return r.pending(), bootstrapRotationEvidenceError(err)
	}
	complete := outcome == BootstrapRotationReconciled
	if outcome == BootstrapRotationPending {
		outcome = BootstrapRotationReconciled
	}
	if err := r.VerifyActive(ctx, sc); err != nil {
		return r.pending(), bootstrapRotationEvidenceError(err)
	}
	after, index, err := r.Read(ctx)
	if err != nil {
		return r.pending(), err
	}
	if !rotationActiveEqual(sc, after) {
		return r.pending(), fmt.Errorf("rotation recipe: durable projection changed during wrapper installation: %w", encryptionwiring.ErrBootstrapEncryptionProjectionPending)
	}
	if err := r.VerifyActive(ctx, after); err != nil {
		return r.pending(), bootstrapRotationEvidenceError(err)
	}
	if err := r.Config.Validate(ctx); err != nil {
		return r.pending(), err
	}
	return bootstrapRotationReceipt{Outcome: outcome, Complete: complete, RecipeDigest: r.digest, AppliedIndex: index, Active: after.Active}, nil
}

func bootstrapRotationEvidenceError(err error) error {
	if err == nil || BootstrapRotationTransient(err) {
		return err
	}
	return errors.Join(ErrBootstrapRotationInvalidEvidence, err)
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

func (r *BootstrapRotationRecipe) VerifyActive(ctx context.Context, sc *encryption.Sidecar) error {
	for _, purpose := range []string{encryption.SidecarPurposeStorage, encryption.SidecarPurposeRaft} {
		id, epoch := sc.Active.Storage, r.Config.StorageEpoch
		if purpose == encryption.SidecarPurposeRaft {
			id, epoch = sc.Active.Raft, r.Config.RaftEpoch
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

func (r *BootstrapRotationRecipe) verifyPurpose(ctx context.Context, sc *encryption.Sidecar, id uint32, epoch uint16, purpose string) error {
	k, ok := sc.Keys[strconv.FormatUint(uint64(id), 10)]
	if !ok || k.Purpose != purpose || len(k.Wrapped) == 0 || k.LocalEpoch != epoch {
		return errors.New("rotation recipe: active key metadata mismatch")
	}
	if err := r.verifyRegistry(id, epoch); err != nil {
		return err
	}
	if err := r.Config.Validate(ctx); err != nil {
		return err
	}
	if err := r.Config.InstallAndVerify(ctx, id, k); err != nil {
		return err
	}
	return nil
}

func (r *BootstrapRotationRecipe) verifyRegistry(id uint32, epoch uint16) error {
	raw, present, err := r.Config.Registry.GetRegistryRow(encryption.RegistryKey(id, encryption.NodeID16(r.Config.NodeID)))
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
	if row.FullNodeID != r.Config.NodeID || row.FirstSeenLocalEpoch > row.LastSeenLocalEpoch {
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
