package clusterbootstrap

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryptionwiring"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
)

var ErrBootstrapExternalRotationRequest = errors.New("external rotation: invalid request")

// The controller retains this immutable recipe across unknown outcomes and
// supplies its existing owned context. Construction grants no proposal authority.
func NewBootstrapExternalRotationRecipe(ctx context.Context, cfg BootstrapRotationRecipeConfig, request *pb.RotateDEKRequest) (*BootstrapRotationRecipe, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	material, err := validateBootstrapExternalRotationRequest(cfg, request)
	if err != nil {
		return nil, err
	}
	defer encryptionwiring.ZeroBytes(material)
	req := CloneBootstrapRotationRequest(request)
	if !cfg.IsLeader() {
		return nil, fmt.Errorf("external rotation: only a leader may plan: %w", raftengine.ErrNotLeader)
	}
	r := &BootstrapRotationRecipe{Config: cfg, RequestsList: []*pb.RotateDEKRequest{req}}
	sc, _, err := r.Read(ctx)
	if err != nil {
		return nil, err
	}
	if err := validateBootstrapExternalRotationKey(sc, req); err != nil {
		return nil, err
	}
	purpose := encryption.SidecarPurposeStorage
	if req.Purpose == pb.RotateDEKRequest_PURPOSE_RAFT {
		purpose = encryption.SidecarPurposeRaft
	}
	if err := encryption.ValidateRotationDEKMaterial(sc, cfg.Wrapper, req.NewDekId, purpose, material); err != nil {
		return nil, errors.Join(ErrBootstrapExternalRotationRequest, err)
	}
	r.Baseline = sc.Active
	if err := r.Hash(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

func validateBootstrapExternalRotationRequest(cfg BootstrapRotationRecipeConfig, request *pb.RotateDEKRequest) ([]byte, error) {
	if _, err := BootstrapRotationPayloadFromRequest(request); err != nil {
		return nil, errors.Join(ErrBootstrapExternalRotationRequest, err)
	}
	epoch := cfg.StorageEpoch
	if request.Purpose == pb.RotateDEKRequest_PURPOSE_RAFT {
		epoch = cfg.RaftEpoch
	}
	if request.ProposerNodeId != cfg.NodeID || request.ProposerLocalEpoch != uint32(epoch) {
		return nil, fmt.Errorf("%w: proposer identity or pinned purpose epoch mismatch", ErrBootstrapExternalRotationRequest)
	}
	dek, err := cfg.Wrapper.Unwrap(bytes.Clone(request.WrappedNewDek))
	if err != nil {
		encryptionwiring.ZeroBytes(dek)
		return nil, errors.Join(ErrBootstrapExternalRotationRequest, fmt.Errorf("external rotation: unwrap requested key: %w", err))
	}
	if len(dek) != encryption.KeySize {
		encryptionwiring.ZeroBytes(dek)
		return nil, fmt.Errorf("%w: invalid unwrapped key size", ErrBootstrapExternalRotationRequest)
	}
	return dek, nil
}

func validateBootstrapExternalRotationKey(sc *encryption.Sidecar, req *pb.RotateDEKRequest) error {
	purpose, active := encryption.SidecarPurposeStorage, sc.Active.Storage
	if req.Purpose == pb.RotateDEKRequest_PURPOSE_RAFT {
		purpose, active = encryption.SidecarPurposeRaft, sc.Active.Raft
	}
	if active == 0 {
		return fmt.Errorf("%w: requested purpose is inactive", ErrBootstrapExternalRotationRequest)
	}
	k, occupied := sc.Keys[strconv.FormatUint(uint64(req.NewDekId), 10)]
	if occupied && (k.Purpose != purpose || uint32(k.LocalEpoch) != req.ProposerLocalEpoch || !bytes.Equal(k.Wrapped, req.WrappedNewDek)) {
		return fmt.Errorf("%w: key ID collision", ErrBootstrapExternalRotationRequest)
	}
	return nil
}
