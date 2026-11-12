package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryptionwiring"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type rotationRecipeTestRegistry map[string][]byte

func (r rotationRecipeTestRegistry) GetRegistryRow(key []byte) ([]byte, bool, error) {
	v, ok := r[string(key)]
	return bytes.Clone(v), ok, nil
}

type rotationRecipeTestAdmin func(context.Context, *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error)

func (f rotationRecipeTestAdmin) RotateDEK(ctx context.Context, req *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
	return f(ctx, req)
}

type rotationRecipeTestKEK struct{ fakeStartupKEK }

func (rotationRecipeTestKEK) Unwrap(wrapped []byte) ([]byte, error) {
	if len(wrapped) == encryption.KeySize+1 && wrapped[0] == 0xAA {
		return bytes.Clone(wrapped[1:]), nil
	}
	if len(wrapped) != encryption.KeySize {
		return nil, errors.New("bad wrapped dek length")
	}
	return bytes.Clone(wrapped), nil
}

type rotationRecipeFixture struct {
	t                      *testing.T
	path                   string
	registry               rotationRecipeTestRegistry
	loaded                 map[uint32][]byte
	cfg                    bootstrapRotationRecipeConfig
	index                  uint64
	leader                 bool
	barriers, installs     int
	calls                  []*pb.RotateDEKRequest
	admin                  func(context.Context, *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error)
	installErr, barrierErr error
}

func newRotationRecipeFixture(t *testing.T) *rotationRecipeFixture {
	t.Helper()
	f := &rotationRecipeFixture{t: t, path: filepath.Join(t.TempDir(), "keys.json"), registry: rotationRecipeTestRegistry{}, loaded: map[uint32][]byte{}, index: 10, leader: true}
	f.cfg = bootstrapRotationRecipeConfig{NodeID: 91, StorageEpoch: 3, RaftEpoch: 5, Wrapper: rotationRecipeTestKEK{}, Registry: f.registry,
		IsLeader: func() bool { return f.leader }, Validate: func(ctx context.Context) error { return ctx.Err() },
		ReadSidecar: func() (*encryption.Sidecar, error) { return encryption.ReadSidecar(f.path) },
		Barrier: func(ctx context.Context) (uint64, error) {
			f.barriers++
			return f.index, errors.Join(ctx.Err(), f.barrierErr)
		},
		InstallAndVerify: func(ctx context.Context, id uint32, key encryption.SidecarKey) error {
			f.installs++
			sc, err := encryption.ReadSidecar(f.path)
			require.NoError(t, err)
			require.Equal(t, sc.Keys[strconv.FormatUint(uint64(id), 10)], key)
			if !bytes.Equal(f.loaded[id], key.Wrapped) {
				return errors.New("installed key material mismatch")
			}
			return errors.Join(ctx.Err(), f.installErr)
		}}
	f.cfg.Admin = rotationRecipeTestAdmin(func(ctx context.Context, req *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
		f.calls = append(f.calls, cloneBootstrapRotationRequest(req))
		if f.admin != nil {
			return f.admin(ctx, req)
		}
		f.apply(req)
		return &pb.RotateDEKResponse{AppliedIndex: f.index}, nil
	})
	sc := &encryption.Sidecar{Active: encryption.ActiveKeys{Storage: 1, Raft: 2}, RaftAppliedIndex: f.index, Keys: map[string]encryption.SidecarKey{
		"1": {Purpose: encryption.SidecarPurposeStorage, Wrapped: bytes.Repeat([]byte{1}, encryption.KeySize), LocalEpoch: 3},
		"2": {Purpose: encryption.SidecarPurposeRaft, Wrapped: bytes.Repeat([]byte{2}, encryption.KeySize), LocalEpoch: 5},
	}}
	require.NoError(t, encryption.WriteSidecar(f.path, sc))
	f.register(1, 3)
	f.register(2, 5)
	f.loaded[1], f.loaded[2] = bytes.Clone(sc.Keys["1"].Wrapped), bytes.Clone(sc.Keys["2"].Wrapped)
	return f
}

func (f *rotationRecipeFixture) register(id uint32, epoch uint16) {
	f.registry[string(encryption.RegistryKey(id, encryption.NodeID16(f.cfg.NodeID)))] = encryption.EncodeRegistryValue(encryption.RegistryValue{FullNodeID: f.cfg.NodeID, FirstSeenLocalEpoch: epoch, LastSeenLocalEpoch: epoch})
}

func (f *rotationRecipeFixture) apply(req *pb.RotateDEKRequest) {
	f.t.Helper()
	sc, err := encryption.ReadSidecar(f.path)
	require.NoError(f.t, err)
	purpose := encryption.SidecarPurposeStorage
	if req.Purpose == pb.RotateDEKRequest_PURPOSE_RAFT {
		purpose, sc.Active.Raft = encryption.SidecarPurposeRaft, req.NewDekId
	} else {
		sc.Active.Storage = req.NewDekId
	}
	f.index++
	sc.RaftAppliedIndex = f.index
	sc.Keys[strconv.FormatUint(uint64(req.NewDekId), 10)] = encryption.SidecarKey{Purpose: purpose, Wrapped: bytes.Clone(req.WrappedNewDek), LocalEpoch: uint16(req.ProposerLocalEpoch)}
	require.NoError(f.t, encryption.WriteSidecar(f.path, sc))
	f.loaded[req.NewDekId] = bytes.Clone(req.WrappedNewDek)
	f.register(req.NewDekId, uint16(req.ProposerLocalEpoch))
}

func TestBootstrapRotationRecipeInterruptedCreateRetainsExactPlan(t *testing.T) {
	t.Parallel()
	f := newRotationRecipeFixture(t)
	r, err := newBootstrapRotationRecipe(t.Context(), f.cfg)
	require.NoError(t, err)
	digest := r.Digest()
	lost := errors.New("outcome unknown")
	f.admin = func(context.Context, *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) { return nil, lost }
	result, err := r.Run(t.Context())
	require.ErrorIs(t, err, lost)
	require.Equal(t, bootstrapRotationPending, result.Outcome)
	require.Len(t, f.calls, 1)
	f.admin = nil
	result, err = r.Run(t.Context())
	require.NoError(t, err)
	require.True(t, proto.Equal(f.calls[0], f.calls[1]), "retry must retain key ID and wrapped material")
	require.Equal(t, digest, result.RecipeDigest)
	require.Equal(t, bootstrapRotationReconciled, result.Outcome)
	require.True(t, result.Complete)
	require.Equal(t, encryption.ActiveKeys{Storage: 3, Raft: 4}, result.Active)
}

func TestBootstrapRotationRecipeLostResultUsesAppliedState(t *testing.T) {
	t.Parallel()
	f := newRotationRecipeFixture(t)
	r, err := newBootstrapRotationRecipe(t.Context(), f.cfg)
	require.NoError(t, err)
	f.admin = func(ctx context.Context, req *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
		require.Same(t, t.Context(), ctx)
		f.apply(req)
		return nil, errors.New("reply lost after commit")
	}
	result, err := r.Run(t.Context())
	require.NoError(t, err)
	require.Equal(t, bootstrapRotationReconciled, result.Outcome)
	require.Len(t, f.calls, 2)
	require.GreaterOrEqual(t, f.barriers, 7, "planning, each result and wrapper completion all require barriers")
}

func TestBootstrapRotationRecipeWrapperFailureReconcilesWithoutProposal(t *testing.T) {
	t.Parallel()
	f := newRotationRecipeFixture(t)
	r, err := newBootstrapRotationRecipe(t.Context(), f.cfg)
	require.NoError(t, err)
	f.installErr = errors.New("wrapper installation interrupted")
	result, err := r.Run(t.Context())
	require.ErrorIs(t, err, f.installErr)
	require.Equal(t, bootstrapRotationPending, result.Outcome)
	require.Len(t, f.calls, 1)
	f.leader, f.installErr = false, nil
	result, err = r.Reconcile(t.Context())
	require.NoError(t, err, "the current applied barrier resolves the unfinished second purpose as absent")
	require.Equal(t, bootstrapRotationReconciled, result.Outcome)
	require.False(t, result.Complete)
	require.Len(t, f.calls, 1, "follower reconciliation cannot call RotateDEK")
	f.leader = true
	result, err = r.Run(t.Context())
	require.NoError(t, err)
	require.Equal(t, bootstrapRotationReconciled, result.Outcome)
	require.Len(t, f.calls, 2)
}

func TestBootstrapRotationRecipeTermChangeRetainsUnknownResponsibility(t *testing.T) {
	t.Parallel()
	f := newRotationRecipeFixture(t)
	r, err := newBootstrapRotationRecipe(t.Context(), f.cfg)
	require.NoError(t, err)
	old, cancel := context.WithCancel(t.Context())
	f.admin = func(ctx context.Context, req *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
		require.Same(t, old, ctx)
		f.apply(req)
		f.leader = false
		cancel()
		return nil, context.Canceled
	}
	result, err := r.Run(old)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, bootstrapRotationPending, result.Outcome)
	result, err = r.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, bootstrapRotationReconciled, result.Outcome)
	require.False(t, result.Complete, "follower resolves responsibility but cannot claim unfinished rotation complete")
	_, err = r.Run(t.Context())
	require.Error(t, err)
	require.Len(t, f.calls, 1, "Run must not call admin when follower")
	f.leader, f.admin = true, nil
	result, err = r.Run(t.Context())
	require.NoError(t, err)
	require.Equal(t, bootstrapRotationReconciled, result.Outcome)
	require.Len(t, f.calls, 2)
	require.True(t, proto.Equal(r.requests[1], f.calls[1]))
}

func TestBootstrapRotationRecipeSupersededRequiresBarrierRegistryAndWrappers(t *testing.T) {
	t.Parallel()
	f := newRotationRecipeFixture(t)
	r, err := newBootstrapRotationRecipe(t.Context(), f.cfg)
	require.NoError(t, err)
	remote := cloneBootstrapRotationRequest(r.requests[0])
	remote.NewDekId, remote.WrappedNewDek = 8, bytes.Repeat([]byte{8}, 40)
	f.apply(remote)
	f.leader = false
	f.barrierErr = errors.New("current leader unavailable")
	_, err = r.Reconcile(t.Context())
	require.ErrorIs(t, err, f.barrierErr)
	f.barrierErr = nil
	key := string(encryption.RegistryKey(8, encryption.NodeID16(f.cfg.NodeID)))
	delete(f.registry, key)
	_, err = r.Reconcile(t.Context())
	require.ErrorContains(t, err, "missing registry")
	f.register(8, 3)
	result, err := r.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, bootstrapRotationSuperseded, result.Outcome)
	require.Empty(t, f.calls)
	require.Greater(t, f.installs, 0)
}

func TestBootstrapRotationRecipeRejectsUnverifiedCompletion(t *testing.T) {
	t.Parallel()
	for _, corruption := range []string{"material", "purpose", "registry_epoch", "registry_identity", "wrapper", "projection_change"} {
		t.Run(corruption, func(t *testing.T) {
			f := newRotationRecipeFixture(t)
			r, err := newBootstrapRotationRecipe(t.Context(), f.cfg)
			require.NoError(t, err)
			_, err = r.Run(t.Context())
			require.NoError(t, err)
			sc, err := encryption.ReadSidecar(f.path)
			require.NoError(t, err)
			key := sc.Keys["3"]
			switch corruption {
			case "material":
				key.Wrapped = []byte("wrong")
			case "purpose":
				key.Purpose = encryption.SidecarPurposeRaft
			case "registry_epoch":
				f.register(3, 4)
			case "registry_identity":
				f.registry[string(encryption.RegistryKey(3, encryption.NodeID16(f.cfg.NodeID)))] = encryption.EncodeRegistryValue(encryption.RegistryValue{FullNodeID: 92, FirstSeenLocalEpoch: 3, LastSeenLocalEpoch: 3})
			case "wrapper":
				f.installErr = errors.New("wrong installed wrapper")
			case "projection_change":
				r.config.InstallAndVerify = func(context.Context, uint32, encryption.SidecarKey) error {
					f.index++
					sc.RaftAppliedIndex = f.index
					return encryption.WriteSidecar(f.path, sc)
				}
			}
			sc.Keys["3"] = key
			if corruption == "purpose" {
				raw, marshalErr := json.Marshal(sc)
				require.NoError(t, marshalErr)
				require.NoError(t, os.WriteFile(f.path, raw, 0o600), "deliberate on-disk corruption bypasses the production writer's validation")
			} else {
				require.NoError(t, encryption.WriteSidecar(f.path, sc))
			}
			result, err := r.Reconcile(t.Context())
			require.Error(t, err)
			require.Equal(t, bootstrapRotationPending, result.Outcome)
			if corruption == "projection_change" {
				require.ErrorIs(t, err, encryptionwiring.ErrBootstrapEncryptionProjectionPending)
				require.True(t, bootstrapRotationTransient(err))
			}
		})
	}
}

func TestBootstrapRotationRecipeUnknownPartialApplyDoesNotStartNextPurpose(t *testing.T) {
	t.Parallel()
	f := newRotationRecipeFixture(t)
	r, err := newBootstrapRotationRecipe(t.Context(), f.cfg)
	require.NoError(t, err)
	unknown := errors.New("result lost during registry persistence")
	f.admin = func(_ context.Context, req *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
		f.apply(req)
		delete(f.registry, string(encryption.RegistryKey(req.NewDekId, encryption.NodeID16(f.cfg.NodeID))))
		return nil, unknown
	}
	result, err := r.Run(t.Context())
	require.ErrorIs(t, err, unknown)
	require.Equal(t, bootstrapRotationPending, result.Outcome)
	require.Len(t, f.calls, 1)
	f.admin = nil
	_, err = r.Run(t.Context())
	require.ErrorContains(t, err, "missing registry")
	require.Len(t, f.calls, 1, "even a retry cannot start another purpose before resolving this one")
}

func TestBootstrapRotationRecipeRequiresBarrierAfterLostReply(t *testing.T) {
	t.Parallel()
	f := newRotationRecipeFixture(t)
	r, err := newBootstrapRotationRecipe(t.Context(), f.cfg)
	require.NoError(t, err)
	unknown := errors.New("lost reply")
	f.admin = func(_ context.Context, req *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
		f.apply(req)
		f.barrierErr = errors.New("new leader unavailable")
		return nil, unknown
	}
	result, err := r.Run(t.Context())
	require.ErrorIs(t, err, unknown)
	require.ErrorIs(t, err, f.barrierErr)
	require.False(t, result.Complete)
	require.Len(t, f.calls, 1)
	f.leader, f.barrierErr = false, nil
	result, err = r.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, bootstrapRotationReconciled, result.Outcome)
	require.False(t, result.Complete)
	require.Len(t, f.calls, 1)
}

func TestBootstrapRotationRecipeGrantCopiesAndCancelledInstallation(t *testing.T) {
	t.Parallel()
	f := newRotationRecipeFixture(t)
	r, err := newBootstrapRotationRecipe(t.Context(), f.cfg)
	require.NoError(t, err)
	requests := r.Requests()
	digest := r.Digest()
	requests[0].NewDekId++
	requests[0].WrappedNewDek[0]++
	require.NotEqual(t, requests[0].NewDekId, r.Requests()[0].NewDekId)
	require.NotEqual(t, requests[0].WrappedNewDek, r.Requests()[0].WrappedNewDek)
	require.Equal(t, digest, r.Digest())
	ctx, cancel := context.WithCancel(t.Context())
	r.config.InstallAndVerify = func(context.Context, uint32, encryption.SidecarKey) error { cancel(); return nil }
	result, err := r.Run(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, result.Complete)
	require.Len(t, f.calls, 1)
}

func TestBootstrapRotationRecipePlanningIsFencedAndLeaderOnly(t *testing.T) {
	t.Parallel()
	f := newRotationRecipeFixture(t)
	f.leader = false
	_, err := newBootstrapRotationRecipe(t.Context(), f.cfg)
	require.ErrorContains(t, err, "only a leader")
	require.Zero(t, f.barriers)
	f.leader = true
	f.barrierErr = errors.New("no read quorum")
	_, err = newBootstrapRotationRecipe(t.Context(), f.cfg)
	require.ErrorIs(t, err, f.barrierErr)
	require.Empty(t, f.calls)
}

func TestBootstrapRotationRecipeEveryAppliedKeyRetiresRun(t *testing.T) {
	t.Parallel()
	f := newRotationRecipeFixture(t)
	r, err := newBootstrapRotationRecipe(t.Context(), f.cfg)
	require.NoError(t, err)
	for i := range 2 {
		ctx, cancel := context.WithCancel(t.Context())
		f.admin = func(owned context.Context, req *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
			require.Same(t, ctx, owned)
			f.apply(req)
			cancel()
			return nil, context.Canceled
		}
		result, err := r.Run(ctx)
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, result.Complete)
		require.Len(t, f.calls, i+1)
		require.True(t, proto.Equal(f.calls[i], r.Requests()[i]))
	}
	result, err := r.Run(t.Context())
	require.NoError(t, err)
	require.Equal(t, bootstrapRotationReconciled, result.Outcome)
	require.True(t, result.Complete, "fresh generation observes exact completion without another rotation")
	require.Len(t, f.calls, 2)
}

func TestBootstrapRotationRecipeAnotherLeaderUsesSamePlannedID(t *testing.T) {
	t.Parallel()
	f := newRotationRecipeFixture(t)
	r, err := newBootstrapRotationRecipe(t.Context(), f.cfg)
	require.NoError(t, err)
	remote := cloneBootstrapRotationRequest(r.Requests()[0])
	remote.WrappedNewDek = bytes.Repeat([]byte{9}, 40)
	f.apply(remote)
	f.leader = false
	result, err := r.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, bootstrapRotationSuperseded, result.Outcome)
	require.False(t, result.Complete, "current material is verified but is not our recipe's completion")
	require.Empty(t, f.calls)
}

func TestBootstrapRotationRecipeDriftAndNotLeaderAreTransient(t *testing.T) {
	t.Parallel()
	f := newRotationRecipeFixture(t)
	r, err := newBootstrapRotationRecipe(t.Context(), f.cfg)
	require.NoError(t, err)
	r.config.Barrier = func(context.Context) (uint64, error) { return f.index - 1, nil }
	_, err = r.Run(t.Context())
	require.ErrorIs(t, err, encryptionwiring.ErrBootstrapEncryptionProjectionPending)
	require.True(t, bootstrapRotationTransient(err))
	r.config.Barrier = f.cfg.Barrier
	f.leader = false
	_, err = r.Run(t.Context())
	require.ErrorIs(t, err, raftengine.ErrNotLeader)
	require.True(t, bootstrapRotationTransient(err))
	require.Empty(t, f.calls)
}

func TestBootstrapRotationRecipeUnknownCannotHideInvalidEvidence(t *testing.T) {
	t.Parallel()
	f := newRotationRecipeFixture(t)
	r, err := newBootstrapRotationRecipe(t.Context(), f.cfg)
	require.NoError(t, err)
	f.admin = func(_ context.Context, req *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
		f.apply(req)
		f.register(req.NewDekId, f.cfg.StorageEpoch+1)
		return nil, kv.ErrTransactionOutcomeUnknown
	}
	_, err = r.Run(t.Context())
	require.ErrorIs(t, err, encryption.ErrLocalEpochRollback)
	require.False(t, bootstrapRotationTransient(err), "unknown original result must not mask a proven rollback")
}
