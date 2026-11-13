package server

import (
	"context"
	"log/slog"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/bootjp/elastickv/internal/encryptionwiring"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

type bootstrapEncryptionStartupConfig struct {
	runtimes     []*clusterbootstrap.GroupRuntime
	groups       map[uint64]*kv.ShardGroup
	defaultGroup uint64
	network      *clusterbootstrap.BootstrapNetwork
	wiring       encryptionwiring.WriteWiring
	sidecarPath  string
	wrapper      kek.Wrapper
	nodeID       uint64
	requested    bool
	eg           *errgroup.Group
	cleanup      *internalutil.CleanupStack
}

func startDistributionWithBootstrapEncryption(in distributionStartupInput, cfg bootstrapEncryptionStartupConfig) (distributionStartup, error) {
	registered, supervisor, err := configureBootstrapEncryptionStartup(in.ctx, cfg)
	if err != nil {
		return distributionStartup{}, err
	}
	in.ctx = registered
	startup, err := startDistributionStartup(in)
	if err != nil {
		return distributionStartup{}, err
	}
	startup.registrationCtx, startup.rotationSupervisor = registered, supervisor
	return startup, nil
}

// Construct real IO under startup exclusivity, before registration watchers.
// The supervisor may retry registration-dependent observations before listeners
// start; Wait is consumed only after the normal Raft listeners have started.
func configureBootstrapEncryptionStartup(ctx context.Context, cfg bootstrapEncryptionStartupConfig) (context.Context, *clusterbootstrap.BootstrapRotationRuntime, error) {
	domain, err := cfg.protectedDomain()
	if err != nil {
		return nil, nil, err
	}
	if domain == nil {
		return ctx, nil, nil
	}
	if err := cfg.validateOwnership(); err != nil {
		return nil, nil, err
	}
	rt := clusterbootstrap.FindDefaultGroupRuntime(cfg.runtimes, cfg.defaultGroup)
	io, err := clusterbootstrap.NewBootstrapEncryptionRuntimeIO(clusterbootstrap.BootstrapEncryptionRuntimeIOConfig{
		DefaultRuntime: rt, Group: cfg.groups[cfg.defaultGroup], Wiring: cfg.wiring,
		SidecarPath: cfg.sidecarPath, Wrapper: cfg.wrapper, Domain: domain, NodeID: cfg.nodeID, RotationRequested: cfg.requested,
	})
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	if !cfg.wiring.Cache.StorageEnvelopeActive() {
		return nil, nil, errors.Wrap(encryptionwiring.ErrBootstrapEncryptionUnsupported, "protected encrypted startup requires an already active storage envelope; inactive-key registration is unsupported")
	}
	registered, err := clusterbootstrap.WithBootstrapRegistration(ctx, cfg.network, cfg.defaultGroup, cfg.nodeID, io.ValidateEpoch)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	barrier, err := bootstrapEncryptionRecoveryBarrier(domain, rt)
	if err != nil {
		return nil, nil, err
	}
	recipeConfig := io.RecipeConfig(barrier, domain.Admission.ValidateResult)
	supervisor, err := clusterbootstrap.NewBootstrapRotationRuntime(clusterbootstrap.BootstrapRotationRuntimeConfig{
		Admission: domain.Admission, Group: cfg.defaultGroup, Requested: cfg.requested,
		Barrier: barrier, Observe: io.Observe,
		Plan: func(ctx context.Context) (*clusterbootstrap.BootstrapRotationRecipe, error) {
			return clusterbootstrap.NewBootstrapRotationRecipe(ctx, recipeConfig)
		},
		PlanExternal: func(ctx context.Context, request *pb.RotateDEKRequest) (*clusterbootstrap.BootstrapRotationRecipe, error) {
			return clusterbootstrap.NewBootstrapExternalRotationRecipe(ctx, recipeConfig, request)
		},
		Authorize: domain.AuthorizeRotationRecipe, Refresh: domain.Refresh,
	})
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	domain.SetProtectedRotationHandler(supervisor.RotateDEK)
	worker, cancel := context.WithCancel(registered)
	done := make(chan struct{})
	cfg.eg.Go(func() error { defer close(done); return supervisor.Run(worker) })
	cfg.cleanup.Add(func() { cancel(); <-done })
	return registered, supervisor, nil
}

func (cfg bootstrapEncryptionStartupConfig) protectedDomain() (*clusterbootstrap.BootstrapDomain, error) {
	domain := clusterbootstrap.BootstrapDomainForRuntimes(cfg.runtimes)
	if domain == nil || cfg.wiring.EncryptionConfigured() {
		return domain, nil
	}
	if cfg.requested || cfg.sidecarPath != "" {
		return nil, errors.New("protected encryption startup lacks actual cipher wiring")
	}
	return nil, nil
}

func (cfg bootstrapEncryptionStartupConfig) validateOwnership() error {
	if cfg.eg == nil || cfg.cleanup == nil || cfg.network == nil {
		return errors.New("protected encryption startup requires owned workers and network")
	}
	return nil
}

type bootstrapRecoveryReader interface {
	RecoveryLinearizableRead(context.Context) (uint64, error)
}

func bootstrapEncryptionRecoveryBarrier(domain *clusterbootstrap.BootstrapDomain, rt *clusterbootstrap.GroupRuntime) (func(context.Context) (uint64, error), error) {
	if domain == nil || domain.Admission == nil || rt == nil {
		return nil, errors.New("protected encryption recovery barrier lacks runtime")
	}
	engine := clusterbootstrap.RuntimeCore(rt).SnapshotEngine()
	reader, ok := engine.(bootstrapRecoveryReader)
	if !ok {
		return nil, errors.New("protected encryption requires native Recovery read barrier")
	}
	return func(ctx context.Context) (uint64, error) {
		owned, finish, err := domain.Admission.Enter(ctx, raftadmission.Recovery, rt.Spec.ID)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		var index uint64
		_, err = clusterbootstrap.RunBootstrapStartupControl(owned, nil, func(ctx context.Context, _ any) (any, error) {
			if clusterbootstrap.RuntimeCore(rt).SnapshotEngine() != engine {
				return nil, errors.New("protected encryption recovery engine retired")
			}
			var err error
			index, err = reader.RecoveryLinearizableRead(ctx)
			if err == nil && clusterbootstrap.RuntimeCore(rt).SnapshotEngine() != engine {
				return nil, errors.New("protected encryption recovery engine retired")
			}
			return nil, errors.WithStack(err)
		}, finish)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		return index, nil
	}, nil
}

func installBootstrapStartupRotation(supervisor *clusterbootstrap.BootstrapRotationRuntime, ctx context.Context, cfg bootstrapEncryptionStartupConfig) (func(), encryptionwiring.StartupRotationWaiter) {
	if supervisor != nil {
		return func() {}, encryptionwiring.StartupRotationWaiter{WaitFunc: supervisor.Wait, BlockMutator: func() bool { return !supervisor.Config.Admission.LocalPermissionReady() }}
	}
	rt := clusterbootstrap.FindDefaultGroupRuntime(cfg.runtimes, cfg.defaultGroup)
	return encryptionwiring.InstallRotateOnStartup(ctx, cfg.requested, clusterbootstrap.RuntimeCore(rt), postCutoverProposerForRuntime(rt, cfg.groups), cfg.sidecarPath,
		cfg.wrapper, cfg.wiring.RaftEnvelope, cfg.nodeID, cfg.wiring.Epoch, cfg.wiring.RaftEpoch, slog.Default())
}
