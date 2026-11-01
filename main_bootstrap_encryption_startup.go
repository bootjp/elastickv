package main

import (
	"context"
	"log/slog"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

type bootstrapEncryptionStartupConfig struct {
	runtimes     []*raftGroupRuntime
	groups       map[uint64]*kv.ShardGroup
	defaultGroup uint64
	network      *bootstrapNetwork
	wiring       encryptionWriteWiring
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
func configureBootstrapEncryptionStartup(ctx context.Context, cfg bootstrapEncryptionStartupConfig) (context.Context, *bootstrapRotationRuntime, error) {
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
	rt := findDefaultGroupRuntime(cfg.runtimes, cfg.defaultGroup)
	io, err := newBootstrapEncryptionRuntimeIO(bootstrapEncryptionRuntimeIOConfig{
		DefaultRuntime: rt, Group: cfg.groups[cfg.defaultGroup], Wiring: cfg.wiring,
		SidecarPath: cfg.sidecarPath, Wrapper: cfg.wrapper, Domain: domain, NodeID: cfg.nodeID, RotationRequested: cfg.requested,
	})
	if err != nil {
		return nil, nil, err
	}
	if !cfg.wiring.cache.StorageEnvelopeActive() {
		return nil, nil, errors.Wrap(errBootstrapEncryptionUnsupported, "protected encrypted startup requires an already active storage envelope; inactive-key registration is unsupported")
	}
	registered, err := withBootstrapRegistration(ctx, cfg.network, cfg.defaultGroup, cfg.nodeID, io.ValidateEpoch)
	if err != nil {
		return nil, nil, err
	}
	barrier, err := bootstrapEncryptionRecoveryBarrier(domain, rt)
	if err != nil {
		return nil, nil, err
	}
	recipeConfig := io.RecipeConfig(barrier, domain.admission.ValidateResult)
	supervisor, err := newBootstrapRotationRuntime(bootstrapRotationRuntimeConfig{
		admission: domain.admission, group: cfg.defaultGroup, requested: cfg.requested,
		barrier: barrier, observe: io.Observe,
		plan: func(ctx context.Context) (*bootstrapRotationRecipe, error) {
			return newBootstrapRotationRecipe(ctx, recipeConfig)
		},
		authorize: domain.authorizeRotationRecipe, refresh: domain.refresh,
	})
	if err != nil {
		return nil, nil, err
	}
	worker, cancel := context.WithCancel(registered)
	done := make(chan struct{})
	cfg.eg.Go(func() error { defer close(done); return supervisor.Run(worker) })
	cfg.cleanup.Add(func() { cancel(); <-done })
	return registered, supervisor, nil
}

func (cfg bootstrapEncryptionStartupConfig) protectedDomain() (*bootstrapDomain, error) {
	domain := bootstrapDomainForRuntimes(cfg.runtimes)
	if domain == nil || cfg.wiring.encryptionConfigured() {
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

func bootstrapEncryptionRecoveryBarrier(domain *bootstrapDomain, rt *raftGroupRuntime) (func(context.Context) (uint64, error), error) {
	if domain == nil || domain.admission == nil || rt == nil {
		return nil, errors.New("protected encryption recovery barrier lacks runtime")
	}
	engine := rt.snapshotEngine()
	reader, ok := engine.(bootstrapRecoveryReader)
	if !ok {
		return nil, errors.New("protected encryption requires native Recovery read barrier")
	}
	return func(ctx context.Context) (uint64, error) {
		owned, finish, err := domain.admission.Enter(ctx, raftadmission.Recovery, rt.spec.id)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		var index uint64
		_, err = runBootstrapStartupControl(owned, nil, func(ctx context.Context, _ any) (any, error) {
			if rt.snapshotEngine() != engine {
				return nil, errors.New("protected encryption recovery engine retired")
			}
			var err error
			index, err = reader.RecoveryLinearizableRead(ctx)
			if err == nil && rt.snapshotEngine() != engine {
				return nil, errors.New("protected encryption recovery engine retired")
			}
			return nil, errors.WithStack(err)
		}, finish)
		if err != nil {
			return 0, err
		}
		return index, nil
	}, nil
}

func installBootstrapStartupRotation(supervisor *bootstrapRotationRuntime, ctx context.Context, cfg bootstrapEncryptionStartupConfig) (func(), startupRotationWaiter) {
	if supervisor != nil {
		return func() {}, startupRotationWaiter{wait: supervisor.Wait, blockMutator: func() bool { return !supervisor.config.admission.LocalPermissionReady() }}
	}
	rt := findDefaultGroupRuntime(cfg.runtimes, cfg.defaultGroup)
	return installEncryptionRotateOnStartup(ctx, cfg.requested, rt, postCutoverProposerForRuntime(rt, cfg.groups), cfg.sidecarPath,
		cfg.wrapper, cfg.wiring.raftEnvelope, cfg.nodeID, cfg.wiring.epoch, cfg.wiring.raftEpoch, slog.Default())
}
