package main

import (
	"context"
	"sync"
	"time"

	"github.com/bootjp/elastickv/distribution"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

// One domain owns all process-local group authority. Owner pointers are copied
// before entering A; no path acquires this registry mutex while holding A.
type bootstrapDomain struct {
	admission                *raftadmission.Arbiter
	mu                       sync.Mutex
	owners                   map[uint64]*peersession.Owner
	startupToken             string
	activations              map[uint64][]byte
	cluster                  raftbootstrap.UUID
	encryption               *bootstrapEncryptionDomain
	encryptionRequest        noderuntime.BootstrapEncryptionRequest
	protectedRotationHandler bootstrapRotationRPCHandler
}

const bootstrapReadinessRetryInterval = 25 * time.Millisecond

func configureBootstrapOperations(ctx context.Context, runtimes []*raftGroupRuntime, shardStore *kv.ShardStore, cleanup *internalutil.CleanupStack) (*bootstrapDomain, []kv.LockResolverOption) {
	domain := bootstrapDomainForRuntimes(runtimes)
	if domain == nil {
		return nil, nil
	}
	cleanup.Add(domain.admission.Close)
	shardStore.SetOperationAdmission(domain.operationAdmission)
	return domain, []kv.LockResolverOption{kv.WithLockResolverContext(ctx), kv.WithLockResolverOperationAdmission(domain.operationAdmission)}
}

func installBootstrapCoordinator(domain *bootstrapDomain, coordinate *kv.ShardedCoordinator, cleanup *internalutil.CleanupStack) {
	if domain != nil {
		coordinate.WithOperationAdmission(domain.operationAdmission)
	}
	cleanup.Add(func() { _ = coordinate.Close() })
}

func (d *bootstrapDomain) operationAdmission(ctx context.Context, group uint64, purpose kv.AdmissionPurpose) (context.Context, func(error) error, error) {
	kind := raftadmission.Data
	switch purpose {
	case kv.AdmissionData:
	case kv.AdmissionAllocator:
		kind = raftadmission.Allocator
	case kv.AdmissionHLC:
		kind = raftadmission.HLC
	case kv.AdmissionMaintenance:
		kind = raftadmission.Maintenance
	case kv.AdmissionControl:
		kind = raftadmission.Control
	default:
		return nil, nil, raftadmission.ErrClosed
	}
	owned, finish, err := d.admission.Enter(ctx, kind, group)
	return owned, finish, errors.WithStack(err)
}

func bootstrapDomainForRuntimes(runtimes []*raftGroupRuntime) *bootstrapDomain {
	for _, runtime := range runtimes {
		if runtime != nil && runtime.bootstrapDomain != nil {
			return runtime.bootstrapDomain
		}
	}
	return nil
}

func newBootstrapDomain(cluster raftbootstrap.UUID, groups []noderuntime.GroupSpec, request noderuntime.BootstrapEncryptionRequest, encWiring ...encryptionWriteWiring) (*bootstrapDomain, error) {
	enc, err := newBootstrapEncryptionDomain(groups, request, encWiring)
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, len(groups))
	for _, group := range groups {
		ids = append(ids, group.ID)
	}
	d := &bootstrapDomain{cluster: cluster, encryption: enc, encryptionRequest: request, owners: make(map[uint64]*peersession.Owner), activations: make(map[uint64][]byte)}
	cfg := raftadmission.Config{
		Cluster: cluster, Groups: ids, DefaultGroup: noderuntime.DefaultGroupID(groups),
		RotationRequested:    request.RotateOnStartup,
		ValidatePayload:      bootstrapPayloadValidator(cluster),
		EncryptionConfigured: enc != nil,
	}
	if enc != nil {
		cfg.InspectPayload = d.inspectPayload
	}
	a, err := raftadmission.New(cfg)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	d.admission = a
	if enc != nil {
		enc.wiring.cache.SetBeforeEncryptionChange(a.BeginEncryptionMutation)
		enc.wiring.raftEnvelope.setBeforeEncryptionChange(a.BeginEncryptionMutation)
	}
	return d, nil
}

func (d *bootstrapDomain) register(id uint64, owner *peersession.Owner) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if owner == nil || d.owners[id] != nil {
		return errors.New("duplicate or missing protected session owner")
	}
	d.owners[id] = owner
	return nil
}

func (d *bootstrapDomain) refresh() error {
	d.mu.Lock()
	owners := make(map[uint64]*peersession.Owner, len(d.owners))
	for id, owner := range d.owners {
		owners[id] = owner
	}
	d.mu.Unlock()
	for id, owner := range owners {
		projection, ok := owner.GroupProjection(id)
		if !ok || projection.Stage == peersession.Reconciling {
			continue
		}
		if err := d.admission.PublishStage(id, func(stage peersession.Stage, generation uint64) (func(), error) {
			return owner.UpdateStageProjection(id, generation-1, generation, stage)
		}); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (d *bootstrapDomain) catalogLoaded(ticket raftadmission.RecoveryTicket) error {
	if err := d.admission.MarkCatalogLoaded(ticket); err != nil {
		return errors.WithStack(err)
	}
	return d.refresh()
}

// Native callbacks never wait for catalog I/O or retired children. This owner
// retries publication after their joins and reloads the local applied catalog
// against a fresh native ticket when the default group's authority changes.
func startBootstrapReadinessReconciler(ctx context.Context, eg *errgroup.Group, runtimes []*raftGroupRuntime, catalog *distribution.CatalogStore, engine *distribution.Engine) {
	domain := bootstrapDomainForRuntimes(runtimes)
	if domain == nil {
		return
	}
	eg.Go(func() error {
		ticker := time.NewTicker(bootstrapReadinessRetryInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				if err := reconcileBootstrapReadiness(ctx, domain, runtimes, catalog, engine); err != nil {
					domain.admission.LatchFatal(err)
					return err
				}
			}
		}
	})
}

func reconcileBootstrapReadiness(ctx context.Context, domain *bootstrapDomain, runtimes []*raftGroupRuntime, catalog *distribution.CatalogStore, engine *distribution.Engine) error {
	id, err := distributionCatalogGroupID(engine)
	if err != nil {
		return err
	}
	if domain.admission.Stage(id) == peersession.ControlReady {
		if _, err := loadProtectedCatalog(ctx, runtimes, id, catalog, engine); err != nil {
			if err := deferBootstrapCatalogObservation(ctx, domain, id, err); err != nil {
				return err
			}
		}
	}
	return domain.refresh()
}
