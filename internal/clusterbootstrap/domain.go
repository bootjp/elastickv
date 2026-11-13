package clusterbootstrap

import (
	"context"
	"sync"
	"time"

	"github.com/bootjp/elastickv/distribution"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/encryptionwiring"
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
type BootstrapDomain struct {
	Admission                *raftadmission.Arbiter
	mu                       sync.Mutex
	Owners                   map[uint64]*peersession.Owner
	StartupToken             string
	activations              map[uint64][]byte
	cluster                  raftbootstrap.UUID
	Encryption               *bootstrapEncryptionDomain
	encryptionRequest        noderuntime.BootstrapEncryptionRequest
	protectedRotationHandler BootstrapRotationRPCHandler
}

const BootstrapReadinessRetryInterval = 25 * time.Millisecond

func ConfigureBootstrapOperations(ctx context.Context, runtimes []*GroupRuntime, shardStore *kv.ShardStore, cleanup *internalutil.CleanupStack) (*BootstrapDomain, []kv.LockResolverOption) {
	domain := BootstrapDomainForRuntimes(runtimes)
	if domain == nil {
		return nil, nil
	}
	cleanup.Add(domain.Admission.Close)
	shardStore.SetOperationAdmission(domain.OperationAdmission)
	return domain, []kv.LockResolverOption{kv.WithLockResolverContext(ctx), kv.WithLockResolverOperationAdmission(domain.OperationAdmission)}
}

func InstallBootstrapCoordinator(domain *BootstrapDomain, coordinate *kv.ShardedCoordinator, cleanup *internalutil.CleanupStack) {
	if domain != nil {
		coordinate.WithOperationAdmission(domain.OperationAdmission)
	}
	cleanup.Add(func() { _ = coordinate.Close() })
}

func (d *BootstrapDomain) OperationAdmission(ctx context.Context, group uint64, purpose kv.AdmissionPurpose) (context.Context, func(error) error, error) {
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
	owned, finish, err := d.Admission.Enter(ctx, kind, group)
	return owned, finish, errors.WithStack(err)
}

func BootstrapDomainForRuntimes(runtimes []*GroupRuntime) *BootstrapDomain {
	for _, runtime := range runtimes {
		if runtime != nil && runtime.BootstrapDomain != nil {
			return runtime.BootstrapDomain
		}
	}
	return nil
}

func NewBootstrapDomain(cluster raftbootstrap.UUID, groups []noderuntime.GroupSpec, request noderuntime.BootstrapEncryptionRequest, encWiring ...encryptionwiring.WriteWiring) (*BootstrapDomain, error) {
	enc, err := newBootstrapEncryptionDomain(groups, request, encWiring)
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, len(groups))
	for _, group := range groups {
		ids = append(ids, group.ID)
	}
	d := &BootstrapDomain{cluster: cluster, Encryption: enc, encryptionRequest: request, Owners: make(map[uint64]*peersession.Owner), activations: make(map[uint64][]byte)}
	cfg := raftadmission.Config{
		Cluster: cluster, Groups: ids, DefaultGroup: noderuntime.DefaultGroupID(groups),
		RotationRequested:    request.RotateOnStartup,
		ValidatePayload:      BootstrapPayloadValidator(cluster),
		EncryptionConfigured: enc != nil,
	}
	if enc != nil {
		cfg.InspectPayload = d.InspectPayload
	}
	a, err := raftadmission.New(cfg)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	d.Admission = a
	if enc != nil {
		enc.wiring.Cache.SetBeforeEncryptionChange(a.BeginEncryptionMutation)
		enc.wiring.RaftEnvelope.SetBeforeEncryptionChange(a.BeginEncryptionMutation)
	}
	return d, nil
}

func (d *BootstrapDomain) register(id uint64, owner *peersession.Owner) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if owner == nil || d.Owners[id] != nil {
		return errors.New("duplicate or missing protected session owner")
	}
	d.Owners[id] = owner
	return nil
}

func (d *BootstrapDomain) Refresh() error {
	d.mu.Lock()
	owners := make(map[uint64]*peersession.Owner, len(d.Owners))
	for id, owner := range d.Owners {
		owners[id] = owner
	}
	d.mu.Unlock()
	for id, owner := range owners {
		projection, ok := owner.GroupProjection(id)
		if !ok || projection.Stage == peersession.Reconciling {
			continue
		}
		if err := d.Admission.PublishStage(id, func(stage peersession.Stage, generation uint64) (func(), error) {
			return owner.UpdateStageProjection(id, generation-1, generation, stage)
		}); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (d *BootstrapDomain) CatalogLoaded(ticket raftadmission.RecoveryTicket) error {
	if err := d.Admission.MarkCatalogLoaded(ticket); err != nil {
		return errors.WithStack(err)
	}
	return d.Refresh()
}

// Native callbacks never wait for catalog I/O or retired children. This owner
// retries publication after their joins and reloads the local applied catalog
// against a fresh native ticket when the default group's authority changes.
func StartBootstrapReadinessReconciler(ctx context.Context, eg *errgroup.Group, runtimes []*GroupRuntime, catalog *distribution.CatalogStore, engine *distribution.Engine) {
	domain := BootstrapDomainForRuntimes(runtimes)
	if domain == nil {
		return
	}
	eg.Go(func() error {
		ticker := time.NewTicker(BootstrapReadinessRetryInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				if err := ReconcileBootstrapReadiness(ctx, domain, runtimes, catalog, engine); err != nil {
					domain.Admission.LatchFatal(err)
					return err
				}
			}
		}
	})
}

func ReconcileBootstrapReadiness(ctx context.Context, domain *BootstrapDomain, runtimes []*GroupRuntime, catalog *distribution.CatalogStore, engine *distribution.Engine) error {
	id, err := distribution.CatalogGroupID(engine)
	if err != nil {
		return errors.WithStack(err)
	}
	if domain.Admission.Stage(id) == peersession.ControlReady {
		if _, err := loadProtectedCatalog(ctx, runtimes, id, catalog, engine); err != nil {
			if err := DeferBootstrapCatalogObservation(ctx, domain, id, err); err != nil {
				return err
			}
		}
	}
	return domain.Refresh()
}
