package main

import (
	"context"
	stderrors "errors"
	"flag"
	"fmt"
	"path/filepath"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// This opt-in requires explicit accepted-baseline preparation for every group.
var raftBootstrapPeerConfigDir = flag.String("raftBootstrapPeerConfigDir", "", "Directory of prepared protected Raft group peer configurations")

type groupBootstrapSpec struct {
	config *bootstrapPeerConfig
	files  *raftbootstrap.Store
	intent raftbootstrap.Intent
	dir    string
	domain *bootstrapDomain
}

func validateProtectedBootstrapDomain(raftID, baseDir string, groups []groupSpec, multi bool) error {
	if *raftBootstrapPeerConfigDir == "" {
		return nil
	}
	var cluster raftbootstrap.UUID
	for _, group := range groups {
		spec, err := loadGroupBootstrapSpec(raftID, group, groupDataDir(baseDir, raftID, group.id, multi))
		if err != nil {
			return err
		}
		if cluster != (raftbootstrap.UUID{}) && cluster != spec.intent.Identity.Cluster {
			return errors.New("protected groups must belong to one configured cluster identity")
		}
		cluster = spec.intent.Identity.Cluster
	}
	return nil
}

func loadProtectedBootstrapDomain(raftID, baseDir string, groups []groupSpec, multi bool, bootstrapCfg raftBootstrapConfig, encWiring ...encryptionWriteWiring) (map[uint64]*groupBootstrapSpec, *bootstrapDomain, error) {
	if *raftBootstrapPeerConfigDir == "" {
		return nil, nil, nil
	}
	if err := validateProtectedBootstrapDomain(raftID, baseDir, groups, multi); err != nil {
		return nil, nil, err
	}
	specs := make(map[uint64]*groupBootstrapSpec, len(groups))
	var cluster raftbootstrap.UUID
	for _, group := range groups {
		spec, err := loadGroupBootstrapSpec(raftID, group, groupDataDir(baseDir, raftID, group.id, multi))
		if err != nil {
			return nil, nil, err
		}
		specs[group.id], cluster = spec, spec.intent.Identity.Cluster
	}
	domain, err := newBootstrapDomain(cluster, groups, bootstrapCfg.encryption, encWiring...)
	if err != nil {
		return nil, nil, err
	}
	if err := attachBootstrapDomain(specs, domain, bootstrapCfg.serviceTokenFiles); err != nil {
		domain.admission.Close()
		return nil, nil, err
	}
	return specs, domain, nil
}

func attachBootstrapDomain(specs map[uint64]*groupBootstrapSpec, domain *bootstrapDomain, serviceTokenFiles []string) error {
	for _, spec := range specs {
		if spec.config.startupToken == "" || (domain.startupToken != "" && domain.startupToken != spec.config.startupToken) {
			return errors.New("all protected groups require the same dedicated startup token")
		}
		domain.startupToken = spec.config.startupToken
		spec.domain = domain
	}
	return validateBootstrapStartupToken(domain.startupToken, serviceTokenFiles)
}

func (b shardGroupBuilder) bootstrapSpecForGroup(id uint64) (*groupBootstrapSpec, error) {
	spec := b.bootstrapSpecs[id]
	if *raftBootstrapPeerConfigDir != "" && (spec == nil || spec.domain == nil) {
		return nil, errors.New("protected group missing the process-wide bootstrap domain")
	}
	return spec, nil
}

func loadGroupBootstrapSpec(raftID string, group groupSpec, dir string) (*groupBootstrapSpec, error) {
	if *raftBootstrapPeerConfigDir == "" {
		return nil, nil
	}
	config, err := loadBootstrapPeerConfig(filepath.Join(*raftBootstrapPeerConfigDir, fmt.Sprintf("group-%d.json", group.id)))
	if err != nil {
		return nil, err
	}
	if config.local.Member != etcdraftengine.DeriveNodeID(raftID) || config.local.Address != group.address {
		return nil, errors.New("protected local identity does not match configured raft listener")
	}
	files := raftbootstrap.NewStore(dir)
	identity := raftbootstrap.Identity{Cluster: config.local.Cluster, Group: group.id}
	inspection, err := files.Inspect(raftbootstrap.Evidence{Identity: &identity})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if inspection.Marker != raftbootstrap.MarkerRequired || inspection.Intent == nil {
		return nil, errors.New("protected recovery requires a durable intent and exact required marker; legacy activation needs explicit offline preparation")
	}
	if inspection.Intent.Floor.ActivationIndex == 0 && inspection.Intent.BaselineKind != raftbootstrap.Accepted {
		return nil, errors.New("initial protected recovery requires the explicit accepted-baseline rollout")
	}
	return &groupBootstrapSpec{config: config, files: files, intent: *inspection.Intent, dir: dir}, nil
}

type protectedGroupFactory struct {
	raftengine.Factory
	spec *groupBootstrapSpec
}

func bootstrapFactoryForGroup(base raftengine.Factory, spec *groupBootstrapSpec, st store.MVCCStore) (raftengine.Factory, error) {
	if spec == nil {
		return base, nil
	}
	native, ok := base.(*etcdraftengine.Factory)
	if !ok {
		return nil, errors.New("protected recovery requires the native etcd factory")
	}
	setup := newBootstrapStartupForGroup(spec.config, spec.files, spec.intent, st, bootstrapPublicGRPCMethods(), spec.domain)
	return &protectedGroupFactory{Factory: native.WithBootstrapStartup(setup), spec: spec}, nil
}

func (f *protectedGroupFactory) PreflightRaftDataDir(dir string, engineType raftEngineType) error {
	if dir != f.spec.dir || engineType != raftEngineEtcd {
		return errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	inspection, err := f.spec.files.Inspect(raftbootstrap.Evidence{Identity: &f.spec.intent.Identity})
	if err != nil {
		return errors.WithStack(err)
	}
	if inspection.Marker != raftbootstrap.MarkerRequired || inspection.Intent == nil {
		return errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	want, err := raftbootstrap.MarshalIntent(f.spec.intent)
	if err != nil {
		return errors.WithStack(err)
	}
	got, err := raftbootstrap.MarshalIntent(*inspection.Intent)
	if err != nil {
		return errors.WithStack(err)
	}
	if string(want) != string(got) {
		return errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	return nil
}

func protectedRuntimesBlocked(runtimes []*raftGroupRuntime) bool {
	for _, runtime := range runtimes {
		if runtime != nil && runtime.publicReady != nil && !runtime.publicReady() {
			return true
		}
	}
	return false
}

// Startup may lose its catalog ticket to ordinary native recovery while doing
// I/O. Discard that observation, leaving DataReady closed until the readiness
// reconciler independently reloads it. Never fall back to an unguarded Save.
func loadProtectedStartupCatalog(ctx context.Context, runtimes []*raftGroupRuntime, id uint64, catalog *distribution.CatalogStore, engine *distribution.Engine) (bool, error) {
	handled, err := loadProtectedCatalog(ctx, runtimes, id, catalog, engine)
	if !handled || err == nil {
		return handled, err
	}
	return true, deferBootstrapCatalogObservation(ctx, bootstrapDomainForRuntimes(runtimes), id, err)
}

func deferBootstrapCatalogObservation(ctx context.Context, domain *bootstrapDomain, id uint64, err error) error {
	if !bootstrapCatalogObservationStale(err) || domain == nil {
		return err
	}
	if ctx.Err() != nil {
		return errors.Join(err, ctx.Err())
	}
	if _, currentErr := domain.admission.Ticket(id); currentErr != nil {
		return errors.Join(err, currentErr)
	}
	return nil
}

func bootstrapCatalogObservationStale(err error) bool {
	return stderrors.Is(err, raftadmission.ErrStale) || stderrors.Is(err, distribution.ErrEngineSnapshotVersionStale)
}

func loadProtectedCatalog(ctx context.Context, runtimes []*raftGroupRuntime, id uint64, catalog *distribution.CatalogStore, engine *distribution.Engine) (bool, error) {
	for _, runtime := range runtimes {
		if runtime == nil || runtime.spec.id != id || runtime.publicReady == nil {
			continue
		}
		return true, loadProtectedRuntimeCatalog(ctx, runtime, catalog, engine)
	}
	return false, nil
}

func loadProtectedRuntimeCatalog(ctx context.Context, runtime *raftGroupRuntime, catalog *distribution.CatalogStore, engine *distribution.Engine) error {
	if runtime.bootstrapDomain == nil {
		return errors.New("protected catalog lacks the process-wide admission domain")
	}
	ticket, err := runtime.bootstrapDomain.admission.Ticket(runtime.spec.id)
	if err != nil {
		return errors.WithStack(err)
	}
	snapshot, err := catalog.Snapshot(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	if snapshot.Version == 0 || len(snapshot.Routes) == 0 {
		return errors.New("protected catalog is not initialized by a committed guarded entry")
	}
	// ApplySnapshot deliberately treats an equal version as a no-op. Recovery
	// still owes structural verification of every independently loaded image.
	if _, err := distribution.RoutesFromCatalogSnapshot(snapshot); err != nil {
		return errors.WithStack(err)
	}
	if err := engine.ApplySnapshot(snapshot); err != nil {
		return errors.WithStack(err)
	}
	return runtime.bootstrapDomain.catalogLoaded(ticket)
}
