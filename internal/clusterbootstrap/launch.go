package clusterbootstrap

import (
	"context"
	stderrors "errors"
	"fmt"
	"path/filepath"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/encryptionwiring"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

type GroupBootstrapSpec struct {
	Config *BootstrapPeerConfig
	Files  *raftbootstrap.Store
	Intent raftbootstrap.Intent
	Dir    string
	Domain *BootstrapDomain
}

func ValidateProtectedBootstrapDomain(raftID, baseDir string, groups []noderuntime.GroupSpec, multi bool, peerConfigDir string) error {
	if peerConfigDir == "" {
		return nil
	}
	var cluster raftbootstrap.UUID
	for _, group := range groups {
		spec, err := LoadGroupBootstrapSpec(raftID, group, noderuntime.GroupDataDir(baseDir, raftID, group.ID, multi), peerConfigDir)
		if err != nil {
			return err
		}
		if cluster != (raftbootstrap.UUID{}) && cluster != spec.Intent.Identity.Cluster {
			return errors.New("protected groups must belong to one configured cluster identity")
		}
		cluster = spec.Intent.Identity.Cluster
	}
	return nil
}

func LoadProtectedBootstrapDomain(raftID, baseDir string, groups []noderuntime.GroupSpec, multi bool, bootstrapCfg noderuntime.BootstrapConfig, encWiring ...encryptionwiring.WriteWiring) (map[uint64]*GroupBootstrapSpec, *BootstrapDomain, error) {
	if bootstrapCfg.PeerConfigDir == "" {
		return nil, nil, nil
	}
	if err := ValidateProtectedBootstrapDomain(raftID, baseDir, groups, multi, bootstrapCfg.PeerConfigDir); err != nil {
		return nil, nil, err
	}
	specs := make(map[uint64]*GroupBootstrapSpec, len(groups))
	var cluster raftbootstrap.UUID
	for _, group := range groups {
		spec, err := LoadGroupBootstrapSpec(raftID, group, noderuntime.GroupDataDir(baseDir, raftID, group.ID, multi), bootstrapCfg.PeerConfigDir)
		if err != nil {
			return nil, nil, err
		}
		specs[group.ID], cluster = spec, spec.Intent.Identity.Cluster
	}
	domain, err := NewBootstrapDomain(cluster, groups, bootstrapCfg.Encryption, encWiring...)
	if err != nil {
		return nil, nil, err
	}
	if err := attachBootstrapDomain(specs, domain, bootstrapCfg.ServiceTokenFiles); err != nil {
		domain.Admission.Close()
		return nil, nil, err
	}
	return specs, domain, nil
}

func attachBootstrapDomain(specs map[uint64]*GroupBootstrapSpec, domain *BootstrapDomain, serviceTokenFiles []string) error {
	for _, spec := range specs {
		if spec.Config.StartupToken == "" || (domain.StartupToken != "" && domain.StartupToken != spec.Config.StartupToken) {
			return errors.New("all protected groups require the same dedicated startup token")
		}
		domain.StartupToken = spec.Config.StartupToken
		spec.Domain = domain
	}
	return ValidateBootstrapStartupToken(domain.StartupToken, serviceTokenFiles)
}

// LoadGroupBootstrapSpec loads group's prepared peer configuration from
// peerConfigDir (--raftBootstrapPeerConfigDir); empty means the group is not
// protected.
func LoadGroupBootstrapSpec(raftID string, group noderuntime.GroupSpec, dir, peerConfigDir string) (*GroupBootstrapSpec, error) {
	if peerConfigDir == "" {
		return nil, nil
	}
	config, err := LoadBootstrapPeerConfig(filepath.Join(peerConfigDir, fmt.Sprintf("group-%d.json", group.ID)))
	if err != nil {
		return nil, err
	}
	if config.Local.Member != etcdraftengine.DeriveNodeID(raftID) || config.Local.Address != group.Address {
		return nil, errors.New("protected local identity does not match configured raft listener")
	}
	files := raftbootstrap.NewStore(dir)
	identity := raftbootstrap.Identity{Cluster: config.Local.Cluster, Group: group.ID}
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
	return &GroupBootstrapSpec{Config: config, Files: files, Intent: *inspection.Intent, Dir: dir}, nil
}

type protectedGroupFactory struct {
	raftengine.Factory
	spec *GroupBootstrapSpec
}

func BootstrapFactoryForGroup(base raftengine.Factory, spec *GroupBootstrapSpec, st store.MVCCStore) (raftengine.Factory, error) {
	if spec == nil {
		return base, nil
	}
	native, ok := base.(*etcdraftengine.Factory)
	if !ok {
		return nil, errors.New("protected recovery requires the native etcd factory")
	}
	setup := newBootstrapStartupForGroup(spec.Config, spec.Files, spec.Intent, st, BootstrapPublicGRPCMethods(), spec.Domain)
	return &protectedGroupFactory{Factory: native.WithBootstrapStartup(setup), spec: spec}, nil
}

func (f *protectedGroupFactory) PreflightRaftDataDir(dir string, engineType noderuntime.RaftEngineType) error {
	if dir != f.spec.Dir || engineType != noderuntime.RaftEngineEtcd {
		return errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	inspection, err := f.spec.Files.Inspect(raftbootstrap.Evidence{Identity: &f.spec.Intent.Identity})
	if err != nil {
		return errors.WithStack(err)
	}
	if inspection.Marker != raftbootstrap.MarkerRequired || inspection.Intent == nil {
		return errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	want, err := raftbootstrap.MarshalIntent(f.spec.Intent)
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

func ProtectedRuntimesBlocked(runtimes []*GroupRuntime) bool {
	for _, runtime := range runtimes {
		if runtime != nil && runtime.PublicReady != nil && !runtime.PublicReady() {
			return true
		}
	}
	return false
}

// Startup may lose its catalog ticket to ordinary native recovery while doing
// I/O. Discard that observation, leaving DataReady closed until the readiness
// reconciler independently reloads it. Never fall back to an unguarded Save.
func LoadProtectedStartupCatalog(ctx context.Context, runtimes []*GroupRuntime, id uint64, catalog *distribution.CatalogStore, engine *distribution.Engine) (bool, error) {
	handled, err := loadProtectedCatalog(ctx, runtimes, id, catalog, engine)
	if !handled || err == nil {
		return handled, err
	}
	return true, DeferBootstrapCatalogObservation(ctx, BootstrapDomainForRuntimes(runtimes), id, err)
}

func DeferBootstrapCatalogObservation(ctx context.Context, domain *BootstrapDomain, id uint64, err error) error {
	if !BootstrapCatalogObservationStale(err) || domain == nil {
		return err
	}
	if ctx.Err() != nil {
		return errors.Join(err, ctx.Err())
	}
	if _, currentErr := domain.Admission.Ticket(id); currentErr != nil {
		return errors.Join(err, currentErr)
	}
	return nil
}

func BootstrapCatalogObservationStale(err error) bool {
	return stderrors.Is(err, raftadmission.ErrStale) || stderrors.Is(err, distribution.ErrEngineSnapshotVersionStale)
}

func loadProtectedCatalog(ctx context.Context, runtimes []*GroupRuntime, id uint64, catalog *distribution.CatalogStore, engine *distribution.Engine) (bool, error) {
	for _, runtime := range runtimes {
		if runtime == nil || runtime.Spec.ID != id || runtime.PublicReady == nil {
			continue
		}
		return true, loadProtectedRuntimeCatalog(ctx, runtime, catalog, engine)
	}
	return false, nil
}

func loadProtectedRuntimeCatalog(ctx context.Context, runtime *GroupRuntime, catalog *distribution.CatalogStore, engine *distribution.Engine) error {
	if runtime.BootstrapDomain == nil {
		return errors.New("protected catalog lacks the process-wide admission domain")
	}
	ticket, err := runtime.BootstrapDomain.Admission.Ticket(runtime.Spec.ID)
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
	return runtime.BootstrapDomain.CatalogLoaded(ticket)
}
