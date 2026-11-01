package main

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// Do not register a CLI flag until maintenance/control attempts share the
// runtime's revocable admission and owned cancellation/join boundaries.
var raftBootstrapPeerConfigDir = new(string)

type groupBootstrapSpec struct {
	config *bootstrapPeerConfig
	files  *raftbootstrap.Store
	intent raftbootstrap.Intent
	dir    string
}

func validateProtectedBootstrapDomain(raftID, baseDir string, groups []groupSpec, multi bool) error {
	if *raftBootstrapPeerConfigDir == "" {
		return nil
	}
	var cluster raftbootstrap.UUID
	for _, group := range groups {
		if group.id == dedicatedTSORaftGroupID {
			return errors.New("protected timestamp-group policy recovery is not implemented; refusing a partially protected domain")
		}
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
	if inspection.Marker != raftbootstrap.MarkerRequired || inspection.Intent == nil || inspection.Intent.Floor.ActivationIndex == 0 {
		return nil, errors.New("protected recovery requires an activated durable intent and exact required marker; legacy/cold activation needs the explicit rollout protocol")
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
	setup := newBootstrapStartupForGroup(spec.config, spec.files, spec.intent, st, nil)
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

func loadProtectedCatalog(ctx context.Context, runtimes []*raftGroupRuntime, id uint64, catalog *distribution.CatalogStore, engine *distribution.Engine) (bool, error) {
	for _, runtime := range runtimes {
		if runtime == nil || runtime.spec.id != id || runtime.publicReady == nil {
			continue
		}
		snapshot, err := catalog.Snapshot(ctx)
		if err != nil {
			return true, errors.WithStack(err)
		}
		if snapshot.Version == 0 || len(snapshot.Routes) == 0 {
			return true, errors.New("protected catalog is not initialized by a committed guarded entry")
		}
		return true, errors.WithStack(engine.ApplySnapshot(snapshot))
	}
	return false, nil
}
