package distribution

import (
	"bytes"
	"context"
	"math"
	"sort"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

var (
	ErrCatalogBootstrapReadTS = errors.New("catalog bootstrap requires a positive, incrementable fenced snapshot")
	ErrCatalogBootstrapState  = errors.New("catalog bootstrap requires uninitialized catalog state")
	ErrCatalogBootstrapSeed   = errors.New("catalog bootstrap requires unassigned seed routes")
)

// CatalogBootstrapPlan is a read-only recipe for a guarded single-group
// transaction. Its caller must retain the exact read fence/pin, validate every
// dependency's group, and wrap the transaction in the current bootstrap witness.
// Constructing a plan supplies neither admission nor permission to dispatch it.
type CatalogBootstrapPlan struct {
	Snapshot  CatalogSnapshot
	Mutations []*store.KVPairMutation
	ReadKeys  [][]byte
}

// BuildBootstrapPlanAt reads only readTS; it neither chooses a timestamp nor
// changes the store or route cache. Populated catalogs must be loaded instead.
func (s *CatalogStore) BuildBootstrapPlanAt(ctx context.Context, readTS uint64, seeds []Route) (CatalogBootstrapPlan, error) {
	if err := ensureCatalogStore(s); err != nil {
		return CatalogBootstrapPlan{}, err
	}
	if readTS == 0 || readTS == math.MaxUint64 {
		return CatalogBootstrapPlan{}, errors.WithStack(ErrCatalogBootstrapReadTS)
	}
	ctx = contextOrBackground(ctx)
	if err := ctx.Err(); err != nil {
		return CatalogBootstrapPlan{}, errors.WithStack(err)
	}
	if err := s.checkBootstrapStateAt(ctx, readTS); err != nil {
		return CatalogBootstrapPlan{}, err
	}
	routes, err := bootstrapSeedRoutes(seeds)
	if err != nil {
		return CatalogBootstrapPlan{}, err
	}
	plan := savePlan{readTS: readTS, nextVersion: 1, routes: routes}
	mutations, err := s.buildSaveMutations(ctx, &plan)
	if err != nil {
		return CatalogBootstrapPlan{}, err
	}
	readKeys := make([][]byte, len(mutations))
	for i, mutation := range mutations {
		readKeys[i] = bytes.Clone(mutation.Key)
	}
	return CatalogBootstrapPlan{
		Snapshot:  CatalogSnapshot{Version: 1, Routes: cloneRouteDescriptors(plan.routes), ReadTS: readTS},
		Mutations: mutations,
		ReadKeys:  readKeys,
	}, nil
}

func (s *CatalogStore) checkBootstrapStateAt(ctx context.Context, readTS uint64) error {
	version, err := s.versionAt(ctx, readTS)
	if err != nil {
		return err
	}
	if version != 0 {
		return errors.WithStack(ErrCatalogVersionMismatch)
	}
	// A version-zero catalog must not contain orphaned routes, ID allocations,
	// deltas, split state or ownership proofs. Do not sanitize them by seeding.
	prefix := []byte("!dist|")
	cursor, upper := bytes.Clone(prefix), prefixScanEnd(prefix)
	for {
		page, err := s.store.ScanAt(ctx, cursor, upper, catalogScanPageSize, readTS)
		if err != nil {
			return errors.WithStack(err)
		}
		for _, entry := range page {
			if !bytes.Equal(entry.Key, CatalogVersionKey()) {
				return errors.WithStack(ErrCatalogBootstrapState)
			}
		}
		if len(page) < catalogScanPageSize {
			return nil
		}
		cursor = nextScanCursor(page[len(page)-1].Key)
	}
}

func bootstrapSeedRoutes(seeds []Route) ([]RouteDescriptor, error) {
	if len(seeds) == 0 {
		return nil, errors.WithStack(ErrCatalogBootstrapSeed)
	}
	sorted := append([]Route(nil), seeds...)
	sort.Slice(sorted, func(i, j int) bool { return bytes.Compare(sorted[i].Start, sorted[j].Start) < 0 })
	for _, route := range sorted {
		if route.RouteID != 0 || route.StagedVisibilityActive || route.MigrationJobID != 0 || route.MinWriteTSExclusive != 0 {
			return nil, errors.WithStack(ErrCatalogBootstrapSeed)
		}
	}
	routes := routeDescriptorsFromEngine(sorted)
	if _, err := routesFromCatalog(routes); err != nil {
		return nil, err
	}
	return routes, nil
}
