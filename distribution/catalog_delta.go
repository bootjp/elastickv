package distribution

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"sort"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	catalogDeltaPrefix          = "!dist|delta|"
	catalogDeltaFloorStorageKey = catalogMetaPrefix + "delta_floor"

	catalogDeltaCodecVersion       byte = 1
	catalogDeltaHeaderSize              = 1 + 3*catalogUint64Bytes
	catalogDeltaMutationHeaderSize      = 1 + 2*catalogUint64Bytes

	// DefaultCatalogDeltaRetention bounds the durable reconnect window. A
	// watcher that falls behind this many catalog versions receives a snapshot
	// reset instead of an incomplete delta sequence.
	DefaultCatalogDeltaRetention uint64 = 1024
	DefaultCatalogDeltaBatchSize        = 128
	MaxCatalogDeltaBatchSize            = 1024
	CatalogWatchProtocolVersion  uint32 = 1
)

var (
	ErrCatalogInvalidDeltaRecord   = errors.New("catalog delta record is invalid")
	ErrCatalogInvalidDeltaMutation = errors.New("catalog delta mutation is invalid")
	ErrCatalogDeltaVersionGap      = errors.New("catalog delta version is not contiguous")
	ErrCatalogDeltaBaseMismatch    = errors.New("catalog delta base version does not match durable catalog")
	ErrCatalogDeltaVersionFuture   = errors.New("catalog delta cursor is ahead of catalog version")
	ErrCatalogDeltaLimitInvalid    = errors.New("catalog delta limit must be positive")
)

// CatalogMutationOp describes one route-level catalog change.
type CatalogMutationOp byte

const (
	CatalogMutationUpsert CatalogMutationOp = 1
	CatalogMutationDelete CatalogMutationOp = 2
)

// CatalogRouteMutation changes one durable route descriptor.
type CatalogRouteMutation struct {
	Op      CatalogMutationOp
	RouteID uint64
	Route   RouteDescriptor
}

// CatalogDelta is the durable transition from PreviousVersion to Version.
type CatalogDelta struct {
	PreviousVersion uint64
	Version         uint64
	Mutations       []CatalogRouteMutation
}

// CatalogChangeSet contains either a snapshot reset or a contiguous delta
// batch. Reset is non-nil only when the requested cursor predates retention.
type CatalogChangeSet struct {
	Reset  *CatalogSnapshot
	Deltas []CatalogDelta
}

// CatalogDeltaFloorKey stores the oldest retained delta version.
func CatalogDeltaFloorKey() []byte {
	return []byte(catalogDeltaFloorStorageKey)
}

// CatalogDeltaKey returns the ordered durable key for a catalog delta.
func CatalogDeltaKey(version uint64) []byte {
	key := make([]byte, len(catalogDeltaPrefix)+catalogUint64Bytes)
	copy(key, catalogDeltaPrefix)
	binary.BigEndian.PutUint64(key[len(catalogDeltaPrefix):], version)
	return key
}

// IsCatalogDeltaKey reports whether key belongs to the delta log keyspace.
func IsCatalogDeltaKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(catalogDeltaPrefix))
}

// CatalogDeltaVersionFromKey parses the version from a durable delta key.
func CatalogDeltaVersionFromKey(key []byte) (uint64, bool) {
	if !IsCatalogDeltaKey(key) {
		return 0, false
	}
	suffix := key[len(catalogDeltaPrefix):]
	if len(suffix) != catalogUint64Bytes {
		return 0, false
	}
	version := binary.BigEndian.Uint64(suffix)
	return version, version != 0
}

// EncodeCatalogDelta serializes a validated catalog transition.
func EncodeCatalogDelta(delta CatalogDelta) ([]byte, error) {
	if err := validateCatalogDelta(delta); err != nil {
		return nil, err
	}
	if len(delta.Mutations) > (math.MaxInt-catalogDeltaHeaderSize)/catalogDeltaMutationHeaderSize {
		return nil, errors.WithStack(ErrCatalogInvalidDeltaRecord)
	}

	out := make([]byte, 0, catalogDeltaHeaderSize+len(delta.Mutations)*catalogDeltaMutationHeaderSize)
	out = append(out, catalogDeltaCodecVersion)
	out = appendU64(out, delta.PreviousVersion)
	out = appendU64(out, delta.Version)
	out = appendU64(out, uint64(len(delta.Mutations)))
	for _, mutation := range delta.Mutations {
		out = append(out, byte(mutation.Op))
		out = appendU64(out, mutation.RouteID)
		if mutation.Op == CatalogMutationDelete {
			out = appendU64(out, 0)
			continue
		}
		encoded, err := EncodeRouteDescriptor(mutation.Route)
		if err != nil {
			return nil, err
		}
		out = appendU64(out, uint64(len(encoded)))
		out = append(out, encoded...)
	}
	return out, nil
}

// DecodeCatalogDelta deserializes and validates a catalog transition.
func DecodeCatalogDelta(raw []byte) (CatalogDelta, error) {
	if len(raw) < catalogDeltaHeaderSize || raw[0] != catalogDeltaCodecVersion {
		return CatalogDelta{}, errors.WithStack(ErrCatalogInvalidDeltaRecord)
	}
	r := bytes.NewReader(raw[1:])
	delta, count, err := decodeCatalogDeltaHeader(r)
	if err != nil {
		return CatalogDelta{}, err
	}
	delta.Mutations, err = decodeCatalogDeltaMutations(r, count)
	if err != nil {
		return CatalogDelta{}, err
	}
	if r.Len() != 0 {
		return CatalogDelta{}, errors.WithStack(ErrCatalogInvalidDeltaRecord)
	}
	if err := validateCatalogDelta(delta); err != nil {
		return CatalogDelta{}, err
	}
	return delta, nil
}

func decodeCatalogDeltaHeader(r *bytes.Reader) (CatalogDelta, int, error) {
	var delta CatalogDelta
	var mutationCount uint64
	fields := []*uint64{&delta.PreviousVersion, &delta.Version, &mutationCount}
	for _, field := range fields {
		if err := binary.Read(r, binary.BigEndian, field); err != nil {
			return CatalogDelta{}, 0, errors.WithStack(ErrCatalogInvalidDeltaRecord)
		}
	}
	count, err := u64ToInt(mutationCount)
	if err != nil || count > r.Len()/catalogDeltaMutationHeaderSize {
		return CatalogDelta{}, 0, errors.WithStack(ErrCatalogInvalidDeltaRecord)
	}
	return delta, count, nil
}

func decodeCatalogDeltaMutations(r *bytes.Reader, count int) ([]CatalogRouteMutation, error) {
	mutations := make([]CatalogRouteMutation, 0, count)
	for range count {
		mutation, err := decodeCatalogRouteMutation(r)
		if err != nil {
			return nil, err
		}
		mutations = append(mutations, mutation)
	}
	return mutations, nil
}

func decodeCatalogRouteMutation(r *bytes.Reader) (CatalogRouteMutation, error) {
	op, err := r.ReadByte()
	if err != nil {
		return CatalogRouteMutation{}, errors.WithStack(ErrCatalogInvalidDeltaRecord)
	}
	mutation := CatalogRouteMutation{Op: CatalogMutationOp(op)}
	var payloadLen uint64
	if err := binary.Read(r, binary.BigEndian, &mutation.RouteID); err != nil {
		return CatalogRouteMutation{}, errors.WithStack(ErrCatalogInvalidDeltaRecord)
	}
	if err := binary.Read(r, binary.BigEndian, &payloadLen); err != nil {
		return CatalogRouteMutation{}, errors.WithStack(ErrCatalogInvalidDeltaRecord)
	}
	payload, err := readU64LenBytes(r, payloadLen)
	if err != nil {
		return CatalogRouteMutation{}, errors.WithStack(ErrCatalogInvalidDeltaRecord)
	}
	if mutation.Op == CatalogMutationUpsert {
		mutation.Route, err = DecodeRouteDescriptor(payload)
		if err != nil {
			return CatalogRouteMutation{}, err
		}
	} else if len(payload) != 0 {
		return CatalogRouteMutation{}, errors.WithStack(ErrCatalogInvalidDeltaMutation)
	}
	return mutation, nil
}

func validateCatalogDelta(delta CatalogDelta) error {
	if delta.Version == 0 || delta.PreviousVersion == math.MaxUint64 || delta.PreviousVersion+1 != delta.Version {
		return errors.WithStack(ErrCatalogDeltaVersionGap)
	}
	seen := make(map[uint64]struct{}, len(delta.Mutations))
	for _, mutation := range delta.Mutations {
		if mutation.RouteID == 0 {
			return errors.WithStack(ErrCatalogInvalidDeltaMutation)
		}
		if _, ok := seen[mutation.RouteID]; ok {
			return errors.WithStack(ErrCatalogInvalidDeltaMutation)
		}
		seen[mutation.RouteID] = struct{}{}
		if err := validateCatalogDeltaMutation(mutation); err != nil {
			return err
		}
	}
	return nil
}

func validateCatalogDeltaMutation(mutation CatalogRouteMutation) error {
	switch mutation.Op {
	case CatalogMutationUpsert:
		if mutation.Route.RouteID != mutation.RouteID {
			return errors.WithStack(ErrCatalogInvalidDeltaMutation)
		}
		return validateRouteDescriptor(mutation.Route)
	case CatalogMutationDelete:
		if !catalogDeltaRouteIsZero(mutation.Route) {
			return errors.WithStack(ErrCatalogInvalidDeltaMutation)
		}
		return nil
	default:
		return errors.WithStack(ErrCatalogInvalidDeltaMutation)
	}
}

func catalogDeltaRouteIsZero(route RouteDescriptor) bool {
	return route.RouteID == 0 &&
		route.Start == nil &&
		route.End == nil &&
		route.GroupID == 0 &&
		route.State == 0 &&
		route.ParentRouteID == 0 &&
		!route.StagedVisibilityActive &&
		route.MigrationJobID == 0 &&
		route.MinWriteTSExclusive == 0 &&
		route.SplitAtHLC == 0
}

func buildCatalogDelta(existing, desired []RouteDescriptor, previousVersion, version uint64) (CatalogDelta, error) {
	existingByID := make(map[uint64]RouteDescriptor, len(existing))
	desiredByID := make(map[uint64]RouteDescriptor, len(desired))
	for _, route := range existing {
		existingByID[route.RouteID] = route
	}
	for _, route := range desired {
		desiredByID[route.RouteID] = route
	}

	mutations := make([]CatalogRouteMutation, 0)
	for routeID := range existingByID {
		if _, ok := desiredByID[routeID]; !ok {
			mutations = append(mutations, CatalogRouteMutation{Op: CatalogMutationDelete, RouteID: routeID})
		}
	}
	for routeID, route := range desiredByID {
		if current, ok := existingByID[routeID]; ok && routeDescriptorEqual(current, route) {
			continue
		}
		mutations = append(mutations, CatalogRouteMutation{
			Op:      CatalogMutationUpsert,
			RouteID: routeID,
			Route:   CloneRouteDescriptor(route),
		})
	}
	sort.Slice(mutations, func(i, j int) bool { return mutations[i].RouteID < mutations[j].RouteID })
	delta := CatalogDelta{PreviousVersion: previousVersion, Version: version, Mutations: mutations}
	return delta, validateCatalogDelta(delta)
}

// BuildDeltaMutationsAt returns the durable delta/floor/retention mutations
// that must be committed atomically with the catalog version transition.
func (s *CatalogStore) BuildDeltaMutationsAt(ctx context.Context, readTS uint64, delta CatalogDelta) ([]*store.KVPairMutation, error) {
	if err := ensureCatalogStore(s); err != nil {
		return nil, err
	}
	ctx = contextOrBackground(ctx)
	if err := validateCatalogDelta(delta); err != nil {
		return nil, err
	}
	currentVersion, err := s.versionAt(ctx, readTS)
	if err != nil {
		return nil, err
	}
	if currentVersion != delta.PreviousVersion {
		return nil, errors.Wrapf(
			ErrCatalogDeltaBaseMismatch,
			"delta follows %d, durable catalog is at %d",
			delta.PreviousVersion,
			currentVersion,
		)
	}
	encoded, err := EncodeCatalogDelta(delta)
	if err != nil {
		return nil, err
	}
	currentFloor, err := s.deltaFloorAt(ctx, readTS)
	if err != nil {
		return nil, err
	}
	newFloor := nextCatalogDeltaFloor(currentFloor, delta.PreviousVersion, delta.Version)
	mutations := []*store.KVPairMutation{
		{Op: store.OpTypePut, Key: CatalogDeltaKey(delta.Version), Value: encoded},
		{Op: store.OpTypePut, Key: CatalogDeltaFloorKey(), Value: EncodeCatalogVersion(newFloor)},
	}
	if currentFloor != 0 && newFloor > currentFloor {
		for version := currentFloor; version < newFloor; version++ {
			mutations = append(mutations, &store.KVPairMutation{Op: store.OpTypeDelete, Key: CatalogDeltaKey(version)})
		}
	}
	return mutations, nil
}

func nextCatalogDeltaFloor(currentFloor, previousVersion, version uint64) uint64 {
	floor := currentFloor
	if floor == 0 {
		floor = 1
		if previousVersion != 0 {
			floor = version
		}
	}
	if version >= DefaultCatalogDeltaRetention {
		retentionFloor := version - DefaultCatalogDeltaRetention + 1
		if retentionFloor > floor {
			floor = retentionFloor
		}
	}
	return floor
}

func (s *CatalogStore) deltaFloorAt(ctx context.Context, ts uint64) (uint64, error) {
	raw, err := s.store.GetAt(ctx, CatalogDeltaFloorKey(), ts)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, errors.WithStack(err)
	}
	floor, err := DecodeCatalogVersion(raw)
	if err != nil || floor == 0 {
		return 0, errors.WithStack(ErrCatalogInvalidDeltaRecord)
	}
	return floor, nil
}

// DeltaFloor returns the oldest retained durable delta version. Zero means the
// catalog predates delta logging and a watcher must request a snapshot reset.
func (s *CatalogStore) DeltaFloor(ctx context.Context) (uint64, error) {
	if err := ensureCatalogStore(s); err != nil {
		return 0, err
	}
	ctx = contextOrBackground(ctx)
	return s.deltaFloorAt(ctx, s.store.LastCommitTS())
}

// ChangesSince reads a consistent reconnect batch after afterVersion.
func (s *CatalogStore) ChangesSince(ctx context.Context, afterVersion uint64, limit int) (CatalogChangeSet, error) {
	if err := ensureCatalogStore(s); err != nil {
		return CatalogChangeSet{}, err
	}
	if limit <= 0 {
		return CatalogChangeSet{}, errors.WithStack(ErrCatalogDeltaLimitInvalid)
	}
	ctx = contextOrBackground(ctx)
	readTS := s.store.LastCommitTS()
	currentVersion, err := s.versionAt(ctx, readTS)
	if err != nil {
		return CatalogChangeSet{}, err
	}
	requested, current, err := nextCatalogChangeVersion(afterVersion, currentVersion)
	if err != nil {
		return CatalogChangeSet{}, err
	}
	if current {
		return CatalogChangeSet{}, nil
	}
	floor, err := s.deltaFloorAt(ctx, readTS)
	if err != nil {
		return CatalogChangeSet{}, err
	}
	if floor > currentVersion {
		return CatalogChangeSet{}, errors.Wrapf(
			ErrCatalogInvalidDeltaRecord,
			"delta floor %d exceeds catalog version %d",
			floor,
			currentVersion,
		)
	}
	if floor == 0 || requested < floor {
		return s.catalogSnapshotResetAt(ctx, readTS)
	}
	return s.catalogDeltasAt(ctx, readTS, requested, currentVersion, limit)
}

func nextCatalogChangeVersion(afterVersion, currentVersion uint64) (uint64, bool, error) {
	if afterVersion > currentVersion {
		return 0, false, errors.WithStack(ErrCatalogDeltaVersionFuture)
	}
	if afterVersion == currentVersion {
		return 0, true, nil
	}
	if afterVersion == math.MaxUint64 {
		return 0, false, errors.WithStack(ErrCatalogVersionOverflow)
	}
	return afterVersion + 1, false, nil
}

func (s *CatalogStore) catalogSnapshotResetAt(ctx context.Context, readTS uint64) (CatalogChangeSet, error) {
	snapshot, err := s.snapshotAt(ctx, readTS)
	if err != nil {
		return CatalogChangeSet{}, err
	}
	return CatalogChangeSet{Reset: &snapshot}, nil
}

func (s *CatalogStore) catalogDeltasAt(
	ctx context.Context,
	readTS uint64,
	requested uint64,
	currentVersion uint64,
	limit int,
) (CatalogChangeSet, error) {
	deltas := make([]CatalogDelta, 0)
	for version := requested; version <= currentVersion && len(deltas) < limit; version++ {
		delta, err := s.catalogDeltaAt(ctx, readTS, version)
		if err != nil {
			return CatalogChangeSet{}, err
		}
		deltas = append(deltas, delta)
		if version == math.MaxUint64 {
			break
		}
	}
	return CatalogChangeSet{Deltas: deltas}, nil
}

func (s *CatalogStore) catalogDeltaAt(ctx context.Context, readTS, version uint64) (CatalogDelta, error) {
	raw, err := s.store.GetAt(ctx, CatalogDeltaKey(version), readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return CatalogDelta{}, errors.Wrapf(ErrCatalogDeltaVersionGap, "missing version %d", version)
		}
		return CatalogDelta{}, errors.WithStack(err)
	}
	delta, err := DecodeCatalogDelta(raw)
	if err != nil {
		return CatalogDelta{}, err
	}
	expectedPrevious := version - 1
	if delta.Version != version || delta.PreviousVersion != expectedPrevious {
		return CatalogDelta{}, errors.Wrapf(ErrCatalogDeltaVersionGap, "got %d after %d", delta.Version, expectedPrevious)
	}
	return delta, nil
}

func (s *CatalogStore) snapshotAt(ctx context.Context, readTS uint64) (CatalogSnapshot, error) {
	version, err := s.versionAt(ctx, readTS)
	if err != nil {
		return CatalogSnapshot{}, err
	}
	routes, err := s.routesAt(ctx, readTS)
	if err != nil {
		return CatalogSnapshot{}, err
	}
	return CatalogSnapshot{Version: version, Routes: routes, ReadTS: readTS}, nil
}
