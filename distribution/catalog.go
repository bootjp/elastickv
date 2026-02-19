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
	catalogMetaPrefix        = "!dist|meta|"
	catalogRoutePrefix       = "!dist|route|"
	catalogVersionStorageKey = catalogMetaPrefix + "version"

	catalogUint64Bytes       = 8
	catalogVersionRecordSize = 1 + catalogUint64Bytes

	catalogVersionCodecVersion byte = 1
	catalogRouteCodecVersion   byte = 1

	catalogScanPageSize = 256
)

var (
	ErrCatalogStoreRequired        = errors.New("catalog store is required")
	ErrCatalogVersionMismatch      = errors.New("catalog version mismatch")
	ErrCatalogVersionOverflow      = errors.New("catalog version overflow")
	ErrCatalogRouteIDRequired      = errors.New("catalog route id is required")
	ErrCatalogGroupIDRequired      = errors.New("catalog group id is required")
	ErrCatalogDuplicateRouteID     = errors.New("catalog route id must be unique")
	ErrCatalogInvalidRouteRange    = errors.New("catalog route range is invalid")
	ErrCatalogInvalidVersionRecord = errors.New("catalog version record is invalid")
	ErrCatalogInvalidRouteRecord   = errors.New("catalog route record is invalid")
	ErrCatalogInvalidRouteState    = errors.New("catalog route state is invalid")
	ErrCatalogInvalidRouteKey      = errors.New("catalog route key is invalid")
	ErrCatalogRouteKeyIDMismatch   = errors.New("catalog route key and record route id mismatch")
)

// RouteState describes the control-plane state of a route.
type RouteState byte

const (
	// RouteStateActive is a normal serving route.
	RouteStateActive RouteState = iota
	// RouteStateWriteFenced blocks writes during cutover.
	RouteStateWriteFenced
	// RouteStateMigratingSource means range data is being copied out.
	RouteStateMigratingSource
	// RouteStateMigratingTarget means range data is being copied in.
	RouteStateMigratingTarget
)

func (s RouteState) valid() bool {
	switch s {
	case RouteStateActive, RouteStateWriteFenced, RouteStateMigratingSource, RouteStateMigratingTarget:
		return true
	default:
		return false
	}
}

// RouteDescriptor is the durable representation of a route.
type RouteDescriptor struct {
	RouteID       uint64
	Start         []byte
	End           []byte
	GroupID       uint64
	State         RouteState
	ParentRouteID uint64
}

// CatalogSnapshot is a point-in-time snapshot of the route catalog.
type CatalogSnapshot struct {
	Version uint64
	Routes  []RouteDescriptor
}

// CatalogStore provides persistence helpers for route catalog state.
type CatalogStore struct {
	store store.MVCCStore
}

// NewCatalogStore creates a route catalog persistence helper.
func NewCatalogStore(st store.MVCCStore) *CatalogStore {
	return &CatalogStore{store: st}
}

// CatalogVersionKey returns the reserved key used for catalog version storage.
func CatalogVersionKey() []byte {
	return []byte(catalogVersionStorageKey)
}

// CatalogRouteKey returns the reserved key used for a route descriptor.
func CatalogRouteKey(routeID uint64) []byte {
	key := make([]byte, len(catalogRoutePrefix)+catalogUint64Bytes)
	copy(key, []byte(catalogRoutePrefix))
	binary.BigEndian.PutUint64(key[len(catalogRoutePrefix):], routeID)
	return key
}

// IsCatalogRouteKey reports whether key belongs to the route catalog keyspace.
func IsCatalogRouteKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(catalogRoutePrefix))
}

// CatalogRouteIDFromKey parses the route ID from a catalog route key.
func CatalogRouteIDFromKey(key []byte) (uint64, bool) {
	if !IsCatalogRouteKey(key) {
		return 0, false
	}
	suffix := key[len(catalogRoutePrefix):]
	if len(suffix) != catalogUint64Bytes {
		return 0, false
	}
	return binary.BigEndian.Uint64(suffix), true
}

// EncodeCatalogVersion serializes a catalog version record.
func EncodeCatalogVersion(version uint64) []byte {
	out := make([]byte, catalogVersionRecordSize)
	out[0] = catalogVersionCodecVersion
	binary.BigEndian.PutUint64(out[1:], version)
	return out
}

// DecodeCatalogVersion deserializes a catalog version record.
func DecodeCatalogVersion(raw []byte) (uint64, error) {
	if len(raw) != catalogVersionRecordSize {
		return 0, errors.WithStack(ErrCatalogInvalidVersionRecord)
	}
	if raw[0] != catalogVersionCodecVersion {
		return 0, errors.Wrapf(ErrCatalogInvalidVersionRecord, "unsupported version %d", raw[0])
	}
	return binary.BigEndian.Uint64(raw[1:]), nil
}

// EncodeRouteDescriptor serializes a route descriptor record.
func EncodeRouteDescriptor(route RouteDescriptor) ([]byte, error) {
	if err := validateRouteDescriptor(route); err != nil {
		return nil, err
	}

	out := make([]byte, 0, routeDescriptorEncodedSize(route))
	out = append(out, catalogRouteCodecVersion)
	out = appendU64(out, route.RouteID)
	out = appendU64(out, route.GroupID)
	out = append(out, byte(route.State))
	out = appendU64(out, route.ParentRouteID)
	out = appendU64(out, uint64(len(route.Start)))
	out = append(out, route.Start...)
	if route.End == nil {
		out = append(out, 0)
		return out, nil
	}

	out = append(out, 1)
	out = appendU64(out, uint64(len(route.End)))
	out = append(out, route.End...)
	return out, nil
}

// DecodeRouteDescriptor deserializes a route descriptor record.
func DecodeRouteDescriptor(raw []byte) (RouteDescriptor, error) {
	if len(raw) < 1 {
		return RouteDescriptor{}, errors.WithStack(ErrCatalogInvalidRouteRecord)
	}
	if raw[0] != catalogRouteCodecVersion {
		return RouteDescriptor{}, errors.Wrapf(ErrCatalogInvalidRouteRecord, "unsupported version %d", raw[0])
	}

	r := bytes.NewReader(raw[1:])
	route, err := decodeRouteDescriptorHeader(r)
	if err != nil {
		return RouteDescriptor{}, err
	}
	route.End, err = decodeRouteDescriptorEnd(r)
	if err != nil {
		return RouteDescriptor{}, err
	}
	if r.Len() != 0 {
		return RouteDescriptor{}, errors.WithStack(ErrCatalogInvalidRouteRecord)
	}
	if err := validateRouteDescriptor(route); err != nil {
		return RouteDescriptor{}, err
	}
	return route, nil
}

// Snapshot reads a consistent route catalog snapshot at the store's latest
// known commit timestamp.
func (s *CatalogStore) Snapshot(ctx context.Context) (CatalogSnapshot, error) {
	if err := ensureCatalogStore(s); err != nil {
		return CatalogSnapshot{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	readTS := s.store.LastCommitTS()
	version, err := s.versionAt(ctx, readTS)
	if err != nil {
		return CatalogSnapshot{}, err
	}
	routes, err := s.routesAt(ctx, readTS)
	if err != nil {
		return CatalogSnapshot{}, err
	}
	return CatalogSnapshot{Version: version, Routes: routes}, nil
}

// Save updates the route catalog using optimistic version checks and bumps the
// catalog version by exactly one on success.
func (s *CatalogStore) Save(ctx context.Context, expectedVersion uint64, routes []RouteDescriptor) (CatalogSnapshot, error) {
	if err := ensureCatalogStore(s); err != nil {
		return CatalogSnapshot{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	plan, err := s.prepareSave(ctx, expectedVersion, routes)
	if err != nil {
		return CatalogSnapshot{}, err
	}
	mutations, err := s.buildSaveMutations(ctx, plan)
	if err != nil {
		return CatalogSnapshot{}, err
	}
	if err := s.applySaveMutations(ctx, plan, mutations); err != nil {
		return CatalogSnapshot{}, err
	}

	return CatalogSnapshot{
		Version: plan.nextVersion,
		Routes:  cloneRouteDescriptors(plan.routes),
	}, nil
}

func ensureCatalogStore(s *CatalogStore) error {
	if s == nil || s.store == nil {
		return errors.WithStack(ErrCatalogStoreRequired)
	}
	return nil
}

func normalizeRoutes(routes []RouteDescriptor) ([]RouteDescriptor, error) {
	if len(routes) == 0 {
		return []RouteDescriptor{}, nil
	}
	out := make([]RouteDescriptor, 0, len(routes))
	seen := make(map[uint64]struct{}, len(routes))
	for _, route := range routes {
		if err := validateRouteDescriptor(route); err != nil {
			return nil, err
		}
		if _, exists := seen[route.RouteID]; exists {
			return nil, errors.WithStack(ErrCatalogDuplicateRouteID)
		}
		seen[route.RouteID] = struct{}{}
		out = append(out, cloneRouteDescriptor(route))
	}
	sortRouteDescriptors(out)
	return out, nil
}

func validateRouteDescriptor(route RouteDescriptor) error {
	if route.RouteID == 0 {
		return errors.WithStack(ErrCatalogRouteIDRequired)
	}
	if route.GroupID == 0 {
		return errors.WithStack(ErrCatalogGroupIDRequired)
	}
	if !route.State.valid() {
		return errors.WithStack(ErrCatalogInvalidRouteState)
	}
	if route.End != nil && bytes.Compare(route.Start, route.End) >= 0 {
		return errors.WithStack(ErrCatalogInvalidRouteRange)
	}
	return nil
}

func cloneRouteDescriptor(route RouteDescriptor) RouteDescriptor {
	return RouteDescriptor{
		RouteID:       route.RouteID,
		Start:         cloneBytes(route.Start),
		End:           cloneBytes(route.End),
		GroupID:       route.GroupID,
		State:         route.State,
		ParentRouteID: route.ParentRouteID,
	}
}

func cloneRouteDescriptors(routes []RouteDescriptor) []RouteDescriptor {
	out := make([]RouteDescriptor, len(routes))
	for i := range routes {
		out[i] = cloneRouteDescriptor(routes[i])
	}
	return out
}

func sortRouteDescriptors(routes []RouteDescriptor) {
	sort.Slice(routes, func(i, j int) bool {
		return routeDescriptorLess(routes[i], routes[j])
	})
}

func routeDescriptorLess(left, right RouteDescriptor) bool {
	if c := bytes.Compare(left.Start, right.Start); c != 0 {
		return c < 0
	}

	if left.End == nil && right.End != nil {
		return false
	}
	if left.End != nil && right.End == nil {
		return true
	}
	if left.End != nil && right.End != nil {
		if c := bytes.Compare(left.End, right.End); c != 0 {
			return c < 0
		}
	}

	return left.RouteID < right.RouteID
}

func readU64LenBytes(r *bytes.Reader, rawLen uint64) ([]byte, error) {
	n, err := u64ToInt(rawLen)
	if err != nil {
		return nil, err
	}
	if n > r.Len() {
		return nil, errors.WithStack(ErrCatalogInvalidRouteRecord)
	}
	if n == 0 {
		return []byte{}, nil
	}
	out := make([]byte, n)
	if _, err := r.Read(out); err != nil {
		return nil, errors.WithStack(err)
	}
	return out, nil
}

func u64ToInt(v uint64) (int, error) {
	if v > uint64(math.MaxInt) {
		return 0, errors.WithStack(ErrCatalogInvalidRouteRecord)
	}
	return int(v), nil
}

func (s *CatalogStore) versionAt(ctx context.Context, ts uint64) (uint64, error) {
	raw, err := s.store.GetAt(ctx, CatalogVersionKey(), ts)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, errors.WithStack(err)
	}
	v, err := DecodeCatalogVersion(raw)
	if err != nil {
		return 0, err
	}
	return v, nil
}

func (s *CatalogStore) routesAt(ctx context.Context, ts uint64) ([]RouteDescriptor, error) {
	entries, err := s.scanRouteEntriesAt(ctx, ts)
	if err != nil {
		return nil, err
	}
	out := make([]RouteDescriptor, 0, len(entries))
	seen := make(map[uint64]struct{}, len(entries))
	for _, kvp := range entries {
		routeID, ok := CatalogRouteIDFromKey(kvp.Key)
		if !ok {
			return nil, errors.WithStack(ErrCatalogInvalidRouteKey)
		}
		route, err := DecodeRouteDescriptor(kvp.Value)
		if err != nil {
			return nil, err
		}
		if route.RouteID != routeID {
			return nil, errors.WithStack(ErrCatalogRouteKeyIDMismatch)
		}
		if _, exists := seen[route.RouteID]; exists {
			return nil, errors.WithStack(ErrCatalogDuplicateRouteID)
		}
		seen[route.RouteID] = struct{}{}
		out = append(out, route)
	}
	sortRouteDescriptors(out)
	return out, nil
}

func (s *CatalogStore) scanRouteEntriesAt(ctx context.Context, ts uint64) ([]*store.KVPair, error) {
	prefix := []byte(catalogRoutePrefix)
	upper := prefixScanEnd(prefix)
	cursor := cloneBytes(prefix)
	out := make([]*store.KVPair, 0)

	for {
		page, err := s.store.ScanAt(ctx, cursor, upper, catalogScanPageSize, ts)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if len(page) == 0 {
			break
		}

		var (
			lastKey []byte
			done    bool
		)
		out, lastKey, done = appendRoutePage(out, page, prefix)
		if done {
			return out, nil
		}
		if lastKey == nil || len(page) < catalogScanPageSize {
			break
		}
		nextCursor, inRange := nextCursorWithinUpper(lastKey, upper)
		if !inRange {
			break
		}
		cursor = nextCursor
	}
	return out, nil
}

type savePlan struct {
	readTS      uint64
	minCommitTS uint64
	nextVersion uint64
	routes      []RouteDescriptor
}

func (s *CatalogStore) prepareSave(ctx context.Context, expectedVersion uint64, routes []RouteDescriptor) (savePlan, error) {
	normalized, err := normalizeRoutes(routes)
	if err != nil {
		return savePlan{}, err
	}

	readTS := s.store.LastCommitTS()
	currentVersion, err := s.versionAt(ctx, readTS)
	if err != nil {
		return savePlan{}, err
	}
	if currentVersion != expectedVersion {
		return savePlan{}, errors.WithStack(ErrCatalogVersionMismatch)
	}

	nextVersion := expectedVersion + 1
	if nextVersion == 0 {
		return savePlan{}, errors.WithStack(ErrCatalogVersionOverflow)
	}
	minCommitTS := readTS + 1
	if minCommitTS == 0 {
		return savePlan{}, errors.WithStack(ErrCatalogVersionOverflow)
	}

	return savePlan{
		readTS:      readTS,
		minCommitTS: minCommitTS,
		nextVersion: nextVersion,
		routes:      normalized,
	}, nil
}

func (s *CatalogStore) buildSaveMutations(ctx context.Context, plan savePlan) ([]*store.KVPairMutation, error) {
	existingRoutes, err := s.routesAt(ctx, plan.readTS)
	if err != nil {
		return nil, err
	}

	mutations := make([]*store.KVPairMutation, 0, len(existingRoutes)+len(plan.routes)+1)
	mutations = appendDeleteRouteMutations(mutations, existingRoutes, plan.routes)
	mutations, err = appendUpsertRouteMutations(mutations, existingRoutes, plan.routes)
	if err != nil {
		return nil, err
	}
	mutations = append(mutations, &store.KVPairMutation{
		Op:    store.OpTypePut,
		Key:   CatalogVersionKey(),
		Value: EncodeCatalogVersion(plan.nextVersion),
	})
	return mutations, nil
}

func (s *CatalogStore) applySaveMutations(ctx context.Context, plan savePlan, mutations []*store.KVPairMutation) error {
	commitTS, err := s.commitTSForApply(plan.minCommitTS)
	if err != nil {
		return err
	}
	if err := s.store.ApplyMutations(ctx, mutations, plan.readTS, commitTS); err != nil {
		if errors.Is(err, store.ErrWriteConflict) {
			return errors.WithStack(ErrCatalogVersionMismatch)
		}
		return errors.WithStack(err)
	}
	return nil
}

func (s *CatalogStore) commitTSForApply(minCommitTS uint64) (uint64, error) {
	currentLast := s.store.LastCommitTS()
	candidateFromStore := currentLast + 1
	if candidateFromStore == 0 {
		return 0, errors.WithStack(ErrCatalogVersionOverflow)
	}

	if candidateFromStore > minCommitTS {
		return candidateFromStore, nil
	}
	return minCommitTS, nil
}

func appendDeleteRouteMutations(out []*store.KVPairMutation, existing []RouteDescriptor, desired []RouteDescriptor) []*store.KVPairMutation {
	desiredByID := make(map[uint64]struct{}, len(desired))
	for _, route := range desired {
		desiredByID[route.RouteID] = struct{}{}
	}

	for _, route := range existing {
		if _, ok := desiredByID[route.RouteID]; ok {
			continue
		}
		out = append(out, &store.KVPairMutation{
			Op:  store.OpTypeDelete,
			Key: CatalogRouteKey(route.RouteID),
		})
	}
	return out
}

func appendUpsertRouteMutations(out []*store.KVPairMutation, existing []RouteDescriptor, desired []RouteDescriptor) ([]*store.KVPairMutation, error) {
	existingByID := make(map[uint64]RouteDescriptor, len(existing))
	for _, route := range existing {
		existingByID[route.RouteID] = route
	}

	for _, route := range desired {
		if existingRoute, ok := existingByID[route.RouteID]; ok && routeDescriptorEqual(existingRoute, route) {
			continue
		}
		encoded, err := EncodeRouteDescriptor(route)
		if err != nil {
			return nil, err
		}
		out = append(out, &store.KVPairMutation{
			Op:    store.OpTypePut,
			Key:   CatalogRouteKey(route.RouteID),
			Value: encoded,
		})
	}
	return out, nil
}

func routeDescriptorEqual(left, right RouteDescriptor) bool {
	return left.RouteID == right.RouteID &&
		bytes.Equal(left.Start, right.Start) &&
		bytes.Equal(left.End, right.End) &&
		left.GroupID == right.GroupID &&
		left.State == right.State &&
		left.ParentRouteID == right.ParentRouteID
}

func appendU64(dst []byte, v uint64) []byte {
	var encoded [catalogUint64Bytes]byte
	binary.BigEndian.PutUint64(encoded[:], v)
	return append(dst, encoded[:]...)
}

func routeDescriptorEncodedSize(route RouteDescriptor) int {
	size := 1 + catalogUint64Bytes + catalogUint64Bytes + 1 + catalogUint64Bytes + catalogUint64Bytes + len(route.Start) + 1
	if route.End != nil {
		size += catalogUint64Bytes + len(route.End)
	}
	return size
}

func decodeRouteDescriptorHeader(r *bytes.Reader) (RouteDescriptor, error) {
	var routeID uint64
	var groupID uint64
	var parentRouteID uint64
	var startLen uint64

	if err := binary.Read(r, binary.BigEndian, &routeID); err != nil {
		return RouteDescriptor{}, errors.WithStack(err)
	}
	if err := binary.Read(r, binary.BigEndian, &groupID); err != nil {
		return RouteDescriptor{}, errors.WithStack(err)
	}
	stateRaw, err := r.ReadByte()
	if err != nil {
		return RouteDescriptor{}, errors.WithStack(err)
	}
	if err := binary.Read(r, binary.BigEndian, &parentRouteID); err != nil {
		return RouteDescriptor{}, errors.WithStack(err)
	}
	if err := binary.Read(r, binary.BigEndian, &startLen); err != nil {
		return RouteDescriptor{}, errors.WithStack(err)
	}

	start, err := readU64LenBytes(r, startLen)
	if err != nil {
		return RouteDescriptor{}, err
	}
	return RouteDescriptor{
		RouteID:       routeID,
		GroupID:       groupID,
		State:         RouteState(stateRaw),
		ParentRouteID: parentRouteID,
		Start:         start,
	}, nil
}

func decodeRouteDescriptorEnd(r *bytes.Reader) ([]byte, error) {
	endFlag, err := r.ReadByte()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if endFlag == 0 {
		return nil, nil
	}
	if endFlag != 1 {
		return nil, errors.Wrapf(ErrCatalogInvalidRouteRecord, "invalid end flag %d", endFlag)
	}

	var endLen uint64
	if err := binary.Read(r, binary.BigEndian, &endLen); err != nil {
		return nil, errors.WithStack(err)
	}
	return readU64LenBytes(r, endLen)
}

func appendRoutePage(out []*store.KVPair, page []*store.KVPair, prefix []byte) ([]*store.KVPair, []byte, bool) {
	var lastKey []byte
	for _, kvp := range page {
		if kvp == nil {
			continue
		}
		if !bytes.HasPrefix(kvp.Key, prefix) {
			return out, lastKey, true
		}
		out = append(out, &store.KVPair{
			Key:   cloneBytes(kvp.Key),
			Value: cloneBytes(kvp.Value),
		})
		lastKey = kvp.Key
	}
	return out, lastKey, false
}

func nextCursorWithinUpper(lastKey []byte, upper []byte) ([]byte, bool) {
	if len(lastKey) == 0 {
		return nil, false
	}
	nextCursor := nextScanCursor(lastKey)
	if upper != nil && bytes.Compare(nextCursor, upper) >= 0 {
		return nil, false
	}
	return nextCursor, true
}

func prefixScanEnd(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	out := cloneBytes(prefix)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] == ^byte(0) {
			continue
		}
		out[i]++
		return out[:i+1]
	}
	return nil
}

func nextScanCursor(lastKey []byte) []byte {
	next := make([]byte, len(lastKey)+1)
	copy(next, lastKey)
	return next
}
