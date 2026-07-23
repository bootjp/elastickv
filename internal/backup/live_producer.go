package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

const (
	defaultLiveBackupEndTimeout   = 30 * time.Second
	defaultLiveBackupBeginTimeout = 30 * time.Second
	liveBackupOutputDirMode       = 0o755
	liveBackupRenewalDivisor      = 3
	liveBackupPercentDivisor      = 100
	integerSqrtRadix              = 2
)

var (
	ErrCompactionDuringDump         = errors.New("backup: key shortfall while live pin was active")
	ErrLiveBackupOutputExists       = errors.New("backup: live output already exists")
	ErrLiveBackupRenewal            = errors.New("backup: live pin renewal failed")
	ErrLiveBackupRestoreUnsupported = errors.New("backup: live producer mode is unsupported by native restore")
	ErrLiveBackupScope              = errors.New("backup: requested live scope is unavailable")
)

// LiveBackupRPC is the transport-neutral control plane used by RunLiveBackup.
// The command package adapts the generated gRPC client to this narrow surface.
type LiveBackupRPC interface {
	BeginBackup(context.Context, *pb.BeginBackupRequest) (*pb.BeginBackupResponse, error)
	RenewBackup(context.Context, *pb.RenewBackupRequest) (*pb.RenewBackupResponse, error)
	EndBackup(context.Context, *pb.EndBackupRequest) (*pb.EndBackupResponse, error)
	ListAdaptersAndScopes(context.Context, *pb.ListAdaptersAndScopesRequest) (*pb.ListAdaptersAndScopesResponse, error)
	StreamBackup(context.Context, *pb.StreamBackupRequest, func(*pb.BackupKV) error) error
}

// LiveBackupOptions controls one pinned live dump.
type LiveBackupOptions struct {
	OutputRoot string
	Adapters   AdapterSet
	Scopes     []Scope
	TTL        time.Duration

	IncludeIncompleteUploads bool
	IncludeOrphans           bool
	PreserveSQSVisibility    bool
	IncludeSQSSideRecords    bool
	RenameS3Collisions       bool
	DynamoDBBundleJSONL      bool
	DynamoDBBundleSizeBytes  int64

	ElastickvVersion string
	ClusterID        string
	BeginTimeout     time.Duration
	EndTimeout       time.Duration
	Now              func() time.Time
	WarnSink         func(event string, fields ...any)
}

// LiveBackupResult reports the consistency point and work completed.
type LiveBackupResult struct {
	ReadTS      uint64
	Scopes      []Scope
	Counters    DecodeCounters
	PinRenewals uint64
}

// RunLiveBackup owns the complete Begin -> renew -> stream -> finalize -> End
// lifecycle. The output root is created exclusively and intentionally retained
// on failure without MANIFEST.json for forensic inspection and safe cleanup.
func RunLiveBackup(ctx context.Context, rpc LiveBackupRPC, opts LiveBackupOptions) (LiveBackupResult, error) {
	if err := validateLiveBackupOptions(rpc, opts); err != nil {
		return LiveBackupResult{}, err
	}
	if err := createLiveBackupOutputRoot(opts.OutputRoot); err != nil {
		return LiveBackupResult{}, err
	}

	begin, err := beginLiveBackup(ctx, rpc, opts)
	if err != nil {
		return LiveBackupResult{}, err
	}

	dumpCtx, cancelDump := context.WithCancelCause(ctx)
	defer cancelDump(nil)
	ttl, err := durationFromMillis(begin.GetTtlMsEffective())
	if err != nil {
		endErr := endLiveBackup(rpc, begin.GetPinToken(), opts.EndTimeout)
		return LiveBackupResult{}, combineLiveBackupErrors(err, endErr)
	}
	lease := newLiveBackupLease(rpc, begin, ttl, cancelDump)
	lease.start(dumpCtx)
	result, dumpErr := dumpPinnedBackup(dumpCtx, rpc, lease, begin, opts)
	return finishLiveBackup(ctx, rpc, lease, begin, opts, result, dumpErr)
}

func finishLiveBackup(
	ctx context.Context,
	rpc LiveBackupRPC,
	lease *liveBackupLease,
	begin *pb.BeginBackupResponse,
	opts LiveBackupOptions,
	result LiveBackupResult,
	dumpErr error,
) (LiveBackupResult, error) {
	renewErr := lease.stop()
	if renewErr != nil {
		if opts.WarnSink != nil {
			opts.WarnSink("backup_pin_renewal_failed", "error", renewErr)
		}
	}
	dumpErr = combineLiveBackupPhaseErrors(dumpErr, renewErr)
	result.PinRenewals = lease.renewalCount()
	dumpErr = publishCompletedLiveBackup(ctx, lease, begin, opts, result, dumpErr)
	endErr := endLiveBackup(rpc, lease.tokenSnapshot(), opts.EndTimeout)
	if err := combineLiveBackupErrors(dumpErr, endErr); err != nil {
		return result, err
	}
	return result, nil
}

func combineLiveBackupPhaseErrors(primary, secondary error) error {
	if primary == nil {
		return secondary
	}
	if secondary == nil {
		return primary
	}
	return errors.Wrap(errors.WithSecondaryError(primary, secondary), "live backup and pin renewal failed")
}

func publishCompletedLiveBackup(
	ctx context.Context,
	lease *liveBackupLease,
	begin *pb.BeginBackupResponse,
	opts LiveBackupOptions,
	result LiveBackupResult,
	priorErr error,
) error {
	if priorErr != nil {
		return priorErr
	}
	if ctx.Err() != nil {
		return errors.Wrap(context.Cause(ctx), "live backup canceled before manifest publication")
	}
	manifest := liveBackupManifest(begin, lease.tokenSnapshot(), scopeSet(result.Scopes), opts)
	return FinalizeDump(opts.OutputRoot, manifest)
}

func beginLiveBackup(ctx context.Context, rpc LiveBackupRPC, opts LiveBackupOptions) (*pb.BeginBackupResponse, error) {
	timeout := effectiveTimeout(opts.BeginTimeout, defaultLiveBackupBeginTimeout)
	beginCtx, cancel := context.WithTimeout(ctx, timeout)
	begin, err := rpc.BeginBackup(beginCtx, &pb.BeginBackupRequest{
		TtlMs:    durationMillis(opts.TTL),
		Adapters: liveBackupAdapterNames(opts.Adapters),
		Scopes:   protoScopes(scopeSet(opts.Scopes)),
	})
	cancel()
	if err != nil {
		return nil, errors.Wrap(err, "begin live backup")
	}
	if err := validateBeginBackupResponse(begin); err != nil {
		return nil, cleanupInvalidBeginResponse(rpc, begin, err)
	}
	return begin, nil
}

func cleanupInvalidBeginResponse(rpc LiveBackupRPC, begin *pb.BeginBackupResponse, beginErr error) error {
	if begin == nil || len(begin.GetPinToken()) == 0 {
		return beginErr
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), defaultLiveBackupEndTimeout)
	_, cleanupErr := rpc.EndBackup(cleanupCtx, &pb.EndBackupRequest{PinToken: begin.GetPinToken()})
	cancel()
	if cleanupErr == nil {
		return beginErr
	}
	combined := errors.WithSecondaryError(beginErr, errors.Wrap(cleanupErr, "clean up invalid BeginBackup response"))
	return errors.Wrap(combined, "invalid BeginBackup response")
}

func endLiveBackup(rpc LiveBackupRPC, token []byte, timeout time.Duration) error {
	endCtx, cancel := context.WithTimeout(context.Background(), effectiveTimeout(timeout, defaultLiveBackupEndTimeout))
	defer cancel()
	_, err := rpc.EndBackup(endCtx, &pb.EndBackupRequest{PinToken: token})
	return errors.Wrap(err, "end live backup")
}

func combineLiveBackupErrors(dumpErr, endErr error) error {
	if dumpErr == nil {
		return endErr
	}
	if endErr == nil {
		return dumpErr
	}
	return errors.Wrap(errors.WithSecondaryError(dumpErr, endErr), "live backup and cleanup failed")
}

func effectiveTimeout(configured, fallback time.Duration) time.Duration {
	if configured > 0 {
		return configured
	}
	return fallback
}

func validateLiveBackupOptions(rpc LiveBackupRPC, opts LiveBackupOptions) error {
	switch {
	case rpc == nil:
		return errors.New("backup: live RPC client is required")
	case opts.OutputRoot == "":
		return errors.New("backup: live output root is required")
	case opts.Adapters == (AdapterSet{}):
		return errors.New("backup: at least one adapter is required")
	case opts.TTL <= 0:
		return errors.New("backup: live TTL must be positive")
	case opts.DynamoDBBundleSizeBytes < 0:
		return errors.New("backup: DynamoDB bundle size must not be negative")
	default:
		return ValidateLiveBackupRestoreCompatibility(opts)
	}
}

// ValidateLiveBackupRestoreCompatibility rejects producer modes that the
// documented native snapshot restore path cannot faithfully reverse yet.
// Keeping this check public lets the CLI fail before dialing the cluster while
// RunLiveBackup repeats it for in-process callers.
func ValidateLiveBackupRestoreCompatibility(opts LiveBackupOptions) error {
	checks := []struct {
		unsupported bool
		flag        string
	}{
		{opts.DynamoDBBundleJSONL && opts.Adapters.DynamoDB, "--dynamodb-bundle-mode=jsonl"},
		{opts.IncludeIncompleteUploads && opts.Adapters.S3, "--include-incomplete-uploads"},
		{opts.IncludeOrphans && opts.Adapters.S3, "--include-orphans"},
		{opts.PreserveSQSVisibility && opts.Adapters.SQS, "--preserve-sqs-visibility"},
		{opts.IncludeSQSSideRecords && opts.Adapters.SQS, "--include-sqs-side-records"},
	}
	for _, check := range checks {
		if check.unsupported {
			return errors.Wrap(ErrLiveBackupRestoreUnsupported, check.flag)
		}
	}
	return nil
}

func createLiveBackupOutputRoot(root string) error {
	root = filepath.Clean(root)
	parent := filepath.Dir(root)
	if parent != "." && parent != root {
		if err := os.MkdirAll(parent, liveBackupOutputDirMode); err != nil {
			return errors.WithStack(err)
		}
	}
	if err := os.Mkdir(root, liveBackupOutputDirMode); err != nil {
		if errors.Is(err, os.ErrExist) {
			return errors.Wrapf(ErrLiveBackupOutputExists, "%s", root)
		}
		return errors.WithStack(err)
	}
	return nil
}

func validateBeginBackupResponse(resp *pb.BeginBackupResponse) error {
	if resp == nil || resp.GetReadTs() == 0 || len(resp.GetPinToken()) == 0 || resp.GetTtlMsEffective() == 0 {
		return errors.New("backup: BeginBackup returned incomplete pin metadata")
	}
	_, err := durationFromMillis(resp.GetTtlMsEffective())
	return err
}

func dumpPinnedBackup(
	ctx context.Context,
	rpc LiveBackupRPC,
	lease *liveBackupLease,
	begin *pb.BeginBackupResponse,
	opts LiveBackupOptions,
) (LiveBackupResult, error) {
	listed, err := rpc.ListAdaptersAndScopes(ctx, &pb.ListAdaptersAndScopesRequest{PinToken: lease.tokenSnapshot()})
	if err != nil {
		return LiveBackupResult{}, errors.Wrap(err, "list live backup scopes")
	}
	selected, err := selectLiveBackupScopes(opts.Adapters, opts.Scopes, listed.GetScopes(), begin.GetExpectedKeys())
	if err != nil {
		return LiveBackupResult{}, err
	}
	decoder, err := NewLiveDecoder(DecodeOptions{
		OutRoot:                  opts.OutputRoot,
		Adapters:                 opts.Adapters,
		IncludeIncompleteUploads: opts.IncludeIncompleteUploads,
		IncludeOrphans:           opts.IncludeOrphans,
		RenameS3Collisions:       opts.RenameS3Collisions,
		PreserveSQSVisibility:    opts.PreserveSQSVisibility,
		IncludeSQSSideRecords:    opts.IncludeSQSSideRecords,
		DynamoDBBundleJSONL:      opts.DynamoDBBundleJSONL,
		DynamoDBBundleSizeBytes:  opts.DynamoDBBundleSizeBytes,
		WarnSink:                 opts.WarnSink,
	})
	if err != nil {
		return LiveBackupResult{}, err
	}
	actual := make(map[Scope]uint64, len(selected))
	streamErr := streamSelectedBackup(ctx, rpc, lease, selected, actual, decoder)
	counters, finalizeErr := decoder.Finalize()
	if streamErr != nil {
		return LiveBackupResult{ReadTS: begin.GetReadTs(), Scopes: sortedScopeSet(selected)}, streamErr
	}
	if finalizeErr != nil {
		return LiveBackupResult{ReadTS: begin.GetReadTs(), Scopes: sortedScopeSet(selected)}, finalizeErr
	}
	actual, err = decoder.FinalizedScopeCounts(actual)
	if err != nil {
		return LiveBackupResult{ReadTS: begin.GetReadTs(), Scopes: sortedScopeSet(selected), Counters: counters}, err
	}
	if err := validateExpectedLiveCounts(selected, actual, begin.GetExpectedKeys()); err != nil {
		return LiveBackupResult{ReadTS: begin.GetReadTs(), Scopes: sortedScopeSet(selected), Counters: counters}, err
	}
	return LiveBackupResult{ReadTS: begin.GetReadTs(), Scopes: sortedScopeSet(selected), Counters: counters}, nil
}

func streamSelectedBackup(
	ctx context.Context,
	rpc LiveBackupRPC,
	lease *liveBackupLease,
	selected map[Scope]struct{},
	actual map[Scope]uint64,
	decoder *LiveDecoder,
) error {
	if len(selected) == 0 {
		return nil
	}
	req := &pb.StreamBackupRequest{PinToken: lease.tokenSnapshot(), Scopes: protoScopes(selected)}
	err := rpc.StreamBackup(ctx, req, func(pair *pb.BackupKV) error {
		if pair == nil {
			return errors.New("backup: stream returned a nil record")
		}
		scope, scoped, err := ScopeForKey(pair.GetKey())
		if err != nil {
			return err
		}
		if !scoped {
			return errors.New("backup: stream returned an unscoped internal record")
		}
		if _, ok := selected[scope]; !ok {
			return errors.Wrapf(ErrLiveBackupScope, "stream returned unselected %s", scope)
		}
		if err := decoder.Add(pair.GetKey(), pair.GetValue()); err != nil {
			return err
		}
		actual[scope]++
		return nil
	})
	if err != nil {
		if cause := context.Cause(ctx); errors.Is(cause, ErrLiveBackupRenewal) {
			return errors.WithStack(cause)
		}
		return errors.Wrap(err, "stream pinned live backup")
	}
	return nil
}

func selectLiveBackupScopes(
	adapters AdapterSet,
	requested []Scope,
	listed []*pb.BackupScope,
	baseline []*pb.BackupExpectedKeys,
) (map[Scope]struct{}, error) {
	available, err := availableLiveBackupScopes(adapters, listed)
	if err != nil {
		return nil, err
	}
	if err := addBaselineLiveBackupScopes(available, adapters, baseline); err != nil {
		return nil, err
	}
	if len(requested) == 0 {
		return available, nil
	}
	return requestedLiveBackupScopes(adapters, requested, available)
}

func addBaselineLiveBackupScopes(
	selected map[Scope]struct{},
	adapters AdapterSet,
	baseline []*pb.BackupExpectedKeys,
) error {
	for _, item := range baseline {
		if item == nil || item.GetKeyCount() == 0 {
			continue
		}
		scope := Scope{Adapter: item.GetAdapter(), Name: item.GetScope()}
		if scope.Adapter == "" || scope.Name == "" {
			return errors.Wrap(ErrLiveBackupScope, "server returned an empty baseline scope")
		}
		if err := validateLiveScope(scope); err != nil {
			return err
		}
		if adapterEnabled(adapters, scope.Adapter) {
			selected[scope] = struct{}{}
		}
	}
	return nil
}

func availableLiveBackupScopes(adapters AdapterSet, listed []*pb.BackupScope) (map[Scope]struct{}, error) {
	available := make(map[Scope]struct{}, len(listed))
	for _, item := range listed {
		if item == nil {
			continue
		}
		scope := Scope{Adapter: item.GetAdapter(), Name: item.GetScope()}
		if scope.Adapter == "" || scope.Name == "" {
			return nil, errors.Wrap(ErrLiveBackupScope, "server returned an empty scope")
		}
		if err := validateLiveScope(scope); err != nil {
			return nil, err
		}
		if adapterEnabled(adapters, scope.Adapter) {
			available[scope] = struct{}{}
		}
	}
	return available, nil
}

func requestedLiveBackupScopes(
	adapters AdapterSet,
	requested []Scope,
	available map[Scope]struct{},
) (map[Scope]struct{}, error) {
	selected := make(map[Scope]struct{}, len(requested))
	for _, scope := range requested {
		if !adapterEnabled(adapters, scope.Adapter) {
			return nil, errors.Wrapf(ErrLiveBackupScope, "%s is excluded by --adapter", scope)
		}
		if _, ok := available[scope]; !ok {
			return nil, errors.Wrapf(ErrLiveBackupScope, "%s is absent at the pinned timestamp", scope)
		}
		selected[scope] = struct{}{}
	}
	return selected, nil
}

func validateLiveScope(scope Scope) error {
	if scope.Adapter != adapterRedis {
		return nil
	}
	db, ok := strings.CutPrefix(scope.Name, "db_")
	if !ok {
		return errors.Wrapf(ErrLiveBackupScope, "invalid Redis scope %q", scope.Name)
	}
	if _, err := strconv.ParseUint(db, 10, 32); err != nil {
		return errors.Wrapf(ErrLiveBackupScope, "invalid Redis scope %q", scope.Name)
	}
	return nil
}

func adapterEnabled(set AdapterSet, name string) bool {
	return AdapterEnabled(set, name)
}

func liveBackupAdapterNames(set AdapterSet) []string {
	names := make([]string, 0, 4) //nolint:mnd // four logical backup adapters.
	if set.DynamoDB {
		names = append(names, adapterDynamoDB)
	}
	if set.S3 {
		names = append(names, adapterS3)
	}
	if set.Redis {
		names = append(names, adapterRedis)
	}
	if set.SQS {
		names = append(names, adapterSQS)
	}
	return names
}

func protoScopes(scopes map[Scope]struct{}) []*pb.BackupScope {
	sorted := sortedScopeSet(scopes)
	out := make([]*pb.BackupScope, 0, len(sorted))
	for _, scope := range sorted {
		out = append(out, &pb.BackupScope{Adapter: scope.Adapter, Scope: scope.Name})
	}
	return out
}

func sortedScopeSet(scopes map[Scope]struct{}) []Scope {
	counts := make(map[Scope]uint64, len(scopes))
	for scope := range scopes {
		counts[scope] = 0
	}
	return SortedScopes(counts)
}

func scopeSet(scopes []Scope) map[Scope]struct{} {
	out := make(map[Scope]struct{}, len(scopes))
	for _, scope := range scopes {
		out[scope] = struct{}{}
	}
	return out
}

func validateExpectedLiveCounts(selected map[Scope]struct{}, actual map[Scope]uint64, baseline []*pb.BackupExpectedKeys) error {
	expected := make(map[Scope]uint64, len(baseline))
	for _, item := range baseline {
		if item == nil {
			continue
		}
		scope := Scope{Adapter: item.GetAdapter(), Name: item.GetScope()}
		if _, ok := expected[scope]; ok {
			return errors.Wrapf(ErrCompactionDuringDump, "duplicate baseline for %s", scope)
		}
		expected[scope] = item.GetKeyCount()
	}
	for scope := range selected {
		want, ok := expected[scope]
		if !ok {
			return errors.Wrapf(ErrCompactionDuringDump, "missing baseline for %s", scope)
		}
		minimum := minimumAcceptedLiveCount(want)
		if actual[scope] < minimum {
			return errors.Wrapf(ErrCompactionDuringDump,
				"%s: actual=%d baseline=%d minimum=%d", scope, actual[scope], want, minimum)
		}
	}
	return nil
}

func minimumAcceptedLiveCount(expected uint64) uint64 {
	onePercent := expected / liveBackupPercentDivisor
	if expected%liveBackupPercentDivisor != 0 {
		onePercent++
	}
	tolerance := onePercent + integerSqrt(expected)
	if tolerance >= expected {
		if expected > 0 {
			return 1
		}
		return 0
	}
	return expected - tolerance
}

func integerSqrt(n uint64) uint64 {
	if n < integerSqrtRadix {
		return n
	}
	x := uint64(1) << ((bits.Len64(n) + 1) / integerSqrtRadix)
	for {
		y := (x + n/x) / integerSqrtRadix
		if y >= x {
			return x
		}
		x = y
	}
}

func liveBackupManifest(begin *pb.BeginBackupResponse, token []byte, selected map[Scope]struct{}, opts LiveBackupOptions) Manifest {
	now := time.Now
	if opts.Now != nil {
		now = opts.Now
	}
	m := NewPhase0SnapshotManifest(now())
	m.Phase = PhasePhase1LivePinned
	m.Source = nil
	m.ElastickvVersion = opts.ElastickvVersion
	m.ClusterID = opts.ClusterID
	m.LastCommitTS = begin.GetReadTs()
	m.Live = &Live{ReadTS: begin.GetReadTs(), PinTokenSHA256: tokenDigest(token)}
	m.BackupTSTTLMS = begin.GetTtlMsEffective()
	m.MaxActiveBackupPins = begin.GetMaxActiveBackupPins()
	m.Shards = make([]LiveShard, 0, len(begin.GetShards()))
	for _, shard := range begin.GetShards() {
		if shard != nil {
			m.Shards = append(m.Shards, LiveShard{RaftGroupID: shard.GetRaftGroupId(), AppliedIndex: shard.GetAppliedIndex()})
		}
	}
	sort.Slice(m.Shards, func(i, j int) bool { return m.Shards[i].RaftGroupID < m.Shards[j].RaftGroupID })
	m.Adapters = liveManifestAdapters(opts.Adapters, selected, len(opts.Scopes) == 0)
	m.Exclusions = &Exclusions{
		IncludeIncompleteUploads: opts.IncludeIncompleteUploads,
		IncludeOrphans:           opts.IncludeOrphans,
		PreserveSQSVisibility:    opts.PreserveSQSVisibility,
		IncludeSQSSideRecords:    opts.IncludeSQSSideRecords,
		RenameS3Collisions:       opts.RenameS3Collisions,
	}
	if opts.DynamoDBBundleJSONL {
		m.DynamoDBLayout = DynamoDBLayoutJSONL
	}
	return m
}

func liveManifestAdapters(enabled AdapterSet, selected map[Scope]struct{}, includeEnabledEmpty bool) *Adapters {
	out := &Adapters{}
	if includeEnabledEmpty && enabled.DynamoDB {
		out.DynamoDB = &Adapter{}
	}
	if includeEnabledEmpty && enabled.S3 {
		out.S3 = &Adapter{}
	}
	if includeEnabledEmpty && enabled.Redis {
		out.Redis = &Adapter{}
	}
	if includeEnabledEmpty && enabled.SQS {
		out.SQS = &Adapter{}
	}
	for _, scope := range sortedScopeSet(selected) {
		addLiveManifestScope(out, scope)
	}
	return out
}

func addLiveManifestScope(out *Adapters, scope Scope) {
	switch scope.Adapter {
	case adapterDynamoDB:
		if out.DynamoDB == nil {
			out.DynamoDB = &Adapter{}
		}
		out.DynamoDB.Tables = append(out.DynamoDB.Tables, scope.Name)
	case adapterS3:
		if out.S3 == nil {
			out.S3 = &Adapter{}
		}
		out.S3.Buckets = append(out.S3.Buckets, scope.Name)
	case adapterRedis:
		if out.Redis == nil {
			out.Redis = &Adapter{}
		}
		addLiveManifestRedisScope(out.Redis, scope.Name)
	case adapterSQS:
		if out.SQS == nil {
			out.SQS = &Adapter{}
		}
		out.SQS.Queues = append(out.SQS.Queues, scope.Name)
	}
}

func addLiveManifestRedisScope(out *Adapter, name string) {
	db, ok := strings.CutPrefix(name, "db_")
	if !ok {
		return
	}
	index, err := strconv.ParseUint(db, 10, 32)
	if err == nil {
		out.Databases = append(out.Databases, uint32(index))
	}
}

func tokenDigest(token []byte) string {
	sum := sha256.Sum256(token)
	return hex.EncodeToString(sum[:])
}

func durationMillis(d time.Duration) uint64 {
	return uint64(d / time.Millisecond) //nolint:gosec // validated positive and bounded by time.Duration.
}

func durationFromMillis(ms uint64) (time.Duration, error) {
	if ms == 0 || ms > uint64((time.Duration(1<<63-1))/time.Millisecond) {
		return 0, errors.New("backup: effective TTL is outside the supported duration range")
	}
	return time.Duration(ms) * time.Millisecond, nil //nolint:gosec // upper bound checked above.
}

type liveBackupLease struct {
	rpc    LiveBackupRPC
	ttl    time.Duration
	cancel context.CancelCauseFunc

	mu        sync.RWMutex
	token     []byte
	expiresAt time.Time
	renewals  uint64
	stopOnce  sync.Once
	stopCh    chan struct{}
	doneCh    chan error
}

func newLiveBackupLease(
	rpc LiveBackupRPC,
	begin *pb.BeginBackupResponse,
	ttl time.Duration,
	cancel context.CancelCauseFunc,
) *liveBackupLease {
	return &liveBackupLease{
		rpc: rpc, ttl: ttl,
		token: append([]byte(nil), begin.GetPinToken()...), expiresAt: time.Now().Add(ttl), cancel: cancel,
		stopCh: make(chan struct{}), doneCh: make(chan error, 1),
	}
}

func (l *liveBackupLease) start(ctx context.Context) {
	go func() { l.doneCh <- l.renewLoop(ctx) }()
}

func (l *liveBackupLease) stop() error {
	l.stopOnce.Do(func() { close(l.stopCh) })
	l.cancel(nil)
	return <-l.doneCh
}

func (l *liveBackupLease) renewLoop(ctx context.Context) error {
	interval, err := liveBackupRenewalInterval(l.ttl)
	if err != nil {
		return l.failRenewal(err)
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-l.stopCh:
			return nil
		case <-timer.C:
			nextInterval, err := l.renewOnce(ctx)
			if err != nil {
				if l.stopping(ctx) {
					return nil
				}
				return l.failRenewal(err)
			}
			timer.Reset(nextInterval)
		}
	}
}

func (l *liveBackupLease) renewOnce(ctx context.Context) (time.Duration, error) {
	l.mu.RLock()
	ttl := l.ttl
	expiresAt := l.expiresAt
	l.mu.RUnlock()
	attemptTimeout, err := liveBackupRenewalAttemptTimeout(ttl, time.Until(expiresAt))
	if err != nil {
		return 0, err
	}
	renewCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
	defer cancel()
	resp, err := l.rpc.RenewBackup(renewCtx, &pb.RenewBackupRequest{
		PinToken: l.tokenSnapshot(), TtlMs: durationMillis(ttl),
	})
	if err != nil {
		return 0, errors.Wrap(err, "renew live backup pin")
	}
	if resp == nil || len(resp.GetPinToken()) == 0 || resp.GetTtlMsEffective() == 0 {
		return 0, errors.New("renew returned incomplete pin metadata")
	}
	renewedTTL, err := durationFromMillis(resp.GetTtlMsEffective())
	if err != nil {
		return 0, err
	}
	interval, err := liveBackupRenewalInterval(renewedTTL)
	if err != nil {
		return 0, err
	}
	l.mu.Lock()
	l.token = append(l.token[:0], resp.GetPinToken()...)
	l.ttl = renewedTTL
	l.expiresAt = time.Now().Add(renewedTTL)
	l.renewals++
	l.mu.Unlock()
	return interval, nil
}

func liveBackupRenewalAttemptTimeout(ttl, remaining time.Duration) (time.Duration, error) {
	timeout := ttl / liveBackupRenewalDivisor
	if remaining < timeout {
		timeout = remaining
	}
	if timeout <= 0 {
		return 0, errors.New("renewal deadline has expired")
	}
	return timeout, nil
}

func liveBackupRenewalInterval(ttl time.Duration) (time.Duration, error) {
	interval := ttl / liveBackupRenewalDivisor
	if interval <= 0 {
		return 0, errors.New("renewal interval is not positive")
	}
	return interval, nil
}

func (l *liveBackupLease) failRenewal(err error) error {
	renewErr := errors.Wrap(ErrLiveBackupRenewal, err.Error())
	l.cancel(renewErr)
	return renewErr
}

func (l *liveBackupLease) stopping(ctx context.Context) bool {
	if ctx.Err() != nil {
		return true
	}
	select {
	case <-l.stopCh:
		return true
	default:
		return false
	}
}

func (l *liveBackupLease) tokenSnapshot() []byte {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return append([]byte(nil), l.token...)
}

func (l *liveBackupLease) renewalCount() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.renewals
}
