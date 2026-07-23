package adapter

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"time"

	logicalbackup "github.com/bootjp/elastickv/internal/backup"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	backupProtocolVersionV1        uint32 = 1
	backupTokenVersion             byte   = 2
	backupTokenHeaderLen                  = 1 + 16 + 8 + 8 + 4
	backupTokenMACLen                     = sha256.Size
	maxBackupTokenGroups                  = 4096
	defaultLiveBackupTTL                  = 30 * time.Minute
	defaultLiveBackupMinTTL               = time.Minute
	defaultLiveBackupMaxTTL               = time.Hour
	defaultLiveBackupBeginDeadline        = 5 * time.Second
	defaultLiveBackupHeadroom             = 1000
	defaultLiveBackupScanPageSize         = 1024
	defaultLiveBackupRenewAttempts        = 3
	defaultLiveBackupRenewBackoff         = 500 * time.Millisecond
	backupAppliedPollInterval             = 10 * time.Millisecond
	defaultLiveBackupMaxActivePins        = 4
)

var (
	ErrBackupUnavailable = errors.New("backup control plane is unavailable")
	ErrBackupToken       = errors.New("backup pin token is invalid")
)

type BackupReadFence func(context.Context) (uint64, error)

type BackupPeerVersion struct {
	NodeVersion           string
	BackupProtocolVersion uint32
}

type BackupPeerProbe func(context.Context, string) (BackupPeerVersion, error)

type BackupStore interface {
	CaptureBackupRouteSnapshotAt(context.Context, uint64) (kv.BackupRouteSnapshot, error)
	ValidateBackupSnapshotAt(context.Context, kv.BackupRouteSnapshot, uint64, int) error
	NewBackupKeyScannerAtSnapshot(snapshot kv.BackupRouteSnapshot, ts uint64, pageSize int) kv.BackupKeyScanner
	NewBackupScannerAtSnapshot(snapshot kv.BackupRouteSnapshot, ts uint64, pageSize int) kv.BackupScanner
}

type filteredBackupScanStore interface {
	NewFilteredBackupScannerAtSnapshot(
		snapshot kv.BackupRouteSnapshot,
		ts uint64,
		pageSize int,
		keyFilter kv.BackupKeyFilter,
	) kv.BackupScanner
}

type backupBaselineSelection struct {
	adapters logicalbackup.AdapterSet
	scopes   map[logicalbackup.Scope]bool
}

type BackupPinLimiter interface {
	PinWithDeadline(pinID kv.BackupPinID, readTS uint64, deadline time.Time) error
	ReleaseBackupPin(pinID kv.BackupPinID)
}

type AdminBackupConfig struct {
	DefaultTTL              time.Duration
	MinTTL                  time.Duration
	MaxTTL                  time.Duration
	BeginDeadline           time.Duration
	SnapshotHeadroomEntries uint64
	ScanPageSize            int
	MaxActivePins           int
	RenewAttempts           int
	RenewBackoff            time.Duration
}

type backupConfig struct {
	defaultTTL              time.Duration
	minTTL                  time.Duration
	maxTTL                  time.Duration
	beginDeadline           time.Duration
	snapshotHeadroomEntries uint64
	scanPageSize            int
	maxActivePins           int
	renewAttempts           int
	renewBackoff            time.Duration
}

func defaultBackupConfig() backupConfig {
	return backupConfig{
		defaultTTL:              defaultLiveBackupTTL,
		minTTL:                  defaultLiveBackupMinTTL,
		maxTTL:                  defaultLiveBackupMaxTTL,
		beginDeadline:           defaultLiveBackupBeginDeadline,
		snapshotHeadroomEntries: defaultLiveBackupHeadroom,
		scanPageSize:            defaultLiveBackupScanPageSize,
		maxActivePins:           defaultLiveBackupMaxActivePins,
		renewAttempts:           defaultLiveBackupRenewAttempts,
		renewBackoff:            defaultLiveBackupRenewBackoff,
	}
}

func WithAdminBackupControl(
	store BackupStore,
	readFence BackupReadFence,
	peerProbe BackupPeerProbe,
	limiter BackupPinLimiter,
	tokenKey []byte,
) AdminOption {
	return func(s *AdminServer) {
		if store == nil || readFence == nil || peerProbe == nil || limiter == nil || len(tokenKey) == 0 {
			return
		}
		s.backupStore = store
		s.backupReadFence = readFence
		s.backupPeerProbe = peerProbe
		s.backupLimiter = limiter
		s.backupTokenKey = sha256.Sum256(tokenKey)
		s.backupProtocolVersion = backupProtocolVersionV1
	}
}

func WithAdminBackupConfig(cfg AdminBackupConfig) AdminOption {
	return func(s *AdminServer) {
		if cfg.DefaultTTL > 0 {
			s.backupConfig.defaultTTL = cfg.DefaultTTL
		}
		if cfg.MinTTL > 0 {
			s.backupConfig.minTTL = cfg.MinTTL
		}
		if cfg.MaxTTL > 0 {
			s.backupConfig.maxTTL = cfg.MaxTTL
		}
		if cfg.BeginDeadline > 0 {
			s.backupConfig.beginDeadline = cfg.BeginDeadline
		}
		if cfg.SnapshotHeadroomEntries > 0 {
			s.backupConfig.snapshotHeadroomEntries = cfg.SnapshotHeadroomEntries
		}
		if cfg.ScanPageSize > 0 {
			s.backupConfig.scanPageSize = cfg.ScanPageSize
		}
		if cfg.MaxActivePins > 0 {
			s.backupConfig.maxActivePins = cfg.MaxActivePins
		}
		if cfg.RenewAttempts > 0 {
			s.backupConfig.renewAttempts = cfg.RenewAttempts
		}
		if cfg.RenewBackoff > 0 {
			s.backupConfig.renewBackoff = cfg.RenewBackoff
		}
	}
}

// RegisterBackupProposer binds the wrap-aware proposer for one group. The
// ordinary AdminGroup remains read-only so existing status-only fakes do not
// gain mutation methods.
func (s *AdminServer) RegisterBackupProposer(groupID uint64, proposer raftengine.Proposer) {
	if s == nil || proposer == nil {
		return
	}
	s.groupsMu.Lock()
	s.backupProposers[groupID] = proposer
	s.groupsMu.Unlock()
}

type backupGroup struct {
	id       uint64
	reader   AdminGroup
	status   raftengine.Status
	every    uint64
	proposer raftengine.Proposer
}

type backupToken struct {
	pinID    kv.BackupPinID
	readTS   uint64
	deadline time.Time
	groupIDs []uint64
}

type backupSession struct {
	routes   kv.BackupRouteSnapshot
	readTS   uint64
	deadline time.Time
}

type preparedBackup struct {
	groups       []backupGroup
	commits      map[uint64]uint64
	controlGroup backupGroup
	pinID        kv.BackupPinID
	readTS       uint64
	ttl          time.Duration
	routes       kv.BackupRouteSnapshot
}

func (s *AdminServer) BeginBackup(ctx context.Context, req *pb.BeginBackupRequest) (*pb.BeginBackupResponse, error) {
	if err := s.requireBackupControl(); err != nil {
		return nil, err
	}
	ttl, err := s.effectiveBackupTTL(req.GetTtlMs())
	if err != nil {
		return nil, err
	}
	selection, err := backupBaselineSelectionFromBeginRequest(req)
	if err != nil {
		return nil, err
	}

	// Serialize BeginBackup on one admin endpoint so local preflight capacity
	// and proposal compensation cannot interleave.
	s.backupMu.Lock()
	defer s.backupMu.Unlock()

	beginCtx, cancel := context.WithTimeout(ctx, s.backupConfig.beginDeadline)
	defer cancel()
	prepared, err := s.prepareBackup(beginCtx, ttl)
	if err != nil {
		return nil, err
	}
	counts, appliedAtCount, err := s.buildExpectedBackupBaseline(ctx, prepared, selection)
	if err != nil {
		s.compensateBackupRelease(prepared.controlGroup, prepared.groups, prepared.pinID)
		return nil, err
	}
	deadline, err := s.renewBackupGroups(ctx, prepared.groups, prepared.pinID, prepared.readTS, prepared.ttl)
	if err != nil {
		s.compensateBackupRelease(prepared.controlGroup, prepared.groups, prepared.pinID)
		return nil, status.Errorf(codes.Unavailable, "refresh backup pin after baseline: %v", err)
	}
	tok := backupToken{
		pinID: prepared.pinID, readTS: prepared.readTS, deadline: deadline,
		groupIDs: backupGroupIDs(prepared.groups),
	}
	encodedToken, err := s.encodeBackupToken(tok)
	if err != nil {
		s.compensateBackupRelease(prepared.controlGroup, prepared.groups, prepared.pinID)
		return nil, status.Errorf(codes.Internal, "encode backup token: %v", err)
	}
	s.rememberBackupSession(tok, prepared.routes)

	return &pb.BeginBackupResponse{
		ReadTs:              prepared.readTS,
		PinToken:            encodedToken,
		TtlMsEffective:      uint64(prepared.ttl / time.Millisecond), //nolint:gosec // validated positive.
		Shards:              backupShardResponses(prepared.groups, prepared.commits),
		ExpectedKeys:        backupExpectedResponses(counts, appliedAtCount),
		MaxActiveBackupPins: uint32(s.backupConfig.maxActivePins), //nolint:gosec // startup validation requires a positive int.
	}, nil
}

func (s *AdminServer) prepareBackup(ctx context.Context, ttl time.Duration) (preparedBackup, error) {
	if err := s.gateBackupPeerVersions(ctx); err != nil {
		return preparedBackup{}, err
	}
	groups, err := s.snapshotBackupGroups()
	if err != nil {
		return preparedBackup{}, err
	}
	if err := s.checkBackupSnapshotHeadroom(groups); err != nil {
		return preparedBackup{}, err
	}
	readTS, err := s.prepareBackupReadTimestamp(ctx)
	if err != nil {
		return preparedBackup{}, err
	}
	pinID, err := newBackupPinID()
	if err != nil {
		return preparedBackup{}, status.Errorf(codes.Internal, "generate backup pin id: %v", err)
	}
	deadline := s.nowSnapshot().Add(ttl)
	controlGroup := groups[0]
	commits, err := s.pinBackupGroups(ctx, groups, controlGroup, pinID, readTS, deadline)
	if err != nil {
		return preparedBackup{}, err
	}
	routes, err := s.captureBackupRoutesAfterPin(ctx, groups, controlGroup, pinID, commits, readTS)
	if err != nil {
		return preparedBackup{}, err
	}
	return preparedBackup{
		groups: groups, commits: commits, controlGroup: controlGroup,
		pinID: pinID, readTS: readTS, ttl: ttl, routes: routes,
	}, nil
}

func (s *AdminServer) prepareBackupReadTimestamp(ctx context.Context) (uint64, error) {
	readTS, err := s.backupReadFence(ctx)
	if err != nil {
		return 0, status.Errorf(codes.FailedPrecondition, "backup read fence failed: %v", err)
	}
	if readTS == 0 || readTS == ^uint64(0) {
		return 0, status.Errorf(codes.FailedPrecondition, "%s", "backup read fence returned an invalid timestamp")
	}
	return readTS, nil
}

func (s *AdminServer) captureBackupRoutesAfterPin(
	ctx context.Context,
	groups []backupGroup,
	controlGroup backupGroup,
	pinID kv.BackupPinID,
	commits map[uint64]uint64,
	readTS uint64,
) (kv.BackupRouteSnapshot, error) {
	if err := waitBackupGroupsApplied(ctx, groups, commits); err != nil {
		s.compensateBackupRelease(controlGroup, groups, pinID)
		return kv.BackupRouteSnapshot{}, status.Errorf(codes.FailedPrecondition, "wait for local backup pin apply: %v", err)
	}
	// Capture catalog ownership only after every data group has applied the
	// pin's timestamp floor. A preallocated catalog write at or below readTS
	// either lands before this point and is visible here, or is rejected by the
	// floor; it cannot race the captured route view after this call returns.
	routes, err := s.backupStore.CaptureBackupRouteSnapshotAt(ctx, readTS)
	if err != nil {
		s.compensateBackupRelease(controlGroup, groups, pinID)
		return kv.BackupRouteSnapshot{}, status.Errorf(codes.FailedPrecondition, "capture backup routes at read timestamp: %v", err)
	}
	return kv.BackupRouteSnapshotWithScanGroups(routes, backupGroupIDs(groups)), nil
}

func (s *AdminServer) pinBackupGroups(
	ctx context.Context,
	groups []backupGroup,
	controlGroup backupGroup,
	pinID kv.BackupPinID,
	readTS uint64,
	deadline time.Time,
) (map[uint64]uint64, error) {
	reserveEntry := kv.EncodeBackupReserveEntry(kv.BackupReserveEntry{PinID: pinID, ReadTS: readTS, Deadline: deadline})
	if _, _, err := proposeBackupAll(ctx, []backupGroup{controlGroup}, reserveEntry); err != nil {
		if backupCapacityReservationFull(err) {
			return nil, status.Errorf(codes.ResourceExhausted, "%s", kv.ErrTooManyActiveBackups)
		}
		return nil, status.Errorf(codes.Unavailable, "reserve backup capacity: %v", err)
	}

	entry := kv.EncodeBackupPinEntry(kv.BackupPinEntry{PinID: pinID, ReadTS: readTS, Deadline: deadline})
	commits, committed, err := proposeBackupAll(ctx, groups, entry)
	if err != nil {
		s.compensateBackupRelease(controlGroup, committed, pinID)
		return nil, status.Errorf(codes.Unavailable, "commit backup pin: %v", err)
	}
	return commits, nil
}

func (s *AdminServer) buildExpectedBackupBaseline(
	ctx context.Context,
	prepared preparedBackup,
	selection backupBaselineSelection,
) (map[logicalbackup.Scope]uint64, uint64, error) {
	stopRenew := make(chan struct{})
	renewDone := make(chan error, 1)
	go func() {
		renewDone <- s.renewBackupLoop(
			ctx, stopRenew, prepared.groups, prepared.pinID, prepared.readTS, prepared.ttl,
		)
	}()
	validateErr := s.backupStore.ValidateBackupSnapshotAt(
		ctx, prepared.routes, prepared.readTS, s.backupConfig.scanPageSize,
	)
	var counts map[logicalbackup.Scope]uint64
	var appliedAtCount uint64
	var scanErr error
	if validateErr != nil {
		scanErr = errors.Wrap(validateErr, "validate backup transaction locks")
	} else {
		counts, appliedAtCount, scanErr = s.scanBackupScopeCounts(
			ctx, prepared.routes, prepared.readTS, prepared.groups, selection, nil,
		)
	}
	close(stopRenew)
	renewErr := <-renewDone
	if scanErr != nil {
		return nil, 0, status.Errorf(codes.FailedPrecondition, "build expected-key baseline: %v", scanErr)
	}
	if renewErr != nil {
		return nil, 0, status.Errorf(codes.Unavailable, "renew backup pin while building baseline: %v", renewErr)
	}
	return counts, appliedAtCount, nil
}

func waitBackupGroupsApplied(ctx context.Context, groups []backupGroup, commits map[uint64]uint64) error {
	for {
		pending := false
		for _, group := range groups {
			target := commits[group.id]
			if group.reader == nil || target == 0 {
				return errors.Wrapf(ErrBackupUnavailable, "raft group %d has no local apply target", group.id)
			}
			if group.reader.Status().AppliedIndex < target {
				pending = true
				break
			}
		}
		if !pending {
			return nil
		}
		timer := time.NewTimer(backupAppliedPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.WithStack(ctx.Err())
		case <-timer.C:
		}
	}
}

func (s *AdminServer) RenewBackup(ctx context.Context, req *pb.RenewBackupRequest) (*pb.RenewBackupResponse, error) {
	if err := s.requireBackupControl(); err != nil {
		return nil, err
	}
	ttl, err := s.effectiveBackupTTL(req.GetTtlMs())
	if err != nil {
		return nil, err
	}
	tok, err := s.decodeBackupToken(req.GetPinToken())
	if err != nil {
		return nil, err
	}
	if err := s.requireUnexpiredBackupToken(tok); err != nil {
		return nil, err
	}
	if _, err := s.backupRouteSnapshotForToken(tok); err != nil {
		return nil, err
	}
	groups, err := s.backupGroupsForToken(tok)
	if err != nil {
		return nil, err
	}
	deadline, err := s.renewBackupGroups(ctx, groups, tok.pinID, tok.readTS, ttl)
	if err != nil {
		s.compensateBackupRelease(groups[0], groups, tok.pinID)
		s.forgetBackupSession(tok.pinID)
		return nil, status.Errorf(codes.Unavailable, "renew backup pin: %v", err)
	}
	if err := s.requireUnexpiredBackupToken(tok); err != nil {
		s.compensateBackupRelease(groups[0], groups, tok.pinID)
		s.forgetBackupSession(tok.pinID)
		return nil, err
	}
	return s.finishRenewBackup(groups, tok, ttl, deadline)
}

func (s *AdminServer) finishRenewBackup(
	groups []backupGroup,
	tok backupToken,
	ttl time.Duration,
	deadline time.Time,
) (*pb.RenewBackupResponse, error) {
	tok.deadline = deadline
	if !s.extendBackupSession(tok) {
		s.compensateBackupRelease(groups[0], groups, tok.pinID)
		return nil, status.Errorf(codes.FailedPrecondition, "%s", "backup pin token has expired")
	}
	encodedToken, err := s.encodeBackupToken(tok)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode renewed backup token: %v", err)
	}
	return &pb.RenewBackupResponse{
		TtlMsEffective: uint64(ttl / time.Millisecond), //nolint:gosec // validated positive.
		PinToken:       encodedToken,
	}, nil
}

func (s *AdminServer) EndBackup(ctx context.Context, req *pb.EndBackupRequest) (*pb.EndBackupResponse, error) {
	if err := s.requireBackupControl(); err != nil {
		return nil, err
	}
	tok, err := s.decodeBackupToken(req.GetPinToken())
	if err != nil {
		return nil, err
	}
	defer s.forgetBackupSession(tok.pinID)
	groups, err := s.backupGroupsForToken(tok)
	if err != nil {
		return nil, err
	}
	entry := kv.EncodeBackupReleaseEntry(kv.BackupReleaseEntry{PinID: tok.pinID})
	_, _, pinErr := proposeBackupAll(ctx, groups, entry)
	unreserve := kv.EncodeBackupUnreserveEntry(kv.BackupUnreserveEntry{PinID: tok.pinID})
	_, _, reserveErr := proposeBackupAll(ctx, groups[:1], unreserve)
	if pinErr != nil {
		return nil, status.Errorf(codes.Unavailable, "release backup pin: %v", pinErr)
	}
	if reserveErr != nil {
		return nil, status.Errorf(codes.Unavailable, "release backup capacity reservation: %v", reserveErr)
	}
	return &pb.EndBackupResponse{}, nil
}

func (s *AdminServer) ListAdaptersAndScopes(
	ctx context.Context,
	req *pb.ListAdaptersAndScopesRequest,
) (*pb.ListAdaptersAndScopesResponse, error) {
	if err := s.requireBackupControl(); err != nil {
		return nil, err
	}
	tok, err := s.decodeBackupToken(req.GetPinToken())
	if err != nil {
		return nil, err
	}
	if err := s.requireUnexpiredBackupToken(tok); err != nil {
		return nil, err
	}
	groups, err := s.backupGroupsForToken(tok)
	if err != nil {
		return nil, err
	}
	routes, err := s.backupRouteSnapshotForToken(tok)
	if err != nil {
		return nil, err
	}
	counts, _, err := s.scanBackupScopeCounts(
		ctx, routes, tok.readTS, groups,
		backupBaselineSelection{adapters: logicalbackup.AllAdapters()},
		func() error {
			return s.requireLiveBackupSession(tok)
		},
	)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "list backup scopes: %v", err)
	}
	scopes := logicalbackup.SortedScopes(counts)
	resp := &pb.ListAdaptersAndScopesResponse{Scopes: make([]*pb.BackupScope, 0, len(scopes))}
	for _, scope := range scopes {
		resp.Scopes = append(resp.Scopes, &pb.BackupScope{Adapter: scope.Adapter, Scope: scope.Name})
	}
	return resp, nil
}

func (s *AdminServer) StreamBackup(
	req *pb.StreamBackupRequest,
	stream grpc.ServerStreamingServer[pb.BackupKV],
) error {
	if err := s.requireBackupControl(); err != nil {
		return err
	}
	tok, err := s.decodeBackupToken(req.GetPinToken())
	if err != nil {
		return err
	}
	if _, err := s.backupGroupsForToken(tok); err != nil {
		return err
	}
	routes, err := s.backupRouteSnapshotForToken(tok)
	if err != nil {
		return err
	}
	if err := s.requireLiveBackupSession(tok); err != nil {
		return err
	}
	selected, err := selectedBackupScopes(req.GetScopes())
	if err != nil {
		return err
	}
	scanner := s.newBackupStreamScanner(routes, tok.readTS, selected)
	if scanner == nil {
		return status.Errorf(codes.Unavailable, "%s", "backup scanner is nil")
	}
	scanErr := streamBackupRecords(stream, scanner, selected, func() error {
		return s.requireLiveBackupSession(tok)
	})
	if err := finishBackupScan(stream.Context(), scanner, scanErr); err != nil {
		if scanErr != nil {
			return err
		}
		return status.Errorf(codes.Internal, "close backup scanner: %v", err)
	}
	return nil
}

func (s *AdminServer) newBackupStreamScanner(
	routes kv.BackupRouteSnapshot,
	readTS uint64,
	selected map[logicalbackup.Scope]bool,
) kv.BackupScanner {
	if len(selected) > 0 {
		if filtered, ok := s.backupStore.(filteredBackupScanStore); ok {
			return filtered.NewFilteredBackupScannerAtSnapshot(routes, readTS, s.backupConfig.scanPageSize, func(key []byte) (bool, error) {
				return backupKeySelected(key, selected)
			})
		}
	}
	return s.backupStore.NewBackupScannerAtSnapshot(routes, readTS, s.backupConfig.scanPageSize)
}

func streamBackupRecords(
	stream grpc.ServerStreamingServer[pb.BackupKV],
	scanner kv.BackupScanner,
	selected map[logicalbackup.Scope]bool,
	requireLive func() error,
) error {
	for {
		if err := requireLive(); err != nil {
			return err
		}
		pair, ok, err := scanner.Next(stream.Context())
		if err != nil {
			return backupScanStreamError(err)
		}
		if !ok {
			return nil
		}
		selectedRecord, err := backupRecordSelected(pair, selected)
		if err != nil {
			return err
		}
		if !selectedRecord {
			continue
		}
		if err := requireLive(); err != nil {
			return err
		}
		if err := stream.Send(&pb.BackupKV{Key: pair.Key, Value: pair.Value}); err != nil {
			return backupSendStreamError(err)
		}
	}
}

func backupRecordSelected(pair *store.KVPair, selected map[logicalbackup.Scope]bool) (bool, error) {
	if pair == nil {
		return false, status.Errorf(codes.Internal, "%s", "backup scanner returned a nil record")
	}
	return backupKeySelected(pair.Key, selected)
}

func backupKeySelected(key []byte, selected map[logicalbackup.Scope]bool) (bool, error) {
	scope, scoped, err := logicalbackup.ScopeForKey(key)
	if err != nil {
		return false, status.Errorf(codes.FailedPrecondition, "classify backup key: %v", err)
	}
	return scoped && (len(selected) == 0 || selected[scope]), nil
}

func backupScanStreamError(err error) error {
	if contextErr := backupContextStreamError(err); contextErr != nil {
		return contextErr
	}
	return status.Errorf(codes.FailedPrecondition, "scan backup at read_ts: %v", err)
}

func backupSendStreamError(err error) error {
	if contextErr := backupContextStreamError(err); contextErr != nil {
		return contextErr
	}
	return errors.WithStack(err)
}

func backupContextStreamError(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return status.Errorf(codes.Canceled, "%s", err)
	case errors.Is(err, context.DeadlineExceeded):
		return status.Errorf(codes.DeadlineExceeded, "%s", err)
	default:
		return nil
	}
}

func selectedBackupScopes(scopes []*pb.BackupScope) (map[logicalbackup.Scope]bool, error) {
	selected := make(map[logicalbackup.Scope]bool, len(scopes))
	for _, scope := range scopes {
		if scope == nil || scope.GetAdapter() == "" || scope.GetScope() == "" {
			return nil, status.Errorf(codes.InvalidArgument, "%s", "backup scope requires adapter and scope")
		}
		switch scope.GetAdapter() {
		case "dynamodb", "s3", "redis", "sqs":
		default:
			return nil, status.Errorf(codes.InvalidArgument, "unknown backup adapter %q", scope.GetAdapter())
		}
		selected[logicalbackup.Scope{Adapter: scope.GetAdapter(), Name: scope.GetScope()}] = true
	}
	return selected, nil
}

func backupBaselineSelectionFromBeginRequest(req *pb.BeginBackupRequest) (backupBaselineSelection, error) {
	adapters, err := backupAdaptersFromNames(req.GetAdapters())
	if err != nil {
		return backupBaselineSelection{}, err
	}
	scopes, err := selectedBackupScopes(req.GetScopes())
	if err != nil {
		return backupBaselineSelection{}, err
	}
	for scope := range scopes {
		if !logicalbackup.AdapterEnabled(adapters, scope.Adapter) {
			return backupBaselineSelection{}, status.Errorf(codes.InvalidArgument, "%s is excluded by adapters", scope.Adapter)
		}
	}
	return backupBaselineSelection{adapters: adapters, scopes: scopes}, nil
}

func backupAdaptersFromNames(names []string) (logicalbackup.AdapterSet, error) {
	if len(names) == 0 {
		return logicalbackup.AllAdapters(), nil
	}
	var out logicalbackup.AdapterSet
	for _, name := range names {
		switch name {
		case "dynamodb":
			out.DynamoDB = true
		case "s3":
			out.S3 = true
		case "redis":
			out.Redis = true
		case "sqs":
			out.SQS = true
		default:
			return logicalbackup.AdapterSet{}, status.Errorf(codes.InvalidArgument, "unknown backup adapter %q", name)
		}
	}
	if out == (logicalbackup.AdapterSet{}) {
		return logicalbackup.AdapterSet{}, status.Errorf(codes.InvalidArgument, "%s", "backup requires at least one adapter")
	}
	return out, nil
}

func backupBaselineKeySelected(key []byte, selection backupBaselineSelection) (bool, error) {
	adapter, ok := logicalbackup.AdapterForKey(key)
	if !ok {
		return false, nil
	}
	if !logicalbackup.AdapterEnabled(selection.adapters, adapter) {
		return false, nil
	}
	scope, scoped, err := logicalbackup.ScopeForKey(key)
	if err != nil {
		return false, errors.Wrap(err, "classify backup baseline key")
	}
	if !scoped {
		return false, nil
	}
	if len(selection.scopes) > 0 && !selection.scopes[scope] {
		return false, nil
	}
	return true, nil
}

func (s *AdminServer) requireBackupControl() error {
	if s == nil || s.backupStore == nil || s.backupReadFence == nil || s.backupPeerProbe == nil || s.backupLimiter == nil || s.backupTokenKey == ([32]byte{}) {
		return status.Errorf(codes.Unavailable, "%s", ErrBackupUnavailable)
	}
	return nil
}

func (s *AdminServer) effectiveBackupTTL(ttlMS uint64) (time.Duration, error) {
	if ttlMS == 0 {
		return s.backupConfig.defaultTTL, nil
	}
	if ttlMS > uint64(math.MaxInt64/int64(time.Millisecond)) { //nolint:gosec // positive constant ratio.
		return 0, status.Errorf(codes.InvalidArgument, "%s", "backup ttl_ms overflows duration")
	}
	ttl := time.Duration(ttlMS) * time.Millisecond //nolint:gosec // bounded above before conversion.
	if ttl < s.backupConfig.minTTL || ttl > s.backupConfig.maxTTL {
		return 0, status.Errorf(codes.InvalidArgument, "backup ttl must be between %s and %s", s.backupConfig.minTTL, s.backupConfig.maxTTL)
	}
	return ttl, nil
}

func (s *AdminServer) nowSnapshot() time.Time {
	s.groupsMu.RLock()
	now := s.now
	s.groupsMu.RUnlock()
	return now()
}

func (s *AdminServer) snapshotBackupGroups() ([]backupGroup, error) {
	s.groupsMu.RLock()
	groups := make([]backupGroup, 0, len(s.groups))
	for id, group := range s.groups {
		// Group zero is reserved for TSO state and has no user keyspace.
		// Backup tokens also reserve zero as an invalid group ID.
		if id == 0 {
			continue
		}
		proposer := s.backupProposers[id]
		if group == nil || proposer == nil {
			s.groupsMu.RUnlock()
			return nil, status.Errorf(codes.Unavailable, "backup proposer unavailable for raft group %d", id)
		}
		groups = append(groups, backupGroup{id: id, reader: group, status: group.Status(), every: group.SnapshotEvery(), proposer: proposer})
	}
	s.groupsMu.RUnlock()
	if len(groups) == 0 {
		return nil, status.Errorf(codes.Unavailable, "%s", "no raft groups registered for backup")
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].id < groups[j].id })
	return groups, nil
}

func (s *AdminServer) checkBackupSnapshotHeadroom(groups []backupGroup) error {
	for _, group := range groups {
		if group.every == 0 {
			continue
		}
		if group.status.AppliedIndex < group.status.LastSnapshotIndex {
			return status.Errorf(codes.FailedPrecondition, "raft group %d reports applied index below snapshot index", group.id)
		}
		used := group.status.AppliedIndex - group.status.LastSnapshotIndex
		remaining := uint64(0)
		if used < group.every {
			remaining = group.every - used
		}
		if remaining < s.backupConfig.snapshotHeadroomEntries {
			return status.Errorf(codes.FailedPrecondition, "raft group %d has %d snapshot entries remaining; need %d", group.id, remaining, s.backupConfig.snapshotHeadroomEntries)
		}
	}
	return nil
}

func (s *AdminServer) gateBackupPeerVersions(ctx context.Context) error {
	members, err := s.snapshotBackupMembers(ctx)
	if err != nil {
		return err
	}
	type result struct {
		nodeID  string
		version BackupPeerVersion
		err     error
	}
	results := make(chan result, len(members))
	md, _ := metadata.FromIncomingContext(ctx)
	launched := 0
	for _, member := range members {
		if member == nil {
			continue
		}
		launched++
		if member.GetNodeId() == s.self.NodeID {
			results <- result{nodeID: member.GetNodeId(), version: BackupPeerVersion{
				NodeVersion: s.nodeVersion, BackupProtocolVersion: s.backupProtocolVersion,
			}}
			continue
		}
		go func() {
			peerCtx, cancel := context.WithTimeout(ctx, s.backupConfig.beginDeadline)
			defer cancel()
			peerCtx = metadata.NewOutgoingContext(peerCtx, md.Copy())
			version, err := s.backupPeerProbe(peerCtx, member.GetGrpcAddress())
			results <- result{nodeID: member.GetNodeId(), version: version, err: err}
		}()
	}
	failures := make([]string, 0)
	for i := 0; i < launched; i++ {
		result := <-results
		switch {
		case result.err != nil:
			failures = append(failures, fmt.Sprintf("node %s did not respond within %s: %v", result.nodeID, s.backupConfig.beginDeadline, result.err))
		case result.version.BackupProtocolVersion < backupProtocolVersionV1:
			failures = append(failures, fmt.Sprintf("node %s reports version %s with backup protocol %d; minimum protocol is %d", result.nodeID, result.version.NodeVersion, result.version.BackupProtocolVersion, backupProtocolVersionV1))
		}
	}
	if len(failures) > 0 {
		sort.Strings(failures)
		return status.Errorf(codes.FailedPrecondition, "%s", failures[0])
	}
	return nil
}

// snapshotBackupMembers is the fail-closed counterpart to snapshotMembers.
// Capability gating must observe every Raft Configuration before proposing a
// backup FSM entry; the overview RPC intentionally retains its tolerant view.
func (s *AdminServer) snapshotBackupMembers(ctx context.Context) ([]*pb.NodeIdentity, error) {
	groups := s.cloneGroupsSorted()
	if len(groups) == 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "%s", "backup membership has no registered raft groups")
	}
	results := fanoutConfigurationCalls(ctx, groups)
	if len(results) != len(groups) {
		return nil, status.Errorf(codes.FailedPrecondition, "backup membership incomplete: received %d of %d raft configurations", len(results), len(groups))
	}
	sort.Slice(results, func(i, j int) bool { return results[i].i < results[j].i })
	live := liveMembers{
		addrByID: make(map[string]string),
		seenID:   make(map[string]struct{}),
		order:    make([]string, 0),
	}
	for _, result := range results {
		if result.i < 0 || result.i >= len(groups) {
			return nil, status.Errorf(codes.FailedPrecondition, "%s", "backup membership returned an invalid raft group result")
		}
		if result.err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "backup membership for raft group %d: %v", groups[result.i].id, result.err)
		}
		mergeBackupConfiguration(result.cfg, s.self.NodeID, &live)
	}
	live.authoritative = true
	mergeSeedMembers(s.members, s.self.NodeID, &live)
	appendMissingBackupMembers(&live)

	out := make([]*pb.NodeIdentity, 0, len(live.order))
	for _, id := range live.order {
		address := live.addrByID[id]
		if address == "" {
			return nil, status.Errorf(codes.FailedPrecondition, "backup membership for node %s has no gRPC address", id)
		}
		out = append(out, &pb.NodeIdentity{NodeId: id, GrpcAddress: address})
	}
	return out, nil
}

func appendMissingBackupMembers(live *liveMembers) {
	missingAddresses := make([]string, 0)
	for id := range live.seenID {
		if _, hasAddress := live.addrByID[id]; !hasAddress {
			missingAddresses = append(missingAddresses, id)
		}
	}
	sort.Strings(missingAddresses)
	live.order = append(live.order, missingAddresses...)
}

func mergeBackupConfiguration(cfg raftengine.Configuration, selfID string, live *liveMembers) {
	for _, server := range cfg.Servers {
		if server.ID == "" || server.ID == selfID {
			continue
		}
		live.seenID[server.ID] = struct{}{}
		if server.Address == "" {
			continue
		}
		if _, exists := live.addrByID[server.ID]; exists {
			continue
		}
		live.addrByID[server.ID] = server.Address
		live.order = append(live.order, server.ID)
	}
}

func proposeBackupAll(ctx context.Context, groups []backupGroup, entry []byte) (map[uint64]uint64, []backupGroup, error) {
	type result struct {
		group backupGroup
		index uint64
		err   error
	}
	results := make(chan result, len(groups))
	for _, group := range groups {
		go func() {
			proposal, err := group.proposer.ProposeAdmin(ctx, entry)
			if err == nil {
				err = backupProposalResponseError(proposal)
			}
			index := uint64(0)
			if proposal != nil {
				index = proposal.CommitIndex
			}
			results <- result{group: group, index: index, err: err}
		}()
	}
	commits := make(map[uint64]uint64, len(groups))
	committed := make([]backupGroup, 0, len(groups))
	var firstErr error
	for range groups {
		result := <-results
		if result.err != nil {
			if firstErr == nil {
				firstErr = backupProposalGroupError(result.group.id, result.err)
			}
			continue
		}
		commits[result.group.id] = result.index
		committed = append(committed, result.group)
	}
	return commits, committed, firstErr
}

func backupProposalResponseError(result *raftengine.ProposalResult) error {
	if result == nil {
		return errors.New("raft proposal returned nil result")
	}
	if result.Response == nil {
		return nil
	}
	if err, ok := result.Response.(error); ok {
		return errors.WithStack(err)
	}
	return errors.Wrapf(ErrBackupUnavailable, "unexpected backup apply response %T", result.Response)
}

func backupProposalGroupError(groupID uint64, err error) error {
	if status.Code(err) == codes.ResourceExhausted {
		return status.Errorf(codes.ResourceExhausted, "raft group %d: %v", groupID, err)
	}
	return errors.Wrapf(err, "raft group %d", groupID)
}

func backupCapacityReservationFull(err error) bool {
	return errors.Is(err, kv.ErrTooManyActiveBackups) || status.Code(err) == codes.ResourceExhausted
}

func (s *AdminServer) compensateBackupRelease(control backupGroup, groups []backupGroup, pinID kv.BackupPinID) {
	ctx, cancel := context.WithTimeout(context.Background(), s.backupConfig.beginDeadline)
	defer cancel()
	if len(groups) > 0 {
		entry := kv.EncodeBackupReleaseEntry(kv.BackupReleaseEntry{PinID: pinID})
		_, _, _ = proposeBackupAll(ctx, groups, entry)
	}
	unreserve := kv.EncodeBackupUnreserveEntry(kv.BackupUnreserveEntry{PinID: pinID})
	_, _, _ = proposeBackupAll(ctx, []backupGroup{control}, unreserve)
}

func (s *AdminServer) proposeBackupWithRetry(
	ctx context.Context,
	groups []backupGroup,
	entry []byte,
) (map[uint64]uint64, error) {
	pending := append([]backupGroup(nil), groups...)
	commits := make(map[uint64]uint64, len(groups))
	var firstErr error
	for attempt := 0; attempt < s.backupConfig.renewAttempts && len(pending) > 0; attempt++ {
		attemptCommits, committed, err := proposeBackupAll(ctx, pending, entry)
		for groupID, index := range attemptCommits {
			commits[groupID] = index
		}
		if err == nil {
			return commits, nil
		}
		firstErr = err
		pending = remainingBackupGroups(pending, committed)
		if len(pending) > 0 && attempt+1 < s.backupConfig.renewAttempts {
			if err := waitBackupRetry(ctx, s.backupConfig.renewBackoff); err != nil {
				return commits, err
			}
		}
	}
	return commits, firstErr
}

func remainingBackupGroups(pending, committed []backupGroup) []backupGroup {
	committedIDs := make(map[uint64]struct{}, len(committed))
	for _, group := range committed {
		committedIDs[group.id] = struct{}{}
	}
	next := pending[:0]
	for _, group := range pending {
		if _, ok := committedIDs[group.id]; !ok {
			next = append(next, group)
		}
	}
	return next
}

func waitBackupRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-timer.C:
		return nil
	}
}

func (s *AdminServer) renewBackupGroups(
	ctx context.Context,
	groups []backupGroup,
	pinID kv.BackupPinID,
	readTS uint64,
	ttl time.Duration,
) (time.Time, error) {
	if len(groups) == 0 {
		return time.Time{}, errors.New("no raft groups available for backup renewal")
	}
	deadline := s.nowSnapshot().Add(ttl)
	reserveEntry := kv.EncodeBackupReserveEntry(kv.BackupReserveEntry{PinID: pinID, ReadTS: readTS, Deadline: deadline})
	if _, err := s.proposeBackupWithRetry(ctx, groups[:1], reserveEntry); err != nil {
		return time.Time{}, errors.Wrap(err, "capacity reservation")
	}
	// Reapply the complete pin rather than only its deadline. This restores a
	// missing replica-local fence after partial delivery while duplicate Pin
	// apply preserves the earliest read timestamp and latest deadline.
	pinEntry := kv.EncodeBackupPinEntry(kv.BackupPinEntry{PinID: pinID, ReadTS: readTS, Deadline: deadline})
	commits, err := s.proposeBackupWithRetry(ctx, groups, pinEntry)
	if err != nil {
		return time.Time{}, errors.Wrap(err, "group pins")
	}
	if err := waitBackupGroupsApplied(ctx, groups, commits); err != nil {
		return time.Time{}, errors.Wrap(err, "wait for renewed group pins")
	}
	return deadline, nil
}

func (s *AdminServer) renewBackupLoop(
	ctx context.Context,
	stop <-chan struct{},
	groups []backupGroup,
	pinID kv.BackupPinID,
	readTS uint64,
	ttl time.Duration,
) error {
	interval := ttl / 3 //nolint:mnd // live-backup contract renews at one third of TTL.
	if interval <= 0 {
		return errors.New("backup renewal interval is not positive")
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case <-stop:
			return nil
		case <-timer.C:
			if _, err := s.renewBackupGroups(ctx, groups, pinID, readTS, ttl); err != nil {
				return err
			}
			timer.Reset(interval)
		}
	}
}

func (s *AdminServer) scanBackupScopeCounts(
	ctx context.Context,
	routes kv.BackupRouteSnapshot,
	readTS uint64,
	groups []backupGroup,
	selection backupBaselineSelection,
	requireLive func() error,
) (counts map[logicalbackup.Scope]uint64, applied uint64, retErr error) {
	if selection.adapters == (logicalbackup.AdapterSet{}) {
		selection.adapters = logicalbackup.AllAdapters()
	}
	counter, err := logicalbackup.NewLiveScopeCounter(selection.adapters)
	if err != nil {
		return nil, 0, errors.Wrap(err, "create retained backup baseline counter")
	}
	metadataKeys, err := s.scanBackupScopeKeys(ctx, routes, readTS, selection, counter, requireLive)
	if err != nil {
		return nil, 0, err
	}
	if err := s.scanBackupScopeMetadata(ctx, routes, readTS, metadataKeys, counter, requireLive); err != nil {
		return nil, 0, err
	}
	if err := requireBackupScopeScanLive(requireLive); err != nil {
		return nil, 0, err
	}
	applied, err = currentMinBackupAppliedIndex(groups)
	if err != nil {
		return nil, 0, err
	}
	counts, err = counter.RetainedCounts()
	if err != nil {
		return nil, 0, errors.Wrap(err, "finalize retained backup baseline")
	}
	return counts, applied, nil
}

func (s *AdminServer) scanBackupScopeKeys(
	ctx context.Context,
	routes kv.BackupRouteSnapshot,
	readTS uint64,
	selection backupBaselineSelection,
	counter *logicalbackup.LiveScopeCounter,
	requireLive func() error,
) (metadataKeys map[string]struct{}, retErr error) {
	scanner := s.backupStore.NewBackupKeyScannerAtSnapshot(routes, readTS, s.backupConfig.scanPageSize)
	if scanner == nil {
		return nil, errors.New("backup key scanner is nil")
	}
	defer func() {
		retErr = finishBackupScan(ctx, scanner, retErr)
	}()
	metadataKeys = make(map[string]struct{})
	for {
		if err := requireBackupScopeScanLive(requireLive); err != nil {
			return nil, err
		}
		key, ok, err := scanner.Next(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "scan backup baseline")
		}
		if !ok {
			break
		}
		selected, err := backupBaselineKeySelected(key, selection)
		if err != nil {
			return nil, errors.Wrap(err, "select backup baseline key")
		}
		if !selected {
			continue
		}
		needsValue, err := counter.AddKey(key)
		if err != nil {
			return nil, errors.Wrap(err, "count retained backup baseline key")
		}
		if needsValue {
			metadataKeys[string(key)] = struct{}{}
		}
	}
	return metadataKeys, requireBackupScopeScanLive(requireLive)
}

func (s *AdminServer) scanBackupScopeMetadata(
	ctx context.Context,
	routes kv.BackupRouteSnapshot,
	readTS uint64,
	metadataKeys map[string]struct{},
	counter *logicalbackup.LiveScopeCounter,
	requireLive func() error,
) (retErr error) {
	if len(metadataKeys) == 0 {
		return requireBackupScopeScanLive(requireLive)
	}
	scanner := s.newBackupBaselineMetadataScanner(routes, readTS, metadataKeys)
	if scanner == nil {
		return errors.New("backup metadata scanner is nil")
	}
	defer func() {
		retErr = finishBackupScan(ctx, scanner, retErr)
	}()
	for {
		if err := requireBackupScopeScanLive(requireLive); err != nil {
			return err
		}
		pair, ok, err := scanner.Next(ctx)
		if err != nil {
			return errors.Wrap(err, "scan backup baseline metadata")
		}
		if !ok {
			break
		}
		if pair == nil {
			return errors.New("backup metadata scanner returned a nil record")
		}
		if _, ok := metadataKeys[string(pair.Key)]; !ok {
			continue
		}
		if err := counter.AddValue(pair.Key, pair.Value); err != nil {
			return errors.Wrap(err, "count retained backup baseline metadata")
		}
	}
	return requireBackupScopeScanLive(requireLive)
}

func requireBackupScopeScanLive(requireLive func() error) error {
	if requireLive == nil {
		return nil
	}
	return requireLive()
}

func (s *AdminServer) newBackupBaselineMetadataScanner(
	routes kv.BackupRouteSnapshot,
	readTS uint64,
	metadataKeys map[string]struct{},
) kv.BackupScanner {
	if filtered, ok := s.backupStore.(filteredBackupScanStore); ok {
		return filtered.NewFilteredBackupScannerAtSnapshot(routes, readTS, s.backupConfig.scanPageSize, func(key []byte) (bool, error) {
			_, ok := metadataKeys[string(key)]
			return ok, nil
		})
	}
	return s.backupStore.NewBackupScannerAtSnapshot(routes, readTS, s.backupConfig.scanPageSize)
}

func (s *AdminServer) rememberBackupSession(tok backupToken, routes kv.BackupRouteSnapshot) {
	now := s.nowSnapshot()
	s.backupStateMu.Lock()
	defer s.backupStateMu.Unlock()
	s.reapBackupSessionsLocked(now)
	if s.backupSessions == nil {
		s.backupSessions = make(map[kv.BackupPinID]backupSession)
	}
	s.backupSessions[tok.pinID] = backupSession{routes: routes, readTS: tok.readTS, deadline: tok.deadline}
}

func (s *AdminServer) backupRouteSnapshotForToken(tok backupToken) (kv.BackupRouteSnapshot, error) {
	now := s.nowSnapshot()
	s.backupStateMu.Lock()
	defer s.backupStateMu.Unlock()
	s.reapBackupSessionsLocked(now)
	session, ok := s.backupSessions[tok.pinID]
	if !ok || session.readTS != tok.readTS {
		return kv.BackupRouteSnapshot{}, status.Errorf(codes.FailedPrecondition, "%s", "backup route snapshot is unavailable on this endpoint")
	}
	return session.routes, nil
}

func (s *AdminServer) requireLiveBackupSession(tok backupToken) error {
	now := s.nowSnapshot()
	s.backupStateMu.Lock()
	defer s.backupStateMu.Unlock()
	session, ok := s.backupSessions[tok.pinID]
	if !ok || session.readTS != tok.readTS {
		return status.Errorf(codes.FailedPrecondition, "%s", "backup pin token has expired")
	}
	if session.deadline.IsZero() || !now.Before(session.deadline) {
		delete(s.backupSessions, tok.pinID)
		return status.Errorf(codes.FailedPrecondition, "%s", "backup pin token has expired")
	}
	s.reapBackupSessionsLocked(now)
	return nil
}

func (s *AdminServer) extendBackupSession(tok backupToken) bool {
	s.backupStateMu.Lock()
	defer s.backupStateMu.Unlock()
	session, ok := s.backupSessions[tok.pinID]
	if !ok || session.readTS != tok.readTS {
		return false
	}
	if tok.deadline.After(session.deadline) {
		session.deadline = tok.deadline
		s.backupSessions[tok.pinID] = session
	}
	return true
}

func (s *AdminServer) forgetBackupSession(pinID kv.BackupPinID) {
	s.backupStateMu.Lock()
	delete(s.backupSessions, pinID)
	s.backupStateMu.Unlock()
}

func (s *AdminServer) reapBackupSessionsLocked(now time.Time) {
	for pinID, session := range s.backupSessions {
		if !now.Before(session.deadline) {
			delete(s.backupSessions, pinID)
		}
	}
}

func finishBackupScan(ctx context.Context, scanner interface{ Close() error }, scanErr error) error {
	closeErr := scanner.Close()
	if closeErr == nil {
		return scanErr
	}
	closeErr = errors.Wrap(closeErr, "close backup scanner")
	if scanErr == nil {
		return closeErr
	}
	slog.ErrorContext(ctx, "backup scanner cleanup failed after scan error", "err", closeErr)
	return scanErr
}

func currentMinBackupAppliedIndex(groups []backupGroup) (uint64, error) {
	var min uint64
	for _, group := range groups {
		if group.reader == nil {
			return 0, errors.Wrapf(ErrBackupUnavailable, "raft group %d status reader is nil", group.id)
		}
		idx := group.reader.Status().AppliedIndex
		if min == 0 || idx < min {
			min = idx
		}
	}
	return min, nil
}

func backupExpectedResponses(counts map[logicalbackup.Scope]uint64, applied uint64) []*pb.BackupExpectedKeys {
	scopes := logicalbackup.SortedScopes(counts)
	out := make([]*pb.BackupExpectedKeys, 0, len(scopes))
	for _, scope := range scopes {
		out = append(out, &pb.BackupExpectedKeys{
			Adapter: scope.Adapter, Scope: scope.Name, KeyCount: counts[scope], AppliedIndexAtCount: applied,
		})
	}
	return out
}

func backupShardResponses(groups []backupGroup, commits map[uint64]uint64) []*pb.BackupShardApplied {
	out := make([]*pb.BackupShardApplied, 0, len(groups))
	for _, group := range groups {
		idx := commits[group.id]
		if idx == 0 {
			idx = group.status.AppliedIndex
		}
		out = append(out, &pb.BackupShardApplied{RaftGroupId: group.id, AppliedIndex: idx})
	}
	return out
}

func backupGroupIDs(groups []backupGroup) []uint64 {
	ids := make([]uint64, 0, len(groups))
	for _, group := range groups {
		ids = append(ids, group.id)
	}
	return ids
}

func newBackupPinID() (kv.BackupPinID, error) {
	var id kv.BackupPinID
	if _, err := rand.Read(id[:]); err != nil {
		return kv.BackupPinID{}, errors.WithStack(err)
	}
	if id.IsZero() {
		return kv.BackupPinID{}, errors.New("random backup pin id is zero")
	}
	return id, nil
}

func (s *AdminServer) encodeBackupToken(tok backupToken) ([]byte, error) {
	deadlineMS := tok.deadline.UnixMilli()
	if tok.pinID.IsZero() || tok.readTS == 0 || deadlineMS <= 0 || len(tok.groupIDs) == 0 || len(tok.groupIDs) > maxBackupTokenGroups {
		return nil, errors.WithStack(ErrBackupToken)
	}
	buf := make([]byte, backupTokenHeaderLen+len(tok.groupIDs)*8+backupTokenMACLen) //nolint:mnd // uint64 group IDs.
	buf[0] = backupTokenVersion
	copy(buf[1:17], tok.pinID[:])
	binary.BigEndian.PutUint64(buf[17:25], tok.readTS)
	binary.BigEndian.PutUint64(buf[25:33], uint64(deadlineMS))        //nolint:gosec // positive above.
	binary.BigEndian.PutUint32(buf[33:37], uint32(len(tok.groupIDs))) //nolint:gosec // capped above.
	offset := backupTokenHeaderLen
	var previous uint64
	for i, groupID := range tok.groupIDs {
		if groupID == 0 || (i > 0 && groupID <= previous) {
			return nil, errors.WithStack(ErrBackupToken)
		}
		binary.BigEndian.PutUint64(buf[offset:offset+8], groupID) //nolint:mnd // uint64 width.
		offset += 8
		previous = groupID
	}
	mac := hmac.New(sha256.New, s.backupTokenKey[:])
	_, _ = mac.Write(buf[:offset])
	copy(buf[offset:], mac.Sum(nil))
	return buf, nil
}

func (s *AdminServer) decodeBackupToken(raw []byte) (backupToken, error) {
	count, payloadEnd, ok := backupTokenEnvelope(raw)
	if !ok {
		return backupToken{}, invalidBackupTokenError()
	}
	mac := hmac.New(sha256.New, s.backupTokenKey[:])
	_, _ = mac.Write(raw[:payloadEnd])
	if !hmac.Equal(raw[payloadEnd:], mac.Sum(nil)) {
		return backupToken{}, invalidBackupTokenError()
	}
	var tok backupToken
	copy(tok.pinID[:], raw[1:17])
	tok.readTS = binary.BigEndian.Uint64(raw[17:25])
	deadlineMS := binary.BigEndian.Uint64(raw[25:33])
	if tok.pinID.IsZero() || tok.readTS == 0 || deadlineMS == 0 || deadlineMS > math.MaxInt64 {
		return backupToken{}, invalidBackupTokenError()
	}
	tok.deadline = time.UnixMilli(int64(deadlineMS)) //nolint:gosec // bounded above.
	tok.groupIDs, ok = decodeBackupTokenGroupIDs(raw, count)
	if !ok {
		return backupToken{}, invalidBackupTokenError()
	}
	return tok, nil
}

func backupTokenEnvelope(raw []byte) (int, int, bool) {
	if len(raw) < backupTokenHeaderLen+8+backupTokenMACLen || raw[0] != backupTokenVersion { //nolint:mnd // one group minimum.
		return 0, 0, false
	}
	count := int(binary.BigEndian.Uint32(raw[33:37]))
	expectedLen := backupTokenHeaderLen + count*8 + backupTokenMACLen //nolint:mnd // uint64 group IDs.
	if count <= 0 || count > maxBackupTokenGroups || len(raw) != expectedLen {
		return 0, 0, false
	}
	return count, len(raw) - backupTokenMACLen, true
}

func decodeBackupTokenGroupIDs(raw []byte, count int) ([]uint64, bool) {
	groupIDs := make([]uint64, 0, count)
	offset := backupTokenHeaderLen
	var previous uint64
	for i := 0; i < count; i++ {
		groupID := binary.BigEndian.Uint64(raw[offset : offset+8]) //nolint:mnd // uint64 width.
		if groupID == 0 || (i > 0 && groupID <= previous) {
			return nil, false
		}
		groupIDs = append(groupIDs, groupID)
		previous = groupID
		offset += 8
	}
	return groupIDs, true
}

func invalidBackupTokenError() error {
	return status.Errorf(codes.InvalidArgument, "%s", ErrBackupToken)
}

func (s *AdminServer) requireUnexpiredBackupToken(tok backupToken) error {
	if tok.deadline.IsZero() || !s.nowSnapshot().Before(tok.deadline) {
		return status.Errorf(codes.FailedPrecondition, "%s", "backup pin token has expired")
	}
	return nil
}

func (s *AdminServer) backupGroupsForToken(tok backupToken) ([]backupGroup, error) {
	groups, err := s.snapshotBackupGroups()
	if err != nil {
		return nil, err
	}
	byID := make(map[uint64]backupGroup, len(groups))
	for _, group := range groups {
		byID[group.id] = group
	}
	out := make([]backupGroup, 0, len(tok.groupIDs))
	for _, id := range tok.groupIDs {
		group, ok := byID[id]
		if !ok {
			return nil, status.Errorf(codes.FailedPrecondition, "backup token references unavailable raft group %d", id)
		}
		out = append(out, group)
	}
	return out, nil
}
