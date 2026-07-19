package adapter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"runtime/debug"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/bootjp/elastickv/internal/admin"
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// EncryptionAdminServer implements proto.EncryptionAdmin. Wires
// the §6.1 read-only probes (GetCapability, GetSidecarState,
// ResyncSidecar) plus the three mutating opcodes (Bootstrap,
// RotateDEK, RegisterEncryptionWriter) onto Stage 3's raft
// envelope and Stage 4's fsmwire body encoders. Mutators are
// leader-gated through requireLeader / VerifyLeader.
type EncryptionAdminServer struct {
	sidecarPath        string
	fullNodeID         uint64
	writerRegistry     encryption.WriterRegistryStore
	buildSHA           string
	latestAppliedIndex func() uint64
	// proposer is the raw raft proposer used for cleartext-only
	// control entries. The EnableRaftEnvelope cutover marker MUST
	// use this path so the marker at index == cutover remains
	// cleartext for the strict-`>` unwrap hook.
	proposer raftengine.Proposer
	// postCutoverProposer is the wrap-aware admin proposal path for
	// all non-cutover-marker control-plane entries. It defaults to
	// proposer for backward compatibility; production wiring replaces
	// it with the ShardGroup.Proposer() chain so RotateDEK /
	// RegisterEncryptionWriter entries after the raft cutover carry
	// the raft envelope.
	postCutoverProposer raftengine.Proposer
	leaderView          raftengine.LeaderView
	// recoveryLeaderView is the authority for sidecar/registry recovery.
	// In multi-group deployments it points at the default group even when
	// this server is registered on another group's listener.
	recoveryLeaderView raftengine.LeaderView
	// capabilityFanout, when wired, runs the §4 Voters ∪ Learners
	// fan-out before the §7.1 Phase 1 cutover entry is proposed.
	// A nil value short-circuits EnableStorageEnvelope with
	// FailedPrecondition — the 6D-6 main.go wiring (lands in 6D-6c)
	// is what threads the route-snapshot builder + DialFunc +
	// timeout into this closure. Other mutator RPCs are unaffected.
	capabilityFanout CapabilityFanoutFn
	// cutoverSem serializes concurrent EnableStorageEnvelope
	// calls per design §2.1 #4 ("The cutover-RPC mutator
	// serializes overlapping calls on the propose side").
	// Without serialization, two concurrent cutover calls could
	// both observe StorageEnvelopeActive=false, both propose, and
	// the loser would assemble a freshCutoverResponse with
	// was_already_active=false but the FIRST cutover's
	// applied_index — violating the §6.4 fresh-success contract
	// (coderabbit Major on PR812).
	//
	// A capacity-1 channel rather than a plain sync.Mutex so
	// acquisition can honor ctx cancellation: an admin RPC with
	// a short deadline whose mutator lock is held by an in-flight
	// fan-out + propose call surfaces Canceled / DeadlineExceeded
	// at the gRPC boundary rather than blocking indefinitely.
	//
	// The same semaphore also serializes RotateDEK and
	// RegisterEncryptionWriter around their proposal step. Before the
	// raft-envelope wrap is installed, barrier-exempt admin proposals
	// must not slip in at indexes greater than the raft cutover marker
	// while the wrap-aware proposer still has nil wrap.
	cutoverSem chan struct{}
	// unsafeRaftCutoverAdminBlocked latches after EnableRaftEnvelope
	// reaches an ambiguous/post-propose failure that leaves the user
	// barrier open. Once set, non-cutover admin proposals must fail
	// closed instead of committing cleartext above a marker whose
	// eventual outcome is unknown on this process.
	unsafeRaftCutoverAdminBlocked atomic.Bool
	// cutoverBarrier drives the §7.1 6-step quiescence barrier that
	// EnableRaftEnvelope wraps around its cutover proposal. nil means
	// the barrier is unwired — EnableRaftEnvelope refuses with
	// FailedPrecondition before composing any proposal (matches the
	// proposer / leaderView posture for the other mutator RPCs).
	//
	// Production wiring fans this out over every ShardGroup that
	// participates in the cutover so the barrier engages all leaders
	// for the few-ms cutover window. A nil controller still fails
	// closed before any proposal is composed.
	cutoverBarrier CutoverBarrierController
	pb.UnimplementedEncryptionAdminServer
}

// CutoverBarrierController coordinates the §7.1 quiescence barrier
// across every ShardGroup that participates in the raft-envelope
// cutover. EnableRaftEnvelope drives the 6-step sequence:
//
//	Begin()          // step 1: block USER proposals
//	WaitDrained(ctx) // step 2: drain in-flight
//	<handler proposes cutover entry via ProposeAdmin>
//	<handler waits for FSM apply via latestAppliedIndex>
//	InstallWrap()    // step 5: SetRaftPayloadWrap on each group
//	End()            // step 6: unblock USER proposals
//
// Production wiring (6E-2e: main.go) fans the controller over every
// participating ShardGroup so each leader's
// dynamicWrappedProposer.Propose engages the gate. Tests stub the
// interface to drive the state machine without spinning real engines.
//
// All four methods MUST be safe to call from one handler goroutine
// at a time (per-handler serialization via cutoverSem); the
// controller does not need internal locking against re-entry from
// itself. Cross-goroutine reads of the underlying state (Propose
// gate check, drain signal) ARE concurrent and rely on the
// controller's own happens-before ordering.
type CutoverBarrierController interface {
	// Begin opens the barrier on every participating ShardGroup.
	// Returns a channel that closes when in-flight drains. The
	// typical caller uses WaitDrained instead so context cancellation
	// composes; the channel is exposed so callers MAY use a select
	// with other signals when needed.
	Begin() <-chan struct{}
	// WaitDrained blocks until in-flight drains or ctx fires.
	// Returns nil on drain, wrapped ctx.Err() on cancellation. A
	// barrier-incapable controller (test fixture) may degrade to
	// immediate-success; production controllers MUST honour the
	// drain semantic so the handler doesn't propose the cutover
	// while user proposals are still landing.
	WaitDrained(ctx context.Context) error
	// InstallWrap publishes the active raft envelope wrap closure
	// on every participating ShardGroup, in step 5 of the barrier
	// sequence (after the cutover entry has both committed AND
	// applied locally). The closure source is owned by the
	// controller, not the EncryptionAdminServer — 6E-2e populates
	// it via main.go wiring from the sidecar's Active.Raft DEK.
	InstallWrap()
	// End closes the barrier on every participating ShardGroup.
	// Idempotent against double-End. Pair with Begin via defer in
	// the handler.
	End()
}

type cutoverBarrierScopeValidator interface {
	ValidateCutoverScope() error
}

// CapabilityFanoutFn is the closure the server invokes to run the
// §4 Voters ∪ Learners pre-flight before the cutover proposal.
// Production wiring composes it from
// internal/admin.CapabilityFanout(routes, dial, timeout) where
// routes is built from the Raft engine's live membership view and
// dial reuses the existing admin connection pool. Tests stub it
// with a deterministic result to exercise the §4.3 OK / refuse
// branches at the RPC layer without spinning real clients.
type CapabilityFanoutFn func(ctx context.Context) (admin.CapabilityFanoutResult, error)

// EncryptionAdminServerOption configures EncryptionAdminServer behavior.
type EncryptionAdminServerOption func(*EncryptionAdminServer)

// WithEncryptionAdminSidecarPath sets the §5.1 keys.json path the
// server reads on every GetCapability / GetSidecarState / ResyncSidecar
// call. An empty path means "encryption is not configured on this
// node"; GetCapability then returns encryption_capable=false instead
// of erroring.
func WithEncryptionAdminSidecarPath(path string) EncryptionAdminServerOption {
	return func(s *EncryptionAdminServer) {
		s.sidecarPath = path
	}
}

// WithEncryptionAdminFullNodeID sets the 64-bit node id reported in
// CapabilityReport.full_node_id. The §5.6 step 1a batch keys writer
// registry entries on (dek_id, uint16(full_node_id)); the leader
// derives the uint16 narrowing itself, so the server-side value
// stays at the full 64-bit precision.
func WithEncryptionAdminFullNodeID(id uint64) EncryptionAdminServerOption {
	return func(s *EncryptionAdminServer) {
		s.fullNodeID = id
	}
}

// WithEncryptionAdminWriterRegistry wires the §4.1 writer-registry
// store that read-only sidecar recovery RPCs use to project
// writer_registry_for_caller. A nil argument is a no-op so tests and
// encryption-disabled nodes keep the pre-Stage-7 empty-map posture.
func WithEncryptionAdminWriterRegistry(reg encryption.WriterRegistryStore) EncryptionAdminServerOption {
	return func(s *EncryptionAdminServer) {
		if reg == nil {
			return
		}
		s.writerRegistry = reg
	}
}

// WithEncryptionAdminBuildSHA overrides the auto-detected
// runtime/debug build SHA. Tests use this to pin a deterministic
// value; production wiring leaves it empty.
func WithEncryptionAdminBuildSHA(sha string) EncryptionAdminServerOption {
	return func(s *EncryptionAdminServer) {
		s.buildSHA = sha
	}
}

// WithEncryptionAdminLatestAppliedIndex registers a callback the
// server uses to populate SidecarStateReport.latest_applied_index
// and ResyncSidecarResponse.leader_latest_applied_index. A nil
// callback (the default) reports the value persisted in the
// sidecar, which lags the in-memory apply counter; the callback is
// the §5.5 escape hatch for callers that want the freshest value.
func WithEncryptionAdminLatestAppliedIndex(fn func() uint64) EncryptionAdminServerOption {
	return func(s *EncryptionAdminServer) {
		s.latestAppliedIndex = fn
	}
}

// WithEncryptionAdminProposer registers the raftengine.Proposer the
// server uses to propose the §11.3 0x03 / 0x04 / 0x05 entries
// (Stage 4 wire format). An unset proposer makes every mutating
// RPC return FailedPrecondition with "proposer not configured",
// preserving the PR-A production-inert guarantee until Stage 6
// flips the cluster flag.
func WithEncryptionAdminProposer(p raftengine.Proposer) EncryptionAdminServerOption {
	return func(s *EncryptionAdminServer) {
		s.proposer = p
	}
}

// WithEncryptionAdminPostCutoverProposer registers the wrap-aware
// proposer used for non-cutover-marker admin entries. A nil argument
// is a no-op, and NewEncryptionAdminServer falls back to the raw
// proposer when this option is not supplied, preserving the Stage 6B
// mutator wiring contract.
func WithEncryptionAdminPostCutoverProposer(p raftengine.Proposer) EncryptionAdminServerOption {
	return func(s *EncryptionAdminServer) {
		if p == nil {
			return
		}
		s.postCutoverProposer = p
	}
}

// WithEncryptionAdminCapabilityFanout wires the §4 Voters ∪
// Learners pre-flight that the §7.1 Phase 1 cutover RPC
// (EnableStorageEnvelope) runs before composing the
// RotateSubEnableStorageEnvelope payload. Without this option
// EnableStorageEnvelope refuses with FailedPrecondition —
// matching the proposer / leaderView posture for the other
// mutator RPCs. A nil argument is a no-op (the server stays in
// the cutover-disabled posture), mirroring the
// WithEncryptionAdmin* convention.
func WithEncryptionAdminCapabilityFanout(fn CapabilityFanoutFn) EncryptionAdminServerOption {
	return func(s *EncryptionAdminServer) {
		if fn == nil {
			return
		}
		s.capabilityFanout = fn
	}
}

// WithEncryptionAdminLeaderView registers the leadership oracle.
// Mutating RPCs and ResyncSidecar reject on followers with
// FailedPrecondition; the leader's id and address are embedded in
// the status detail so the operator's CLI can retry against the
// right node without parsing free-form error text.
func WithEncryptionAdminLeaderView(v raftengine.LeaderView) EncryptionAdminServerOption {
	return func(s *EncryptionAdminServer) {
		s.leaderView = v
	}
}

// WithEncryptionAdminRecoveryLeaderView registers the default-group
// leadership oracle used by ResyncSidecar. A nil value preserves the
// single-group fallback to leaderView.
func WithEncryptionAdminRecoveryLeaderView(v raftengine.LeaderView) EncryptionAdminServerOption {
	return func(s *EncryptionAdminServer) {
		if v != nil {
			s.recoveryLeaderView = v
		}
	}
}

// WithEncryptionAdminCutoverBarrier wires the §7.1 quiescence
// barrier controller used by EnableRaftEnvelope. A nil argument is
// a no-op (the server stays in the cutover-disabled posture);
// EnableRaftEnvelope refuses with FailedPrecondition until both
// raftEnvelopeWrapEnabled is open AND this controller is wired.
//
// Production wiring (6E-2e: main.go) fans the controller over every
// ShardGroup that participates in the cutover. Tests pass a stub
// implementation to exercise the state machine deterministically.
func WithEncryptionAdminCutoverBarrier(c CutoverBarrierController) EncryptionAdminServerOption {
	return func(s *EncryptionAdminServer) {
		if c == nil {
			return
		}
		s.cutoverBarrier = c
	}
}

// NewEncryptionAdminServer constructs an EncryptionAdminServer. The
// returned server is safe to register on the same gRPC listener as
// Admin / Distribution; every RPC is concurrency-safe because the
// only mutable state is the sidecar file, which encryption.WriteSidecar
// already serialises via the §5.1 crash-durable write protocol.
//
// Production wiring MUST call Validate after construction so a
// configuration that wires a proposer but forgets the leaderView
// fails closed at startup rather than silently letting followers
// mutate state.
func NewEncryptionAdminServer(opts ...EncryptionAdminServerOption) *EncryptionAdminServer {
	// cutoverSem buffer of 1 = mutual-exclusion semaphore;
	// initialized eagerly so EnableStorageEnvelope never has to
	// nil-check (a nil chan send blocks forever, defeating the
	// ctx-aware acquire below).
	s := &EncryptionAdminServer{cutoverSem: make(chan struct{}, 1)}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	if s.buildSHA == "" {
		s.buildSHA = autoBuildSHA()
	}
	if s.postCutoverProposer == nil {
		s.postCutoverProposer = s.proposer
	}
	return s
}

// Validate enforces the option-pairing invariants the constructor
// cannot express through Go's option signature. Tests that wire a
// proposer without a leaderView are intentionally allowed (the
// requireLeader path treats nil-leaderView as always-leader for
// the proposer-wiring unit tests); production wiring in main.go
// MUST call this method so a misconfiguration fails closed.
func (s *EncryptionAdminServer) Validate() error {
	if s.proposer != nil && s.leaderView == nil {
		return errors.New("encryption: proposer wired without leaderView — followers must not mutate state; call WithEncryptionAdminLeaderView")
	}
	if s.postCutoverProposer != nil && s.proposer == nil {
		return errors.New("encryption: post-cutover proposer wired without raw proposer — call WithEncryptionAdminProposer")
	}
	return nil
}

// GetCapability returns the §6.1 CapabilityReport for the local
// node. The RPC is read-only and side-effect-free; it must be
// servable on every node regardless of leadership state so the
// §7.1 cutover command can fan it out across voters and learners.
//
// encryption_capable is gated on "this node was restarted with
// --encryption-enabled" (proxied here by a non-empty sidecar path),
// NOT on whether the sidecar has been bootstrapped. The §7.1 Phase
// 0 capability gate fires before bootstrap proposes the first DEK,
// so gating on Active.Storage != 0 would deadlock the cutover
// against the very entry it is gating. SidecarPresent then carries
// the orthogonal "has the bootstrap entry landed yet" signal.
//
// A node that has never been started with --encryption-enabled
// (empty sidecarPath) returns encryption_capable=false with
// local_epoch=0 and full_node_id=0 per the §6.1 contract: the
// cutover command refuses with ErrCapabilityCheckFailed in that
// case so the empty epoch never reaches the writer registry.
func (s *EncryptionAdminServer) GetCapability(_ context.Context, _ *pb.Empty) (*pb.CapabilityReport, error) {
	if s.sidecarPath == "" {
		return &pb.CapabilityReport{BuildSha: s.buildSHA}, nil
	}
	report := &pb.CapabilityReport{
		EncryptionCapable: true,
		BuildSha:          s.buildSHA,
		FullNodeId:        s.fullNodeID,
		// LocalEpoch stays at 0 until Stage 7 wires the §4.1
		// writer-registry counter. The §5.6 step 1a pre-check
		// happens before any DEK exists, so 0 is the correct
		// value at the cutover-time read regardless.
		LocalEpoch: 0,
	}
	_, err := encryption.ReadSidecar(s.sidecarPath)
	switch {
	case err == nil:
		report.SidecarPresent = true
	case errors.Is(err, os.ErrNotExist) || encryption.IsNotExist(err):
		// Phase 0 window: --encryption-enabled is set but the
		// bootstrap entry has not yet created the sidecar.
		// Capable, not present.
		report.SidecarPresent = false
	default:
		return nil, statusFromSidecarErr(err)
	}
	return report, nil
}

// GetSidecarState returns the §5.5 compaction-fallback snapshot. The
// response includes every unretired wrapped DEK plus the two active
// pointers; the wrapped material is leakage-safe because it is
// KEK-wrapped, which is the same property the on-disk sidecar has.
//
// When the writer registry is wired, writer_registry_for_caller
// carries this node's recorded last_seen_local_epoch per sidecar DEK.
// Unwired tests and encryption-disabled nodes keep the historical
// empty non-nil map.
func (s *EncryptionAdminServer) GetSidecarState(_ context.Context, _ *pb.Empty) (*pb.SidecarStateReport, error) {
	if s.sidecarPath == "" {
		return nil, grpcStatusError(codes.FailedPrecondition, "encryption: sidecar path is not configured on this node")
	}
	sc, err := encryption.ReadSidecar(s.sidecarPath)
	if err != nil {
		return nil, statusFromSidecarErr(err)
	}
	writerRegistry, err := s.writerRegistryForCaller(sc, s.fullNodeID, codes.Internal)
	if err != nil {
		return nil, err
	}
	resp := &pb.SidecarStateReport{
		ActiveStorageId:          sc.Active.Storage,
		ActiveRaftId:             sc.Active.Raft,
		StorageEnvelopeActive:    sc.StorageEnvelopeActive,
		RaftEnvelopeCutoverIndex: sc.RaftEnvelopeCutoverIndex,
		LatestAppliedIndex:       s.appliedIndex(sc.RaftAppliedIndex),
		WrappedDeksById:          wrappedDEKMap(sc),
		WriterRegistryForCaller:  writerRegistry,
	}
	return resp, nil
}

// ResyncSidecar is the §5.5 follower-repair RPC. The follower asks
// the leader for the current wrapped DEK set so it can rewrite a
// sidecar that fell behind a Raft-log compaction window. The RPC is
// read-only on the server side; no Raft proposal is involved.
//
// Leader-only via requireLeader, which calls VerifyLeader(ctx) to
// confirm leadership through a Raft ReadIndex round-trip. Without
// the quorum check a partitioned former leader (State() still
// reports StateLeader pre-step-down) could ship stale wrapped-DEK
// state to a recovering follower and silently overwrite recent
// rotations.
func (s *EncryptionAdminServer) ResyncSidecar(ctx context.Context, req *pb.ResyncSidecarRequest) (*pb.ResyncSidecarResponse, error) {
	if err := s.requireRecoveryLeader(ctx); err != nil {
		return nil, err
	}
	if s.sidecarPath == "" {
		return nil, grpcStatusError(codes.FailedPrecondition, "encryption: sidecar path is not configured on this node")
	}
	sc, err := encryption.ReadSidecar(s.sidecarPath)
	if err != nil {
		return nil, statusFromSidecarErr(err)
	}
	var callerFullNodeID uint64
	if req != nil {
		callerFullNodeID = req.GetCallerFullNodeId()
	}
	writerRegistry, err := s.writerRegistryForCaller(sc, callerFullNodeID, codes.InvalidArgument)
	if err != nil {
		return nil, err
	}
	return &pb.ResyncSidecarResponse{
		WrappedDeksById:          wrappedDEKMap(sc),
		ActiveStorageId:          sc.Active.Storage,
		ActiveRaftId:             sc.Active.Raft,
		LeaderLatestAppliedIndex: s.appliedIndex(sc.RaftAppliedIndex),
		WriterRegistryForCaller:  writerRegistry,
	}, nil
}

func (s *EncryptionAdminServer) writerRegistryForCaller(sc *encryption.Sidecar, fullNodeID uint64, missingIDCode codes.Code) (map[uint32]uint32, error) {
	out := map[uint32]uint32{}
	if s.writerRegistry == nil {
		return out, nil
	}
	if fullNodeID == 0 {
		return nil, grpcStatusError(missingIDCode,
			"encryption: full_node_id is required to project writer_registry_for_caller")
	}
	nodeID16 := encryption.NodeID16(fullNodeID)
	for idStr := range sc.Keys {
		dekID, err := parseSidecarKeyID(idStr)
		if err != nil {
			return nil, grpcStatusErrorf(codes.Internal,
				"encryption: sidecar key id %q could not be projected into writer registry: %v", idStr, err)
		}
		raw, ok, err := s.writerRegistry.GetRegistryRow(encryption.RegistryKey(dekID, nodeID16))
		if err != nil {
			return nil, grpcStatusErrorf(codes.Internal,
				"encryption: read writer registry row for dek_id=%d full_node_id=%#x: %v", dekID, fullNodeID, err)
		}
		if !ok {
			continue
		}
		row, err := encryption.DecodeRegistryValue(raw)
		if err != nil {
			return nil, grpcStatusErrorf(codes.Internal,
				"encryption: decode writer registry row for dek_id=%d full_node_id=%#x: %v", dekID, fullNodeID, err)
		}
		if row.FullNodeID != fullNodeID {
			return nil, grpcStatusErrorf(codes.Internal,
				"encryption: writer registry node_id collision for dek_id=%d caller_full_node_id=%#x registry_full_node_id=%#x",
				dekID, fullNodeID, row.FullNodeID)
		}
		out[dekID] = uint32(row.LastSeenLocalEpoch)
	}
	return out, nil
}

func (s *EncryptionAdminServer) appliedIndex(sidecarValue uint64) uint64 {
	if s.latestAppliedIndex == nil {
		return sidecarValue
	}
	return s.latestAppliedIndex()
}

// wrappedDEKMap projects the sidecar's string-keyed wrapped DEK
// table back into the uint32-keyed proto map. validateSidecar has
// already rejected non-decimal keys and key_id 0, so the
// strconv.ParseUint here can only fail on values the validator
// missed — treat any such failure as a programming error and skip
// the entry rather than letting the RPC fall over. The skip is
// logged at error level so a future sidecar-format migration that
// outpaces validateSidecar is visible in operator logs rather
// than appearing as a silently shrunken wrapped_deks_by_id map.
func wrappedDEKMap(sc *encryption.Sidecar) map[uint32][]byte {
	out := make(map[uint32][]byte, len(sc.Keys))
	for idStr, key := range sc.Keys {
		id, err := parseSidecarKeyID(idStr)
		if err != nil {
			slog.Error("encryption: dropping malformed sidecar key id from wrapped_deks_by_id",
				"id", idStr, "err", err)
			continue
		}
		dup := make([]byte, len(key.Wrapped))
		copy(dup, key.Wrapped)
		out[id] = dup
	}
	return out
}

// parseSidecarKeyID enforces the §5.1 invariant that map keys are
// decimal uint32 strings. strconv.ParseUint with base=10, bits=32
// rejects negatives, non-decimal characters, and overflow in one
// step; validateSidecar has already filtered these out at read
// time, so a failure here only fires on programming errors.
func parseSidecarKeyID(s string) (uint32, error) {
	const (
		base = 10
		bits = 32
	)
	v, err := strconv.ParseUint(s, base, bits)
	if err != nil {
		return 0, errors.New("invalid sidecar key id")
	}
	return uint32(v), nil
}

func statusFromSidecarErr(err error) error {
	switch {
	case errors.Is(err, os.ErrNotExist) || encryption.IsNotExist(err):
		return grpcStatusError(codes.FailedPrecondition, "encryption: sidecar file is missing")
	case errors.Is(err, encryption.ErrSidecarVersion):
		return grpcStatusErrorf(codes.FailedPrecondition, "encryption: %v", err)
	default:
		return grpcStatusErrorf(codes.Internal, "encryption: read sidecar: %v", err)
	}
}

type typedStatusError struct {
	err      error
	sentinel error
}

func (e *typedStatusError) Error() string {
	return e.err.Error()
}

func (e *typedStatusError) Unwrap() error {
	return e.err
}

func (e *typedStatusError) Is(target error) bool {
	return target == e.sentinel || errors.Is(e.err, target)
}

func (e *typedStatusError) GRPCStatus() *status.Status {
	st, _ := status.FromError(e.err)
	return st
}

func markStatusError(err error, sentinel error) error {
	return &typedStatusError{err: err, sentinel: sentinel}
}

func localEpochOutOfRangeStatusf(format string, args ...any) error {
	return markStatusError(
		grpcStatusErrorf(codes.InvalidArgument, format, args...),
		encryption.ErrLocalEpochOutOfRange)
}

func encryptionNotBootstrappedStatus(msg string) error {
	return markStatusError(
		grpcStatusError(codes.FailedPrecondition, msg),
		encryption.ErrEncryptionNotBootstrapped)
}

// BootstrapEncryption proposes the §5.6 0x04 OpBootstrap entry
// that installs the initial wrapped DEK pair AND the cluster-wide
// writer-registry batch in a single Raft transaction. The server
// accepts a pre-built writer batch (capability fan-out is the
// CLI's job today) and validates every entry against the §6.1
// invariants before delegating to fsmwire.EncodeBootstrap. FSM
// apply enforces the idempotency check (rejects if the sidecar's
// active.storage is already set), so re-running this RPC against
// an already-bootstrapped cluster is safe — the engine returns
// the apply error and ErrEncryptionApply halts the apply loop.
func (s *EncryptionAdminServer) BootstrapEncryption(ctx context.Context, req *pb.BootstrapEncryptionRequest) (*pb.BootstrapEncryptionResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, err
	}
	if s.proposer == nil {
		return nil, grpcStatusError(codes.FailedPrecondition, "encryption: proposer is not configured on this node")
	}
	if req.GetStorageDekId() == 0 {
		return nil, grpcStatusError(codes.InvalidArgument, "encryption: storage_dek_id must be non-zero (key id 0 is reserved per §5.1)")
	}
	if req.GetRaftDekId() == 0 {
		return nil, grpcStatusError(codes.InvalidArgument, "encryption: raft_dek_id must be non-zero (key id 0 is reserved per §5.1)")
	}
	if req.GetStorageDekId() == req.GetRaftDekId() {
		return nil, grpcStatusErrorf(codes.InvalidArgument,
			"encryption: storage_dek_id and raft_dek_id must differ; got both %d",
			req.GetStorageDekId())
	}
	if err := validateWrappedDEK("storage", req.GetWrappedStorageDek()); err != nil {
		return nil, err
	}
	if err := validateWrappedDEK("raft", req.GetWrappedRaftDek()); err != nil {
		return nil, err
	}
	batch, err := buildBootstrapBatch(req.GetWriterBatch(), req.GetStorageDekId(), req.GetRaftDekId())
	if err != nil {
		return nil, err
	}
	payload := fsmwire.BootstrapPayload{
		StorageDEKID:   req.GetStorageDekId(),
		WrappedStorage: req.GetWrappedStorageDek(),
		RaftDEKID:      req.GetRaftDekId(),
		WrappedRaft:    req.GetWrappedRaftDek(),
		BatchRegistry:  batch,
	}
	body := fsmwire.EncodeBootstrap(payload)
	idx, err := s.proposeSerializedEncryptionEntry(ctx, fsmwire.OpBootstrap, body)
	if err != nil {
		return nil, err
	}
	return &pb.BootstrapEncryptionResponse{AppliedIndex: idx}, nil
}

// buildBootstrapBatch projects the proto writer batch into the
// §11.3 0x04 fsmwire form. Each member is expanded into TWO
// registry rows (one per active dek_id — storage and raft) so the
// §4.1 nonce-uniqueness invariant holds for both envelope
// purposes from the first post-bootstrap entry. The result size
// is bounded by fsmwire.maxBootstrapBatchCount (2 × len(writers))
// — checked at the gRPC boundary so a too-large batch is rejected
// before the Propose round-trip.
func buildBootstrapBatch(writers []*pb.WriterRegistryEntry, storageDEKID, raftDEKID uint32) ([]fsmwire.RegistrationPayload, error) {
	if len(writers) == 0 {
		return nil, grpcStatusError(codes.InvalidArgument,
			"encryption: writer_batch must contain at least one member (per §5.6 step 1a)")
	}
	totalRows := bootstrapRowsPerWriter * len(writers)
	if totalRows > bootstrapBatchRowCap {
		return nil, grpcStatusErrorf(codes.InvalidArgument,
			"encryption: writer_batch produces %d registry rows, exceeding the §4.1 bound %d",
			totalRows, bootstrapBatchRowCap)
	}
	// Per-writer shape validation first (rejects nil entries, zero
	// node id, oversize epoch) so the uniqueness pass below can
	// assume every entry is non-nil and well-formed. Both passes
	// run before we allocate the output slice — a near-cap batch
	// with a collision at the end is rejected without building
	// thousands of throwaway registry rows.
	validated := make([]fsmwire.RegistrationPayload, len(writers))
	for i, w := range writers {
		row, err := validateBootstrapWriter(i, w)
		if err != nil {
			return nil, err
		}
		validated[i] = row
	}
	if err := validateWriterBatchUniqueness(writers); err != nil {
		return nil, err
	}
	out := make([]fsmwire.RegistrationPayload, 0, totalRows)
	for _, w := range validated {
		out = append(out,
			fsmwire.RegistrationPayload{DEKID: storageDEKID, FullNodeID: w.FullNodeID, LocalEpoch: w.LocalEpoch},
			fsmwire.RegistrationPayload{DEKID: raftDEKID, FullNodeID: w.FullNodeID, LocalEpoch: w.LocalEpoch},
		)
	}
	return out, nil
}

// validateBootstrapWriter enforces the per-writer invariants the
// §5.6 step 1a batch must satisfy before it can be expanded into
// the two-row-per-writer bootstrap layout. Returns a partial
// RegistrationPayload populated with the validated fields; the
// caller supplies dek_id when building the storage / raft rows.
func validateBootstrapWriter(i int, w *pb.WriterRegistryEntry) (fsmwire.RegistrationPayload, error) {
	if w == nil {
		return fsmwire.RegistrationPayload{}, grpcStatusErrorf(codes.InvalidArgument,
			"encryption: writer_batch[%d] is nil", i)
	}
	if w.GetFullNodeId() == 0 {
		return fsmwire.RegistrationPayload{}, grpcStatusErrorf(codes.InvalidArgument,
			"encryption: writer_batch[%d].full_node_id must be non-zero (0 is reserved as the §6.1 not-capable sentinel)", i)
	}
	if w.GetLocalEpoch() > math.MaxUint16 {
		return fsmwire.RegistrationPayload{}, localEpochOutOfRangeStatusf(
			"encryption: writer_batch[%d].local_epoch=%d exceeds the §4.1 16-bit bound (max 0xFFFF)",
			i, w.GetLocalEpoch())
	}
	return fsmwire.RegistrationPayload{
		FullNodeID: w.GetFullNodeId(),
		LocalEpoch: uint32ToLocalEpoch(w.GetLocalEpoch()),
	}, nil
}

// bootstrapRowsPerWriter is the §5.6 step 1a fan-out per member:
// one storage-DEK row + one raft-DEK row so the §4.1
// nonce-uniqueness invariant covers both envelope purposes from
// the first post-bootstrap entry.
const bootstrapRowsPerWriter = 2

// maxWrappedDEKSize bounds the KEK-wrapped DEK payload size at the
// gRPC boundary so a crafted oversized blob cannot push
// fsmwire.EncodeBootstrap toward its safeU32 length-prefix guard
// or inflate the resulting Raft entry beyond practical limits.
// A real KEK-wrapped 32-byte DEK runs hundreds of bytes (KMS
// metadata + AES-GCM tag + envelope header); 4 KiB is ~10x
// headroom and far below the gRPC default 4 MiB message cap.
const maxWrappedDEKSize = 4096

func validateWrappedDEK(purpose string, wrapped []byte) error {
	if len(wrapped) == 0 {
		return grpcStatusErrorf(codes.InvalidArgument,
			"encryption: wrapped_%s_dek is required", purpose)
	}
	if len(wrapped) > maxWrappedDEKSize {
		return grpcStatusErrorf(codes.InvalidArgument,
			"encryption: wrapped_%s_dek size %d exceeds the %d-byte gRPC-boundary cap",
			purpose, len(wrapped), maxWrappedDEKSize)
	}
	return nil
}

// validateWriterBatchUniqueness rejects writer batches that would
// fail closed at FSM apply time: §4.1 case-3 (the same
// full_node_id appearing twice → ErrLocalEpochRollback) and §4.1
// case-4 (two distinct full_node_ids colliding on the uint16
// narrowing used as the registry-row key → ErrNodeIDCollision).
// FSM apply errors halt the apply loop via HaltApply, so a
// malformed bootstrap that reached apply would stop the cluster.
//
// The caller (buildBootstrapBatch) runs validateBootstrapWriter
// over the slice first, so every entry here is guaranteed
// non-nil with a non-zero full_node_id — no defensive nil-guard
// is needed inside this loop.
func validateWriterBatchUniqueness(writers []*pb.WriterRegistryEntry) error {
	seenFull := make(map[uint64]int, len(writers))
	seenTrunc := make(map[uint16]uint64, len(writers))
	for i, w := range writers {
		id := w.GetFullNodeId()
		if prev, dup := seenFull[id]; dup {
			return grpcStatusErrorf(codes.InvalidArgument,
				"encryption: writer_batch[%d] duplicates full_node_id %d already present at index %d",
				i, id, prev)
		}
		trunc := uint16(id & localEpochMaskU64) //nolint:gosec // masked to 16 bits; G115 cannot trace the bitwise narrowing
		if prevFull, collision := seenTrunc[trunc]; collision {
			return grpcStatusErrorf(codes.InvalidArgument,
				"encryption: writer_batch[%d].full_node_id=%d collides with %d on the uint16 narrowing 0x%04x (registry key)",
				i, id, prevFull, trunc)
		}
		seenFull[id] = i
		seenTrunc[trunc] = id
	}
	return nil
}

// localEpochMaskU64 mirrors localEpochMask in uint64 form for the
// uint64→uint16 narrowing used by the registry row key. Named
// here so the magic-number linter does not flag the masking
// conversion below.
const localEpochMaskU64 uint64 = 0xFFFF

// bootstrapBatchRowCap mirrors fsmwire.maxBootstrapBatchCount.
// The §5.6 step 1a bootstrap produces (storage + raft) rows per
// member, so the practical member count cap is half this value.
// Named here to avoid the magic-number linter and to keep the
// boundary check next to where it fires.
const bootstrapBatchRowCap = 1 << 14

// RotateDEK proposes a §5.2 rotation as a §11.3 0x05 OpRotation
// entry. The server validates purpose / dek_id / local_epoch
// boundaries at the gRPC boundary (the §6.1 doc rule for
// local_epoch <= 0xFFFF on the wire) and the FSM apply layer
// enforces the sidecar-level invariants (no rotation while a
// previous rotation is in flight, etc). Returns the commit index
// once the entry is durable on a Raft quorum.
func (s *EncryptionAdminServer) RotateDEK(ctx context.Context, req *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, err
	}
	if s.proposer == nil {
		return nil, grpcStatusError(codes.FailedPrecondition, "encryption: proposer is not configured on this node")
	}
	purpose, err := protoRotatePurpose(req.GetPurpose())
	if err != nil {
		return nil, err
	}
	if req.GetNewDekId() == 0 {
		return nil, grpcStatusError(codes.InvalidArgument, "encryption: new_dek_id must be non-zero (key id 0 is reserved per §5.1)")
	}
	if err := validateWrappedDEK("new", req.GetWrappedNewDek()); err != nil {
		return nil, err
	}
	if req.GetProposerLocalEpoch() > math.MaxUint16 {
		return nil, localEpochOutOfRangeStatusf(
			"encryption: proposer_local_epoch=%d exceeds the §4.1 16-bit bound (max 0xFFFF)",
			req.GetProposerLocalEpoch())
	}
	if req.GetProposerNodeId() == 0 {
		// §6.1 reserves full_node_id=0 as the "not encryption-capable"
		// sentinel. Forwarding 0 into a writer-registry row would
		// collide with every other un-bootstrapped node's sentinel
		// and break the §4.1 nonce-uniqueness invariant.
		return nil, grpcStatusError(codes.InvalidArgument,
			"encryption: proposer_node_id must be non-zero (0 is reserved as the §6.1 not-capable sentinel)")
	}
	payload := fsmwire.RotationPayload{
		SubTag:  fsmwire.RotateSubRotateDEK,
		DEKID:   req.GetNewDekId(),
		Purpose: purpose,
		Wrapped: req.GetWrappedNewDek(),
		ProposerRegistration: fsmwire.RegistrationPayload{
			DEKID:      req.GetNewDekId(),
			FullNodeID: req.GetProposerNodeId(),
			LocalEpoch: uint32ToLocalEpoch(req.GetProposerLocalEpoch()),
		},
	}
	body := fsmwire.EncodeRotation(payload)
	idx, err := s.proposeSerializedEncryptionEntry(ctx, fsmwire.OpRotation, body)
	if err != nil {
		return nil, err
	}
	return &pb.RotateDEKResponse{AppliedIndex: idx}, nil
}

// EnableStorageEnvelope is the §7.1 Phase 1 cutover RPC: it
// proposes a RotateSubEnableStorageEnvelope (0x04) rotation entry
// that flips sidecar.StorageEnvelopeActive on every replica
// simultaneously and stores the original cutover index in
// sidecar.StorageEnvelopeCutoverIndex (§6.4). After the entry
// applies, the storage layer's WithStorageEnvelopeGate (Stage 6D-5)
// reads true on every Put and the cluster begins emitting §4.1
// envelopes for new versions.
//
// The RPC composes the §4 Voters ∪ Learners capability fan-out
// helper (Stage 6D-3), the Stage 6D-4 wire dispatch, and the
// idempotency contract (§6.4): a duplicate call against an
// already-active sidecar returns OK with `was_already_active=true`
// and `applied_index = sidecar.StorageEnvelopeCutoverIndex`.
// Returning AlreadyExists instead would drop the response body
// per unary-gRPC semantics, so the idempotency discriminator
// lives on the success path.
//
// The server-side sequence (per design doc §3.2):
//
//  1. Validate `proposer_node_id != 0` and `proposer_local_epoch
//     <= 0xFFFF` at the gRPC boundary.
//  2. Verify Stage 6B mutators are enabled — implicit via
//     `s.proposer == nil` (matches RotateDEK / BootstrapEncryption
//     posture).
//  3. Verify we are the default-group leader via `requireLeader`.
//  4. Verify the sidecar has Active.Storage != 0 (bootstrap
//     committed) — return FailedPrecondition with a "run
//     BootstrapEncryption first" hint otherwise.
//  5. If sidecar.StorageEnvelopeActive == true, return the §3.2
//     step 5 idempotent-retry response (OK + was_already_active +
//     applied_index = StorageEnvelopeCutoverIndex). Skip the
//     fan-out — the original cutover already passed the gate.
//  6. Refuse with FailedPrecondition if the capability fan-out is
//     not wired (`s.capabilityFanout == nil`): the cutover MUST
//     have a §4 pre-flight; a silent skip would let an
//     unreachable learner sneak through.
//  7. Run the fan-out. Any verdict with Reachable=false or
//     EncryptionCapable=false refuses with FailedPrecondition;
//     the response detail names the specific node.
//  8. Compose the RotationPayload (§2.1: empty Wrapped, DEKID =
//     sidecar.Active.Storage, Purpose = PurposeStorage,
//     ProposerRegistration covering the active storage DEK).
//  9. Propose through Raft via `proposeEncryptionEntry`.
//  10. Re-read the sidecar to discriminate fresh-success vs.
//     stale-DEKID race vs. concurrent-overlap idempotent and
//     assemble the response.
//
// FSM-level no-op outcomes (stale DEKID via a RotateDEK race,
// already-active via a duplicate cutover) do NOT halt the apply
// path — the 6D-4 applier deliberately consumes those entries
// without flipping the sidecar field. The RPC discriminates by
// reading the post-apply sidecar: still false ⇒ stale DEKID,
// surface as FailedPrecondition with the §2.1 #3 retry hint; now
// true with cutover-index mismatch ⇒ another cutover landed
// concurrently, treat as idempotent success.
func (s *EncryptionAdminServer) EnableStorageEnvelope(ctx context.Context, req *pb.EnableStorageEnvelopeRequest) (*pb.EnableStorageEnvelopeResponse, error) {
	// Serialize concurrent cutover RPCs (design §2.1 #4 + PR812
	// coderabbit Major). The semaphore spans the entire precheck
	// → fan-out → propose → postcheck sequence so a second
	// overlapping call sees StorageEnvelopeActive=true at its
	// precheck and takes the §6.4 idempotent-retry short-circuit
	// rather than re-proposing.
	//
	// Acquire honors ctx cancellation so a caller with a short
	// deadline does not block indefinitely on an in-flight
	// cutover's fan-out + propose (codex P2 round-3 on PR812).
	if err := s.acquireCutoverSemaphore(ctx); err != nil {
		return nil, err
	}
	defer s.releaseCutoverSemaphore()
	preSidecar, earlyResp, err := s.cutoverPrecheck(ctx, req)
	if err != nil {
		return nil, err
	}
	if earlyResp != nil {
		// Idempotent retry: preSidecar already reports
		// StorageEnvelopeActive=true. The precheck returned the
		// §3.2 step 5 response shape; no propose, no fan-out.
		return earlyResp, nil
	}
	fanoutResult, err := s.runCutoverFanout(ctx)
	if err != nil {
		return nil, err
	}
	proposedIdx, err := s.proposeCutoverEntry(ctx, preSidecar, req)
	if err != nil {
		return nil, err
	}
	return s.cutoverPostcheck(proposedIdx, fanoutResult)
}

// acquireCutoverSemaphore takes the cutoverSem with ctx-aware
// wait semantics. When the caller's ctx fires before the
// semaphore frees, the wait returns immediately with a gRPC
// status matching the ctx error — Canceled or DeadlineExceeded.
// A plain sync.Mutex would block past the caller's deadline,
// breaking RPC cancellation semantics.
//
// The explicit ctx.Err() check before the select is load-bearing:
// Go's select picks uniformly at random among ready cases (it
// does NOT prioritize ctx.Done()), so an already-canceled ctx
// paired with a free semaphore would coin-flip between acquiring
// the slot (and running precheck / fan-out / propose against an
// aborted caller) and the cancellation return. Checking
// ctx.Err() first turns the cancellation into a deterministic
// short-circuit.
func (s *EncryptionAdminServer) acquireCutoverSemaphore(ctx context.Context) error {
	if s.cutoverSem == nil {
		return grpcStatusError(codes.FailedPrecondition, "encryption: cutover semaphore is not configured on this node")
	}
	if err := ctx.Err(); err != nil {
		return cutoverSemaphoreErrorToStatus(err)
	}
	select {
	case s.cutoverSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return cutoverSemaphoreErrorToStatus(ctx.Err())
	}
}

// cutoverSemaphoreErrorToStatus maps the ctx-cancellation
// outcomes of an acquireCutoverSemaphore wait to their native
// gRPC codes. Pulled out so the status detail names the wait
// site (semaphore vs. fan-out) — without that, an operator
// debugging a Canceled response would not know which stage
// of the RPC the cancellation actually fired in.
func cutoverSemaphoreErrorToStatus(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return grpcStatusErrorf(codes.Canceled,
			"encryption: cutover mutator wait canceled: %v", err)
	case errors.Is(err, context.DeadlineExceeded):
		return grpcStatusErrorf(codes.DeadlineExceeded,
			"encryption: cutover mutator wait deadline exceeded: %v", err)
	default:
		return grpcStatusErrorf(codes.Internal,
			"encryption: cutover mutator wait failed: %v", err)
	}
}

// releaseCutoverSemaphore returns the cutoverSem token. Always
// called via defer from EnableStorageEnvelope; never called
// without a prior successful acquireCutoverSemaphore.
func (s *EncryptionAdminServer) releaseCutoverSemaphore() {
	<-s.cutoverSem
}

// cutoverPrecheck runs the §3.2 steps 1-5 that fire before the
// fan-out: input validation, leader check, sidecar read, bootstrap
// gate, and the idempotent-retry short-circuit. Returns either
//
//   - (preSidecar, nil, nil) on the propose-path: continue with
//     the fan-out and Raft proposal.
//   - (nil, earlyResp, nil) on the §6.4 idempotent retry: the
//     caller short-circuits and returns earlyResp without
//     touching the fan-out or Raft.
//   - (nil, nil, err) on any precheck refusal: the gRPC error
//     already carries the right status code.
func (s *EncryptionAdminServer) cutoverPrecheck(ctx context.Context, req *pb.EnableStorageEnvelopeRequest) (*encryption.Sidecar, *pb.EnableStorageEnvelopeResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, nil, err
	}
	if s.proposer == nil {
		return nil, nil, grpcStatusError(codes.FailedPrecondition, "encryption: proposer is not configured on this node")
	}
	if s.sidecarPath == "" {
		return nil, nil, grpcStatusError(codes.FailedPrecondition, "encryption: sidecar path is not configured on this node")
	}
	if err := validateEnableStorageEnvelopeRequest(req); err != nil {
		return nil, nil, err
	}
	preSidecar, err := encryption.ReadSidecar(s.sidecarPath)
	if err != nil {
		return nil, nil, statusFromSidecarErr(err)
	}
	if preSidecar.Active.Storage == 0 {
		return nil, nil, encryptionNotBootstrappedStatus(
			"encryption: cluster not bootstrapped (Active.Storage == 0) — call BootstrapEncryption first")
	}
	if preSidecar.StorageEnvelopeActive {
		// §6.4 idempotent-retry path. The original cutover already
		// passed the §4 fan-out; re-running it would add latency to
		// what is effectively a no-op call. Return OK with the
		// stable applied_index.
		return nil, idempotentCutoverResponse(preSidecar), nil
	}
	return preSidecar, nil, nil
}

// runCutoverFanout invokes the §4 Voters ∪ Learners pre-flight
// helper and translates the OK / refuse / error branches into the
// §3.2 step 6-7 status codes. Pulled out so the orchestration
// body stays under the cyclomatic-complexity budget.
//
// Context-cancellation errors flow through with their gRPC code
// (codes.Canceled / codes.DeadlineExceeded) so a client that
// cancels mid-fan-out gets the right retry-semantics shape;
// wrapping every err as FailedPrecondition would be a configuration-
// failure signal that misleads automated retry logic (codex P2 on
// PR812). Configuration-shape errors (zero-member snapshot, etc.)
// remain FailedPrecondition.
func (s *EncryptionAdminServer) runCutoverFanout(ctx context.Context) (admin.CapabilityFanoutResult, error) {
	if s.capabilityFanout == nil {
		return admin.CapabilityFanoutResult{}, grpcStatusError(codes.FailedPrecondition,
			"encryption: capability fan-out is not configured on this node")
	}
	result, err := s.capabilityFanout(ctx)
	if err != nil {
		return admin.CapabilityFanoutResult{}, capabilityFanoutErrorToStatus(err)
	}
	if !result.OK {
		// Codex P2 round-2 on PR812: the production fan-out
		// helper can return (result, nil) with OK=false when ctx
		// expires mid-probe — it synthesizes Reachable=false
		// verdicts and returns the result rather than erroring
		// out. In that case, classifying the outcome as a
		// configuration refusal (FailedPrecondition) hides the
		// real transport-layer cancellation/deadline from
		// retry-aware clients. Check ctx.Err() first so the
		// gRPC status code matches what the caller observed.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return admin.CapabilityFanoutResult{}, capabilityFanoutErrorToStatus(ctxErr)
		}
		return admin.CapabilityFanoutResult{}, grpcStatusErrorf(codes.FailedPrecondition,
			"encryption: capability check refused cutover (%s)", capabilityRefusalSummary(result))
	}
	return result, nil
}

// capabilityFanoutErrorToStatus maps the fan-out helper's
// possible failure modes to gRPC status codes. The transport-
// layer ctx errors (caller canceled / deadline expired) keep
// their native codes so a client's retry behaviour stays
// correct; anything else surfaces as a configuration failure
// (FailedPrecondition).
func capabilityFanoutErrorToStatus(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return grpcStatusErrorf(codes.Canceled,
			"encryption: capability fan-out canceled: %v", err)
	case errors.Is(err, context.DeadlineExceeded):
		return grpcStatusErrorf(codes.DeadlineExceeded,
			"encryption: capability fan-out deadline exceeded: %v", err)
	default:
		return grpcStatusErrorf(codes.FailedPrecondition,
			"encryption: capability fan-out failed: %v", err)
	}
}

// proposeCutoverEntry composes the RotationPayload per §2.1 and
// drives it through Raft. The §2.1 #2 length-based-empty-Wrapped
// constraint is satisfied at composition (the payload uses
// []byte{}, not nil), matching the 6D-4 applier's length-based
// check.
func (s *EncryptionAdminServer) proposeCutoverEntry(ctx context.Context, preSidecar *encryption.Sidecar, req *pb.EnableStorageEnvelopeRequest) (uint64, error) {
	payload := fsmwire.RotationPayload{
		SubTag:  fsmwire.RotateSubEnableStorageEnvelope,
		DEKID:   preSidecar.Active.Storage,
		Purpose: fsmwire.PurposeStorage,
		Wrapped: []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{
			DEKID:      preSidecar.Active.Storage,
			FullNodeID: req.GetProposerNodeId(),
			LocalEpoch: uint32ToLocalEpoch(req.GetProposerLocalEpoch()),
		},
	}
	return s.proposeEncryptionEntry(ctx, fsmwire.OpRotation, fsmwire.EncodeRotation(payload))
}

// cutoverPostcheck re-reads the sidecar after the Raft propose
// returns and discriminates the §2.1 outcomes:
//
//   - Fresh success (StorageEnvelopeActive == true with the
//     cutover index set by the apply) — assemble the §3.2 happy-
//     path response.
//   - Stale-DEKID race (StorageEnvelopeActive still false because
//     a RotateDEK raced and the 6D-4 applier consumed the entry
//     as a benign no-op) — refuse with the §2.1 #3 retry hint.
func (s *EncryptionAdminServer) cutoverPostcheck(proposedIdx uint64, fanoutResult admin.CapabilityFanoutResult) (*pb.EnableStorageEnvelopeResponse, error) {
	postSidecar, err := encryption.ReadSidecar(s.sidecarPath)
	if err != nil {
		return nil, statusFromSidecarErr(err)
	}
	if !postSidecar.StorageEnvelopeActive {
		// §2.1 #3 stale-DEKID race: a RotateDEK committed between
		// propose and apply, the 6D-4 applier consumed the entry
		// as a benign no-op, sidecar still false. Surface to the
		// operator with a retry hint — not Aborted (transient
		// concurrency conflict shape, but FailedPrecondition is
		// what the design's §6.4 row pins).
		return nil, grpcStatusError(codes.FailedPrecondition,
			"encryption: cutover proposal raced a RotateDEK (sidecar.Active.Storage moved); retry against the new active DEK")
	}
	return freshCutoverResponse(postSidecar, proposedIdx, fanoutResult), nil
}

// validateEnableStorageEnvelopeRequest enforces the §3.2 step 1
// gRPC-boundary checks. Pulled out so the EnableStorageEnvelope
// orchestration body stays under the cyclomatic-complexity budget
// and so tests can exercise the validation slice in isolation.
func validateEnableStorageEnvelopeRequest(req *pb.EnableStorageEnvelopeRequest) error {
	if req.GetProposerNodeId() == 0 {
		// §6.1 reserves full_node_id=0 as the "not-capable"
		// sentinel. Identical posture to RotateDEK and
		// BootstrapEncryption — accepting 0 would weaken the
		// writer-registry collision invariant.
		return grpcStatusError(codes.InvalidArgument,
			"encryption: proposer_node_id must be non-zero (0 is reserved as the §6.1 not-capable sentinel)")
	}
	if req.GetProposerLocalEpoch() > math.MaxUint16 {
		return localEpochOutOfRangeStatusf(
			"encryption: proposer_local_epoch=%d exceeds the §4.1 16-bit bound (max 0xFFFF)",
			req.GetProposerLocalEpoch())
	}
	return nil
}

// idempotentCutoverResponse is the §3.2 step 5 retry-success
// shape: OK status, was_already_active=true, applied_index =
// sidecar.StorageEnvelopeCutoverIndex (the original cutover's
// apply index). The defensive cutover_index_unknown branch fires
// only when an attacker / schema rollback / hand-edited sidecar
// has StorageEnvelopeActive=true paired with
// StorageEnvelopeCutoverIndex=0 — operationally impossible under
// normal apply but hedged against per §6.4.
func idempotentCutoverResponse(sc *encryption.Sidecar) *pb.EnableStorageEnvelopeResponse {
	resp := &pb.EnableStorageEnvelopeResponse{
		WasAlreadyActive:    true,
		CapabilitySummary:   nil, // empty on idempotent retries per §3.1
		AppliedIndex:        sc.StorageEnvelopeCutoverIndex,
		CutoverIndexUnknown: false,
	}
	if sc.StorageEnvelopeCutoverIndex == 0 {
		// §6.4 defensive branch.
		resp.AppliedIndex = sc.RaftAppliedIndex
		resp.CutoverIndexUnknown = true
	}
	return resp
}

// freshCutoverResponse is the §3.2 fresh-success shape: OK,
// was_already_active=false, applied_index = the Raft index of
// the entry the leader just proposed and waited to apply,
// capability_summary projects the fan-out verdicts into the
// wire shape.
func freshCutoverResponse(sc *encryption.Sidecar, proposedIdx uint64, fanoutResult admin.CapabilityFanoutResult) *pb.EnableStorageEnvelopeResponse {
	// The reported applied_index is the post-apply sidecar's
	// StorageEnvelopeCutoverIndex when the apply set it equal to
	// proposedIdx (the fresh-success path). A mismatch means a
	// concurrent cutover entry landed between propose and apply
	// — operator-impossible under §2.1 #4 (mutator lock
	// serialises overlapping calls on the propose side) but the
	// applier still records the FIRST cutover's index. Treat as
	// idempotent: report the original index with
	// was_already_active=false (this call's propose committed)
	// but the surfaced index is the FIRST cutover's. The CLI
	// sees the discrepancy via the applied_index vs. its own
	// expected value; the RPC must not lie about which call
	// proposed which entry.
	appliedIndex := sc.StorageEnvelopeCutoverIndex
	if appliedIndex == 0 {
		// Defensive: the apply path always sets the cutover
		// index alongside the active flag. A zero here means
		// the post-apply read raced (unlikely, but the §6.4
		// fallback exists for hand-edited sidecars).
		// CutoverIndexUnknown stays false on this branch — the
		// proto field is "only meaningful when
		// was_already_active=true" (§3.1), and here we know the
		// correct applied_index: it is proposedIdx (the Raft
		// entry this call just committed). The idempotent-retry
		// path is the only context where the original cutover
		// index is irrecoverable, hence its CutoverIndexUnknown
		// signal. Claude bot informational on PR812.
		appliedIndex = proposedIdx
	}
	return &pb.EnableStorageEnvelopeResponse{
		AppliedIndex:        appliedIndex,
		CapabilitySummary:   projectCapabilityVerdicts(fanoutResult.Verdicts),
		CutoverIndexUnknown: false,
		WasAlreadyActive:    false,
	}
}

// raftEnvelopeWrapEnabled gates fresh EnableRaftEnvelope cutovers.
// The production default is open now that the wrap-on-propose,
// unwrap-on-apply, and cutover barrier wiring ship. Keep the atomic gate
// as an emergency/test kill switch around the fresh cutover state machine.
//
// Kept as atomic.Bool rather than a const so tests can override the
// gate without racing sibling t.Parallel cases.
var raftEnvelopeWrapEnabled atomic.Bool

func init() {
	raftEnvelopeWrapEnabled.Store(true)
}

// EnableRaftEnvelope is the Stage 6E Phase 2 cutover — flips Raft
// proposals from cleartext to §4.2-envelope. Structural mirror of
// EnableStorageEnvelope; the differences are:
//
//   - Target Purpose is PurposeRaft.
//   - Source DEK slot is sidecar.Active.Raft (not Active.Storage).
//   - The "already active" sentinel is the single field
//     sidecar.RaftEnvelopeCutoverIndex != 0 — there is no separate
//     bool flag, so the raft variant has no equivalent of the
//     §6.4 cutover_index_unknown defensive fallback (a zero index
//     is exactly the not-active state, not a corrupted-active
//     state, and the 6E-1a applier fail-closes on raftIdx == 0
//     before ApplyRegistration).
//
// The semaphore, pre-check / fan-out / propose / post-check
// sequence, and error mapping match the storage variant verbatim;
// see EnableStorageEnvelope for the full design rationale.
//
// **Gate posture**: raftEnvelopeWrapEnabled remains a test/emergency kill
// switch. If closed, the pre-gate validation surface (leader, semaphore
// acquire, request shape) still fires, but no Raft proposal is composed and
// no sidecar mutation occurs.
func (s *EncryptionAdminServer) EnableRaftEnvelope(ctx context.Context, req *pb.EnableRaftEnvelopeRequest) (*pb.EnableRaftEnvelopeResponse, error) {
	if err := s.acquireCutoverSemaphore(ctx); err != nil {
		return nil, err
	}
	defer s.releaseCutoverSemaphore()
	preSidecar, earlyResp, err := s.raftCutoverPrecheck(ctx, req)
	if err != nil {
		return nil, err
	}
	if earlyResp != nil {
		return earlyResp, nil
	}
	if !raftEnvelopeWrapEnabled.Load() {
		// Test/fail-closed posture: recording a cutover index
		// without the paired wrap-on-propose path would let
		// cleartext entries land at indexes > N and later be
		// interpreted as envelopes. Refuse before the fan-out and
		// propose so no sidecar state changes.
		return nil, grpcStatusError(codes.FailedPrecondition,
			"encryption: enable-raft-envelope is administratively gated; accepting the cutover without wrap-on-propose / unwrap-on-apply / §7.1 proposal-quiescence-barrier would make the cluster unsafe")
	}
	if s.cutoverBarrier == nil {
		// Production wiring injects the barrier controller. Without
		// it the handler cannot drive the §7.1 6-step sequence —
		// refuse before any side effect rather than silently skip
		// the barrier and let a fresh USER proposal land at index >
		// proposedIdx mid-cutover.
		return nil, grpcStatusError(codes.FailedPrecondition,
			"encryption: cutover barrier controller is not wired — wire WithEncryptionAdminCutoverBarrier before enabling the raft envelope cutover")
	}
	if s.latestAppliedIndex == nil {
		// Pre-flight check: awaitCutoverApply (§7.1 step-4) consults
		// this callback to know when the local FSM has applied the
		// cutover entry. Checking AFTER the propose would leave a
		// committed cutover entry in Raft with no wrap installed —
		// releasing the barrier from that state lets post-cutover
		// USER proposals land cleartext at indexes > cutover_index,
		// which the §6.3 strict-`>` apply hook then halts on. The
		// check belongs here, before any side effect.
		return nil, grpcStatusError(codes.FailedPrecondition,
			"encryption: latest-applied-index callback is not wired — wire WithEncryptionAdminLatestAppliedIndex before enabling the raft envelope cutover")
	}
	if err := s.validateCutoverBarrierScope(); err != nil {
		return nil, err
	}
	fanoutResult, err := s.runCutoverFanout(ctx)
	if err != nil {
		return nil, err
	}
	// §7.1 6-step quiescence barrier. Steps 1 + 6 are bracketed
	// here as the most direct deferred-cleanup shape; steps 2-5
	// run between. The barrier is held for the smallest possible
	// window — only the propose + wait-apply + install-wrap path —
	// so user writes are blocked just for the few-ms cutover entry
	// commit RTT. Fanout runs BEFORE the barrier so a refused
	// capability check fails fast without blocking user writes.
	proposedIdx, err := s.runRaftEnvelopeCutoverBarrier(ctx, preSidecar, req)
	if err != nil {
		return nil, err
	}
	return s.raftCutoverPostcheck(proposedIdx, fanoutResult)
}

func (s *EncryptionAdminServer) validateCutoverBarrierScope() error {
	validator, ok := s.cutoverBarrier.(cutoverBarrierScopeValidator)
	if !ok {
		return nil
	}
	if err := validator.ValidateCutoverScope(); err != nil {
		return grpcStatusError(codes.FailedPrecondition, err.Error())
	}
	return nil
}

// runRaftEnvelopeCutoverBarrier executes the §7.1 step-1..6
// quiescence-barrier sequence around the cutover proposal:
//
//	step 1: Begin() opens the barrier.
//	step 2: WaitDrained blocks until in-flight USER proposals drain.
//	step 3: ProposeAdmin (proposeRaftCutoverEntry) — barrier-exempt
//	        by interface contract so this call doesn't deadlock on
//	        its own barrier.
//	step 4: awaitCutoverApply polls the local FSM applied index
//	        until it reaches the cutover entry's commit index, by
//	        which point the apply path has set
//	        sidecar.RaftEnvelopeCutoverIndex.
//	step 5: InstallWrap publishes the wrap closure on every
//	        participating ShardGroup (the dynamic-wrap-pointer
//	        hot-swap installed in 6E-2c).
//	step 6: End() closes the barrier so fresh USER proposals carry
//	        the active wrap closure.
//
// Returns the cutover entry's commit_index on success.
//
// Critical barrier-safety invariant: once a cutover marker MAY have
// entered Raft, the handler MUST NOT release the barrier without
// first installing the wrap closure.
// Doing so lets fresh USER proposals on this leader land cleartext
// at indexes > cutover_index, which the §6.3 strict-`>` apply hook
// then halts on cluster-wide. The releaseSafe flag gates the End()
// call so:
//
//   - Pre-propose failures (drain timeout) release safely; no
//     cutover entry exists in Raft to brick the cluster.
//   - Propose errors: the etcd engine cannot distinguish a
//     ctx-cancel that fired before rawNode.Propose from one that
//     fired after — both surface as a ProposeAdmin error, but only
//     the latter leaves the marker submitted to Raft. Treat ALL
//     propose errors as ambiguous and leave the barrier OPEN.
//     releaseSafe is flipped to false BEFORE the propose call so
//     the deferred End() does not race the ctx window.
//   - Post-propose failures (apply-wait timeout, stale-DEK no-op
//     branch, post-apply sidecar I/O fault) leave the barrier
//     OPEN; operator intervention (this leader's restart, or a
//     retry that hits the idempotent pre-check path on a new
//     leader after apply caught up) is required.
//   - Full success releases the barrier explicitly via End().
//
// This is a deliberate per-leader safety trade-off: a leader whose
// barrier remains open after a cutover commit refuses all USER
// writes until restart. The alternative (release the barrier and
// risk cluster-wide halt) is unrecoverable.
func (s *EncryptionAdminServer) runRaftEnvelopeCutoverBarrier(ctx context.Context, preSidecar *encryption.Sidecar, req *pb.EnableRaftEnvelopeRequest) (uint64, error) {
	_ = s.cutoverBarrier.Begin()
	// releaseSafe gates the deferred End(). See the function comment
	// for the invariant: post-propose-success, releasing without an
	// installed wrap is unsafe.
	releaseSafe := true
	defer func() {
		if releaseSafe {
			s.cutoverBarrier.End()
		}
	}()

	if err := s.cutoverBarrier.WaitDrained(ctx); err != nil {
		return 0, classifyCutoverCtxErr(err, "encryption: §7.1 step-2 drain wait", codes.Unavailable)
	}

	// Keep the barrier closed across ambiguous propose outcomes.
	// proposeRaftCutoverEntry calls raftengine.Proposer.ProposeAdmin,
	// which submits to
	// rawNode.Propose and then waits on a pending response. If ctx
	// fires AFTER the engine submitted the marker to Raft but
	// BEFORE the local apply resolves it, ProposeAdmin returns an
	// error even though the marker may still commit cluster-wide.
	// The etcd engine's cancellation path
	// (internal/raftengine/etcd/engine.go) removes the pending
	// response but cannot retract the rawNode.Propose submission.
	// Treating any propose-side error as "marker never reached
	// Raft" would release the barrier; if the marker subsequently
	// commits on every other replica, user proposals admitted
	// after End() would land at indexes > cutoverIdx in cleartext
	// and halt the cluster on the §6.3 strict-`>` unwrap hook.
	//
	// Flip releaseSafe to false BEFORE the propose so any error
	// path leaves the barrier open. The cost is a false-positive
	// barrier-stuck state on errors that DID definitively reject
	// before submission (ErrNotLeader transients, etc.) — that's
	// recoverable by operator restart of this leader; the cluster-
	// brick scenario is not.
	releaseSafe = false

	proposedIdx, err := s.proposeRaftCutoverEntry(ctx, preSidecar, req)
	if err != nil {
		s.blockNonCutoverAdminAfterUnsafeRaftCutover()
		return 0, err
	}

	if err := s.awaitCutoverApply(ctx, proposedIdx); err != nil {
		s.blockNonCutoverAdminAfterUnsafeRaftCutover()
		return 0, err
	}

	// Verify the cutover applied as a fresh-success (not a §6E-1a
	// constraint #3 stale-DEK benign no-op). Re-read the sidecar: if
	// RaftEnvelopeCutoverIndex is still 0, a concurrent RotateDEK
	// changed Active.Raft between propose and apply and the applier
	// consumed the marker as a no-op without flipping the cutover
	// field.
	//
	// In that race we MUST NOT install the wrap closure: the §6.3
	// strict-`>` hook compares against the (still-zero)
	// RaftEnvelopeCutoverIndex, so a fresh wrap-active proposal at
	// index > 0 would be treated as a wrapped envelope while
	// pre-install admin entries (the racing RotateDEK, cleartext
	// via ProposeAdmin) still sit in the apply queue at indexes
	// > 0 and halt every follower on unwrap-failure.
	//
	// Recovery shape: leave releaseSafe=true (no cutover took
	// effect, safe to resume user writes in pre-cutover cleartext
	// mode), let the deferred End() close the barrier, surface a
	// FailedPrecondition. The operator's CLI re-runs the cutover
	// RPC against the now-updated Active.Raft.
	postSidecar, err := encryption.ReadSidecar(s.sidecarPath)
	if err != nil {
		// Sidecar I/O failure post-apply: the cutover entry IS
		// committed in Raft but we cannot determine whether it
		// applied as fresh-success or as a stale-DEK no-op. Leave
		// the barrier open (releaseSafe stays false) — operator
		// must intervene because we can't safely install or skip
		// the wrap without knowing the actual outcome.
		s.blockNonCutoverAdminAfterUnsafeRaftCutover()
		return 0, statusFromSidecarErr(err)
	}
	if postSidecar.RaftEnvelopeCutoverIndex == 0 {
		// Stale-DEK no-op: no cutover took effect. Release the
		// barrier (no committed cutover state to protect against)
		// and surface a FailedPrecondition for the operator
		// retry-against-new-DEK path.
		releaseSafe = true
		return 0, grpcStatusError(codes.FailedPrecondition,
			"encryption: cutover marker applied as stale-DEK no-op (concurrent RotateDEK race) — retry against the now-updated Active.Raft")
	}

	s.cutoverBarrier.InstallWrap()
	releaseSafe = true
	return proposedIdx, nil
}

// classifyCutoverCtxErr maps a ctx-derived error from the cutover
// barrier sequence onto a structured gRPC status code so clients
// see the right retry semantics:
//
//   - context.Canceled → codes.Canceled (client aborted; don't
//     retry the same request)
//   - context.DeadlineExceeded → codes.DeadlineExceeded (client
//     may retry against a new deadline)
//   - anything else → fallbackCode (typically Unavailable for
//     transient barrier-internal errors)
//
// Without this distinction a canceled RPC surfaces as Unavailable
// or DeadlineExceeded, breaking retry logic that switches on the
// gRPC code (gemini medium, claude finding 1).
func classifyCutoverCtxErr(err error, msg string, fallbackCode codes.Code) error {
	switch {
	case errors.Is(err, context.Canceled):
		return grpcStatusErrorf(codes.Canceled, "%s: %v", msg, err)
	case errors.Is(err, context.DeadlineExceeded):
		return grpcStatusErrorf(codes.DeadlineExceeded, "%s: %v", msg, err)
	default:
		return grpcStatusErrorf(fallbackCode, "%s: %v", msg, err)
	}
}

// awaitCutoverApply blocks until the local FSM applied index
// reaches proposedIdx — by that point the cutover entry's apply
// has run, which writes sidecar.RaftEnvelopeCutoverIndex. Polls on
// latestAppliedIndex every cutoverApplyPollInterval; the expected
// duration in a healthy cluster is single-digit milliseconds.
//
// Pre-condition: s.latestAppliedIndex MUST be non-nil. The
// EnableRaftEnvelope handler enforces this BEFORE proposing (see
// the pre-flight check in EnableRaftEnvelope) so any nil-callback
// misconfiguration fails closed without leaving a committed
// cutover entry stranded in Raft.
//
// Returns an error classified via classifyCutoverCtxErr on ctx
// fire (Canceled vs DeadlineExceeded).
func (s *EncryptionAdminServer) awaitCutoverApply(ctx context.Context, proposedIdx uint64) error {
	if s.latestAppliedIndex() >= proposedIdx {
		return nil
	}
	ticker := time.NewTicker(cutoverApplyPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return classifyCutoverCtxErr(
				ctx.Err(),
				fmt.Sprintf("encryption: §7.1 step-4 await apply of index %d", proposedIdx),
				codes.DeadlineExceeded)
		case <-ticker.C:
			if s.latestAppliedIndex() >= proposedIdx {
				return nil
			}
		}
	}
}

// cutoverApplyPollInterval bounds the poll cadence inside the §7.1
// step-4 wait. A short interval keeps the cutover window tight in
// healthy clusters (single Raft RTT). The handler's caller-supplied
// ctx governs the upper bound: the loop fires up to
// `deadline / cutoverApplyPollInterval` times before either
// succeeding or returning a ctx error. For a typical 60s admin
// deadline that's ~120k polls — cheap (each iteration is an atomic
// load) but worth keeping in mind if operators pick generous
// deadlines for diagnostic runs.
const cutoverApplyPollInterval = 500 * time.Microsecond

// raftCutoverPrecheck runs the Stage 6E §3.2 steps 1-5: input
// validation, leader gate, bootstrap gate, idempotent-retry
// short-circuit. Returns:
//
//   - (preSidecar, nil, nil) on the propose-path
//   - (nil, earlyResp, nil) on the idempotent retry
//   - (nil, nil, err) on any refusal
func (s *EncryptionAdminServer) raftCutoverPrecheck(ctx context.Context, req *pb.EnableRaftEnvelopeRequest) (*encryption.Sidecar, *pb.EnableRaftEnvelopeResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, nil, err
	}
	if s.proposer == nil {
		return nil, nil, grpcStatusError(codes.FailedPrecondition, "encryption: proposer is not configured on this node")
	}
	if s.sidecarPath == "" {
		return nil, nil, grpcStatusError(codes.FailedPrecondition, "encryption: sidecar path is not configured on this node")
	}
	if err := validateEnableRaftEnvelopeRequest(req); err != nil {
		return nil, nil, err
	}
	preSidecar, err := encryption.ReadSidecar(s.sidecarPath)
	if err != nil {
		return nil, nil, statusFromSidecarErr(err)
	}
	if preSidecar.Active.Raft == 0 {
		return nil, nil, encryptionNotBootstrappedStatus(
			"encryption: cluster not bootstrapped (Active.Raft == 0) — call BootstrapEncryption first")
	}
	if preSidecar.RaftEnvelopeCutoverIndex != 0 {
		// Idempotent retry — return OK with was_already_active=true
		// and the original cutover index. Skip the fan-out: the
		// original cutover already passed the gate.
		return nil, idempotentRaftCutoverResponse(preSidecar), nil
	}
	return preSidecar, nil, nil
}

// proposeRaftCutoverEntry composes the §2.1 RotationPayload for
// the raft variant and drives it through Raft. Purpose=PurposeRaft,
// DEKID = sidecar.Active.Raft, Wrapped=empty (length-based, not
// nil, matching the 6E-1a applier's length-based reject).
func (s *EncryptionAdminServer) proposeRaftCutoverEntry(ctx context.Context, preSidecar *encryption.Sidecar, req *pb.EnableRaftEnvelopeRequest) (uint64, error) {
	payload := fsmwire.RotationPayload{
		SubTag:  fsmwire.RotateSubEnableRaftEnvelope,
		DEKID:   preSidecar.Active.Raft,
		Purpose: fsmwire.PurposeRaft,
		Wrapped: []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{
			DEKID:      preSidecar.Active.Raft,
			FullNodeID: req.GetProposerNodeId(),
			LocalEpoch: uint32ToLocalEpoch(req.GetProposerLocalEpoch()),
		},
	}
	return s.proposeEncryptionEntryWith(ctx, s.proposer, fsmwire.OpRotation, fsmwire.EncodeRotation(payload))
}

// raftCutoverPostcheck re-reads the sidecar after the Raft propose
// returns and discriminates the §2.1 outcomes:
//
//   - Fresh success: RaftEnvelopeCutoverIndex == proposedIdx → §3.2
//     happy path.
//   - Stale-DEKID race: RaftEnvelopeCutoverIndex still 0 because
//     a RotateDEK raced and the applier consumed the entry as a
//     benign no-op → FailedPrecondition with retry hint.
//   - Concurrent overlap: RaftEnvelopeCutoverIndex != 0 but !=
//     proposedIdx → another cutover landed first (operator-
//     impossible under the semaphore, but the applier records
//     the FIRST cutover's index; surface that index with
//     was_already_active=false because THIS call's propose
//     committed an entry that the applier treated as the
//     idempotent path).
func (s *EncryptionAdminServer) raftCutoverPostcheck(proposedIdx uint64, fanoutResult admin.CapabilityFanoutResult) (*pb.EnableRaftEnvelopeResponse, error) {
	postSidecar, err := encryption.ReadSidecar(s.sidecarPath)
	if err != nil {
		return nil, statusFromSidecarErr(err)
	}
	if postSidecar.RaftEnvelopeCutoverIndex == 0 {
		return nil, grpcStatusError(codes.FailedPrecondition,
			"encryption: cutover proposal raced a RotateDEK (sidecar.Active.Raft moved); retry against the new active DEK")
	}
	return freshRaftCutoverResponse(postSidecar, proposedIdx, fanoutResult), nil
}

// validateEnableRaftEnvelopeRequest enforces the §3.2 step 1
// gRPC-boundary checks. Pulled out so the EnableRaftEnvelope
// orchestration body stays under the cyclomatic-complexity budget
// and so tests can exercise the validation slice in isolation.
func validateEnableRaftEnvelopeRequest(req *pb.EnableRaftEnvelopeRequest) error {
	if req.GetProposerNodeId() == 0 {
		return grpcStatusError(codes.InvalidArgument,
			"encryption: proposer_node_id must be non-zero (0 is reserved as the §6.1 not-capable sentinel)")
	}
	if req.GetProposerLocalEpoch() > math.MaxUint16 {
		return localEpochOutOfRangeStatusf(
			"encryption: proposer_local_epoch=%d exceeds the §4.1 16-bit bound (max 0xFFFF)",
			req.GetProposerLocalEpoch())
	}
	return nil
}

// idempotentRaftCutoverResponse is the §3.2 step 5 retry-success
// shape for the raft variant: OK, was_already_active=true,
// applied_index = sidecar.RaftEnvelopeCutoverIndex (the original
// cutover's apply index). The storage variant's
// cutover_index_unknown defensive branch is intentionally absent
// — the raft variant uses the cutover index itself as the active
// sentinel, so a non-zero index here cannot coexist with the
// "active but unknown index" state the storage hedge was for.
func idempotentRaftCutoverResponse(sc *encryption.Sidecar) *pb.EnableRaftEnvelopeResponse {
	return &pb.EnableRaftEnvelopeResponse{
		WasAlreadyActive:  true,
		CapabilitySummary: nil,
		AppliedIndex:      sc.RaftEnvelopeCutoverIndex,
	}
}

// freshRaftCutoverResponse is the §3.2 fresh-success shape for the
// raft variant. applied_index is sourced from the post-apply
// sidecar's RaftEnvelopeCutoverIndex, which raftCutoverPostcheck
// has already validated as non-zero (the stale-DEKID branch
// refuses earlier, so reaching here implies the apply set the
// cutover index). The storage variant's `appliedIndex == 0`
// defensive branch has no analogue here because the raft variant
// uses the cutover index itself as the active sentinel: a zero
// at this point would be an upstream invariant violation, not a
// hand-edit hazard.
func freshRaftCutoverResponse(sc *encryption.Sidecar, _ uint64, fanoutResult admin.CapabilityFanoutResult) *pb.EnableRaftEnvelopeResponse {
	return &pb.EnableRaftEnvelopeResponse{
		AppliedIndex:      sc.RaftEnvelopeCutoverIndex,
		CapabilitySummary: projectCapabilityVerdicts(fanoutResult.Verdicts),
		WasAlreadyActive:  false,
	}
}

// projectCapabilityVerdicts marshals the internal CapabilityVerdict
// shape into the wire-format proto.CapabilityVerdict. Reachable /
// Err fields are intentionally NOT projected: the cutover RPC only
// returns this summary on the OK path, so every verdict in the
// slice has Reachable=true and Err=nil by construction. Operators
// who need transport-layer diagnostics consult the leader's logs.
func projectCapabilityVerdicts(in []admin.CapabilityVerdict) []*pb.CapabilityVerdict {
	if len(in) == 0 {
		return nil
	}
	out := make([]*pb.CapabilityVerdict, 0, len(in))
	for _, v := range in {
		out = append(out, &pb.CapabilityVerdict{
			FullNodeId:        v.FullNodeID,
			EncryptionCapable: v.EncryptionCapable,
			BuildSha:          v.BuildSHA,
			SidecarPresent:    v.SidecarPresent,
		})
	}
	return out
}

// capabilityRefusalSummary builds the human-readable detail
// included in the FailedPrecondition status when the fan-out
// refused. Names the first unreachable / not-capable member so
// the operator's CLI can immediately diagnose without trawling
// logs.
func capabilityRefusalSummary(result admin.CapabilityFanoutResult) string {
	for _, v := range result.Verdicts {
		if !v.Reachable {
			return "unreachable member full_node_id=" + strconv.FormatUint(v.FullNodeID, 10)
		}
		if !v.EncryptionCapable {
			return "not-capable member full_node_id=" + strconv.FormatUint(v.FullNodeID, 10)
		}
	}
	return "fan-out reported OK=false with no per-member refusal — check leader logs"
}

// RegisterEncryptionWriter proposes a §11.3 0x03 OpRegistration
// entry for the calling node's first encrypted-write epoch under
// the supplied dek_id. The proto carries `repeated WriterBatch`
// for forward-compatibility, but each PR-B call MUST provide
// exactly one entry; batched registrations belong to the §5.6
// step 1a bootstrap path (deferred to PR-C). The server enforces
// the single-entry contract here so an operator who accidentally
// fans out a batched RegisterEncryptionWriter does not silently
// propose a fraction of it.
func (s *EncryptionAdminServer) RegisterEncryptionWriter(ctx context.Context, req *pb.RegisterEncryptionWriterRequest) (*pb.RegisterEncryptionWriterResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, err
	}
	if s.proposer == nil {
		return nil, grpcStatusError(codes.FailedPrecondition, "encryption: proposer is not configured on this node")
	}
	if req.GetDekId() == 0 {
		return nil, grpcStatusError(codes.InvalidArgument, "encryption: dek_id must be non-zero (key id 0 is reserved per §5.1)")
	}
	writers := req.GetWriters()
	switch len(writers) {
	case 1:
		// expected shape; fall through to validation + propose.
	case 0:
		return nil, grpcStatusError(codes.InvalidArgument,
			"encryption: RegisterEncryptionWriter requires exactly one writer, got 0")
	default:
		return nil, grpcStatusErrorf(codes.InvalidArgument,
			"encryption: RegisterEncryptionWriter requires exactly one writer, got %d (use BootstrapEncryption for multi-writer batches)",
			len(writers))
	}
	w := writers[0]
	if w.GetLocalEpoch() > math.MaxUint16 {
		return nil, localEpochOutOfRangeStatusf(
			"encryption: writers[0].local_epoch=%d exceeds the §4.1 16-bit bound (max 0xFFFF)",
			w.GetLocalEpoch())
	}
	if w.GetFullNodeId() == 0 {
		// Same §6.1 sentinel rule as RotateDEK.proposer_node_id.
		return nil, grpcStatusError(codes.InvalidArgument,
			"encryption: writers[0].full_node_id must be non-zero (0 is reserved as the §6.1 not-capable sentinel)")
	}
	payload := fsmwire.RegistrationPayload{
		DEKID:      req.GetDekId(),
		FullNodeID: w.GetFullNodeId(),
		LocalEpoch: uint32ToLocalEpoch(w.GetLocalEpoch()),
	}
	body := fsmwire.EncodeRegistration(payload)
	idx, err := s.proposeSerializedEncryptionEntry(ctx, fsmwire.OpRegistration, body)
	if err != nil {
		return nil, err
	}
	return &pb.RegisterEncryptionWriterResponse{AppliedIndex: idx}, nil
}

func (s *EncryptionAdminServer) proposeSerializedEncryptionEntry(ctx context.Context, opcode byte, body []byte) (uint64, error) {
	if err := s.acquireCutoverSemaphore(ctx); err != nil {
		return 0, err
	}
	defer s.releaseCutoverSemaphore()
	if err := s.refuseNonCutoverAdminAfterUnsafeRaftCutover(); err != nil {
		return 0, err
	}
	return s.proposeEncryptionEntry(ctx, opcode, body)
}

func (s *EncryptionAdminServer) blockNonCutoverAdminAfterUnsafeRaftCutover() {
	s.unsafeRaftCutoverAdminBlocked.Store(true)
}

func (s *EncryptionAdminServer) refuseNonCutoverAdminAfterUnsafeRaftCutover() error {
	if !s.unsafeRaftCutoverAdminBlocked.Load() {
		return nil
	}
	return grpcStatusError(codes.FailedPrecondition,
		"encryption: raft envelope cutover outcome is unsafe on this process; refusing non-cutover admin proposals until restart")
}

// proposeEncryptionEntry prepends the §11.3 opcode tag to a
// fsmwire-encoded body and submits the resulting Raft entry. The
// Stage 4 FSM apply path peels the tag, dispatches into
// applyEncryption, and Halt-Applies on any decode failure — this
// helper is just the byte-level glue between the server-side
// encoder and raftengine.Proposer.
func (s *EncryptionAdminServer) proposeEncryptionEntry(ctx context.Context, opcode byte, body []byte) (uint64, error) {
	return s.proposeEncryptionEntryWith(ctx, s.nonCutoverAdminProposer(), opcode, body)
}

func (s *EncryptionAdminServer) nonCutoverAdminProposer() raftengine.Proposer {
	if s.postCutoverProposer != nil {
		return s.postCutoverProposer
	}
	return s.proposer
}

func (s *EncryptionAdminServer) proposeEncryptionEntryWith(ctx context.Context, proposer raftengine.Proposer, opcode byte, body []byte) (uint64, error) {
	if proposer == nil {
		return 0, grpcStatusError(codes.FailedPrecondition, "encryption: proposer is not configured on this node")
	}
	entry := make([]byte, 0, 1+len(body))
	entry = append(entry, opcode)
	entry = append(entry, body...)
	// proposeEncryptionEntry composes control-plane entries
	// (BootstrapEncryption, EnableStorageEnvelope, the §6E
	// EnableRaftEnvelope cutover marker, RotateDEK,
	// RegisterEncryptionWriter, etc.). The §7.1 quiescence barrier
	// Stage 6E-2d installs on Propose would reject these — most
	// trivially, the cutover entry itself would deadlock on its
	// own barrier — so admin ops route through the
	// barrier-exempt ProposeAdmin path.
	//
	// ProposeAdmin is barrier-exempt only; the wrap layer above
	// the engine (kv.wrappedProposer, when configured) still
	// applies its wrap closure to ProposeAdmin payloads, so a
	// post-cutover RotateDEK or RegisterEncryptionWriter
	// committed at `index > raftEnvelopeCutoverIndex` carries the
	// AEAD envelope the §6.3 strict-`>` apply hook expects.
	// EnableRaftEnvelope's cutover marker (at `index == cutover`)
	// bypasses this wrapper-aware helper and calls it with the raw
	// proposer, so strict-`>` leaves the marker itself cleartext.
	res, err := proposer.ProposeAdmin(ctx, entry)
	if err != nil {
		return 0, proposeErrorToStatus(err, opcode)
	}
	if res == nil {
		return 0, grpcStatusError(codes.Internal, "encryption: proposer returned nil result")
	}
	return res.CommitIndex, nil
}

// proposeErrorToStatus maps raftengine.Proposer.Propose errors to
// gRPC status codes so clients see structured retry semantics
// instead of the default codes.Unknown:
//
//   - Leadership lost between requireLeader() and Propose, or a
//     leadership transfer raced the call → FailedPrecondition.
//     The client retries against the new leader, same shape as
//     the up-front requireLeader() rejection.
//   - Context errors (caller canceled / deadline elapsed) →
//     Canceled / DeadlineExceeded so the client does not retry a
//     genuinely-aborted call.
//   - Anything else (engine closed, propose-queue full, etc.) →
//     Unavailable, the standard "transient, retryable" code.
func proposeErrorToStatus(err error, opcode byte) error {
	switch {
	case errors.Is(err, raftengine.ErrNotLeader),
		errors.Is(err, raftengine.ErrLeadershipLost),
		errors.Is(err, raftengine.ErrLeadershipTransferInProgress):
		return grpcStatusErrorf(codes.FailedPrecondition,
			"encryption: propose 0x%02x rejected by raft engine: %v", opcode, err)
	case errors.Is(err, context.Canceled):
		return grpcStatusErrorf(codes.Canceled,
			"encryption: propose 0x%02x canceled: %v", opcode, err)
	case errors.Is(err, context.DeadlineExceeded):
		return grpcStatusErrorf(codes.DeadlineExceeded,
			"encryption: propose 0x%02x deadline exceeded: %v", opcode, err)
	default:
		return grpcStatusErrorf(codes.Unavailable,
			"encryption: propose 0x%02x failed: %v", opcode, err)
	}
}

// requireLeader rejects on followers with FailedPrecondition. The
// status detail embeds the currently-known leader's id and
// address so the operator's CLI can re-target without parsing
// free-form error text. A nil leaderView is allowed for tests
// that exercise the proposer wiring directly; the production
// wiring in main.go MUST register one (enforced as a Stage 6
// startup-time assertion).
//
// Two checks happen here:
//
//  1. Fast path: State() == StateLeader rejects on followers and
//     candidates without a Raft round-trip.
//  2. Quorum confirmation: VerifyLeader(ctx) does a ReadIndex
//     round-trip so a *partitioned former leader* whose
//     State() still reports StateLeader (the local view has
//     not stepped down yet) cannot serve mutating RPCs or
//     ResyncSidecar against stale local state. Without this,
//     a follower's recovery flow could pull an outdated DEK
//     set from a stranded leader and miss recent rotations.
func (s *EncryptionAdminServer) requireLeader(ctx context.Context) error {
	return requireEncryptionLeader(ctx, s.leaderView)
}

func (s *EncryptionAdminServer) requireRecoveryLeader(ctx context.Context) error {
	view := s.recoveryLeaderView
	if view == nil {
		view = s.leaderView
	}
	return requireEncryptionLeader(ctx, view)
}

func requireEncryptionLeader(ctx context.Context, view raftengine.LeaderView) error {
	if view == nil {
		return nil
	}
	if view.State() != raftengine.StateLeader {
		leader := view.Leader()
		if leader.ID == "" && leader.Address == "" {
			return grpcStatusError(codes.FailedPrecondition, "encryption: not leader (no known leader)")
		}
		return grpcStatusErrorf(codes.FailedPrecondition,
			"encryption: not leader (current leader id=%q address=%q)",
			leader.ID, leader.Address)
	}
	if err := view.VerifyLeader(ctx); err != nil {
		return verifyLeaderErrorToStatus(err)
	}
	return nil
}

// verifyLeaderErrorToStatus maps LeaderView.VerifyLeader errors to
// gRPC status codes. Mirrors proposeErrorToStatus so that a
// client-side timeout during the ReadIndex round-trip surfaces as
// codes.DeadlineExceeded (the retryable transport-timing
// semantics) rather than codes.FailedPrecondition (which the
// client would treat as a leadership rejection).
func verifyLeaderErrorToStatus(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return grpcStatusErrorf(codes.Canceled,
			"encryption: VerifyLeader canceled: %v", err)
	case errors.Is(err, context.DeadlineExceeded):
		return grpcStatusErrorf(codes.DeadlineExceeded,
			"encryption: VerifyLeader deadline exceeded: %v", err)
	default:
		return grpcStatusErrorf(codes.FailedPrecondition,
			"encryption: VerifyLeader failed, refusing to act on stale-leader state: %v", err)
	}
}

func protoRotatePurpose(p pb.RotateDEKRequest_Purpose) (fsmwire.Purpose, error) {
	switch p {
	case pb.RotateDEKRequest_PURPOSE_STORAGE:
		return fsmwire.PurposeStorage, nil
	case pb.RotateDEKRequest_PURPOSE_RAFT:
		return fsmwire.PurposeRaft, nil
	case pb.RotateDEKRequest_PURPOSE_UNSPECIFIED:
		// Proto3 default value — caller forgot to set purpose.
		// Falls into the same InvalidArgument as an out-of-range
		// value below; spelled out separately so the exhaustive
		// linter does not flag this switch.
		return 0, grpcStatusError(codes.InvalidArgument,
			"encryption: purpose must be PURPOSE_STORAGE or PURPOSE_RAFT, got PURPOSE_UNSPECIFIED")
	default:
		return 0, grpcStatusErrorf(codes.InvalidArgument,
			"encryption: purpose must be PURPOSE_STORAGE or PURPOSE_RAFT, got %v", p)
	}
}

// uint32ToLocalEpoch narrows a wire-format uint32 local_epoch to
// its on-disk uint16 representation. Callers MUST have rejected
// values > math.MaxUint16 first; the mask is a defence-in-depth
// truncation that cannot drift even if a future caller skips the
// bound check. gosec's G115 cannot see the prior validation
// because it lives at the gRPC handler boundary; the nolint here
// is scoped to this single conversion site so callsite drift is
// impossible.
// localEpochMask is the §4.1 16-bit nonce mask. Named here so the
// magic-number linter does not flag the masking conversion below.
const localEpochMask uint32 = 0xFFFF

func uint32ToLocalEpoch(v uint32) uint16 {
	return uint16(v & localEpochMask) //nolint:gosec // bound-checked + masked; G115 cannot trace across the handler boundary
}

func autoBuildSHA() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" {
			return setting.Value
		}
	}
	return ""
}
