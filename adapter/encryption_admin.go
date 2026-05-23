package adapter

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"os"
	"runtime/debug"
	"strconv"

	"github.com/bootjp/elastickv/internal/admin"
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"google.golang.org/grpc/codes"
)

// EncryptionAdminServer implements proto.EncryptionAdmin. Wires
// the §6.1 read-only probes (GetCapability, GetSidecarState,
// ResyncSidecar) plus the three mutating opcodes (Bootstrap,
// RotateDEK, RegisterEncryptionWriter) onto Stage 3's raft
// envelope and Stage 4's fsmwire body encoders. Mutators are
// leader-gated through requireLeader / VerifyLeader.
type EncryptionAdminServer struct {
	sidecarPath        string
	keystore           *encryption.Keystore
	fullNodeID         uint64
	buildSHA           string
	latestAppliedIndex func() uint64
	proposer           raftengine.Proposer
	leaderView         raftengine.LeaderView
	// capabilityFanout, when wired, runs the §4 Voters ∪ Learners
	// fan-out before the §7.1 Phase 1 cutover entry is proposed.
	// A nil value short-circuits EnableStorageEnvelope with
	// FailedPrecondition — the 6D-6 main.go wiring (lands in 6D-6c)
	// is what threads the route-snapshot builder + DialFunc +
	// timeout into this closure. Other mutator RPCs are unaffected.
	capabilityFanout CapabilityFanoutFn
	pb.UnimplementedEncryptionAdminServer
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

// WithEncryptionAdminKeystore lets the server consult the in-memory
// keystore for the §5.5 fallback paths. Stage 5 PR-A / PR-B do not
// require it because ReadSidecar covers every field the read-only
// RPCs need and RotateDEK relies on FSM-side validation rather
// than a pre-check. Stage 7 (writer registry) will use it for the
// in-memory counter fast-path.
func WithEncryptionAdminKeystore(k *encryption.Keystore) EncryptionAdminServerOption {
	return func(s *EncryptionAdminServer) {
		s.keystore = k
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
	s := &EncryptionAdminServer{}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	if s.buildSHA == "" {
		s.buildSHA = autoBuildSHA()
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
// The writer_registry_for_caller map is empty until Stage 7 wires
// the registry. Callers in the §7.1 cutover path tolerate an empty
// map because the §5.6 step 1a batch is sourced from the
// GetCapability fan-out, not from this RPC.
func (s *EncryptionAdminServer) GetSidecarState(_ context.Context, _ *pb.Empty) (*pb.SidecarStateReport, error) {
	if s.sidecarPath == "" {
		return nil, grpcStatusError(codes.FailedPrecondition, "encryption: sidecar path is not configured on this node")
	}
	sc, err := encryption.ReadSidecar(s.sidecarPath)
	if err != nil {
		return nil, statusFromSidecarErr(err)
	}
	resp := &pb.SidecarStateReport{
		ActiveStorageId:          sc.Active.Storage,
		ActiveRaftId:             sc.Active.Raft,
		StorageEnvelopeActive:    sc.StorageEnvelopeActive,
		RaftEnvelopeCutoverIndex: sc.RaftEnvelopeCutoverIndex,
		LatestAppliedIndex:       s.appliedIndex(sc.RaftAppliedIndex),
		WrappedDeksById:          wrappedDEKMap(sc),
		WriterRegistryForCaller:  map[uint32]uint32{},
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
	// req.CallerFullNodeId is intentionally unused for the
	// recovery payload itself in PR-B; Stage 7 will use it to
	// scope the writer-registry projection to that specific
	// caller per §5.5. Recording it here keeps the field on the
	// hot path so a future leader-side audit log can correlate
	// resyncs to the requesting member without a wire-format
	// change.
	_ = req
	if err := s.requireLeader(ctx); err != nil {
		return nil, err
	}
	if s.sidecarPath == "" {
		return nil, grpcStatusError(codes.FailedPrecondition, "encryption: sidecar path is not configured on this node")
	}
	sc, err := encryption.ReadSidecar(s.sidecarPath)
	if err != nil {
		return nil, statusFromSidecarErr(err)
	}
	return &pb.ResyncSidecarResponse{
		WrappedDeksById:          wrappedDEKMap(sc),
		ActiveStorageId:          sc.Active.Storage,
		ActiveRaftId:             sc.Active.Raft,
		LeaderLatestAppliedIndex: s.appliedIndex(sc.RaftAppliedIndex),
		// §5.5 follower-repair: leader's recorded
		// last_seen_local_epoch per (dek_id, caller). Stage 7
		// fills this from the writer registry. PR-A returns an
		// empty non-nil map because a node recovering before
		// the registry exists has nothing to re-derive.
		WriterRegistryForCaller: map[uint32]uint32{},
	}, nil
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
	idx, err := s.proposeEncryptionEntry(ctx, fsmwire.OpBootstrap, body)
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
	for i, w := range writers {
		if _, err := validateBootstrapWriter(i, w); err != nil {
			return nil, err
		}
	}
	if err := validateWriterBatchUniqueness(writers); err != nil {
		return nil, err
	}
	out := make([]fsmwire.RegistrationPayload, 0, totalRows)
	for _, w := range writers {
		epoch := uint32ToLocalEpoch(w.GetLocalEpoch())
		out = append(out,
			fsmwire.RegistrationPayload{DEKID: storageDEKID, FullNodeID: w.GetFullNodeId(), LocalEpoch: epoch},
			fsmwire.RegistrationPayload{DEKID: raftDEKID, FullNodeID: w.GetFullNodeId(), LocalEpoch: epoch},
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
		return fsmwire.RegistrationPayload{}, grpcStatusErrorf(codes.InvalidArgument,
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
		return nil, grpcStatusErrorf(codes.InvalidArgument,
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
	idx, err := s.proposeEncryptionEntry(ctx, fsmwire.OpRotation, body)
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
		return nil, nil, grpcStatusError(codes.FailedPrecondition,
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
func (s *EncryptionAdminServer) runCutoverFanout(ctx context.Context) (admin.CapabilityFanoutResult, error) {
	if s.capabilityFanout == nil {
		return admin.CapabilityFanoutResult{}, grpcStatusError(codes.FailedPrecondition,
			"encryption: capability fan-out is not configured on this node")
	}
	result, err := s.capabilityFanout(ctx)
	if err != nil {
		return admin.CapabilityFanoutResult{}, grpcStatusErrorf(codes.FailedPrecondition,
			"encryption: capability fan-out failed: %v", err)
	}
	if !result.OK {
		return admin.CapabilityFanoutResult{}, grpcStatusErrorf(codes.FailedPrecondition,
			"encryption: capability check refused cutover (%s)", capabilityRefusalSummary(result))
	}
	return result, nil
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
		return grpcStatusErrorf(codes.InvalidArgument,
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
		appliedIndex = proposedIdx
	}
	return &pb.EnableStorageEnvelopeResponse{
		AppliedIndex:        appliedIndex,
		CapabilitySummary:   projectCapabilityVerdicts(fanoutResult.Verdicts),
		CutoverIndexUnknown: false,
		WasAlreadyActive:    false,
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
		return nil, grpcStatusErrorf(codes.InvalidArgument,
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
	idx, err := s.proposeEncryptionEntry(ctx, fsmwire.OpRegistration, body)
	if err != nil {
		return nil, err
	}
	return &pb.RegisterEncryptionWriterResponse{AppliedIndex: idx}, nil
}

// proposeEncryptionEntry prepends the §11.3 opcode tag to a
// fsmwire-encoded body and submits the resulting Raft entry. The
// Stage 4 FSM apply path peels the tag, dispatches into
// applyEncryption, and Halt-Applies on any decode failure — this
// helper is just the byte-level glue between the server-side
// encoder and raftengine.Proposer.
func (s *EncryptionAdminServer) proposeEncryptionEntry(ctx context.Context, opcode byte, body []byte) (uint64, error) {
	entry := make([]byte, 0, 1+len(body))
	entry = append(entry, opcode)
	entry = append(entry, body...)
	res, err := s.proposer.Propose(ctx, entry)
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
	if s.leaderView == nil {
		return nil
	}
	if s.leaderView.State() != raftengine.StateLeader {
		leader := s.leaderView.Leader()
		if leader.ID == "" && leader.Address == "" {
			return grpcStatusError(codes.FailedPrecondition, "encryption: not leader (no known leader)")
		}
		return grpcStatusErrorf(codes.FailedPrecondition,
			"encryption: not leader (current leader id=%q address=%q)",
			leader.ID, leader.Address)
	}
	if err := s.leaderView.VerifyLeader(ctx); err != nil {
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
