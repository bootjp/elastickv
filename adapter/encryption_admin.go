package adapter

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"os"
	"runtime/debug"
	"strconv"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	pkgerrors "github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
)

// EncryptionAdminServer implements proto.EncryptionAdmin. PR-A
// (Stage 5 foundation) wired the read-only capability and state
// probes; PR-B (this slice) wires RotateDEK +
// RegisterEncryptionWriter as leader-only proposers on top of
// Stage 3's raft envelope and Stage 4's fsmwire bodies, plus the
// leader guard on ResyncSidecar. BootstrapEncryption stays
// Unimplemented until PR-C lands the §5.6 step 1a capability
// fan-out.
type EncryptionAdminServer struct {
	sidecarPath        string
	keystore           *encryption.Keystore
	fullNodeID         uint64
	buildSHA           string
	latestAppliedIndex func() uint64
	proposer           raftengine.Proposer
	leaderView         raftengine.LeaderView
	pb.UnimplementedEncryptionAdminServer
}

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
// keystore for the §5.5 fallback paths. Stage 5 PR-A does not require
// it because ReadSidecar covers every field the read-only RPCs need.
// PR-B will use it to fast-path the RotateDEK pre-check.
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
// PR-A serves this from the local sidecar without leadership
// verification because that is sufficient for the read-only
// observability tests in this PR. PR-B will add a leader-only
// guard so a follower with a stale sidecar cannot poison the
// recovery path of a peer with an even-staler sidecar.
func (s *EncryptionAdminServer) ResyncSidecar(_ context.Context, req *pb.ResyncSidecarRequest) (*pb.ResyncSidecarResponse, error) {
	// req.CallerFullNodeId is intentionally unused for the
	// recovery payload itself in PR-B; Stage 7 will use it to
	// scope the writer-registry projection to that specific
	// caller per §5.5. Recording it here keeps the field on the
	// hot path so a future leader-side audit log can correlate
	// resyncs to the requesting member without a wire-format
	// change.
	_ = req
	if err := s.requireLeader(); err != nil {
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

// RotateDEK proposes a §5.2 rotation as a §11.3 0x05 OpRotation
// entry. The server validates purpose / dek_id / local_epoch
// boundaries at the gRPC boundary (the §6.1 doc rule for
// local_epoch <= 0xFFFF on the wire) and the FSM apply layer
// enforces the sidecar-level invariants (no rotation while a
// previous rotation is in flight, etc). Returns the commit index
// once the entry is durable on a Raft quorum.
func (s *EncryptionAdminServer) RotateDEK(ctx context.Context, req *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
	if err := s.requireLeader(); err != nil {
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
	if len(req.GetWrappedNewDek()) == 0 {
		return nil, grpcStatusError(codes.InvalidArgument, "encryption: wrapped_new_dek is required")
	}
	if req.GetProposerLocalEpoch() > math.MaxUint16 {
		return nil, grpcStatusErrorf(codes.InvalidArgument,
			"encryption: proposer_local_epoch=%d exceeds the §4.1 16-bit bound (max 0xFFFF)",
			req.GetProposerLocalEpoch())
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
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if s.proposer == nil {
		return nil, grpcStatusError(codes.FailedPrecondition, "encryption: proposer is not configured on this node")
	}
	if req.GetDekId() == 0 {
		return nil, grpcStatusError(codes.InvalidArgument, "encryption: dek_id must be non-zero (key id 0 is reserved per §5.1)")
	}
	writers := req.GetWriters()
	if len(writers) != 1 {
		return nil, grpcStatusErrorf(codes.InvalidArgument,
			"encryption: RegisterEncryptionWriter requires exactly one writer in PR-B, got %d (use BootstrapEncryption for multi-writer batches)",
			len(writers))
	}
	w := writers[0]
	if w.GetLocalEpoch() > math.MaxUint16 {
		return nil, grpcStatusErrorf(codes.InvalidArgument,
			"encryption: writers[0].local_epoch=%d exceeds the §4.1 16-bit bound (max 0xFFFF)",
			w.GetLocalEpoch())
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
		// Raft-engine errors are operator-visible diagnostics:
		// not-leader / context-canceled / propose-queue full all
		// surface here. We surface the engine's error rather
		// than rewriting it so the operator can grep against
		// the engine's own logs.
		return 0, pkgerrors.Wrapf(err, "encryption: propose 0x%02x", opcode)
	}
	if res == nil {
		return 0, grpcStatusError(codes.Internal, "encryption: proposer returned nil result")
	}
	return res.CommitIndex, nil
}

// requireLeader rejects on followers with FailedPrecondition. The
// status detail embeds the currently-known leader's id and
// address so the operator's CLI can re-target without parsing
// free-form error text. A nil leaderView is allowed for tests
// that exercise the proposer wiring directly; the production
// wiring in main.go MUST register one.
func (s *EncryptionAdminServer) requireLeader() error {
	if s.leaderView == nil {
		return nil
	}
	if s.leaderView.State() == raftengine.StateLeader {
		return nil
	}
	leader := s.leaderView.Leader()
	if leader.ID == "" && leader.Address == "" {
		return grpcStatusError(codes.FailedPrecondition, "encryption: not leader (no known leader)")
	}
	return grpcStatusErrorf(codes.FailedPrecondition,
		"encryption: not leader (current leader id=%q address=%q)",
		leader.ID, leader.Address)
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
