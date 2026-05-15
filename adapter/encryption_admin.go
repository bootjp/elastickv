package adapter

import (
	"context"
	"errors"
	"os"
	"runtime/debug"
	"strconv"

	"github.com/bootjp/elastickv/internal/encryption"
	pb "github.com/bootjp/elastickv/proto"
	"google.golang.org/grpc/codes"
)

// EncryptionAdminServer implements proto.EncryptionAdmin. PR-A (Stage 5
// foundation) wires only the read-only capability and state probes
// plus ResyncSidecar; the Bootstrap / Rotate / Register RPCs return
// Unimplemented until PR-B lands the leader-side propose path on top
// of Stage 3's raft envelope and Stage 4's fsmwire bodies.
type EncryptionAdminServer struct {
	sidecarPath        string
	keystore           *encryption.Keystore
	fullNodeID         uint64
	buildSHA           string
	latestAppliedIndex func() uint64
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
// A node that has never been configured for encryption
// (sidecarPath empty, or the file does not exist) returns
// encryption_capable=false with local_epoch=0 and full_node_id=0
// per the §6.1 contract: the cutover command refuses with
// ErrCapabilityCheckFailed in that case so the empty epoch never
// reaches the writer registry.
func (s *EncryptionAdminServer) GetCapability(_ context.Context, _ *pb.Empty) (*pb.CapabilityReport, error) {
	if s.sidecarPath == "" {
		return &pb.CapabilityReport{BuildSha: s.buildSHA}, nil
	}
	sc, err := encryption.ReadSidecar(s.sidecarPath)
	switch {
	case err == nil:
	case errors.Is(err, os.ErrNotExist) || encryption.IsNotExist(err):
		return &pb.CapabilityReport{BuildSha: s.buildSHA}, nil
	default:
		return nil, statusFromSidecarErr(err)
	}
	return &pb.CapabilityReport{
		EncryptionCapable: sc.Active.Storage != 0,
		BuildSha:          s.buildSHA,
		SidecarPresent:    true,
		FullNodeId:        s.fullNodeID,
		// LocalEpoch stays at 0 until Stage 7 wires the §4.1
		// writer-registry counter. Bootstrap pre-check is
		// expected to call GetCapability before any DEK exists,
		// so 0 is the correct value at that point regardless.
		LocalEpoch: 0,
	}, nil
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
func (s *EncryptionAdminServer) ResyncSidecar(_ context.Context, _ *pb.ResyncSidecarRequest) (*pb.ResyncSidecarResponse, error) {
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
// the entry rather than letting the RPC fall over.
func wrappedDEKMap(sc *encryption.Sidecar) map[uint32][]byte {
	out := make(map[uint32][]byte, len(sc.Keys))
	for idStr, key := range sc.Keys {
		id, err := parseSidecarKeyID(idStr)
		if err != nil {
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
