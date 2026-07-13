package kv

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"os"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/s3keys"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

// hlcCeilingFromHLC returns the physical ceiling stored in hlc, or 0 if nil.
func hlcCeilingFromHLC(hlc *HLC) int64 {
	if hlc == nil {
		return 0
	}
	return hlc.PhysicalCeiling()
}

type kvFSM struct {
	store store.MVCCStore
	log   *slog.Logger
	// hlc is the shared HLC instance updated when a HLC lease entry is applied.
	// May be nil for nodes that do not participate in physical ceiling tracking.
	hlc *HLC
	// encryption is the §6.3 / §11.3 encryption-opcode applier. nil in
	// Stage 4 default and in tests that do not exercise opcodes 0x03/
	// 0x04/0x05; Stage 5/6/7 wire a concrete implementation that
	// mutates the keystore + sidecar + writer registry. With nil
	// installed, the encryption-opcode dispatch fails closed via
	// HaltApply rather than silently advancing setApplied past a
	// proposal the local node cannot process.
	encryption EncryptionApplier
	// pendingApplyIdx is the Raft entry index the engine is about
	// to (or currently is) applying. raftengine sets it via the
	// raftengine.ApplyIndexAware seam immediately before each
	// Apply, and applyEncryption reads it to thread the index into
	// EncryptionApplier.ApplyBootstrap / ApplyRotation so the
	// sidecar's RaftAppliedIndex is recorded in the same
	// crash-durable WriteSidecar fsync. Stays 0 for engines that
	// do not implement the seam (the legacy fallback returns 0 to
	// the applier; the applier treats 0 as "skip RaftAppliedIndex
	// write", preserving Stage 6A behavior for backends that did
	// not opt in).
	pendingApplyIdx uint64
	// cutoverSource provides the writer-side view of the Phase-2
	// envelope cutover index for snapshot v1/v2 selection (Stage
	// 8a §3.3). nil = always v1 output.
	cutoverSource CutoverSource
	// snapLatch enforces the §3.3 no-downgrade invariant: once a
	// non-zero cutover is observed, all subsequent snapshots from
	// this FSM emit v2 even if the sidecar later reads 0.
	snapLatch noDowngradeLatch
	// restoredCutover is the Stage 8a §3.4 snapshot-to-applier
	// handoff: Restore stores the v2 cutover here for Stage 6E's
	// apply-hook to consume. Stays 0 for v1 restores and headerless
	// legacy paths (matches the pre-Phase-2 posture exactly).
	restoredCutover uint64
	// routes is the M2 Composed-1 versioned-snapshot provider — the
	// route catalog history this FSM consults at apply time to
	// resolve OwnerOf(key) for the txn's observed catalog version.
	// nil at M2 for nodes that have not been wired (legacy
	// constructors), in which case the M3 verifyComposed1 gate will
	// short-circuit as "unpinned" (no Composed-1 enforcement) —
	// matching the pre-feature behaviour byte-for-byte.  Concrete
	// production type is *distribution.Engine.  See
	// docs/design/2026_05_29_implemented_composed1_cross_group_commit_guard.md
	// §4.2 prerequisite block + §M2.
	routes RouteHistory
	// shardGroupID is the Raft group ID this FSM serves.  Used by
	// the M3 verifyComposed1 gate to compare against the historical
	// owner-of-key resolution: a txn's commit MUST land on the
	// group that owned the key at the txn's observed catalog
	// version.  Zero at M2 means "unset" — the M3 gate will
	// short-circuit (matches the pre-feature behaviour).  Wired by
	// WithRouteHistory from main.go's shard-group construction.
	shardGroupID uint64
	// applyObservers are called after successful logical mutations.
	// They are never mutated after NewKvFSMWithHLC returns.
	applyObservers []ApplyObserver
}

// RouteHistory is the kv-side interface to the route catalog's
// versioned-snapshot ring.  *distribution.Engine satisfies it via
// WrapDistributionEngine.  Defined in the kv package so kvFSM does
// not have to import a concrete type for the field; the M3
// verifyComposed1 gate uses only SnapshotAt + Current + the returned
// snapshot's OwnerOf, so the interface stays minimal.
type RouteHistory interface {
	// SnapshotAt returns the route catalog at the given catalog
	// version.  Returns (zero, false) when the version is outside
	// the ring (either evicted by depth, or in the future).  The
	// M3 gate maps the not-found case to ErrComposed1VersionGCd.
	SnapshotAt(version uint64) (RouteSnapshot, bool)
	// Current returns the route catalog snapshot at the engine's
	// current catalog version.  Returns (zero, false) when the
	// engine has no history (bare-struct case used by some test
	// seams).  The M3 cross-version fence uses this to compare
	// the txn's observed-version owner against the current
	// owner — a mismatch is the §3 codex P1 trace.
	Current() (RouteSnapshot, bool)
}

// RouteSnapshot is the historical view of the route catalog at a
// specific version.  Returned by RouteHistory.SnapshotAt; the M3
// gate uses Version + OwnerOf to compare against the FSM's
// shardGroupID.
type RouteSnapshot interface {
	// Version returns the catalog version this snapshot was
	// recorded at.
	Version() uint64
	// OwnerOf returns the Raft group ID that owned key at this
	// snapshot's version.  (0, false) when no route covered key.
	OwnerOf(key []byte) (uint64, bool)
	// RouteOf returns the complete route descriptor covering key.
	RouteOf(key []byte) (distribution.Route, bool)
	// IntersectingRoutes returns every route intersecting [start, end).
	IntersectingRoutes(start, end []byte) []distribution.Route
	// WriteFencedForKey reports whether key is currently inside a
	// WriteFenced route in this snapshot.
	WriteFencedForKey(key []byte) bool
	// WriteFencedIntersects reports whether [start, end) intersects
	// any WriteFenced route in this snapshot.
	WriteFencedIntersects(start, end []byte) bool
}

// SetApplyIndex implements raftengine.ApplyIndexAware. The engine
// invokes this on the same goroutine as Apply (Raft applies are
// serial), so plain field assignment is race-free under that
// contract. Tests that drive Apply directly should call this with
// the index they want propagated to the encryption applier (or
// leave it at 0 to opt out, matching the pre-Stage-6C-2d default).
func (f *kvFSM) SetApplyIndex(idx uint64) {
	f.pendingApplyIdx = idx
}

// LastAppliedIndex implements raftengine.AppliedIndexReader by
// forwarding to the underlying store when it supports the reader
// seam (pebbleStore does; the in-memory mvccStore does not).
//
// Round-1 of this PR shipped this as a factory method
// (AppliedIndexReader() AppliedIndexReader) intended to be called
// through a separate AppliedIndexReporter interface. Codex P2 on
// round-4 (kv/fsm.go:145) correctly pointed out that the planned
// cold-start skip gate (Branch 3) will type-assert
// fsm.(raftengine.AppliedIndexReader) directly — a factory method
// with a different signature does NOT satisfy the interface, so
// the skip would always fall back even after the meta key is
// populated. Renaming the method to LastAppliedIndex and inlining
// the type-assert forward makes kvFSM directly satisfy
// raftengine.AppliedIndexReader.
//
// (0, false, nil) propagates the strictly-additive fallback when
// the store does not expose the seam — the future skip gate treats
// "missing" as "fall back to full restore." See
// docs/design/2026_06_02_implemented_idempotent_snapshot_restore.md §3 / §4.
func (f *kvFSM) LastAppliedIndex() (uint64, bool, error) {
	r, ok := f.store.(raftengine.AppliedIndexReader)
	if !ok {
		return 0, false, nil
	}
	idx, present, err := r.LastAppliedIndex()
	if err != nil {
		return 0, false, errors.WithStack(err)
	}
	return idx, present, nil
}

// SetDurableAppliedIndex implements raftengine.AppliedIndexWriter by
// forwarding to the underlying store when it supports the writer
// seam. Called by the engine at every snapshot persist site BEFORE
// persist.SaveSnap so a successful snapshot persist implies
// LastAppliedIndex >= snap.Metadata.Index, closing the HLC-lease-
// only / encryption-only fallback (PR #910 design §6).
//
// Returns nil silently when the backing store does not implement
// the writer seam (in-memory mvccStore, test fakes) — the skip
// optimisation simply degrades to "fall back to full restore" for
// those FSMs.
func (f *kvFSM) SetDurableAppliedIndex(idx uint64) error {
	w, ok := f.store.(raftengine.AppliedIndexWriter)
	if !ok {
		return nil
	}
	return errors.WithStack(w.SetDurableAppliedIndex(idx))
}

type FSM interface {
	raftengine.StateMachine
}

// FSMOption configures a *kvFSM at construction. Stage 4 introduces
// WithEncryption; future stages may add more.
type FSMOption func(*kvFSM)

// WithEncryption installs the EncryptionApplier the kvFSM dispatches
// opcodes 0x03 / 0x04 / 0x05 to. Pass nil (or omit the option
// entirely) to leave the FSM in the Stage-4-default fail-closed
// state where any encryption opcode halts the apply loop via
// ErrEncryptionApply.
func WithEncryption(applier EncryptionApplier) FSMOption {
	return func(f *kvFSM) {
		f.encryption = applier
	}
}

// WithCutoverSource installs the Stage 8a §3.3 writer-side view of the
// Phase-2 envelope cutover index. The FSM consults it once per Snapshot
// call to decide v1 vs v2 layout. Pass nil (or omit the option) to keep
// the pre-8a posture where every snapshot is v1.
func WithCutoverSource(src CutoverSource) FSMOption {
	return func(f *kvFSM) {
		f.cutoverSource = src
	}
}

// WithRouteHistory installs the M2 Composed-1 versioned-snapshot
// provider and the FSM's owning shard group ID.  Both fields are
// consumed by the M3 verifyComposed1 gate.  At M2 the values are
// stored but not consulted (M3 wires the check); a caller that
// constructs a kvFSM without this option remains "unpinned" — the
// M3 gate will short-circuit and behave exactly like the
// pre-feature FSM.
//
// shardGroupID MUST match the Raft group ID this FSM serves — the
// gate uses it as the "this group" value when comparing against the
// historical owner-of-key resolution.  Zero is reserved for the
// not-wired case.
//
// See docs/design/2026_05_29_implemented_composed1_cross_group_commit_guard.md
// §M2 + §4.2 prerequisite block.
func WithRouteHistory(routes RouteHistory, shardGroupID uint64) FSMOption {
	return func(f *kvFSM) {
		f.routes = routes
		f.shardGroupID = shardGroupID
	}
}

// NewKvFSMWithHLC creates a KV FSM that updates hlc.physicalCeiling whenever
// a HLC lease entry is applied. The caller must pass the same *HLC instance to
// the coordinator so both sides share the agreed physical ceiling.
//
// Optional FSMOption arguments configure additional handlers (see
// WithEncryption). Existing callers without options keep the
// pre-Stage-4 behaviour byte-for-byte.
func NewKvFSMWithHLC(store store.MVCCStore, hlc *HLC, opts ...FSMOption) FSM {
	f := &kvFSM{
		store: store,
		log: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		})),
		hlc: hlc,
	}
	for _, opt := range opts {
		opt(f)
	}
	f.snapLatch.log = f.log
	return f
}

var _ FSM = (*kvFSM)(nil)
var _ raftengine.StateMachine = (*kvFSM)(nil)

var ErrUnknownRequestType = errors.New("unknown request type")

// ErrRouteWriteFenced is returned when a mutation targets a route that is in
// WriteFenced state during split migration. Callers should retry after routing
// catches up to the promoted owner.
var ErrRouteWriteFenced = errors.New("route is write-fenced; retry after route migration")

var ErrRouteWriteTimestampTooLow = errors.New("route write timestamp is below migration floor")

// ErrComposed1Violation is returned by verifyComposed1 when the
// transaction's commit cannot proceed on this Raft group because the
// txn's read-set or write-set keys are not owned by this group at
// either the txn's observed catalog version (the spec-level §4.2(a)
// check) or the current catalog version observed by the FSM at apply
// time (the §4.4 cross-version-read fence).  Surfaces to the
// coordinator as a retryable error: the M4 coordinator path re-reads
// the route cache, re-routes the txn, and re-issues it once on the
// new owning group.
//
// Wrapped with errors.Wrapf at the call site to carry the
// per-key diagnostic (which key, which observed-version owner, which
// current-version owner) — the caller's retry path uses
// errors.Is(err, ErrComposed1Violation) to match.
var ErrComposed1Violation = errors.New("composed-1: route ownership shifted; retry on new owning group")

// ErrComposed1VersionGCd is returned by verifyComposed1 when the
// txn's observed catalog version is no longer in the engine's
// retention ring — either because the FIFO ring evicted it (the
// txn lived longer than `routeHistoryDepth` versions worth of
// catalog churn) or because the version was never seen on this
// node.  Surfaces to the coordinator as a retryable error: the
// caller's M4 retry path reads the current route cache and
// re-issues the txn with a fresh observedVer.
//
// The not-found ⇒ hard-error semantics (rather than soft-pass)
// matters because a soft-pass would let the gate be bypassed
// exactly in the long-running-txn / high-churn cases where the
// cross-version-read hazard is most likely (design doc §4.3 +
// gemini medium + codex P2 on PR #870).
var ErrComposed1VersionGCd = errors.New("composed-1: observed catalog version evicted from history ring; retry")

type fsmApplyResponse struct {
	results []error
}

func (f *kvFSM) Apply(data []byte) any {
	ctx := context.TODO()
	if resp, handled := f.applyReservedOpcode(ctx, data); handled {
		return resp
	}

	reqs, err := decodeRaftRequests(data)
	if err != nil {
		return errors.WithStack(err)
	}
	if len(reqs) == 1 {
		return f.applyRequest(ctx, reqs[0])
	}

	resp := &fsmApplyResponse{results: make([]error, len(reqs))}
	hasError := false
	for i, req := range reqs {
		err := f.applyRequestErr(ctx, req)
		if err == nil {
			continue
		}
		resp.results[i] = err
		hasError = true
	}
	if hasError {
		return resp
	}
	return nil
}

// applyReservedOpcode handles the FSM-internal opcode bands that are
// dispatched ahead of the legacy proto3 fallback: HLC-lease entries
// (0x02) and the encryption opcode range (§6.3 / §11.3 — 0x03..0x07,
// the band reserved per the proto3 wire-tag analysis in
// fsmwire.OpEncryptionMin/Max). Returning handled=false lets Apply
// fall through to the legacy raft-request decode path. Extracted from
// Apply to keep the dispatcher under the cyclomatic-complexity budget.
//
// Encryption-opcode dispatch is the codex P1 fix for PR748: a stray
// future encryption opcode (e.g. 0x06) — including one emitted by a
// NEWER leader during a rolling upgrade against a STALE follower —
// must not fall through to proto3 unmarshal and silently advance
// setApplied. applyEncryption's default case wraps any unimplemented
// opcode with ErrEncryptionApply, which the engine's HaltApply seam
// recognises as a halt — same fail-closed shape as the Stage 3
// raft-envelope unwrap path.
func (f *kvFSM) applyReservedOpcode(ctx context.Context, data []byte) (any, bool) {
	if len(data) == 0 {
		return nil, false
	}
	switch {
	case data[0] == raftEncodeHLCLease:
		return f.applyHLCLease(data[1:]), true
	case data[0] == raftEncodeMigrationImport:
		return f.applyMigrationImport(ctx, data[1:]), true
	case data[0] == raftEncodeMigrationPromote:
		return f.applyMigrationPromote(ctx, data[1:]), true
	case data[0] >= fsmwire.OpEncryptionMin && data[0] <= fsmwire.OpEncryptionMax:
		return f.applyEncryption(f.pendingApplyIdx, data[0], data[1:]), true
	default:
		return nil, false
	}
}

// hlcLeasePayloadLen is the payload length (tag byte excluded) of an HLC lease entry.
const hlcLeasePayloadLen = 8 //nolint:mnd

// applyHLCLease decodes a physical ceiling from data and advances the shared HLC.
// data must be exactly 8 bytes: a big-endian int64 Unix millisecond value.
func (f *kvFSM) applyHLCLease(data []byte) any {
	if len(data) != hlcLeasePayloadLen {
		return errors.Newf("hlc lease: expected %d bytes, got %d", hlcLeasePayloadLen, len(data)) //nolint:wrapcheck // creating new error, nothing to wrap
	}
	ceilingMs := int64(binary.BigEndian.Uint64(data)) //nolint:gosec // value is a Unix ms timestamp encoded as uint64; fits in int64 for any sane date
	if f.hlc != nil && ceilingMs > 0 {
		f.hlc.SetPhysicalCeiling(ceilingMs)
	}
	return nil
}

const (
	raftEncodeSingle byte = 0x00
	raftEncodeBatch  byte = 0x01
	// raftEncodeHLCLease marks an entry that carries only a physical ceiling for
	// the HLC. The payload is 8 bytes: a big-endian int64 Unix millisecond value.
	// These entries do not touch the MVCC store; they only advance the shared HLC
	// physicalCeiling so the logical counter can continue to increment in memory.
	raftEncodeHLCLease byte = 0x02
	// raftEncodeMigrationImport carries a target-group range-migration import
	// batch. Every target voter applies the raw MVCC versions, import ack, and
	// migration HLC floor before the RPC handler returns success.
	raftEncodeMigrationImport byte = 0x09
	// raftEncodeMigrationPromote carries a target-group range-migration staged
	// data promotion chunk. Every target voter atomically copies staged MVCC
	// versions into the live keyspace and removes the promoted staged rows.
	raftEncodeMigrationPromote byte = 0x0b
)

func decodeRaftRequests(data []byte) ([]*pb.Request, error) {
	if len(data) == 0 {
		return nil, errors.WithStack(ErrInvalidRequest)
	}

	switch data[0] {
	case raftEncodeSingle:
		req := &pb.Request{}
		if err := proto.Unmarshal(data[1:], req); err != nil {
			return nil, errors.WithStack(err)
		}
		return []*pb.Request{req}, nil
	case raftEncodeBatch:
		cmd := &pb.RaftCommand{}
		if err := proto.Unmarshal(data[1:], cmd); err != nil {
			return nil, errors.WithStack(err)
		}
		if len(cmd.Requests) == 0 {
			return nil, errors.WithStack(ErrInvalidRequest)
		}
		return cmd.Requests, nil
	default:
		return decodeLegacyRaftRequest(data)
	}
}

func decodeLegacyRaftRequest(data []byte) ([]*pb.Request, error) {
	cmd := &pb.RaftCommand{}
	if err := proto.Unmarshal(data, cmd); err == nil && len(cmd.Requests) > 0 {
		return cmd.Requests, nil
	}
	req := &pb.Request{}
	if err := proto.Unmarshal(data, req); err != nil {
		return nil, errors.WithStack(err)
	}
	return []*pb.Request{req}, nil
}

func requestCommitTS(r *pb.Request) (uint64, error) {
	if r == nil {
		return 0, errors.WithStack(ErrInvalidRequest)
	}

	commitTS := r.Ts
	if r.IsTxn && (r.Phase == pb.Phase_COMMIT || r.Phase == pb.Phase_ABORT || r.Phase == pb.Phase_NONE) {
		meta, _, err := extractTxnMeta(r.Mutations)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		if meta.CommitTS == 0 {
			return 0, errors.WithStack(ErrTxnCommitTSRequired)
		}
		commitTS = meta.CommitTS
	}
	return commitTS, nil
}

func (f *kvFSM) applyRequest(ctx context.Context, r *pb.Request) any {
	if err := f.applyRequestErr(ctx, r); err != nil {
		return err
	}
	return nil
}

func (f *kvFSM) applyRequestErr(ctx context.Context, r *pb.Request) error {
	commitTS, err := requestCommitTS(r)
	if err != nil {
		return err
	}
	if err := f.handleRequest(ctx, r, commitTS); err != nil {
		return errors.WithStack(err)
	}
	f.observeAppliedCommitTS(commitTS)
	return nil
}

func (f *kvFSM) handleRequest(ctx context.Context, r *pb.Request, commitTS uint64) error {
	switch {
	case r.IsTxn:
		return f.handleTxnRequest(ctx, r, commitTS)
	default:
		return f.handleRawRequest(ctx, r, commitTS)
	}
}

func (f *kvFSM) handleRawRequest(ctx context.Context, r *pb.Request, commitTS uint64) error {
	// DEL_PREFIX mutations are handled by the store's DeletePrefixAt which
	// scans and writes tombstones locally. A DEL_PREFIX request must be the
	// sole mutation in a request (enforced by the coordinator's toRawRequest).
	if hasDelPrefix, prefix := extractDelPrefix(r.Mutations); hasDelPrefix {
		return f.handleDelPrefix(ctx, prefix, commitTS)
	}

	if err := f.validateRawMutationsForApply(ctx, r.Mutations, commitTS); err != nil {
		return err
	}

	muts, err := toStoreMutations(r.Mutations)
	if err != nil {
		return errors.WithStack(err)
	}
	// Raw requests always commit against the latest state; use commitTS as both
	// the validation snapshot and the commit timestamp.
	if err := f.store.ApplyMutationsRaftAt(ctx, muts, nil, commitTS, commitTS, f.pendingApplyIdx); err != nil {
		return errors.WithStack(err)
	}
	f.notifyApplyObservers(commitTS, r.Mutations)
	return nil
}

func (f *kvFSM) validateRawMutationsForApply(ctx context.Context, muts []*pb.Mutation, commitTS uint64) error {
	for _, mut := range muts {
		if err := f.validateRawMutationForApply(ctx, mut, commitTS); err != nil {
			return err
		}
	}
	return nil
}

func (f *kvFSM) validateRawMutationForApply(ctx context.Context, mut *pb.Mutation, commitTS uint64) error {
	if mut == nil || len(mut.Key) == 0 {
		return errors.WithStack(ErrInvalidRequest)
	}
	// Raw requests should not mutate txn-internal keys.
	if isTxnInternalKey(mut.Key) {
		return errors.WithStack(ErrInvalidRequest)
	}
	if err := f.verifyRouteNotFencedForKey(mut.Key); err != nil {
		return err
	}
	if err := f.verifyRouteWriteTimestampFloorForKey(mut.Key, commitTS); err != nil {
		return err
	}
	if err := f.assertNoConflictingTxnLock(ctx, mut.Key, nil, 0); err != nil {
		return err
	}
	return nil
}

// extractDelPrefix checks if the mutations contain a DEL_PREFIX operation.
// If found, it validates that no other operation types are mixed in.
func extractDelPrefix(muts []*pb.Mutation) (bool, []byte) {
	for _, mut := range muts {
		if mut != nil && mut.Op == pb.Op_DEL_PREFIX {
			return true, mut.Key
		}
	}
	return false, nil
}

// handleDelPrefix delegates prefix deletion to the store. Transaction-internal
// keys are always excluded to preserve transactional integrity.
func (f *kvFSM) handleDelPrefix(ctx context.Context, prefix []byte, commitTS uint64) error {
	if err := f.verifyRouteNotFencedForPrefix(prefix); err != nil {
		return err
	}
	if err := f.verifyRouteWriteTimestampFloorForPrefix(prefix, commitTS); err != nil {
		return err
	}
	if err := f.store.DeletePrefixAtRaftAt(ctx, prefix, txnCommonPrefix, commitTS, f.pendingApplyIdx); err != nil {
		return errors.WithStack(err)
	}
	f.notifyApplyObserver(commitTS, pb.Op_DEL_PREFIX, prefix)
	return nil
}

func (f *kvFSM) verifyRouteWriteTimestampFloorForKey(key []byte, commitTS uint64) error {
	if f.routes == nil || commitTS == 0 {
		return nil
	}
	snap, ok := f.routes.Current()
	if !ok {
		return nil
	}
	rkey := routeKey(key)
	route, ok := snap.RouteOf(rkey)
	if !ok || route.MinWriteTSExclusive == 0 || commitTS > route.MinWriteTSExclusive {
		return nil
	}
	return errors.Wrapf(ErrRouteWriteTimestampTooLow, "key %q routeKey %q commit_ts=%d floor=%d", key, rkey, commitTS, route.MinWriteTSExclusive)
}

func (f *kvFSM) verifyRouteWriteTimestampFloorForPrefix(prefix []byte, commitTS uint64) error {
	if f.routes == nil || commitTS == 0 {
		return nil
	}
	snap, ok := f.routes.Current()
	if !ok {
		return nil
	}
	start, end := routePrefixRange(prefix)
	for _, route := range snap.IntersectingRoutes(start, end) {
		if route.MinWriteTSExclusive != 0 && commitTS <= route.MinWriteTSExclusive {
			return errors.Wrapf(ErrRouteWriteTimestampTooLow, "prefix %q route range [%q,%q) commit_ts=%d floor=%d", prefix, start, end, commitTS, route.MinWriteTSExclusive)
		}
	}
	return nil
}

func (f *kvFSM) verifyRouteWriteTimestampFloorForMutations(muts []*pb.Mutation, commitTS uint64) error {
	for _, mut := range muts {
		if mut == nil || len(mut.Key) == 0 || isTxnInternalKey(mut.Key) {
			continue
		}
		if err := f.verifyRouteWriteTimestampFloorForKey(mut.Key, commitTS); err != nil {
			return err
		}
	}
	return nil
}

func (f *kvFSM) verifyRouteNotFencedForMutations(muts []*pb.Mutation) error {
	for _, mut := range muts {
		if mut == nil || len(mut.Key) == 0 || isTxnInternalKey(mut.Key) {
			continue
		}
		if err := f.verifyRouteNotFencedForKey(mut.Key); err != nil {
			return err
		}
	}
	return nil
}

func (f *kvFSM) verifyRouteNotFencedForKey(key []byte) error {
	if f.routes == nil {
		return nil
	}
	snap, ok := f.routes.Current()
	if !ok {
		return nil
	}
	rkey := routeKey(key)
	if !snap.WriteFencedForKey(rkey) {
		return nil
	}
	return errors.Wrapf(ErrRouteWriteFenced, "key %q routeKey %q", key, rkey)
}

func (f *kvFSM) verifyRouteNotFencedForPrefix(prefix []byte) error {
	if f.routes == nil {
		return nil
	}
	snap, ok := f.routes.Current()
	if !ok {
		return nil
	}
	start, end := routePrefixRange(prefix)
	if !snap.WriteFencedIntersects(start, end) {
		return nil
	}
	return errors.Wrapf(ErrRouteWriteFenced, "prefix %q route range [%q,%q)", prefix, start, end)
}

func routePrefixRange(prefix []byte) ([]byte, []byte) {
	if len(prefix) == 0 {
		return []byte(""), nil
	}
	if routeKeyspaceWideRawPrefix(prefix) {
		return []byte(""), nil
	}
	start := routeKey(prefix)
	return start, prefixScanEnd(start)
}

func routeKeyspaceWideRawPrefix(prefix []byte) bool {
	if !rawPrefixMayContainRouteMappedKeys(prefix) {
		return false
	}
	return bytes.Equal(routeKey(prefix), prefix)
}

func rawPrefixMayContainRouteMappedKeys(prefix []byte) bool {
	for _, mappedPrefix := range routeMappedRawPrefixes {
		if bytes.HasPrefix(prefix, mappedPrefix) || bytes.HasPrefix(mappedPrefix, prefix) {
			return true
		}
	}
	return false
}

var routeMappedRawPrefixes = [][]byte{
	[]byte(redisInternalRoutePrefix),
	[]byte(DynamoTableMetaPrefix),
	[]byte(DynamoTableGenerationPrefix),
	[]byte(DynamoItemPrefix),
	[]byte(DynamoGSIPrefix),
	[]byte(sqsInternalPrefix),
	[]byte(store.ListMetaPrefix),
	[]byte(store.ListItemPrefix),
	[]byte(store.ListMetaDeltaPrefix),
	[]byte(store.ListClaimPrefix),
	[]byte(store.HashMetaPrefix),
	[]byte(store.HashFieldPrefix),
	[]byte(store.HashMetaDeltaPrefix),
	[]byte(store.SetMetaPrefix),
	[]byte(store.SetMemberPrefix),
	[]byte(store.SetMetaDeltaPrefix),
	[]byte(store.ZSetMetaPrefix),
	[]byte(store.ZSetMemberPrefix),
	[]byte(store.ZSetScorePrefix),
	[]byte(store.ZSetMetaDeltaPrefix),
	[]byte(store.StreamMetaPrefix),
	[]byte(store.StreamEntryPrefix),
	[]byte(s3keys.BucketMetaPrefix),
	[]byte(s3keys.BucketGenerationPrefix),
	[]byte(s3keys.ObjectManifestPrefix),
	[]byte(s3keys.UploadMetaPrefix),
	[]byte(s3keys.UploadPartPrefix),
	[]byte(s3keys.BlobPrefix),
	[]byte(s3keys.GCUploadPrefix),
	[]byte(s3keys.RoutePrefix),
}

var ErrNotImplemented = errors.New("not implemented")

func (f *kvFSM) Snapshot() (raftengine.Snapshot, error) {
	snapshot, err := f.store.Snapshot()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	cutover := uint64(0)
	if f.cutoverSource != nil {
		cutover = f.cutoverSource.RaftEnvelopeCutoverIndex()
	}
	emitted := f.snapLatch.observe(cutover)
	useV2 := emitted != 0 || f.snapLatch.latched()

	return &kvFSMSnapshot{
		snapshot:  snapshot,
		ceilingMs: hlcCeilingFromHLC(f.hlc),
		cutover:   emitted,
		useV2:     useV2,
	}, nil
}

func (f *kvFSM) Restore(r io.Reader) error {
	// Stage 8a §3.2: the bufio.Reader is owned by the caller (us) and
	// MUST be passed unchanged to the inner-store path on every branch
	// — buffered bytes can sit between the header consumption and the
	// inner-store read; switching readers silently loses them. If the
	// engine already handed us a *bufio.Reader, reuse it to avoid the
	// double-buffering overhead (gemini MEDIUM on PR #886); otherwise
	// wrap once at this boundary.
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReader(r)
	}
	ceilingU, cutover, err := ReadSnapshotHeader(br)
	if err != nil {
		return errors.WithStack(err)
	}
	if f.hlc != nil && ceilingU > 0 {
		f.hlc.SetPhysicalCeiling(int64(ceilingU)) //nolint:gosec // ceiling is a Unix ms timestamp encoded as uint64
	}
	// §3.4 snapshot-to-applier handoff: store the cutover for Stage
	// 6E's apply-hook to read on the next apply. Stays 0 for v1 and
	// headerless-legacy restores.
	f.restoredCutover = cutover
	return errors.WithStack(f.store.Restore(io.NopCloser(br)))
}

// RestoredCutover returns the most recently restored v2
// raft_envelope_cutover_index, or 0 if the last Restore was v1 / headerless
// / has not yet been called. Stage 6E will consume this from the apply
// hook; visible here for test inspection (design §3.4 + §5).
func (f *kvFSM) RestoredCutover() uint64 {
	return f.restoredCutover
}

// ParseSnapshotHeader implements raftengine.SnapshotHeaderApplier
// phase 1 — the cold-start skip path's parse-without-side-effect
// step. The engine has wrapped `r` in a crc32 TeeReader sized at
// the body payload (file size minus 4-byte footer), so every byte
// pulled from `r` flows through the engine's hash. We read the
// v1/v2 header via ReadSnapshotHeader, then drain the rest of the
// body so the wrapping hash covers every payload byte — matching
// restoreAndComputeCRC's behaviour in openAndRestoreFSMSnapshot.
//
// IMPORTANT: this method MUST NOT touch f.hlc or f.restoredCutover.
// The engine calls ApplySnapshotHeader separately, only after the
// wrapping CRC verification passes. Mutating FSM state here would
// defeat the "no side-effect on CRC failure" contract that the
// PR #910 design §5 round-7 split is designed to preserve.
func (f *kvFSM) ParseSnapshotHeader(r io.Reader) (uint64, uint64, error) {
	br := bufio.NewReaderSize(r, 1<<20) //nolint:mnd // 1 MiB, local to kv
	ceiling, cutover, err := ReadSnapshotHeader(br)
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}
	// Drain the remainder so the engine's TeeReader-wrapped CRC
	// covers every byte of the body (LimitReader exhaustion
	// signals "full payload consumed" to the caller).
	if _, err := io.Copy(io.Discard, br); err != nil {
		return 0, 0, errors.WithStack(err)
	}
	return ceiling, cutover, nil
}

// ApplySnapshotHeader implements raftengine.SnapshotHeaderApplier
// phase 2 — pure assignment of the verified header state. Called
// only after ParseSnapshotHeader returned successfully AND the
// engine's wrapping crc32 hash matched the file footer. Mirrors
// the two side-effects Restore would have applied for the header
// portion (HLC physical ceiling + restoredCutover). See PR #910
// design §5 round-7.
func (f *kvFSM) ApplySnapshotHeader(ceiling, cutover uint64) {
	if f.hlc != nil && ceiling > 0 {
		f.hlc.SetPhysicalCeiling(int64(ceiling)) //nolint:gosec // ceiling is a Unix ms timestamp encoded as uint64
	}
	f.restoredCutover = cutover
}

// IsVolatileOnlyPayload satisfies raftengine.VolatileEntryClassifier.
// Returns true iff payload is an HLC lease entry (raftEncodeHLCLease
// tag, 0x02) — those entries only call HLC.SetPhysicalCeiling, which
// is monotonic and lives purely in memory. After the cold-start skip
// gate fires, the engine still delivers WAL committed-tail entries
// past snapshot.Metadata.Index; without this classifier those
// volatile entries get dropped along with KV/MVCC duplicates and the
// post-snapshot ceiling raise is lost. Codex P1 #934 round 7.
//
// Re-applying KV/MVCC entries would re-execute OCC validation against
// store state that has already moved past commit_ts, surfacing
// spurious conflicts. Returning false for any non-HLC payload tag
// preserves that idempotency. Encryption opcodes (0x03..0x07) MUST
// also return false — they persist DEK state in the encryption
// sidecar and re-applying would diverge the sidecar's
// RaftAppliedIndex from the engine's appliedIndex.
func (f *kvFSM) IsVolatileOnlyPayload(payload []byte) bool {
	return len(payload) > 0 && payload[0] == raftEncodeHLCLease
}

func (f *kvFSM) handleTxnRequest(ctx context.Context, r *pb.Request, commitTS uint64) error {
	if err := f.verifyComposed1(r); err != nil {
		return err
	}
	switch r.Phase {
	case pb.Phase_PREPARE:
		return f.handlePrepareRequest(ctx, r)
	case pb.Phase_COMMIT:
		return f.handleCommitRequest(ctx, r)
	case pb.Phase_ABORT:
		return f.handleAbortRequest(ctx, r, commitTS)
	case pb.Phase_NONE:
		return f.handleOnePhaseTxnRequest(ctx, r, commitTS)
	default:
		return errors.WithStack(ErrUnknownRequestType)
	}
}

// verifyComposed1 is the M3 apply-time Composed-1 gate per
// docs/design/2026_05_29_implemented_composed1_cross_group_commit_guard.md
// §4.2(a) + §4.4.  Runs two checks before the txn's writes land:
//
//	(a) Observed-version owner — the txn's read-set was captured
//	    at routes[observedVer], so every write key must be owned
//	    by THIS Raft group at that historical version.  Matches
//	    the spec-level Commit precondition in tla/composed/Composed.tla.
//
//	(b) Current-version owner — even when (a) passes, a route
//	    shift between BeginTxn and Commit can leave the write
//	    landing on the OLD owner while readers at the new
//	    version route to the NEW owner and miss the write (the
//	    §3 codex P1 G1c trace).  The current-version fence
//	    refuses the commit when this group no longer owns the
//	    key, forcing a coordinator retry on the new owner.
//
// Short-circuits cleanly in three legacy / not-applicable cases:
//   - FSM was constructed without WithRouteHistory (legacy / test
//     seam): routes == nil, return nil.
//   - Request carries ObservedRouteVersion == 0 (unpinned —
//     pre-M1 caller, or ABORT request that doesn't carry the
//     version): return nil.
//   - Engine.Current returns (zero, false) — the engine has no
//     history (bare-struct test seam): return nil at the (b) check.
//
// Returns ErrComposed1VersionGCd when the observed version is
// outside the ring (M4 retry), and ErrComposed1Violation wrapped
// with per-key context otherwise.
func (f *kvFSM) verifyComposed1(r *pb.Request) error {
	// ABORT requests MUST always reach the abort handler so the
	// txn's intent locks get released.  If a route shifted between
	// PREPARE and ABORT (or the observed version was evicted), a
	// rejected ABORT would leave the locks pinned until LockResolver
	// noticed at TTL expiry — minutes of write-blocked keys for
	// what should be a one-RPC cleanup.  Production callers carry
	// ObservedRouteVersion=0 on ABORT (the existing observedVer==0
	// check below handles that case), so this guard is defensive
	// belt-and-suspenders: any future ABORT construction site that
	// inadvertently sets the version still bypasses the gate
	// (gemini HIGH on PR #895 — fail-open on ABORT).
	if r.GetPhase() == pb.Phase_ABORT {
		return nil
	}
	// Bypass the gate when EITHER the route history is unwired OR
	// the FSM's shardGroupID is the unset sentinel (0).  Both
	// fields are documented as "unset ⇒ short-circuit" but the
	// original check only honoured `routes == nil`, leaving a
	// partially-wired caller (routes installed before group ID)
	// to silently reject every pinned txn — no real Raft group
	// has ID 0, so OwnerOf would never match (coderabbit MAJOR on
	// PR #895).
	if f.routes == nil || f.shardGroupID == 0 {
		return nil
	}
	observedVer := r.GetObservedRouteVersion()
	if observedVer == 0 {
		return nil
	}

	// (a) Observed-version check.
	observedSnap, ok := f.routes.SnapshotAt(observedVer)
	if !ok {
		return errors.WithStack(ErrComposed1VersionGCd)
	}
	if err := f.verifyOwnerFromSnapshot(r.GetMutations(), observedSnap, observedVer, "observed"); err != nil {
		return err
	}

	// (b) Current-version cross-version-read fence.
	currentSnap, ok := f.routes.Current()
	if !ok {
		// No current snapshot — engine has no history, nothing
		// to compare against.  Fall through (matches the
		// short-circuit posture of an unwired FSM).
		return nil
	}
	return f.verifyOwnerFromSnapshot(r.GetMutations(), currentSnap, currentSnap.Version(), "current")
}

// verifyOwnerFromSnapshot is the shared per-mutation owner-check
// loop used by verifyComposed1's observed-version and current-
// version passes.  `phase` is the diagnostic label ("observed" /
// "current") that ends up in the wrapped error.  isTxnInternalKey
// mutations (the TxnMeta marker prefix) are skipped — they are
// always on every shard and have no Composed-1 ownership.
func (f *kvFSM) verifyOwnerFromSnapshot(mutations []*pb.Mutation, snap RouteSnapshot, snapVer uint64, phase string) error {
	for _, mut := range mutations {
		if mut == nil || len(mut.Key) == 0 {
			continue
		}
		if isTxnInternalKey(mut.Key) {
			continue
		}
		// routeKey-normalize before OwnerOf so the gate routes the
		// same way as ShardRouter.ResolveGroup — raw adapter keys
		// and route catalog ranges live in different lex bands
		// (issue #930).
		rKey := routeKey(mut.Key)
		owner, found := snap.OwnerOf(rKey)
		if !found || owner != f.shardGroupID {
			return errors.Wrapf(ErrComposed1Violation,
				"%s-version v=%d: key %q (routeKey %q) owned by group %d (found=%v); this FSM serves group %d",
				phase, snapVer, mut.Key, rKey, owner, found, f.shardGroupID)
		}
	}
	return nil
}

func (f *kvFSM) validateConflicts(ctx context.Context, muts []*pb.Mutation, startTS uint64) error {
	seen := make(map[string]struct{}, len(muts))
	for _, mut := range muts {
		keyStr := string(mut.Key)
		if _, ok := seen[keyStr]; ok {
			continue
		}
		seen[keyStr] = struct{}{}

		latest, exists, err := f.store.LatestCommitTS(ctx, mut.Key)
		if err != nil {
			return errors.WithStack(err)
		}
		if exists && latest > startTS {
			return errors.WithStack(store.NewWriteConflictError(mut.Key))
		}
	}
	return nil
}

func uniqueMutations(muts []*pb.Mutation) ([]*pb.Mutation, error) {
	if len(muts) == 0 {
		return []*pb.Mutation{}, nil
	}
	seen := make(map[string]struct{}, len(muts))
	reversed := make([]*pb.Mutation, 0, len(muts))
	// Keep the last mutation per key to avoid dropping final operations like
	// PUT followed by DEL in the same transactional batch.
	for i := len(muts) - 1; i >= 0; i-- {
		mut := muts[i]
		if mut == nil || len(mut.Key) == 0 {
			return nil, errors.WithStack(ErrInvalidRequest)
		}
		keyStr := string(mut.Key)
		if _, ok := seen[keyStr]; ok {
			continue
		}
		seen[keyStr] = struct{}{}
		reversed = append(reversed, mut)
	}

	out := make([]*pb.Mutation, 0, len(reversed))
	for i := len(reversed) - 1; i >= 0; i-- {
		out = append(out, reversed[i])
	}
	return out, nil
}

func (f *kvFSM) handlePrepareRequest(ctx context.Context, r *pb.Request) error {
	meta, muts, err := extractTxnMeta(r.Mutations)
	if err != nil {
		return err
	}
	if len(meta.PrimaryKey) == 0 {
		return errors.WithStack(ErrTxnPrimaryKeyRequired)
	}
	if len(muts) == 0 {
		return errors.WithStack(ErrInvalidRequest)
	}

	startTS := r.Ts
	uniq, err := uniqueMutations(muts)
	if err != nil {
		return err
	}
	if err := f.verifyRouteNotFencedForMutations(uniq); err != nil {
		return err
	}
	if err := f.verifyRouteWriteTimestampFloorForMutations(uniq, meta.CommitTS); err != nil {
		return err
	}
	if err := f.validateConflicts(ctx, uniq, startTS); err != nil {
		return errors.WithStack(err)
	}

	expireAt := txnLockExpireAt(meta.LockTTLms)

	storeMuts, err := f.buildPrepareStoreMutations(ctx, uniq, meta.PrimaryKey, startTS, expireAt)
	if err != nil {
		return err
	}

	if err := f.store.ApplyMutationsRaftAt(ctx, storeMuts, r.ReadKeys, startTS, startTS, f.pendingApplyIdx); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// handleOnePhaseTxnRequest applies a single-shard transaction atomically.
// Both write-write and read-write conflicts are checked under the store's
// applyMu lock via ApplyMutations. r.ReadKeys carries the transaction's read
// set (populated by the coordinator from OperationGroup.ReadKeys), so the
// FSM validates read-write conflicts atomically with the commit, eliminating
// the TOCTOU window that existed when validation was only done pre-Raft.
func (f *kvFSM) handleOnePhaseTxnRequest(ctx context.Context, r *pb.Request, commitTS uint64) error {
	meta, muts, err := extractTxnMeta(r.Mutations)
	if err != nil {
		return err
	}
	if len(meta.PrimaryKey) == 0 {
		return errors.WithStack(ErrTxnPrimaryKeyRequired)
	}
	if len(muts) == 0 {
		return errors.WithStack(ErrInvalidRequest)
	}
	startTS := r.Ts
	if commitTS <= startTS {
		return errors.WithStack(ErrTxnCommitTSRequired)
	}

	// Option-2 idempotency dedup: when this is a retry (meta.PrevCommitTS set),
	// the adapter reused the failed attempt's write set under a fresh commitTS.
	// If the previous attempt already landed — its entry survived the leader
	// churn that returned an ambiguous error — there is a committed version of
	// the primary key at exactly PrevCommitTS. Re-applying would create a
	// duplicate (the very :duplicate-elements anomaly), so no-op the whole
	// apply and let the adapter reconstruct the prior result.
	//
	// Determinism note (codex P1 round-11): the underlying CommittedVersionAt
	// intentionally does NOT enforce the retention watermark — branching FSM
	// apply on the per-replica minRetainedTS would let replicas with stale
	// retention surface ErrReadTSCompacted and skip dedup while replicas that
	// still retain the previous version no-op, producing split-brain. The
	// store's probe returns the raw pebble.Get answer; for the option-2 use
	// case (per-element keys, single MVCC version each) physical compaction
	// does not remove the version, so the probe is identical on every replica
	// applying this log entry. The retention-window > max-retry-latency
	// invariant prevents the rare case where a real never-landed retry
	// arrives with PrevCommitTS below pebble's compacted floor.
	dedup, err := f.dedupProbeOnePhase(ctx, meta)
	if err != nil {
		return err
	}
	if dedup {
		return nil
	}

	uniq, err := f.uniqueMutationsNotFencedAboveWriteFloor(muts, commitTS)
	if err != nil {
		return err
	}

	storeMuts, err := f.buildOnePhaseStoreMutations(ctx, uniq)
	if err != nil {
		return err
	}
	if err := f.store.ApplyMutationsRaftAt(ctx, storeMuts, r.ReadKeys, startTS, commitTS, f.pendingApplyIdx); err != nil {
		return errors.WithStack(err)
	}
	f.notifyApplyObservers(commitTS, uniq)
	return nil
}

func (f *kvFSM) uniqueMutationsNotFenced(muts []*pb.Mutation) ([]*pb.Mutation, error) {
	uniq, err := uniqueMutations(muts)
	if err != nil {
		return nil, err
	}
	if err := f.verifyRouteNotFencedForMutations(uniq); err != nil {
		return nil, err
	}
	return uniq, nil
}

func (f *kvFSM) uniqueMutationsNotFencedAboveWriteFloor(muts []*pb.Mutation, commitTS uint64) ([]*pb.Mutation, error) {
	uniq, err := f.uniqueMutationsNotFenced(muts)
	if err != nil {
		return nil, err
	}
	if err := f.verifyRouteWriteTimestampFloorForMutations(uniq, commitTS); err != nil {
		return nil, err
	}
	return uniq, nil
}

func (f *kvFSM) uniqueMutationsAboveWriteFloor(muts []*pb.Mutation, commitTS uint64) ([]*pb.Mutation, error) {
	uniq, err := uniqueMutations(muts)
	if err != nil {
		return nil, err
	}
	if err := f.verifyRouteWriteTimestampFloorForMutations(uniq, commitTS); err != nil {
		return nil, err
	}
	return uniq, nil
}

// dedupProbeOnePhase decides whether handleOnePhaseTxnRequest should no-op
// because the entry is a retry whose prior attempt already landed. Extracted
// to keep handleOnePhaseTxnRequest under the cyclop budget; the determinism
// rationale lives at the call site.
//
// Returns (true, nil) → the entry must no-op (prior attempt landed).
// Returns (false, nil) → fall through to normal apply.
// Returns (false, err) → propagate err; apply must not proceed.
func (f *kvFSM) dedupProbeOnePhase(ctx context.Context, meta TxnMeta) (bool, error) {
	if meta.PrevCommitTS == 0 {
		return false, nil
	}
	landed, err := f.store.CommittedVersionAt(ctx, meta.PrimaryKey, meta.PrevCommitTS)
	if err != nil {
		return false, errors.WithStack(err)
	}
	return landed, nil
}

func (f *kvFSM) handleCommitRequest(ctx context.Context, r *pb.Request) error {
	meta, muts, err := extractTxnMeta(r.Mutations)
	if err != nil {
		return err
	}
	if len(muts) == 0 {
		return errors.WithStack(ErrInvalidRequest)
	}
	commitTS := meta.CommitTS
	startTS := r.Ts
	if commitTS <= startTS {
		return errors.WithStack(ErrTxnCommitTSRequired)
	}
	if len(meta.PrimaryKey) == 0 {
		return errors.WithStack(ErrTxnPrimaryKeyRequired)
	}
	applyStartTS, err := f.commitApplyStartTS(ctx, meta.PrimaryKey, startTS, commitTS)
	if err != nil {
		return err
	}
	uniq, err := f.uniqueMutationsAboveWriteFloor(muts, commitTS)
	if err != nil {
		return err
	}
	storeMuts, err := f.buildCommitStoreMutations(ctx, uniq, meta, startTS, commitTS)
	if err != nil {
		return err
	}
	if len(storeMuts) == 0 {
		return nil
	}
	if err := f.applyCommitWithIdempotencyFallback(ctx, storeMuts, uniq, applyStartTS, commitTS); err != nil {
		return err
	}
	f.notifyApplyObserversForStoreMutations(commitTS, storeMuts)
	return nil
}

// commitApplyStartTS resolves the startTS to use for MVCC conflict detection
// during a COMMIT. If a commit record already exists for the primary key it
// returns commitTS (making the apply idempotent); otherwise it returns startTS.
//
// It is also the symmetric guard to appendRollbackRecord: if a rollback marker
// already exists for (primaryKey, startTS) the commit is rejected with
// ErrTxnAlreadyAborted. Together with the commit-record check in
// appendRollbackRecord, this enforces the invariant that at most one of
// {rollback marker, commit record} is present for any (primaryKey, startTS).
func (f *kvFSM) commitApplyStartTS(ctx context.Context, primaryKey []byte, startTS, commitTS uint64) (uint64, error) {
	recordedCommitTS, committed, err := f.txnCommitTS(ctx, primaryKey, startTS)
	if err != nil {
		return 0, err
	}
	if !committed {
		// No commit record yet: reject if a rollback marker is present.
		// This catches out-of-order apply (COMMIT after ABORT), buggy
		// clients, and replay races.
		exists, rerr := f.store.ExistsAt(ctx, txnRollbackKey(primaryKey, startTS), ^uint64(0))
		if rerr != nil {
			return 0, errors.WithStack(rerr)
		}
		if exists {
			return 0, errors.WithStack(ErrTxnAlreadyAborted)
		}
		return startTS, nil
	}
	if recordedCommitTS != commitTS {
		return 0, errors.Wrapf(
			ErrTxnInvalidMeta,
			"commit_ts mismatch for primary key %s: recordedCommitTS=%d requestedCommitTS=%d startTS=%d",
			string(primaryKey), recordedCommitTS, commitTS, startTS,
		)
	}
	// Commit record exists — use commitTS so stale artifacts can be cleaned up
	// without triggering a write-conflict.
	return commitTS, nil
}

// applyCommitWithIdempotencyFallback applies storeMuts at (applyStartTS,
// commitTS). If the apply fails with a write-conflict and any of the target
// keys already has a committed version at or beyond commitTS, the conflict is
// treated as an idempotent secondary-shard retry and the apply is retried with
// commitTS as the conflict-check baseline.
//
// The secondary-shard LatestCommitTS scan is intentionally deferred to the
// write-conflict path so the hot (first-time) commit path pays no extra cost.
func (f *kvFSM) applyCommitWithIdempotencyFallback(ctx context.Context, storeMuts []*store.KVPairMutation, uniq []*pb.Mutation, applyStartTS, commitTS uint64) error {
	err := f.store.ApplyMutationsRaftAt(ctx, storeMuts, nil, applyStartTS, commitTS, f.pendingApplyIdx)
	if err == nil {
		return nil
	}
	if !errors.Is(err, store.ErrWriteConflict) {
		return errors.WithStack(err)
	}
	// Write-conflict: scan mutations one by one and return as soon as we find
	// a key that is already committed at or beyond commitTS — this indicates an
	// idempotent secondary-shard retry (txnCommitKey lives on the primary
	// shard, not here).  Retry with commitTS as the conflict-check baseline.
	for _, mut := range uniq {
		latestTS, exists, lErr := f.store.LatestCommitTS(ctx, mut.Key)
		if lErr != nil {
			return errors.WithStack(lErr)
		}
		if exists && latestTS >= commitTS {
			return errors.WithStack(f.store.ApplyMutationsRaftAt(ctx, storeMuts, nil, commitTS, commitTS, f.pendingApplyIdx))
		}
	}
	return errors.WithStack(err)
}

func (f *kvFSM) handleAbortRequest(ctx context.Context, r *pb.Request, abortTS uint64) error {
	meta, muts, err := extractTxnMeta(r.Mutations)
	if err != nil {
		return err
	}
	if len(meta.PrimaryKey) == 0 {
		return errors.WithStack(ErrTxnPrimaryKeyRequired)
	}
	if len(muts) == 0 {
		return errors.WithStack(ErrInvalidRequest)
	}
	startTS := r.Ts
	if abortTS <= startTS {
		return errors.WithStack(ErrTxnCommitTSRequired)
	}

	// NOTE: do NOT short-circuit the whole request on rollback-marker
	// presence. The marker only proves that SOME prior abort for this
	// (primaryKey, startTS) ran; it does not prove cleanup ran for the
	// specific keys in *this* request. In particular
	// ShardStore.tryAbortExpiredPrimary issues an ABORT whose mutation
	// list contains only the primary key, so a later lock-resolver
	// abort for a secondary key (same primaryKey, same startTS) would
	// see the marker already present and must still clean up that
	// secondary's lock/intent. Idempotency is enforced per-key in
	// shouldClearAbortKey (lock-missing ⇒ nothing to do) and for the
	// rollback-marker Put in appendRollbackRecord.

	uniq, err := uniqueMutations(muts)
	if err != nil {
		return err
	}
	storeMuts, abortingPrimary, err := f.buildAbortCleanupStoreMutations(ctx, uniq, meta.PrimaryKey, startTS)
	if err != nil {
		return err
	}
	if abortingPrimary {
		if err := f.appendRollbackRecord(ctx, meta.PrimaryKey, startTS, &storeMuts); err != nil {
			return err
		}
	}

	if len(storeMuts) == 0 {
		return nil
	}
	return errors.WithStack(f.store.ApplyMutationsRaftAt(ctx, storeMuts, nil, startTS, abortTS, f.pendingApplyIdx))
}

func (f *kvFSM) buildPrepareStoreMutations(ctx context.Context, muts []*pb.Mutation, primaryKey []byte, startTS, expireAt uint64) ([]*store.KVPairMutation, error) {
	storeMuts := make([]*store.KVPairMutation, 0, len(muts)*txnPrepareStoreMutationFactor)
	for _, mut := range muts {
		preparedMuts, err := f.prepareTxnMutation(ctx, mut, primaryKey, startTS, expireAt)
		if err != nil {
			return nil, err
		}
		storeMuts = append(storeMuts, preparedMuts...)
	}
	return storeMuts, nil
}

func (f *kvFSM) buildOnePhaseStoreMutations(ctx context.Context, muts []*pb.Mutation) ([]*store.KVPairMutation, error) {
	for _, mut := range muts {
		if isTxnInternalKey(mut.Key) {
			return nil, errors.WithStack(ErrInvalidRequest)
		}
		if err := f.assertNoConflictingTxnLock(ctx, mut.Key, nil, 0); err != nil {
			return nil, err
		}
	}
	storeMuts, err := toStoreMutations(muts)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return storeMuts, nil
}

func (f *kvFSM) buildCommitStoreMutations(ctx context.Context, muts []*pb.Mutation, meta TxnMeta, startTS, commitTS uint64) ([]*store.KVPairMutation, error) {
	storeMuts := make([]*store.KVPairMutation, 0, len(muts)*txnCommitStoreMutationFactor+txnCommitStoreMutationSlack)

	committingPrimary := false
	for _, mut := range muts {
		key := mut.Key
		if bytes.Equal(key, meta.PrimaryKey) {
			committingPrimary = true
		}

		keyMuts, err := f.commitTxnKeyMutations(ctx, key, meta.PrimaryKey, startTS)
		if err != nil {
			return nil, err
		}
		storeMuts = append(storeMuts, keyMuts...)
	}

	if committingPrimary {
		storeMuts = append(storeMuts, &store.KVPairMutation{
			Op:    store.OpTypePut,
			Key:   txnCommitKey(meta.PrimaryKey, startTS),
			Value: encodeTxnCommitRecord(commitTS),
		})
	}

	return storeMuts, nil
}

func (f *kvFSM) buildAbortCleanupStoreMutations(ctx context.Context, muts []*pb.Mutation, primaryKey []byte, startTS uint64) ([]*store.KVPairMutation, bool, error) {
	storeMuts := make([]*store.KVPairMutation, 0, len(muts)*txnAbortStoreMutationFactor)
	abortingPrimary := false
	for _, mut := range muts {
		key := mut.Key
		if bytes.Equal(key, primaryKey) {
			abortingPrimary = true
		}

		shouldClear, err := f.shouldClearAbortKey(ctx, key, primaryKey, startTS)
		if err != nil {
			return nil, false, err
		}
		if shouldClear {
			storeMuts = append(storeMuts, txnCleanupMutations(key)...)
		}
	}
	return storeMuts, abortingPrimary, nil
}

func (f *kvFSM) appendRollbackRecord(ctx context.Context, primaryKey []byte, startTS uint64, storeMuts *[]*store.KVPairMutation) error {
	// Desired invariant: for any (primaryKey, startTS) pair, at most
	// one of {rollback marker, commit record} is present. The invariant
	// holds when aborts/commits flow through the symmetric guards in
	// this function and handleCommitRequest, but we cannot *assume* it
	// on entry (a buggy client, replay, or race may violate it), so we
	// verify it in-line below on both the first-time and idempotent
	// paths.
	//
	// Idempotent rollback: if the marker already exists for this
	// (primaryKey, startTS), skip the Put. Rollback markers are
	// deterministic ({txnRollbackVersion}) and are written as normal
	// values via Put; a second Put with applyStartTS=startTS would be
	// rejected by the MVCC store as a write conflict because the key's
	// latestCommitTS is already greater than startTS.
	markerPresent, err := f.store.ExistsAt(ctx, txnRollbackKey(primaryKey, startTS), ^uint64(0))
	if err != nil {
		return errors.WithStack(err)
	}

	// Verify the invariant regardless of marker presence: if a commit
	// record is present for this (primaryKey, startTS), refuse to
	// write (or confirm) a rollback marker. This catches out-of-order
	// apply where a COMMIT somehow landed after a prior ABORT, as
	// well as the normal "commit wins over rollback" race.
	commitExists, err := f.store.ExistsAt(ctx, txnCommitKey(primaryKey, startTS), ^uint64(0))
	if err != nil {
		return errors.WithStack(err)
	}
	if commitExists {
		return errors.WithStack(ErrTxnAlreadyCommitted)
	}

	if markerPresent {
		return nil
	}

	*storeMuts = append(*storeMuts, &store.KVPairMutation{
		Op:    store.OpTypePut,
		Key:   txnRollbackKey(primaryKey, startTS),
		Value: encodeTxnRollbackRecord(),
	})
	return nil
}

func (f *kvFSM) txnCommitTS(ctx context.Context, primaryKey []byte, startTS uint64) (uint64, bool, error) {
	b, err := f.store.GetAt(ctx, txnCommitKey(primaryKey, startTS), ^uint64(0))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return 0, false, nil
		}
		return 0, false, errors.WithStack(err)
	}
	commitTS, derr := decodeTxnCommitRecord(b)
	if derr != nil {
		return 0, false, errors.WithStack(derr)
	}
	return commitTS, true, nil
}

func (f *kvFSM) prepareTxnMutation(ctx context.Context, mut *pb.Mutation, primaryKey []byte, startTS, expireAt uint64) ([]*store.KVPairMutation, error) {
	if err := f.assertNoConflictingTxnLock(ctx, mut.Key, primaryKey, startTS); err != nil {
		return nil, err
	}

	lockVal := encodeTxnLock(txnLock{
		StartTS:      startTS,
		TTLExpireAt:  expireAt,
		PrimaryKey:   primaryKey,
		IsPrimaryKey: bytes.Equal(mut.Key, primaryKey),
	})
	intent, err := txnIntentFromPBMutation(mut, startTS)
	if err != nil {
		return nil, err
	}

	storeMuts := make([]*store.KVPairMutation, 0, txnPrepareStoreMutationFactor)
	storeMuts = append(storeMuts,
		&store.KVPairMutation{Op: store.OpTypePut, Key: txnLockKey(mut.Key), Value: lockVal},
		&store.KVPairMutation{Op: store.OpTypePut, Key: txnIntentKey(mut.Key), Value: encodeTxnIntent(intent)},
	)
	return storeMuts, nil
}

func txnIntentFromPBMutation(mut *pb.Mutation, startTS uint64) (txnIntent, error) {
	switch mut.Op {
	case pb.Op_PUT:
		return txnIntent{StartTS: startTS, Op: txnIntentOpPut, Value: mut.Value}, nil
	case pb.Op_DEL:
		return txnIntent{StartTS: startTS, Op: txnIntentOpDel, Value: nil}, nil
	case pb.Op_DEL_PREFIX:
		return txnIntent{}, errors.WithStack(ErrUnknownRequestType)
	default:
		return txnIntent{}, errors.WithStack(ErrUnknownRequestType)
	}
}

func txnCleanupMutations(key []byte) []*store.KVPairMutation {
	return []*store.KVPairMutation{
		{Op: store.OpTypeDelete, Key: txnLockKey(key)},
		{Op: store.OpTypeDelete, Key: txnIntentKey(key)},
	}
}

func (f *kvFSM) txnLockForCommit(ctx context.Context, key []byte) (txnLock, bool, error) {
	lockBytes, err := f.store.GetAt(ctx, txnLockKey(key), ^uint64(0))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return txnLock{}, false, nil
		}
		return txnLock{}, false, errors.WithStack(err)
	}
	lock, derr := decodeTxnLock(lockBytes)
	if derr != nil {
		return txnLock{}, false, errors.WithStack(derr)
	}
	return lock, true, nil
}

func (f *kvFSM) txnIntentForCommit(ctx context.Context, key []byte) (txnIntent, bool, error) {
	intentBytes, err := f.store.GetAt(ctx, txnIntentKey(key), ^uint64(0))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return txnIntent{}, false, nil
		}
		return txnIntent{}, false, errors.WithStack(err)
	}
	intent, derr := decodeTxnIntent(intentBytes)
	if derr != nil {
		return txnIntent{}, false, errors.WithStack(derr)
	}
	return intent, true, nil
}

func storeMutationForIntent(key []byte, intent txnIntent) (*store.KVPairMutation, error) {
	switch intent.Op {
	case txnIntentOpPut:
		return &store.KVPairMutation{Op: store.OpTypePut, Key: key, Value: intent.Value}, nil
	case txnIntentOpDel:
		return &store.KVPairMutation{Op: store.OpTypeDelete, Key: key}, nil
	default:
		return nil, errors.WithStack(ErrUnknownRequestType)
	}
}

func (f *kvFSM) commitTxnKeyMutations(ctx context.Context, key, primaryKey []byte, startTS uint64) ([]*store.KVPairMutation, error) {
	lock, ok, err := f.txnLockForCommit(ctx, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		// Already resolved (committed/rolled back).
		return nil, nil
	}
	if lock.StartTS != startTS {
		return nil, NewTxnLockedError(key)
	}
	if !bytes.Equal(lock.PrimaryKey, primaryKey) {
		return nil, errors.Wrapf(ErrTxnInvalidMeta, "lock primary_key mismatch for key %s", string(key))
	}

	intent, ok, err := f.txnIntentForCommit(ctx, key)
	if err != nil {
		return nil, err
	}

	out := make([]*store.KVPairMutation, 0, txnCommitStoreMutationFactor)
	if ok {
		if intent.StartTS != startTS {
			return nil, errors.Wrapf(ErrTxnInvalidMeta, "intent start_ts mismatch for key %s", string(key))
		}
		mut, err := storeMutationForIntent(key, intent)
		if err != nil {
			return nil, err
		}
		out = append(out, mut)
	}
	out = append(out, txnCleanupMutations(key)...)
	return out, nil
}

// shouldClearAbortKey reports whether this abort request must emit
// cleanup (lock+intent Delete) mutations for key. It returns false
// when the lock is already missing: lock/intent are always written
// and deleted together in a single ApplyMutations batch
// (lock missing ⇔ intent missing), so missing lock means either
// cleanup already ran for this (startTS, primaryKey) or the key was
// never prepared. Emitting Deletes on already-tombstoned keys would
// trigger MVCC write conflicts and has no observable effect.
func (f *kvFSM) shouldClearAbortKey(ctx context.Context, key, primaryKey []byte, startTS uint64) (bool, error) {
	lockBytes, err := f.store.GetAt(ctx, txnLockKey(key), ^uint64(0))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return false, nil
		}
		return false, errors.WithStack(err)
	}
	lock, derr := decodeTxnLock(lockBytes)
	if derr != nil {
		return false, errors.WithStack(derr)
	}
	if lock.StartTS != startTS {
		return false, nil
	}
	if !bytes.Equal(lock.PrimaryKey, primaryKey) {
		return false, errors.Wrapf(ErrTxnInvalidMeta, "abort primary_key mismatch for key %s", string(key))
	}
	return true, nil
}

func (f *kvFSM) assertNoConflictingTxnLock(ctx context.Context, key, primaryKey []byte, startTS uint64) error {
	lockBytes, err := f.store.GetAt(ctx, txnLockKey(key), ^uint64(0))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil
		}
		return errors.WithStack(err)
	}
	lock, err := decodeTxnLock(lockBytes)
	if err != nil {
		return errors.WithStack(err)
	}
	if startTS != 0 && lock.StartTS == startTS && bytes.Equal(lock.PrimaryKey, primaryKey) {
		return nil
	}
	return NewTxnLockedError(key)
}

func toStoreMutations(muts []*pb.Mutation) ([]*store.KVPairMutation, error) {
	out := make([]*store.KVPairMutation, 0, len(muts))
	for _, mut := range muts {
		switch mut.Op {
		case pb.Op_PUT:
			out = append(out, &store.KVPairMutation{
				Op:    store.OpTypePut,
				Key:   mut.Key,
				Value: mut.Value,
			})
		case pb.Op_DEL:
			out = append(out, &store.KVPairMutation{
				Op:  store.OpTypeDelete,
				Key: mut.Key,
			})
		case pb.Op_DEL_PREFIX:
			return nil, ErrUnknownRequestType
		default:
			return nil, ErrUnknownRequestType
		}
	}
	return out, nil
}
