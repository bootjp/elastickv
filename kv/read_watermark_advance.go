package kv

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/cockroachdb/errors"
)

// raftEncodeWatermarkAdvance carries a read-watermark advance: an entry with
// no mutations that raises the group's applied commit watermark
// (LastCommitTS) to a snapshot timestamp a reader is about to read at.
//
// Adapters take one snapshot timestamp per operation from the maximum
// watermark over all groups (ShardStore.LastCommitTS), while the one-phase
// commit-ts fence (checkCommitTSFence) compares a commit timestamp with the
// applying group's own watermark. Without the advance, a write allocated
// below the snapshot and applied late on a lagging group passes that
// group's fence and lands below a snapshot that was already read: G0 across
// groups (docs/design/2026_09_26_partial_serializable_isolation_audit.md,
// "Multi-group deployments"). Once the advance applies, the fence rejects
// every later one-phase entry at or below the snapshot on that group, so a
// read at the snapshot stays stable.
//
// 0x17 (field 2, wire type 7) is an invalid protobuf prefix, so it cannot be
// mistaken for a legacy bare Request or RaftCommand.
const raftEncodeWatermarkAdvance byte = 0x17

const watermarkAdvancePayloadLen = 8

const watermarkAdvanceApplyPollInterval = 10 * time.Millisecond

var ErrInvalidWatermarkAdvance = errors.New("invalid read watermark advance")

var ErrWatermarkAdvanceReceiptUnavailable = errors.New("read watermark advance apply receipt is unavailable")

// AppliedWatermarkReceipt proves that the local FSM has applied the Raft entry
// that carried an explicit watermark advance. AppliedTimestamp is the local
// durable watermark after that entry; it can exceed RequestedTimestamp when a
// later committed entry applied before the caller observed the receipt.
type AppliedWatermarkReceipt struct {
	GroupID            uint64
	RequestedTimestamp uint64
	AppliedTimestamp   uint64
	CommitIndex        uint64
}

// EncodeWatermarkAdvance returns the Raft payload that raises a group's
// applied commit watermark to ts.
func EncodeWatermarkAdvance(ts uint64) []byte {
	out := make([]byte, 1+watermarkAdvancePayloadLen)
	out[0] = raftEncodeWatermarkAdvance
	binary.BigEndian.PutUint64(out[1:], ts)
	return out
}

// DecodeWatermarkAdvance reports the timestamp a watermark-advance payload
// carries, and whether payload is one. 0 and ^uint64(0) are refused: 0 is
// no snapshot and ^uint64(0) is the empty-store "read latest" sentinel.
func DecodeWatermarkAdvance(payload []byte) (uint64, bool, error) {
	if len(payload) == 0 || payload[0] != raftEncodeWatermarkAdvance {
		return 0, false, nil
	}
	ts, err := decodeWatermarkAdvanceBody(payload[1:])
	return ts, true, err
}

func decodeWatermarkAdvanceBody(body []byte) (uint64, error) {
	if len(body) != watermarkAdvancePayloadLen {
		return 0, errors.Wrapf(ErrInvalidWatermarkAdvance, "expected %d bytes, got %d", watermarkAdvancePayloadLen, len(body))
	}
	ts := binary.BigEndian.Uint64(body)
	if ts == 0 || ts == ^uint64(0) {
		return 0, errors.Wrapf(ErrInvalidWatermarkAdvance, "timestamp %d", ts)
	}
	return ts, nil
}

// applyWatermarkAdvance raises the store's applied commit watermark to the
// carried timestamp. It writes no version; the store persists the raised
// watermark and the applied index in one batch, so the entry is idempotent
// and a replay is a no-op. Like the backup timestamp floor, the HLC observes
// the value so later writes on this replica issue above it instead of being
// fenced. The physical ceiling is raised to the value's physical half first:
// fenced allocations floor at the ceiling and refuse anything above it, so
// an observed value above the ceiling (a snapshot taken from a group whose
// clock runs ahead) would otherwise refuse every persistence timestamp on
// this replica until a lease renewal overtook it. The raise is to the
// physical half exactly, so a group sharing the sender's millisecond does
// not staircase the ceilings under alternating cross-group traffic.
//
// A failure to persist halts the apply loop: a replica that skipped a
// committed advance would accept one-phase writes its peers fence. The
// entry is classified volatile (IsVolatileOnlyPayload) so the engine replays
// it after a restart; the watermark write is then a no-op and the replay
// restores the ceiling the restored HLC would otherwise lack.
func (f *kvFSM) applyWatermarkAdvance(ctx context.Context, body []byte) any {
	ts, err := decodeWatermarkAdvanceBody(body)
	if err != nil {
		return err
	}
	if ts > f.store.LastCommitTS() {
		if err := f.store.ApplyMutationsRaftAt(ctx, nil, nil, ts, ts, f.pendingApplyIdx); err != nil {
			return haltErr(errors.Wrap(errors.Mark(err, ErrRaftEntryApply), "kv/fsm: apply read watermark advance"))
		}
	}
	if f.hlc != nil {
		f.hlc.SetPhysicalCeiling(clampUint64ToInt64(ts >> hlcLogicalBits))
		f.hlc.Observe(ts)
	}
	return nil
}

// ensureReadWatermark makes group g's applied commit watermark cover a read
// at ts before the read runs. It is a no-op when the group already covers
// ts, and when ts is not a snapshot timestamp this node can vouch for: the
// empty-store sentinel, or a value above every watermark this node has
// applied (a client-chosen read timestamp, which is served as before rather
// than allowed to fence writes). Otherwise it proposes a watermark advance
// to the group, through the leader when this node is not leader, and waits
// for it to apply on the leader; a read on the leader after that sees every
// entry at or below ts, and every later one-phase entry at or below ts is
// fenced.
func (s *ShardStore) ensureReadWatermark(ctx context.Context, g *ShardGroup, ts uint64) error {
	if !s.readWatermarkAdvanceNeeded(g, ts) {
		return nil
	}
	proposer := NewLeaderAdminProposer(g.Engine, g.Proposer(), &s.connCache, WithLeaderAdminToken(g.forwardedLeaderReadToken()))
	result, err := proposer.ProposeAdmin(ctx, EncodeWatermarkAdvance(ts))
	if err != nil {
		return errors.Wrap(err, "advance read watermark")
	}
	if result != nil {
		if applyErr, ok := result.Response.(error); ok && applyErr != nil {
			return errors.Wrap(applyErr, "advance read watermark apply")
		}
	}
	return nil
}

// AdvanceReadWatermarkForGroup always proposes an explicit watermark advance
// to groupID and waits until the proposal's commit index is applied locally.
// It is intended for startup/control protocols that must establish an exact
// replicated read boundary; ordinary reads should use ensureReadWatermark.
func (s *ShardStore) AdvanceReadWatermarkForGroup(ctx context.Context, groupID, ts uint64) (AppliedWatermarkReceipt, error) {
	receipt := AppliedWatermarkReceipt{GroupID: groupID, RequestedTimestamp: ts}
	g, err := s.explicitWatermarkAdvanceTarget(groupID, ts)
	if err != nil {
		return receipt, err
	}
	commitIndex, err := s.proposeExplicitWatermarkAdvance(ctx, g, ts)
	if err != nil {
		return receipt, err
	}
	receipt.CommitIndex = commitIndex
	if err := waitForLocalAppliedIndex(ctx, g, commitIndex); err != nil {
		return receipt, err
	}
	receipt.AppliedTimestamp = g.Store.LastCommitTS()
	if receipt.AppliedTimestamp < ts {
		return receipt, errors.Wrapf(ErrWatermarkAdvanceReceiptUnavailable,
			"group %d applied index %d but watermark %d is below requested %d",
			groupID, commitIndex, receipt.AppliedTimestamp, ts)
	}
	return receipt, nil
}

func (s *ShardStore) explicitWatermarkAdvanceTarget(groupID, ts uint64) (*ShardGroup, error) {
	if s == nil || ts == 0 || ts == ^uint64(0) || !ReadWatermarkAdvanceAdmissible(time.Now().UnixMilli(), ts) {
		return nil, errors.WithStack(ErrInvalidWatermarkAdvance)
	}
	g, ok := s.groups[groupID]
	if !ok || g == nil || g.Store == nil || engineForGroup(g) == nil {
		return nil, errors.Wrapf(ErrWatermarkAdvanceReceiptUnavailable, "raft group %d", groupID)
	}
	return g, nil
}

func (s *ShardStore) proposeExplicitWatermarkAdvance(ctx context.Context, g *ShardGroup, ts uint64) (uint64, error) {
	proposer := NewLeaderAdminProposer(g.Engine, g.Proposer(), &s.connCache, WithLeaderAdminToken(g.forwardedLeaderReadToken()))
	result, err := proposer.ProposeAdmin(ctx, EncodeWatermarkAdvance(ts))
	if err != nil {
		return 0, errors.Wrap(err, "advance read watermark")
	}
	if result == nil || result.CommitIndex == 0 {
		return 0, errors.WithStack(ErrWatermarkAdvanceReceiptUnavailable)
	}
	if applyErr, ok := result.Response.(error); ok && applyErr != nil {
		return 0, errors.Wrap(applyErr, "advance read watermark apply")
	}
	return result.CommitIndex, nil
}

type appliedIndexView interface {
	AppliedIndex() uint64
}

type durableAppliedIndexView interface {
	LastAppliedIndex() (uint64, bool, error)
}

func waitForLocalAppliedIndex(ctx context.Context, g *ShardGroup, want uint64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	ticker := time.NewTicker(watermarkAdvanceApplyPollInterval)
	defer ticker.Stop()
	for {
		applied, supported, err := localAppliedIndex(g)
		if err != nil {
			return errors.Wrap(err, "read local applied index")
		}
		if !supported {
			return errors.WithStack(ErrWatermarkAdvanceReceiptUnavailable)
		}
		if applied >= want {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "wait for local watermark advance apply")
		case <-ticker.C:
		}
	}
}

func localAppliedIndex(g *ShardGroup) (uint64, bool, error) {
	if g == nil {
		return 0, false, nil
	}
	if view, ok := g.Engine.(appliedIndexView); ok {
		return view.AppliedIndex(), true, nil
	}
	if view, ok := g.Store.(durableAppliedIndexView); ok {
		index, present, err := view.LastAppliedIndex()
		return index, present, errors.WithStack(err)
	}
	return 0, false, nil
}

// readWatermarkAdvanceNeeded reports whether a read at ts on g needs an
// advance: g is Raft-backed, ts is a snapshot this node has applied
// somewhere, and g's own watermark is still below it.
func (s *ShardStore) readWatermarkAdvanceNeeded(g *ShardGroup, ts uint64) bool {
	if !s.readWatermarkAdvance || g == nil || g.Store == nil || engineForGroup(g) == nil {
		return false
	}
	if ts == 0 || ts == ^uint64(0) || ts <= g.Store.LastCommitTS() {
		return false
	}
	return ts <= s.LastCommitTS()
}

// readWatermarkAdvanceHorizonMs bounds how far ahead of a leader's wall
// clock a forwarded advance may reach. A legitimate snapshot was issued by
// some group's leader at most one lease window (hlcPhysicalWindowMs) ahead
// of that leader's wall clock; the second window absorbs clock skew between
// the two leaders.
const readWatermarkAdvanceHorizonMs = 2 * hlcPhysicalWindowMs

// ReadWatermarkAdvanceAdmissible bounds the timestamp a leader accepts for a
// forwarded watermark advance: its physical half may not exceed the leader's
// wall clock plus readWatermarkAdvanceHorizonMs. The bound is taken from the
// wall clock alone, never from the HLC or the ceiling, because an accepted
// advance raises both: a bound derived from them would let repeated
// requests staircase the watermark arbitrarily far into the future.
func ReadWatermarkAdvanceAdmissible(nowMs int64, ts uint64) bool {
	return clampUint64ToInt64(ts>>hlcLogicalBits) <= nowMs+readWatermarkAdvanceHorizonMs
}
