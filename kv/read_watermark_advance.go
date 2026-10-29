package kv

import (
	"context"
	"encoding/binary"

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
// groups (docs/design/2026_09_26_proposed_serializable_isolation_audit.md,
// "Multi-group deployments"). Once the advance applies, the fence rejects
// every later one-phase entry at or below the snapshot on that group, so a
// read at the snapshot stays stable.
//
// 0x17 (field 2, wire type 7) is an invalid protobuf prefix, so it cannot be
// mistaken for a legacy bare Request or RaftCommand.
const raftEncodeWatermarkAdvance byte = 0x17

const watermarkAdvancePayloadLen = 8

var ErrInvalidWatermarkAdvance = errors.New("invalid read watermark advance")

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
func (f *kvFSM) applyWatermarkAdvance(ctx context.Context, body []byte) any {
	ts, err := decodeWatermarkAdvanceBody(body)
	if err != nil {
		return err
	}
	if ts > f.store.LastCommitTS() {
		if err := f.store.ApplyMutationsRaftAt(ctx, nil, nil, ts, ts, f.pendingApplyIdx); err != nil {
			return errors.Wrap(err, "apply read watermark advance")
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

// readWatermarkAdvanceNeeded reports whether a read at ts on g needs an
// advance: g is Raft-backed, ts is a snapshot this node has applied
// somewhere, and g's own watermark is still below it.
func (s *ShardStore) readWatermarkAdvanceNeeded(g *ShardGroup, ts uint64) bool {
	if g == nil || g.Store == nil || engineForGroup(g) == nil {
		return false
	}
	if ts == 0 || ts == ^uint64(0) || ts <= g.Store.LastCommitTS() {
		return false
	}
	return ts <= s.LastCommitTS()
}

// ReadWatermarkAdvanceAdmissible bounds the timestamp a leader accepts for a
// forwarded watermark advance: its physical half may not exceed
// max(physical(clock.Current()), ceiling) + hlcPhysicalWindowMs. A snapshot
// taken from a group whose clock runs ahead stays inside that bound; a
// far-future value, which would fence every write on the group until the
// wall clock caught up, is refused.
func ReadWatermarkAdvanceAdmissible(clock *HLC, ts uint64) bool {
	if clock == nil {
		return false
	}
	physical := clampUint64ToInt64(ts >> hlcLogicalBits)
	base := max(clampUint64ToInt64(clock.Current()>>hlcLogicalBits), clock.PhysicalCeiling())
	return physical <= base+hlcPhysicalWindowMs
}
