package encryption

import (
	pkgerrors "github.com/cockroachdb/errors"
)

// CheckNodeIDCollision is the §9.1 / 6C-3 startup-guard primitive
// for `ErrNodeIDCollision`. It walks the supplied list of
// `full_node_id` values (typically every voter + learner in the
// default group's local route-catalog snapshot), narrows each to
// its 16-bit `node_id` via the same `uint16(full_node_id & 0xFFFF)`
// mask that the writer-registry keying and §4.1 GCM nonce prefix
// use, and returns `ErrNodeIDCollision` if any two DISTINCT
// `full_node_id` values share the same `node_id`.
//
// Skip conditions handled by the caller (the startup-guard wiring
// in main.go), not by this primitive:
//
//   - Encryption disabled (no nonce-reuse risk).
//   - Membership snapshot empty (single-node pre-bootstrap;
//     nothing to compare yet).
//
// This primitive does NOT consult any sidecar, registry, or RPC
// transport. It runs in the startup-before-serving phase where
// none of those are available yet (the gRPC server is not up).
//
// Determinism: the caller may pass full_node_ids in any order;
// detection is symmetric. On a hit, the returned error wraps
// the two colliding `full_node_id` values and the shared
// `node_id` so the operator triage line names the conflict
// concretely.
//
// Returns nil when:
//
//   - The slice has fewer than two unique values (no possible
//     collision).
//   - No two distinct values map to the same 16-bit `node_id`.
//
// Returns `ErrNodeIDCollision` wrapped with offending IDs when
// any two distinct `full_node_id` values share a `node_id`.
// minMembersForCollision is the smallest membership-set size at
// which two distinct full_node_id values can exist (and therefore
// at which a 16-bit narrowing collision becomes possible). With
// 0 or 1 member there is nothing to compare; skip early.
const minMembersForCollision = 2

func CheckNodeIDCollision(fullNodeIDs []uint64) error {
	if len(fullNodeIDs) < minMembersForCollision {
		return nil
	}
	// node_id -> first full_node_id that mapped to it. On second
	// hit at the same node_id with a DIFFERENT full_node_id, we
	// have a real collision; same full_node_id appearing twice is
	// not a collision (duplicate-membership-entry quirk; the
	// route catalog watcher should already dedupe, but defending
	// here keeps the primitive correct under any input ordering).
	seen := make(map[uint16]uint64, len(fullNodeIDs))
	for _, fnid := range fullNodeIDs {
		nodeID := uint16(fnid & nodeIDMask) //nolint:gosec // masked to 16 bits; matches applier.go/encryption_admin.go convention
		prev, exists := seen[nodeID]
		if !exists {
			seen[nodeID] = fnid
			continue
		}
		if prev == fnid {
			continue
		}
		return pkgerrors.Wrapf(ErrNodeIDCollision,
			"full_node_id=%#x and full_node_id=%#x both narrow to node_id=%#04x",
			prev, fnid, nodeID)
	}
	return nil
}
