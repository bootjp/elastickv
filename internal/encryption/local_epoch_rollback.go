package encryption

import (
	pkgerrors "github.com/cockroachdb/errors"
)

// CheckLocalEpochRollback is the §9.1 / 6C-3 startup-guard
// primitive for `ErrLocalEpochRollback`. It compares this node's
// sidecar `local_epoch` for the active storage DEK against the
// local writer-registry's `LastSeenLocalEpoch` for the
// `(full_node_id, active_storage_dek_id)` row, and returns
// `ErrLocalEpochRollback` when `sidecar <= registry` — the
// strict-ahead invariant from §5.2 of the 6D design doc.
//
// Why strict-ahead and not "strictly less than":
//
//   - `sidecar < registry`: classic rollback (sidecar restored
//     from an old backup); the node would reissue
//     `(node_id, local_epoch)` prefixes already consumed by prior
//     writes under the same DEK.
//
//   - `sidecar == registry`: replay of the same epoch; the node
//     would reissue the SAME prefix and reuse the GCM counter,
//     identical to the collision scenario but at the
//     single-node-restart timescale rather than the
//     cluster-membership timescale (which `ErrNodeIDCollision`
//     handles).
//
//   - `sidecar > registry`: the healthy case; the node has
//     advanced its sidecar past the last replicated registration,
//     so the next nonce prefix is fresh.
//
// The primitive consults LOCAL state only (sidecar + Pebble
// writer-registry on this node). No RPC. This matches the
// startup-before-serving phase where the gRPC server is not up.
//
// Skip conditions handled by the CALLER (the startup-guard
// wiring), not by this primitive:
//
//   - Encryption disabled.
//   - Sidecar's `Active.Storage == 0` (bootstrap not yet
//     committed; no DEK to compare against).
//
// Skip condition handled HERE (inside the primitive):
//
//   - The writer-registry has no row for
//     `(full_node_id, active_storage_dek_id)`. A freshly-joined
//     learner that has not yet proposed a
//     `RegisterEncryptionWriter` legitimately lacks a registry
//     row; the §4.1 case-1 first-seen monotonicity branch will
//     create it on the next encrypted write. Returning nil here
//     preserves that lifecycle.
//
// Returns `ErrLocalEpochRollback` wrapped with both observed
// values when `sidecar <= registry`. Returns the wrapped
// underlying error (NOT marked with `ErrLocalEpochRollback`) on
// a Pebble I/O error from the registry read so the operator
// triages a transport failure separately from a real rollback.
func CheckLocalEpochRollback(
	registry WriterRegistryStore,
	fullNodeID uint64,
	activeStorageDEKID uint32,
	sidecarLocalEpoch uint16,
) error {
	key := RegistryKey(activeStorageDEKID, uint16(fullNodeID&nodeIDMask)) //nolint:gosec // masked to 16 bits; matches applier.go convention
	rawVal, ok, err := registry.GetRegistryRow(key)
	if err != nil {
		return pkgerrors.Wrapf(err,
			"local_epoch rollback guard: read writer-registry row for full_node_id=%#x dek_id=%d",
			fullNodeID, activeStorageDEKID)
	}
	if !ok {
		// Freshly-joined learner with no registry row yet. Per
		// §5.2 skip-condition, this is a legitimate lifecycle
		// state — the first encrypted write will create the row
		// via §4.1 case-1 first-seen.
		return nil
	}
	registryRow, err := DecodeRegistryValue(rawVal)
	if err != nil {
		return pkgerrors.Wrapf(err,
			"local_epoch rollback guard: decode writer-registry row for full_node_id=%#x dek_id=%d",
			fullNodeID, activeStorageDEKID)
	}
	if sidecarLocalEpoch <= registryRow.LastSeenLocalEpoch {
		return pkgerrors.Wrapf(ErrLocalEpochRollback,
			"sidecar local_epoch=%d <= registry last_seen_local_epoch=%d (full_node_id=%#x dek_id=%d) — strict-ahead invariant requires sidecar > registry",
			sidecarLocalEpoch, registryRow.LastSeenLocalEpoch, fullNodeID, activeStorageDEKID)
	}
	return nil
}
