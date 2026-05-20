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
// Missing-registry-row split — `storageEnvelopeActive` parameter:
//
// §5.2 of the 6D design doc splits the missing-row behaviour on
// whether the storage envelope cutover has fired:
//
//   - PRE-cutover (`storageEnvelopeActive == false`): missing
//     row is the freshly-joined-learner lifecycle state. The
//     node has not yet proposed a `RegisterEncryptionWriter`
//     and the §4.1 case-1 first-seen branch will create the
//     row on the next encrypted write. Allow startup.
//
//   - POST-cutover (`storageEnvelopeActive == true`): missing
//     row means there is NO rollback anchor to compare the
//     sidecar against, but encrypted writes are already
//     happening cluster-wide under the active storage DEK.
//     The node could start issuing nonces with no
//     monotonicity guard, which is the exact failure mode the
//     guard exists to prevent. Refuse startup with
//     `ErrLocalEpochRollback` wrapped with a "missing
//     registry row with active envelope" diagnostic.
//
// Pebble I/O errors from the registry read propagate as wrapped
// errors NOT classified as `ErrLocalEpochRollback` (operator
// triages transport failure separately from a real rollback).
//
// Returns `ErrLocalEpochRollback` wrapped with both observed
// values when `sidecar <= registry`. Returns
// `ErrLocalEpochRollback` wrapped with a missing-row
// diagnostic when the row is absent AND
// `storageEnvelopeActive == true`. Returns nil when the row is
// absent AND `storageEnvelopeActive == false` (pre-cutover
// freshly-joined learner) or when `sidecar > registry`.
// missingRegistryRowResult is the shared §5.2 split for "no
// registry row for THIS node": pre-cutover allow startup,
// post-cutover refuse. The function is invoked both when the
// Pebble Get returns ok=false AND when a row exists but its
// FullNodeID does not match the caller's expectation (a
// historical occupant of the same 16-bit slot).
func missingRegistryRowResult(storageEnvelopeActive bool, fullNodeID uint64, activeStorageDEKID uint32) error {
	if storageEnvelopeActive {
		return pkgerrors.Wrapf(ErrLocalEpochRollback,
			"writer-registry has no row for full_node_id=%#x dek_id=%d but storage_envelope_active=true; cannot prove nonce monotonicity for the active DEK",
			fullNodeID, activeStorageDEKID)
	}
	return nil
}

func CheckLocalEpochRollback(
	registry WriterRegistryStore,
	fullNodeID uint64,
	activeStorageDEKID uint32,
	sidecarLocalEpoch uint16,
	storageEnvelopeActive bool,
) error {
	key := RegistryKey(activeStorageDEKID, uint16(fullNodeID&nodeIDMask)) //nolint:gosec // masked to 16 bits; matches applier.go convention
	rawVal, ok, err := registry.GetRegistryRow(key)
	if err != nil {
		return pkgerrors.Wrapf(err,
			"local_epoch rollback guard: read writer-registry row for full_node_id=%#x dek_id=%d",
			fullNodeID, activeStorageDEKID)
	}
	if !ok {
		return missingRegistryRowResult(storageEnvelopeActive, fullNodeID, activeStorageDEKID)
	}
	registryRow, err := DecodeRegistryValue(rawVal)
	if err != nil {
		return pkgerrors.Wrapf(err,
			"local_epoch rollback guard: decode writer-registry row for full_node_id=%#x dek_id=%d",
			fullNodeID, activeStorageDEKID)
	}
	// Defense-in-depth: the registry key narrows full_node_id to
	// its low 16 bits, so a row at the same key MIGHT belong to a
	// DIFFERENT writer that previously occupied the same 16-bit
	// slot. ErrNodeIDCollision is the primary layer that catches
	// active collisions, but historical occupancy (a since-removed
	// member that left its row behind) is not in the route-catalog
	// snapshot the collision guard reads. Treat a row whose
	// FullNodeID does NOT match the caller's expectation as if no
	// row exists for THIS node — route through the §5.2
	// storage_envelope_active split. Pre-cutover this is a
	// freshly-joined-learner-with-historical-collision case (allow
	// startup; the next OpRegistration will overwrite the foreign
	// row at §4.1 case 1). Post-cutover it is an unrecoverable
	// missing rollback anchor for THIS node (refuse startup).
	if registryRow.FullNodeID != fullNodeID {
		return missingRegistryRowResult(storageEnvelopeActive, fullNodeID, activeStorageDEKID)
	}
	if sidecarLocalEpoch <= registryRow.LastSeenLocalEpoch {
		return pkgerrors.Wrapf(ErrLocalEpochRollback,
			"sidecar local_epoch=%d <= registry last_seen_local_epoch=%d (full_node_id=%#x dek_id=%d) — strict-ahead invariant requires sidecar > registry",
			sidecarLocalEpoch, registryRow.LastSeenLocalEpoch, fullNodeID, activeStorageDEKID)
	}
	return nil
}
