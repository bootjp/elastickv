# Runtime readiness for stateful deployments

## Problem

The existing adapter health endpoints answer process liveness or verified local
leadership. Kubernetes currently probes the Redis TCP listener. Neither proves
that a member restored its state, joined the applied configuration, and can keep
applying committed entries. Automated updates must not promote such a member.

## Contract

Add an opt-in, dedicated private health listener with `/livez` for liveness and
`/readyz` (also `/healthz` on this listener) for readiness. Existing protocol health
paths keep their compatible responses. The listener starts before expensive Raft
initialization and reports not ready until runtime sources are registered.

Readiness covers every configured local Raft group: startup replay completed,
engine running without a terminal error, no snapshot installation in progress,
local voter present in the applied membership, a known leader and recent follower
contact, applied index at least the restored snapshot and initial committed
watermark, and continued application while a committed backlog exists. An idle
member with no backlog remains ready. A transient backlog is allowed within a
bounded progress interval; an observed apply regression is not ready.

Snapshot-restore success means the actual engine restore/replay path returned
success, not that a snapshot file exists. A failed restore must never reopen
readiness before engine shutdown is published. Startup and received snapshots
must both be covered. This is not a full byte-level storage scrub, an fsync
proof, or an end-to-end protocol/authentication test.

Use bounded periodic observations and fail closed on missing or stale results.
Responses expose stable reason codes without peer addresses, keys, filesystem
paths, or internal error text. Probes perform no application writes. Use separate
canary operations and cluster-wide quorum/membership checks for release acceptance.

## Deployment

Generic Kubernetes examples use the private listener for HTTP readiness and
process liveness separately. Keep the health port out of public API Services.
Allow only trusted release operators where a NetworkPolicy grant is needed.
During the bootstrap from an image without this endpoint, retain the old probe
until all members have the new listener; then switch readiness. A rollback must
restore compatible probe configuration as well as the prior image if necessary.

## Validation and review

Tests cover idle readiness, startup replay, absent membership/leader, stale
contact, snapshot restore in progress/failure, initial catch-up, stalled versus
progressing backlog, apply regression, and stale cached results. Exercise real
snapshot lifecycle hooks, run focused engine and health tests with race detection,
and run the required replication/Jepsen checks before publishing a production image.
The scope is additive health reporting; persisted and wire formats are unchanged.

Reviewed against the requested deployment gate: keep liveness independent from
long recovery, fail closed on unknown restoration state, and preserve PVC data
through image rollback. No new externally exposed administrative surface is needed.
