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

Use bounded observations on each readiness request and fail closed on missing sources or observation errors.
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
progressing backlog, apply regression, and cancelled observations. Exercise real
snapshot lifecycle hooks, run focused engine and health tests with race detection,
and run the required replication/Jepsen checks before publishing a production image.
The scope is additive health reporting; persisted and wire formats are unchanged.

Reviewed against the requested deployment gate: keep liveness independent from
long recovery, fail closed on unknown restoration state, and preserve PVC data
through image rollback. No new externally exposed administrative surface is needed.

## Implementation review

The listener is opt-in and separate from existing protocol health responses.
The startup serving gate opens only after public services and optional FUSE
startup finish. Engine lifecycle reports initial replay completion and active
snapshot installation; failed received snapshots leave the readiness fence set.
The checker samples current local state per request and retains monotonic progress
history. No cached ready result is reused after an observation failure.

Five review passes:

1. Data loss: no persistence format, timestamp, acknowledgement, or restore order
   changes. Snapshot readiness fencing adds observability only.
2. Concurrency: probe history is serialized; lifecycle reads use the engine lock
   and an atomic snapshot fence, with no reverse callback into the probe. Liveness
   does not take the readiness lock or wait for snapshot installation.
3. Performance: local metadata reads only, no disk scans, ReadIndex calls, or
   canary writes in the handler; tracking state is bounded by configured groups.
4. Consistency: an engine cannot become ready before replay, restored snapshot
   application, local membership, and its initial committed barrier. An idle
   group is not mistaken for a stalled group, and apply regression fails closed.
5. Coverage: race-enabled tests cover lifecycle failure, real restore failure,
   successful snapshot plus WAL replay, membership, progress/stall/idle behavior,
   HTTP method/privacy behavior, and a listener alive before readiness.

Focused race tests and new-diff lint passed. The readiness overlay renders with
Kustomize and deliberately requires a new image digest. Full CI/Jepsen and live
bootstrap validation remain required before enabling this listener in production.
