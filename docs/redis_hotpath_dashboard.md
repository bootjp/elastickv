# Redis Hot Path Dashboard (PR #560 verification)

The "Hot Path (legacy PR #560)" collapsed row at the bottom of
`monitoring/grafana/dashboards/elastickv-redis-summary.json` is the
operator view for the Redis GET hot path. It was added to confirm that
PR #560 (`a45ca291` "perf(redis): fast-path GET to avoid ~17-seek type
probe") landed cleanly in production. Expand the row to see the
panels described below.

## How to confirm #560 worked

Three panels together answer the question. All other panels on the
dashboard are supporting context.

| Panel                                  | Expected direction post-deploy                                                                |
| -------------------------------------- | --------------------------------------------------------------------------------------------- |
| **LinearizableRead Rate (lease miss)** | Falls sharply as each node rolls. A steady GET workload used to push every GET through this.  |
| **GET p99 (success)**                  | Flat or down. The fast path removes ~15 pebble seeks per GET, shaving the head of the tail.   |
| **Lease Fast-Path Hit Ratio**          | Climbs toward 1.0. Leases stay warm because GETs no longer force slow-path ReadIndex traffic. |

If LinearizableRead rate drops but p99 worsens, something on the
lease-read path is regressing (look at Raft Queue Saturation). If p99
drops but the miss rate does not, GETs are still taking the old path,
which usually means the adapter wiring was not updated on that node.

## Metrics surfaced for this dashboard

Added in this PR:

- `elastickv_lease_read_total{outcome="hit|miss"}` -- counter
  incremented at the lease-read call sites in `kv/coordinator.go`
  (`Coordinate.LeaseRead`, `Coordinate.LeaseReadForKey`) and the
  shared `kv.groupLeaseRead` helper used by
  `ShardedCoordinator.LeaseRead` / `LeaseReadForKey`. Wired via the
  `kv.WithLeaseReadObserver` option on `Coordinate` and
  `ShardedCoordinator.WithLeaseReadObserver`.
- `elastickv_raft_dispatch_dropped_total{group}` -- mirrors the etcd
  raft engine's `dispatchDropCount`.
- `elastickv_raft_dispatch_errors_total{group}` -- mirrors
  `dispatchErrorCount`.
- `elastickv_raft_step_queue_full_total{group}` -- new counter that
  increments every time `enqueueStep` rejects an inbound raft message
  because `stepCh` was at capacity (the "etcd raft inbound step queue
  is full" signal).

Already present and reused:

- `elastickv_redis_request_duration_seconds_bucket` for p50/p95/p99.
- `elastickv_redis_requests_total` / `elastickv_redis_errors_total` for
  throughput and error ratio.

The dispatch counters are polled, not incremented inline. The etcd
Engine exposes `DispatchDropCount() / DispatchErrorCount() /
StepQueueFullCount()` accessors (atomic.Uint64 reads) and
`monitoring.DispatchCollector` samples them on a 5s tick that matches
`RaftObserver`. Polling avoids taking an extra interface call on the
hot raft dispatch path.

## What this dashboard deliberately does NOT show

- Pebble SeekGE rate per command. Pebble's built-in metrics are not
  yet wired into the Prometheus registry; when they are, add a panel
  alongside "GET vs SET vs TYPE vs EXISTS Rate" using
  `pebble_seek_ge_total` (or equivalent) so operators can graph real
  seek amplification rather than inferring it.
- goroutine counts in `rawKeyTypeAt` stacks. Go's pprof already
  covers this; surfacing it as a Prometheus metric would require a
  custom collector that walks `runtime.Stack()`, which is too
  expensive to run continuously.
