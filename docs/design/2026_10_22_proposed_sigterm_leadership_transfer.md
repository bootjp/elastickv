# SIGTERM leadership transfer

Status: proposed; implementation and regression tests accompany this change.
Issue: https://github.com/bootjp/elastickv/issues/1284

## Goal

When the production node receives SIGTERM, hand off every locally led Raft
group before stopping transport and closing storage. SIGINT uses the same path.
A follower, a single-voter group, or a group without a healthy successor must
still be able to shut down.

## Shutdown order

1. Reuse the single early `installShutdownSignalHandler` from PR #1273. Before
   startup finishes, its callback cancels immediately. After all servers and the
   filesystem start, `shutdownHandoff.arm` switches the callback to a handoff
   request. Repeated signals remain ignored through storage cleanup; the existing
   clean-exit handling and bounded signal-handler join are preserved.
2. Cancel the local leader-balancer's separate context and join its loop/RPC
   before handing off. This join shares the same five-second deadline as all
   transfers; a stuck balancer skips handoff and still permits bounded shutdown.
   Raft transport and ordinary server contexts remain alive during this step.
   Snapshot each runtime's engine through `snapshotEngine`. For each local
   leader, run a transfer concurrently under one five-second deadline. Re-scan
   after each round until no local leaders remain or that deadline expires, so
   followers promoted during another group's handoff are not missed.
3. Read the group's committed membership, exclude the local voter and learners,
   and call the existing `TransferLeadershipToServerIfEligible` API. The engine
   checks recent activity; Raft catches up the target before sending TimeoutNow.
   Retry failed attempts while the node remains leader and the deadline permits.
   A concurrent loss of leadership ends the attempt successfully.
4. Log transfer failures and continue shutdown. Cancel the server context only
   after the attempts finish, so transfer messages can still use the transport.
5. Reuse #1273's `stopGRPCServerBounded`: drain gRPC requests for up to ten
   seconds, then force remaining streams closed. There is one shared drain policy. Finally, the existing cleanup stack closes services, engines, and
   storage. The supervisor's grace period must include the handoff window, the
   listener/FUSE drain budgets, and storage cleanup.

The bounded gRPC drain is necessary because established Raft streams can keep
`GracefulStop` blocked indefinitely. The initial three-node reproduction
completed both group transfers but remained in `grpc.Server.stop` until killed.

## Scope and safety

The implementation is in `main_shutdown.go`, wired into `run` and
`startRaftServers`. It changes no membership, wire format, persistence format,
timestamp issuance, or Raft election rules. In-flight writes retain the existing
Raft transfer and cancellation semantics. The five-second transfer budget is
shared by all groups, including group 0 when configured.

Successful handoff requires a reachable voting successor. Failure to hand off
is a warning rather than a shutdown error, including on a single-voter node.
An internal error or memory-pressure cancellation proceeds directly to cleanup
without waiting for a handoff. The separate `cmd/server` demo is outside this
production-node lifecycle change.

## Verification

- Unit tests cover multiple leaders, followers, nil runtimes, learner exclusion,
  shared deadlines, transient transfer errors, concurrent leadership loss, and
  pre-existing context cancellation.
- An in-memory gRPC health watch covers draining a persistent stream.
- A three-node, two-group E2E starts the production `run` path in a subprocess,
  moves both leaderships onto it, commits a write, sends an actual SIGTERM, and
  checks clean exit, both handoffs, preserved data, and a subsequent write.
- Run the shutdown tests with the race detector, the available Go package suite,
  lint, and both Jepsen unit suites. The pre-existing FUSE mount test uses a
  Linux-only `Statfs_t.Namelen` field and currently prevents full-suite test and
  lint typechecking on macOS.

Initial validation on macOS with Go 1.26.6, before integrating PR #1273:

- `go test . -run '^TestShutdown' -count=1`: passed, including actual SIGTERM.
- The same command with `-race`: passed.
- Lint across all packages except `internal/filesystem/fuseadapter`: zero issues.
- Root Jepsen `lein -o test`: 204 tests, 566 assertions, zero failures/errors.
- The Go suite excluding FUSE passed every package except `adapter`, which hit
  the default ten-minute timeout in existing
  `Test_consistency_satisfy_write_after_read_sequence` and `Test_grpc_transaction`.
  This change does not modify the adapter package.
- The upstream Redis submodule's unit suite could not load `jepsen.redis`;
  its checked-in test references that absent namespace and contains a placeholder
  failing assertion. The Redis workload tests in the root Jepsen suite passed.


## Review integration with PR #1273

This branch incorporates dependency commit `3eb6989351d360079d7b9e8416a52de8357a2fa0`
from #1273; that PR should merge first. Its early signal handler and bounded
gRPC/HTTP/Redis/FUSE cleanup are reused rather than registering a second signal
handler or retaining a second gRPC drain implementation. The five-second handoff
manager also bounds cancellation independently of backpressured log writes.

The follower-acquisition regression failed before the re-scan fix. The actual
SIGTERM E2E also failed when both original signal registrations were present.
After integration, the shutdown, signal, clean-exit, gRPC, and filesystem lifecycle
tests passed with the race detector, and full-package lint reported zero issues.
The FUSE Linux-only typecheck issue listed in the initial validation is fixed by
the dependency's platform tags; kernel-mount checks still require Linux FUSE.
The inherited recording-helper fixture now uses shell redirection so it also
runs on macOS without `/bin/touch`. The FUSE package passed its full race suite;
targeted adapter/monitoring shutdown tests also passed with the race detector.
The root Jepsen suite again passed all 204 tests and 566 assertions.
