# SIGTERM leadership transfer

Status: proposed; implementation and regression tests accompany this change.
Issue: https://github.com/bootjp/elastickv/issues/1284

## Goal

When the production node receives SIGTERM, hand off every locally led Raft
group before stopping transport and closing storage. SIGINT uses the same path.
A follower, a single-voter group, or a group without a healthy successor must
still be able to shut down.

## Shutdown order

1. Register the signal handler after constructing the group runtimes, before
   starting the servers. Stop the signal subscription during cleanup.
2. Snapshot each runtime's engine through `snapshotEngine`. For each local
   leader, run a transfer concurrently under one five-second deadline.
3. Read the group's committed membership, exclude the local voter and learners,
   and call the existing `TransferLeadershipToServerIfEligible` API. The engine
   checks recent activity; Raft catches up the target before sending TimeoutNow.
   Retry failed attempts while the node remains leader and the deadline permits.
   A concurrent loss of leadership ends the attempt successfully.
4. Log transfer failures and continue shutdown. Cancel the server context only
   after the attempts finish, so transfer messages can still use the transport.
5. Drain gRPC requests for up to five more seconds, then force remaining streams
   closed. Finally, the existing cleanup stack closes services, engines, and
   storage. The supervisor's grace period must also allow storage cleanup.

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

Local validation on macOS with Go 1.26.6:

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
