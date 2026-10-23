# Bounded Split Regressions

`elastickv.split-workload --repeat-splits` opts into exactly three splits:

1. Same-owner split at `m2-split`.
2. Split its right child at `m2-t` into another Raft group.
3. Split the moved right child at `m2-u` back into the original group.

Each step re-reads the route catalog and uses its current version and child ID.
The next split cannot start until the preceding split completes. A rejected
helper call, FAILED/ABANDONED job, timeout, or missing lifecycle fails the run;
it does not count as successful migration or authorize another split.

The oracle checks exact partition bounds, unchanged retained/unrelated routes,
owners, increasing catalog versions, distinct jobs, and the next source being
the previous moving child. SourceRouteID remains original lineage. Observed
working children must keep their ID through cutover (e.g. working route 40,
not the original parent 20). Polling can miss FENCE; absence of a sampled fence
does not prove that a durable fence was correct or never occurred. Job-phase
and route queries are separate RPCs, not an atomic evidence snapshot. The helper
does not expose durable SourceRouteID/WorkingSourceRouteID fields; the lineage
check uses the requested source and observed geometry, not decoded job internals.

Four small integer registers cover all final ranges. Final reads from every
node run after the main phase and fault recovery. The coverage checker requires
an acknowledged write to every register and successful final reads after the
last verification; the existing independent linearizability checker checks values.
The default single-split and explicit abandon workloads are unchanged.

## Running

Use an isolated cluster with an ACTIVE route containing all four register keys,
two Raft groups, and the existing split/list-routes binaries. A programmatic test
uses 8 workers by default; CLI invocations must specify `--concurrency 8` (the
shared CLI default is 5). The worker count must be even and at least the node
count and 8. For an already provisioned cluster, for example:

```sh
lein run -m elastickv.split-workload --local --nodes n1,n2,n3 \
  --raft-groups 1=50051,2=50052 --grpc-host-port n1:50051 \
  --repeat-splits --concurrency 8 --rate 5 --time-limit 90 \
  --split-bin /path/to/elastickv-split --list-routes-bin /path/to/elastickv-list-routes
```

There are at most 100 writes/key by default. Verification polls every 250 ms,
times out after 120 s/job, and retains at most 64 distinct observations; exceeding
that cap fails rather than discarding evidence. A short run that misses a split
fails. `--repeat-splits` and `--abandon-at-fence` are mutually exclusive.

## Size-Guard Limits And Runtime Gate

The controller/oracle unit cases cover a ResourceExhausted helper rejection,
preserved diagnostics, sticky failure, and preflight ABANDONED not being DONE.
They do **not** construct an oversized value or exercise the replicated guard.
Redis register writes are integers; the helpers expose only job ID/phase, not
guard activation, durable fence indexes, preflight cause, or pending PREPAREs.
Adding payload padding alone cannot establish a pre-fence admission test.

The Go adapter's `TestMigrationQuorumPartitionAcceptance` separately exercises
three real data voters, a new learner, configuration/leader changes and native
snapshot restart. It asserts typed moving-range rejection, retained-left large
writes and exact restored bytes. `TestMigrationQuorumPreparedDrainAndHistoricalPreflight`
holds large PREPAREs before guard activation, resolves COMMIT and ABORT through
real Raft, and detects a retained oversized historical version beneath a small
latest value before either a source or catalog write fence. Root metadata in
these data-quorum tests is explicitly stubbed; they do not establish TLS/auth
or full cross-group root/data fault-injection acceptance. Deterministic controller
regressions separately cover repeated cross-group moves and retained negative owners.

Production activation remains opt-in through authentication and opcode flags,
installed read/proposal/consumer hooks, a supported applied configuration and
all-current-apply-member negotiation. Flags alone cannot bypass namespace
activation or authorize imported data. The Jepsen additions here are scheduling
and oracle unit coverage, not a completed real-cluster fault-injection campaign.

Run focused coverage and the full harness suite from `jepsen/`:

```sh
lein test elastickv.split-workload-test elastickv.split-regression-test
lein test
```

The full suite requires the pinned `jepsen/redis` submodule, unlike the focused
split tests. In a fresh worktree, initialize it or add a clean checkout of that
same pinned revision to Lein's source paths; do not substitute stub namespaces.
