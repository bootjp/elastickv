# Rolling out MVCC version CAS

`--versionCAS` defaults to false. It enables CAS admission on the node that receives a public request and on the group leader that submits it. Roll it out as follows.

1. Deploy CAS-capable binaries to every voter and learner and enable protected Raft bootstrap and its pinned peer TLS. Use the protected [Raft bootstrap procedure](../raft_bootstrap_enablement.md) for its fixed membership. Protected native admission rejects voter and learner additions and promotions.
2. Drain transactions prepared before fence activation and start the nodes with `--commitTSFence`. Wait for the public services to become ready. The fence activation is a replicated entry. Public startup with this flag waits for every local data group to apply it.
3. Start the participating nodes with both `--commitTSFence` and `--versionCAS`. Exercise Get with the version requested and a CAS against its returned token. Check both direct leader and follower ingress.

The leader checks its applied fence marker and uses pinned TLS to query every other configured replica, including learners, for `RaftAdmin.Status.supports_replicated_catalog_cas` at the same configuration index. Joint configurations are ineligible. A down, unprovisioned, incompatible, or reconfiguring replica prevents new CAS proposals, even if the group has a write quorum. Raft rechecks the term and configuration when appending the proposal. The first eligible versioned read or CAS activates a replicated migration-version identity guard for its data group. Allow the first request to finish before measuring steady-state latency. Versioned reads and CAS are initially available for ordinary keys on routes without migration floors or ownership transition evidence. The status bit covers activation, protected cleanup and the v2 CAS frame that binds the command to its catalog group. It can be true while the node's own CAS admission flag is off.

Both opt-in point reads (`Get` and `RawGet`) and CAS use the leader's live replica check. The activation uses the same proposal protection as CAS and waits for local apply before serving a token. Activation is demand-driven per group and survives normal Raft snapshots. The catalog group must apply its own activation before serving any token.

The initial rollout serves version tokens on the data group that replicates the authoritative catalog. Requests for routes on other groups receive an admission error and can use the existing transactional API. A follower may receive a request and forward it to the protected leader. Catalog watcher lag must never change the outcome of applying a committed CAS frame.

A route is eligible only while it is active, has a zero migration write floor, has no staged visibility, and has no intersecting replicated migration ownership or armed readiness state. Explicit-group `RawGet` must select the same authoritative owner. Keys with auxiliary, legacy fallback, or partition routing are excluded. Migration cutover records a nonzero floor in the target route, promotion retains it, and descendant splits inherit it. The entire moved range stays ineligible, including writes newer than the floor and ranges routed back to their original group. The same policy applies when an inactive migration target later activates for its other ranges. Existing data on eligible initial routes can produce tokens without a rewrite.

To stop new versioned reads and CAS, stop ingress and drain requests, then restart the nodes without `--versionCAS`. Keep the timestamp fence and CAS-capable binaries. Replicas continue to apply committed entries during catch-up or replay. Until the group's replicated migration identity marker, import and promotion replay with their legacy semantics. After the marker, strict identity checks apply even with the local flag off. Both new Raft frames use a reserved opcode that makes the preceding binary halt upon encountering either frame. Retain the compatible binary on the protected membership for restarts and snapshot recovery, since snapshots can compact away these frames.

A client can reuse a successful response's new version for its next edit. A mismatch means the caller must read again before recomputing an update. `Aborted/CAS_ROUTE_FENCED` certifies that the routed CAS was rejected at apply. An unknown outcome after submission requires reconciliation before retrying a logical update.

## Restore and rollback

CAS tokens belong to one key in one cluster history. Raft snapshot installation and ordinary replay continue that history. An offline point-in-time restore can resurrect a version held by an old client.

Servers refuse `--versionCAS` when a data group carries an external snapshot restore origin marker. Such a replica also reports CAS capability as false with the flag off, which prevents leaders with the flag on from submitting CAS in that group. These checks cover the offline restore tools that persist this marker. Preserve it with the restored data directory.

A manual rewind that copies or replaces data directories can evade origin detection. Before serving a manually rewound cluster, stop all old CAS traffic and restart every node with `--versionCAS` off. Keep it off while old requests can reach the restored cluster. The API does not carry a cluster incarnation. Any future activation after a manual rewind requires fencing all old CAS clients, including pending retries, and requiring fresh version reads from the replacement endpoint before accepting CAS again.

## Check the public CAS history with Jepsen

On a disposable cluster with CAS enabled, point the client at the public endpoints for three voters. Use a fresh run for each cluster history. From the repository root:

```sh
go build -o /tmp/jepsen-grpc-client ./cmd/jepsen-grpc-client
cd jepsen
lein run -m elastickv.grpc-workload --local --host 127.0.0.1 \
  --grpc-ports 50051,50052,50053 --grpc-tls-ca-file /path/to/cluster-ca.pem \
  --grpc-client-bin /tmp/jepsen-grpc-client --grpc-api version-cas \
  --concurrency 6 --time-limit 30 --rate 36
```

The workload initializes one fresh non-expiring key, records versioned reads, and submits both current and previously consumed tokens. The history checker requires a successful CAS using the highest CAS version already acknowledged at invocation, a read after an update, and a confirmed rejection when reusing a consumed token. Insufficient coverage yields `:unknown`. A read that overlaps a successful update can legitimately return the preceding version. A read started after the acknowledgement must observe that version or a later one. Transport errors remain indeterminate. A run can pass with indeterminate operations when the observed history meets all three coverage conditions and contains no detected violations.

Multiple public endpoints can forward to the same leader. A green client history therefore covers only the states those clients observed. The [indexed FSM regressions](../../kv/version_cas_review_regression_test.go) compare independently applied durable states and exercise one-row cleanup and reopen. The [formal model](../../tla/cas/README.md) checks both failure modes under its stated assumptions.
