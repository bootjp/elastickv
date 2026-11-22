# Share the local and forwarded raw commit queue within one Raft group

The full VM150 profile shows two TransactionManager flushers for each of two
leader Engine pointers. The local ShardGroup LeaderProxy uses a wrapped proposer,
while Internal.Forward uses a separately constructed manager for the same engine.
Their independent small batches each require Raft WAL and FSM Sync. Kernel traces
and the full-ramp counters show WAL wait increasing under load, without ongoing
handoff retries, CPU saturation, memory pressure or disk reads. The first parallel
append ABBA did not meet the performance goal and is retained as an intermediate.

Expose a borrowed Forward transaction port from a ShardGroup's existing local
TransactionManager. Unwrap only known group-owned LeaderProxy/lease wrappers;
custom or missing transactions keep the existing separate-manager fallback.
Production server wiring and its real Linux fixture must choose this same port.
It shares the queue, dynamic encryption proposer, observer and final migration
admission hook. No global registry, new worker, timer, batch limit or sync option
is introduced. The group remains the manager's sole lifecycle owner; the borrowed
port has Commit/Abort only and must not close it.

Internal.Forward already performs leader verification, phase checks, timestamp
validation/stamping and StampTimestampAdmission before Commit. The borrowed port
must enter the common manager commit path after that timestamp preparation, so
it never clears or re-stamps the receiver's validated creation epoch. Public
local Commit keeps its existing preparation. Absence guards, per-request results,
sequential transaction phases, and final migration checks still run. The borrowed
port reports readiness only through the underlying configured manager.

Native/owned requests retain the receiver's original context and bypass legacy
cross-request batching as before. Unknown, joined and partial outcomes remain
terminal. Existing FIFO, whole caller groups, queue bounds, per-entry atomic
apply, applied-index and Sync-before-ACK contracts remain unchanged.

Regression: actual local plus authenticated Forward traffic must reach one
queue and share a durable proposal; no ACK before proposal completion. Verify
forwarded epoch preservation, local preparation, migration revalidation, owned
context/cancellation, fallback and dynamic envelope wrapping. Run focused race,
full Go/lint/Jepsen and a fresh profile-free exclusive VM150 ABBA with the original
four-rate workload and all ACKs audited at three ingresses. Performance remains
an acceptance criterion, not a claim of the design alone.
