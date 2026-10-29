# SQS HT-FIFO Jepsen observation boundary

Status: implementation and local validation on this branch. Publication and
merge require final-HEAD review and CI; their receipts are tracked on the
associated PR, not implied by the presence of code or by local regressions.

## Incident and scope

The off-mode job for revision `82a23a5c8c1268557c188023b2ab2e40a4c5bdb2`
reported 27 committed sends and 27 acknowledged receives, no loss or duplicate
acknowledgments, and `g1:5 -> g1:4` as an ordering violation. The original
history and invalid result must remain invalid evidence; missing observations
cannot be reconstructed after the run.

The old client combined ReceiveMessage and every sequential DeleteMessage in
its batch into one Jepsen operation. Worker 3 received `g1:4` followed by four
other groups. Worker 4 completed its `g1:5` operation 2.554301 ms before worker
3 completed its batch. Both operations overlapped. A legal execution deletes
`g1:4`, unlocks that group, and receives/deletes `g1:5` while worker 3 is still
deleting unrelated messages. The old checker mistook final batch completion
order for delivery order. The artifact does not record the individual API
boundaries needed to distinguish this execution from a server violation.

This change affects only the HT-FIFO Jepsen client, checker, and tests. It does
not change production SQS semantics, Raft activation, generator concurrency,
batch size, rate, time limit, drain budget, or visibility timeout. Delayed
messages and production visibility-index ordering are outside this change.

## Observation format

Every runtime operation carries versioned `:sqs-observation` metadata. A fresh
test-wide observer is shared by all clients and the workload publishes its
expected `:sqs-observation-clock-id`. Observation timestamps use elapsed
`System/nanoTime` on the controller, not node wall clocks. A short observer
lock makes timestamps strictly increasing, including equal underlying clock
ticks. It never covers an SQS request.

Sends record `:send-start` immediately before the SDK invocation and
`:send-end` only on success. The latter is diagnostic observation metadata;
the ordering oracle always pairs actual Jepsen framework send invocations and
completions by process. No production fixture flag or completion-only fallback
exists. An `:info` return is not a successful end and cannot establish that the
send preceded another invocation. Its invocation is still a valid lower bound.

Receives record `:receive-start` and `:received-at` immediately after the SDK
returns, before decoding or deleting anything. The complete response batch is
retained in original order as `:deliveries`. Each delivery contains:

- `:delivery-index`, its original zero-based batch position;
- `:tuple`, the decoded group and client sequence identity, or nil if malformed;
- SHA-256 `:message-hash` and `:receipt-hash`, never the plaintext identities;
- nested `:delete` metadata with the delivery index, receipt hash, invocation
  and return timestamps, and `:ok`, `:fail`, or `:info` outcome.

No raw body, queue URL, storage key, receipt handle, or SDK error payload is
added to observation metadata. Delete warning text is static. Malformed bodies
and missing identities remain visible to the checker rather than disappearing
through `keep`. The legacy `:value` remains the ordered vector of successfully
deleted tuples. Partial acknowledged results survive a later unknown exception.
The existing top-level transport/server/client outcome classification is retained.
Recognized error codes and anomaly categories remain diagnostic codes; arbitrary
exception text and unrecognized payloads become `:unexpected-client-error`,
still with `:info` status. RuntimeExceptions during a delete retain the partial
raw trace and fail closed rather than exposing a receipt through `:error`.

## Independent assertions

Acknowledged-consumption loss and duplicates use successful deletes. They are
not inferred from raw delivery count. A failed or unknown delete contributes
no acknowledgment, but its raw delivery remains in the ordering evidence.

The ordering oracle uses raw response timestamp followed by delivery index.
It never sorts by client sequence and never removes repeated deliveries.
For same-group sends A and B, it requires A before B only when A's successful
framework completion is strictly earlier than B's framework invocation.
Overlapping sends and equal endpoints do not establish this precedence.

During the raw-delivery scan, each group retains the previously observed send
with maximum invocation time. If the current send's known successful end is
less than that maximum, the earlier-observed message is a witness to inversion.
This detects nonadjacent inversions even when adjacent send intervals overlap.
Matching and validation are linear; response sorting is O(n log n), followed
by a linear scan. The checker does not enumerate all pairs.

## Fail closed

The checker requires the expected run clock ID, supported schema, send invoke
and completion pairs, valid timestamps, contiguous batch indices, decoded
tuples and hashed identities. Each delete must reference its own delivery and
receipt, follow the raw response, and respect sequential batch delete order.
Successful delete tuples must exactly match `:value`. Unknown deliveries,
reused receipt identities, changed message identities, reused response times,
and ambiguous sends carrying a successful end invalidate the trace.

Missing or malformed observations return boolean `:valid? false` with
`:observation-errors`; `:ordering-checked? false` explicitly distinguishes an
uncheckable ordering history from a checked history with no violations.
Legacy acknowledgment counts remain available for diagnosis, not promotion.
`cli/fail-on-invalid!` already accepts only `(true? valid?)`, so neither false
nor an unknown verdict can produce a successful CLI outcome.

## Regression gates

The actual Client.invoke! regression uses channel barriers: deliver 4, delete
4, deliver/delete 5, then finish the unrelated delete in 4's batch. It fails
under the original checker and passes under raw observation. The paired
negative control delivers 5 before 4 while arranging apparently correct final
completion order; it must fail. Each worker signals actual body exit through
a CountDownLatch in finally. Gates are closed and workers are joined inside
with-redefs before restoring the mocked SDK, including failure paths.

Other tests cover wrong within-batch order, nonoverlapping inversions,
overlapping sends, unknown send ends, nonadjacent witnesses, raw redeliveries,
loss, duplicate acknowledgments, malformed bodies and metadata, foreign clock
IDs, missing invokes, receipt replay, and plaintext exclusion. Completion-only
synthetic fixtures explicitly expand test-side point sends into ordinary
framework invocation/completion pairs; the production checker has no bypass.

The historical 27/27 artifact must reproduce its exact original false result
with the original checker, and must remain non-passing under the new checker
because its raw observation metadata is absent. A fresh real-cluster trace is
required to claim ordering validation for the new observation boundary.

## Unit and historical evidence

The channel-barrier regression was first run against the unchanged checker:
one test, four assertions, one failure with `g1:5 -> g1:4`, no loss or
duplicate acknowledgments. After moving the observation boundary, the legal
schedule passes and the genuinely reversed raw-delivery control fails its
ordering contract even when final batch completion order looks correct.

Historical replay used the original checker source at `82a23a5c` and compared
its complete result to the untouched artifact's `results.edn`: exact equality,
including `:valid? false`, 27 committed, 27 acknowledged, and the original
`g1` inversion. The new checker returns false, preserves the 27/27 counts,
reports 98 `:missing-observation` errors and `:ordering-checked? false`.
No raw metadata was invented for that artifact.

Local Jepsen validation uses the installed Leiningen and JDK. An initial run
used a read-only existing checkout at the pinned Redis submodule revision
because the new worktree's submodule was not initialized. The final standard
run instead initializes that exact gitlink, without an external source path:

```sh
git submodule update --init --recursive -- jepsen/redis
cd jepsen
LEIN_OFFLINE=true lein test
```

The pinned submodule is `54ab5c10ace229fc780e43e188ef1b64c3a32ee4`; offline
mode assumes dependencies have already been fetched. The final standard suite
passed 331 tests and 1206 assertions with zero failures or errors. An independent
review exposed malformed SDK batch shapes being converted to empty results or
lost before their return timestamp was recorded. The actual-client regression
first reproduced 13 failures in 49 assertions, despite an already successful
send/ack history. Receive now records the SDK return before conversion and
validates both the response map and batch collection. Seven malformed variants
fail closed; three normal empty shapes still pass. Independent re-review found
no remaining required finding.

The actual-client positive and reversed-raw barrier tests also passed 20
repetitions each: 40 test executions, 240 assertions, zero failures or errors.

An owned loopback three-node, native Raft-backed run of the final workload blob
`482b478adf3946f2396dfa9268740963b592933a` passed with 12 confirmed sends,
12 acknowledged receives and 12 raw deliveries, `:valid? true` and
`:ordering-checked? true`. Loss, duplicates, ordering violations and observation
errors were empty. It used the unchanged five-second/rate-five/concurrency-five,
four-partition/six-group workload and fifteen-second drain. Owned processes
were joined and data/history retained. This short unencrypted local trace does
not prove VM nemesis, long stress or encrypted execution. Fresh PR CI runs both
encryption modes and remains a publication requirement. Unfiltered repository
lint reported zero issues.

## Limitations

These are controller-side API observations, not server apply indices or a
claim to have recovered the historical server schedule. The SDK response
boundary is observed before this client can delete and release the group;
unrelated batch completion delays no longer move that observation. This is a
send-precedence/delivery-order checker, not a full SQS linearizability model,
and it does not add new visibility-expiry or delete-idempotence guarantees.
