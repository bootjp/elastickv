# SQS side-record derivation (Phase 0b M5-2) — proposed

**Status:** Proposed (no implementation yet).
**Parent:** [`2026_05_25_proposed_snapshot_logical_encoder.md`](2026_05_25_proposed_snapshot_logical_encoder.md) — this resolves the §"Per-adapter reverse encoders / SQS per-message side records" decision gate that M5-1 (`PR #846`) explicitly deferred.
**Predecessor on disk:** M5-1 emits `!sqs|queue|meta|`, `!sqs|queue|gen|`, `!sqs|queue|seq|`, `!sqs|msg|data|` for every queue + message in the dump. The four **side records** below are NOT emitted by M5-1; restoring without them silently breaks FIFO dedup, group locking, the by-age reaper scan, and visibility resumption.

## What needs to land

For every `!sqs|msg|data|...` record M5-1 already writes, the encoder must also write the **derived** side records the live adapter would have written:

| Family            | Classic key (partition_count = 1)                                                                              | Partitioned key (partition_count > 1)                                                                            | Value (live)                              | Derivation rule                                                                                                                                                          |
| ----------------- | -------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- | ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `vis`             | `!sqs\|msg\|vis\|<queue-seg>\|<gen-BE>\|<visibleAt-BE>\|<msgID-seg>`                                           | `!sqs\|msg\|vis\|p\|<queue-seg>\|<part-BE>\|<gen-BE>\|<visibleAt-BE>\|<msgID-seg>`                               | `[]byte(messageID)`                       | One row per data record. **`visibleAt` always restored to `available_at_millis`** (= "visible now"), matching parent §"SQS: vis zeroed by default."                      |
| `byage`           | `!sqs\|msg\|byage\|<queue-seg>\|<gen-BE>\|<sendTs-BE>\|<msgID-seg>`                                            | `!sqs\|msg\|byage\|p\|<queue-seg>\|<part-BE>\|<gen-BE>\|<sendTs-BE>\|<msgID-seg>`                                | `[]byte(messageID)`                       | One row per data record. `sendTs = send_timestamp_millis` from `messages.jsonl`. Reaper needs this even on a brand-new restore — without it, retention never expires.    |
| `dedup` (FIFO)    | `!sqs\|msg\|dedup\|<queue-seg>\|<gen-BE>\|<dedupID-seg>`                                                       | `!sqs\|msg\|dedup\|p\|<queue-seg>\|<part-BE>\|<gen-BE>\|<group-seg>\|<dedupID-seg>`                              | `sqsFifoDedupRecord` JSON                 | **FIFO queues only.** One row per data record whose `message_dedup_id` is non-empty. `OriginalSequence = sequence_number`; `ExpiresAtMillis = SendTimestampMs + window`. |
| `group` (FIFO)    | `!sqs\|msg\|group\|<queue-seg>\|<gen-BE>\|<group-seg>`                                                         | `!sqs\|msg\|group\|p\|<queue-seg>\|<part-BE>\|<gen-BE>\|<group-seg>`                                             | `sqsFifoGroupLock` JSON (zero value)      | **FIFO queues only.** One row per distinct `message_group_id` across the queue's messages — NOT per message. Lock value is zeroed (no in-flight receive on a fresh restore). |

All key shapes are defined verbatim in `adapter/sqs_keys.go` (prefixes `SqsMsg{Vis,ByAge,Dedup,Group}Prefix`, `SqsPartitionedMsg{...}Prefix`) and the constructors `sqsMsg{Vis,ByAge,Dedup,Group}Key{,Dispatch}` / `sqsPartitionedMsg{...}Key`. Encoded segments use `encodeSQSSegment` (base64-raw-URL). The encoder **MUST reuse these constructors via duplicated helpers in `internal/backup`** (parent doc §"Per-adapter reverse encoders / SQS per-message side records" — "same rules as `adapter/sqs_messages.go` / `sqs_keys.go`"), exactly as M3b-3 duplicated GSI helpers — with a cross-check test asserting byte-identical output for a shared fixture.

## Decision gate: full reconstruction vs. lazy rebuild

The parent doc (§"Re-derivable indexes" → scope note) allows a per-adapter fallback. M5-2 resolves this for SQS:

**Recommended: full reconstruction (this proposal).** Emit all four families inline during M5-1's existing per-message walk. Rationale:

- **Dedup is correctness, not perf.** A FIFO restore without dedup rows lets the queue redeliver an already-acked message (parent doc §"Scope note" — "fallback is not zero-cost transparency"). There is no graceful degradation.
- **`byage` is required for retention.** Without it, the reaper (`sqs_reaper.go`) never finds the message and `MessageRetentionPeriod` is silently ignored — a restored queue grows unboundedly.
- **`vis` is required for receive.** ReceiveMessage scans `!sqs|msg|vis|` — a data row without a matching vis row is invisible forever.
- **Cost is bounded.** All four are `O(messages-in-dump)` walks over data already loaded by M5-1; no extra disk read, no cross-message coordination, no Raft round-trip (encoder is offline).

**Fallback (rejected): lazy rebuild.** Would require a post-restore admin `SCAN+REBUILD` pass on the target cluster before traffic resumes (parent doc requires this for any fallback). For SQS this would mean walking every `!sqs|msg|data|` record post-restore and writing the derived rows — i.e. doing M5-2's work anyway, just at restore time instead of encode time, with traffic gated until completion. No reason to defer the same work to a less-tested code path.

**Out of scope (deferred):**

- **In-flight receives.** A live cluster may have non-zero `VisibleAtMillis` for messages currently held by a consumer. The dump format (`messages.jsonl`) does not preserve this — parent §"SQS" mandates "vis zeroed by default." Anyone wanting at-the-instant transactional snapshots should use a different mechanism than logical encode/decode. Documented in the encoder header.
- **Tombstones (`!sqs|queue|tombstone|`).** These mark superseded-generation cleanup work for the reaper. A fresh restore has only the current generation = `sqsRestoreGeneration` (1), with no superseded gens to clean up. The encoder writes no tombstones; documented in the file header.
- **Active group locks.** A receive holds `sqsFifoGroupLock.OwnerToken` until DeleteMessage. The dump preserves no in-flight state, so locks restore zeroed (no owner) — first post-restore receive on the group acquires the lock cleanly.

## Files to add (M5-2 implementation slice)

```
internal/backup/encode_sqs_side.go        # derivation + key helpers (duplicated from adapter/sqs_keys.go)
internal/backup/encode_sqs_side_test.go   # byte-identical cross-check vs. live adapter + round-trip
```

Wire into `encode_sqs.go`'s existing `encodeQueueMessages` walk: after `addMessage(...)` emits `!sqs|msg|data|`, call `addSideRecords(...)` to emit the matching vis/byage/dedup/group rows in the same loop. No new disk reads.

## Test plan

| Test                                                              | Verifies                                                                                                                                                                                |
| ----------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `TestSQSEncodeSideRecordsCrossCheckClassic`                       | For a fixture queue (1 partition, mixed FIFO/standard, varied dedup/group), each derived row is byte-identical to what `adapter/sqs_fifo.go`'s send path would write. **Gold-standard.** |
| `TestSQSEncodeSideRecordsCrossCheckPartitioned`                   | Same, with `partition_count = 4`. Both key family AND value JSON match.                                                                                                                 |
| `TestSQSEncodeSideRecordsStandardQueueOmitsFIFOFamilies`          | Non-FIFO queue: vis + byage emitted; dedup + group **NOT** emitted (would be junk per `adapter/sqs_keys.go` semantics).                                                                 |
| `TestSQSEncodeSideRecordsEmptyDedupOmitsDedupRow`                 | FIFO message with empty `message_dedup_id`: data + vis + byage + group emitted; dedup row **NOT** emitted.                                                                              |
| `TestSQSEncodeSideRecordsGroupRowDeduped`                         | FIFO queue with N messages across M distinct groups: exactly M group rows emitted (one per group, not one per message).                                                                 |
| `TestSQSEncodeFifoRestoreRoundTrip`                               | Build dump → encode → boot single-node cluster → ReceiveMessage returns every message in the original send order, exactly once. Re-sending with the same dedup-id is rejected.            |
| `TestSQSEncodeReaperFindsRestoredMessage`                         | Set `MessageRetentionPeriod` short, restore old messages, advance HLC: reaper deletes them. Without `byage`, this would hang.                                                            |

The first two are the §"Encoder cross-check" pattern parent doc §"Per-adapter reverse encoders" mandates. The last two pin the end-to-end correctness rationale for choosing full reconstruction over lazy.

## Self-review (5 passes) — proposed

1. **Data loss.** Full reconstruction (not fallback) eliminates the silent-redeliver + invisible-message classes called out above. Restore-time state matches what the live writer would have produced for the same send sequence.
2. **Concurrency / distributed failures.** Pure offline derivation; no Raft, no lock ordering. The restored cluster's first sends/receives go through the normal OCC path against the restored state.
3. **Performance.** Each side row is one `snapshotBuilder.Add` per data row (vis/byage), plus dedup-when-present and group-deduped. No additional disk reads. Snapshot size grows ~3-4× the message count, all small fixed-width keys.
4. **Data consistency.** Group rows MUST be deduped before emission; emitting one per message would create duplicate keys and corrupt the snapshot (snapshotBuilder rejects). FIFO families MUST be gated on `meta.FIFO`; emitting them for standard queues poisons the keyspace. Both pinned by tests above.
5. **Test coverage.** Two cross-check tests (classic + partitioned) match what M3b-3 did for DDB GSI; two end-to-end restore tests pin the rationale; three negative tests pin the conditional-emission rules.

## Milestones / sequencing

M5-2 lands as a **single PR** (this doc + implementation + tests) once the doc is reviewed. No further splits — the work is small and the families are tightly coupled (any caller-audit needs to see all of them at once).

## Open questions

None at proposal time. Reviewers: please flag if there is any in-flight-state preservation requirement that "vis zeroed on restore" would break for known users, or any tombstone scenario the omission rationale above misses.
