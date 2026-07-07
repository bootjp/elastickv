# SQS side-record derivation (Phase 0b M5-2) — implemented

**Status:** Implemented.
**Parent:** [`2026_05_25_implemented_snapshot_logical_encoder.md`](2026_05_25_implemented_snapshot_logical_encoder.md) — this resolves the §"Per-adapter reverse encoders / SQS per-message side records" decision gate that M5-1 (`PR #846`) explicitly deferred.
**Predecessor on disk:** M5-1 emits `!sqs|queue|meta|`, `!sqs|queue|gen|`, `!sqs|queue|seq|`, `!sqs|msg|data|` for every queue + message in the dump. M5-2 adds the **side records** below (vis / byage / dedup; group is intentionally not emitted — see table), which are required for FIFO dedup, the by-age reaper scan, and visibility resumption after restore.

## Implemented behavior

For every `!sqs|msg|data|...` record M5-1 already writes, the encoder must also write the **derived** side records the live adapter would have written:

**Scope:** M5-2 implements the classic queue side-record derivation (`partition_count = 1`). The partitioned-FIFO lift is implemented by the M5-3 child doc and reuses the same no-group-row and dedup/byage/vis derivation rules with partitioned key shapes.

| Family         | Key (classic)                                                            | Value (live)              | Derivation rule                                                                                                                                                          |
| -------------- | ------------------------------------------------------------------------ | ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `vis`          | `!sqs\|msg\|vis\|<queue-seg><gen-BE><visibleAt-BE><msgID-seg>`           | `[]byte(messageID)`       | One row per data record. **`visibleAt` always restored to `available_at_millis`** (= "visible now"), matching parent §"SQS: vis zeroed by default."                      |
| `byage`        | `!sqs\|msg\|byage\|<queue-seg><gen-BE><sendTs-BE><msgID-seg>`            | `[]byte(messageID)`       | One row per data record. `sendTs = send_timestamp_millis` from `messages.jsonl`. Reaper needs this even on a brand-new restore — without it, retention never expires.    |
| `dedup` (FIFO) | `!sqs\|msg\|dedup\|<queue-seg><gen-BE><dedupID-seg>`                     | `sqsFifoDedupRecord` JSON | **FIFO queues only.** One row per data record whose `message_dedup_id` is non-empty. All four fields of `sqsFifoDedupRecord` (see derivation note below).                |
| `group` (FIFO) | (not emitted)                                                            | (not emitted)             | **NOT emitted on restore.** The live `loadFifoGroupLock` (`adapter/sqs_fifo.go:135`) treats *key presence alone* as "lock held" (returns the parsed lock; only missing-key returns nil). Emitting any value — even a zeroed `sqsFifoGroupLock` — would falsely indicate an in-flight receive with no owner token, **permanently blocking every group** until manual cleanup (gemini critical #885 line 37). Live writes the lock only on `ReceiveMessage` (`sqs_fifo.go:293`), never on send; a fresh restore has no in-flight receives, so the correct steady state is *no group rows*. |

> **Notation.** Pipe characters inside `<seg>…<seg>` are visual separators in this table, not literal bytes. The only literal `|` bytes are inside the family prefix (`!sqs|msg|vis|` etc.); variable-length segments are concatenated directly (with length-determined boundaries baked into the constructors). The single source of truth is the live key constructors cited below.

**`sqsFifoDedupRecord` field derivation** (all four fields are required; live writer at `adapter/sqs_fifo.go:219-223`):

| Field             | JSON tag             | Restored from                                                          |
| ----------------- | -------------------- | ---------------------------------------------------------------------- |
| `MessageID`       | `message_id`         | `message_id` from `messages.jsonl`                                     |
| `SendTimestampMs` | `send_timestamp_ms`  | `send_timestamp_millis` from `messages.jsonl`                          |
| `ExpiresAtMillis` | `expires_at_millis`  | `SendTimestampMs + sqsFifoDedupWindowMillis` (`= sendTs + 5 minutes`)  |
| `OriginalSequence`| `original_sequence`  | `sequence_number` from `messages.jsonl`                                |

**Source-file attribution** (M5-2 is classic-only — `*Dispatch` wrappers in `adapter/sqs_keys_dispatch.go` route between classic and partitioned constructors and are NOT needed here; M5-3 will pick them up):

| Family  | Prefix constant     | File                          | Constructor      | File                          |
| ------- | ------------------- | ----------------------------- | ---------------- | ----------------------------- |
| `vis`   | `SqsMsgVisPrefix`   | `adapter/sqs_messages.go:30`  | `sqsMsgVisKey`   | `adapter/sqs_messages.go:158` |
| `byage` | `SqsMsgByAgePrefix` | `adapter/sqs_keys.go:38`      | `sqsMsgByAgeKey` | `adapter/sqs_keys.go:224`     |
| `dedup` | `SqsMsgDedupPrefix` | `adapter/sqs_keys.go:28`      | `sqsMsgDedupKey` | `adapter/sqs_keys.go:110`     |

M5-1 already mirrors the split layout — `internal/backup/sqs.go:30` re-declares `SQSMsgVisPrefix` alongside `SQSMsgDataPrefix` (both originally from `sqs_messages.go`). The M5-2 encoder MUST reuse the live constructors via duplicated helpers in `internal/backup`, exactly as M3b-3 duplicated GSI helpers, with a cross-check test asserting byte-identical output for a shared fixture.

## Decision gate: full reconstruction vs. lazy rebuild

The parent doc (§"Re-derivable indexes" → scope note) allows a per-adapter fallback. M5-2 resolves this for SQS:

**Chosen: full reconstruction of the three non-lock families.** Emit `vis` + `byage` + `dedup` inline during M5-1's existing per-message walk; do *not* emit `group` rows (see families table above for why — key presence alone signals a held lock). Rationale:

- **Dedup is correctness, not perf, *within the 5-minute window*.** Dedup rows gate FIFO **sends** (not receives — receive-side replay protection comes from data+vis): the live check at `adapter/sqs_fifo.go:111` treats a record as non-existent once `ExpiresAtMillis < now`. Because `ExpiresAtMillis = send_timestamp_millis + 5 min`, every dedup row in a backup older than 5 minutes restores already-expired and provides no protection. What's preserved is the narrow window of producers that were mid-dedup-window when the backup was taken — the same "fallback is not zero-cost transparency" framing the parent doc applies to dedup correctness, scoped to that window. Beyond it, dedup gating naturally rolls off, matching live-system behavior.
- **`byage` is required for retention.** Without it, the reaper (`sqs_reaper.go`) never finds the message and `MessageRetentionPeriod` is silently ignored — a restored queue grows unboundedly.
- **`vis` is required for receive.** ReceiveMessage scans `!sqs|msg|vis|` — a data row without a matching vis row is invisible forever.
- **Cost is bounded.** All three are `O(messages-in-dump)` walks over data already loaded by M5-1; no extra disk read, no cross-message coordination, no Raft round-trip (encoder is offline).

**Fallback (rejected): lazy rebuild.** Would require a post-restore admin `SCAN+REBUILD` pass on the target cluster before traffic resumes (parent doc requires this for any fallback). For SQS this would mean walking every `!sqs|msg|data|` record post-restore and writing the derived rows — i.e. doing M5-2's work anyway, just at restore time instead of encode time, with traffic gated until completion. No reason to defer the same work to a less-tested code path.

**Out of scope (deferred):**

- **In-flight receives.** A live cluster may have non-zero `VisibleAtMillis` for messages currently held by a consumer. The dump format (`messages.jsonl`) does not preserve this — parent §"SQS" mandates "vis zeroed by default." Anyone wanting at-the-instant transactional snapshots should use a different mechanism than logical encode/decode. Documented in the encoder header.
- **Tombstones (`!sqs|queue|tombstone|`).** These mark superseded-generation cleanup work for the reaper. A fresh restore has only the current generation = `sqsRestoreGeneration` (1), with no superseded gens to clean up. The encoder writes no tombstones; documented in the file header.
- **Group lock rows entirely.** See families table — emitting any row falsely blocks the group. The live receive path writes the lock on its own (`sqs_fifo.go:293`); a fresh restore correctly has no rows, and the first post-restore receive on each group acquires the lock cleanly via the normal path.

## Partitioned-FIFO follow-up

M5-2 originally scoped side-record derivation to classic queues. M5-3 later lifted the partitioned-FIFO gate with coordinated changes to BOTH M5-1 (decoder dump format + encoder iteration over partitions) AND M5-2 (partitioned-key constructors for vis/byage/dedup).

The implemented M5-3 slice adds these key shapes — sourced from `SqsPartitionedMsg{Vis,ByAge,Dedup}Prefix` + `sqsPartitionedMsg{...}Key` in `adapter/sqs_keys.go`:

| Family         | Key (partitioned, `partition_count > 1`)                                                                       |
| -------------- | -------------------------------------------------------------------------------------------------------------- |
| `vis`          | `!sqs\|msg\|vis\|p\|<queue-seg>\|<part-BE><gen-BE><visibleAt-BE><msgID-seg>`                                   |
| `byage`        | `!sqs\|msg\|byage\|p\|<queue-seg>\|<part-BE><gen-BE><sendTs-BE><msgID-seg>`                                    |
| `dedup` (FIFO) | `!sqs\|msg\|dedup\|p\|<queue-seg>\|<part-BE><gen-BE><group-seg><dedupID-seg>`                                  |
| `group`        | (not emitted — same rationale as classic)                                                                      |

The dedup-field derivation, group-row prohibition, and decision-gate rationale above all apply unchanged in the partitioned case.

## Files to add (M5-2 implementation slice)

```
internal/backup/encode_sqs_side.go        # derivation + key helpers (duplicated from adapter/sqs_keys.go)
internal/backup/encode_sqs_side_test.go   # byte-identical cross-check vs. live adapter + round-trip
```

Wire into `encode_sqs.go`'s existing `encodeQueueMessages` walk: after `addMessage(...)` emits `!sqs|msg|data|`, call `addSideRecords(...)` to emit the matching `vis` + `byage` (always) and `dedup` (FIFO + non-empty dedup-id only) rows in the same loop. **No `group` rows ever.** No new disk reads.

## Test plan

| Test                                                              | Verifies                                                                                                                                                                                |
| ----------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `TestSQSEncodeSideRecordsCrossCheckClassic`                       | For a fixture queue (1 partition, FIFO, varied dedup), each emitted row (vis/byage/dedup) is byte-identical AND byte-equal-value to what `adapter/sqs_fifo.go`'s send path would write for the same fixture. **Gold-standard.** The partitioned variant is covered by M5-3. |
| `TestSQSEncodeSideRecordsStandardQueueOmitsFIFOFamilies`          | Non-FIFO queue: vis + byage emitted; dedup **NOT** emitted (would be junk per `adapter/sqs_keys.go` semantics). group is never emitted, so trivially absent here too.                  |
| `TestSQSEncodeSideRecordsEmptyDedupOmitsDedupRow`                 | FIFO message with empty `message_dedup_id`: data + vis + byage emitted; dedup row **NOT** emitted.                                                                                      |
| `TestSQSEncodeSideRecordsNoGroupRows`                             | FIFO queue with N messages across M distinct groups: **zero** `!sqs\|msg\|group\|` rows emitted (regression pin for gemini critical #885 — any row falsely indicates a held lock and blocks every group post-restore). |
| `TestSQSEncodeFifoRestoreRoundTrip`                               | Build dump → encode → boot single-node cluster → ReceiveMessage returns every message in the original send order, exactly once. Re-sending with the same dedup-id is rejected. **The first ReceiveMessage per group must succeed** — verifies no spurious group lock was emitted. |
| `TestSQSEncodeReaperFindsRestoredMessage`                         | Set `MessageRetentionPeriod` short, restore old messages, advance HLC: reaper deletes them. Without `byage`, this would hang.                                                            |

`TestSQSEncodeSideRecordsCrossCheckClassic` is the §"Encoder cross-check" pattern the parent doc §"Per-adapter reverse encoders" mandates. The two restore round-trip tests pin the end-to-end correctness rationale for choosing full reconstruction over lazy.

## Self-review (5 passes) — implemented

1. **Data loss.** Full reconstruction (not fallback) eliminates the silent-redeliver + invisible-message classes called out above. Restore-time state matches what the live writer would have produced for the same send sequence.
2. **Concurrency / distributed failures.** Pure offline derivation; no Raft, no lock ordering. The restored cluster's first sends/receives go through the normal OCC path against the restored state.
3. **Performance.** Each side row is one `snapshotBuilder.Add` per data row (vis/byage), plus dedup-when-present. No additional disk reads. Snapshot size grows ~2-3× the message count, all small fixed-width keys. (Group rows are not emitted — see families table.)
4. **Data consistency.** Group rows MUST NOT be emitted at all: `loadFifoGroupLock` treats key presence as "lock held" with no graceful "empty owner = no lock" decode path, so any row would permanently block receives for the group (gemini critical #885 — pinned by `TestSQSEncodeSideRecordsNoGroupRows`). Dedup rows MUST be gated on non-empty `message_dedup_id`. FIFO-only families (dedup) MUST be gated on `meta.FIFO`; emitting them for standard queues poisons the keyspace. All three rules pinned by tests above.
5. **Test coverage.** One cross-check test for classic queues mirrors what M3b-3 did for DDB GSI; M5-3 adds the partitioned cross-check. The restore and negative tests pin the conditional-emission rules.

## Milestones / sequencing

M5-2 landed as a **single implementation slice** (doc + implementation + tests). The families are tightly coupled and must be reviewed together.

## Open questions

None at proposal time. Reviewers: please flag if there is any in-flight-state preservation requirement that "vis zeroed on restore" would break for known users, or any tombstone scenario the omission rationale above misses.
