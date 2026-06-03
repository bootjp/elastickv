# SQS partitioned-FIFO reverse encoder (Phase 0b M5-3) — proposed

**Status:** Proposed (no implementation yet).
**Parent:** [`2026_05_25_partial_snapshot_logical_encoder.md`](2026_05_25_partial_snapshot_logical_encoder.md) — this lifts the §"SQS" decision gate that M5-1 (`PR #849`) and M5-2 (`PR #892`) deferred for `partition_count > 1`.
**Predecessor on disk:** M5-1 emits `!sqs|queue|meta|`, `!sqs|queue|gen|`, `!sqs|queue|seq|`, `!sqs|msg|data|` for classic queues. M5-2 adds `!sqs|msg|vis|`, `!sqs|msg|byage|`, `!sqs|msg|dedup|`. Both reject `PartitionCount > 1` via `ErrSQSEncodeUnsupportedPartitioned` (`internal/backup/encode_sqs.go:162`); the M5-2 doc explicitly defers partitioned-FIFO support to "M5-3."

## What needs to land

For every queue with `partition_count > 1` in `_queue.json`, the encoder must read each message's partition assignment from the dump and emit the **partitioned** key family instead of the classic family:

| Family   | Classic shape (M5-1 / M5-2)                                                   | Partitioned shape (M5-3)                                                                              |
| -------- | ----------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `data`   | `!sqs\|msg\|data\|<queue-seg><gen-BE><msgID-seg>`                             | `!sqs\|msg\|data\|p\|<queue-seg>\|<part-BE><gen-BE><msgID-seg>`                                       |
| `vis`    | `!sqs\|msg\|vis\|<queue-seg><gen-BE><visibleAt-BE><msgID-seg>`                | `!sqs\|msg\|vis\|p\|<queue-seg>\|<part-BE><gen-BE><visibleAt-BE><msgID-seg>`                          |
| `byage`  | `!sqs\|msg\|byage\|<queue-seg><gen-BE><sendTs-BE><msgID-seg>`                 | `!sqs\|msg\|byage\|p\|<queue-seg>\|<part-BE><gen-BE><sendTs-BE><msgID-seg>`                           |
| `dedup`  | `!sqs\|msg\|dedup\|<queue-seg><gen-BE><dedupID-seg>`                          | `!sqs\|msg\|dedup\|p\|<queue-seg>\|<part-BE><gen-BE><group-seg>\|<dedupID-seg>`                       |
| `group`  | (not emitted — see M5-2 §"families table" rationale)                          | (not emitted — same rationale)                                                                        |

> **Notation.** Pipe characters inside `<seg>…<seg>` are visual separators, not literal bytes; the only literal `|` bytes are (1) inside the family prefix, (2) the `|p|` partitioned-key discriminator, (3) the `sqsPartitionedQueueTerminator='|'` byte appended after `<queue-seg>` in every partitioned key (see `adapter/sqs_keys.go:82`), and (4) for the partitioned dedup family ONLY, an additional `sqsPartitionedQueueTerminator` between `<group-seg>` and `<dedupID-seg>` — the live `sqsPartitionedMsgDedupKey` (`adapter/sqs_keys.go:389`) emits this delimiter because `encodeSQSSegment` uses `base64.RawURLEncoding` (no padding) and back-to-back raw-base64 segments are ambiguous, so distinct `(groupID, dedupID)` pairs could collapse onto the same key (CodeRabbit major PR #732 round 6 + codex P2 found this missing from v1 of this doc). Constants in `adapter/sqs_keys.go`: `SqsPartitionedMsgDataPrefix` (line 41), `SqsPartitionedMsgVisPrefix`, `SqsPartitionedMsgByAgePrefix`, `SqsPartitionedMsgDedupPrefix`. Constructors: `sqsPartitionedMsgDataKey` (line 339), and the `*Vis,ByAge,Dedup,GroupKey` siblings in the same file.

## Dump-format change (M5-1 decoder + encoder, NEW)

`sqsMessageRecord` (`internal/backup/sqs.go:233`) does NOT currently carry a `partition` field. M5-3 adds it, plus a corresponding writer-side population in the decoder for `partition_count > 1` queues:

```go
// sqsMessageRecord adds (in M5-3):
Partition uint32 `json:"partition,omitempty"`
```

`omitempty` is load-bearing — every classic-queue dump produced before M5-3 lands has no `partition` field, and the encoder MUST default to `partition=0` (the only valid value for `partition_count == 1`). New partitioned-queue dumps populate `partition` from the live key's partition trailer.

**Backward compat:** a dump written before M5-3 (no `partition` field anywhere) round-trips through the M5-3 encoder unchanged — `omitempty` handles the read side, `meta.PartitionCount <= 1` already short-circuits the partitioned emit path.

**Forward compat:** a dump written by an M5-3 decoder, then read by a pre-M5-3 encoder, would silently lose the `partition` field. The encoder is offline so cross-version replays are an operator-driven scenario; the parent-doc convention is to surface this via a fail-closed format-version bump if it ever matters. M5-3 keeps `format_version=1` because adding an optional field is backward compatible by JSON convention; if a later milestone needs to break compat it can bump the version then.

## Decoder lift (M5-1 follow-up)

The current key parser only returns the encoded queue segment:

```go
// internal/backup/sqs.go:534
func parseSQSMessageDataKey(key []byte) (string, error)
// internal/backup/sqs.go:611
func parseSQSPartitionedQueueAndTrailer(rest string, hasMsgID bool, originalKey []byte) (string, error)
```

The partition `uint32` is parsed inside `parseSQSPartitionedQueueAndTrailer` (the trailer carries `<partition u32><gen u64>` immediately after the queue segment + terminator) but is currently DISCARDED — codex P2 found this in the v1 review. M5-3 MUST extend both signatures to return the partition:

```go
// internal/backup/sqs.go (M5-3):
func parseSQSMessageDataKey(key []byte) (encQueue string, partition uint32, isPartitioned bool, err error)
func parseSQSPartitionedQueueAndTrailer(rest string, hasMsgID bool, originalKey []byte) (encQueue string, partition uint32, err error)
```

(`isPartitioned` is needed because `parseSQSMessageDataKey` is also the classic dispatcher — caller can branch on it without re-checking the `|p|` discriminator. For the classic path `partition` is always 0.)

`decodeSQSMessageValue` then receives the parsed `partition` from its caller (the FSM-apply / dump-walker), populates `sqsMessageRecord.Partition`, and `writeMessagesJSONL` serializes it under the new JSON field. The live message value itself never contained the partition (the value pair is `<message-id, message-record>`, and partition routing lives in the KEY); M5-3 closes that gap by passing through the key-derived partition rather than re-deriving it on the decoder side.

**Per-partition vs single-file layout.** Two candidate disk layouts:

| Option              | Layout                                                                                                                | Pros                                                                                                  | Cons                                                                                                |
| ------------------- | --------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| **A (recommended)** | Single `messages.jsonl` per queue. Each line includes `"partition": <N>`.                                             | Minimal change to decoder; mirrors classic shape. Single file → atomic write + fsync stays simple.   | Encoder must group-by-partition in memory (already loads the full file).                            |
| B                   | Per-partition directory: `sqs/<queueDir>/partitions/<part>/messages.jsonl`.                                           | Encoder can stream a partition at a time; smaller per-file working set on huge queues.                | Decoder needs N writers, N fsyncs, partition-dir management. Breaks symmetry with classic dumps.    |

Option A is recommended for parity with the classic layout and to avoid a decoder rewrite. The encoder's `encodeQueueMessages` is already in-memory-buffered (loads full file → sorts → emits) so per-partition streaming wouldn't measurably help.

## Encoder lift (M5-1 + M5-2)

Three changes to `internal/backup/encode_sqs.go`:

1. **Drop `ErrSQSEncodeUnsupportedPartitioned`.** Remove the `meta.PartitionCount > 1` gate at line 162.
2. **Branch on `PartitionCount`.** When `> 1`, use partitioned key constructors (duplicated from `adapter/sqs_keys.go` following the established M3b-3 GSI pattern). When `<= 1`, classic constructors as today.
3. **Group-by-partition before emit.** Sort messages by `(partition, send_timestamp_millis, message_id)` so per-partition order is stable across runs — required for byte-identical re-encodes.

Plus three additions to `internal/backup/encode_sqs_side.go` (the M5-2 file):

1. Partitioned `vis` constructor → `!sqs|msg|vis|p|...`.
2. Partitioned `byage` constructor → `!sqs|msg|byage|p|...`.
3. Partitioned `dedup` constructor → `!sqs|msg|dedup|p|...`. Note the partitioned shape adds a `<group-seg>` segment before `<dedupID-seg>` (per `adapter/sqs_keys.go:sqsPartitionedMsgDedupKey` line N+50ish); the classic shape has only `<dedupID-seg>`. This means `message_group_id` is now load-bearing for dedup-row construction on FIFO partitioned queues — but the existing `messages.jsonl` already carries it as `message_group_id`.

## Validation invariants (fail-closed)

The encoder fails closed with the existing per-adapter sentinels on:

- `meta.PartitionCount > 1` AND any message has `Partition == 0` AND the dump's record count for partition 0 doesn't match the live partition assignment. (Detectable only if the encoder can recompute the partition; deferred to a self-test invariant rather than a runtime check.)
- `meta.PartitionCount > 1` AND any message's `Partition >= meta.PartitionCount` — out-of-range partition number, dump is malformed. New sentinel `ErrSQSEncodeOutOfRangePartition`.
- `meta.PartitionCount == 1` (classic) AND any message has `Partition != 0` — dump is internally inconsistent. Reuses `ErrSQSInvalidMessage`.

## Decision gate: full reconstruction vs lazy rebuild (carry-over from M5-2)

M5-2's "full reconstruction" gate applies unchanged to M5-3 partitioned queues. The recommendation is the same — emit `vis` + `byage` + `dedup` inline during the per-message walk; do NOT emit `group` rows. Cost remains O(messages-in-dump); no extra disk read or Raft round-trip.

## Out of scope (deferred)

- **Cross-partition rebalancing.** A partition count change between dump and restore would invalidate every message's partition assignment. M5-3 forbids this — `meta.PartitionCount` must match the input dump exactly. A future milestone can add a `--repartition` flag that re-hashes message IDs into the target partition count.
- **In-flight cross-partition receives.** Same `vis`-is-zero rule as M5-2; restored messages are visible. Documented in the encoder header.
- **Group lock rows.** Same prohibition as M5-2 — emitting any row falsely blocks the group permanently. M5-3 inherits the rule.

## Files to add / modify (M5-3 implementation slice)

```
internal/backup/sqs.go                       # sqsMessageRecord +Partition; decodeSQSMessageValue plumbs partition
internal/backup/encode_sqs.go                # drop ErrSQSEncodeUnsupportedPartitioned; branch on PartitionCount
internal/backup/encode_sqs_side.go           # add partitioned vis/byage/dedup constructors + emit
internal/backup/encode_sqs_test.go           # round-trip partitioned-FIFO fixture (2 partitions × 3 messages)
internal/backup/encode_sqs_side_test.go      # cross-check partitioned vis/byage/dedup vs live constructors
internal/backup/sqs_test.go                  # decoder round-trip with partition field populated
```

## Milestones (within M5-3)

The slice ships as a single PR — the decoder format change and encoder partition branch are tightly coupled (a partial landing would either reject all M5-3 dumps at the new encoder or break old encoders against new dumps).

## Test plan

| Test                                                              | Verifies                                                                                                              |
| ----------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `TestSQSEncodePartitionedQueueRoundTrip`                          | `partition_count=2`, 3 messages across both partitions → all data + side records emitted with `|p|` prefix             |
| `TestSQSEncodePartitionedDedupBuildsGroupSegment`                 | FIFO partitioned dedup row's `<group-seg>` matches `message_group_id` from `messages.jsonl`                            |
| `TestSQSEncodeRejectsOutOfRangePartition`                         | message with `Partition >= meta.PartitionCount` → `ErrSQSEncodeOutOfRangePartition`                                    |
| `TestSQSEncodeRejectsNonZeroPartitionOnClassicQueue`              | `PartitionCount=1` but message has `Partition=2` → `ErrSQSInvalidMessage`                                              |
| `TestSQSEncodeLegacyDumpsWithoutPartitionStillRoundTrip`          | a pre-M5-3 `messages.jsonl` with no `partition` field round-trips through M5-3 encoder unchanged                       |
| `TestSQSEncodePartitionedSideRecordsByteCrossCheckLiveAdapter`    | M5-2-style cross-check: partitioned `vis|p|` / `byage|p|` / `dedup|p|` bytes equal `sqsPartitionedMsg{...}Key(...)`    |

## References

- Parent: `2026_05_25_partial_snapshot_logical_encoder.md` §"SQS"
- M5-2 doc (decision gate template, classic side records): `2026_05_30_proposed_sqs_side_record_derivation.md`
- M5-1 PR: #849
- M5-2 PR: #892
- Live partitioned constructors: `adapter/sqs_keys.go:337+` (`sqsPartitionedMsgDataKey` and siblings)
- Existing partitioned dispatch (cross-classic-partitioned routing): `adapter/sqs_keys_dispatch.go`
- Existing gate in encoder: `internal/backup/encode_sqs.go:162` (`ErrSQSEncodeUnsupportedPartitioned`)
