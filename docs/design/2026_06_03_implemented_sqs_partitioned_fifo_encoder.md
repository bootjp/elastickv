# SQS partitioned-FIFO reverse encoder (Phase 0b M5-3) — implemented

**Status:** Implemented.
**Parent:** [`2026_05_25_implemented_snapshot_logical_encoder.md`](2026_05_25_implemented_snapshot_logical_encoder.md) — this lifts the §"SQS" decision gate that M5-1 (`PR #849`) and M5-2 (`PR #892`) deferred for `partition_count > 1`.
**Predecessor on disk:** M5-1 emits `!sqs|queue|meta|`, `!sqs|queue|gen|`, `!sqs|queue|seq|`, `!sqs|msg|data|` for classic queues. M5-2 adds `!sqs|msg|vis|`, `!sqs|msg|byage|`, `!sqs|msg|dedup|`. M5-3 removes the former partitioned-FIFO gate and emits the partitioned data/vis/byage/dedup key families.

## Implemented behavior

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

`sqsMessageRecord` (`internal/backup/sqs.go:233`) does NOT currently carry a `partition` field. M5-3 adds it as a **pointer** (`*uint32`) so the encoder can distinguish "partition was captured and is 0" from "partition field absent in the dump":

```go
// sqsMessageRecord adds (in M5-3):
Partition *uint32 `json:"partition,omitempty"`
```

**Why `*uint32` instead of `uint32`** (codex P1 / gemini #914 v2): partition 0 is a valid partition (it's the only valid partition for `partition_count == 1` classic queues AND a legitimate partition for FIFO-hash-routed messages in a multi-partition queue). With a plain `uint32 + omitempty`, a partition-0 message serializes as `{}` — identical on the wire to a legacy classic-queue message that was never partition-aware. A future operator who manually flips `_queue.json`'s `partition_count` from 1 to N and replays the old dump would have every message silently land in partition 0, breaking the FIFO group-hash routing invariant without an error. The pointer form makes "partition was captured" detectable.

`omitempty` stays on the JSON tag so the dump for a classic-queue message (no partition concept) omits the field entirely; legacy `format_version=1` dumps produced before M5-3 deserialize with `Partition == nil`, which the encoder then handles by branch:

| Manifest                                 | `Partition` nil?  | Encoder behavior                                                                                     |
| ---------------------------------------- | ----------------- | ---------------------------------------------------------------------------------------------------- |
| `partition_count == 1` (classic)         | nil               | Emit classic-shape keys; this is the legacy / forward-compatible path.                               |
| `partition_count == 1` (classic)         | non-nil + `== 0`  | Emit classic-shape keys; allowed (a freshly-decoded classic dump writes `0` explicitly under M5-3). |
| `partition_count == 1` (classic)         | non-nil + `!= 0`  | Fail-closed `ErrSQSInvalidMessage` (dump is internally inconsistent).                                |
| `partition_count > 1` (partitioned)      | **nil**           | **Fail-closed `ErrSQSEncodeMissingPartition`** — pre-M5-3 dumps cannot be replayed under M5-3's lifted gate; the operator must re-decode with an M5-3 decoder.   |
| `partition_count > 1` (partitioned)      | non-nil + in-range| Emit partitioned-shape keys with `*rec.Partition`.                                                   |
| `partition_count > 1` (partitioned)      | non-nil + out-of-range | Fail-closed `ErrSQSEncodeOutOfRangePartition`.                                                  |

New sentinel `ErrSQSEncodeMissingPartition` (alongside `ErrSQSEncodeOutOfRangePartition` from v1) covers the legacy-dump-under-lifted-gate case.

**Backward compat:** a dump written before M5-3 (no `partition` field anywhere) for a classic queue (`partition_count == 1`) round-trips through the M5-3 encoder unchanged — `*uint32` decodes as `nil`, the classic branch takes the `nil → classic-shape keys` path.

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

`decodeSQSMessageValue` then receives the parsed `partition` from its caller (`HandleMessageData`, `internal/backup/sqs.go:341`), populates `sqsMessageRecord.Partition` as a `*uint32`, and `writeMessagesJSONL` serializes it under the new JSON field. The live message value itself never contained the partition (the value pair is `<message-id, message-record>`, and partition routing lives in the KEY); M5-3 closes that gap by passing through the key-derived partition rather than re-deriving it on the decoder side.

**Specific call-site updates** (claude v2 review caught these — make the impl PR mechanically faithful):

- `HandleMessageData` (`sqs.go:341`): receive `(encQueue, partition uint32, isPartitioned bool, err)`, decode the value bytes, then set `rec.Partition = &partition` ONLY when `isPartitioned` is true. Classic-key call site keeps `rec.Partition = nil` so the no-partition-concept case round-trips as a legacy dump.
- `decodeSQSMessageValue` (`sqs.go:719`) keeps its current `(value []byte) (sqsMessageRecord, error)` signature — note the return is a value, not a pointer (claude v3 v914 caught the earlier doc typo). Partition wiring happens at the call site, not inside the decoder, because the decoder never sees the key.
- `parseSQSGenericKey` (`sqs.go:571`) — wraps `parseSQSPartitionedQueueAndTrailer` and is called by `HandleSideRecord` for `vis`/`byage`/`dedup` value handling (`sqs.go:367`). Side-record handlers route to `_internals/` by queue and don't need partition, so the new partition return from `parseSQSPartitionedQueueAndTrailer` is discarded inside `parseSQSGenericKey`. The signature change is mechanical; impl PR must touch the wrapper to compile.

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
3. **Group-by-partition before emit.** Sort messages by `(partition, send_timestamp_millis, sequence_number, message_id)` so per-partition order is stable across runs — required for byte-identical re-encodes. `sequence_number` is the definitive tiebreaker between messages that share a send timestamp (burst case); `message_id` is the last-resort tiebreaker. Gemini suggested this in the v2 review; claude v3 corrected the wording — the live `sortMessagesForEmit` (`sqs.go:815`) is a **3-field** tuple `(send_timestamp_millis, sequence_number, message_id)`, and the partitioned path prepends `partition` as the leading key.

4. **Per-message dispatch.** `addMessage` (`encode_sqs.go:241`) currently calls `sqsMsgDataKeyBytes(queueName, sqsRestoreGeneration, rec.MessageID)` at line 268. M5-3 threads a `partition *uint32` parameter through `addMessage`: when non-nil, it calls a new `sqsPartitionedMsgDataKeyBytes(queueName, *partition, sqsRestoreGeneration, rec.MessageID)`; when nil, the existing classic constructor. Adding the parameter (rather than a peer `addPartitionedMessage`) keeps the side-record dispatch in lockstep — `addSideRecords` runs on the same record and needs the same partition info. Claude v3 noted that the doc previously left this implicit.

Plus three additions to `internal/backup/encode_sqs_side.go` (the M5-2 file):

1. Partitioned `vis` constructor → `!sqs|msg|vis|p|...`.
2. Partitioned `byage` constructor → `!sqs|msg|byage|p|...`.
3. Partitioned `dedup` constructor → `!sqs|msg|dedup|p|...`. Note the partitioned shape adds a `<group-seg>` segment before `<dedupID-seg>` (per `adapter/sqs_keys.go:sqsPartitionedMsgDedupKey` line N+50ish); the classic shape has only `<dedupID-seg>`. This means `message_group_id` is now load-bearing for dedup-row construction on FIFO partitioned queues — but the existing `messages.jsonl` already carries it as `message_group_id`.

`addSideRecords` (the per-message side-record dispatcher in `encode_sqs_side.go`) gains the same `partition *uint32` parameter as `addMessage` so the partitioned constructors can be selected per message. Branching by `partition != nil` (rather than re-reading `meta.PartitionCount`) keeps the call site one decision deep and makes mixed-mode bugs impossible — a classic-mode dump can never accidentally call a partitioned constructor because `rec.Partition` is enforced nil for `PartitionCount == 1` (the new fail-closed gate in §"Validation invariants" forbids any other shape). Claude v3 noted that v3 didn't spell this out.

## Validation invariants (fail-closed)

The full decision matrix lives in the table under §"Dump-format change." The encoder fails closed with these sentinels:

- `meta.PartitionCount > 1` AND `rec.Partition == nil` (pre-M5-3 dump under lifted gate, or M5-3 decoder bug) → **new sentinel `ErrSQSEncodeMissingPartition`**. The operator must re-decode with an M5-3 decoder; replaying a legacy dump into a partitioned queue would silently move every message to partition 0 (codex P1 / gemini #914 v2).
- `meta.PartitionCount > 1` AND `*rec.Partition >= meta.PartitionCount` → **new sentinel `ErrSQSEncodeOutOfRangePartition`** (out-of-range partition number, dump is malformed).
- `meta.PartitionCount == 1` (classic) AND `rec.Partition != nil && *rec.Partition != 0` → reuses **`ErrSQSInvalidMessage`** (dump is internally inconsistent).
- `meta.PartitionCount > 1` AND `meta.FifoThroughputLimit == "perQueue"` AND `*rec.Partition != 0` → **new sentinel `ErrSQSEncodePartitionRoutingMismatch`**. The live router (`adapter/sqs_partitioning.go:71-72` in `partitionFor`) forces every group to partition 0 whenever `FifoThroughputLimit == "perQueue"`, regardless of `PartitionCount`; ReceiveMessage only scans the partition-0 lane (`adapter/sqs_keys_dispatch.go:125-126`). Accepting `*rec.Partition >= 1` for a `perQueue` queue would restore messages onto `|p|1|...` lanes the live receive fan-out never visits — silent data loss on first read. Codex P2 v914 v4 caught this gap. Pinned by `TestSQSEncodeRejectsNonzeroPartitionOnPerQueueHTFIFO`.

**All four gates above use raw `meta.PartitionCount > 1` as the partitioned-queue predicate.** Codex P2 v914 v7 caught a subtle but data-loss-prone error in v6, which proposed switching the missing-partition / out-of-range gates to `effectivePartitionCount`: that would loosen the missing-partition gate so a perQueue dump with `Partition == nil` would slip past validation, then `addMessage`'s `partition != nil` branch would emit classic-shape `!sqs|msg|data|<id>` keys for a queue whose live readers scan only the partitioned `!sqs|msg|data|p|0|<id>` keyspace (key shape is selected from raw `PartitionCount > 1` in `adapter/sqs_keys_dispatch.go`). Result: restored messages would be invisible. The `effectivePartitionCount(meta) uint32` helper is still useful for diagnostics and for the ReceiveMessage scan fan-out, but it MUST NOT replace the raw predicate in encoder validation gates.

**Implementation note — define a NEW copy in `internal/backup/`.** An identically-named function already exists in `adapter/sqs_keys_dispatch.go` (operating on `*adapter.sqsQueueMeta`, the unexported live struct). The backup package MUST NOT import it — `internal/backup/` cannot pull from `adapter/` (M3b-3 circular-dependency pattern; same reason `sqsFifoDedupWindowMillis` is mirrored in `encode_sqs_side.go:15` rather than imported). The new copy operates on `sqsQueueMetaPublic` (the public/dump struct, `internal/backup/sqs.go:148`). Note: `encode_sqs.go:66` defines a different struct, `sqsStoredQueueMeta`, used for the `!sqs|queue|meta|` value — not the dump-side struct (claude v914 v6 caught the earlier file-path error). The `"perQueue"` string constant likewise must be mirrored — the adapter's `htfifoThroughputPerQueue = "perQueue"` is unexported and unimportable. Claude v914 v5 caught this naming-collision risk.

**Key-shape vs. validation distinction.** Both the "Branch on `PartitionCount`" key-shape choice in §"Encoder lift" AND the validation gates above use the raw `meta.PartitionCount > 1` predicate, never `effectivePartitionCount`. A `perQueue` queue with `PartitionCount=2` writes partitioned-shape keys (`|p|` prefix, all messages with partition=0) — the live adapter does this because `meta.PartitionCount > 1` selects the partitioned keyspace at dispatch, and `partitionFor` separately collapses every group to partition 0. Restoring such a dump under classic keys would silently flip the queue's wire shape, and accepting a `Partition == nil` message would similarly cause `addMessage` to emit classic keys via the `partition != nil` dispatch — making restored rows invisible on first read.

Per-partition-record-count integrity (detecting "all messages in partition 0, none in partition 1..N-1") cannot be checked from the dump alone — the live partition assignment is `partitionFor(messageGroupID) → uint32` and the encoder doesn't run that hash. The self-test loop catches it because a re-decode would write the messages into the same partitions the encoder used — but only if the encoder respects the per-message partition, which is what the new fail-closed gates above ensure.

## Decision gate: full reconstruction vs lazy rebuild (carry-over from M5-2)

M5-2's "full reconstruction" gate applies unchanged to M5-3 partitioned queues. The recommendation is the same — emit `vis` + `byage` + `dedup` inline during the per-message walk; do NOT emit `group` rows. Cost remains O(messages-in-dump); no extra disk read or Raft round-trip.

## Out of scope (deferred)

- **Cross-partition rebalancing.** A partition count change between dump and restore would invalidate every message's partition assignment. M5-3 forbids this — `meta.PartitionCount` must match the input dump exactly. A future milestone can add a `--repartition` flag that re-hashes message IDs into the target partition count.
- **In-flight cross-partition receives.** Same `vis`-is-zero rule as M5-2; restored messages are visible. Documented in the encoder header.
- **Group lock rows.** Same prohibition as M5-2 — emitting any row falsely blocks the group permanently. M5-3 inherits the rule.

## Files to add / modify (M5-3 implementation slice)

```
internal/backup/sqs.go                       # sqsMessageRecord +Partition *uint32; parseSQSMessageDataKey + parseSQSPartitionedQueueAndTrailer new (partition uint32) return values; HandleMessageData wires rec.Partition = &partition only when isPartitioned; parseSQSGenericKey wrapper discards the new return (claude v3 v914)
internal/backup/encode_sqs.go                # drop ErrSQSEncodeUnsupportedPartitioned; addMessage takes partition *uint32 + branches between sqsMsgDataKeyBytes and the new sqsPartitionedMsgDataKeyBytes
internal/backup/encode_sqs_side.go           # addSideRecords takes partition *uint32; add partitioned vis/byage/dedup constructors + emit
internal/backup/encode_sqs_test.go           # round-trip partitioned-FIFO fixture (2 partitions × 3 messages)
internal/backup/encode_sqs_side_test.go      # cross-check partitioned vis/byage/dedup vs live constructors
internal/backup/sqs_test.go                  # decoder round-trip with partition field populated
```

## Milestones (within M5-3)

The slice landed as a single implementation unit: the decoder format change and encoder partition branch are tightly coupled. A partial landing would either reject all M5-3 dumps at the new encoder or break old encoders against new dumps.

## Test plan

| Test                                                              | Verifies                                                                                                              |
| ----------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `TestSQSEncodePartitionedQueueRoundTrip`                          | `partition_count=2`, 3 messages across both partitions → all data + side records emitted with `|p|` prefix             |
| `TestSQSEncodePartitionedDedupBuildsGroupSegment`                 | FIFO partitioned dedup row's `<group-seg>` matches `message_group_id` from `messages.jsonl`                            |
| `TestSQSEncodeRejectsMissingPartitionOnPartitionedQueue`          | `partition_count=2`, message with `Partition == nil` (legacy dump shape) → `ErrSQSEncodeMissingPartition`              |
| `TestSQSEncodeRejectsOutOfRangePartition`                         | message with `*Partition >= meta.PartitionCount` → `ErrSQSEncodeOutOfRangePartition`                                   |
| `TestSQSEncodeRejectsNonZeroPartitionOnClassicQueue`              | `PartitionCount=1` but message has `Partition=2` → `ErrSQSInvalidMessage`                                              |
| `TestSQSEncodeRejectsNonzeroPartitionOnPerQueueHTFIFO`            | `PartitionCount=2` + `FifoThroughputLimit="perQueue"` + message has `*Partition=1` → `ErrSQSEncodePartitionRoutingMismatch` (codex P2 v914 v4) |
| `TestSQSEncodeLegacyDumpsWithoutPartitionStillRoundTrip`          | a pre-M5-3 `messages.jsonl` with no `partition` field round-trips through M5-3 encoder unchanged                       |
| `TestSQSEncodePartitionedSideRecordsByteCrossCheckLiveAdapter`    | M5-2-style cross-check: partitioned `vis|p|` / `byage|p|` / `dedup|p|` bytes equal `sqsPartitionedMsg{...}Key(...)`    |

## References

- Parent: `2026_05_25_implemented_snapshot_logical_encoder.md` §"SQS"
- M5-2 doc (decision gate template, classic side records): `2026_05_30_implemented_sqs_side_record_derivation.md`
- M5-1 PR: #849
- M5-2 PR: #892
- Live partitioned constructors: `adapter/sqs_keys.go:337+` (`sqsPartitionedMsgDataKey` and siblings)
- Existing partitioned dispatch (cross-classic-partitioned routing): `adapter/sqs_keys_dispatch.go`
- Former gate removed by M5-3: `ErrSQSEncodeUnsupportedPartitioned`
