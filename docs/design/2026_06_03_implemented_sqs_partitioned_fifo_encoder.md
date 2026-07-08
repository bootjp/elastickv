# SQS partitioned-FIFO reverse encoder (Phase 0b M5-3) — implemented

**Status:** Implemented.
**Parent:** [`2026_05_25_implemented_snapshot_logical_encoder.md`](2026_05_25_implemented_snapshot_logical_encoder.md) — this lifts the §"SQS" decision gate that M5-1 (`PR #849`) and M5-2 (`PR #892`) deferred for `partition_count > 1`.
**Predecessor on disk:** M5-1 emits `!sqs|queue|meta|`, `!sqs|queue|gen|`, `!sqs|queue|seq|`, `!sqs|msg|data|` for classic queues. M5-2 adds `!sqs|msg|vis|`, `!sqs|msg|byage|`, `!sqs|msg|dedup|`. The M5-3 implementation removes the earlier `PartitionCount > 1` rejection and emits the partitioned key families below.

## Implemented behavior

For every FIFO queue with an accepted live-equivalent `partition_count > 1` in `_queue.json`, the encoder reads each message's partition assignment from the dump and emits the **partitioned** key family instead of the classic family. Accepted partition counts are powers of two in `[2,32]`, matching the live HT-FIFO structural cap. The key-shape predicate is the raw queue-level `PartitionCount > 1`; the per-message `partition` value is validation and routing input after that queue-level decision.

| Family   | Classic shape (M5-1 / M5-2)                                                   | Partitioned shape (M5-3)                                                                              |
| -------- | ----------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `data`   | `!sqs\|msg\|data\|<queue-seg><gen-BE><msgID-seg>`                             | `!sqs\|msg\|data\|p\|<queue-seg>\|<part-BE><gen-BE><msgID-seg>`                                       |
| `vis`    | `!sqs\|msg\|vis\|<queue-seg><gen-BE><visibleAt-BE><msgID-seg>`                | `!sqs\|msg\|vis\|p\|<queue-seg>\|<part-BE><gen-BE><visibleAt-BE><msgID-seg>`                          |
| `byage`  | `!sqs\|msg\|byage\|<queue-seg><gen-BE><sendTs-BE><msgID-seg>`                 | `!sqs\|msg\|byage\|p\|<queue-seg>\|<part-BE><gen-BE><sendTs-BE><msgID-seg>`                           |
| `dedup`  | `!sqs\|msg\|dedup\|<queue-seg><gen-BE><dedupID-seg>`                          | `!sqs\|msg\|dedup\|p\|<queue-seg>\|<part-BE><gen-BE><group-seg>\|<dedupID-seg>`                       |
| `group`  | (not emitted — see M5-2 §"families table" rationale)                          | (not emitted — same rationale)                                                                        |

> **Notation.** Pipe characters inside `<seg>…<seg>` are visual separators, not literal bytes; the only literal `|` bytes are (1) inside the family prefix, (2) the `|p|` partitioned-key discriminator, (3) the `sqsPartitionedQueueTerminator='|'` byte appended after `<queue-seg>` in every partitioned key, and (4) for the partitioned dedup family ONLY, an additional `sqsPartitionedQueueTerminator` between `<group-seg>` and `<dedupID-seg>`. The live `sqsPartitionedMsgDedupKey` in `adapter/sqs_keys.go` emits this delimiter because `encodeSQSSegment` uses `base64.RawURLEncoding` (no padding) and back-to-back raw-base64 segments are ambiguous, so distinct `(groupID, dedupID)` pairs could collapse onto the same key. The partitioned prefix constants are `SqsPartitionedMsgDataPrefix`, `SqsPartitionedMsgVisPrefix`, `SqsPartitionedMsgByAgePrefix`, and `SqsPartitionedMsgDedupPrefix`; the constructor set is `sqsPartitionedMsgDataKey` and the `*Vis`, `*ByAge`, `*Dedup`, and `*GroupKey` siblings in the same file.

## Dump format

`sqsMessageRecord` in `internal/backup/sqs.go` carries a `partition` field as a **pointer** (`*uint32`) so the encoder can distinguish "partition was captured and is 0" from "partition field absent in the dump":

```go
// sqsMessageRecord carries:
Partition *uint32 `json:"partition,omitempty"`
```

**Why `*uint32` instead of `uint32`:** partition 0 is a valid partition (it's the only valid partition for `partition_count == 1` classic queues AND a legitimate partition for FIFO-hash-routed messages in a multi-partition queue). With a plain `uint32 + omitempty`, a partition-0 message serializes as `{}` — identical on the wire to a legacy classic-queue message that was never partition-aware. A future operator who manually flips `_queue.json`'s `partition_count` from 1 to N and replays the old dump would have every message silently land in partition 0, breaking the FIFO group-hash routing invariant without an error. The pointer form makes "partition was captured" detectable.

`omitempty` stays on the JSON tag so the dump for a classic-queue message (no partition concept) omits the field entirely; legacy `format_version=1` dumps produced before M5-3 deserialize with `Partition == nil`, which the encoder then handles by branch:

| Manifest                                 | `Partition` nil?  | Encoder behavior                                                                                     |
| ---------------------------------------- | ----------------- | ---------------------------------------------------------------------------------------------------- |
| `partition_count == 1` (classic)         | nil               | Emit classic-shape keys; this is the legacy / forward-compatible path.                               |
| `partition_count == 1` (classic)         | non-nil + `== 0`  | Emit classic-shape keys; allowed for hand-authored or forward-compatible dumps that explicitly carry `0`. |
| `partition_count == 1` (classic)         | non-nil + `!= 0`  | Fail-closed `ErrSQSEncodeInvalidMessage` (dump is internally inconsistent).                          |
| `partition_count > 1` (partitioned)      | **nil**           | **Fail-closed `ErrSQSEncodeMissingPartition`** — pre-M5-3 dumps cannot be replayed under M5-3's lifted gate; the operator must re-decode with an M5-3 decoder.   |
| `partition_count > 1` (partitioned)      | non-nil + in-range| Emit partitioned-shape keys with `*rec.Partition`.                                                   |
| `partition_count > 1` (partitioned)      | non-nil + out-of-range | Fail-closed `ErrSQSEncodeOutOfRangePartition`.                                                  |

New sentinel `ErrSQSEncodeMissingPartition` (alongside `ErrSQSEncodeOutOfRangePartition` from v1) covers the legacy-dump-under-lifted-gate case.

**Backward compat:** a dump written before M5-3 (no `partition` field anywhere) for a classic queue (`partition_count == 1`) round-trips through the M5-3 encoder unchanged — `*uint32` decodes as `nil`, the classic branch takes the `nil → classic-shape keys` path.

**Forward compat:** a dump written by an M5-3 decoder, then read by a pre-M5-3 encoder, would silently lose the `partition` field. The encoder is offline so cross-version replays are an operator-driven scenario; the parent-doc convention is to surface this via a fail-closed format-version bump if it ever matters. M5-3 keeps `format_version=1` because adding an optional field is backward compatible by JSON convention; if a later milestone needs to break compat it can bump the version then.

## Decoder behavior

The key parser returns the encoded queue segment, the parsed partition, and a queue-shape flag:

```go
// internal/backup/sqs.go
func parseSQSMessageDataKey(key []byte) (encQueue string, partition uint32, isPartitioned bool, err error)
func parseSQSPartitionedQueueAndTrailer(rest string, hasMsgID bool, originalKey []byte) (encQueue string, partition uint32, err error)
```

`isPartitioned` is needed because `parseSQSMessageDataKey` is also the classic dispatcher — callers branch on it without re-checking the `|p|` discriminator. For the classic path `partition` is always 0.

`decodeSQSMessageValue` then receives the parsed `partition` from its caller (`HandleMessageData` in `internal/backup/sqs.go`), populates `sqsMessageRecord.Partition` as a `*uint32`, and `writeMessagesJSONL` serializes it under the new JSON field. The live message value itself never contained the partition (the value pair is `<message-id, message-record>`, and partition routing lives in the KEY); M5-3 closes that gap by passing through the key-derived partition rather than re-deriving it on the decoder side.

**Specific call-site behavior:**

- `HandleMessageData` in `internal/backup/sqs.go`: receive `(encQueue, partition uint32, isPartitioned bool, err)`, decode the value bytes, then set `rec.Partition = &partition` ONLY when `isPartitioned` is true. Classic-key call site keeps `rec.Partition = nil` so the no-partition-concept case round-trips as a legacy dump.
- `decodeSQSMessageValue` in `internal/backup/sqs.go` keeps its `(value []byte) (sqsMessageRecord, error)` signature. Partition wiring happens at the call site, not inside the decoder, because the decoder never sees the key.
- `parseSQSGenericKey` in `internal/backup/sqs.go` wraps `parseSQSPartitionedQueueAndTrailer` and is called by `HandleSideRecord` for `vis`/`byage`/`dedup` value handling. Side-record handlers route to `_internals/` by queue and don't need partition, so the new partition return from `parseSQSPartitionedQueueAndTrailer` is discarded inside `parseSQSGenericKey`. The signature change is mechanical and compiled in the implemented path.

**Per-partition vs single-file layout.** Two candidate disk layouts:

| Option              | Layout                                                                                                                | Pros                                                                                                  | Cons                                                                                                |
| ------------------- | --------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| **A (recommended)** | Single `messages.jsonl` per queue. Each line includes `"partition": <N>`.                                             | Minimal change to decoder; mirrors classic shape. Single file → atomic write + fsync stays simple.   | Encoder must group-by-partition in memory (already loads the full file).                            |
| B                   | Per-partition directory: `sqs/<queueDir>/partitions/<part>/messages.jsonl`.                                           | Encoder can stream a partition at a time; smaller per-file working set on huge queues.                | Decoder needs N writers, N fsyncs, partition-dir management. Breaks symmetry with classic dumps.    |

Option A is recommended for parity with the classic layout and to avoid a decoder rewrite. The encoder's `encodeQueueMessages` is already in-memory-buffered (loads full file → sorts → emits) so per-partition streaming wouldn't measurably help.

## Encoder behavior

`internal/backup/encode_sqs.go` implements the reverse path as follows:

1. **No unsupported-partitioned gate remains.** Partitioned queues are accepted after `_queue.json` validation.
2. **Validate live-equivalent HT-FIFO metadata.** `readQueueMeta` rejects `_queue.json` shapes that the live control plane cannot create: non-power-of-two `partition_count` values, values above the HT-FIFO cap of 32, `partition_count > 1` on non-FIFO queues, FIFO-only HT-FIFO attributes on standard queues, unknown HT-FIFO enum values, `fifo_throughput_limit:"perMessageGroupId"` without partitions, and `partition_count > 1` with `deduplication_scope:"queue"`. All failures return `ErrSQSEncodeInvalidQueue` before any message is staged.
3. **Branch on raw `PartitionCount`.** `stageMessageRecords` derives `isPartitioned := meta.PartitionCount > 1` once from the queue meta. When true, data and side records use partitioned key constructors duplicated from `adapter/sqs_keys.go` following the established M3b-3 GSI pattern. When false, classic constructors are used.
4. **Group-by-partition before emit.** Sort messages by `(partition, send_timestamp_millis, sequence_number, message_id)` so per-partition order is stable across runs. `sequence_number` is the definitive tiebreaker between messages that share a send timestamp (burst case); `message_id` is the last-resort tiebreaker. The classic `sortMessagesForEmit` in `internal/backup/sqs.go` is a **3-field** tuple `(send_timestamp_millis, sequence_number, message_id)`, and the partitioned path prepends `partition` as the leading key.

`addMessage` and `addSideRecords` both take `(isPartitioned bool, partition uint32)`. The boolean selects the key family; the integer carries the already-validated lane. This distinction is observable: a classic dump may explicitly carry `"partition":0`, and the encoder must still emit classic keys because the queue's raw `PartitionCount <= 1` selects the classic keyspace.

`internal/backup/encode_sqs_side.go` mirrors the partitioned side-record constructors:

1. Partitioned `vis` constructor → `!sqs|msg|vis|p|...`.
2. Partitioned `byage` constructor → `!sqs|msg|byage|p|...`.
3. Partitioned `dedup` constructor → `!sqs|msg|dedup|p|...`. The partitioned shape adds a `<group-seg>` segment before `<dedupID-seg>`; the classic shape has only `<dedupID-seg>`. This means `message_group_id` is load-bearing for dedup-row construction on FIFO partitioned queues, and `messages.jsonl` already carries it as `message_group_id`.

`addSideRecords` runs on the same `isPartitioned` and `partition` inputs as `addMessage`, keeping data and side-record dispatch in lockstep.

## Validation invariants (fail-closed)

The full decision matrix lives in the table under §"Dump format." The encoder fails closed with these sentinels:

- `_queue.json` has a live-impossible HT-FIFO shape → **`ErrSQSEncodeInvalidQueue`**. This includes non-power-of-two `partition_count` values, values above the HT-FIFO cap of 32, `partition_count > 1` on non-FIFO queues, FIFO-only HT-FIFO attributes on standard queues, unknown HT-FIFO enum values, `fifo_throughput_limit:"perMessageGroupId"` without partitions, and `partition_count > 1` with `deduplication_scope:"queue"`. `partitionForGroup` uses the same mask contract as the live adapter, and partitioned dedup rows are keyed by `(partition, group, dedupID)`, so malformed metadata must fail before any message is staged. Pinned by `TestSQSEncodeRejectsNonPowerOfTwoPartitionCount`, `TestSQSEncodeRejectsTooLargePartitionCount`, `TestSQSEncodeRejectsPartitionedStandardQueue`, `TestSQSEncodeRejectsStandardQueueWithFIFOOnlyAttrs`, `TestSQSEncodeRejectsPerMessageGroupLimitWithoutPartitions`, `TestSQSEncodeRejectsUnknownHTFIFOAttributeValues`, `TestSQSEncodeRejectsQueueScopedDedupOnPartitionedFIFO`, and `TestSQSEncodeAcceptsPowerOfTwoPartitionCount`.
- `meta.PartitionCount > 1` AND `rec.Partition == nil` (pre-M5-3 dump under lifted gate, or M5-3 decoder bug) → **`ErrSQSEncodeMissingPartition`**. The operator must re-decode with an M5-3 decoder; replaying a legacy dump into a partitioned queue would silently move every message to partition 0.
- `meta.PartitionCount > 1` AND `*rec.Partition >= meta.PartitionCount` → **`ErrSQSEncodeOutOfRangePartition`** (out-of-range partition number, dump is malformed).
- `meta.PartitionCount > 1` AND `meta.FifoThroughputLimit == "perQueue"` AND `*rec.Partition != 0` → **`ErrSQSEncodePartitionRoutingMismatch`**. The live router (`adapter/sqs_partitioning.go:71-72` in `partitionFor`) forces every group to partition 0 whenever `FifoThroughputLimit == "perQueue"`, regardless of `PartitionCount`; ReceiveMessage only scans the partition-0 lane (`adapter/sqs_keys_dispatch.go:125-126`). Accepting `*rec.Partition >= 1` for a `perQueue` queue would restore messages onto `|p|1|...` lanes the live receive fan-out never visits — silent data loss on first read. Pinned by `TestSQSEncodeRejectsNonzeroPartitionOnPerQueueHTFIFO`.
- `meta.PartitionCount > 1` AND `meta.FifoThroughputLimit != "perQueue"` AND `*rec.Partition != partitionForGroup(meta, rec.MessageGroupID)` → **`ErrSQSEncodePartitionHashMismatch`**. For per-message-group HT-FIFO queues, the encoder mirrors the live FNV-1a partition hash and rejects an in-range partition that disagrees with `partitionFor(message_group_id)`. Pinned by `TestSQSEncodeRejectsHashMismatchOnPerMessageGroupId` and `TestSQSEncodePartitionForGroup_LiveAdapterParity`.
- `meta.PartitionCount <= 1` (classic) AND `rec.Partition != nil && *rec.Partition != 0` → **`ErrSQSEncodeInvalidMessage`** (dump is internally inconsistent).

**All partitioned-message gates above use raw `meta.PartitionCount > 1` as the partitioned-queue predicate.** Switching the missing-partition or out-of-range gates to `effectivePartitionCount` would loosen the missing-partition gate so a perQueue dump with `Partition == nil` could slip past validation. `stageMessageRecords` would still use raw `PartitionCount > 1` for key-shape dispatch and would emit partitioned `!sqs|msg|data|p|...partition 0...` keys, not classic keys. The risk is different but still unacceptable: a legacy or hand-authored dump would be treated as partition-aware without evidence that the decoder captured the partition, bypassing the re-decode requirement and the hash/routing consistency checks. The `effectivePartitionCount(meta) uint32` helper is still useful for diagnostics and for the ReceiveMessage scan fan-out, but it must not replace the raw predicate in encoder validation gates.

**Implementation note — define a local copy in `internal/backup/`.** An identically named function exists in `adapter/sqs_keys_dispatch.go` (operating on `*adapter.sqsQueueMeta`, the unexported live struct). The backup package does not import it — `internal/backup/` cannot pull from `adapter/` (M3b-3 circular-dependency pattern; same reason `sqsFifoDedupWindowMillis` is mirrored in `encode_sqs_side.go` rather than imported). The local copy operates on `sqsQueueMetaPublic` (the public/dump struct in `internal/backup/sqs.go`). Note: `sqsStoredQueueMeta` in `internal/backup/encode_sqs.go` is a different struct used for the `!sqs|queue|meta|` value — not the dump-side struct. The `"perQueue"` string constant likewise is mirrored because the adapter's `htfifoThroughputPerQueue = "perQueue"` is unexported and unimportable.

**Key-shape vs. validation distinction.** Both the "Branch on raw `PartitionCount`" key-shape choice in §"Encoder behavior" and the validation gates above use the raw `meta.PartitionCount > 1` predicate, never `effectivePartitionCount`. A `perQueue` queue with `PartitionCount=2` writes partitioned-shape keys (`|p|` prefix, all messages with partition=0) — the live adapter does this because `meta.PartitionCount > 1` selects the partitioned keyspace at dispatch, and `partitionFor` separately collapses every group to partition 0. Restoring such a dump under classic keys would silently flip the queue's wire shape; accepting a `Partition == nil` message would silently synthesize partition 0 rather than proving the dump came from the partitioned decoder.

For `perMessageGroupId`, the encoder runs the same `partitionFor(message_group_id)` hash mirror and rejects mismatches. For `perQueue`, the live routing collapses all messages to partition 0 and the encoder rejects nonzero partitions. Empty partitions remain valid; the invariant is routing correctness, not a requirement that every partition has at least one record.

## Decision gate: full reconstruction vs lazy rebuild (carry-over from M5-2)

M5-2's "full reconstruction" gate applies unchanged to M5-3 partitioned queues. The recommendation is the same — emit `vis` + `byage` + `dedup` inline during the per-message walk; do NOT emit `group` rows. Cost remains O(messages-in-dump); no extra disk read or Raft round-trip.

## Out of scope (deferred)

- **Cross-partition rebalancing.** A partition count change between dump and restore would invalidate every message's partition assignment. M5-3 forbids this — `meta.PartitionCount` must match the input dump exactly. A future milestone can add a `--repartition` flag that re-hashes `message_group_id` into the target partition count.
- **In-flight cross-partition receives.** Same `vis`-is-zero rule as M5-2; restored messages are visible. Documented in the encoder header.
- **Group lock rows.** Same prohibition as M5-2 — emitting any row falsely blocks the group permanently. M5-3 inherits the rule.

## Implemented files

```
internal/backup/sqs.go                       # sqsMessageRecord.Partition; partition-aware data-key parsing; HandleMessageData wires rec.Partition only for partitioned keys
internal/backup/encode_sqs.go                # queue meta validation; raw PartitionCount dispatch; partitioned data keys; partition validation gates
internal/backup/encode_sqs_side.go           # partitioned vis/byage/dedup constructors and side-record dispatch
internal/backup/encode_sqs_partitioned_test.go # partitioned encoder, validation, routing, and byte-shape coverage
internal/backup/encode_sqs_test.go           # classic encoder coverage and shared helpers
internal/backup/encode_sqs_side_test.go      # classic side-record coverage
internal/backup/sqs_test.go                  # partitioned data/side-key parser coverage
```

## Milestones (within M5-3)

The slice landed as a single PR because the decoder format change and encoder partition branch are tightly coupled. A partial landing would either reject all M5-3 dumps at the new encoder or break old encoders against new dumps.

## Test plan

| Test                                                              | Verifies                                                                                                              |
| ----------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `TestSQSEncodePartitionedQueueRoundTrip`                          | `partition_count=2`, 3 messages across both partitions → all data + side records emitted with `|p|` prefix             |
| `TestSQSEncodePartitionedDedupBuildsGroupSegment`                 | FIFO partitioned dedup row's `<group-seg>` matches `message_group_id` from `messages.jsonl`                            |
| `TestSQSEncodeRejectsMissingPartitionOnPartitionedQueue`          | `partition_count=2`, message with `Partition == nil` (legacy dump shape) → `ErrSQSEncodeMissingPartition`              |
| `TestSQSEncodeRejectsOutOfRangePartition`                         | message with `*Partition >= meta.PartitionCount` → `ErrSQSEncodeOutOfRangePartition`                                   |
| `TestSQSEncodeRejectsNonZeroPartitionOnClassicQueue`              | `PartitionCount=1` but message has `Partition=2` → `ErrSQSEncodeInvalidMessage`                                        |
| `TestSQSEncodeRejectsNonzeroPartitionOnPerQueueHTFIFO`            | `PartitionCount=2` + `FifoThroughputLimit="perQueue"` + message has `*Partition=1` → `ErrSQSEncodePartitionRoutingMismatch` |
| `TestSQSEncodeRejectsHashMismatchOnPerMessageGroupId`             | per-message-group HT-FIFO message's `partition` must match `partitionForGroup(message_group_id)`                       |
| `TestSQSEncodeRejectsNonPowerOfTwoPartitionCount`                 | non-power-of-two `partition_count > 1` fails closed with `ErrSQSEncodeInvalidQueue`                                     |
| `TestSQSEncodeRejectsTooLargePartitionCount`                      | power-of-two `partition_count` values above the live cap of 32 fail closed with `ErrSQSEncodeInvalidQueue`              |
| `TestSQSEncodeRejectsPartitionedStandardQueue`                    | `partition_count > 1` on a non-FIFO queue fails closed with `ErrSQSEncodeInvalidQueue`                                  |
| `TestSQSEncodeRejectsStandardQueueWithFIFOOnlyAttrs`              | standard queues with FIFO-only `fifo_throughput_limit` / `deduplication_scope` fail closed with `ErrSQSEncodeInvalidQueue` |
| `TestSQSEncodeRejectsPerMessageGroupLimitWithoutPartitions`       | `fifo_throughput_limit:"perMessageGroupId"` requires `partition_count > 1`                                             |
| `TestSQSEncodeRejectsUnknownHTFIFOAttributeValues`                | unknown HT-FIFO enum values fail closed before message staging                                                         |
| `TestSQSEncodeRejectsQueueScopedDedupOnPartitionedFIFO`           | `partition_count > 1` with `deduplication_scope:"queue"` fails closed with `ErrSQSEncodeInvalidQueue`                   |
| `TestSQSEncodeAcceptsPowerOfTwoPartitionCount`                    | power-of-two partition counts remain accepted                                                                          |
| `TestSQSEncodeClassicQueueWithExplicitPartitionZeroUsesClassicKeys` | classic queue with explicit `"partition":0` still emits classic keys                                                  |
| `TestSQSEncodeLegacyDumpsWithoutPartitionStillRoundTrip`          | a pre-M5-3 `messages.jsonl` with no `partition` field round-trips through M5-3 encoder unchanged                       |
| `TestSQSEncodePartitionedSideRecordsByteCrossCheck`               | partitioned `vis|p|` / `byage|p|` / `dedup|p|` bytes equal the live partitioned key shape                             |
| `TestSQSEncodePartitionedSortStableAcrossPartitions`              | partitioned emission order is stable by `(partition, send_timestamp_millis, sequence_number, message_id)`              |
| `TestSQSEncodePartitionForGroup_LiveAdapterParity`                | backup `partitionForGroup` mirror matches live-adapter representative routing cases                                     |

## References

- Parent: `2026_05_25_implemented_snapshot_logical_encoder.md` §"SQS"
- M5-2 doc (decision gate template, classic side records): `2026_05_30_implemented_sqs_side_record_derivation.md`
- M5-1 PR: #849
- M5-2 PR: #892
- Live partitioned constructors: `adapter/sqs_keys.go:337+` (`sqsPartitionedMsgDataKey` and siblings)
- Existing partitioned dispatch (cross-classic-partitioned routing): `adapter/sqs_keys_dispatch.go`
- Encoder queue-meta guard: `internal/backup/encode_sqs.go:readQueueMeta`
- Encoder partition validation: `internal/backup/encode_sqs.go:validatePartitioningPartitioned`
