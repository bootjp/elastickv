# Fenced adapter read snapshots

Latest reads must establish a committed-log barrier **before** selecting an
MVCC timestamp. A store read that forwards to the leader does not repair an
old timestamp supplied by the caller. For example, a follower can return
`GET = 0` after `PUT = 1` completed elsewhere if it sampled its local watermark
before the leader fence.

## Construction

`adapter/read_snapshot.go` owns snapshot construction through
`beginFencedReadSnapshot(ctx, coordinator, store, label, keys...)`. It accepts
no timestamp or timestamp-selection callback. It returns the existing opaque
`kv.ReadTimestamp`, preserving Phase-D validation and dispatch vouchers.

The factory:

1. Uses the store's strict keyed committed floor when a complete key set is
   supplied, or its global committed floor otherwise.
2. For local stores without those capabilities, waits for the coordinator's
   linearizable barrier and only then samples the local committed watermark.
   A missing coordinator fails closed.
3. Returns errors from the fence; it never tries a best-effort RPC or local
   watermark after a distributed floor failure.
4. Normalizes an empty watermark to 1, observes the floor in the HLC, then
   constructs the coordinator-validated read token. A fresh clock allocation
   must not replace the applied watermark.

Redis MULTI/EXEC and list range reads preserve explicit physical group IDs,
including legacy wide-column groups, through `beginTargetReadSnapshot`.
Targets without an explicit group use keyed routing rather than treating
logical group 0 as a physical group. The existing lease checks around pinning
remain in place.

S3 GET/HEAD fence bucket metadata and the current generation's object group,
then re-read the bucket generation at their common snapshot. A concurrent
bucket recreation causes bounded retry. Metadata, manifest and chunks use the
same pinned timestamp. Other S3 reads and mutation pre-reads, SQS reads and
mutation pre-reads, DynamoDB transaction/admin/schema/migration reads, Redis
reads/mutation pre-reads/compaction, and timestamp-free gRPC reads use the fenced
constructors. Operations without a complete group set use the global floor;
this deliberately requires the participating groups to be available.

## Separate contracts

- Explicit gRPC timestamps are historical reads and retain their supplied
  timestamp. They do not silently become latest reads.
- DynamoDB eventual reads retain their local snapshot semantics. Its lease
  planning may speculatively inspect local schema; actual strongly consistent
  reads establish their own fence. The local helper is named
  `eventualSnapshotTS` to expose this distinction.
- Migration catalog snapshots, committed item seals and raw watermark
  inspection have separate protocol contracts. Their construction sites are
  enumerated individually in the architecture test.

## Enforcement and validation

Unfenced convenience methods (`readTS`, `nextTxnReadTS`, `txnStartTS`, and the
old best-effort `globalSnapshotTS`) are absent from production code. Fixture
helpers live in `_test.go`, so production use fails to compile.

`TestReadSnapshotConstructionIsCentralized` parses every production adapter
file. It rejects direct local/best-effort watermark access and new calls to the
low-level token constructor outside explicitly audited functions. Exceptions
are function-specific, not whole-file exemptions. New handlers use the fenced
factory rather than adding an exception.

Behavioral tests cover delayed apply before sampling, stale local watermarks,
failed distributed fences without fallback, empty snapshots, Phase-D tokens,
S3 GET/HEAD and metadata reads, and the distinction between latest and explicit
historical gRPC reads. The hosted Jepsen matrix additionally exercises Redis,
DynamoDB, S3, SQS Standard and HT-FIFO under partition, kill and pause.
