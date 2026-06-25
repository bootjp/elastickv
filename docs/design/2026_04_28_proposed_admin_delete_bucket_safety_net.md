# AdminDeleteBucket Orphan-Object Safety Net

**Status:** Proposed
**Author:** bootjp
**Date:** 2026-04-28

## 1. Background

`AdminDeleteBucket` and the SigV4 `s3.go:deleteBucket` share a known
TOCTOU race resolved by this design and recorded in the
implementation-status block of
[`docs/design/2026_04_24_implemented_admin_dashboard.md`](2026_04_24_implemented_admin_dashboard.md).
coderabbitai 🔴/🟠 flagged it during PR #669 review.

The current shape:

```go
err := s.retryS3Mutation(ctx, func() error {
    readTS  := s.readTS()
    startTS := s.txnStartTS(readTS)

    // (1) "is bucket empty?" probe at readTS
    kvs := s.store.ScanAt(ctx, ObjectManifestPrefixForBucket(name, gen),
                          end, /*limit=*/1, readTS)
    if len(kvs) > 0 {
        return ErrAdminBucketNotEmpty
    }

    // (2) commit BucketMetaKey delete
    return s.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
        IsTxn:   true,
        StartTS: startTS,
        Elems:   []*kv.Elem[kv.OP]{{Op: kv.Del, Key: BucketMetaKey(name)}},
    })
})
```

The OCC validator only inspects keys appearing in
`OperationGroup.ReadKeys`. The empty-probe scan reads a *range*, but
that range is not expressible as a `ReadKeys` entry today. A
concurrent `PutObject` that lands a manifest key in the scanned
prefix between `readTS` and the delete's commit will not conflict
— the delete commits successfully, leaving the new object orphaned
under a now-deleted `BucketMetaKey`.

## 2. Why this matters operationally

The orphan state is invisible to every existing API:

- `ListBuckets` does not show the bucket (BucketMetaKey is gone).
- `ListObjects` cannot be called (returns 404 NoSuchBucket).
- The orphaned manifest + chunk data persists in Pebble until
  compaction picks up tombstones — but there are no tombstones for
  these specific keys, so they live forever.

There is no in-tree garbage collector for unreachable manifest /
blob data; the only recovery path today is a custom scan+delete
tool an operator would have to write.

Bucket re-creation is *not* impacted: `AdminCreateBucket` bumps
`BucketGenerationKey` so the new bucket runs under a different
generation prefix. Orphans from the old generation stay invisible
under the new bucket. So this is purely a disk-space leak with no
data-corruption surface — but a leak that grows without bound
under a workload that mixes `AdminDeleteBucket` with concurrent
writes.

## 3. Design space

Three options were canvassed during PR #669 review:

### 3.1 Option A — bump `BucketGenerationKey` on every PutObject

Add `Put BucketGenerationKey` to every `PutObject` mutation, and
include `BucketGenerationKey` in `AdminDeleteBucket.ReadKeys`. A
concurrent `PutObject` would then conflict because it modified the
generation key.

- **Pro:** schema unchanged; existing OCC catches the race.
- **Con:** every `PutObject` in production (the hot path)
  serializes through one bucket-level key. Concurrent writes to the
  same bucket are now linearly ordered. Throughput regression is
  proportional to `PutObject` parallelism.

### 3.2 Option B — extend `OperationGroup` with `ReadRanges`

Add a `ReadRanges []KeyRange` field. The FSM, at apply time at
`commitTS`, scans each range and aborts the txn if any visible key
falls inside it (when the txn observed it as empty).

- **Pro:** semantically clean — preserves the "bucket must be empty
  to delete" invariant strictly. PutObject sees its 200-OK response
  honored; `AdminDeleteBucket` aborts and surfaces 409 to the
  operator.
- **Con:** schema change to `OperationGroup`, coordinator dispatch,
  and FSM apply paths. Cross-shard ReadRanges adds shard-routing
  decisions for range probes. Larger blast radius; needs Jepsen
  re-validation.

### 3.3 Option C (this proposal) — `DEL_PREFIX` safety net

`AdminDeleteBucket` augments its commit with `DEL_PREFIX` over every
per-bucket prefix in `s3keys`. The empty-probe is preserved as the
operator-facing UX (still returns 409 when non-empty); the
`DEL_PREFIX` ops act as a safety net that wipes anything that snuck
in during the race window.

- **Pro:** existing op type (`pb.Op_DEL_PREFIX` already exists in
  `kv/coordinator.go:894`); no schema change; `PutObject` hot path
  unchanged.
- **Con:** *contract change* — a `PutObject` that returned 200 OK
  to the client can have its data swept by a racing
  `AdminDeleteBucket`. Operationally bounded: this only happens
  when an operator issues an admin delete against a bucket that is
  receiving concurrent writes, which `docs/admin_deployment.md`
  already advises against.

## 4. Decision

**Adopt Option C.** Rationale:

- Option A imposes a permanent throughput tax on every PutObject
  to fix a race that fires only at admin-delete time. That cost
  ratio is wrong.
- Option B is the "semantically purest" fix but the implementation
  surface (proto + coordinator + FSM + Jepsen) is large and
  blocks unrelated work in those areas. Worth deferring until a
  second use case for `ReadRanges` appears.
- Option C closes the orphan window with the smallest patch.
  The contract change is bounded, documented, and matches what an
  operator who reads `docs/admin_deployment.md` already expects
  ("pause writes before admin delete").

## 5. Per-bucket prefix inventory

Every key family that lives under a bucket+generation tuple in
`internal/s3keys/keys.go`:

| Key family | Prefix const | Per-bucket prefix |
|---|---|---|
| Object manifest | `ObjectManifestPrefix` | `ObjectManifestPrefixForBucket(bucket, gen)` (exists) |
| Upload metadata | `UploadMetaPrefix` | new `UploadMetaPrefixForBucket(bucket, gen)` |
| Upload parts | `UploadPartPrefix` | new `UploadPartPrefixForBucket(bucket, gen)` |
| Object data chunks | `BlobPrefix` | new `BlobPrefixForBucket(bucket, gen)` |
| GC tracking | `GCUploadPrefix` | new `GCUploadPrefixForBucket(bucket, gen)` |
| Routing keys | `RoutePrefix` | new `RoutePrefixForBucket(bucket, gen)` |
| Bucket meta | `BucketMetaPrefix` | point key (existing `Del`) |
| Bucket gen | `BucketGenerationPrefix` | point key — **kept** across delete |

`BucketGenerationKey` is intentionally *not* deleted. Re-creating
the bucket bumps the generation; orphan blobs from the old
generation stay invisible under the new bucket. Removing the
generation key would lose this property and is therefore avoided.

## 6. Concrete change

### 6.1 New `s3keys` helpers

```go
// keys.go
func UploadMetaPrefixForBucket(bucket string, generation uint64) []byte
func UploadPartPrefixForBucket(bucket string, generation uint64) []byte
func BlobPrefixForBucket(bucket string, generation uint64) []byte
func GCUploadPrefixForBucket(bucket string, generation uint64) []byte
func RoutePrefixForBucket(bucket string, generation uint64) []byte
```

Each is `<family-prefix><EncodeSegment(bucket)><appendU64(gen)>`,
identical to the existing `ObjectManifestPrefixForBucket`.

### 6.2 `AdminDeleteBucket` (and `s3.go:deleteBucket`) commit shape

The original revision of this design proposed a **single
OperationGroup** carrying both the `Del BucketMetaKey` and the
six `DelPrefix` ops, relying on the FSM to apply them all at one
commitTS. That shape is rejected by the production coordinator
(Codex P1 on PR #695):

- `kv/sharded_coordinator.go:dispatchDelPrefixBroadcast` rejects
  any `OperationGroup` containing `DelPrefix` when `IsTxn` is
  true: *"DEL_PREFIX not supported in transactions"*.
- The same broadcast path runs `validateDelPrefixOnly` and rejects
  mixed `Del` / `Put` + `DelPrefix` groups: *"DEL_PREFIX cannot be
  mixed with other operations"*.

Together those two rules forbid the single-group shape. The
implementation splits into two `Dispatch` calls:

```go
// Phase 1: Del BucketMetaKey in a txn (OCC-protected against
// concurrent AdminCreateBucket racing this delete).
s.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
    IsTxn:   true,
    StartTS: startTS,
    Elems:   []*kv.Elem[kv.OP]{{Op: kv.Del, Key: BucketMetaKey(name)}},
})

// Phase 2: DEL_PREFIX safety net broadcast (non-txn). Only runs
// after Phase 1 commits; failure here is best-effort and logged
// rather than propagated.
s.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
    Elems: []*kv.Elem[kv.OP]{
        {Op: kv.DelPrefix, Key: ObjectManifestPrefixForBucket(name, gen)},
        {Op: kv.DelPrefix, Key: UploadMetaPrefixForBucket(name, gen)},
        {Op: kv.DelPrefix, Key: UploadPartPrefixForBucket(name, gen)},
        {Op: kv.DelPrefix, Key: BlobPrefixForBucket(name, gen)},
        {Op: kv.DelPrefix, Key: GCUploadPrefixForBucket(name, gen)},
        {Op: kv.DelPrefix, Key: RoutePrefixForBucket(name, gen)},
    },
})
```

The empty-probe stays as the operator-facing UX: when the bucket
is genuinely non-empty (no race), the operator still sees 409 and
neither phase runs.

**Phase-2 failure semantics**: Phase 1 is the point of no return.
If Phase 2's `Dispatch` returns an error (transport blip,
cluster transient, etc.), the bucket meta is already gone but
the per-bucket prefixes may still contain orphans. The operator-
visible delete reports success (the bucket really is gone from
the API surface, and a retry would 404 at `loadBucketMetaAt`) and
the failure is recorded via `slog.WarnContext`. The resulting
state is no worse than the pre-fix behaviour on main — orphans
were already the failure mode the original race produced. A
future cluster-wide sweep tool (when one exists) can recover the
disk space.

**Why not Phase 2 first?** A "DEL_PREFIX before Del" ordering
would wipe per-bucket data while the bucket meta still exists. If
Phase 1 then fails (concurrent recreate races the OCC), readers
see "bucket exists" but their chunks/manifests don't — confusing
state with no clean recovery. Phase-1-first localises any partial
failure to "bucket gone, orphan data may persist", which has a
well-defined audit trail in slog.

**Test-coordinator parity**: `adapter/dynamodb_migration_test.go`
mirrors the production rejection rules in `localAdapterCoordinator.
validateDispatchShape`. Without that, the original single-group
shape passed local tests while production rejected every bucket
delete with `ErrInvalidRequest` — exactly the gap Codex P1 caught.

### 6.3 Apply-time cost

`pb.Op_DEL_PREFIX` is broadcast to every shard
(`kv/sharded_coordinator.go:230` — "DEL_PREFIX cannot be routed to
a single shard"). Each shard scans the prefix and tombstones every
key found.

This is acceptable for `AdminDeleteBucket` because:

1. The empty-probe already confirmed the prefix is empty under
   normal operation, so the FSM-side scan finds 0–1 keys per
   shard. The cost is the per-shard *open scan* itself, not the
   tombstone count.
2. `AdminDeleteBucket` is a low-frequency operation (operator
   action, not data-plane traffic).
3. Six DEL_PREFIX ops × N shards is bounded; each scan is O(log
   keys per shard) for the index lookup plus O(matching keys) for
   the tombstone, where matching keys ≈ 0 in the common case.

### 6.4 Contract update

`docs/admin_deployment.md` §4.1 ("Adding or removing an admin
key") and §3.3 ("Plaintext development setups") will gain a new
paragraph in the bucket-delete section:

> When `AdminDeleteBucket` runs against a bucket receiving
> concurrent writes, any object whose `PutObject` lands during the
> empty-probe → commit window will be swept along with the bucket
> meta. The client may have received `200 OK` for that PutObject;
> the data does not survive. Pause writes against the bucket
> before issuing the admin delete to retain in-flight writes.

This matches the existing "pause writes before admin delete"
guidance — we're making the failure mode well-defined (data loss
within the race window) rather than under-specified (orphan
objects with no recovery path).

## 7. Tests

Per CLAUDE.md "test the bug first":

1. **Reproduction test (current bug)**: `adapter/s3_admin_test.go`
   adds a goroutine race between `AdminDeleteBucket` and
   `PutObject`. Without the fix: assert that after delete completes,
   a manifest scan still finds the racing object's manifest key
   under the deleted bucket's old generation. With the fix: assert
   the scan returns zero keys.

2. **Empty-bucket happy path**: confirm the `DEL_PREFIX` ops are a
   no-op (no spurious tombstones planted) when the bucket is
   genuinely empty.

3. **Generation isolation**: delete bucket → recreate with same
   name → write objects → confirm new generation prefix is
   unaffected by the prior DEL_PREFIX (already isolated by
   `appendU64(gen)`, but pin the contract).

4. **Symmetric coverage**: the same test set exercises both
   `AdminDeleteBucket` and `s3.go:deleteBucket` (SigV4 path).

5. **Property test (bonus)**: `pgregory.net/rapid` over `(N writes,
   1 delete)` interleavings, asserting that after the delete every
   per-bucket prefix is empty.

## 8. Out of scope

- The Option A / B alternatives. Recorded in §3 but not pursued.
- A general-purpose "delete bucket and all objects" admin endpoint
  (force-delete). The empty-probe + 409 is preserved; only the
  race window is closed.
- Hot-path PutObject changes. None.
- Cross-bucket atomicity. None — the change is per-bucket.

## 9. Rollout

1. Land the `s3keys` prefix helpers in a stand-alone refactor
   commit (no behavior change), so the diff is reviewable in
   isolation.
2. Land the `AdminDeleteBucket` and `s3.go:deleteBucket` op-list
   change.
3. Run the relevant Jepsen suites — the S3 adapter does not have
   a dedicated suite today, but `make test -race` plus the new
   reproduction test must pass.
4. Update `docs/admin_deployment.md` and the partial design doc's
   Outstanding section (mark TOCTOU as fixed).
5. Per `docs/design/README.md` lifecycle, this proposal renames to
   `2026_04_28_implemented_admin_delete_bucket_safety_net.md`
   after the implementation lands.
