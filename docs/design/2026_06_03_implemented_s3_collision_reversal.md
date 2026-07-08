# S3 collision-rename reversal (Phase 0b M4-2b) — implemented

**Status:** Implemented.
**Parent:** [`2026_05_25_implemented_snapshot_logical_encoder.md`](2026_05_25_implemented_snapshot_logical_encoder.md) — this resolves §"S3 — bodies re-chunked + manifest + collision/suffix reversal" by adding the inverse of the decoder's rename-collisions write path.
**Predecessor on disk:** M4-1 (`PR #847`) emits `!s3|bucket|meta|` + `!s3|bucket|gen|`. M4-2a (`PR #864`) emits `!s3|obj|head|` + `!s3|blob|` for every object whose key is recoverable from the on-disk path alone. M4-2b removes the former collision-tracker rejection and teaches the encoder to reverse each bucket's `KEYMAP.jsonl` entries before emitting object records.

## Implemented behavior

For every bucket whose decoder run wrote a per-bucket `KEYMAP.jsonl`, the encoder reads that file and uses it to translate on-disk dump filenames back to original S3 object keys before emitting `!s3|obj|head|<key>` / `!s3|blob|<key>` records.

**Three rename kinds the decoder records** (`internal/backup/keymap.go:33-43`):

| `Kind` constant     | When the decoder records it                                                                                                                                                                                                                                                          | What to invert                                                                          |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------- |
| `KindS3LeafData`    | An S3 bucket contains both object `<key>` and one or more objects matching `<key>/...`. The shorter key cannot coexist with a directory of the same name, so the decoder writes `<key>.elastickv-leaf-data` and records `Encoded=<key>.elastickv-leaf-data → OriginalB64=<key>`.    | Strip the `.elastickv-leaf-data` suffix; emit the object under the original `<key>`.    |
| `KindMetaCollision` | The operator ran the decoder with `--rename-collisions=true` AND a user object key naturally ends in `.elastickv-meta.json` (which the decoder otherwise reserves for per-object sidecars). The decoder rewrites the user key to a non-colliding form and records the inverse.       | Recover the original `.elastickv-meta.json`-suffixed key from `OriginalB64`.            |
| `KindSHAFallback`   | The on-disk filename segment is `<sha-prefix-32>__<truncated-original>` because the original segment exceeded `EncodeSegment`'s 240-byte filesystem ceiling. NOT specific to S3 — the Redis encoder already handles this kind via `loadKeymap` (`internal/backup/encode_redis.go`). | Resolve the encoded segment back to the original key bytes via the keymap.              |

The Redis reverse encoder already consumes `KEYMAP.jsonl` for `KindSHAFallback` (`encode_redis.go:113`); M4-2b is the structural parallel for S3, adding per-bucket support for `KindS3LeafData` and `KindMetaCollision` on top.

## Decision gate: load-once vs. lazy lookup

The parent doc (§"Re-derivable indexes") gives each adapter discretion on the build strategy. M4-2b resolves this for S3:

**Implemented: load-once per bucket.** Open `<bucket>/KEYMAP.jsonl` once at the top of `encodeBucketObjects`, build a `map[encodedSegment]KeymapRecord`, then look up each leaf during the existing `filepath.WalkDir` of the bucket's object tree. Rationale:

- **Per-bucket cost is O(records-in-keymap).** A 1 MiB-key cap × 4 MiB-line cap × hundreds of renames is still well under the snapshot builder's working-set budget. The Redis encoder already follows this pattern (`encode_redis.go:113-132`) without measured impact.
- **Lazy lookup is impractical.** Walking the keymap once per object would be O(objects × keymap-records), and the keymap needs O(1) reverse-lookup by encoded segment anyway.

**Boundaries:**

- **Cross-bucket keymaps.** Each bucket gets its own `KEYMAP.jsonl` because object keys are bucket-scoped; M4-2b does NOT support a single top-level keymap. The decoder enforces per-bucket already.
- **Reserved-root-collision keymaps (`_incomplete_uploads`, `_orphans`).** These subtrees are gated by `ErrEncodeUnsupportedS3IncompleteUploads` / `ErrEncodeUnsupportedS3Orphans`; M4-2b does not reverse their renames. Any KEYMAP record whose original key's first slash-split component is exactly `_incomplete_uploads` or `_orphans` is treated as a malformed dump and fails closed. Similar user keys such as `_orphansFoo/x`, `_foo/bar`, or `nested/_orphans/x` remain valid.
- **Cross-suffix collisions.** A `KindS3LeafData` rename target that also collides with `KindMetaCollision`'s reserved suffix is already rejected at decode time (`s3.go:892`) — the encoder inherits this invariant; no separate validation needed.

## Per-bucket keymap loading

Helper in `encode_s3_collision.go`:

```go
// loadBucketKeymap reads <bucket>/KEYMAP.jsonl into an
// encoded-segment → KeymapRecord map. The file is optional (a bucket
// with no collisions has no keymap). On a malformed line the encoder
// fails closed with ErrInvalidKeymapRecord wrapped with the file path.
func (e *S3RecordEncoder) loadBucketKeymap(root *os.Root, bucketDir string) (map[string]KeymapRecord, error)
```

**Refactor target.** The existing call site is `isKeymapCollisionTracker` (`encode_s3_objects.go:108`, called at line 91; returns `(tracker bool, err error)`). M4-2b updates this site:

1. Keep `isKeymapCollisionTracker` to disambiguate. When it returns `tracker = false`, the bucket has a legitimate `KEYMAP.jsonl` user object — the file is paired with a regular-file `KEYMAP.jsonl.elastickv-meta.json` sidecar and round-trips as a normal S3 body (existing `TestS3EncodeKeymapObjectRoundTrip` pins this). A directory at `KEYMAP.jsonl.elastickv-meta.json/` is not a sidecar and keeps tracker classification active (pinned by `TestS3EncodeTrackerWithDirectorySidecar`). `loadBucketKeymap` does not touch legitimate user objects; M4-2a's object emission already covers them.
2. When `tracker == true`, call `loadBucketKeymap`. This replaces the former `ErrS3EncodeUnsupportedCollision` branch.

`loadBucketKeymap` reuses the existing per-line `KeymapReader`. **It does not use `OpenSidecarFile`** — that helper is write-side on every platform: `O_WRONLY|O_CREATE|O_TRUNC` on Windows (`open_nofollow_windows.go:32`) and the non-unix/non-windows fallback (`open_nofollow_other.go:32`), and `O_WRONLY|O_CREATE` + a deferred `Truncate(0)` on Unix (`open_nofollow_unix.go:60` + `:97`). Calling it on `KEYMAP.jsonl` would erase the file before the reader could see it, losing every rename mapping. The loader instead uses the **read-side** pipeline that `openRootRegular` (`encode_s3_objects.go:260`) already establishes for object bodies: `root.Lstat` → `refuseHardLink` → `root.Open` (read-only). `isKeymapCollisionTracker` (`encode_s3_objects.go:108`) only performs `root.Lstat` + `IsRegular` on `KEYMAP.jsonl`, and then `root.Lstat` + `IsRegular` on `KEYMAP.jsonl.elastickv-meta.json` for the sidecar disambiguation; it does NOT itself `root.Open` the tracker. `loadBucketKeymap` therefore re-runs the full `root.Lstat` → `refuseHardLink` → `root.Open` sequence; treat the `isKeymapCollisionTracker` check as a precondition that confirms tracker classification, not as work `loadBucketKeymap` can skip.

**Duplicate-key contract — diverges from `LoadKeymap`.** The shared `keymap.go:LoadKeymap` documents last-wins behavior for duplicate `Encoded` segments (the Redis encoder tolerates this because its decoder's keymap writer always emits one record per encoded segment, and a duplicate from a hand-edited dump just retains the most recent). M4-2b's invariant is stricter: the S3 decoder's `recordKeymap` writes exactly one entry per renamed object, so a duplicate `Encoded` segment means the decoder wrote two distinct rename targets for the same on-disk name — a corrupt dump the encoder cannot disambiguate. Therefore `loadBucketKeymap` does NOT call `LoadKeymap`; it iterates `KeymapReader.Next()` in a manual loop and fails closed on the second occurrence of any `Encoded` value (`TestS3EncodeRejectsDuplicateKeymapEntry`).

**Keymap key is the full relative path, NOT per-segment.** The decoder's `recordKeymap` call site (`internal/backup/s3.go:728`) records the full filesystem-relative path produced by `resolveObjectFilename` (e.g. `path/to.elastickv-leaf-data`) as the `Encoded` field and the full original S3 object key (e.g. `path/to`) as the `OriginalB64`. Per-segment lookup would miss every nested-collision entry; the encoder looks up the full rel-path.

## Object-walk integration

`encodeBucketObjects`' existing `filepath.WalkDir` continues to enumerate the bucket's `s3/<bucketDir>/**/*` tree. For each file (sidecar or body), the leaf walker resolves the on-disk relative path to the original S3 object key by:

1. Compute the relative path under `bucketDir` (e.g. `path/to.elastickv-leaf-data`) — same string the decoder passed as `Encoded` to `recordKeymap`.
2. Look it up directly in the keymap map. If a record is found, decode `OriginalB64` to get the full original object key (e.g. `path/to`) — no suffix-stripping needed; the decoder already stored the un-suffixed original.
3. If no record is found, the on-disk relative path IS the original object key (the no-collision case M4-2a already handles).

The resolved key feeds the existing `!s3|obj|head|<key>` / `!s3|blob|<key>` emission unchanged.

**Difference from Redis.** Redis's `loadKeymap` (`encode_redis.go:113`) handles only `KindSHAFallback`, where the keymap entry is keyed by a single filename segment (because filename-length overflow happens segment-by-segment). S3 collision records are scoped to the full object key, so the lookup unit is the full bucket-relative path rather than a single segment. No common helper between the two; the S3 lookup lives in `encode_s3_collision.go`.

**`KindSHAFallback` policy for S3.** S3's decoder rename path (`s3.go:resolveObjectFilename`) emits `KindS3LeafData` and `KindMetaCollision`; `KindSHAFallback` is reserved for a later case where a single S3 key segment exceeds `EncodeSegment`'s 240-byte filesystem ceiling. M4-2b keeps the encoder permissive for forward compatibility: a `KindSHAFallback` record in an S3 `KEYMAP.jsonl` is honored by the same full-rel-path lookup (the decoder would write `Encoded = <sha-prefix>__<truncated>` as the full relative segment), so the existing object-walk path naturally handles it. Any unknown `Kind` value not in `{S3LeafData, MetaCollision, SHAFallback}` fails closed with `ErrS3EncodeInvalidBucket` so a hand-edited dump that injects a novel Kind cannot silently bypass invariants.

## Error contract

The encoder fails closed with the existing per-adapter sentinel `ErrS3EncodeInvalidBucket` (wrapping `ErrInvalidKeymapRecord`) on:

- Malformed JSON in `KEYMAP.jsonl`.
- A `KindS3LeafData` record whose `Encoded` does not end in `.elastickv-leaf-data`.
- A `KindMetaCollision` record whose original key does not end in `.elastickv-meta.json`.
- An original key whose first top-level path component is exactly one of the dump-control reserved subtrees: `_incomplete_uploads` or `_orphans`. (User object keys like `_foo` or `_foo/bar` are LEGITIMATE — the whole `_` namespace is NOT reserved, only the two named subtrees are.)
- A keymap record referencing an on-disk segment that doesn't exist (orphan record — the dump is internally inconsistent).
- A multiply-defined `Encoded` segment (the same on-disk name listed twice with different originals).

`ErrS3EncodeUnsupportedCollision` is removed; its old rejection test (`encode_s3_objects_test.go:300`) now asserts the same fixture round-trips successfully.

## Self-test cross-check

Each existing M4-2a self-test fixture (`encode_s3_objects_test.go`) has a sibling that:

1. Sets up a bucket with both `path/to` and `path/to/sub` keys (leaf-data rename).
2. Round-trips through `S3Encoder.Decode → S3RecordEncoder.Encode`.
3. Asserts the produced `!s3|obj|head|` keys are `path/to` and `path/to/sub` (NOT `path/to.elastickv-leaf-data`).

The suite also includes a `--rename-collisions=true` variant for `KindMetaCollision`.

## Implementation files

```
internal/backup/encode_s3_collision.go        # loadBucketKeymap + resolveS3Segment
internal/backup/encode_s3_collision_test.go   # keymap parse, segment resolution, error paths
internal/backup/encode_s3_objects.go          # tracker detection, bucket keymap loading, WalkDir key resolution
internal/backup/encode_s3_objects_test.go     # round-trip leaf-data + meta-collision coverage
internal/backup/encode_snapshot.go            # updated exit-2 routing comment after removing the unsupported-collision sentinel
```

(v3's earlier draft of this section incorrectly named `encode_s3.go` as the home of `ErrS3EncodeUnsupportedCollision`; it is actually in `encode_s3_objects.go`. Corrected in v4.)

## Milestone

The keymap loader, segment resolver, and integration into `encodeBucketObjects` landed as one slice because a partial landing would be incoherent: the decoder write contract and the encoder read contract are inseparable.

## Test plan

| Test                                                                | Verifies                                                                                                                       |
| ------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `TestS3EncodeRoundTripsLeafDataCollision`                           | Bucket with `path/to` + `path/to/sub` → both objects emitted under their original keys; no `.elastickv-leaf-data` suffix leaks |
| `TestS3EncodeRoundTripsMetaCollision`                               | Bucket with user object key ending `.elastickv-meta.json` + `--rename-collisions` → original key recovered                     |
| `TestS3EncodeRejectsKeymapTargetingReservedSubtree`                 | `KEYMAP.jsonl` record claiming `Original="_orphans/foo"` or `"_incomplete_uploads/x"` → `ErrS3EncodeInvalidBucket` (PR body line: `TestS3EncodeRejectsKeymapWithReservedPrefix` — both refer to the same case; the doc is the authoritative name) |
| `TestS3EncodeAcceptsKeymapWithUserUnderscoreKey`                    | `KEYMAP.jsonl` record claiming `Original="_foo"` (legitimate user key) → round-trips successfully                              |
| `TestS3EncodeKeymapObjectRoundTrip`                                  | Bucket with a real user object named `KEYMAP.jsonl` (paired regular-file sidecar at `KEYMAP.jsonl.elastickv-meta.json`) → `isKeymapCollisionTracker` returns `tracker=false`, object round-trips as a normal S3 body, `loadBucketKeymap` is NOT called |
| `TestS3EncodeTrackerWithDirectorySidecar`                            | A directory at `KEYMAP.jsonl.elastickv-meta.json/` does not count as the sidecar for a legitimate `KEYMAP.jsonl` user object; the tracker remains active |
| `TestS3EncodeRejectsOrphanKeymapEntry`                              | `KEYMAP.jsonl` record references a segment that doesn't exist on disk → fail closed                                            |
| `TestS3EncodeRejectsDuplicateKeymapEntry`                           | Same `Encoded` listed twice → fail closed (we can't pick a winner; diverges from `LoadKeymap` last-wins)                        |
| `TestValidateKeymapRecord_UnknownKind`                               | `Kind` not in `{S3LeafData, MetaCollision, SHAFallback}` → `ErrInvalidKeymapRecord` before the bucket-level wrapper maps it to `ErrS3EncodeInvalidBucket` |
| `TestS3EncodeRejectsMalformedKeymapJSON`                            | `KEYMAP.jsonl` has an invalid line → `ErrInvalidKeymapRecord` (existing sentinel from `internal/backup/keymap.go`)             |
| `TestS3EncodeMissingKeymapIsValidNoCollisionDump`                   | Bucket without `KEYMAP.jsonl` continues to encode (the no-collision case M4-2a already covers)                                 |

## References

- Parent: `2026_05_25_implemented_snapshot_logical_encoder.md` §"S3"
- Decoder write path: `internal/backup/s3.go:611-700` (`flushObjectWithCollision`, `closeBucketKeymap`)
- Keymap format: `internal/backup/keymap.go` (single source of truth for `KeymapRecord`)
- Sibling encoder pattern: `internal/backup/encode_redis.go:113-132` (Redis's `loadKeymap`)
- Predecessor PRs: M4-1 #847 (bucket meta), M4-2a #864 (object bodies, no collisions)
