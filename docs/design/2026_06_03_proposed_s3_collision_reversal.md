# S3 collision-rename reversal (Phase 0b M4-2b) — proposed

**Status:** Proposed (no implementation yet).
**Parent:** [`2026_05_25_partial_snapshot_logical_encoder.md`](2026_05_25_partial_snapshot_logical_encoder.md) — this resolves §"S3 — bodies re-chunked + manifest + collision/suffix reversal" by adding the inverse of the decoder's rename-collisions write path.
**Predecessor on disk:** M4-1 (`PR #847`) emits `!s3|bucket|meta|` + `!s3|bucket|gen|`. M4-2a (`PR #864`) emits `!s3|obj|head|` + `!s3|blob|` for every object whose key is recoverable from the on-disk path alone. Both currently fail closed via `ErrS3EncodeUnsupportedCollision` whenever a bucket's dump tree carries a `KEYMAP.jsonl` collision-tracker — i.e., any dump that exercised the decoder's rename-collisions path is rejected without a partial restore (`internal/backup/encode_s3_objects.go:95-98`).

## What needs to land

For every bucket whose decoder run wrote a per-bucket `KEYMAP.jsonl`, the encoder must read that file and use it to translate on-disk dump filenames back to original S3 object keys before emitting `!s3|obj|head|<key>` / `!s3|blob|<key>` records.

**Three rename kinds the decoder records** (`internal/backup/keymap.go:33-43`):

| `Kind` constant     | When the decoder records it                                                                                                                                                                                                                                                          | What to invert                                                                          |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------- |
| `KindS3LeafData`    | An S3 bucket contains both object `<key>` and one or more objects matching `<key>/...`. The shorter key cannot coexist with a directory of the same name, so the decoder writes `<key>.elastickv-leaf-data` and records `Encoded=<key>.elastickv-leaf-data → OriginalB64=<key>`.    | Strip the `.elastickv-leaf-data` suffix; emit the object under the original `<key>`.    |
| `KindMetaCollision` | The operator ran the decoder with `--rename-collisions=true` AND a user object key naturally ends in `.elastickv-meta.json` (which the decoder otherwise reserves for per-object sidecars). The decoder rewrites the user key to a non-colliding form and records the inverse.       | Recover the original `.elastickv-meta.json`-suffixed key from `OriginalB64`.            |
| `KindSHAFallback`   | The on-disk filename segment is `<sha-prefix-32>__<truncated-original>` because the original segment exceeded `EncodeSegment`'s 240-byte filesystem ceiling. NOT specific to S3 — the Redis encoder already handles this kind via `loadKeymap` (`internal/backup/encode_redis.go`). | Resolve the encoded segment back to the original key bytes via the keymap.              |

The Redis reverse encoder already consumes `KEYMAP.jsonl` for `KindSHAFallback` (`encode_redis.go:113`); M4-2b is the structural parallel for S3, adding per-bucket support for `KindS3LeafData` and `KindMetaCollision` on top.

## Decision gate: load-once vs. lazy lookup

The parent doc (§"Re-derivable indexes") gives each adapter discretion on the build strategy. M4-2b resolves this for S3:

**Recommended: load-once per bucket (this proposal).** Open `<bucket>/KEYMAP.jsonl` once at the top of `encodeBucketObjects`, build a `map[encodedSegment]KeymapRecord`, then look up each leaf during the existing `filepath.WalkDir` of the bucket's object tree. Rationale:

- **Per-bucket cost is O(records-in-keymap).** A 1 MiB-key cap × 4 MiB-line cap × hundreds of renames is still well under the snapshot builder's working-set budget. The Redis encoder already follows this pattern (`encode_redis.go:113-132`) without measured impact.
- **Lazy lookup is impractical.** Walking the keymap once per object would be O(objects × keymap-records), and the keymap needs O(1) reverse-lookup by encoded segment anyway.

**Out of scope (deferred):**

- **Cross-bucket keymaps.** Each bucket gets its own `KEYMAP.jsonl` because object keys are bucket-scoped; M4-2b does NOT support a single top-level keymap. The decoder enforces per-bucket already.
- **Reserved-prefix-collision keymaps (`_incomplete_uploads`, `_orphans`).** These subtrees are gated by `ErrEncodeUnsupportedS3IncompleteUploads` / `ErrEncodeUnsupportedS3Orphans` (codex P2 v21 #904) — M4-2b will NOT touch their renames. Any KEYMAP record whose original key starts with a reserved prefix is treated as a malformed dump and fails closed.
- **Cross-suffix collisions.** A `KindS3LeafData` rename target that also collides with `KindMetaCollision`'s reserved suffix is already rejected at decode time (`s3.go:892`) — the encoder inherits this invariant; no separate validation needed.

## Per-bucket keymap loading

New helper in `encode_s3_objects.go` (or a sibling file):

```go
// loadBucketKeymap reads <bucket>/KEYMAP.jsonl into an
// encoded-segment → KeymapRecord map. The file is optional (a bucket
// with no collisions has no keymap). On a malformed line the encoder
// fails closed with ErrInvalidKeymapRecord wrapped with the file path.
func (e *S3RecordEncoder) loadBucketKeymap(root *os.Root, bucketDir string) (map[string]KeymapRecord, error)
```

The existing `checkBucketKeymap` (`encode_s3_objects.go:103`) — which currently fails closed when `KEYMAP.jsonl` is present — becomes `loadBucketKeymap`. Its no-keymap path (returns nil, no error) and its hard-link / symlink / non-regular refusal paths are preserved.

**Keymap key is the full relative path, NOT per-segment.** The decoder's `recordKeymap` call site (`internal/backup/s3.go:728`) records the full filesystem-relative path produced by `resolveObjectFilename` (e.g. `path/to.elastickv-leaf-data`) as the `Encoded` field and the full original S3 object key (e.g. `path/to`) as the `OriginalB64`. Per-segment lookup would miss every nested-collision entry; the encoder MUST look up the full rel-path (gemini medium / codex P1 found in this doc's first-round review).

## Object-walk integration

`encodeBucketObjects`' existing `filepath.WalkDir` continues to enumerate the bucket's `s3/<bucketDir>/**/*` tree. For each file (sidecar or body), the leaf walker resolves the on-disk relative path to the original S3 object key by:

1. Compute the relative path under `bucketDir` (e.g. `path/to.elastickv-leaf-data`) — same string the decoder passed as `Encoded` to `recordKeymap`.
2. Look it up directly in the keymap map. If a record is found, decode `OriginalB64` to get the full original object key (e.g. `path/to`) — no suffix-stripping needed; the decoder already stored the un-suffixed original.
3. If no record is found, the on-disk relative path IS the original object key (the no-collision case M4-2a already handles).

The resolved key feeds the existing `!s3|obj|head|<key>` / `!s3|blob|<key>` emission unchanged.

**Difference from Redis.** Redis's `loadKeymap` (`encode_redis.go:113`) handles only `KindSHAFallback`, where the keymap entry is keyed by a single filename segment (because filename-length overflow happens segment-by-segment). S3 collision records are scoped to the full object key, so the lookup unit is the full bucket-relative path rather than a single segment. No common helper between the two; the S3 lookup lives in `encode_s3_collision.go`.

## Error contract

The encoder fails closed with the existing per-adapter sentinel `ErrS3EncodeInvalidBucket` (wrapping `ErrInvalidKeymapRecord`) on:

- Malformed JSON in `KEYMAP.jsonl`.
- A `KindS3LeafData` record whose `Encoded` does not end in `.elastickv-leaf-data`.
- A `KindMetaCollision` record whose original key does not end in `.elastickv-meta.json`.
- An original key whose first top-level path component is exactly one of the dump-control reserved subtrees: `_incomplete_uploads` or `_orphans`. (User object keys like `_foo` or `_foo/bar` are LEGITIMATE — the whole `_` namespace is NOT reserved, only the two named subtrees are. Codex P2 found this in the first-round review of this doc.)
- A keymap record referencing an on-disk segment that doesn't exist (orphan record — the dump is internally inconsistent).
- A multiply-defined `Encoded` segment (the same on-disk name listed twice with different originals).

`ErrS3EncodeUnsupportedCollision` is removed once M4-2b lands; its existing test (`encode_s3_objects_test.go:300`) is rewritten to assert the round-trip succeeds on the same fixture rather than rejecting it.

## Self-test cross-check

Each existing M4-2a self-test fixture (`encode_s3_objects_test.go`) gets a sibling that:

1. Sets up a bucket with both `path/to` and `path/to/sub` keys (leaf-data rename).
2. Round-trips through `S3Encoder.Decode → S3RecordEncoder.Encode`.
3. Asserts the produced `!s3|obj|head|` keys are `path/to` and `path/to/sub` (NOT `path/to.elastickv-leaf-data`).

Plus a `--rename-collisions=true` variant for `KindMetaCollision`.

## Files to add / modify (M4-2b implementation slice)

```
internal/backup/encode_s3_collision.go        # loadBucketKeymap + resolveS3Segment
internal/backup/encode_s3_collision_test.go   # keymap parse, segment resolution, error paths
internal/backup/encode_s3_objects.go          # rewrite checkBucketKeymap call site + WalkDir leaf resolver
internal/backup/encode_s3_objects_test.go     # round-trip leaf-data + meta-collision; drop ErrS3EncodeUnsupportedCollision test
internal/backup/encode_s3.go                  # remove ErrS3EncodeUnsupportedCollision (no longer reachable)
```

## Milestones (within M4-2b)

The slice ships as a single PR since the keymap loader, segment resolver, and integration into `encodeBucketObjects` are tightly coupled (a partial landing would be incoherent — the decoder write contract and the encoder read contract are inseparable). Codex P1 v13 #904's design-doc-first discipline applies: this doc lands first, then the implementation in a follow-up PR.

## Test plan

| Test                                                                | Verifies                                                                                                                       |
| ------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `TestS3EncodeRoundTripsLeafDataCollision`                           | Bucket with `path/to` + `path/to/sub` → both objects emitted under their original keys; no `.elastickv-leaf-data` suffix leaks |
| `TestS3EncodeRoundTripsMetaCollision`                               | Bucket with user object key ending `.elastickv-meta.json` + `--rename-collisions` → original key recovered                     |
| `TestS3EncodeRejectsKeymapTargetingReservedSubtree`                 | `KEYMAP.jsonl` record claiming `Original="_orphans/foo"` or `"_incomplete_uploads/x"` → `ErrS3EncodeInvalidBucket`              |
| `TestS3EncodeAcceptsKeymapWithUserUnderscoreKey`                    | `KEYMAP.jsonl` record claiming `Original="_foo"` (legitimate user key) → round-trips successfully                              |
| `TestS3EncodeRejectsOrphanKeymapEntry`                              | `KEYMAP.jsonl` record references a segment that doesn't exist on disk → fail closed                                            |
| `TestS3EncodeRejectsDuplicateKeymapEntry`                           | Same `Encoded` listed twice → fail closed (we can't pick a winner)                                                             |
| `TestS3EncodeRejectsMalformedKeymapJSON`                            | `KEYMAP.jsonl` has an invalid line → `ErrInvalidKeymapRecord` (existing sentinel from `internal/backup/keymap.go`)             |
| `TestS3EncodeMissingKeymapIsValidNoCollisionDump`                   | Bucket without `KEYMAP.jsonl` continues to encode (the no-collision case M4-2a already covers)                                 |

## References

- Parent: `2026_05_25_partial_snapshot_logical_encoder.md` §"S3"
- Decoder write path: `internal/backup/s3.go:611-700` (`flushObjectWithCollision`, `closeBucketKeymap`)
- Keymap format: `internal/backup/keymap.go` (single source of truth for `KeymapRecord`)
- Sibling encoder pattern: `internal/backup/encode_redis.go:113-132` (Redis's `loadKeymap`)
- Predecessor PRs: M4-1 #847 (bucket meta), M4-2a #864 (object bodies, no collisions)
