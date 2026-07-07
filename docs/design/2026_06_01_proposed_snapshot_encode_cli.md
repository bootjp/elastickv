# `cmd/elastickv-snapshot-encode` CLI (Phase 0b M6) — proposed

**Status:** Proposed (no implementation yet).
**Parent:** [`2026_05_25_implemented_snapshot_logical_encoder.md`](2026_05_25_implemented_snapshot_logical_encoder.md) — this resolves the §"Encoder: `cmd/elastickv-snapshot-encode`" + §"Round-trip self-test" milestone (M6) the parent doc left at sketch level. Adapter slices M1–M5 are merged (#807/#841/#847/#849/#864/#846/#892); the parent doc was promoted `proposed` → `partial` in v6 (#896); the CLI is the capstone that exposes them.

## What needs to land

A single binary `cmd/elastickv-snapshot-encode` that:

1. Reads `MANIFEST.json` from `--input` (a directory tree produced by `elastickv-snapshot-decode` or by a future Phase 1 live extractor).
2. Calls `backup.EncodeSnapshot(EncodeOptions)` (new) which internally constructs the `snapshotBuilder` (unexported), invokes each enabled adapter encoder in deterministic order (redis → dynamodb → s3 → sqs; the same order `ENCODE_INFO.json` `adapters_enabled` reflects), runs the optional self-test against the in-memory bytes, and returns an `EncodeResult`. The `.fsm` bytes are written to the caller-supplied `io.Writer`; they are not returned.
3. **Write-then-rename atomic publish:** the `.fsm` bytes are written to `<output>.tmp-<random>`, fsynced + closed, and ONLY after a successful self-test (when `--self-test` is set) renamed atomically to `<output>`. A self-test failure leaves nothing at the restore-visible path — the temp file is `os.RemoveAll`'d in the failure path (codex P2 v2 #896). When `--self-test` is not set, the rename happens immediately after fsync; behavior matches the decoder's atomic-publish discipline.
4. Emits `<output>.encode_info.json` next to the renamed `.fsm` (provenance + integrity anchor; see §"`ENCODE_INFO.json`" for the path-derivation rule).
5. On `--self-test` failure: exit 2 with `<output>.mismatch.txt` (next to where the `.fsm` would have been) and no `.fsm` at the publish path.

**New library entrypoint (`backup.EncodeSnapshot`).** Codex P2 v2 #896 correctly flagged that `snapshotBuilder` / `newSnapshotBuilder` are unexported and the adapter `Encode` methods take `*snapshotBuilder`, so an external `main` package cannot directly wire them. M6 lands a high-level wrapper in `internal/backup/encode.go` mirroring the decoder's `DecodeSnapshot`:

```go
type EncodeOptions struct {
    InputRoot      string
    Adapters       AdapterSet
    LastCommitTS   uint64        // effective T (manifest or override)
    SelfTest       bool
    ScratchBase    string        // empty → os.TempDir(); always given a unique subdir
}

type EncodeResult struct {
    Header              SnapshotHeader  // returned by ReadSnapshot on the produced bytes
    BytesWritten        int64
    SHA256              [32]byte
    SelfTestRan         bool
    SelfTestMatched     bool
    SelfTestMismatchTxt []byte          // non-nil when SelfTestRan && !SelfTestMatched
}

func EncodeSnapshot(opts EncodeOptions, out io.Writer) (EncodeResult, error)
```

The CLI's responsibility is reduced to: read MANIFEST → call `EncodeSnapshot(opts, tempFile)` → on success rename + write sidecar → on `!SelfTestMatched` write `mismatch.txt` and exit 2. This keeps the builder unexported, matches the decoder's API shape, and unblocks future callers (Phase 1 live extractor, integration tests) that want encoder access without going through the CLI.

**Internal buffering model for self-test.** `out io.Writer` is write-only, so the self-test cannot read back the bytes that were just written. When `opts.SelfTest=true`, `EncodeSnapshot` writes the FSM into an internal `*bytes.Buffer` first, decodes from that buffer for the self-test, then copies the buffer to `out` only if the self-test matched. The memory cost is one extra FSM-sized allocation on top of the existing key-sort working set — the same memory bound the parent doc §"Encoder memory" already accepts. When `opts.SelfTest=false` the FSM streams straight to `out` with no extra buffering.

**Parameter order** matches the decoder's `DecodeSnapshot(r io.Reader, opts DecodeOptions)` shape by inversion: encoder writes (so `io.Writer` is the back), decoder reads (so `io.Reader` is the front). The asymmetry is deliberate so each function's I/O argument sits adjacent to the direction it travels.

## CLI surface

```
elastickv-snapshot-encode \
  --input  <dir>              # required, root containing MANIFEST.json
  --output <path>             # required, .fsm destination
  [--adapter dynamodb,s3,redis,sqs] # default: all (matching decoder convention)
  [--last-commit-ts <uint64>] # override; must be >= manifest value
  [--self-test]               # round-trip via decoder, default false
  [--scratch-root <dir>]      # self-test scratch (default: TempDir)
```

Mirrors the decoder's `--adapter` parsing (`"all"` shorthand; unknown names hard-error so a typo cannot silently disable an adapter). `--bundle-mode`, `--cluster-id`, and all `--include-*` flags are decode-side concerns and have no encode counterpart — the manifest already records what is in the tree.

**`--s3-chunk-size` deferred.** Parent doc §"Per-adapter reverse encoders" calls out an optional `--s3-chunk-size` flag (`encode_s3_objects.go` currently hardcodes `s3ChunkSize = 1 << 20`). M6 deliberately omits it: every dump the decoder produces today uses the canonical 1 MiB chunk size, and a non-default override only matters for restoring dumps from deployments that themselves run a non-default chunk size — a scenario that does not yet exist in this codebase. The flag is a follow-up; encoder hardcodes the canonical value.

**Exit codes:** 0 on success, 1 on user-input error (flag parse, missing file), 2 on data-correctness failure (manifest mismatch, encoder error, self-test diff). Same convention as the decoder; lets shell wrappers tell "user typed wrong" from "the tree is broken". A CLI flag whose value violates a data constraint derived from the manifest (e.g. `--last-commit-ts T < manifest.last_commit_ts`) is classified as exit 2, not exit 1 — the constraint comes from the data, not from the flag's syntax.

## `--last-commit-ts` override

Verbatim implementation of the parent doc §"MVCC re-encoding / --last-commit-ts T override semantics":

- Default (no flag): the EKVPBBL1 header's `lastCommitTS` and every key's `invTS = ^T` use `manifest.last_commit_ts` verbatim.
- With `--last-commit-ts T`: same value is used for BOTH the header AND every key's `invTS` (single atomic substitution; the uniform-stamping invariant is preserved).
- **Fail-closed validation:** reject `T < manifest.last_commit_ts`. A lower ceiling would let the restored node's leader issue a read ts ≤ a restored row's commit ts — the HLC-ceiling regression CLAUDE.md forbids. Equality is the default; raising the ceiling is always safe.

Pinned by `TestCLIRejectsLowerLastCommitTSOverride`.

## `ENCODE_INFO.json` (provenance + integrity)

A new sidecar emitted next to the output `.fsm`, NOT inside it (the EKVPBBL1 byte format is fixed). **Sidecar filename is derived from the `.fsm` path**, not a static `ENCODE_INFO.json` — multiple `.fsm` files can share a directory (e.g., per-node dumps under `/backups/`), and a static name would silently overwrite siblings (gemini medium #896). The convention is:

```
<output>.encode_info.json
```

i.e. `/backups/node1.fsm` → `/backups/node1.fsm.encode_info.json`. Same scheme `gpg` and `sha256sum` follow when their input is path-addressable. Tests pin both the single-file case and the two-files-same-dir case (`TestCLIEncodeInfoPathDerivedFromOutput`, `TestCLIEncodeInfoTwoFilesNoCollision`).

Keep the schema minimal — restore operators need to confirm "encoded for the right cluster, by the right encoder version, against this exact file":

```json
{
  "format_version": 1,
  "encoder_version":          "<git rev or release tag>",
  "encoder_key_format_version": 1,
  "wall_time_iso":            "2026-06-01T...Z",
  "input_root":               "<--input value>",
  "output_fsm_path":          "<--output value>",
  "output_fsm_sha256":        "<lowercase hex>",
  "last_commit_ts":           18446744073709551615,
  "last_commit_ts_overridden": false,
  "manifest_last_commit_ts":  18446744073709551615,
  "manifest_cluster_id":      "<from MANIFEST.json>",
  "adapters_enabled":         ["redis","dynamodb","s3","sqs"],
  "self_test": {
    "ran":     true,
    "matched": true
  }
}
```

`output_fsm_sha256` is computed AFTER `WriteTo` returns and BEFORE writing the sidecar. This is the parent doc's note on repurposing the dead `Source.FSMCRC32C` field as a SHA-256 — see §"Decoder cleanup" below; the encoder side already records SHA-256 here so the sidecar is the integrity anchor regardless of whether the manifest field ever gets repurposed.

**`encoder_version` stamping** mirrors the decoder's pattern at `cmd/elastickv-snapshot-decode/main.go:45`: a package-level `var version = "dev"` populated at build time via `-ldflags "-X main.version=$(git rev-parse HEAD)"`. Test builds keep the literal `"dev"` so `TestCLIRoundTripSelfTestAllAdapters` can assert the field is present without depending on a release tag.

**`adapters_enabled` ordering** is the canonical encoder fan-out order (`redis`, `dynamodb`, `s3`, `sqs`), NOT the CLI input order. The encoder dispatches in that fixed order to keep `ENCODE_INFO.json` bytewise reproducible across runs that pass `--adapter` in different sequences. Reproducibility is also why the schema lists strings rather than ordinals.

The encoder fan-out order (`redis` first) intentionally differs from the decoder's finalize order at `decode.go:278-296` (`dynamodb` first). The final `.fsm` byte sequence is determined by encoded key sort (`snapshotBuilder.WriteTo` sorts entries before writing — see `encode.go:204`), not by adapter fan-out order, so either ordering is correct as long as it is fixed. The two sides pick differently for purely historical reasons: the decoder finalizes in alphabetical-with-tweaks order; the encoder follows the parent design doc's enumeration order. No-op for restore correctness.

**`cluster_id` enforcement** is restore-prepare-side (`cmd/elastickv-snapshot-prepare-restore` refuses to create a target data dir when the sidecar cluster_id differs from the target node — parent §"Risks"). The encoder just propagates the manifest value into `ENCODE_INFO.json`; it does not gate on it.

## Round-trip self-test (`--self-test`)

The encoder's correctness gate. Parent doc §"Round-trip self-test" spec:

```text
dirTree  --encode-->  .fsm  --decode-->  dirTree'
assert dirTree == dirTree'   (excluding wall-time + encoder-provenance fields)
```

Implementation:

1. After the primary `.fsm` is written + sha256-anchored, invoke `backup.DecodeSnapshot` on the produced bytes into a unique scratch subdirectory. The scratch dir is ALWAYS a fresh `encode-self-test-<random>` subdirectory under the resolved base (default base `os.TempDir()`; operator override via `--scratch-root`). The unique-suffix is non-optional even when `--scratch-root` is pinned — concurrent encodes against the same pinned base would otherwise collide and produce false failures (gemini medium #896).
2. **DecodeOptions are read back from the input MANIFEST.json.** The self-test MUST feed `DecodeSnapshot` the same option flags the decoder used to produce `--input`, otherwise a tree produced with (e.g.) `--include-incomplete-uploads` or `--dynamodb-bundle-mode jsonl` will spuriously fail the diff (codex P2 v3 #896). The CLI reads `MANIFEST.json` `Exclusions.{IncludeIncompleteUploads,IncludeOrphans,PreserveSQSVisibility,IncludeSQSSideRecords,RenameS3Collisions}` and the top-level `DynamoDBLayout` field, and threads them into the scratch `DecodeOptions` (field name `DynamoDBBundleJSONL bool` — set to `true` when `manifest.DynamoDBLayout == "jsonl"`, per `decode.go:89`). Manifests that omit these fields (older dumps) decode with defaults — same forgiveness the decoder applies. Pinned by `TestCLISelfTestPreservesManifestDecodeOptions`.

> **Schema extension folded into M6.** `Exclusions` currently lacks `RenameS3Collisions` even though the decoder CLI's `--rename-collisions` flag drives `DecodeOptions.RenameS3Collisions` (codex P2 v4 #896): dumps produced with that flag CANNOT be perfectly self-tested today because the encoder has no way to know to ask for the same renaming pass. M6 adds the field to the `Exclusions` struct + JSON tag (`rename_s3_collisions`) and updates the decoder CLI's `emitManifest` to populate it. **The new field is intentionally NOT added to `exclusionsRequiredFields` (`manifest.go:350-355`)** so `ReadManifest` treats its absence as the zero value `false` (= no-rename, matching the decoder default). This is an intentional asymmetry with the four pre-existing `Exclusions` fields, all of which ARE in the required list: backward compatibility with every existing manifest (which doesn't emit `rename_s3_collisions`) is the constraint, and refusing them would be an unannounced breaking change. New manifests written by the M6 PR's `emitManifest` will always include the field; older manifests fall through to the zero value via the same `json.Unmarshal` default that protects every other new optional field added to `Manifest`. Pinned by `TestExclusionsLegacyManifestOmitsRenameS3Collisions`.
>
> **Decoder-cleanup milestone attribution.** Parent doc said "Decoder cleanup folded into M1." M1 shipped without it. M6 picks up both the `RenameS3Collisions` schema extension above and the `Source.FSMCRC32C` → SHA-256 repurpose (§"Decoder cleanup folded in" below). The deferral is noted here so a reader diffing the two docs knows M6 is now the authoritative landing site.
3. **Header check (no MANIFEST.json in scratch):** `DecodeSnapshot` returns a `DecodeResult` whose `Header.LastCommitTS` is the value the decoder read from the produced `.fsm`. Compare it against the effective `T` (manifest value or `--last-commit-ts` override). The decoder library does NOT emit `MANIFEST.json` (that is the cmd wrapper's job per `internal/backup/decode.go`); the self-test deliberately consumes the library output and compares the in-memory header instead, avoiding a redundant manifest synthesis step (codex P2 v1 #896).
4. **Tree diff:** structurally compare the scratch adapter subtrees against the corresponding subdirs of `--input`. The diff is byte-equal on adapter files (filenames + bytes — every per-adapter dump is deterministic). `MANIFEST.json` from `--input` is NOT compared at all in the scratch tree (the scratch has none); the `last_commit_ts` field is covered by the header check above. `wall_time_iso` and other manifest transient fields are not in scope.
5. **Eager cleanup.** The scratch subdir is removed via `os.RemoveAll` on ALL exit paths (success, mismatch, encoder error), in a `defer` set up immediately after `MkdirTemp` returns. A stale `mismatch.txt` next to the output `.fsm` is removed at the start of every run (success or fresh-failure), so the file is always the latest run's record, never a stale one.
6. **On mismatch:** exit code 2; `mismatch.txt` (next to `.fsm`, named `<output>.mismatch.txt` per the same path-derivation rule as the sidecar) lists the first N differing paths + the header check result; `ENCODE_INFO.json` records `self_test.matched: false` before the exit. Restore operators inspect `<output>.mismatch.txt`.

**Why default-off:** self-test doubles encode time and uses 2× disk. It is the gold-standard correctness check, but most operational uses (re-encode a known-good tree for restore) skip it. CI runs `--self-test` on every encoder PR; the M6 CLI test file `TestCLIRoundTripSelfTest` does too.

## Files to add (M6 implementation slice)

```
internal/backup/encode_snapshot.go              # EncodeSnapshot wrapper (new public entrypoint)
internal/backup/encode_snapshot_test.go         # library-level round-trip, self-test, override validation
internal/backup/encode_info.go                  # ENCODE_INFO.json struct + WriteEncodeInfo / ReadEncodeInfo helpers
internal/backup/encode_info_test.go             # round-trip JSON, version gate
cmd/elastickv-snapshot-encode/main.go           # flag parsing, temp-file dance, ENCODE_INFO emission
cmd/elastickv-snapshot-encode/main_test.go      # CLI-level tests (exit codes, write-then-rename atomicity)
```

`internal/backup/encode_snapshot.go` mirrors the decoder's `DecodeSnapshot`-in-the-same-package convention: the high-level wrapper, the per-adapter encoders, and `snapshotBuilder` all live together; `main` only handles flag parsing + filesystem-level concerns (temp dance, sidecar). `internal/backup/encode_info.go` lives in the encoder package because the schema is encoder-specific (decoder never writes it) and the writers in the package can reuse `WriteManifest`'s Sync+Close hardening (gemini r1 medium on #810).

## Test plan

| Test | Verifies |
|---|---|
| `TestCLIRoundTripSelfTestAllAdapters` | Build a multi-adapter fixture tree (Redis strings+hashes, DDB items+GSI, S3 bucket+object, SQS FIFO with dedup) → run CLI with `--self-test` → assert exit 0 and `ENCODE_INFO.json` reports `matched:true`. **Gold-standard.** |
| `TestCLIRejectsLowerLastCommitTSOverride` | `--last-commit-ts T < manifest` → exit 2 with the HLC-ceiling error, no `.fsm` written. Fail-closed pin. |
| `TestCLIAcceptsEqualAndHigherLastCommitTSOverride` | `T == manifest` and `T > manifest` both succeed; the emitted `.fsm` header carries the chosen `T`. |
| `TestCLIRejectsMissingManifest` | `--input` directory has no `MANIFEST.json` → exit 1, clear error. |
| `TestCLIRejectsUnknownAdapter` | `--adapter foo` → exit 1, no `.fsm` written. Decoder-parity test for the adapter CSV parser. |
| `TestEncodeSnapshotSelfTestDetectsCorruption` | Use a same-package test-only hook (an unexported `EncodeOptions` field — convention `corruptBufferForTest func([]byte)`) that runs against the internal buffer AFTER `WriteTo` returns but BEFORE the self-test decodes. The corruption thus reaches the self-test's `DecodeSnapshot` but never reaches `out` (a self-test failure must NOT publish the corrupt bytes per the write-then-rename rule above). The decode either errors or produces a diff; the CLI exits 2 and writes `mismatch.txt`. The hook is unexported and resides in the encoder package's test file, so external callers cannot set it — pins that the self-test actually compares (not just runs) without exposing a corrupted-output API on the public surface (codex P2 v6 #896 — wrapping `out io.Writer` would corrupt bytes after self-test had already passed). |
| `TestEncodeInfoRoundTrip` | `WriteEncodeInfo` → `ReadEncodeInfo` of the same struct equal. Forward-compat: an `ENCODE_INFO.json` with extra fields decodes cleanly. |
| `TestEncodeInfoRejectsUnknownFormatVersion` | format_version != 1 → typed error, mirroring decoder's `TestManifestVersionGate`. |
| `TestCLIEncodeInfoPathDerivedFromOutput` | `--output /tmp/a.fsm` produces `/tmp/a.fsm.encode_info.json`, not `/tmp/ENCODE_INFO.json` (pins gemini medium #896). |
| `TestCLIEncodeInfoTwoFilesNoCollision` | Two `--output` paths in the same dir produce two distinct sidecars; second run does not overwrite the first. |
| `TestCLISelfTestPinnedScratchRootStillUniqueSubdir` | `--scratch-root=/tmp/pinned` produces `/tmp/pinned/encode-self-test-<random>/`, not `/tmp/pinned/` itself — concurrent runs do not collide (gemini medium #896). |
| `TestCLISelfTestCleansScratchOnAllPaths` | Scratch subdir is removed via `os.RemoveAll` on success, mismatch, AND encoder-error paths (pinned via a t.Helper that asserts the dir is gone after the CLI returns). |
| `TestCLISelfTestFailureLeavesNoFsmAtOutputPath` | With `--self-test` enabled and a deliberately-mangled encoder: the corrupt `.fsm` is NEVER visible at `--output` (the publish path); the temp file is removed; exit 2 (codex P2 v2 #896). Pins the write-then-rename atomic-publish discipline. |
| `TestEncodeSnapshotLibraryRoundTrip` | `backup.EncodeSnapshot` + immediate `DecodeSnapshot` on the returned bytes produces an equivalent adapter tree without going through `cmd/`. Pins the library entrypoint independently of the CLI, so future callers (Phase 1 live extractor, integration tests) have a tested API surface (codex P2 v2 #896 — encoder entrypoint exposure). |
| `TestCLISelfTestPreservesManifestDecodeOptions` | A `--input` tree produced with `--include-incomplete-uploads` and `--dynamodb-bundle-mode jsonl` is re-encoded with `--self-test`; the test asserts the scratch tree has the corresponding sections (does not spuriously diff because of default-vs-set option mismatch). Pins codex P2 v3 #896 — DecodeOptions are threaded from MANIFEST.json into the self-test, not hardcoded. |
| `TestCLISelfTestPreservesRenameS3Collisions` | A `--input` tree produced with `--rename-collisions` (so the manifest carries `Exclusions.RenameS3Collisions=true`) is re-encoded with `--self-test`; the test asserts the scratch decode reads the manifest field, threads it back, and produces a matching renamed-collision tree. Pins codex P2 v4 #896 — the new `Exclusions.RenameS3Collisions` field is in the round-trip. |
| `TestExclusionsLegacyManifestOmitsRenameS3Collisions` | An older MANIFEST.json without `rename_s3_collisions` field decodes with the zero value (`false`), matching decoder default — no fail-on-missing. Forward-compat regression pin. |

**P1 cluster-boot test deferred.** Parent doc's P1 test table includes `TestEncoderProducesLoadableSnapshot` — an end-to-end test that boots a single-node cluster from the encoded `.fsm` and reads back every adapter's data. That test is deferred from M6 because: (a) it requires the production server entrypoint + Pebble engine, not the encoder package; (b) `TestEncodeSnapshotLibraryRoundTrip` above already exercises the encoder → decoder library round-trip exactly, and the `TestCLIRoundTripSelfTestAllAdapters` integration test exercises the full CLI; (c) the cluster-boot path is owned by `store/lsm_store.go`'s native restore code, which has its own coverage. M6 tracks the cluster-boot test as a follow-up task in the M6 PR description, not in this design.

## Self-review (5 passes) — proposed

1. **Data loss.** The CLI is a wrapper over already-merged + tested encoder slices. The only new write paths are the `.fsm` output and `ENCODE_INFO.json`; both use the WriteManifest-style fsync-then-close discipline (gemini r1 medium on #810). The self-test gate is a defense-in-depth check that catches encoder regressions before a restore operator does.
2. **Concurrency / distributed failures.** Pure offline. No Raft, no HLC issuance, no cluster contact. The output `.fsm` is loaded by a STOPPED node via stop-replace-restart (parent §"Restore via stop-replace-restart"); concurrency surfaces only on the receiving cluster's restart path, which is owned by the node's existing snapshot loader.
3. **Performance.** Encode is O(records in dump); each adapter's Encode is O(its inputs). Self-test doubles this. No hot-loop allocations introduced by the CLI itself (it just wires existing constructors). Big trees may want `GOGC` tuning; documented in the file header, not a flag.
4. **Data consistency.** `--last-commit-ts T` validation is the load-bearing invariant — `T ≥ manifest.last_commit_ts` is fail-closed (pinned by `TestCLIRejectsLowerLastCommitTSOverride`). Self-test compares against the effective `T`, not the manifest value, per parent doc requirement. Adapter set passed to the CLI matches the adapter set the encoders walk — same parse-and-validate function as the decoder (`parseAdapterSet`) so a typo cannot silently downgrade scope.
5. **Test coverage.** Seventeen tests cover the four user-visible behaviors (round-trip, override validation, scope parsing, missing-manifest), the two safety nets (self-test catches corruption, ENCODE_INFO schema versioned), and nine robustness regressions identified in #896 review (per-`.fsm` sidecar naming, two-files-no-collision, unique scratch subdir under pinned root, eager cleanup on every exit path, write-then-rename atomic publish on self-test failure, library entrypoint round-trip, self-test threads MANIFEST DecodeOptions, new `RenameS3Collisions` field round-trips, legacy manifests without the new field decode safely). Adapter-internal coverage is already pinned by M1–M5 PRs and is not re-tested here.

## Decoder cleanup folded in (parent doc §"Decoder cleanup")

The parent doc flags `internal/backup/manifest.go` `Source.FSMCRC32C` as dead (native `.fsm` has no CRC32C footer). M6 either removes it or repurposes it as a whole-file SHA-256. **Recommended: repurpose.** The CLI already computes `output_fsm_sha256` for `ENCODE_INFO.json`; populating `Source.FSMCRC32C` (or a renamed `FSMSHA256` field) with the same value gives the manifest a per-file integrity anchor without needing the sidecar. M6 lands this rename in the same PR; the field is JSON-tagged so existing decoder consumers reading `fsm_crc32c` continue to work until a deprecation window closes.

If the rename grows into a larger schema migration, M6 falls back to **removing** the dead field; the sidecar's SHA-256 is sufficient. Decision deferred to implementation review.

## Milestones / sequencing

M6 lands as a **single PR** (this doc + implementation + tests). No further splits — the wiring is small and the self-test is integral to the CLI's value proposition; splitting it out would mean shipping an untested CLI first.

## Open questions

None at proposal time. Reviewers: please flag if there is a known case where the decoder leaves transient state in `MANIFEST.json` that would defeat the structural diff (the test plan's `TestCLIRoundTripSelfTestAllAdapters` will catch this in CI, but a known-skip list documented here would shorten the debug loop).
