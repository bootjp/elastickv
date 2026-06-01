# `cmd/elastickv-snapshot-encode` CLI (Phase 0b M6) — proposed

**Status:** Proposed (no implementation yet).
**Parent:** [`2026_05_25_proposed_snapshot_logical_encoder.md`](2026_05_25_proposed_snapshot_logical_encoder.md) — this resolves the §"Encoder: `cmd/elastickv-snapshot-encode`" + §"Round-trip self-test" milestone (M6) the parent doc left at sketch level. Adapter slices M1–M5 are merged (#807/#841/#847/#849/#864/#846/#892); the CLI is the capstone that exposes them.

## What needs to land

A single binary `cmd/elastickv-snapshot-encode` that:

1. Reads `MANIFEST.json` from `--input` (a directory tree produced by `elastickv-snapshot-decode` or by a future Phase 1 live extractor).
2. Constructs a `snapshotBuilder` stamped with the manifest's `last_commit_ts` (or an operator override — see §"`--last-commit-ts` override").
3. Invokes each enabled adapter encoder (`NewRedisEncoder`, `NewDynamoDBEncoder`, `NewS3RecordEncoder`, `NewSQSRecordEncoder`) on the same input root in deterministic order, each adding its records to the builder.
4. Writes the builder out to `--output` (a `.fsm` file path) via `snapshotBuilder.WriteTo`.
5. Emits `ENCODE_INFO.json` next to the output `.fsm` (provenance + integrity anchor).
6. Optionally runs the round-trip self-test (`--self-test`): re-decode the produced `.fsm` into a scratch dir and diff against the input tree.

The CLI is the **only new external surface**; the encoder slices already export their constructors and `Encode(*snapshotBuilder) error`. M6 is mostly wiring + provenance + the self-test gate.

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

**Exit codes:** 0 on success, 1 on user-input error (flag parse, missing file), 2 on data-correctness failure (manifest mismatch, encoder error, self-test diff). Same convention as the decoder; lets shell wrappers tell "user typed wrong" from "the tree is broken".

## `--last-commit-ts` override

Verbatim implementation of the parent doc §"MVCC re-encoding / --last-commit-ts T override semantics":

- Default (no flag): the EKVPBBL1 header's `lastCommitTS` and every key's `invTS = ^T` use `manifest.last_commit_ts` verbatim.
- With `--last-commit-ts T`: same value is used for BOTH the header AND every key's `invTS` (single atomic substitution; the uniform-stamping invariant is preserved).
- **Fail-closed validation:** reject `T < manifest.last_commit_ts`. A lower ceiling would let the restored node's leader issue a read ts ≤ a restored row's commit ts — the HLC-ceiling regression CLAUDE.md forbids. Equality is the default; raising the ceiling is always safe.

Pinned by `TestCLIRejectsLowerLastCommitTSOverride`.

## `ENCODE_INFO.json` (provenance + integrity)

A new sidecar emitted next to the output `.fsm`, NOT inside it (the EKVPBBL1 byte format is fixed). Keep the schema minimal — restore operators need to confirm "encoded for the right cluster, by the right encoder version, against this exact file":

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

**`cluster_id` enforcement** is decoder-side (the restore runbook step refuses to place a file whose cluster_id differs from the target node — parent §"Risks"). The encoder just propagates the manifest value into `ENCODE_INFO.json`; it does not gate on it.

## Round-trip self-test (`--self-test`)

The encoder's correctness gate. Parent doc §"Round-trip self-test" spec:

```text
dirTree  --encode-->  .fsm  --decode-->  dirTree'
assert dirTree == dirTree'   (excluding wall-time + encoder-provenance fields)
```

Implementation:

1. After the primary `.fsm` is written + sha256-anchored, invoke `backup.DecodeSnapshot` on the produced bytes into a scratch directory (default `os.TempDir()/encode-self-test-<random>/`; operator can pin via `--scratch-root` for tmpfs).
2. Diff the scratch tree against `--input`. The diff is structural, not byte-equal on transient fields:
   - **Excluded from compare:** `MANIFEST.json` `wall_time_iso`, any future `phase`-specific provenance, the `MANIFEST.json` `source.fsm_path` (decoder's record of where it read from, not data).
   - **Required equal:** every adapter subdir's full tree (filenames + bytes), `MANIFEST.json` adapter-scope arrays, `last_commit_ts`.
3. On mismatch: the CLI exits with code 2, writes a `mismatch.txt` next to the output `.fsm` listing the first N differing paths, and the `ENCODE_INFO.json` records `self_test.matched: false` before the exit. Restore operators inspect `mismatch.txt`.

**Why default-off:** self-test doubles encode time and uses 2× disk. It is the gold-standard correctness check, but most operational uses (re-encode a known-good tree for restore) skip it. CI runs `--self-test` on every encoder PR; the M6 CLI test file `TestCLIRoundTripSelfTest` does too.

## Files to add (M6 implementation slice)

```
cmd/elastickv-snapshot-encode/main.go           # flag parsing, orchestration, ENCODE_INFO emission
cmd/elastickv-snapshot-encode/main_test.go      # CLI-level tests (round-trip, override validation, exit codes)
internal/backup/encode_info.go                  # ENCODE_INFO.json struct + WriteEncodeInfo / ReadEncodeInfo helpers
internal/backup/encode_info_test.go             # round-trip JSON, version gate
```

`internal/backup/encode_info.go` lives in the encoder package because the schema is encoder-specific (decoder never writes it) and the writers in the package can reuse `WriteManifest`'s Sync+Close hardening (gemini r1 medium on #810).

## Test plan

| Test | Verifies |
|---|---|
| `TestCLIRoundTripSelfTestAllAdapters` | Build a multi-adapter fixture tree (Redis strings+hashes, DDB items+GSI, S3 bucket+object, SQS FIFO with dedup) → run CLI with `--self-test` → assert exit 0 and `ENCODE_INFO.json` reports `matched:true`. **Gold-standard.** |
| `TestCLIRejectsLowerLastCommitTSOverride` | `--last-commit-ts T < manifest` → exit 2 with the HLC-ceiling error, no `.fsm` written. Fail-closed pin. |
| `TestCLIAcceptsEqualAndHigherLastCommitTSOverride` | `T == manifest` and `T > manifest` both succeed; the emitted `.fsm` header carries the chosen `T`. |
| `TestCLIRejectsMissingManifest` | `--input` directory has no `MANIFEST.json` → exit 1, clear error. |
| `TestCLIRejectsUnknownAdapter` | `--adapter foo` → exit 1, no `.fsm` written. Decoder-parity test for the adapter CSV parser. |
| `TestCLISelfTestDetectsCorruption` | Patch the in-tree encoder to deliberately mangle one byte after writing the `.fsm`; `--self-test` catches the diff, exits 2, writes `mismatch.txt`. Pins that the self-test actually compares (not just runs). |
| `TestEncodeInfoRoundTrip` | `WriteEncodeInfo` → `ReadEncodeInfo` of the same struct equal. Forward-compat: an `ENCODE_INFO.json` with extra fields decodes cleanly. |
| `TestEncodeInfoRejectsUnknownFormatVersion` | format_version != 1 → typed error, mirroring decoder's `TestManifestVersionGate`. |

## Self-review (5 passes) — proposed

1. **Data loss.** The CLI is a wrapper over already-merged + tested encoder slices. The only new write paths are the `.fsm` output and `ENCODE_INFO.json`; both use the WriteManifest-style fsync-then-close discipline (gemini r1 medium on #810). The self-test gate is a defense-in-depth check that catches encoder regressions before a restore operator does.
2. **Concurrency / distributed failures.** Pure offline. No Raft, no HLC issuance, no cluster contact. The output `.fsm` is loaded by a STOPPED node via stop-replace-restart (parent §"Restore via stop-replace-restart"); concurrency surfaces only on the receiving cluster's restart path, which is owned by the node's existing snapshot loader.
3. **Performance.** Encode is O(records in dump); each adapter's Encode is O(its inputs). Self-test doubles this. No hot-loop allocations introduced by the CLI itself (it just wires existing constructors). Big trees may want `GOGC` tuning; documented in the file header, not a flag.
4. **Data consistency.** `--last-commit-ts T` validation is the load-bearing invariant — `T ≥ manifest.last_commit_ts` is fail-closed (pinned by `TestCLIRejectsLowerLastCommitTSOverride`). Self-test compares against the effective `T`, not the manifest value, per parent doc requirement. Adapter set passed to the CLI matches the adapter set the encoders walk — same parse-and-validate function as the decoder (`parseAdapterSet`) so a typo cannot silently downgrade scope.
5. **Test coverage.** Eight tests above cover the four user-visible behaviors (round-trip, override validation, scope parsing, missing-manifest) plus the two safety nets (self-test catches corruption, ENCODE_INFO schema versioned). Adapter-internal coverage is already pinned by M1–M5 PRs and is not re-tested here.

## Decoder cleanup folded in (parent doc §"Decoder cleanup")

The parent doc flags `internal/backup/manifest.go` `Source.FSMCRC32C` as dead (native `.fsm` has no CRC32C footer). M6 either removes it or repurposes it as a whole-file SHA-256. **Recommended: repurpose.** The CLI already computes `output_fsm_sha256` for `ENCODE_INFO.json`; populating `Source.FSMCRC32C` (or a renamed `FSMSHA256` field) with the same value gives the manifest a per-file integrity anchor without needing the sidecar. M6 lands this rename in the same PR; the field is JSON-tagged so existing decoder consumers reading `fsm_crc32c` continue to work until a deprecation window closes.

If the rename grows into a larger schema migration, M6 falls back to **removing** the dead field; the sidecar's SHA-256 is sufficient. Decision deferred to implementation review.

## Milestones / sequencing

M6 lands as a **single PR** (this doc + implementation + tests). No further splits — the wiring is small and the self-test is integral to the CLI's value proposition; splitting it out would mean shipping an untested CLI first.

## Open questions

None at proposal time. Reviewers: please flag if there is a known case where the decoder leaves transient state in `MANIFEST.json` that would defeat the structural diff (the test plan's `TestCLIRoundTripSelfTestAllAdapters` will catch this in CI, but a known-skip list documented here would shorten the debug loop).
