# Stage 8a — Snapshot header v2 (`raft_envelope_cutover_index` carriage)

| Field | Value |
|---|---|
| Status | implemented |
| Date | 2026-05-29 |
| Implemented | 2026-07-07 status audit; code landed before this doc promotion |
| Parent designs | [`2026_04_29_partial_data_at_rest_encryption.md`](2026_04_29_partial_data_at_rest_encryption.md) (§4.4 snapshot envelope, §7.1 Phase-2 cutover) |
| Builds on | Stage 5 (sidecar `raft_envelope_cutover_index` field), forthcoming Stage 6E (`enable-raft-envelope` admin RPC populates the index) |
| Sibling slice | Stage 8b — WAL coverage closure; implemented as a no-code analysis in [`2026_06_01_implemented_8b_wal_coverage_closure.md`](2026_06_01_implemented_8b_wal_coverage_closure.md). |

## Implementation audit

The implementation is present in `kv/snapshot.go`, `kv/fsm.go`,
`kv/fsm_applied_index_iface_check.go`, and the raft snapshot skip
path in `internal/raftengine/etcd/wal_store.go`.
The test coverage called out in §5 is present in `kv/snapshot_test.go`
and `internal/raftengine/etcd/wal_store_skip_gate_test.go`.

## 0. Why this slice exists

The FSM snapshot stream today carries an 8-byte HLC ceiling in a
fixed 16-byte header (`kv/snapshot.go`'s `hlcSnapshotMagic` = the
literal string `EKVTHLC1`). Once Stage 6E lands the `enable-raft-
envelope` cutover, every Phase-2-aware snapshot needs to carry one
more field — `raft_envelope_cutover_index` — so a node that restores
from snapshot can know whether the subsequent Raft log entries are
plaintext-wrapped (pre-cutover indices) or AEAD-wrapped (post-
cutover indices) without consulting the sidecar.

Without 8a, a node that restores a Phase-2 snapshot would either:
- Miss the cutover index entirely and try to unwrap plaintext entries
  (or, conversely, pass through enveloped entries un-unwrapped), or
- Be forced to side-channel the cutover index out-of-band through
  the sidecar, which forks the source of truth for FSM-level
  decisions away from the snapshot it was applied against.

8a closes that gap by versioning the snapshot header — `EKVTHLC1`
stays the on-disk format for Phase-0/Phase-1 snapshots, `EKVTHLC2`
adds the cutover index field. The parent design (§4.4) has the full
format spec; this slice is the implementation cut.

## 1. Scope

### In scope

- New constant `hlcSnapshotMagicV2 = [8]byte{'E','K','V','T','H','L','C','2'}`
  in `kv/snapshot.go`.
- New v2 header layout: `magic(8) | len(2, big-endian uint16) |
  ceiling(8) | cutover(8)`. `len = 0x0010` (16 bytes — the two
  defined fields). Forward-compat hatch: v2 readers ignore bytes
  past the fields they understand, sized by `len`.
- `ReadSnapshotHeader` (refactored from today's `readHLCCeiling` or
  equivalent) — peek-and-discriminate logic per parent §4.4:
  - `EKVTHLC2` → consume + parse v2 payload (ceiling + cutover +
    skip trailing bytes per `len`).
  - `EKVTHLC1` → consume + parse v1 payload (ceiling; cutover = 0).
  - Else if leading 7 bytes are `EKVTHLC*` → an unknown future
    version → `ErrSnapshotHeaderUnknownMagic`.
  - Else (no `EKVTHLC` prefix at all) → headerless legacy snapshot;
    return `(ceiling=0, cutover=0)` AND leave the peeked bytes in
    the underlying stream for the inner-store restore path.
- `WriteSnapshotHeader` (refactored from today's `writeHLCCeiling`):
  - v1 layout when the local node has NOT applied an
    `enable-raft-envelope` entry (sidecar's
    `raft_envelope_cutover_index == 0`).
  - v2 layout once the local node observes
    `raft_envelope_cutover_index != 0`. Never downgrades back to v1
    even if a subsequent rotation clears the field (that scenario
    is out of scope — rotation does not retract cutover).
- `ErrSnapshotHeaderUnknownMagic` typed error in `kv/snapshot.go`
  for the `EKVTHLC<unknown>` family path.
- `kv/fsm.go::Restore` updated to consume the new
  `(ceiling, cutover)` tuple and thread the cutover into the
  applier / engine pre-apply hook so subsequent Raft entries route
  through the correct wrap/unwrap path.

### Out of scope

- **WAL / Raft log encryption on disk.** The sibling Stage 8b closure
  owns this analysis. (The parent doc's Stage 8 table row originally labeled both
  slices as "Snapshot header v2 + WAL coverage (§4.4, §4.5)", but
  §4.5 in the parent is actually "Distribution catalog and HLC
  ceiling entries"; WAL-file encryption is a separate concern that
  8b formally closes as a no-code decision.) Not blocked on 8a;
  conceptually independent.
- **Snapshot stream encryption**. Per parent §4.4, the FSM snapshot
  stream IS ciphertext by construction once §4.1 envelopes are in
  use; the header is the only thing that needs wrapping.
- **Stage 6E** (the `enable-raft-envelope` admin RPC that populates
  `raft_envelope_cutover_index`). 8a depends on 6E for the field to
  ever become non-zero, but it does NOT require 6E to ship first —
  the v2 writer can compile + run with the sidecar field defined
  but always-zero, and existing snapshots remain v1.
- **Migration of existing v1 snapshots to v2.** None needed; v1
  files keep restoring under the new build forever.

## 2. Architecture choice — one viable option

Only one option in the parent design (§4.4): version the header
via a distinct 8-byte magic. The alternative considered (and
rejected in the parent) was a heuristic on a shared magic that
inspects the high byte of the would-be ceiling — rejected because
an early-epoch HLC ceiling can legitimately have a low high byte
and would misclassify v1 streams as v2.

The "distinct magic per version" choice is unambiguous: a single
`bytes.Equal` of the leading 8 bytes selects the format. This
slice does not reopen the architecture; it implements the
specified design.

## 3. Design (per parent §4.4)

### 3.1 Header layouts

```text
v1 (legacy, unchanged on-disk):
+------------+----------+
| magic(8)   | ceiling  |
| EKVTHLC1   |   8B     |
+------------+----------+

v2 (Phase-2-aware):
+------------+--------+----------+----------+
| magic(8)   | len(2) | ceiling  | cutover  |
| EKVTHLC2   |  0x10  |   8B     |   8B     |
+------------+--------+----------+----------+
```

- `len` is `uint16` big-endian, the byte count of the payload
  **after** the `len` field itself. v2 with the two defined fields
  has `len = 0x0010` (16 bytes).
- `ceiling` is the HLC ceiling, identical semantics to v1.
- `cutover` is `raft_envelope_cutover_index` as big-endian
  `uint64`. `0` is the correct value for any snapshot taken in
  Phase 0 or Phase 1 (no `enable-raft-envelope` applied yet on the
  cluster as observed by the local node at snapshot time).

#### 3.1.1 Forward-compat boundary: what may extend v2 vs. require a new magic

The `len`-skip hatch lets a future writer append optional fields to
the v2 payload while preserving backward compat with old v2 readers
(which size their payload read by `len` and ignore trailing bytes
they do not understand). This is **only** safe when the appended
fields are advisory — i.e., an old v2 reader that ignores them
still produces correct restore behavior.

If a future field affects FSM correctness (e.g., a flag that
changes how the restore initializes the applier, or a new
mandatory routing index), an old v2 reader that ignores it would
silently misroute / mis-initialize. Such fields require a new
magic (`EKVTHLC3`), not a v2 payload extension. The discriminator
in `ReadSnapshotHeader` is the gatekeeper for the "I do not
understand this version" fail-closed branch (see §3.2, step 4).

### 3.2 Read path

`ReadSnapshotHeader` runs **once** at the top of `kv/fsm.go::Restore`
before the inner-store payload is consumed.

**API shape and `bufio.Reader` ownership (load-bearing).** The
**caller** creates a `bufio.Reader` wrapping the input
`io.Reader` and passes it as the single parameter:

```go
func ReadSnapshotHeader(r *bufio.Reader) (ceiling, cutover uint64, err error)
```

The caller MUST pass the **same `r`** to the inner-store restore
call on **all branches** (v1, v2, and headerless-legacy) — not
just headerless. The reason: `bufio.Reader` may have read more
bytes from the underlying `io.Reader` than it returned to
`ReadSnapshotHeader` (buffer fill is opportunistic); inner-store
bytes can sit in the `bufio.Reader`'s internal buffer
between header consumption and the inner-store read. If the
caller switches to the original `io.Reader` after the header is
parsed, those buffered bytes are silently lost. Always-pass-the-
same-bufio.Reader keeps the byte stream contiguous regardless of
branch.

0. Peek up to 8 leading bytes via `bufio.Reader.Peek(8)`. **If the
   stream contains fewer than 8 bytes** (`Peek` returns
   `io.EOF` / `io.ErrUnexpectedEOF` with a short slice), fall
   through directly to step 5 (headerless legacy) with the partial
   bytes left in the buffer — this preserves
   `TestFSMSnapshotRestoreSmallLegacy`'s contract that pre-HLC
   short snapshots restore unchanged.
1. With exactly 8 bytes peeked, discriminate via `bytes.Equal` on
   the 8 leading bytes:
2. `bytes.Equal(peeked, hlcSnapshotMagicV2[:])` (the v2 sentinel):
   - Consume the 8 magic bytes.
   - Read 2 bytes → `len` (big-endian uint16).
   - **Validate `len`**: require `16 <= len <= maxSnapshotHeaderPayload`
     (suggested `maxSnapshotHeaderPayload = 1024`).
     - `len < 16` (payload too short to hold ceiling+cutover) →
       return `(0, 0, ErrSnapshotHeaderInvalidLength)` rather than
       a downstream `io.EOF` / panic on slice indexing.
     - `len > maxSnapshotHeaderPayload` → return
       `(0, 0, ErrSnapshotHeaderInvalidLength)` to prevent a
       malformed or malicious snapshot from triggering a 64 KiB
       allocation per restore (DoS hardening — coderabbit major on
       PR #877). The constant is intentionally small; raise it only
       when a v2 extension genuinely needs the headroom.
   - Read `len` payload bytes (now bounded by the validated `len`).
   - Parse `ceiling` from the first 8 of payload, `cutover` from
     the next 8, ignore any trailing bytes (forward-compat;
     see §3.1.1 below for the boundary of what new fields may be
     added under v2 vs. requiring a new magic).
   - Return `(ceiling, cutover, nil)`.
3. `bytes.Equal(peeked, hlcSnapshotMagic[:])` (the v1 sentinel):
   - Consume the 8 magic bytes.
   - Read 8 bytes → `ceiling`.
   - Return `(ceiling, 0, nil)`.
4. `bytes.HasPrefix(peeked, []byte("EKVTHLC"))` (a `EKVTHLC*` magic
   with an unknown version byte):
   - Return `(0, 0, ErrSnapshotHeaderUnknownMagic)` — the
     restore is fail-closed; the operator must upgrade to a binary
     that recognises this version.
5. Else (no `EKVTHLC` prefix at all, OR fewer than 8 bytes
   available per step 0):
   - Headerless legacy snapshot. Return `(0, 0, nil)` AND **leave
     the peeked bytes in the underlying `bufio.Reader`** (do NOT
     consume them); the inner-store payload reader — passed the
     same `bufio.Reader` — sees the stream from byte 0.

The headerless-legacy fallback is the existing behavior preserved
by today's `TestFSMSnapshotRestoreOldFormat` and
`TestFSMSnapshotRestoreSmallLegacy` regression tests. These tests
MUST continue to pass under the new reader.

### 3.3 Write path

`WriteSnapshotHeader` runs once at the top of the snapshot stream
on the writer side:

- If the local node's sidecar has `raft_envelope_cutover_index == 0`
  (the Phase-0 / Phase-1 posture): write v1 layout. Byte-for-byte
  identical to today's output; any reader (old or new) handles it.
- If `raft_envelope_cutover_index != 0`: write v2 layout. `cutover`
  comes from the sidecar value; `ceiling` from the same HLC source
  as today.

**No-downgrade enforcement (load-bearing).** Once a writer has
observed a non-zero `raft_envelope_cutover_index` even once during
its process lifetime, it MUST continue writing v2 even if a
subsequent sidecar read returns `cutover = 0`. Without this latch,
a future code path that resets the sidecar field (e.g. a
hypothetical reseed or factory-reset operation) would silently
produce a v1 snapshot from a Phase-2 node; a follower restoring
that snapshot would configure `cutover = 0` and then fail to
unwrap AEAD-wrapped log entries — silent data corruption.

The latch is **per process load**: in-memory state on the writer
remembers "I have seen non-zero cutover" and forces v2 from then
on. Process restart re-reads the sidecar; if the sidecar still has
`cutover != 0`, the latch reactivates immediately on the first
snapshot. A node whose sidecar legitimately has `cutover = 0` (a
pre-Phase-2 node that never observed the cutover) never engages the
latch.

Additionally, the implementation SHOULD log.Error (not Fatal — the
write path is in a snapshot goroutine; killing the process on a
sidecar inconsistency would surface as snapshot failure rather
than a useful diagnostic) when it observes a non-zero → zero
sidecar transition while the latch is engaged. This makes the
invariant violation visible without taking the node down
mid-snapshot.

### 3.4 Restore-side integration

`kv/fsm.go::Restore` consumes the `(ceiling, cutover)` tuple:

- `ceiling` is plumbed exactly as today.
- `cutover` is plumbed to the applier / engine pre-apply hook so
  subsequent Raft entries route through the correct wrap/unwrap
  path. The routing logic is **two-step**, with the `cutover == 0`
  precondition load-bearing (gemini medium on PR #877):

  1. If `cutover == 0` (v1 snapshot OR v2 snapshot taken in
     Phase 0/1, i.e. before `enable-raft-envelope` applied):
     ALL entries are pre-Phase-2 plaintext, no routing
     consultation — behavior matches the pre-8a posture exactly.
     A naive `raftIdx >= cutover` test would always be true at
     `cutover == 0` and incorrectly attempt to unwrap every
     entry as AEAD; the `cutover == 0` precondition guard MUST
     come first.
  2. If `cutover != 0`:
     - Entries with `raftIdx < cutover` are pre-Phase-2
       plaintext; pass through.
     - The entry with `raftIdx == cutover` is the plaintext cutover
       marker itself; pass through.
     - Entries with `raftIdx > cutover` are AEAD-wrapped;
       unwrap.

The exact applier-side plumbing depends on Stage 6E's apply-hook
shape; this slice defines the snapshot-to-applier handoff but
leaves the apply-hook itself to 6E. With 6E not yet shipped, the
plumbing is a one-liner that stores `cutover` on the applier (or
the engine's restore context) without yet consulting it on apply.

## 4. Why no migration step

Every existing snapshot file on disk is v1. Under the new build:

- v1 snapshots restore unchanged via the v1 branch of
  `ReadSnapshotHeader` → identical observable behavior.
- New snapshots written in Phase 0 / Phase 1 are still v1.
- Only snapshots written AFTER `enable-raft-envelope` has applied
  are v2.

A cluster that never enables Phase 2 never sees a v2 snapshot.
This is the rollout-without-migration property the parent design
explicitly calls out (§4.4 line 972–974).

## 5. Verification action items (for the implementation PR)

1. `kv/snapshot_test.go`:
   - `TestReadSnapshotHeader_V1ReturnsZeroCutover` — write a v1
     header, verify `(ceiling=X, cutover=0, err=nil)`.
   - `TestReadSnapshotHeader_V2ReturnsBothFields` — write a v2
     header with a non-zero cutover, verify both fields.
   - `TestReadSnapshotHeader_V2ForwardCompatExtraBytes` — write a
     v2 header with `len = 0x18` (24 bytes) and 8 trailing
     bytes after the defined fields; verify the reader skips the
     trailing bytes cleanly.
   - `TestReadSnapshotHeader_UnknownEKVTHLCMagicFails` — write
     `EKVTHLC9...`; expect `ErrSnapshotHeaderUnknownMagic`.
   - `TestReadSnapshotHeader_HeaderlessLegacyPreserved` — write a
     payload that does NOT start with `EKVTHLC`; verify
     `(0, 0, nil)` and that the inner-store reader sees the
     payload from byte 0 (re-pin existing
     `TestFSMSnapshotRestoreOldFormat` /
     `TestFSMSnapshotRestoreSmallLegacy`).
   - `TestReadSnapshotHeader_V2WithLenTooShortFails` — write a v2
     header with `len = 0x08` (below the 16-byte minimum needed
     for `ceiling+cutover`); expect `ErrSnapshotHeaderInvalidLength`,
     NOT an EOF / "short payload" panic.
   - `TestReadSnapshotHeader_V2WithLenTooLargeFails` — write a v2
     header with `len = 0xFFFF`; expect
     `ErrSnapshotHeaderInvalidLength` (DoS hardening — pins the
     upper bound from §3.2 step 2).
   - `TestReadSnapshotHeader_ShortStreamFallsBackToLegacy` —
     stream contains 0, 4, and 7 bytes (sub-cases); each must
     return `(0, 0, nil)` and leave the available bytes in the
     `bufio.Reader` for the inner-store path, exercising step 0
     of the read algorithm (gemini medium + coderabbit major on
     PR #877).
   - `TestWriteSnapshotHeader_PreCutoverWritesV1` — sidecar
     `raft_envelope_cutover_index == 0` → output is byte-for-byte
     identical to today's v1.
   - `TestWriteSnapshotHeader_PostCutoverWritesV2` — sidecar
     `raft_envelope_cutover_index != 0` → output is the v2 layout
     with the cutover correctly carried.
   - `TestWriteSnapshotHeader_NoDowngradeAfterCutoverSeen` — first
     snapshot is taken with sidecar `cutover = 5` → v2 output;
     then the sidecar field is artificially reset to 0 (simulating
     a hypothetical future reseed path) and a second snapshot is
     taken on the same writer → MUST still be v2 (the no-downgrade
     latch from §3.3). Pins the load-bearing rule against silent
     data corruption.
2. `kv/fsm.go` restore path integration:
   - `TestFSMSnapshotRestoreV2_PlumbsCutover` — drive a v2
     restore end-to-end; verify the applier's restore context
     reads back the cutover (via a fake applier or an inspectable
     field).
3. Self-review (5-lens) for the implementation PR — particular
   attention to:
   - **Data loss**: legacy headerless and v1 snapshots MUST restore
     byte-for-byte the same as today. Single regression in this
     area is a blocker.
   - **Data consistency**: v2 writer must NEVER produce a v1
     snapshot once cutover is non-zero on the local sidecar (no
     silent downgrade).
   - **Test coverage**: every case in §3.2 read-path branches has
     a dedicated test; the existing v1-only and headerless-legacy
     tests stay green.

## 6. Rollout / migration

8a is purely additive — no on-disk migration, no FSM apply
semantics change.

- **Pre-8a binary reading a v2 snapshot**: cannot happen in
  practice, because a pre-8a binary cannot have applied
  `enable-raft-envelope` (which only exists once 6E ships, and 6E
  follows 8a's reader). If an operator manually constructed a v2
  snapshot for a pre-8a binary, the pre-8a reader would not match
  `EKVTHLC1` and would fall through to its legacy path — passing
  the whole stream (including the unexpected `EKVTHLC2` header
  bytes) to the inner store, which then fails to parse the inner
  payload. This is a loud failure, not silent corruption —
  acceptable for a hand-rolled scenario. (Note: pre-8a code has no
  "unknown `EKVTHLC*` magic" branch, so the `EKVTHLC` prefix in
  `EKVTHLC2` does not trigger a typed error there. The fail-closed
  guarantee in §3.2 step 4 is a property of the 8a reader only.)
- **Post-8a binary reading a v1 snapshot**: byte-for-byte
  compatible. No change in behavior.
- **Post-8a binary reading a v2 snapshot**: the steady state once
  6E + Phase 2 are enabled. Cutover plumbed to the applier.

Mixed-version cluster during the 8a rollout itself: pre-8a nodes
still write v1 snapshots; post-8a nodes write v1 in
Phase 0/1 (the only state until 6E ships). The cluster cannot enter
a state where 8a's v2 layout matters until 6E lands, so 8a and
6E sequencing is the only operational constraint — 8a MUST ship
before 6E. The reverse (6E before 8a) would leave 6E with no way
to carry the cutover through a snapshot restore.

## 7. After 8a

- **Stage 8b** — WAL coverage closure (Raft log encryption on disk;
  see §1 Out-of-Scope). Implemented as a no-code closure analysis in
  [`2026_06_01_implemented_8b_wal_coverage_closure.md`](2026_06_01_implemented_8b_wal_coverage_closure.md).
- **Stage 6E** — `enable-raft-envelope` admin RPC + cutover apply.
  Depends on 8a's reader being deployed first so a node restoring
  from a snapshot taken after cutover can resume correctly.
- **Stage 9** — KMS-backed wrappers, compression, rotation/retire/
  rewrite, Jepsen.
