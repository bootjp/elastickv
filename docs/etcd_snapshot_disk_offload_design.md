# etcd Engine: FSM Snapshot Disk Offload Design

## Background and Problem

### Observed Symptoms

Clusters using the etcd engine exhibit large memory spikes across all nodes at snapshot
creation intervals (`defaultSnapshotEvery = 10,000` entries). The spike magnitude scales
with FSM data size, and simultaneous spikes across multiple nodes compound the pressure.

### Root Cause

The current implementation stores the entire FSM state as a `[]byte` in
`raftpb.Snapshot.Data`. Because the etcd-raft protobuf API requires this type, there are
three distinct points of excessive memory consumption.

#### Problem 1: Peak allocation during snapshot creation

```
internal/raftengine/etcd/wal_store.go: snapshotBytes()

FSM.WriteTo(spool)   → write to a temporary file on disk
spool.Bytes()        → io.ReadAll materializes the entire payload as []byte  ← spike
persist.SaveSnap()   → write to disk again (redundant copy)
```

`snapshotSpool` already avoids a second in-memory buffer during serialization, but the
final `spool.Bytes()` call still materializes the entire dataset at once.

#### Problem 2: Sustained retention in MemoryStorage

```go
storage.CreateSnapshot(applied, &confState, payload)
```

`MemoryStorage` holds `raftpb.Snapshot.Data = payload` until the next snapshot is created
(i.e., until another 10,000 entries are processed), keeping the full FSM export resident
in memory the entire time.

#### Problem 3: Re-allocation when sending to followers

```go
// grpc_transport.go: sendSnapshot()
header, payload, err := splitSnapshotMessage(msg)
// payload = msg.Snapshot.Data  ← []byte extracted from MemoryStorage
```

Sending `MsgSnap` to a follower extracts `Data` from `MemoryStorage` and expands it into
memory again.

### Official etcd Position

In etcd-io/etcd#9000 ("Snapshot splitting"), core contributor @xiang90 explicitly
recommended the following architecture:

> The suggested way to handle the problem you described is to **always only include
> metadata into the raft snapshot message, and use a side-channel to supply the actual
> application snapshot**.

This design follows that recommendation: `raftpb.Snapshot.Data` carries only a small
reference token, and FSM data is managed entirely on disk.

---

## Design Goals

| Goal | Detail |
|------|--------|
| **Eliminate peak memory** | Remove `spool.Bytes()`; never materialize FSM data as `[]byte` |
| **Eliminate sustained memory** | `MemoryStorage` holds only a 17-byte token, not the full FSM |
| **No upstream changes** | etcd-raft `raftpb` API is unchanged; no upstream PR needed |
| **Reuse existing gRPC transport** | `sendSnapshotReaderChunks` already accepts `io.Reader` |
| **Crash safety** | A node crash during write must never produce a corrupt or partially-applied snapshot |
| **Corruption detection** | CRC32C on every `.fsm` file; verified at write, restore, and receive |

### Non-Goals

- Changes to etcd-io/raft upstream
- Any impact on the hashicorp engine
- Changes to the WAL format

---

## Proposed Architecture

### File Layout

```
{dataDir}/
├── wal/                           # unchanged
│   └── *.wal
├── snap/                          # unchanged (raft snap metadata)
│   └── 0000000100000064.snap      # raftpb.Snapshot{Data: token, Metadata: ...}
└── fsm-snap/                      # new directory
    ├── 0000000000000064.fsm       # FSM payload (Pebble key-value stream + CRC32C footer)
    └── 0000000000000064.fsm.tmp   # in-flight write; renamed to .fsm on atomic commit
```

### `raftpb.Snapshot.Data` Token Format

```
Before: Data = full FSM payload (variable length, up to 1 GiB)

After:  Data = [magic:4][version:1][index:8][crc32c:4]
               17 bytes, fixed size
```

| Field   | Size   | Value |
|---------|--------|-------|
| magic   | 4 byte | `EKVT` (ElasticKV Token) |
| version | 1 byte | `0x01` |
| index   | 8 byte | applied log index of the snapshot (little-endian uint64) |
| crc32c  | 4 byte | CRC32C checksum of the entire `.fsm` file (little-endian uint32) |

The magic prefix distinguishes the new format from legacy payloads (see Migration section).
Embedding the CRC32C in the token allows integrity verification at the metadata level,
before the file is even opened.

> **Magic allocation note**: The existing persistence layer already uses `EKVR` (state
> file), `EKVM` (metadata file), and `EKVW` (entries file) as defined in
> `internal/raftengine/etcd/persistence.go`. `EKVT` is chosen to avoid collision with
> any of these. The full registry is:
>
> | Magic  | File |
> |--------|------|
> | `EKVR` | `etcd-raft-state.bin` |
> | `EKVM` | `etcd-raft-meta.bin` |
> | `EKVW` | `etcd-raft-entries.bin` |
> | `EKVT` | `raftpb.Snapshot.Data` token (this design) |

### `.fsm` File Format

```
[Pebble snapshot stream (existing format)]
  ├── magic:        8 bytes  {'E','K','V','P','B','B','L','1'}
  ├── lastCommitTS: 8 bytes  (little-endian uint64)
  └── entries:      variable [keyLen:8][key][valLen:8][val] repeated
[footer]
  └── crc32c:       4 bytes  CRC32C of all bytes preceding this field (little-endian uint32)
```

The CRC32C is computed incrementally as data is written, so no second pass over the file
is needed to finalize the checksum.

**Algorithm choice: CRC32C (Castagnoli)**

This is the same algorithm used by etcd's `Snapshotter`
(`etcdserver/api/snap/snapshotter.go`). It is hardware-accelerated on both x86
(SSE4.2 `CRC32` instruction) and ARM64 (ARMv8.1 `CRC32C`), delivering throughput in the
range of several GB/s with negligible CPU overhead. The Go standard library exposes it as
`hash/crc32.MakeTable(crc32.Castagnoli)`.

### CRC Authority

The **on-disk footer is the authoritative checksum**. The CRC embedded in the token is a
*transmission integrity check* — it is derived from the file at write time and travels
with the raft snapshot metadata. The two values must always agree; a mismatch indicates:

- **footer ≠ computed**: the `.fsm` file is corrupt on disk → delete and attempt WAL
  replay; do not trust the token.
- **footer == computed, token ≠ computed**: the `.snap` metadata is corrupt → the file
  itself is trustworthy; the snap file should be rewritten from the file's actual CRC.
- **footer ≠ computed AND token ≠ computed**: both are corrupt → WAL replay only.

This distinction is implemented in `verifyFSMSnapshotFile`, which returns typed errors
(`ErrFSMSnapshotFileCRC`, `ErrFSMSnapshotTokenCRC`) to allow callers to take the correct
recovery action.

---

## Flow Changes

### 1. Local Snapshot Creation

> **Scope note**: This flow applies to **all three snapshot creation paths**: local
> periodic snapshots (`persistLocalSnapshot`), config-change snapshots
> (`persistConfigSnapshot`), and bootstrap (`stateMachineSnapshotBytes`). All three must
> be migrated in Phase 1; leaving any path on the legacy `snapshotBytesAndClose` route
> re-introduces the memory problem and breaks the token invariant.

**Before:**
```
maybePersistLocalSnapshot()
  → storageIndex := storage.Snapshot().Metadata.Index   # record current index
  → fsm.Snapshot()
  → snapshotBytesAndClose()
      → spool.WriteTo()        # write to temp file on disk
      → spool.Bytes()          # ← problem: io.ReadAll materializes full []byte
  → persist.SaveSnap(snap{Data: payload})  # write to disk again
  → storage.CreateSnapshot(_, _, payload)  # hold in MemoryStorage
```

**After:**
```
maybePersistLocalSnapshot()
  # Capture the MemoryStorage index BEFORE calling fsm.Snapshot() so the
  # staleness check in persistLocalSnapshotPayload uses the same baseline.
  # If a follower restore advances storage between Snapshot() and
  # persistLocalSnapshotPayload, the index <= current.Metadata.Index guard
  # will correctly discard the stale file.
  → storageIndex := storage.Snapshot().Metadata.Index
  → fsmSnap := fsm.Snapshot()
  → crc32c, err := writeFSMSnapshotFile(fsmSnap, fsmSnapDir, storageIndex)
      → os.CreateTemp() → {fsmSnapDir}/{storageIndex}.fsm.tmp
      → crcWriter := newCRC32CWriter(tmpFile)
      → fsmSnap.WriteTo(crcWriter)                              # stream to disk + compute CRC
      → binary.Write(tmpFile, binary.LittleEndian, crcWriter.Sum32()) # append CRC footer
      → tmpFile.Sync()
      → os.Rename(tmp, final)                                   # atomic commit
      → syncDir(fsmSnapDir)                                     # persist directory entry
      → return crcWriter.Sum32()
  → token := encodeSnapshotToken(storageIndex, crc32c)  # 17 bytes
  → persist.SaveSnap(snap{Data: token})                 # small snap file
  → storage.CreateSnapshot(_, _, token)                 # MemoryStorage: 17 bytes only
  → purgeOldSnapshotFiles(snapDir, fsmSnapDir)          # coordinated purge (see Retention)
```

**Memory impact:**
- Peak: FSM data size → 0 (direct disk write only)
- Resident: FSM data size → 17 bytes

### 2. Restore on Restart

Restore uses a **single-pass** helper `openAndRestoreFSMSnapshot` that opens the file
once, verifies the CRC while streaming data to the FSM, and never reads the file a second
time. This eliminates the double-read that would result from a separate verify pass
followed by a restore pass.

**Before:**
```
loadWalState()
  → snapshotter.LoadNewestAvailable()
  → restoreSnapshotState(fsm, snap)
      → fsm.Restore(bytes.NewReader(snap.Data))  # snap.Data is the full FSM payload
```

**After:**
```
loadWalState()
  → snapshotter.LoadNewestAvailable()
  → restoreSnapshotState(fsm, snap, fsmSnapDir)
      → if isSnapshotToken(snap.Data):
            index, tokenCRC := decodeSnapshotToken(snap.Data)
            path := fsmSnapPath(fsmSnapDir, index)
            openAndRestoreFSMSnapshot(fsm, path, tokenCRC)
              # single pass: open fd → stream payload through CRC accumulator
              #              → call fsm.Restore → compare computed vs footer vs tokenCRC
              # returns ErrFSMSnapshotFileCRC or ErrFSMSnapshotTokenCRC on mismatch
         else:
            fsm.Restore(bytes.NewReader(snap.Data))  # legacy format fallback
```

`openAndRestoreFSMSnapshot` keeps a single open file descriptor for the full operation:

```go
func openAndRestoreFSMSnapshot(fsm StateMachine, path string, tokenCRC uint32) error {
    info, err := os.Stat(path)
    if err != nil {
        return errors.WithStack(err)
    }
    // Minimum valid .fsm: 8 bytes Pebble magic + 8 bytes lastCommitTS + 4 bytes CRC footer = 20 bytes.
    const minFSMFileSize = 20
    if info.Size() < minFSMFileSize {
        return errors.Wrapf(ErrFSMSnapshotTooSmall,
            "file too small: %d bytes (minimum %d)", info.Size(), minFSMFileSize)
    }
    f, err := os.Open(path)
    if err != nil {
        return errors.WithStack(err)
    }
    defer f.Close()

    payloadSize := info.Size() - 4
    h := crc32.New(crc32cTable)
    // Tee: data flows to both the CRC accumulator and fsm.Restore simultaneously.
    tee := io.TeeReader(io.LimitReader(f, payloadSize), h)
    if err := fsm.Restore(bufio.NewReaderSize(tee, 1<<20)); err != nil {
        return errors.WithStack(err)
    }
    var footer uint32
    if err := binary.Read(f, binary.LittleEndian, &footer); err != nil {
        return errors.WithStack(err)
    }
    computed := h.Sum32()
    if computed != footer {
        return errors.Wrapf(ErrFSMSnapshotFileCRC,
            "path=%s footer=%08x computed=%08x", path, footer, computed)
    }
    if computed != tokenCRC {
        return errors.Wrapf(ErrFSMSnapshotTokenCRC,
            "path=%s token=%08x computed=%08x", path, tokenCRC, computed)
    }
    return nil
}
```

Key properties:
- **Single read pass**: `io.TeeReader` feeds data to both `fsm.Restore` and the CRC hash
  simultaneously; no second scan.
- **`bufio.NewReaderSize(tee, 1<<20)`**: the 1 MiB read-ahead buffer amortizes the
  per-entry small reads that `writePebbleSnapshotEntries` / the restore loop issues,
  matching the throughput of the former `bytes.Reader` path.
- **Single open fd**: the file descriptor is held across the full verify+restore
  operation, eliminating the TOCTOU window between a separate verify and a subsequent
  restore open.

### 3. Sending MsgSnap to a Follower

**Before:**
```
sendMessages()
  → Dispatch(msg)
      → sendSnapshot(msg)
          → splitSnapshotMessage(msg)   # extracts large msg.Snapshot.Data
          → sendSnapshotChunks(...)     # sends []byte in chunks
```

**After:**
```
sendMessages()
  → Dispatch(msg)
      → if isSnapshotToken(msg.Snapshot.Data):
            index, _ := decodeSnapshotToken(msg.Snapshot.Data)
            f := os.Open(fsmSnapPath(fsmSnapDir, index))
            sendSnapshotFileChunks(stream, header, f)   # stream .fsm file including footer
         else:
            sendSnapshot(msg)   # legacy format fallback
```

The `.fsm` file is sent **including its CRC footer** as the final 4 bytes. The receiver
stores the full file and verifies the CRC before committing.

### 4. Receiving and Applying MsgSnap on a Follower

Two concurrency concerns are addressed here:

1. **Concurrent receive for the same index**: a leader retry or simultaneous MsgSnap
   delivery could cause two goroutines to write `{index}.fsm` at the same time. A
   `singleflight.Group` keyed on the snapshot index serializes receive operations for the
   same index; only the first completes, the second reuses its result.

2. **TOCTOU between verify and apply**: verify and restore use the same open file
   descriptor via `openAndRestoreFSMSnapshot` (see §2), eliminating the window where the
   file could be replaced between a standalone verify call and a subsequent open for
   restore.

**Before:**
```
receiveSnapshotStream()
  → receive all chunks → assemble into []byte
  → raftpb.Message{Snapshot: {Data: fullBytes}}
  → handler(msg)
      → applyReadySnapshot(snap)
          → fsm.Restore(bytes.NewReader(snap.Data))
```

**After:**
```
receiveSnapshotStream()
  → receive header chunk → parse token → extract index, tokenCRC
  → result, err := snapReceiveGroup.Do(strconv.FormatUint(index, 10), func() {
        → os.CreateTemp() → {fsmSnapDir}/{index}.fsm.tmp
        → stream remaining chunks to tmpFile (with bufio.NewWriterSize)
        → tmpFile.Sync()
        → verify CRC of tmpPath against footer and tokenCRC
          # footer is authoritative; return ErrFSMSnapshotFileCRC if mismatch
        → os.Rename(tmpPath, {fsmSnapDir}/{index}.fsm)  # atomic commit on success
        → syncDir(fsmSnapDir)                           # flush directory entry
        → return token
    })
  → raftpb.Message{Snapshot: {Data: token}}
  → handler(msg)
      → applyReadySnapshot(snap)
          → index, tokenCRC := decodeSnapshotToken(snap.Data)
          → openAndRestoreFSMSnapshot(fsm, path, tokenCRC)
            # single-pass verify+restore; no second verifyFSMSnapshotFile call needed
            # the file was already committed atomically by receiveSnapshotStream
```

**Why the second `verifyFSMSnapshotFile` call is removed from `applyReadySnapshot`:**
After `receiveSnapshotStream` has verified, atomically renamed, and `syncDir`'d the file,
no other process modifies `.fsm` files between that point and `applyReadySnapshot`. The
`openAndRestoreFSMSnapshot` call already recomputes the CRC as a by-product of streaming
data to `fsm.Restore`, so integrity is confirmed without an extra full-file scan.

---

## CRC32C Implementation Details

### Writing: Incremental Computation

```go
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// crc32CWriter wraps an io.Writer and computes the CRC32C of all bytes written through
// it. Call Sum32() after writing is complete to obtain the checksum.
type crc32CWriter struct {
    w io.Writer
    h hash.Hash32
}

func newCRC32CWriter(w io.Writer) *crc32CWriter {
    return &crc32CWriter{w: w, h: crc32.New(crc32cTable)}
}

func (c *crc32CWriter) Write(p []byte) (int, error) {
    n, err := c.w.Write(p)
    c.h.Write(p[:n])
    return n, err
}

func (c *crc32CWriter) Sum32() uint32 { return c.h.Sum32() }
```

Passing `crc32CWriter` to `snapshot.WriteTo` computes the CRC without any changes to the
existing `WriteTo` implementation.

### Reading: Single-Pass Verify and Restore

Rather than a standalone `verifyFSMSnapshotFile` followed by a separate `fsm.Restore`,
all read paths use `openAndRestoreFSMSnapshot` (see §2: Restore on Restart). This
function:

1. Opens the file once and holds the fd for the entire operation.
2. Uses `io.TeeReader` to feed data simultaneously to the CRC accumulator and
   `fsm.Restore`.
3. Reads the 4-byte footer after `fsm.Restore` returns and checks `computed == footer`
   and `computed == tokenCRC`.
4. Returns typed errors distinguishing file corruption (`ErrFSMSnapshotFileCRC`) from
   token corruption (`ErrFSMSnapshotTokenCRC`).

A standalone `verifyFSMSnapshotFile(path, tokenCRC)` is still provided for contexts where
the FSM must not be modified (e.g., startup cleanup health-checking orphan files), but it
is **never called in sequence with a subsequent restore**.

### Verification Points

| When | Location | Mechanism |
|------|----------|-----------|
| **On restart** | `restoreSnapshotState` | `openAndRestoreFSMSnapshot` (single pass) |
| **After follower receive** | `receiveSnapshotStream` | `verifyFSMSnapshotFile` on tmp before rename |
| **On follower apply** | `applyReadySnapshot` | `openAndRestoreFSMSnapshot` (single pass; no extra verify) |
| **Startup orphan check** | `cleanupStaleFSMSnaps` | `verifyFSMSnapshotFile` (read-only, no restore) |
| **Background health check** *(future)* | background task | `verifyFSMSnapshotFile` |

### Error Types and Recovery

```go
var (
    ErrFSMSnapshotFileCRC   = errors.New("fsm snapshot: file CRC32C mismatch (file corrupt)")
    ErrFSMSnapshotTokenCRC  = errors.New("fsm snapshot: token CRC32C mismatch (metadata corrupt)")
    ErrFSMSnapshotNotFound  = errors.New("fsm snapshot: file not found")
    ErrFSMSnapshotTooSmall  = errors.New("fsm snapshot: file too small to contain footer")
    ErrFSMSnapshotTokenInvalid = errors.New("fsm snapshot: token format invalid")
)
```

| Error | Meaning | Recovery |
|-------|---------|---------|
| `ErrFSMSnapshotFileCRC` | On-disk file is corrupt; footer ≠ computed | Delete file; WAL replay from `FirstIndex` |
| `ErrFSMSnapshotTokenCRC` | File is intact (footer == computed) but token differs | Rewrite token from file's actual CRC; no WAL replay needed |
| `ErrFSMSnapshotNotFound` | `.fsm` file missing for a valid token | WAL replay from `FirstIndex` |

---

## Key Implementation Changes

### New File

| File | Contents |
|------|----------|
| `internal/raftengine/etcd/fsm_snapshot_file.go` | `crc32CWriter`, `openAndRestoreFSMSnapshot`, `verifyFSMSnapshotFile`, `writeFSMSnapshotFile`, token encode/decode, error types |

### Changes to Existing Files

| File | Change |
|------|--------|
| `internal/raftengine/etcd/wal_store.go` | Remove `snapshotBytes`; add `writeFSMSnapshotFile`; update `restoreSnapshotState` and `stateMachineSnapshotBytes` (bootstrap) |
| `internal/raftengine/etcd/engine.go` | Update `persistLocalSnapshot`, `persistConfigSnapshot`, `persistConfigState`, and `persistConfigSnapshotPayload` to use `writeFSMSnapshotFile` + token; add `fsmSnapDir` and `snapReceiveGroup` fields |
| `internal/raftengine/etcd/grpc_transport.go` | Detect token in `Dispatch` → file-streaming send; update `receiveSnapshotStream` to write chunks to file with `singleflight` serialization and `syncDir` |
| `internal/raftengine/etcd/snapshot_spool.go` | Remove `snapshotSpool` (Phase 3) |

### Code Removed

- `snapshotBytesAndClose()` — the `[]byte` materialization path is no longer needed
- `snapshotBytes()` — same
- `maxSnapshotPayloadBytes` — file size bounds are delegated to the OS / disk quota

---

## Crash Safety

### Write-time Crash Protection

```
writeFSMSnapshotFile():
  1. os.CreateTemp(fsmSnapDir, "*.fsm.tmp")         # write to temp file
  2. snapshot.WriteTo(crcWriter)                     # stream FSM + accumulate CRC
  3. binary.Write(tmpFile, binary.LittleEndian, crcWriter.Sum32()) # append CRC footer
  4. tmpFile.Sync()                                  # flush to durable storage
  5. os.Rename(tmp, final)                           # atomic commit
  6. syncDir(fsmSnapDir)                             # persist directory entry
```

`persist.SaveSnap(snap{Data: token})` is called **only after step 6 succeeds**.
The receiver path (`receiveSnapshotStream`) follows the same sequence and also calls
`syncDir(fsmSnapDir)` after rename.

| State at crash | Recovery |
|----------------|----------|
| Only `.fsm.tmp` exists | Deleted by startup cleanup (orphan) |
| `.fsm` exists, snap not yet saved | Snap points to previous index; next snapshot overwrites |
| snap(token) exists, `.fsm` missing | `ErrFSMSnapshotNotFound` → WAL replay from `FirstIndex` |
| `.fsm` footer CRC mismatch | `ErrFSMSnapshotFileCRC` → delete file; WAL replay |
| `.fsm` footer ok, token CRC differs | `ErrFSMSnapshotTokenCRC` → rewrite token; restore from file |

### Startup Cleanup

`cleanupStaleFSMSnaps(snapDir, fsmSnapDir)` runs at engine open time and:

1. Removes all `*.fsm.tmp` (orphans from a previous crash).
2. Enumerates all live snap tokens by reading `*.snap` files in `snapDir`.
3. Removes any `.fsm` file whose index does not correspond to a live token — this handles
   the case where a `.fsm` was written but the corresponding `.snap` was never saved
   (upgrade crash), as well as files left over after purge ordering bugs in older versions.
4. For each remaining `.fsm` file, calls `verifyFSMSnapshotFile` and removes files where
   `ErrFSMSnapshotFileCRC` is returned (corrupt files cannot be used for restore).

This index-based orphan detection is stricter than a CRC-only check: a file with a valid
CRC but no matching token is still an orphan and must be removed.

---

## File Retention Policy

### Coordinated Purge

Snap files and FSM files **must always be purged together** in a single function.
Calling them independently risks deleting a `.fsm` file while its token `.snap` still
exists, which makes the node unrecoverable if the WAL has already been compacted.

```go
// purgeOldSnapshotFiles removes old snap and fsm files in tandem, always deleting
// the snap file BEFORE its corresponding fsm file. This ordering guarantees that
// no live token can ever reference a deleted fsm file.
func purgeOldSnapshotFiles(snapDir, fsmSnapDir string) error {
    // 1. List all snap files sorted oldest-first.
    // 2. Keep the newest defaultMaxSnapFiles (3); mark the rest for deletion.
    // 3. For each file to delete:
    //    a. os.Remove(snapFile)      ← snap first
    //    b. os.Remove(fsmFile)       ← fsm second
    //    A crash between a and b leaves a .fsm with no token: treated as orphan on
    //    next startup and removed by cleanupStaleFSMSnaps.
    // 4. syncDir(snapDir) and syncDir(fsmSnapDir).
}
```

`purgeOldSnapshotFiles` is the **only** call site for deleting either type of file.
The existing `purgeOldSnapFiles` function is removed and replaced entirely.

---

## Migration (Legacy Format Compatibility)

If the first 4 bytes of `raftpb.Snapshot.Data` equal `EKVR`, the payload is treated as a
token. Any other prefix is treated as a legacy FSM payload.

```go
const snapshotTokenMagic = [4]byte{'E', 'K', 'V', 'T'}

func isSnapshotToken(data []byte) bool {
    if len(data) < 4 {
        return false
    }
    return [4]byte(data[:4]) == snapshotTokenMagic
}
```

**When a legacy snap file is present on startup:**
- `isSnapshotToken` returns false → restore via `bytes.NewReader(snap.Data)` (no CRC check)
- The next snapshot creation writes a new-format `.fsm` file with CRC
- No manual migration step is required

**Orphan `.fsm` files during upgrade rollback:**
If a node begins writing `.fsm` files and then rolls back to the previous binary, the
`.fsm` files have no corresponding token in the legacy `.snap` files.
`cleanupStaleFSMSnaps` removes them by walking live tokens, so rollback is safe.

**When a follower receives a legacy-format MsgSnap:**
- `isSnapshotToken` returns false → restore via `bytes.NewReader` (no CRC check)
- Compatible with rolling upgrades where not all nodes have been updated yet

---

## Rolling Upgrade and Zero-Downtime Cutover

### Compatibility Matrix

The `isSnapshotToken` magic check provides the compatibility bridge. Because `EKVT` is
a prefix that can never appear in a valid legacy FSM payload (Pebble snapshots start with
`EKVPBBL1`; gob-encoded legacy payloads start with gob framing bytes), there is no
ambiguity when a mixed-version cluster is operating.

| Sender version | Receiver version | Outcome |
|----------------|-----------------|---------|
| Legacy (no token) | Legacy | Unchanged — existing path |
| Legacy (no token) | New | `isSnapshotToken` → false → `bytes.NewReader` fallback |
| New (token) | Legacy | Legacy node does not understand the token; restores from the 17-byte payload as if it were FSM data → **corrupt FSM** |
| New (token) | New | Token path → `.fsm` file |

### Rolling Upgrade Strategy

The third case — a new-format leader sending a token to a legacy follower — is the only
dangerous scenario. To prevent it, the rollout must proceed as follows:

1. **Deploy Phase 1 to all nodes before Phase 2.** Phase 1 changes only local snapshot
   creation and restore; it does not change what is sent over the wire. A Phase 1 node
   still sends full `[]byte` payloads in `MsgSnap` and can receive them. Mixed Phase 1 /
   legacy clusters are safe.

2. **Deploy Phase 2 to all nodes simultaneously, or use the feature flag below.** Once
   any node begins sending token-format `MsgSnap`, all receivers must be at Phase 2.

3. **Feature flag (recommended)**: add a `DisableFSMSnapshotToken` config field (default
   `false`). When set to `true`, `Dispatch` falls back to the legacy `sendSnapshot` path
   even if the local MemoryStorage contains a token. This allows operators to:
   - Deploy Phase 2 binaries cluster-wide with the flag enabled.
   - Verify stability, then disable the flag on each node progressively.
   - Roll back safely by re-enabling the flag without restarting.

### Rollback Safety

If a rollback to the pre-Phase 1 binary is required after Phase 1 has created `.fsm`
files:
- The legacy binary reads the `.snap` file and finds `Data` = a 17-byte token starting
  with `EKVT`.
- `fsm.Restore` will attempt to interpret 17 bytes as a Pebble snapshot and fail with a
  decode error (the Pebble magic `EKVPBBL1` is not present).
- **Mitigation**: Before rolling back, stop the node and manually delete `fsm-snap/`; the
  legacy binary will then fall back to WAL replay from the compacted snapshot index.
  Document this procedure in the runbook.

Alternatively, Phase 1 can write a **dual-format** `.snap` file during a configurable
transition window: `raftpb.Snapshot.Data` = token AND a copy of the full legacy payload
stored in an auxiliary file. The legacy binary would then decode the full payload
normally. This dual-write mode is a forward compatibility bridge and can be removed after
the fleet has been fully upgraded.

---

## Trade-offs

### Benefits

- **Peak memory**: spikes caused by snapshot creation drop to near zero
- **Resident memory**: `MemoryStorage` snapshot footprint reduced to 17 bytes
- **I/O**: eliminates the spool → Bytes → SaveSnap double-write; total disk I/O decreases
- **Single-pass restore**: `io.TeeReader` combines CRC verification and FSM restore into
  one sequential scan, eliminating the double-read present in a naive verify-then-restore
  design
- **gRPC send**: existing `sendSnapshotReaderChunks` accepts `io.Reader` as-is
- **No upstream dependency**: no changes to etcd-raft protobuf or library code

### Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| `.fsm` and snap file inconsistency | `persist.SaveSnap` only after `syncDir`; single `purgeOldSnapshotFiles` function enforces deletion ordering |
| Stale local snapshot overwriting newer follower state | `storageIndex` captured before `fsm.Snapshot()`; `persistLocalSnapshotPayload` staleness guard uses the same baseline |
| Concurrent follower receive for same index | `singleflight.Group` keyed on index in `receiveSnapshotStream` |
| TOCTOU between verify and restore | `openAndRestoreFSMSnapshot` holds a single fd across the entire verify+restore operation |
| `Metadata.Index` diverging from `.fsm` content in config snapshots | `persistConfigSnapshot` migrated in Phase 1; FSM snapshot taken with the same index used as the file name and token |
| Disk space increase | `.fsm` files are the same size as the former `.snap` payloads; net usage unchanged |
| CRC false negatives | CRC32C has a 1-in-2³² collision rate; adequate for accidental corruption detection |

---

## Implementation Phases

### Phase 1: Local Snapshot Disk Offload

Scope: local creation and local restore for **all three snapshot paths** (periodic,
config-change, and bootstrap). Follower send/receive is unchanged.

**Implementation tasks:**

- Create `fsm_snapshot_file.go`:
  - `crc32CWriter` (streaming CRC writer)
  - `writeFSMSnapshotFile` (write with CRC footer + syncDir)
  - `openAndRestoreFSMSnapshot` (single-pass verify+restore via TeeReader)
  - `verifyFSMSnapshotFile` (read-only CRC check for orphan detection)
  - `encodeSnapshotToken` / `decodeSnapshotToken` (17-byte token with CRC)
  - Error types: `ErrFSMSnapshotFileCRC`, `ErrFSMSnapshotTokenCRC`, etc.
- Update `persistLocalSnapshot` → `writeFSMSnapshotFile` + token
- Update `persistConfigSnapshot`, `persistConfigState`, and `persistConfigSnapshotPayload` → same
- Update `stateMachineSnapshotBytes` (bootstrap) → `writeFSMSnapshotFile` + token
- Update `restoreSnapshotState` → `openAndRestoreFSMSnapshot`
- Replace `purgeOldSnapFiles` with `purgeOldSnapshotFiles(snapDir, fsmSnapDir)`
- Add `cleanupStaleFSMSnaps(snapDir, fsmSnapDir)` (index-based orphan removal)
- Capture `storageIndex` before `fsm.Snapshot()` in `maybePersistLocalSnapshot`
- All existing tests must continue to pass

**Effect**: eliminates memory spikes during local snapshot creation; reduces
`MemoryStorage` resident memory; all snapshot creation paths are consistent.

### Phase 2: Streaming Follower Send/Receive

Scope: update `GRPCTransport` MsgSnap paths to use file streaming.

**Implementation tasks:**

- `Dispatch`: detect token → open `.fsm` file → `sendSnapshotFileChunks`
- `receiveSnapshotStream`:
  - Write chunks to temp file (with `bufio.NewWriterSize`)
  - `verifyFSMSnapshotFile` on tmp before rename
  - `syncDir(fsmSnapDir)` after rename
  - `singleflight.Group` keyed on index to serialize concurrent receives
- `applyReadySnapshot`: use `openAndRestoreFSMSnapshot` (no extra verify call)
- Test legacy-format compatibility paths

**Effect**: eliminates memory spikes during large snapshot transfers to followers;
removes three-read-pass anti-pattern on the receive side.

### Phase 3: Cleanup

- Remove `snapshotBytesAndClose`, `snapshotBytes`, and `snapshotSpool`
- Remove standalone `purgeOldSnapFiles` (replaced by `purgeOldSnapshotFiles`)
- Resolve `maxSnapshotPayloadBytes`
- Update documentation

---

## Required Tests

### P0 — Must have before Phase 1 merge

| Test | What it verifies |
|------|-----------------|
| `TestTokenRoundTrip` | `encodeSnapshotToken` / `decodeSnapshotToken` round-trip for boundary values (`0`, `MaxUint64`, `MaxUint32`) |
| `TestTokenMagicRejection` | `isSnapshotToken` returns false for non-`EKVR` prefixes; `decodeSnapshotToken` returns `ErrFSMSnapshotTokenInvalid` for lengths 0–16 and wrong magic |
| `TestCRCWriterMatchesStdlib` | `crc32CWriter.Sum32()` matches `crc32.Checksum` for identical bytes; incremental writes accumulate correctly |
| `TestOpenAndRestoreFSMSnapshotGoodFile` | Correct file restores FSM state without error |
| `TestOpenAndRestoreFSMSnapshotBadFooter` | Footer byte flipped → `ErrFSMSnapshotFileCRC` |
| `TestOpenAndRestoreFSMSnapshotTokenMismatch` | File footer ok, wrong `tokenCRC` → `ErrFSMSnapshotTokenCRC` |
| `TestOpenAndRestoreFSMSnapshotTooSmall` | File shorter than 4 bytes → `ErrFSMSnapshotTooSmall` |
| `TestStripFooterReaderBoundary` | `io.TeeReader` with `LimitReader(size-4)` exposes exactly the payload; footer bytes not passed to FSM |
| `TestCrashAfterTmpBeforeRename` | A leftover `*.fsm.tmp` is deleted by `cleanupStaleFSMSnaps`; no `.fsm` is promoted |
| `TestSnapSavedOnlyAfterRename` | `WriteTo` error → no `.snap` token written, no `.fsm` final file committed |
| `TestPurgeOldSnapshotFilesOrdering` | `purgeOldSnapshotFiles` always removes `.snap` before `.fsm`; verified by intercepting `os.Remove` calls |
| `TestCleanupStaleFSMSnapsIndexBased` | Orphan `.fsm` with no matching live token is removed even when its CRC is valid |

### P1 — High value, ship in Phase 2 or immediately after

| Test | What it verifies |
|------|-----------------|
| `TestReceiveTruncatedFile` | Stream ending mid-file → `ErrFSMSnapshotFileCRC`; no `.fsm` committed |
| `TestReceiveWrongCRCInFooter` | Correct length, corrupted footer → verify rejects before rename |
| `TestReceiveTokenCRCMismatchesFileCRC` | Token carries wrong CRC, file footer is self-consistent → `ErrFSMSnapshotTokenCRC` in `openAndRestoreFSMSnapshot` |
| `TestConcurrentReceiveSameIndex` | Two goroutines receiving the same index via `singleflight`; only one `.fsm` committed; no torn file |
| `TestLegacyFormatFallbackOnRestore` | `snap.Data` without `EKVR` prefix → `bytes.NewReader` path; no `.fsm` file opened |
| `TestLegacyFormatFallbackOnSend` | Non-token `MsgSnap` → old `sendSnapshot` path; no file opened from `fsmSnapDir` |
| `TestSyncDirCalledAfterRename` | Both `writeFSMSnapshotFile` and `receiveSnapshotStream` call `syncDir` after rename (verified via mock or filesystem hook) |
| Conformance `SnapshotRestoreAfterRestart` | Propose 10,001 entries, close engine, reopen; assert FSM state recovered from `.fsm` snapshot (not WAL replay) |

### P2 — Nice to have

| Test | What it verifies |
|------|-----------------|
| `FuzzTokenEncodeDecode` | `decodeSnapshotToken` never panics on arbitrary 17-byte input; round-trips for valid tokens |
| `FuzzOpenAndRestoreFSMSnapshot` | Arbitrary file content → only typed errors returned, never panics |
| `TestConcurrentSnapshotAndEngineClose` | Snapshot worker crash on engine close leaves no torn file |

---

## References

- [etcd-io/etcd#9000 - Snapshot splitting](https://github.com/etcd-io/etcd/issues/9000) — basis for the side-channel architecture recommendation
- [etcd-io/raft#124 - Clean up and improve snapshot handling](https://github.com/etcd-io/raft/issues/124)
- `internal/raftengine/etcd/snapshot_spool.go`: comment "the prototype cannot stream snapshots end-to-end yet"
- `internal/raftengine/etcd/grpc_transport.go`: `sendSnapshotReaderChunks` — existing streaming send implementation
