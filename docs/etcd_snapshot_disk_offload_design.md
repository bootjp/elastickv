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
| magic   | 4 byte | `EKVR` (ElasticKV Reference) |
| version | 1 byte | `0x01` |
| index   | 8 byte | applied log index of the snapshot (little-endian uint64) |
| crc32c  | 4 byte | CRC32C checksum of the entire `.fsm` file (little-endian uint32) |

The magic prefix distinguishes the new format from legacy payloads (see Migration section).
Embedding the CRC32C in the token allows integrity verification at the metadata level,
before the file is even opened.

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

---

## Flow Changes

### 1. Local Snapshot Creation

**Before:**
```
maybePersistLocalSnapshot()
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
  → fsm.Snapshot()
  → writeFSMSnapshotFile(snapshot, fsmSnapDir, index)
      → os.CreateTemp() → {fsmSnapDir}/{index}.fsm.tmp
      → crcWriter := newCRC32CWriter(tmpFile)
      → snapshot.WriteTo(crcWriter)                               # stream to disk + compute CRC
      → binary.Write(tmpFile, LittleEndian, crcWriter.Sum32())   # append CRC footer
      → tmpFile.Sync() → os.Rename(tmp, final)                   # atomic commit
      → return crcWriter.Sum32()
  → token := encodeSnapshotToken(index, crc32c)  # 17 bytes
  → persist.SaveSnap(snap{Data: token})          # small snap file
  → storage.CreateSnapshot(_, _, token)          # MemoryStorage: 17 bytes only
  → purgeOldFSMSnapFiles(fsmSnapDir)
```

**Memory impact:**
- Peak: FSM data size → 0 (direct disk write only)
- Resident: FSM data size → 17 bytes

### 2. Restore on Restart

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
            index, expectedCRC := decodeSnapshotToken(snap.Data)
            path := fsmSnapPath(fsmSnapDir, index)
            verifyFSMSnapshotFile(path, expectedCRC)          # fail fast on corruption
            f := os.Open(path)
            fsm.Restore(newCRC32CStripFooterReader(f))        # exclude footer from stream
         else:
            fsm.Restore(bytes.NewReader(snap.Data))           # legacy format fallback
```

`verifyFSMSnapshotFile` reads the file sequentially, computes the CRC incrementally, and
compares it against both the on-disk footer and the `expectedCRC` from the token. A
mismatch returns `ErrFSMSnapshotCRCMismatch` and aborts startup.

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

The `.fsm` file is sent including its CRC footer. The receiver stores the full file and
performs CRC verification after all chunks arrive.

### 4. Receiving and Applying MsgSnap on a Follower

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
  → receive header chunk → parse token → extract index, expectedCRC
  → os.CreateTemp() → {fsmSnapDir}/{index}.fsm.tmp
  → stream remaining chunks to tmpFile
  → tmpFile.Sync()
  → verifyFSMSnapshotFile(tmpPath, expectedCRC)              # verify before committing
  → os.Rename(tmpPath, {fsmSnapDir}/{index}.fsm)             # atomic commit on success
  → raftpb.Message{Snapshot: {Data: token}}
  → handler(msg)
      → applyReadySnapshot(snap)
          → index, expectedCRC := decodeSnapshotToken(snap.Data)
          → verifyFSMSnapshotFile(path, expectedCRC)         # second check before apply
          → fsm.Restore(newCRC32CStripFooterReader(f))
```

**Why verify immediately after receive:**
- Detects in-transit bit errors before the file is applied to the FSM
- Prevents a corrupt file from overwriting a healthy FSM state
- On failure, report `SnapshotFailure` to prompt the leader to retry

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

### Reading: Streaming Verification

`verifyFSMSnapshotFile` uses `stat` to determine the file size, then reads
`(size - 4)` bytes through a CRC accumulator, and compares the result against the
4-byte footer and the `expectedCRC` from the token. No second read pass is required.

```go
func verifyFSMSnapshotFile(path string, expectedCRC uint32) error {
    info, err := os.Stat(path)
    if err != nil {
        return errors.WithStack(err)
    }
    if info.Size() < 4 {
        return errors.Wrapf(ErrFSMSnapshotCRCMismatch, "file too small: %d bytes", info.Size())
    }
    f, err := os.Open(path)
    if err != nil {
        return errors.WithStack(err)
    }
    defer f.Close()

    h := crc32.New(crc32cTable)
    if _, err := io.Copy(h, io.LimitReader(f, info.Size()-4)); err != nil {
        return errors.WithStack(err)
    }
    var footer uint32
    if err := binary.Read(f, binary.LittleEndian, &footer); err != nil {
        return errors.WithStack(err)
    }
    computed := h.Sum32()
    if computed != footer {
        return errors.Wrapf(ErrFSMSnapshotCRCMismatch,
            "path=%s footer=%08x computed=%08x", path, footer, computed)
    }
    if computed != expectedCRC {
        return errors.Wrapf(ErrFSMSnapshotCRCMismatch,
            "path=%s token=%08x computed=%08x", path, expectedCRC, computed)
    }
    return nil
}
```

`newCRC32CStripFooterReader` wraps the open file and exposes only the first
`(size - 4)` bytes to the FSM's `Restore` call so the footer is not interpreted as
key-value data.

### Verification Points

| When | Location | What is checked |
|------|----------|-----------------|
| **On restart** | `restoreSnapshotState` | file CRC ↔ footer ↔ token CRC |
| **After follower receive** | `receiveSnapshotStream` | file CRC ↔ footer ↔ token CRC |
| **Before follower apply** | `applyReadySnapshot` | token CRC ↔ file CRC (second check) |
| **Background health check** *(future)* | background task | periodic re-computation of file CRC |

### Error Handling

```go
var (
    ErrFSMSnapshotCRCMismatch  = errors.New("fsm snapshot: CRC32C mismatch")
    ErrFSMSnapshotNotFound     = errors.New("fsm snapshot: file not found")
    ErrFSMSnapshotTokenInvalid = errors.New("fsm snapshot: token format invalid")
)
```

- `ErrFSMSnapshotCRCMismatch`: log `{path, expected, actual}` and abort the operation.
  - On startup: attempt WAL replay from `FirstIndex` before giving up.
  - On follower receive: report `SnapshotFailure`; the leader will retry.

---

## Key Implementation Changes

### New File

| File | Contents |
|------|----------|
| `internal/raftengine/etcd/fsm_snapshot_file.go` | FSM snapshot file read/write, CRC32C computation and verification, token encode/decode |

### Changes to Existing Files

| File | Change |
|------|--------|
| `internal/raftengine/etcd/wal_store.go` | Remove `snapshotBytes`; add `writeFSMSnapshotFile`; update `restoreSnapshotState` to handle token format |
| `internal/raftengine/etcd/engine.go` | Replace payload retrieval in `persistLocalSnapshot` with `writeFSMSnapshotFile`; add `fsmSnapDir` field |
| `internal/raftengine/etcd/grpc_transport.go` | Detect token in `Dispatch` → route to file-streaming send path; update `receiveSnapshotStream` to write chunks to file |
| `internal/raftengine/etcd/snapshot_spool.go` | Repurpose or remove `snapshotSpool` |

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
  3. binary.Write(tmpFile, LE, crcWriter.Sum32())    # append CRC footer
  4. tmpFile.Sync()                                  # flush to durable storage
  5. os.Rename(tmp, final)                           # atomic commit
  6. syncDir(fsmSnapDir)                             # persist directory entry
```

`persist.SaveSnap(snap{Data: token})` is called only after the rename succeeds.
Because the `.fsm` file is committed before the token is written to the WAL or snap
file, the following crash scenarios are all safely recoverable:

| State at crash | Recovery |
|----------------|----------|
| Only `.fsm.tmp` exists | Deleted by startup cleanup (treated as orphan) |
| `.fsm` exists, snap not yet saved | Snap points to previous index; next snapshot overwrites |
| snap(token) exists, `.fsm` missing | Token dereferences a missing file → error → WAL replay fallback |
| `.fsm` CRC mismatch | Treated as corrupt orphan; deleted at startup; WAL replay fallback |

### Startup Cleanup

Similar to `cleanupStaleSnapshotSpools`, a new `cleanupStaleFSMSnaps(fsmSnapDir)` removes
any `*.fsm.tmp` files left by a previously crashed process. `.fsm` files whose CRC does
not match any live token are also removed, since WAL replay can reconstruct the FSM
without them.

---

## File Retention Policy

```
purgeOldFSMSnapFiles(fsmSnapDir):
  - Retain the same count as snap files: defaultMaxSnapFiles = 3
  - Match .fsm files to snap files by index; delete in tandem
  - Always delete the snap file before its corresponding .fsm file
    (reverse order risks leaving a token that points to a deleted file)
```

---

## Migration (Legacy Format Compatibility)

If the first 4 bytes of `raftpb.Snapshot.Data` equal `EKVR`, the payload is treated as a
token. Any other prefix is treated as a legacy FSM payload.

```go
const snapshotTokenMagic = [4]byte{'E', 'K', 'V', 'R'}

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

**When a follower receives a legacy-format MsgSnap:**
- `isSnapshotToken` returns false → restore via `bytes.NewReader` (no CRC check)
- Compatible with rolling upgrades where not all nodes have been updated yet

---

## Trade-offs

### Benefits

- **Peak memory**: spikes caused by snapshot creation drop to near zero
- **Resident memory**: `MemoryStorage` snapshot footprint reduced to 17 bytes
- **I/O**: eliminates the spool → Bytes → SaveSnap double-write; total disk I/O decreases
- **gRPC send**: existing `sendSnapshotReaderChunks` accepts `io.Reader` as-is
- **No upstream dependency**: no changes to etcd-raft protobuf or library code

### Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| `.fsm` and snap file inconsistency | `persist.SaveSnap` is called only after successful rename; ordering is strict |
| Disk space increase | `.fsm` files are roughly the same size as the former `.snap` payloads; net usage is unchanged |
| Increased code complexity | `isSnapshotToken` branch is small; legacy path is preserved intact |
| CRC false negatives | CRC32C has a 1-in-2³² collision rate; adequate for accidental corruption detection |

---

## Implementation Phases

### Phase 1: Local Snapshot Disk Offload

Scope: local creation and local restore only. Follower send/receive is unchanged.

- Create `fsm_snapshot_file.go`:
  - `crc32CWriter` (streaming CRC computation)
  - `writeFSMSnapshotFile` (write with CRC footer)
  - `verifyFSMSnapshotFile` (CRC verification)
  - `newCRC32CStripFooterReader` (expose payload without footer)
  - `encodeSnapshotToken` / `decodeSnapshotToken` (17-byte token including CRC)
- Update `persistLocalSnapshot` to use `writeFSMSnapshotFile` + token
- Update `restoreSnapshotState` to handle token format with CRC verification
- All existing tests must continue to pass

**Effect**: eliminates memory spikes during local snapshot creation; reduces `MemoryStorage` resident memory

### Phase 2: Streaming Follower Send/Receive

Scope: update `GRPCTransport` MsgSnap paths to use file streaming.

- `Dispatch`: detect token → open file → `sendSnapshotFileChunks`
- `receiveSnapshotStream`: write chunks to temp file → verify CRC → atomic rename
- Test legacy-format compatibility paths

**Effect**: eliminates memory spikes during large snapshot transfers to followers

### Phase 3: Cleanup

- Remove `snapshotBytesAndClose`, `snapshotBytes`, and `snapshotSpool`
- Resolve `maxSnapshotPayloadBytes`
- Update documentation

---

## References

- [etcd-io/etcd#9000 - Snapshot splitting](https://github.com/etcd-io/etcd/issues/9000) — basis for the side-channel architecture recommendation
- [etcd-io/raft#124 - Clean up and improve snapshot handling](https://github.com/etcd-io/raft/issues/124)
- `internal/raftengine/etcd/snapshot_spool.go`: comment "the prototype cannot stream snapshots end-to-end yet"
- `internal/raftengine/etcd/grpc_transport.go`: `sendSnapshotReaderChunks` — existing streaming send implementation
