# FUSE Filesystem Guide

This guide explains how to enable, mount, and operate the optional FUSE
filesystem that exposes the Elastickv key-value store as a POSIX-style file
tree. It is the operator-facing companion to the
[filesystem design](design/2026_02_24_implemented_filesystem_on_elastickv.md),
which records the data model, placement rules, and recovery protocol.

The filesystem is **experimental**, like the rest of Elastickv. Do not place
data on it that you cannot recreate.

## What you get

- Regular files and directories, backed by the same replicated, sharded store
  as the other protocols. Every node of the cluster can mount the filesystem;
  all mounts show the same tree.
- Offset reads and writes, sparse files, truncate (shrink and extend), `chmod`,
  `chown`, `utimes`, plain same-directory `rename` (no `renameat2` flags),
  `mkdir`/`rmdir`, `unlink` while a file is still open, resumable `readdir`,
  and `statfs`.
- Every metadata change and every FUSE write request is one Raft-committed
  transaction. When a system call returns, the change is durable on a quorum,
  and any other mount that opens the file afterwards reads it (see
  [Durability and caching](#semantics-worth-knowing) for the two caveats:
  the kernel splits a large `write(2)` into several requests, and a
  descriptor that is already open may still serve cached pages).

Not supported (the kernel receives an explicit error, never a silent no-op):

| Operation | Result |
|---|---|
| `rename` across directories | `EXDEV` (GNU `mv` then falls back to copy + delete, which is not atomic) |
| `renameat2` with flags (`RENAME_NOREPLACE`, `RENAME_EXCHANGE`, `RENAME_WHITEOUT`) | `EOPNOTSUPP` (only a flag-less `rename` is served) |
| Hard links (`link`) | `EOPNOTSUPP` |
| Symbolic links (`symlink`, `readlink`) | `EOPNOTSUPP` |
| POSIX advisory locks (`flock`, `fcntl` locks) | **Succeed, but only within one mount.** The kernel handles locks locally and never forwards them to the server, so two processes on different nodes can both hold "exclusive" locks on the same file. Do not rely on locks for cross-node coordination. |
| Extended attributes | disabled at mount time |
| Special files other than regular files and directories | `EOPNOTSUPP` |

File names may not be empty, `.`, `..`, or contain `/` or a NUL byte. `statfs`
advertises a 255-byte name limit.

## Prerequisites

- Linux with FUSE: `/dev/fuse` must exist and be accessible to the server
  process.
- The `fusermount3` helper (package `fuse3` on Debian/Ubuntu, `fuse3` on
  Fedora/RHEL) or the legacy `fusermount` must be on the server's `PATH`. The
  server mounts through this helper; it does not perform a direct `mount(2)`,
  so even a root process cannot mount without it.
- An existing, empty directory to use as the mount point.
- Every configured Raft group must have a reachable leader when the server
  starts: root initialization first takes a fenced read across all groups,
  then creates the root through a transaction routed by the catalog to the
  groups that own the root's inode, home, directory-version and usage keys
  (see [Startup sequence](#startup-sequence)). On a multi-group cluster a
  group without quorum fails the mount even if it owns none of those keys.

The official container image (`ghcr.io/bootjp/elastickv`) is built on
`distroless/static` and does not ship `fusermount3`. To mount from a
container, build an image that installs `fuse3`, give the container
`--device /dev/fuse --cap-add SYS_ADMIN` (and
`--security-opt apparmor:unconfined` on hosts where AppArmor confines the
runtime), and bind-mount the mount point with `:rshared` so the host can see
it. Running the binary directly on the host is the simpler option.

## Enabling the filesystem

The filesystem is off unless `--filesystemMount` is set. The relevant server
flags:

| Flag | Default | Meaning |
|---|---|---|
| `--filesystemMount` | empty (disabled) | Directory to mount the filesystem on. |
| `--filesystemClientID` | value of `--raftId` | Identity recorded in open-handle leases. Must be unique per mount in the cluster. |
| `--filesystemRootUID` | `0` | Owner UID of the root directory. Applied only when the root is first created. |
| `--filesystemRootGID` | `0` | Owner GID of the root directory. Applied only when the root is first created. |
| `--filesystemRootMode` | `0755` | Permission bits of the root directory. Applied only when the root is first created. |
| `--filesystemCapacity` | `0` (unlimited) | Byte capacity reported by `statfs`. Reporting only; writes are not rejected. |
| `--filesystemMaxFiles` | `0` (unlimited) | Inode capacity reported by `statfs`. Reporting only. |
| `--filesystemPlacementScanInterval` | `30s` | How often placement and recovery gauges are refreshed. Non-positive disables the scan. |
| `--filesystemLeaseReapInterval` | `30s` | How often expired open-handle leases are reclaimed. Non-positive disables the reaper. |

The chunk size (4 MiB) and the open-handle lease TTL (5 minutes) are fixed in
this release and have no flags.

### Single node

```bash
mkdir -p /tmp/elastickv-mount
go run . \
  --address "127.0.0.1:50051" \
  --redisAddress "127.0.0.1:6379" \
  --raftId "n1" \
  --raftBootstrap \
  --filesystemMount "/tmp/elastickv-mount" \
  --filesystemRootUID "$(id -u)" \
  --filesystemRootGID "$(id -g)"
```

Wait for the log line `filesystem FUSE mounted` (or `mountpoint -q
/tmp/elastickv-mount`), then use ordinary commands from another shell:

```bash
mkdir /tmp/elastickv-mount/docs
printf 'hello\n' > /tmp/elastickv-mount/docs/hello.txt
cat /tmp/elastickv-mount/docs/hello.txt
ls -l /tmp/elastickv-mount/docs
df -h /tmp/elastickv-mount
```

`--filesystemRootUID` / `--filesystemRootGID` matter even on a single node:
the mount enforces `default_permissions`, so a root directory owned by UID 0
(the default) rejects `mkdir` and file creation from any other user. Set them
to the user that will write to the mount **before the first start**; they are
ignored once the root exists. If the root was already created with the wrong
owner, there is no shortcut from another account: the mount has no
`allow_other`, so only the user running the server can reach it, and that
user cannot `chown` a root-owned directory. Either start the cluster over
with the right flags, or restart the server as root once, `chown` the mount
point from a root shell, and then go back to the normal user.

### Multi-node cluster

Add the same filesystem flags to every node that should expose a mount.
Nothing has to be configured on the other nodes: the file tree lives in the
replicated store, and a node that does not mount still stores and replicates
the data. Starting from the per-node command in
[docs/docker_multinode_manual_run.md](docker_multinode_manual_run.md), the
addition for `n1` is:

```bash
  --filesystemMount "/mnt/elastickv" \
  --filesystemRootUID 1000 \
  --filesystemRootGID 1000 \
```

Rules that apply across nodes:

- Pass identical `--filesystemRoot*` values everywhere. Only the node that
  creates the root applies them; the others find the root already present and
  ignore their own values. The root owner and mode are stored once and are
  enforced by the kernel on every mount (`default_permissions`), so the
  cluster needs one numeric UID/GID that the processes using the mount share
  on every node. If the first node to start wins with the wrong values, the
  mounts on the other nodes are read-only for their users until an
  administrator fixes it with `chown`/`chmod` on the root from any mount.
- Leave `--filesystemClientID` at its default (the Raft ID). It is recorded
  in open-handle lease keys and in the `filesystem FUSE mounted` log line,
  so a distinct value per mount makes leases and logs attributable to a node.
  Lease keys also carry a per-open random handle ID, so even two mounts with
  the same client ID do not interfere with each other's handles; the ID is
  for operators, not for correctness.
- A follower mount forwards writes to the leader and fences its reads on the
  leader's commit position before serving them, so a read on any node
  observes every write that completed on any other node before the read
  started.
- When bootstrapping a brand-new cluster with per-node joins, start the
  bootstrap node with the mount first and add the mount to the other nodes
  once they have joined. Root initialization needs a leader; a node that
  cannot find one exits with `initialize filesystem root` instead of waiting.
  Clusters bootstrapped from a fixed `--raftBootstrapMembers` list can mount
  on every node from the start.

## Startup sequence

When `--filesystemMount` is set, the server:

1. Starts its network listeners (gRPC, Redis, and the other enabled
   protocols).
2. Creates the root directory if it does not exist yet, through a sharded
   transaction whose keys (`!fs|ino|`, `!fs|home|`, `!fs|dirv|`, `!fs|usage`)
   are routed by the catalog; on a single-group cluster that is the default
   group, on a multi-group cluster it can span several groups. Concurrent
   root creation from several nodes is resolved by write-conflict retries;
   the losers find the root the first node created.
3. Replays filesystem recovery state: safely aborted create/delete intents are
   cleared and unfinished file moves are resumed, in batches of 256, until
   nothing is left. The whole replay must finish within 5 minutes; if it does
   not, the mount fails and the server exits. Recovery is resumable, so
   restarting the server continues where the previous attempt stopped.
4. Mounts the directory with `fusermount3`, waits for the kernel to confirm
   the mount, and logs `filesystem FUSE mounted` with the mount point and
   client ID.
5. Starts the placement-metrics collector and the lease reaper.

Because the listeners come up before the mount, a readiness check on the gRPC
or Redis port is not enough for clients that use the mount. Check the
mount point itself, and issue an I/O request against it rather than only
consulting the mount table, so that a mount left behind by a previous server
process (see [Stopping and restarting](#stopping-and-restarting)) does not
pass the check:

```bash
until mountpoint -q /mnt/elastickv && ls /mnt/elastickv >/dev/null 2>&1; do
  sleep 1
done
```

A mount failure is fatal: the server exits rather than running without the
filesystem it was asked to expose.

## Stopping and restarting

The server has no signal handler: `SIGTERM`, `SIGINT`, and `SIGKILL` all end
the process immediately, and the kernel keeps the now-dead mount attached.
Until it is detached, every access to the mount point fails with
`Transport endpoint is not connected`, and a restarted server exits with
`fusermount3: failed to access mountpoint ...: Transport endpoint is not
connected`. The orderly unmount built into the server only runs when the
server shuts itself down (a failed component or the memory watchdog).

Stop and restart a mounted node like this:

```bash
kill "$(pidof elastickv)"          # or your supervisor's stop action
fusermount3 -uz /mnt/elastickv     # detach the stale mount (umount -l also works)
# start the server again with the same flags
```

Put the `fusermount3 -uz` step in the service's `ExecStopPost` (systemd) or
the equivalent hook of your supervisor so an automatic restart does not loop
on the stale mount. Detaching the mount loses nothing that a completed
system call wrote: every `write(2)` that returned is committed in the store,
and open handles on the dead mount are released by the lease reaper. The one
exception is `mmap`: stores into a shared mapping sit in the kernel page
cache until writeback turns them into FUSE write requests, and once the
server is gone there is no daemon left to commit them. Writers that use
`mmap` must `msync(MS_SYNC)` (or `fsync`) before the server stops; `mmap`
coherency is outside what the filesystem guarantees.

## Semantics worth knowing

**Durability and caching.** The mount sets zero attribute and entry cache
timeouts, so every `stat`, `lookup`, and `readdir` goes to the backend and
reflects the latest committed state. Each FUSE write request is applied as one
transaction over the chunks it touches and is durable when the request
returns; `flush` is a no-op and `fsync` only re-reads the inode, because there
is no write-back buffer on the server side. Two caveats:

- *A large `write(2)` is not atomic.* The kernel splits a write into FUSE
  requests of at most 128 KiB (the go-fuse default; the mount does not raise
  it), and each request commits on its own. A 1 MiB write can therefore
  commit its first requests and then fail or conflict on a later one, leaving
  a partially applied write and a short count. Treat atomicity as per 128 KiB
  request, and write temp-file-plus-`rename` when a whole file must appear at
  once.
- *Data reads can be served from the kernel page cache.* The mount uses
  cached I/O without `direct_io`, and nothing invalidates a node's page cache
  when another node writes. The kernel drops cached pages when a file is
  opened, so a fresh `open` always reads the latest data, but a process that
  keeps a descriptor open and has already read a region may keep seeing the
  old bytes after another mount overwrote them. Readers that must observe
  remote writes through a long-lived descriptor should reopen the file or
  open it with `O_DIRECT`.

**Concurrency.** The backend uses optimistic concurrency. Two callers that
modify the same file or the same directory at the same moment can race; the
loser's system call fails with `EAGAIN` (`Resource temporarily unavailable`)
at `open`, `write`, `close`, `rename`, or `unlink`. Retry `open`, `write`,
`rename`, and `unlink`: rewriting the same bytes at the same offset is
idempotent, and a retried `rename` or `unlink` whose first attempt did apply
reports `ENOENT`, which tells you it is already done. The exception is
`O_APPEND` across mounts: the kernel picks the append offset from the size it
last saw and the server writes at that offset, so appends from different
nodes are not atomic with respect to each other, and retrying one after
`EAGAIN` can land on an offset another node has since written. Give each
appending file a single writer, or append through one mount. Never retry
`close`:
the kernel has already released the descriptor when it reports the error (the
`EAGAIN` comes from the lease refresh in the final flush), and a second
`close` could hit a descriptor another thread has since reused. Treat a
failed `close` as a signal to redo the higher-level operation through a fresh
descriptor. Applications that expect POSIX serialization under contention
must retry on `EAGAIN`.

**Open / replace race across mounts.** FUSE splits `open(2)` into a lookup and
an open. If another mount replaces the file (for example with
write-temp-then-`rename`) between the two steps, the opener can get `ENOENT`
for a path that exists again a moment later. Retry the `open`.

**Remove while open.** `unlink` on a file that another handle still has open
removes the name immediately; the data stays readable through the open handle
and is garbage-collected after the last handle is released. Each open handle
holds a lease (5 minutes, refreshed on every read and write through that
handle). If a mount dies without releasing its handles, the leases expire and
the lease reaper reclaims the space. The reaper runs only on mounted nodes
whose `--filesystemLeaseReapInterval` is positive (the default is 30s);
if every mount disables it, space held by dead handles is never returned.

**Directory listing.** `readdir` captures a snapshot of the directory when
the listing starts and pages through that snapshot, so entries created or
removed after the first page normally appear in the next listing, not in the
current one. The snapshot is held only as long as MVCC retention keeps its
timestamp: a listing paused for longer than the retention window (the
compactor is on by default) resumes from a newer snapshot at the name it
reached, and the remaining pages can then include later creations or skip
later removals.

**Permissions.** `default_permissions` is always set, so the kernel enforces
mode bits against the caller's UID/GID. `allow_other` is not set, and there is
no flag to set it: the kernel lets only the user that runs the server access
the mount and denies every other user, root included. Run the server as the
user that will use the files (with `--filesystemRootUID`/`GID` naming that
user), or as root when root is the only consumer.

**Chunking and sparse files.** File data is stored in fixed 4 MiB chunks. A
partial write reads, patches, and rewrites the affected chunk; missing chunks
read as zeros, so `truncate` to a larger size or writing past EOF does not
allocate anything for the hole.

**Capacity.** `df` shows `--filesystemCapacity` and `--filesystemMaxFiles`
when set and the current visible usage, but neither limit is enforced.

## Placement and sharding

Filesystem state is stored as ordinary keys under the `!fs|` prefix (inodes,
directory entries, chunks, open-handle leases, intents, move jobs). Nothing
special is needed for a single-group cluster.

On a multi-group cluster, each file has one *home*: all chunks of a file are
routed as one unit, so a file is never split across Raft groups by range
splitting. Automatic and manual `SplitRange` snap a split key inside a file
back to the file boundary; a range that is dominated by one huge file is left
alone and reported through
`elastickv_fs_file_pinned_hotspot_total{reason="split_boundary"}` and the
`ElastickvFilesystemFilePinnedHotspot` alert. Moving a file to another group
is implemented as an epoch-fenced, resumable job in the service (`MoveFile`),
but there is no operator-facing RPC or CLI for it yet; a move interrupted by a
crash is resumed at the next startup of a mounted node.

## Monitoring

Filesystem metrics are served on the usual `--metricsAddress` endpoint when
`--filesystemMount` is set:

| Metric | Meaning |
|---|---|
| `elastickv_fs_chunk_read_ops_total`, `elastickv_fs_chunk_write_ops_total` | Chunk I/O counters. |
| `elastickv_fs_chunk_read_latency_ms`, `elastickv_fs_chunk_write_latency_ms` | Chunk I/O latency histograms. |
| `elastickv_fs_open_handle_lease_active` | Live open-handle leases. |
| `elastickv_fs_orphan_inode_gc_pending` | Unlinked inodes whose chunks are still waiting for garbage collection. |
| `elastickv_fs_files_by_group{group_id}` | Files homed on each Raft group. |
| `elastickv_fs_file_multi_shard_current`, `elastickv_fs_file_multi_shard_detected_total` | Files whose chunks were found on more than one group. Should stay at zero. |
| `elastickv_fs_file_move_inflight` | File moves in progress. |
| `elastickv_fs_home_epoch_conflict_total` | Writes rejected because the file's home was being moved. |
| `elastickv_fs_file_pinned_hotspot_total` | Split candidates skipped to keep a file on one group. |
| `elastickv_fs_inode_id_collision_retry_total` | Inode ID allocation retries. |

The gauges derived from scanning the store are refreshed every
`--filesystemPlacementScanInterval`. The Grafana dashboard
`monitoring/grafana/dashboards/elastickv-filesystem.json` and the alert rules
in `monitoring/prometheus/rules/filesystem.yml` (pinned hotspot, multi-shard
file, stuck move, pending orphan GC, home epoch conflicts) cover these
metrics.

## Backup and restore

**The logical backup pipeline does not cover the filesystem.** The live
backup described in the [live backup runbook](operations/backup_restore.md)
and the dump-to-snapshot conversion in the
[snapshot restore runbook](operations/snapshot_restore.md) recognize only the
DynamoDB, S3, Redis, and SQS key families; `!fs|` keys are skipped by the
scope classifier, so a dump taken from a cluster with a filesystem and
restored through those runbooks comes back without the file tree. Until a
filesystem backup adapter exists, there are two options:

- Copy the tree through the mount with ordinary file tools (`rsync`, `tar`).
  Those tools read each file on its own, not from one filesystem-wide
  snapshot, so quiesce the writers first (and have `mmap` users
  `msync` their mappings); a copy taken under live writes can mix versions of
  different files or catch a file between two FUSE write requests.
- Keep a cold copy of every node's Raft data directory, taken with the
  servers stopped. With data-at-rest encryption enabled the copy must also
  include the key sidecar (`--encryptionSidecarPath`, which may live outside
  the data directory) and the KEK source configuration (`--kekFile` /
  `--kekUri`); restoring data without its sidecar leaves the values
  unrecoverable, and the KEK is never part of any backup. See the
  [encryption design](design/2026_04_29_partial_data_at_rest_encryption.md).

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Server exits with `mount filesystem frontend: ...` naming `fusermount3` or `fusermount` as not found in `$PATH` | `fuse3` is not installed or not on `PATH`. Install it, or run the server outside the distroless image. |
| Server exits with `initialize filesystem root: ... leader not found` | The node could not reach a Raft leader at startup. Start or repair the cluster first; for per-node joins, mount on the bootstrap node first and on peers after they joined. |
| `mkdir: cannot create directory: Permission denied` right after the first mount | The root directory is owned by UID 0 with mode `0755`. Start with `--filesystemRootUID` / `--filesystemRootGID` set to the writing user (only effective before the root exists) or `chown` the mount point as root. |
| `Transport endpoint is not connected` on the mount point | The server process stopped (any signal ends it without unmounting). Run `fusermount3 -uz <mount-point>` (or `umount -l`), then restart the server. |
| Restart exits with `fusermount3: failed to access mountpoint ...: Transport endpoint is not connected` | The previous mount is still attached. Detach it with `fusermount3 -uz` first; see [Stopping and restarting](#stopping-and-restarting). |
| `Resource temporarily unavailable` (`EAGAIN`) | Another mount modified the same file or directory at the same time, or the file is being moved between groups. Retry the operation. |
| `Input/output error` (`EIO`) | A backend error that is not a write conflict, for example a lost connection to the leader. Check the server log; retry once the cluster is healthy. |
| `ENOENT` opening a file that exists | The file was replaced by `rename` from another mount between lookup and open. Retry. |
| `Operation not supported` | Hard link, symlink, special file, or cross-directory rename. See the table at the top. |
| `mountpoint -q` fails while the server is up | The listeners start before the mount; wait for `filesystem FUSE mounted` in the log. If it never appears, the mount failed and the server exits. |
| `df` shows an absurdly large size | `--filesystemCapacity` / `--filesystemMaxFiles` are unset, so `statfs` reports the maximum. Both affect reporting only. |

## Verifying a build

- `go test -race ./internal/filesystem/... .` exercises the service and the
  FUSE adapter against an in-memory store and, through the `main` package,
  the production mount and shutdown wiring (`go test -race -run Filesystem .`
  runs only the latter). The kernel-mount end-to-end tests in
  `internal/filesystem/fuseadapter` and in `main` mount a real FUSE
  filesystem; they skip when `/dev/fuse` or `fusermount3` is unavailable, and
  `ELASTICKV_REQUIRE_FUSE=1` turns that skip into a failure.
- The Jepsen linearizable-register workload drives real mounts on every node
  of a cluster (`jepsen/src/elastickv/filesystem_workload.clj`). See
  "Running Jepsen tests" in the README; `scripts/run-jepsen-local.sh` runs it
  against a local 3-node cluster, and the `jepsen-test` and
  `jepsen-test-scheduled` workflows run it in CI.
