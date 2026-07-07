# Snapshot Logical Restore Runbook

This runbook restores a Phase 0 logical dump into a stopped elastickv
node by converting the dump back to an EKVPBBL1 payload and then
sealing that payload into the Raft disk-offload snapshot layout.

Use this only while the target node is stopped. Phase 0 is an offline
restore path; it is not a live import RPC.

## Inputs

- A decoded logical dump directory containing `MANIFEST.json`.
- A target raft data directory for one raft group.
- The target snapshot `index`, `term`, and ConfState voter set.
- The target cluster plan, including the intended `cluster_id`.

The encoder output is a native EKVPBBL1 payload stream without the
disk-offload CRC32C footer. Do not copy it directly into
`fsm-snap/`. Always run `elastickv-snap-token` to produce the
footer-sealed `.fsm`, the matching `.snap` token metadata, and the
WAL snapshot pointer that makes startup consume that token.

## Single-Node Fresh Restore

1. Stop the target process.

2. Encode the logical tree:

   ```sh
   elastickv-snapshot-encode \
     --input /backup/dump \
     --output /tmp/restore.fsm \
     --self-test
   ```

3. Inspect `/backup/dump/MANIFEST.json` and
   `/tmp/restore.fsm.encode_info.json`. Confirm `cluster_id`,
   `format_version`, `last_commit_ts`, and enabled adapters match the
   restore plan.

4. Generate the restore pair. `--voters` uses the same string IDs as
   `--raftId`; use `node:<uint64>` only when the target was configured
   with an explicit numeric raft node ID.

   ```sh
   elastickv-snap-token \
     --input /tmp/restore.fsm \
     --data-dir /var/lib/elastickv/group-1 \
     --index 1 \
     --term 1 \
     --voters n1
   ```

   This writes:

   ```text
   /var/lib/elastickv/group-1/fsm-snap/0000000000000001.fsm
   /var/lib/elastickv/group-1/snap/0000000000000001-0000000000000001.snap
   /var/lib/elastickv/group-1/wal/
   ```

5. Start the node with the same raft ID and raft data directory.

6. Verify adapter reads against the restored data before accepting
   writes.

## Existing Cluster Restore

For a cluster that already had multiple members, restore into one
stopped seed node first, then rebuild the other members from that
seed by rejoining or re-adding them. Do not place the same logical
snapshot independently onto multiple live members; each member's WAL,
term, membership metadata, and applied-index state must agree.

Use the target group's current or planned ConfState:

```sh
elastickv-snap-token \
  --input /tmp/restore.fsm \
  --data-dir /var/lib/elastickv/group-1 \
  --index 18432021 \
  --term 57 \
  --voters n1,n2,n3
```

If a target `.fsm`, `.snap`, or `wal/` already exists, the helper
refuses to overwrite it. `--force` stages the replacement set first
and only then swaps it into place, but keep an external backup until
the restored node has been verified.

## Multi-Group Restore

Run the encode and token-seal flow once per raft group. Phase 0
snapshots are per-group and can represent different applied indexes.
If the application requires a single cross-group point in time, use a
future Phase 1 live pinned backup instead of this offline path.

For each group:

1. Select that group's logical dump or encoded `.fsm`.
2. Run `elastickv-snap-token` with that group's `--data-dir`,
   `--index`, `--term`, and voter set.
3. Start only after every group directory has a matching
   `fsm-snap/<index>.fsm`, `snap/<term>-<index>.snap`, and `wal/`
   snapshot pointer.

## Safety Checks

- Confirm the target process is stopped before writing under the data
  directory.
- Keep a copy of the pre-restore `wal/`, `snap/`, and `fsm-snap/`
  directories until verification completes.
- Match `--voters` to the raft IDs or explicit node IDs used by the
  target configuration.
- Do not lower the HLC ceiling: `elastickv-snapshot-encode` rejects a
  `--last-commit-ts` below `MANIFEST.last_commit_ts`.
- Treat Phase 0 dumps as per-group crash-consistent artifacts, not as
  live cluster-wide point-in-time backups.
