# Snapshot Restore Runbook

This runbook covers the Phase 0 stop-replace-restart path for restoring a
logical dump back into a stopped elastickv node.

## Inputs

- A decoded dump tree with `MANIFEST.json` and `CHECKSUMS`.
- An encoded EKVPBBL1 payload from `elastickv-snapshot-encode`.
- The matching `<payload>.encode_info.json` sidecar.
- A fresh raft data directory path. The prepare command refuses to merge into
  an existing directory.

## Pack And Move A Dump Tree

Verify and pack the decoded tree before moving it off host:

```sh
elastickv-snapshot-archive pack \
  --input /backups/dump-root \
  --output /backups/dump-root.tar.zst
```

Unpack on the restore host:

```sh
elastickv-snapshot-archive unpack \
  --input /backups/dump-root.tar.zst \
  --output /restore/dump-root
```

Both commands read `MANIFEST.json` and verify `CHECKSUMS`. `unpack` refuses
path traversal, symlinks, hard links, and non-regular archive entries.

## Encode

Encode the dump tree with self-test enabled:

```sh
elastickv-snapshot-encode \
  --input /restore/dump-root \
  --output /restore/restore.fsm \
  --self-test
```

The encoder writes `/restore/restore.fsm.encode_info.json`. Keep the sidecar
with the payload; the restore prepare command uses it for SHA-256,
cluster-id, key-format, and self-test guards.

## Prepare A Fresh Raft Data Directory

For a restore into the same logical cluster, pass the target cluster id:

```sh
elastickv-snapshot-prepare-restore \
  --input /restore/restore.fsm \
  --data-dir /var/lib/elastickv-restored \
  --index 1 \
  --term 1 \
  --peers node-1=10.0.0.10:2380 \
  --target-cluster-id prod-cluster
```

For a brand-new cluster that has no target cluster id yet:

```sh
elastickv-snapshot-prepare-restore \
  --input /restore/restore.fsm \
  --data-dir /var/lib/elastickv-restored \
  --index 1 \
  --term 1 \
  --peers node-1=10.0.0.10:2380 \
  --fresh-cluster
```

The command creates:

- `fsm-snap/<index>.fsm`: the encoded payload plus the runtime CRC32C footer.
- `snap/<term>-<index>.snap`: the raft snapshot metadata containing the EKVT token.
- `wal/`: bootstrap WAL state.
- `peers`: persisted raft peer metadata.

It refuses to proceed when:

- the sidecar SHA-256 does not match the input payload;
- `encoder_key_format_version` differs from this binary's supported key format;
- the encoder self-test did not pass, unless `--allow-unverified-self-test` is set;
- source and target `cluster_id` disagree;
- the destination data directory already exists.

## Start The Node

1. Stop the target node.
2. Move the old data directory aside; do not merge the prepared directory into
   an existing WAL/snapshot directory.
3. Move `/var/lib/elastickv-restored` into the path used by `--raftDataDir`.
4. Start one node first and verify adapter reads.
5. Add other nodes as fresh members so they catch up from the seeded node's
   snapshot.

Phase 0 restores one snapshot's state. It does not create a new live point-in-
time backup and does not provide cross-shard consistency. Use a Phase 1 live
extractor once that path exists when those properties are required.

