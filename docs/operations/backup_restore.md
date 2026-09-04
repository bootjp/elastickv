# Live Logical Backup Runbook

This runbook covers Phase 1 point-in-time logical backups from a running
elastickv cluster. The backup tree is vendor-independent and can also be
converted into a native snapshot for the existing stop-replace-restart restore
path.

## Preconditions

- Every cluster member runs a build with the same live-backup protocol. The
  server rejects `BeginBackup` when a current member is unreachable or lacks
  the required protocol.
- The Admin gRPC endpoint is reachable from the backup host. Use the existing
  Admin bearer token and TLS configuration for non-loopback endpoints.
- MVCC retention exceeds the expected backup duration plus compaction lag.
- Every Raft group has enough snapshot headroom. A rejected `BeginBackup` is a
  safety result; retry after the affected group takes a snapshot instead of
  weakening the server policy without a retention review.
- The destination directory does not exist. The producer never merges with or
  overwrites an earlier dump.

Relevant server controls are:

```text
--backupDefaultTTL
--backupMaxTTL
--backupBeginDeadline
--backupSnapshotHeadroomEntries
--backupScanPageSize
--backupMaxActivePins
```

The defaults are a 30-minute pin, a 1-hour maximum renewal TTL, a 5-second
server deadline, 1000 snapshot-headroom entries, 1024 keys per baseline page,
and four concurrent backup pins.

## Create A Directory Backup

```sh
elastickv-backup dump \
  --address 192.168.0.210:50051 \
  --output-dir /backups/elastickv-2026-07-18 \
  --cluster-id production \
  --admin-token-file /run/secrets/elastickv-admin-token \
  --ttl-ms 1800000
```

For a TLS Admin endpoint, add the CA and expected server name:

```text
--tls-ca-cert-file /etc/elastickv/admin-ca.pem
--tls-server-name elastickv-admin.internal
```

`--tls-insecure-skip-verify` is intended only for controlled development
environments.

Limit the dump with `--adapter` and repeatable `--scope` flags:

```sh
elastickv-backup dump \
  --address 192.168.0.210:50051 \
  --output-dir /backups/orders-and-photos \
  --cluster-id production \
  --admin-token-file /run/secrets/elastickv-admin-token \
  --adapter dynamodb,s3 \
  --scope dynamodb=orders \
  --scope s3=photos
```

The producer currently requires DynamoDB per-item output. JSONL output remains
disabled until the native snapshot encoder can reverse that layout. The S3
incomplete-upload/orphan and SQS visibility/side-record opt-ins are likewise
rejected while the documented native restore path cannot preserve them.

## Stream An Archive

The producer always builds and validates the directory tree first. It can then
write a tar or zstd-compressed tar stream to stdout:

```sh
elastickv-backup dump \
  --address 192.168.0.210:50051 \
  --output-dir /var/tmp/elastickv-live-backup \
  --cluster-id production \
  --admin-token-file /run/secrets/elastickv-admin-token \
  --output-format tar+zstd \
  > /backups/elastickv-live-backup.tar.zst
```

The archive is emitted only after `MANIFEST.json` and `CHECKSUMS` validate.
Keep stderr separate from stdout because stdout carries the archive bytes.

## Verify Completion

A dump is complete only when `MANIFEST.json` exists. The producer writes it
atomically after every adapter has finalized and after `CHECKSUMS` includes the
prepared manifest digest.

```sh
test -f /backups/elastickv-2026-07-18/MANIFEST.json
cd /backups/elastickv-2026-07-18
sha256sum -c CHECKSUMS
jq '{phase, cluster_id, last_commit_ts, live, adapters}' MANIFEST.json
```

On macOS, use `shasum -a 256 -c CHECKSUMS`.

Expected properties include:

- `phase` is `phase1-live-pinned`;
- `live.read_ts` is non-zero;
- every selected adapter and scope is present;
- checksum verification reports success for every entry, including
  `MANIFEST.json`.

The completion log reports `read_ts`, scope and record counts,
`pin_renewals_total`, and the selected output format. For a long-running dump,
the renewal count should increase. A completed backup with zero renewals is
normal when the dump finishes within the first third of its effective TTL.

## Monitor A Running Backup

Monitor the backup process together with every cluster member. The server-side
signals that require investigation are `backup_pin_expired`, failed backup-pin
proposals, snapshot-headroom refusal, and repeated leader-election errors during
renewal. A renewal failure is terminal for that run; the producer cancels the
stream and never publishes `MANIFEST.json`.

Track free space and inode use on the backup host while the dump runs:

```sh
df -h /backups
df -i /backups
du -sh /backups/elastickv-2026-07-18
```

Per-item DynamoDB output can consume one inode per item. Capacity planning must
include that inode cost until JSONL output and its native reverse encoder ship
together.

## Failure Handling

- No `MANIFEST.json`: the tree is incomplete and must not be restored or
  archived as a valid backup. Retain it only for diagnosis, then choose a new
  output directory for the retry.
- `ResourceExhausted`: the cluster reached `--backupMaxActivePins`. Find the
  active backup jobs or wait for their TTL cleanup before retrying.
- `FailedPrecondition` at begin: inspect the reported old or unreachable
  member, snapshot headroom, or shard catch-up failure. No scan has started.
- Renewal failure: the producer cancels the stream, omits `MANIFEST.json`, and
  calls `EndBackup` with the latest token. The replicated TTL remains the final
  cleanup bound if the client is terminated.
- Expected-key shortfall: treat it as a consistency failure. Do not reuse the
  partial tree; investigate retention, snapshot installation, and compaction.
- `EndBackup` failure after manifest publication: the artifact is complete,
  but the command reports failure because cleanup was not confirmed. Verify
  the dump, monitor pin expiry, and investigate Admin/Raft reachability.
- Archive-stream failure after manifest publication: the directory tree is a
  complete backup, but stdout may contain a truncated archive. Discard the
  archive stream, verify the directory tree again, and repack from that tree.

## Restore Through A Native Snapshot

Phase 1 produces the same logical tree accepted by the offline snapshot
encoder. To restore into a fresh or stopped cluster, first encode and self-test
the tree:

```sh
elastickv-snapshot-encode \
  --input /backups/elastickv-2026-07-18 \
  --output /restore/elastickv-2026-07-18.fsm \
  --self-test
```

Then follow [Snapshot Restore Runbook](./snapshot_restore.md) to prepare a new
Raft data directory and start the replacement cluster. Do not merge the
prepared directory into an existing WAL or snapshot directory.

Direct replay into a running cluster through public DynamoDB, S3, Redis, and
SQS endpoints is the separate Phase 2 restore consumer and is not part of the
Phase 1 producer.

## Periodic Recovery Drill

Treat checksum verification alone as artifact validation, not recovery proof.
On a schedule, encode a completed logical tree with `--self-test`, restore it
into an isolated replacement cluster, and compare adapter scope counts and
representative object/item/key/message contents at the recorded
`live.read_ts`. Do not run the drill against an existing Raft data directory.
