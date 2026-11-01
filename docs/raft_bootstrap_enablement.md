# Existing-cluster bootstrap enablement

Author: bootjp

This procedure is an explicit, offline accepted-baseline migration. It does not
prove historical serializability or repair replicas that already disagree.
Do not delete member data, rebootstrap a voting identity, or select a maximum
watermark to force disagreeing probes to match.

## Preconditions

- Stop writes, drain prepared transactions and maintenance, then stop every
  member of every affected group. Retain an independently verified backup.
- Preserve the original cluster UUID, group IDs, member IDs, addresses, TLS
  SPKI pins, full role roster, WAL, snapshots, term and vote.
- Use a fresh nonzero 32-digit lowercase hexadecimal operation ID per group.
  Reuse that ID only when resuming that same migration.
- The initial supported profile uses an already populated replicated catalog,
  plaintext stores and no startup encryption rotation. Unsupported encryption
  profiles fail startup explicitly. Membership reconfiguration and lost-state
  replacement require separate protocols; this procedure does not authorize them.
- Use canonical absolute paths. Keep artifacts outside the source data tree.
  Commands refuse overwriting existing output artifacts.
- Protected Redis wire `MULTI`/`EXEC`/`DISCARD` is currently refused before
  admission: retaining transaction ownership across idle queued commands is
  not implemented. Do not enable this profile for clients requiring those
  commands. Ordinary writes, Lua and internal transactions have owned contexts;
  legacy mode retains its existing wire transaction behavior.

## Observe And Agree

Create independently reviewed `pins.json` with `version`, `cluster_uuid`,
`group_id`, `minimum` and `members`. Version is `1`; minimum entry, snapshot and
session versions are each `1`. Group and member IDs are canonical decimal
strings. Each member supplies `member_id`, `address`, `public_key_sha256` and
`roles` (`voter`, `outgoing`, `learner`, `next`). Include every current member,
not merely a quorum. The pins must not contain a selected cut or data digest.
Optional `auto_leave` defaults to false; true is rejected because an automatic
configuration transition is not a stable offline baseline. Quiesce that
transition first rather than changing the pinned evidence to hide it.

Each node also needs its existing peer identity configuration: `cluster_uuid`,
numeric `local_member`, `members` (numeric IDs, address, SPKI pin and numeric
`groups`), and absolute `ca_file`, `cert_file`, `key_file` paths. Certificates
must validate for server and client use and match their independently pinned
local identity. Never distribute private keys with collected artifacts.

On every member, using its actual group data directory:

```sh
./elastickv bootstrap probe --pinned-config /ops/pins.json \
  --peer-config /ops/group-1.json --raft-data-dir /data/group-1 \
  --operation-id "$OP" --member-id 1 --output /ops/member-1.probe
```

The command obtains native offline ownership, measures retained data and actual
committed native state, then signs only that local observation. Standard output
is a measured expected-config proposal. Independently compare and review the
proposals from all members; retain the agreed proposal as `/ops/expected.json`.
Any different cut, role, data digest or watermark requires investigation before
migration. Do not edit evidence or normalize a differing cut.

Collect the public signed probes and assemble one manifest:

```sh
./elastickv bootstrap manifest --expected-config /ops/expected.json \
  --operation-id "$OP" --probe /ops/member-1.probe \
  --probe /ops/member-2.probe --probe /ops/member-3.probe \
  --output /ops/baseline.manifest
```

## Prepare And Certify

Run `inspect`, then `prepare`, then `resume` on every member, with the same
manifest, expected configuration, operation ID and that member's original ID:

```sh
./elastickv bootstrap inspect --expected-config /ops/expected.json \
  --manifest /ops/baseline.manifest --raft-data-dir /data/group-1 \
  --operation-id "$OP" --member-id 1
```

Replace `inspect` with `prepare` and then `resume`. Preparation retains a
verified source backup and journal. Resume completes the exact prepared
transformation; a successful bound migration is not yet policy activation.
Retain the journal and backups when any command reports an ambiguous failure.

While **all nodes remain stopped**, attest each bound local state:

```sh
./elastickv bootstrap attest --expected-config /ops/expected.json \
  --manifest /ops/baseline.manifest --raft-data-dir /data/group-1 \
  --operation-id "$OP" --member-id 1 --peer-config /ops/group-1.json
```

Retain each public JSON acknowledgment and collect all of them. The attest
command verifies the bound WAL, marker, intent, original backup and exact store
transformation before signing. Original probes are not readiness acknowledgments.

```sh
./elastickv bootstrap certify --expected-config /ops/expected.json \
  --manifest /ops/baseline.manifest --operation-id "$OP" \
  --ack /ops/member-1.ack --ack /ops/member-2.ack --ack /ops/member-3.ack \
  --output /ops/activation.bin
```

Repeat independently for every group, including dedicated TSO group zero when
present. Only then configure each group's `activation_file` with that group's
complete activation artifact and `startup_token_file` with a shared startup-only
credential distinct from admin, migration and S3 credentials.
Keep the independently supplied activation artifact configured across restarts.
A received snapshot can carry another member's original baseline probe. Such
a probe is only source evidence: runtime must match it to the complete configured
manifest and separately verify this receiver's own version-2 WAL identity and
preparation digest. A snapshot never establishes local preparation by itself.

## Start And Verify

Start all members with their original identities and the protected per-group
configuration directory, using `--raftBootstrapPeerConfigDir=/ops/protected`.
That directory contains `group-N.json` for every local group, including group
zero when dedicated TSO is present. Identity/session traffic must be available before
catalog readiness; public data remains closed until every local group reaches
DataReady. Never treat an activation RPC timeout as proof of either abort or
success: inspect independently recovered durable policy and applied progress.

Verify the same activated policy on all replicas, catalog routes, ordinary
leader and follower read/write paths, dedicated TSO when used, and a controlled
restart without losing data or policy. Keep backups and migration artifacts
until these checks and the latest-HEAD review/CI gates have completed.
