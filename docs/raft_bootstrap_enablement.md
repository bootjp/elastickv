# Existing-cluster bootstrap enablement

Author: bootjp

This procedure is an explicit, offline accepted-baseline migration. It does not
prove historical serializability or repair replicas that already disagree.
Do not delete member data, rebootstrap a voting identity, or select a maximum
watermark to force disagreeing probes to match.

## Scope And Verification Status

The offline procedure below accepts existing **plaintext** native state. It
covers data groups and dedicated TSO group zero; the latter reconstructs its
complete legacy scalar state from the verified original snapshot/WAL backup
and explicitly initializes `tso-control.db`. Runtime does not automatically
import legacy TSO snapshots into a missing control database. Resume verifies
the original backup, not the already transformed target as a new baseline.

Protected Redis wire `MULTI`/`EXEC`/`DISCARD` has receiver-owned queue and
execution support; it is no longer categorically refused. The encryption
runtime extension is **only** the exact local startup recipe requested by
`--encryptionRotateOnStartup`, with an already active storage envelope.
Inactive storage-envelope configurations are synchronously unsupported before
worker startup, including inactive-key registration. Raft envelope activation
and multigroup profiles must still pass the existing namespace/runtime scope
validation; this recipe grants no exception. Its supervisor/installed-projection
wiring is implemented and verified for the single-data-group active storage
and Raft envelope profile described below. This is not authorization for
arbitrary external `EncryptionAdmin.RotateDEK`, enable or bootstrap RPCs;
those remain fail-closed unless separately wired. Do not infer availability
of that profile from helper tests or bypass a closed gate to force startup.

There is no encrypted legacy/offline migration procedure here. A plaintext
baseline with committed but inactive DEK metadata is not an encrypted source.
Cold seed, missing-state replacement and arbitrary protected membership
reconfiguration remain out of scope. Selected plaintext native snapshot
catch-up/restart regressions do not certify the full v5 snapshot/journal
protocol. Integration tests do not establish historical correctness or
production operation; final composed review and CI remain required.

## Preconditions

- Stop writes, drain prepared transactions and maintenance, then stop every
  member of every affected group. Retain an independently verified backup.
- Preserve the original cluster UUID, group IDs, member IDs, addresses, TLS
  SPKI pins, full role roster, WAL, snapshots, term and vote.
- Use a fresh nonzero 32-digit lowercase hexadecimal operation ID per group.
  Reuse that ID only when resuming that same migration.
- The accepted-baseline source uses an already populated replicated catalog,
  plaintext stores and plaintext retained snapshot/WAL payloads. Do not use
  startup rotation to bypass this offline source restriction. Preserve any
  committed inactive DEK metadata and its required key material unchanged.
  Unsupported encryption profiles fail closed. Membership reconfiguration and
  lost-state replacement require separate protocols; this procedure does not
  authorize them.
- Use canonical absolute paths. Keep artifacts outside the source data tree.
  Commands refuse overwriting existing output artifacts.
- Keep encryption sidecars, KEK access and original full writer identities
  alongside the retained migration/restart artifacts when encryption is
  configured. Missing or inconsistent durable/installed state is not repaired
  by deleting a sidecar, registering a new identity or relaxing admission.

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

## Protected Wire Transactions

`MULTI` acquires one receiver-local owner. Queued commands copy their arguments
and retain that owner through `EXEC` or `DISCARD`; neither queued commands nor
EXEC obtain replacement authority. A nested MULTI returns its protocol error
without replacing the original queue. Unsupported commands and queue-time arity
failures poison EXEC with `EXECABORT`; ordinary execution errors retain their
per-element result semantics. A queue is bounded by 65,536 commands and a 64 MiB
accounting budget including copies/overhead. All protected transaction queues
also share a server-wide 64 MiB budget, reserved before cloning commands and
retained until execution and owned children finish. Resource overflow discards
the transaction and closes its connection; buffered commands cannot become new
standalone writes. Idle queued transactions expire after 30 seconds by default.
These bounds cover retained protected queue copies, not all parser buffers,
execution allocations or legacy-mode queues.

Revocation discards the queue and closes the old connection. Reconnect only
for a genuinely new request; do not attach old queued work to a new handle.
Disconnect, idle timeout and server Close cancel and join owned children;
context-insensitive work retains ownership until it has actually returned.
Follower EXEC forwarding retains the original receiver owner, uses a dedicated
target connection and does not automatically retry a lost reply. A top-level
unknown-outcome error, including terminal response overflow after a write,
must not be interpreted as an EXEC abort or proof that writes were absent.
Legacy mode retains its existing transaction behavior.

## Protected Startup Rotation

For a separately verified protected deployment with the existing encryption
configuration/key material, `--encryptionRotateOnStartup` requests the local
typed startup recipe. It does not supply admin authority to callers. Keep the
public data gate closed while rotation responsibility or the current installed
encryption projection is unresolved. A completed startup Wait is not permanent
permission: later election, native encryption apply or wrapper publication can
invalidate the generation and require fresh recovery/registration.

An already active storage envelope is mandatory. Startup rejects an inactive
storage envelope synchronously, before workers start; waiting for registration
or identity-only readiness cannot make that profile supported. Preserve the
existing Raft envelope activation, namespace and multigroup restrictions.
Local startup rotation does not enable a previously unsupported combination.
The actual single-data-group active storage and Raft envelope E2E covers local
requested startup rotation, business write/read and protected restart; the final
focused race run passed in 22.119s. Its accepted offline baseline cut remains
plaintext. Encrypted three-member follower permission (F), term-change and
lost-result scenarios remain unvalidated end to end; lifetime unit coverage is
not a substitute. This bounded result does not establish production operation.

Validation uses the actual installed wrapper, active Raft key, nonce identity
and cutover state, checked against durable sidecar/checkpoint/registration
evidence. A sidecar naming a new key does not mean that key is already installed
for writes. Registration can run through the still-installed old wrapper while
the new key is pending; the decoder must not depend on completion of the very
registration it is needed to admit. Authenticated complete inner parsing and
exact recipe grants are required even for encrypted controls. The final
admission seal covers ciphertext; an opaque encrypted prefix is not a bypass.

On a follower, the supervisor must complete the native Recovery ReadIndex and
applied barrier under its original handle, then validate/publish the current
installed projection before local permission can reopen. This recovery-only
path does not relax ordinary read or peer-proposal admission. Lost replies,
leadership changes and cancellation leave an owned rotation outcome unresolved
until independent reconciliation; do not delete state or issue an arbitrary
admin retry to force completion. Fatal mismatches keep admission closed.

Do not use this runtime path to migrate encrypted legacy snapshots/WAL or
stores. Check the exact build/profile against the bounded evidence above before
operational enablement. Composed lint/review remains a separate gate; single-node
rotation/restart and native follower-barrier tests do not substitute for the
unvalidated encrypted multi-member scenarios.
