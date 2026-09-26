# Elastickv glossary

The vocabulary this repository uses, in the sense the repository uses it.
Every entry paraphrases a definition that already exists in a design doc, a
runbook, or a package comment; the source is named so the full mechanism can
be read there. Use these terms in identifiers, doc titles, test names, and
design docs. If a concept you need is missing, that is a signal: either the
project does not use that language (reconsider) or there is a real gap
(extend this file through `/domain-modeling`). This file holds vocabulary
only; decisions live in `docs/design/`, invariants in `CLAUDE.md`.

## Cluster and replication

- **Elastickv** — An experimental distributed key-value store aimed at cloud
  environments in the manner of DynamoDB, built on Raft replication. Not
  production-ready. (`README.md`)
- **Raft group** — The unit of replication. A key range's group ID names the
  Raft group that owns it; one process can run several groups, each electing
  its leader independently. (`distribution/engine.go`,
  `docs/design/2026_06_11_implemented_leader_balance_scheduler.md`)
- **default Raft group** — The lowest-ID group. Its reserved internal keys
  hold the route catalog, and HLC ceiling entries replicate through it.
  (`CLAUDE.md`, `docs/architecture_overview.md`)
- **FSM (KV FSM)** — Applies committed Raft entries to the storage layer and
  to the HLC ceiling. (`CLAUDE.md`, `kv/fsm.go`)
- **voter** — A full member counted in the quorum. (`docs/raft_learner_operations.md`)
- **learner** — A member that receives entries and snapshots and applies them
  locally but does not vote, is not counted toward a majority, and cannot be
  a leadership-transfer target. (`docs/design/2026_04_26_implemented_raft_learner.md`)
- **fresh learner join** — The join mode for a wiped or replacement node that
  has none of the durable local state identifying an existing member; it is
  deliberately distinct from a normal restart, which recovers membership
  from disk. (`docs/design/2026_07_18_implemented_raft_fresh_learner_join.md`)
- **fenced voter replacement** — The scripted, non-automatic operation
  (`scripts/raft-member-replace.sh`) that replaces one voter under the same
  ID in one group while a surviving quorum remains: fence the old owner, then
  join fresh. (`docs/design/2026_07_18_implemented_fenced_raft_member_replacement.md`)
- **leader balance scheduler** — Spreads group leaderships across nodes so
  one node does not lead every group and carry all leader-only work.
  (`docs/design/2026_06_11_implemented_leader_balance_scheduler.md`)
- **follower forwarding** — Serving a request locally on a follower would
  expose stale reads, so every follower request is forwarded to the leader.
  (`adapter/dynamodb.go`)
- **LeaderProxy / leader proxy circuit breaker** — The forwarding component,
  which bounds one request with a retry budget; the circuit breaker gives it
  cross-request memory so repeated work against a leader identity that just
  failed is suppressed while one request may still wait through a short
  election. (`docs/design/2026_07_19_implemented_leader_proxy_circuit_breaker.md`)
- **raft-engine marker** — A marker written into each Raft data directory so
  a node refuses to reopen the directory under a different backend.
  (`docs/etcd_raft_migration_operations.md`, `README.md`)
- **SST ingest snapshot transfer** — A Pebble-backed state machine emits
  checkpoint-derived external SST files; the Raft snapshot transport carries
  them in a self-describing stream and the receiver verifies and ingests them
  into an empty temporary database before replacing the live one.
  (`docs/design/2026_07_19_implemented_pebble_sst_ingest_snapshot_transfer.md`)
- **snapshot disk offload** — The etcd-engine change that never materializes
  FSM data as a byte slice and keeps only a small token in memory storage.
  (`docs/design/2026_04_14_implemented_etcd_snapshot_disk_offload.md`)
- **physical snapshot object offload** — Periodic per-group physical
  snapshots stored in an external S3-compatible object store with bounded
  retention, restorable into a fresh data directory; the object layer treats
  the FSM payload as opaque bytes.
  (`docs/design/2026_07_19_partial_physical_snapshot_object_offload.md`)

## Routing and sharding

- **route** — A mapping from a key range to a Raft group. Ranges are right
  half-open `[Start, End)`; a nil `End` is unbounded. (`distribution/engine.go`)
- **RouteState** — A route's control-plane state: `Active`, `WriteFenced`
  (writes blocked during cutover), `MigratingSource`, `MigratingTarget`.
  (`distribution/catalog.go`)
- **route catalog** — The durable route records persisted in reserved keys of
  the default Raft group. All mutations go through control-plane RPCs, never
  direct key writes. (`CLAUDE.md`, `distribution/catalog.go`)
- **catalog version** — The catalog's monotonic version. It bumps on every
  mutation so watchers fan out; `SplitRange` requires the caller's expected
  version to match. (`CLAUDE.md`, `README.md`)
- **RouteEngine** — The in-memory route cache that every routing decision
  reads. It is refreshed by the catalog watcher, never read from the catalog
  directly. (`distribution/engine.go`, `docs/architecture_overview.md`)
- **catalog watcher** — Applies versioned catalog snapshots into the route
  engine on every node. (`CLAUDE.md`, `distribution/watcher.go`)
- **route catalog delta watch** — A durable, versioned route delta log with a
  server-streaming gRPC watch that replaces full-catalog refresh as the
  steady-state propagation path; stale cursors reset from a full snapshot and
  rolling upgrades fall back to the poll.
  (`docs/design/2026_07_18_implemented_route_catalog_delta_watch.md`)
- **ShardedCoordinator** — The data-plane entry point every adapter
  dispatches into. It resolves keys through the ShardRouter against the
  RouteEngine, then drives a ShardStore per group. (`CLAUDE.md`,
  `kv/sharded_coordinator.go`)
- **ShardRouter** — Routes requests to Raft groups by key range.
  (`kv/shard_router.go`)
- **ShardStore** — Routes MVCC reads to shard-specific stores and proxies to
  leaders when needed. (`kv/shard_store.go`)
- **PartitionResolver / partition map** — Maps a key to its owning group for
  partition-scheme keyspaces such as SQS HT-FIFO, consulted before the
  byte-range engine; operators supply it with `--sqsFifoPartitionMap`.
  (`kv/shard_router.go`, `docs/design/2026_04_26_implemented_sqs_split_queue_fifo.md`)
- **SplitRange** — The control-plane RPC that splits a route at a key.
  Milestone 1 splits inside the same group only; no data moves.
  (`README.md`, `proto/distribution.proto`)
- **hotspot shard split** — The program to detect hot ranges, split them,
  move children to other groups, and keep consistency throughout, in three
  milestones: M1 control plane (implemented), M2 migration plane (partial),
  M3 automation (implemented, same-group only).
  (`docs/design/2026_02_18_partial_hotspot_shard_split.md`)
- **SplitJob** — The durable, resumable state of an M2 migration
  (`PLANNED → BACKFILL → FENCE → DELTA_COPY → CUTOVER → CLEANUP → DONE`),
  persisted beside the route catalog.
  (`docs/design/2026_06_11_partial_hotspot_split_milestone2_migration.md`)
- **Composed-1** — The TLA+ safety property that every committed write key
  was owned by the committing group at the transaction's observed catalog
  version. (`docs/design/2026_05_29_implemented_composed1_cross_group_commit_guard.md`)
- **ObservedRouteVersion** — The catalog version at which a transaction's
  read set was captured; zero means unpinned. (`proto/internal.proto`)
- **route shuffle** — The Jepsen nemesis that issues catalog splits during a
  workload. (`docs/design/2026_06_02_implemented_composed1_m5_jepsen_route_shuffle.md`)

## Transactions and timestamps

- **HLC (hybrid logical clock)** — The 64-bit timestamp: a 48-bit physical
  half (Unix milliseconds) whose upper bound is agreed through Raft, and a
  16-bit logical counter advanced in memory. (`kv/hlc.go`)
- **physical ceiling** — The Raft-agreed upper bound on the physical half.
  The leader periodically commits a ceiling entry; every node applies it, so
  a new leader never issues timestamps inside the previous leader's window.
  (`kv/hlc.go`)
- **logical counter** — The in-memory low half, incremented per `Next()`
  within a millisecond, reset when wall time advances. No Raft round trip.
  (`kv/hlc.go`)
- **NextFenced / ErrCeilingExpired** — The persistence-grade allocation entry
  point; it refuses a timestamp once the wall clock reaches the ceiling and
  no renewal has landed. (`kv/hlc.go`)
- **TSO (timestamp oracle)** — The centralized allocation path: a local
  allocator over the leader's HLC plus a batch allocator that hands out
  slots from committed windows. (`docs/design/2026_04_16_implemented_centralized_tso.md`)
- **Phase D** — The TSO mode in which the group-0 leader is the only issuer
  of persistence timestamps and writes may persist only at timestamps the
  TSO durably handed out. (`docs/design/2026_08_29_proposed_tso_batch_slot_claims.md`)
- **StartTS** — A transaction's start timestamp; conflict validation checks
  that no read or write key was committed after it. (`proto/internal.proto`)
- **CommitTS** — The timestamp at which a value or tombstone is committed.
  Intended invariant: unique, so a version at a given commit timestamp
  belongs to exactly one transaction; on `main` today the default TSO mode
  lets separate nodes issue the same value (audit gap G10), and A0b restores
  the invariant. (`store/store.go`)
- **PrevCommitTS** — The commit timestamp of a failed earlier attempt of the
  same transaction, used as that attempt's identity for dedup.
  (`kv/coordinator.go`)
- **read set / ReadKeys** — The keys a transaction read, carried in the
  request so the FSM validates read-write conflicts atomically with the
  commit. (`proto/internal.proto`, `kv/fsm.go`)
- **write conflict** — `ApplyMutations` fails with `ErrWriteConflict` when any
  mutation key or read key has a commit newer than `StartTS`. (`store/store.go`)
- **OCC (optimistic concurrency control)** — Commit-time validation of a
  transaction's write set and read set against `StartTS` (see *write
  conflict*); reads take no locks while the transaction runs. Multi-shard
  commits do write per-key transaction locks and intents for their write
  keys only at PREPARE, held until COMMIT, ABORT, or the LockResolver
  resolves them; read keys get no lock. The acronym is used
  throughout the code and TLA+ modules without expansion. (`kv/fsm.go`,
  `kv/lock_resolver.go`)
- **one-phase transaction** — A single-shard transaction applied by one Raft
  entry, with write-write and read-write conflicts checked under the apply
  lock. (`kv/fsm.go`)
- **one-phase dedup** — Retry safety for one-phase transactions: a stable
  write set plus an exact-`commit_ts` probe at apply, separating the dedup
  identity from the fresh commit ordering.
  (`docs/design/2026_05_21_implemented_txn_secondary_idempotency.md`)
- **two-phase commit (2PC)** — The multi-shard path, taken when mutations or
  read keys span more than one shard. (`kv/sharded_coordinator.go`)
- **prewrite / PREPARE** — The first 2PC phase; each write shard's read keys
  travel in its PREPARE entry and are validated under the apply lock.
  (`store/store.go`, `kv/sharded_coordinator.go`)
- **primary key (transaction)** — The lexicographically smallest write key;
  its COMMIT record decides the transaction's outcome. (`kv/coordinator.go`)
- **secondaries** — The non-primary shards of a 2PC transaction; their
  commits are best-effort and read-time lock resolution finishes them.
  (`kv/sharded_coordinator.go`)
- **LockResolver** — The background component that scans for expired
  transaction locks and resolves them. (`kv/lock_resolver.go`)
- **linearizable read / ReadIndex** — A read that blocks until the returned
  index is safe to serve from the local FSM; the slow path is etcd/raft
  `ReadOnlySafe`. (`internal/raftengine/engine.go`,
  `docs/design/2026_04_20_implemented_lease_read.md`)
- **lease read** — A read served from local state while the leader-local
  lease (tracked on a monotonic clock by `leaseState`) is valid; expired
  leases fall back to a linearizable read. (`kv/lease_state.go`,
  `docs/design/2026_04_20_implemented_lease_read.md`)

## Storage

- **MVCC store** — The multi-version store interface; every operation takes
  an explicit snapshot or commit timestamp, and `GetAt` returns the newest
  version at or below it. (`store/store.go`)
- **compaction / FSMCompactor** — Removal of versions older than a minimum
  timestamp, run on a timer off the apply path. (`store/store.go`,
  `docs/design/2026_03_20_implemented_fsm_compaction.md`)
- **retention (RetentionController)** — The minimum timestamp a store still
  retains; older reads fail with `ErrReadTSCompacted`. (`store/store.go`)
- **TTL inline value** — Expiry carried inside the value instead of a
  separate TTL key, so data and expiry come from one commit.
  (`docs/design/2026_04_17_implemented_ttl_inline_value.md`)

## Protocol adapters

- **adapter** — One per wire protocol under `adapter/`: gRPC RawKV /
  TransactionalKV, Redis, DynamoDB, S3, SQS, and the operator control plane.
  (`CLAUDE.md`)
- **filesystem on Elastickv** — A filesystem layer with fixed-size chunks and
  a placement policy that keeps a file's chunks on one shard, exposed through
  FUSE. (`docs/design/2026_02_24_implemented_filesystem_on_elastickv.md`)
- **redis-proxy (dual-write / shadow-read)** — The Redis-protocol reverse
  proxy for phased migration: dual-write writes to both stores, shadow-read
  compares the Elastickv read against Redis. (`docs/redis-proxy-deployment.md`)
- **workload isolation** — Keeping one expensive command path from starving
  the others that share a Go runtime: heavy-command concurrency limits,
  per-peer admission, and removal of O(N) hot paths.
  (`docs/design/2026_04_24_implemented_workload_isolation.md`)
- **admission control (S3)** — A per-node bound on in-flight PUT body bytes
  in the leader's Raft pipeline; when full, clients get `SlowDown`.
  (`docs/design/2026_04_25_implemented_s3_admission_control.md`)
- **S3 raft blob offload** — Keeping large object payloads out of the Raft
  log so snapshots and follower catch-up stay bounded.
  (`docs/design/2026_04_25_partial_s3_raft_blob_offload.md`)
- **HT-FIFO (high-throughput FIFO)** — SQS's partitioned FIFO mode; Elastickv
  gives each partition its own Raft group, preserving order per message
  group. (`docs/design/2026_04_26_implemented_sqs_split_queue_fifo.md`)
- **DLQ redrive** — Dead-letter handling: a message moves to the configured
  DLQ when the next receive would exceed the redrive policy's maximum.
  (`docs/design/2026_06_16_implemented_sqs_dlq_redrive_admin_ui.md`)
- **SigV4 static credentials** — The JSON credentials file (shared by
  `--s3CredentialsFile` and `--sqsCredentialsFile`) mapping access keys to
  secrets; an empty path leaves authorization off. (`main_sigv4_creds.go`)

## Encryption

- **envelope encryption** — AES-256-GCM primitives, a wire-format envelope,
  and an in-memory keystore; 34 bytes of overhead per value.
  (`internal/encryption/doc.go`)
- **storage envelope** — The authenticated envelope around every value
  before it reaches Pebble; each user value is encrypted exactly once, at the
  storage layer. (`docs/design/2026_04_29_partial_data_at_rest_encryption.md`)
- **raft envelope** — The separate envelope around the FSM payload under a
  distinct Raft DEK, with a distinguishing AAD prefix so storage ciphertext
  cannot be replayed into the Raft layer. (`internal/encryption/raft_envelope.go`)
- **KEK (key encryption key)** — The master key held outside the cluster,
  from a file, an environment variable, AWS KMS, GCP KMS, or Vault Transit
  (`--kekFile`, `--kekUri`). No default; with encryption enabled and no KEK
  source the process refuses to start.
  (`docs/design/2026_04_29_partial_data_at_rest_encryption.md`,
  `docs/design/2026_07_18_implemented_9b_kek_providers.md`)
- **DEK (data encryption key)** — A 32-byte AES key per purpose (storage,
  Raft), generated on the leader and committed through Raft so every replica
  sees the same key ID at the same index.
  (`docs/design/2026_04_29_partial_data_at_rest_encryption.md`)
- **sidecar (encryption)** — The per-node `keys.json` holding KEK-wrapped
  DEKs and encryption state, reconciled against Raft on startup.
  (`docs/design/2026_04_29_partial_data_at_rest_encryption.md`)
- **writer registry** — The Raft-replicated registry, keyed per DEK, that
  keeps the nonce-uniqueness invariant unconditional.
  (`internal/encryption/registry.go`)
- **compress-then-encrypt** — Snappy on plaintext before AES-GCM, chosen only
  when strictly smaller, with the compressed flag authenticated in the AAD.
  (`docs/design/2026_07_18_implemented_9a_encryption_compression.md`)

## Backup and snapshots

- **logical snapshot decoder / encoder** — Offline tools that turn a native
  `.fsm` snapshot into a vendor-independent per-adapter directory tree and
  back. (`docs/design/2026_04_29_implemented_snapshot_logical_decoder.md`,
  `docs/design/2026_05_25_implemented_snapshot_logical_encoder.md`)
- **logical backup (Phase 1)** — Live, cluster-wide point-in-time extraction
  across Raft groups through `BeginBackup` / `RenewBackup` / `EndBackup`, in
  the same on-disk format as the offline tools.
  (`docs/design/2026_04_29_implemented_logical_backup.md`)

## Observability and operations

- **KeyViz (key visualizer)** — The TiKV-style time × key-range heatmap of
  load, fed by a hot-path-safe per-route sampler; series are read and write
  counts and bytes. (`docs/admin_ui_key_visualizer_design.md`, `keyviz/`)
- **KeyViz label** — The adapter-family label on samples (`dynamo`, `redis`,
  `s3`, `sqs`, `rawkv`). (`keyviz/labels.go`)
- **hot-key top-K** — Per-cell drill-down to the hottest keys via a bounded
  Space-Saving sketch with a reported error bound.
  (`docs/design/2026_05_28_implemented_keyviz_hot_key_topk.md`)
- **admin dashboard** — The optional separate HTTP listener serving a React
  SPA and JSON API for cluster inspection and DynamoDB / SQS / S3 management,
  including item and object CRUD (the data browser). Disabled by default.
  (`docs/admin.md`, `docs/design/2026_05_22_implemented_admin_data_browser.md`)
- **rolling update over Tailscale** — The GitHub Actions deployment that
  rolls `scripts/rolling-update.sh` across tailnet nodes.
  (`docs/design/2026_04_24_implemented_deploy_via_tailscale.md`,
  `docs/deploy_via_tailscale_runbook.md`)

## Verification and process

- **TLA+ safety specs** — Machine-checked models of HLC, OCC, MVCC, routes,
  and their composition under `tla/`, run by `scripts/tla-check.sh` with gap
  configurations that must fail. (`tla/README.md`)
- **Jepsen workloads** — Fault-injection workloads under
  `jepsen/src/elastickv/` for the Redis, DynamoDB, S3, and SQS surfaces, using
  Elle list-append, knossos registers, or custom checkers; gRPC and the
  filesystem have none yet. (`CLAUDE.md`, `.github/workflows/jepsen-test.yml`)
- **design doc lifecycle** — `proposed` (accepted, not implemented),
  `partial` (some milestones shipped), `implemented` (as-built record), as
  filename markers under `docs/design/`. (`docs/design/README.md`, `CLAUDE.md`)
- **five review lenses** — The five passes every code change is reviewed
  through, one at a time, with each result recorded in the PR description:
  data loss, concurrency / distributed failures, performance, data
  consistency, test coverage. (`CLAUDE.md`)
