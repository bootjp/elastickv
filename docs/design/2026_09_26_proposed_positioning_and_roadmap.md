# Positioning and near-term roadmap

Status: Proposed
Author: bootjp
Date: 2026-09-26

This is a roadmap document (the kind `docs/design/README.md` admits under
"Concrete implementation plans"). It records where elastickv is aimed, what it
claims, how that is proven, and in what order the next work lands. It proposes
no implementation itself; the milestones it orders each get their own design
doc. It supersedes the priority ordering implied by
`2026_06_23_proposed_scaling_roadmap.md`, which stays in force as the
description of the future scale-out track.

The content was settled in a structured design interview on 2026-09-26; the
answers are recorded under **Decisions** at the end so later readers can see
what was chosen and what was merely assumed. Every statement about current
code or docs was re-checked against `main` at `4ca7e90d` before this doc was
written.

---

## 1. What elastickv is for

**Who.** People who build against AWS-style APIs (DynamoDB, S3, SQS) and Redis
and need to run the same application outside AWS: on-premises, at the edge, or
inside an air-gapped network, on three to five machines they operate
themselves.

**What.** One Raft-replicated, MVCC/OCC store that speaks those protocols from
a single binary, with serializable multi-key transactions and per-key
linearizability, and no external timestamp or placement service to run
alongside it.

**Proof.** The engineering record is a deliverable, not a by-product: every
non-trivial change has a design doc, safety properties are model-checked in
TLA+ (`tla/`), and the Redis, DynamoDB, and S3 surfaces each have a Jepsen
workload (`jepsen/src/elastickv/`; the SQS workload exists but validates
nothing today because of a harness bug recorded in the audit's A4, and gRPC
and the filesystem have none yet, see §6.3). The same artifact therefore serves two audiences:
adopters who want a self-hosted AWS-compatible store, and readers who want a
documented, verified multi-raft transactional KV in Go.

**Success, in order.** (1) Research, learning, and technical proof: write-ups
and talks backed by the verification record. (2) Adoption: other people run it
and report issues. Capacity is one maintainer plus coding agents, a few hours a
week, no deadline; the plan below is sized for that.

## 2. Comparison

| Dimension | FoundationDB | TiKV | DynamoDB | Bigtable | elastickv |
|---|---|---|---|---|---|
| API surface | Key-value core plus separately deployed layers (Record Layer, Document Layer) | Raw KV + transactional KV (gRPC), coprocessor; SQL via TiDB | Item API (partition key model), transactions | Wide-column (HBase API) | gRPC RawKV / TransactionalKV, Redis, DynamoDB, S3, SQS, and a FUSE filesystem, in one process |
| Transactions | Strict serializable ACID | Percolator: snapshot isolation, optimistic or pessimistic | Serializable for `TransactWriteItems` / `TransactGetItems` | Single-row atomicity only | Atomic across keys and shards (2PC with OCC validation of write and read sets at FSM apply); serializable is the target pending the audit's A0 to A2 (§6.3); per-key linearizable except on the G10 paths until A0b lands |
| Timestamps / ordering | Sequencer process role | PD as global TSO | Managed | Managed | HLC issued by Raft leaders; physical half fenced by a Raft-agreed ceiling; optional centralized TSO (group 0, Phase D, opt-in via `--tsoPhaseDEnabled`) with batch allocation; no external service |
| Scale-out | Data distribution + storage roles; single region primary + DR | Auto region split / merge / rebalance via PD | Elastic, managed | Massive, managed | Multi-raft groups with a durable route catalog and streaming delta watch; automatic same-group split (keyviz-driven); cross-group migration in progress; no merge, no automatic rebalancing yet |
| Operations | Many process classes, cluster file | PD + TiKV nodes, tiup | None (managed) | None (managed) | Single binary per node, `rolling-update.sh` over Tailscale from GitHub Actions; learner join, fenced voter replacement; admin dashboard + key visualizer; no Kubernetes operator |
| Verification story | Deterministic simulation | Jepsen (TiDB), tests | Internal (formal methods, TLA+) | Internal | Design docs, TLA+ (HLC, OCC, MVCC, routes, composed), Jepsen for Redis / DynamoDB / S3 / SQS (Elle list-append, knossos, custom checkers) |
| Where elastickv is weaker today | Strict serializability, simulation testing | Auto migration / merge / rebalance, PD ecosystem, published numbers | Elasticity, global tables, zero ops | Scale | See §3 (future goals) and §4 (open gaps) |

Read the table as: DynamoDB is the API reference, TiKV is the architectural
peer, FoundationDB is the correctness bar, Bigtable is not a peer.

## 3. Scope today and future goals

README will present scope in two tiers instead of a non-goals section.

**Today (what a release must hold).**

- Three to five voters per Raft group, optional learners, single region.
  Membership operations: learner add / promote, fresh learner join for wiped
  nodes, fenced same-ID voter replacement.
- Protocol surfaces: gRPC RawKV / TransactionalKV (single-key operations;
  `PreWrite` / `Commit` / `Rollback` are not implemented, so multi-key
  transactions are reached through the protocol adapters below), Redis, DynamoDB
  (13 operations incl. `Scan`, `BatchWriteItem`, `TransactWriteItems`,
  `TransactGetItems`), S3 (path-style, SigV4 static credentials), SQS
  (opt-in, incl. HT-FIFO, DLQ redrive), FUSE filesystem
  (`2026_02_24_implemented_filesystem_on_elastickv.md`).
- Consistency: per-key linearizable on the paths whose timestamps the
  leader issues today (every Redis write except `EVAL`, DynamoDB, S3, SQS);
  `EVAL`, gRPC `RawKV` / `TransactionalKV`, the filesystem, and the
  asynchronous cleanups are excluded until A0b lands (G10, reproduced);
  multi-key transactions atomic, with
  OCC validation of write and read sets at apply. **Serializable is the
  target, not yet the claim**: the audit (§6.3) found and reproduced that validation assumes
  entries apply in commit-timestamp order, which nothing enforces (G0), and
  that 2PC read keys are unprotected between PREPARE and COMMIT (G1); both
  have failing tests on `main`. README
  makes the serializable claim only after the audit's A0 to A2 fixes land
  (§6.1). Leader reads via ReadIndex or leader lease; no follower reads.
- Durability and operations: at-rest encryption (storage and Raft envelopes,
  compress-then-encrypt, KEK from file, AWS KMS, GCP KMS, or Vault Transit),
  live point-in-time logical backup plus offline snapshot encode / decode /
  restore tooling, physical snapshot publish / restore tooling for an
  S3-compatible object store (the periodic scheduler is not yet wired into
  the server; that is the design's M3), Pebble SST ingest snapshot
  transfer, admin dashboard with data
  browser, key visualizer with automatic same-group split, Prometheus
  metrics, rolling update over Tailscale.

**Future goals (stated in README as such, not promised for any release).**

- TiKV-class horizontal scale: cross-group range migration (hotspot split
  M2, partial), range merge, automatic rebalancing, and the rest of
  `2026_06_23_proposed_scaling_roadmap.md`.
- Multi-region / geo-replication.

## 4. Open gaps that the next milestones close

| Gap | Evidence (on `main` at `4ca7e90d`) | Closed by |
|---|---|---|
| No authentication on the DynamoDB, Redis, and gRPC data planes; no TLS on any data-plane listener | `adapter/dynamodb*.go` has no SigV4 path (only the admin and migration files mention it); `adapter/redis_server_cmds.go` rejects `HELLO AUTH` ("elastickv's Redis adapter has no AUTH layer"); the only `--*TLSCertFile` flags are the admin listener's | DynamoDB, Redis, and TLS: security milestone (§6.2). gRPC `RawKV` / `TransactionalKV` authentication (mTLS or bearer) is deferred by §6.2 and **stays open** after it; until then the gRPC listener must sit inside a private network or tailnet |
| Serializability has known holes: commit timestamps are allocated before proposal and the apply path never rejects an entry below the watermark that reads use as their snapshot (G0); 2PC read keys are validated only at PREPARE apply, with locks on write keys only and no re-check at COMMIT (G1); 2PC read-only shards are validated outside the FSM lock; the S3 adapter populates `ReadKeys` only in the upload-part path and never re-reads multipart part descriptors at the commit snapshot; Lua scripts do not surface reads of keys they do not write; several Redis paths (standalone `DEL` and the other emptying paths, `SETNX`, `MULTI` type probes) bypass the collection fences; gRPC `TransactionalKV` has no read-set-bearing transaction API (audit Appendix A); in the default TSO mode `ShardedCoordinator` stamps commit timestamps on whichever node runs `Dispatch` (`EVAL`, gRPC `RawKV` / `TransactionalKV`, the filesystem, async cleanups, multi-group stamping) and the store's stale-reapply fast path treats an equal `commitTS` as "already applied", so two writers on two nodes can both read the same value, both be told OK, and one write is lost (audit G10, reproduced) | `resolveDispatchCommitTS` in `kv/coordinator.go`; `alignCommitTS` in `store/`; `verifyBackupTimestampFloor` in `kv/fsm_backup.go` (the backup-only fence); `handlePrepareRequest` / `handleCommitRequest` in `kv/fsm.go`; `grep ReadKeys adapter/s3*.go`; `luaWideFenceReadKeysForPlan` in `adapter/redis_lua_context.go` | Serializable isolation audit (§6.3) |
| `tla/occ/OCC.tla` does not validate `readObs` at commit and has no property forbidding write skew (OCC-2 covers write sets only) | `Prepare` / `Commit` actions in `tla/occ/OCC.tla` | Serializable isolation audit (§6.3) |
| No published performance numbers; six `*_benchmark_test.go` files; no `bench/` | `docs/redis_hotpath_dashboard.md` is directional only | Benchmark harness (§6.4) |
| README's "Implemented Features" omits SQS, encryption, backup, snapshot offload, and the filesystem, and its consistency bullet says only "write-after-read checks … are covered by tests" | `README.md` §Implemented Features | README refresh after §6.1 lands |
| Stale docs: `docs/docker_multinode_manual_run.md` listed `Scan` / `BatchWriteItem` as unsupported; `docs/review_todo.md` 4.2 described the engine as snapshot isolation | Fixed in the same change as this doc | — |

## 5. Consistency claims

What README and `docs/architecture_overview.md` will state, and the evidence
behind each claim.

1. **Per-key linearizability.** All writes go through the owning Raft group's
   leader; reads are leader reads (ReadIndex) or leader-lease reads. On
   `main` today this holds only where the leader issues the commit
   timestamp: the G10 paths (`EVAL`, gRPC `RawKV` / `TransactionalKV`, the
   filesystem, asynchronous cleanups) can lose a write with both clients told
   OK, so the unconditional claim waits for A0b. Evidence:
   `jepsen/src/elastickv/dynamodb_types_workload.clj` and `s3_workload.clj`
   (knossos linearizable register).
2. **Multi-key transactions are atomic.** Single-shard transactions apply in
   one Raft entry; multi-shard transactions use two-phase commit with a
   primary key and lock resolution (`kv/lock_resolver.go`).
3. **Multi-key transactions are serializable.** The FSM validates the
   transaction's write set and read set against every commit newer than
   `StartTS` under the store's apply lock (`checkConflictsLocked` in
   `store/mvcc_store.go`), so two transactions that read each other's writes
   cannot both commit: write skew is rejected as a write conflict. Coverage
   today: no path can be called serializable yet. The audit's A2 analysis
   found, and reproduced with a test, that the check assumes apply order
   equals commit-timestamp order, which is not enforced (G0), and that 2PC read keys are unprotected between
   PREPARE and COMMIT (G1); S3 handlers and Lua string reads do not surface
   their reads at all (G2, G3). The claim is made once A0 to A2 of §6.3
   land. Evidence
   today: Elle list-append under `:strict-serializable` for Redis MULTI/EXEC
   and DynamoDB `TransactGetItems` + `TransactWriteItems`. That workload
   cannot exhibit write skew (every anti-dependency comes with a write-write
   dependency on the same key), so the audit added rw-register workloads that
   can; they are red on `main` and green on the audit's fix branches under
   the same load, including repeated leader kills (audit A4); partitions
   and multi-group faults are not covered yet.
4. **Caveat.** Leader-lease reads rely on bounded clock drift; under an
   arbitrary partition a deposed leader may serve a lease read until its
   lease expires (`2026_04_20_implemented_lease_read.md`). README states
   this. "Strict serializable" is not claimed until the lease caveat is
   either removed or bounded by a documented clock assumption.

## 6. Milestones, in order

Each milestone that changes code gets its own design doc before
implementation, per `CLAUDE.md`.

### 6.1 This doc and the audit doc

`2026_09_26_proposed_positioning_and_roadmap.md` (this) and
`2026_09_26_proposed_serializable_isolation_audit.md`. README gets a short
"Who is this for" section and the two-tier scope once both docs land; the
consistency claims go into README only after the audit's A0 to A2 fixes are
merged.

### 6.2 Security milestone

Prerequisite for the on-premises showcase. Scope:

- DynamoDB adapter: SigV4 with static credentials, reusing the S3 / SQS
  credentials loader (`--dynamoCredentialsFile`).
- Redis adapter: `AUTH` with a static password (`requirepass` equivalent);
  `HELLO AUTH` accepted. ACLs are out of scope.
- TLS on every listener (gRPC, Redis, DynamoDB, S3, SQS), reusing the admin
  listener's certificate flags.
- Deferred: gRPC mTLS or bearer tokens; node-to-node Raft authentication
  (assumed to run inside a private network or tailnet).

### 6.3 Serializable isolation audit

See the audit doc. Summary: reproduce and close the apply-order gap with a
permanent commit-timestamp fence at apply (A0), audit every read-then-write
path per adapter for `ReadKeys` coverage (S3 and Lua first, A1), protect 2PC
read keys with read locks installed at PREPARE, which also replaces the
out-of-lock read-only-shard check (A2), model allocation and apply
separately in `tla/occ` and add a no-write-skew property (A3), add
write-skew Jepsen workloads for gRPC, Redis, DynamoDB, and Lua (A4), and
rewrite the consistency sections of README, `architecture_overview.md`, and
`review_todo.md` (A5). The A0 reproduction test is the first thing to run;
its result decides what README may say.

The two open TSO proposals (`2026_08_29_proposed_tso_batch_slot_claims.md`,
`2026_09_02_proposed_prephase_d_resolution_evidence.md`) protect the
timestamp-uniqueness assumption the OCC check relies on. They belong to the
same correctness track and are listed as active in §7; their order relative
to the audit is an open question (§9).

### 6.4 Benchmark harness and published numbers

- Harness under `bench/`, runbook in `docs/benchmarks.md`, summary in README.
- Workloads: DynamoDB (YCSB A/B/C-equivalent mixes over `PutItem` / `GetItem`
  / `UpdateItem`), Redis (`GET` / `SET` via memtier or redis-benchmark), and
  failover latency (leader kill during a run: p99 and time to recover).
- Topology: three identical Linux nodes; exact hosts and specs are fixed in
  the harness design doc (open question, §9).
- Absolute numbers for elastickv only. No side-by-side runs of other systems;
  the harness is public so others can compare on their own hardware.
- Runs after 6.3 so the numbers include the final validation path.

### 6.5 HLC ceiling and lease read write-up

External (blog or talk). Material: `kv/hlc.go`, `kv/lease_state.go`,
`tla/hlc/HLC.tla`, `2026_04_20_implemented_lease_read.md`,
`2026_04_16_implemented_centralized_tso.md`. The repo gets a pointer, nothing
more. Can proceed in parallel with 6.2 to 6.4.

### 6.6 On-premises showcase

- A three-node install guide (systemd units, extending
  `docs/deploy_via_tailscale_runbook.md` and `docs/docker_multinode_manual_run.md`).
- A sample application under `examples/` in Go with AWS SDK for Go v2, using
  DynamoDB + S3 + SQS with only the endpoint overridden.

### 6.7 Feature track (in-flight partial designs, continued after 6.2 to 6.6)

1. `2026_07_19_partial_physical_snapshot_object_offload.md`: M3 (the
   dominant work on `main` since July; continues at its current pace).
2. `2026_04_25_partial_s3_raft_blob_offload.md`: M3 reference counting,
   grace queue, orphan scanner, GC readiness; M4 legacy `BlobKey` migrator.
3. `2026_06_11_partial_hotspot_split_milestone2_migration.md`:
   `StartSplitMigration`, migrator state machine, fence, cutover, cleanup.
   Parent: `2026_02_18_partial_hotspot_shard_split.md`.

## 7. Status of existing design docs

| Track | Docs |
|---|---|
| Active, correctness | this doc; `2026_09_26_proposed_serializable_isolation_audit.md`; `2026_08_29_proposed_tso_batch_slot_claims.md`; `2026_09_02_proposed_prephase_d_resolution_evidence.md`; the security and benchmark docs to be written |
| Active, features | `2026_07_19_partial_physical_snapshot_object_offload.md` (M3); `2026_04_25_partial_s3_raft_blob_offload.md` (M3, M4); `2026_06_11_partial_hotspot_split_milestone2_migration.md` with parent `2026_02_18_partial_hotspot_shard_split.md` |
| Future track (horizontal scale) | `2026_06_23_proposed_scaling_roadmap.md` (its 06/12 predecessor is already marked superseded). Referenced from README's future goals; re-evaluated when the active list is empty. |
| Deferred | Remaining Stage 9 of `2026_04_29_partial_data_at_rest_encryption.md` (9C+: rotation budget, rewrap / retire / rewrite, `last_proposed_index_per_raft_dek`, encrypted Jepsen). Stage 5E (the capability fan-out helper) is shipped and wired (`buildEncryptionCapabilityFanout` in `main.go`) although that doc's header still says deferred; the header is corrected in the A5 documentation pass. |
| Closed since the interview's inputs were gathered | `logical_backup`, `raft_learner`, `centralized_tso`, `hotspot_split_milestone3_automation`, `ttl_inline_value`, `fsm_apply_observer`, `filesystem_on_elastickv`, `deploy_via_tailscale`, `9a_encryption_compression`, `9b_kek_providers` are all `implemented` on `main`; the interview's active / deferred choices that named them are moot and are not carried into the table above. |

## 8. Decisions

Recorded from the 2026-09-26 design interview, then reconciled with `main`.

- Success is research and technical proof first, adoption second. Capacity:
  one maintainer plus agents, a few hours a week, no deadline.
- Main arena: self-hosted AWS-compatible store plus correctness evidence. First
  showcase: on-premises / edge / air-gapped deployment, delivered as an install
  guide plus a sample app.
- Consistency claim (targets, not yet made for every path): per-key
  linearizable; multi-key transactions atomic and serializable. Write skew must not occur. The Jepsen strict-serializable
  checkers are evidence for the claim, and write-skew workloads are added per
  adapter.
- README uses a two-tier scope (today / future goals); there is no non-goals
  section. Horizontal scale of TiKV class and multi-region are future goals.
- Hotspot split M2 stays active (M3 is already implemented). The scaling
  roadmap doc is the future track, not abandoned.
- Performance: absolute numbers on three nodes for DynamoDB, Redis, and
  failover latency; elastickv only; harness published.
- Security milestone scope: DynamoDB SigV4 static credentials, Redis AUTH,
  TLS on all listeners. gRPC mTLS deferred.
- Order: security → serializable audit implementation → benchmarks → HLC
  write-up (parallel) → showcase → feature track.
- Spec of record is `docs/design/`; no ADRs; decisions live in design docs
  (this section is the pattern); repository artifacts are English.
- The audit's A2 analysis found gaps G0 and G1 on `main`, and both are
  reproduced by failing tests; the serializable claim is the target and is
  not made for any path until A0 to A2 land.

Assumptions made while reconciling with `main` (not put to the interview):

- The two TSO admission proposals are treated as active correctness work
  alongside the audit rather than deferred.
- Physical snapshot object offload M3 keeps its current momentum and is
  listed first in the feature track.
- Encryption 9C+ stays deferred even though 9A / 9B (compression, KMS
  providers) shipped; the interview's "defer Stage 9" answer is applied to
  what remains.

## 9. Open questions

1. Benchmark hosts: which three machines, and their CPU / memory / disk / NIC.
   Fixed in the benchmark harness design doc.
2. Order of the two TSO admission proposals relative to the serializable
   audit (before, after, or interleaved).
3. Sample app scope: which DynamoDB / S3 / SQS operations it exercises, and
   whether Redis and the FUSE filesystem are included.
4. What "multi-region" means for a future milestone: async replica, or a
   second Raft group set with cross-region routes. Not needed until the
   future track opens.
5. Whether to run the audit's A0 reproduction test before the security
   milestone: it is about a day of work and decides the claim, so the
   recommendation is yes, without changing the rest of the order.
