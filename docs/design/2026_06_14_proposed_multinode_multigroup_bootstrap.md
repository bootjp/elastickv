# Multi-node multi-group bootstrap — standing up N nodes × M Raft groups at startup

Status: Proposed
Author: bootjp
Date: 2026-06-12
Date source: first add commit `252df5601700821c7bee7b642c9f0d758103f85f` (`git log --follow --diff-filter=A`), authored on 2026-06-12.

Sibling / prerequisite-for:
- [Leader-balance scheduler design PR #953](https://github.com/bootjp/elastickv/pull/953) §1.1a (PR0) + OQ-9 — this doc **is** that PR0. The leader-balance scheduler's transfer-issuing milestones (PR2/PR3) are blocked on a Raft group whose voter set spans more than one node; that topology cannot be declared at startup today, and OQ-9 resolved "option (a): extend the bootstrap/flag surface." This document is the design for option (a).
- [2026_02_18_partial_hotspot_shard_split.md](2026_02_18_partial_hotspot_shard_split.md) Milestone 2 — cross-group range migration testing also needs ≥2 nodes hosting the same group to be meaningful (see §7).

## 1. Background

### 1.1 The gap: a real multi-node deployment can only run single-group today

elastickv runs multiple Raft groups in one process (`--raftGroups id=addr,id=addr,…`, parsed by `parseRaftGroups`, `shard_config.go:61-99`; default group is the lowest ID, `defaultGroupID`, `shard_config.go:386-397`). Each group gets its own `raftGroupRuntime` with its own engine and its own gRPC listener at `rt.spec.address` (`startRaftServers`, `main.go:1610-1620`). This is a genuine multi-Raft-group runtime — **within one process**. What does **not** exist is the ability to spread any single group's voters across more than one node. Verified at file:line on `main`:

- **`groupSpec` carries one address per group — this node's own listener, not a member list.** `type groupSpec struct { id uint64; address string }` (`shard_config.go:14-17`); `parseRaftGroups` parses each `id=addr` entry into exactly that one address (`shard_config.go:80-93`). There is no field on `groupSpec` for the *other* nodes that should vote in the group.
- **`resolveBootstrapServers` rejects `--raftBootstrapMembers` whenever `len(groups) != 1`.** `if len(groups) != 1 { return nil, errors.WithStack(ErrBootstrapMembersRequireSingleGroup) }` (`main.go:746-748`, error defined `main.go:736`). So a multi-node initial membership can be declared only for a **single-group** deployment. `--raftBootstrapMembers` itself parses `id=host:port,…` into a flat `[]raftengine.Server` voter list (`parseRaftBootstrapMembers`, `shard_config.go:352-384`) — one list, applied to the one group.
- **`buildRuntimeForGroup` passes the *same* `bootstrapServers` to *every* group.** It threads the single process-wide `bootstrapServers` slice into `factory.Create(...)` via `Peers:` with `LocalAddress: group.address` (`multiraft_runtime.go:234-254`). In a multi-group config `bootstrapServers` is `nil` (it can only be non-nil for `len(groups)==1`, per the guard above), so each group bootstraps with `Peers: nil`.
- **A `nil`/single peer list bootstraps a single-member group — and never even builds a transport.** The etcd factory only constructs the inter-node gRPC transport `if len(peers) > 1` (`internal/raftengine/etcd/factory.go:49-52`). With `peers == nil` each group is a one-voter cluster with no transport, so `TransferLeadershipToServer` has no other voter to move leadership to, and no peer to replicate to.
- **The integration tooling documents exactly this limitation.** `scripts/run-jepsen-m5-local.sh:5-22` records in prose that today's `validateShardRanges` / `buildShardGroups` "only support a 'single process hosts all groups' model — separate processes per group fail validation or race on Raft listeners," that it launches "ONE process hosting BOTH **single-member** groups," and that "True distributed multi-group is M6+ work."

**Consequence:** the only deployable multi-group topology is one process hosting single-voter groups (the M5 Jepsen layout, and the `cmd/server/demo.go` single-group×3-node demo is the only multi-*node* topology — but it is single-*group*). There is no startup wiring that produces "N nodes, each a voter in all M groups."

### 1.2 What already works (the primitives we compose)

The single-group multi-node path is fully built and is the template this design generalizes:

- **`--raftBootstrapMembers id=addr,…` → a voter `[]raftengine.Server`** (`parseRaftBootstrapMembers`, `shard_config.go:352-384`), validated against the local node (must include `--raftId`, local address must match the group address: `resolveBootstrapServers`, `main.go:752-768`).
- **The factory builds a transport when `len(peers) > 1`** and wires it into `Open(...)` (`internal/raftengine/etcd/factory.go:41-90`). `Open` normalizes/validates peers, and on first open writes them to a persisted-peers file (`normalizePeers` / `validateOpenPeers` / `savePersistedPeers`, `internal/raftengine/etcd/engine.go:620-643`; `LoadPersistedPeers`, `internal/raftengine/etcd/peer_metadata.go:40`). On restart, current code reloads the persisted list before opening (`factory.go:43-47`; `normalizeOpenConfig`, `engine.go:3298-3306`) and `validateOpenPeers` protects the persisted snapshot's ConfState against that loaded list (`engine.go:3313-3327`; `errClusterMismatch`, `:116`). It does **not** yet compare a newly supplied flag list against the persisted file after reload; PR-B must add that explicit configured-list-vs-persisted-list validation for `--raftGroupPeers`.
- **The transport resolves peers by node ID → address from the bootstrap list** (`NewGRPCTransport(peers)` builds `map[nodeID]Peer`, `internal/raftengine/etcd/grpc_transport.go:67-86`; sends dial `peer.Address`, `:493-517`), and supports runtime membership churn via `UpsertPeer` / `RemovePeer` (`:145-170`) as conf-changes commit.
- **Each group already gets its own listener and its own `RaftAdmin` service** (`startRaftServers` registers `RegisterOperationalServicesWithInterceptor(ctx, gs, rt.engine, …)` then `lc.Listen(ctx, "tcp", rt.spec.address)` per runtime, `main.go:1610-1615`). `AddVoter`/`AddLearner`/`PromoteLearner`/`RemoveServer` are reachable per group (`cmd/raftadmin/main.go:197-285`; engine `AddVoter`, `internal/raftengine/etcd/engine.go:1252-1257`).
- **`cmd/server/demo.go` already stands up 3 nodes that bootstrap one shared group.** All three node configs set `raftBootstrap=true` and receive the **same** `raftPeers` list (all three `{Suffrage:"voter", ID, Address}`), `cmd/server/demo.go:180-219`. The comment at `:215-219` records the key etcd requirement: *"every member of a fresh cluster must bootstrap with the same peer list."* This is exactly the per-group bootstrap discipline §3 generalizes to M groups.
- **Per-group data dir + `raft-engine` marker is already per-group.** `groupDataDir(baseDir, raftID, groupID, multi)` returns `…/raftID/group-N` in multi mode (`multiraft_runtime.go:110-115`); `ensureRaftEngineDataDir` writes/reads the `raft-engine` marker and refuses an engine mismatch *per dir* (`multiraft_runtime.go:117-151`). So idempotent-restart detection is already per group.

The only missing piece is a **flag/parse/wiring path that gives each group its own multi-node voter set at bootstrap** instead of a single shared list rejected for multi-group.

## 2. Goals and Non-Goals

### 2.1 Goals

1. Deploy **N nodes × M Raft groups** where **every group is a multi-voter Raft cluster** (each group's voter set spans ≥2 nodes), declarable entirely at process startup.
2. A concrete, validated **flag surface** for per-group peer lists, with strict back-compat: every existing single-group flag (`--raftBootstrapMembers`, `--raftBootstrap`, `--raftGroups`) and the `cmd/server/demo.go` single-process demo behave **exactly as today**.
3. **Deterministic, idempotent bootstrap**: every founding node starts each group with the same initial configuration; restart re-detects existing state and does not re-bootstrap; partial-bootstrap failures are recoverable.
4. **Reuse the existing per-group transport, listener, `RaftAdmin`, marker-dir, and persisted-peers machinery** (§1.2) — no new replication or wire surface for the data path.
5. An **in-process integration harness** that stands up 3 nodes × 2 groups with every group multi-voter, so the leader-balance convergence test and hotspot-M2 cross-group tests have a topology to run against.

### 2.2 Non-Goals

1. **Dynamic group creation / deletion at runtime.** The set of groups (M) is fixed at startup. Creating a new group while the cluster runs is out of scope (it belongs to a future control-plane RPC).
2. **Replica / leader rebalancing.** Moving where a group's voters live, or spreading leaderships, is **not** this doc — that is leader-balance (#953) and hotspot-split M2 (#945). This doc only stands up the static topology those features need.
3. **Live topology expansion as the bootstrap mechanism.** Growing a group from one voter to many via `AddVoter`/`PromoteLearner` after bootstrap stays the supported **live-expansion** path (§5), but it is explicitly **not** the way this design declares the initial topology (per #953 OQ-9).
4. **Heterogeneous group membership** (groups whose voter sets are different subsets of nodes). v1 targets **homogeneous** membership — every node is a voter in every group — matching the leader-balance scheduler's stated assumption (#953 §2.2 non-goal 5). Heterogeneous sets are a forward extension (§8 OQ-4); the flag syntax (§3.1) is chosen so it does not foreclose them.
5. **Per-protocol address-map changes.** `--raftRedisMap` / `--raftDynamoMap` / `--raftS3Map` / `--raftSqsMap` map *Raft listener address → protocol listener address* and are orthogonal to voter-set membership; they are unchanged (§4.3).

## 3. Design

### 3.1 Flag surface — per-group peer lists

**Decision: add a companion flag `--raftGroupPeers` with its own per-group resolver, while keeping `resolveBootstrapServers` and its `len(groups)==1` guard for `--raftBootstrapMembers`.** Keep `--raftGroups` (group→local-address) exactly as is; declare the *cross-node* voter set per group in a new flag.

```
--raftGroupPeers "1=n1@host1:5051,n2@host2:5051,n3@host3:5051;2=n1@host1:5054,n2@host2:5054,n3@host3:5054"
```

Grammar:
- Group entries separated by `;` (matching the `--sqsFifoPartitionMap` precedent, which already uses `;` between queues and reserves `,` for the per-entry list, `parseSQSFifoPartitionMap`, `shard_config.go:174-196`).
- Each entry is `groupID=member,member,…`.
- Each `member` is `raftID@host:port` — the `@` separates the node's stable Raft ID (matching `--raftId` semantics) from its listener address for that group. (`raftID` is needed explicitly because etcd's bootstrap requires the same `id→address` mapping on every node, `cmd/server/demo.go:215-219`; the address alone is not the identity.)

Concrete 3-node × 2-group local example (all nodes share the same `--raftGroupPeers` and full N×M `--raftRedisMap`; `--raftGroups` names only that node's local raft listeners):

```
RAFT_GROUP_PEERS="1=n1@127.0.0.1:5051,n2@127.0.0.1:5052,n3@127.0.0.1:5053;2=n1@127.0.0.1:5054,n2@127.0.0.1:5055,n3@127.0.0.1:5056"
RAFT_REDIS_MAP="127.0.0.1:5051=127.0.0.1:6379,127.0.0.1:5054=127.0.0.1:6379,127.0.0.1:5052=127.0.0.1:6380,127.0.0.1:5055=127.0.0.1:6380,127.0.0.1:5053=127.0.0.1:6381,127.0.0.1:5056=127.0.0.1:6381"

# node n1
--raftId n1 \
--redisAddress "127.0.0.1:6379" \
--raftGroups "1=127.0.0.1:5051,2=127.0.0.1:5054" \
--raftGroupPeers "$RAFT_GROUP_PEERS" \
--raftRedisMap "$RAFT_REDIS_MAP"

# node n2
--raftId n2 \
--redisAddress "127.0.0.1:6380" \
--raftGroups "1=127.0.0.1:5052,2=127.0.0.1:5055" \
--raftGroupPeers "$RAFT_GROUP_PEERS" \
--raftRedisMap "$RAFT_REDIS_MAP"

# node n3
--raftId n3 \
--redisAddress "127.0.0.1:6381" \
--raftGroups "1=127.0.0.1:5053,2=127.0.0.1:5056" \
--raftGroupPeers "$RAFT_GROUP_PEERS" \
--raftRedisMap "$RAFT_REDIS_MAP"
```

**Why a new flag rather than extending `--raftGroups` entry syntax.** `--raftGroups` entries are `id=addr` and that `addr` is *this node's own* listener (`groupSpec.address`, used as `LocalAddress`, `multiraft_runtime.go:248`). Overloading it to also carry the full member list would make every node's `--raftGroups` value identical across the cluster *except* that the local-address role would have to be inferred — error-prone. A separate `--raftGroupPeers` keeps "what do I listen on" (`--raftGroups`) cleanly separate from "who are the voters" (`--raftGroupPeers`), and mirrors how single-group already separates `--address`/`--raftGroups` from `--raftBootstrapMembers`.

**Back-compat rules (strict):**
- `--raftGroupPeers` empty ⇒ behavior is **byte-for-byte today's**: `resolveBootstrapServers` runs unchanged (single-group `--raftBootstrapMembers` still works; multi-group still bootstraps single-member groups). No existing deployment or test changes.
- `--raftBootstrapMembers` and `--raftGroupPeers` are **mutually exclusive** — setting both is a validation error (`--raftBootstrapMembers` is the single-group spelling; `--raftGroupPeers` is the multi-group spelling). Single-group deployments may continue to use `--raftBootstrapMembers` and never need to learn the new flag.
- `cmd/server/demo.go` is unchanged: it bootstraps one group with a shared peer list via `raftPeers` directly (`cmd/server/demo.go:180-219`), not via these flags.

**Validation rules** (fail fast at startup, before any engine opens — same posture as `parseRaftGroups`/`validateShardRanges`):
1. Every group ID in `--raftGroupPeers` must appear in `--raftGroups`, and (v1 homogeneous goal) **every** group in `--raftGroups` must appear in `--raftGroupPeers` when the flag is non-empty. A group with no peer list would silently fall back to single-member — a foot-gun we reject.
2. Each group's member list must **include the local node**: a `member` whose `raftID == --raftId` must be present, and its `host:port` must equal that group's `--raftGroups` local address (`groupSpec.address`). This is the per-group generalization of the existing single-group check `ErrBootstrapMembersLocalAddrMismatch` (`main.go:760-765`).
3. No duplicate `raftID` within a group (mirrors `parseRaftBootstrapMembers`'s `duplicate id` check, `shard_config.go:373-375`).
4. v1 homogeneity check: the set of `raftID`s must be **identical across all groups** (every node votes in every group). Violations are rejected with a clear error pointing at the first divergent group. (Relaxing this is OQ-4.)
5. Each member's address must be non-empty and well-formed `host:port` (reuse existing address parsing).

### 3.2 Bootstrap semantics

The wiring change is small and local: instead of one process-wide `bootstrapServers` threaded into every group, **resolve a per-group `[]raftengine.Server` and pass each group its own list**. Concretely, `buildShardGroups` / `buildRuntimeForGroup` change from a single `bootstrapServers []raftengine.Server` parameter (`multiraft_runtime.go:234`, `main.go:777`) to a static `map[uint64][]raftengine.Server`, built once from the parsed `--raftGroupPeers`. Everything downstream — the factory's `len(peers) > 1` transport gate (`factory.go:50`), `Open`'s peer normalize/validate/persist (`engine.go:620-643`), the marker dir, the per-group listener — already operates per group and needs no change.

**Initial configuration model (decision: every node bootstraps with the identical per-group peer list — the etcd model — NOT a single designated proposer).** etcd/raft's bootstrap model is that **every** founding member calls `Bootstrap` with the **same** `ConfState`/peer list; raft then elects a leader among them. This is exactly what `cmd/server/demo.go` does for the single group (`raftBootstrap=true` on all three nodes with the shared `raftPeers`, `:204-219`) and what `resolveBootstrapServers` sets up for single-group (`bootstrap = *raftBootstrap || len(bootstrapServers) > 0`, `main.go:534`). We generalize it: when `--raftGroupPeers` is set, **every group on every node bootstraps with that group's full peer list**, and `bootstrap` is implied true for those groups (the operator does not also need `--raftBootstrap`; see the interaction rule below).

We do **not** invent a "lexicographically-smallest peer proposes, others wait-and-join" protocol. That single-proposer pattern is the *AddVoter-composition* path (§5), not the bootstrap path — and adopting it for bootstrap would mean the non-proposer nodes start with an empty conf and must be added one-by-one, which is fragile (ordering, the proposer must be up first and must be leader) and is exactly the "manual AddVoter dance in every test harness" #953 OQ-9 rejected. The all-nodes-same-list model has no designated-proposer ordering requirement: nodes can start in any order, and raft elects a leader once a quorum is up.

**Idempotency on restart (decision: persisted-peers + marker dir are the restart boundary; PR-B validates the bootstrap seed only while membership is still bootstrap-era).** On first open of a group dir, `Open` writes the normalized peer set to the persisted-peers file (`savePersistedPeers`, `engine.go:643`; format in `peer_metadata.go:205`). On restart, the factory/Open path **loads the persisted peers and uses them in preference to the flag-supplied list** (`factory.go:43-47`; `normalizeOpenConfig`, `engine.go:3298-3306`), and `validateOpenPeers` verifies the persisted snapshot's ConfState against that loaded peer set (`engine.go:632`; `validateOpenPeers`, `engine.go:3313-3327`; `errClusterMismatch`, `:116`). That is enough for same-list idempotency, but not enough to reject an operator who changes the initial `--raftGroupPeers` before any membership change: current code discards the newly supplied list before validation. PR-B therefore adds an explicit comparison of the normalized per-group flag list against the persisted peers **only when the persisted peer metadata still represents the initial bootstrap configuration**; divergence returns `errClusterMismatch` (or a wrapped validation error with that sentinel). Once a live membership change has committed, the persisted peer metadata is advanced by conf-change apply (`applyConfChangeCommitted`, `engine.go:2280-2282`; `writeCurrentPersistedPeers`, `engine.go:2688`) and becomes authoritative; `--raftGroupPeers` remains a bootstrap seed, not a desired-membership flag. So:
- A restart with the same `--raftGroupPeers` re-loads the same persisted set per group → no re-bootstrap, no data risk.
- A restart with a *different* `--raftGroupPeers` than the bootstrap-era persisted set **fails fast** in PR-B's explicit bootstrap-seed validation rather than silently ignoring the changed flag or re-bootstrapping over committed data.
- A restart after `AddVoter`/`RemoveServer` has changed membership uses the committed persisted peer set even if the original bootstrap flag is still present; operators change membership through RaftAdmin, not by editing `--raftGroupPeers`.
- The `raft-engine` marker (`ensureRaftEngineDataDir`, `multiraft_runtime.go:117-151`) independently guards against opening a group dir under the wrong engine type — unchanged, already per group.

**`bootstrap` flag interaction (decision: `--raftGroupPeers` implies bootstrap=true for configured groups; `--raftBootstrap` stays for the single-group/demo path).** Mirror the existing single-group rule `bootstrap = *raftBootstrap || len(bootstrapServers) > 0` (`main.go:534`): when `--raftGroupPeers` is non-empty, the resolved bootstrap flag is true for every group that has a non-empty peer list. `--raftBootstrap` continues to mean "bootstrap" for deployments that don't use `--raftGroupPeers`. Setting `--raftBootstrap=false` together with `--raftGroupPeers` is a no-op contradiction for a fresh dir — we treat a non-empty `--raftGroupPeers` as authoritative for those groups (bootstrap=true), and document it. On a restart, the data-loss guard is the WAL path: `openDiskState` checks `wal.Exist(walDir)` and returns `loadWalState` before consulting `cfg.Bootstrap` (`wal_store.go:50-52`); only dirs without a WAL reach `bootstrapNewCluster` (`wal_store.go:62`, `:93-100`). PR-B's five-lens review must preserve that invariant.

**Partial-bootstrap failure modes and recovery:**
- *One node never comes up.* With an N-voter group, raft tolerates up to ⌊(N−1)/2⌋ down at bootstrap and still elects a leader once a quorum starts. A 3-voter group forms with 2 up. The down node joins when it starts (its dir is fresh → bootstraps with the same list → catches up via snapshot/log). No operator action.
- *A node bootstrapped with the wrong list.* On restart while membership is still bootstrap-era, PR-B's bootstrap-seed validation catches the mismatch before opening the engine. On a first bootstrap with fresh dirs, there is no local persisted reference to compare against; nodes with mismatched lists may form incompatible Raft configurations that do not share a quorum and therefore fail to elect a usable leader or make progress. Recovery: stop the misconfigured node(s), wipe only the fresh group dirs that bootstrapped with the wrong list, and restart with the identical `--raftGroupPeers` value used by the rest of the founding members. If any group has already committed user data, recovery must follow the live membership/change path instead of wiping.
- *A node crashes mid-bootstrap after writing the persisted file but before committing entries.* Restart re-loads the persisted peers (`factory.go:43-47`) and rejoins; the persisted file is written atomically (`writePersistedPeersFile`, `peer_metadata.go:205`), so a torn write is not a partial state. No special handling beyond what single-group already has.

### 3.3 Determinism and testability of the bootstrapper

There is no "elect a bootstrapper" step to test, because the model is all-nodes-same-list (§3.2). What is unit-testable and must be deterministic is **flag parsing → per-group `[]raftengine.Server`**: given a `--raftGroupPeers` string + `--raftGroups` + `--raftId`, the resolver produces a fixed `map[uint64][]raftengine.Server` (sorted by member raftID for reproducibility) or a precise validation error. This is a pure function (like `parseRaftGroups` / `parseRaftBootstrapMembers`) and gets table-driven tests (§6). The leader that emerges is raft's business, not ours.

### 3.4 Per-group transport / addressing

**Today (verified):** the gRPC raft transport resolves a peer by deriving its 64-bit node ID and looking up `host:port` in the bootstrap-seeded `map[nodeID]Peer` (`NewGRPCTransport`, `grpc_transport.go:67-86`; `peerFor`/dial, `:493-517`). Membership changes update that map via `UpsertPeer`/`RemovePeer` (`:145-170`). Each group has its **own** transport instance, created by the factory only when `len(peers) > 1` (`factory.go:49-52`), and registered on that group's own listener in `startRaftServers` (`main.go:1610-1615`).

**What changes:** nothing in the transport itself. Once each group receives its own multi-node peer list (§3.2), the factory's `len(peers) > 1` check trips per group, a transport is built per group, and it resolves that group's peers from that group's list. The change is entirely upstream (feeding per-group lists in); the transport is already per-group and address-map driven.

**One listener per group vs. per-group ports (decision: keep one listener per group — the existing `rt.spec.address` model — i.e. one port per group per node).** elastickv already binds **one gRPC listener per group per node** at `rt.spec.address` (`main.go:1613`), multiplexing the data-plane gRPC services, the per-group `RaftAdmin`, *and* that group's raft transport onto it (`rt.registerGRPC(gs)` + `RegisterOperationalServices…` + the transport's `Register`, `main.go:1605-1613`). This matches the `5005{1,2,3}` (group 1) / `5005{4,5,6}` (group 2) port convention already used by the M5 script and the demo. We keep it:
- It is the established convention and needs zero transport/listener changes.
- A single shared listener multiplexing *all* groups' raft traffic was considered and rejected: it would require demultiplexing by group ID inside the transport (the `EtcdRaft` service is currently per-engine, registered once per listener, `grpc_transport.go:88-…`), a larger change with no operational benefit at the scales this targets.
- Per-group ports keep each group's raft transport, `RaftAdmin`, and metrics cleanly attributable per group — useful for the leader-balance forward path (#953 §3.4 dials `rt.spec.address` of the source group's leader) and for partition nemeses that want to isolate one group.

So the addressing model is: **N nodes × M groups ⇒ N×M (raftID, host:port) listener endpoints**, exactly the cross product `--raftGroupPeers` declares. Each node opens M listeners (one per group), each member of a group dials the other members' per-group endpoints.

## 4. Unchanged surfaces (explicitly)

### 4.1 Single-group path
With `--raftGroupPeers` empty, `resolveBootstrapServers` runs unchanged (`main.go:742-768`). `--raftBootstrapMembers` still works for single-group, including its three local-node validation errors (`main.go:752-768`).

### 4.2 The in-process demo
`cmd/server/demo.go` bootstraps one group across 3 nodes via `raftPeers` (`:204-219`); it never reads `--raftGroupPeers`. Unchanged.

### 4.3 Per-protocol address maps
`--raftRedisMap` / `--raftDynamoMap` / `--raftS3Map` / `--raftSqsMap` map *Raft listener address → protocol listener address* (`parseRaftAddressMap`, `shard_config.go:327-350`; consumed in `multiraft_runtime.go` group→protocol wiring). They are about *where a group exposes its protocol endpoint*, not *who votes in the group*, so they are orthogonal and unchanged. In a true multi-node deployment, every node that accepts follower ingress must include entries for every possible leader raft address, not just its local listeners: Redis `leaderClientForKey` indexes the map by `RaftLeaderForKey` (`adapter/redis.go:4282-4288`), and the HTTP leader proxy indexes by `RaftLeader()` (`adapter/leader_http_proxy.go:47-54`). When one process hosts multiple groups, those groups' raft addresses can map to the same per-process protocol listener, such as the single Redis listener from `--redisAddress` (`main.go:85`, `main.go:1647-1655`). A local-only map works only if clients always connect directly to the current leader.

### 4.4 Encryption startup ordering
The encryption writer-registration startup path (`main_encryption_registration.go`) is **leader-relative, not single-node-per-group**, so it already tolerates multi-voter groups. `buildProcessStartRegistrationGate` proposes through the **default group** and, when this node is not the default-group leader, **forwards to the current leader** over `EncryptionAdmin` with bounded retry (`proposeWriterRegistration`, `:472-520`; `IsLeader()`/`RaftLeader()` gating, `:482-511`). It assumes only that a default-group leader exists and is reachable — which is *more* true with a multi-voter default group, not less. The `raft-engine` marker and per-group dirs are already per group (§1.2). No encryption guard assumes a single-node-per-group bootstrap order; nothing here changes. (The five-lens "data consistency" review per PR must still confirm the registration forward path behaves when the default group is mid-election at boot, but that is an existing property, not new.)

## 5. Alternative considered — AddVoter-composition

**Bootstrap each group single-member, then grow it to N voters at runtime via `AddVoter`/`PromoteLearner`.** The primitives exist and are exercised: engine `AddVoter` (`internal/raftengine/etcd/engine.go:1252-1257`), `AddLearner`/`PromoteLearner` (`:1640-1689`), the per-group `RaftAdmin` service (`cmd/raftadmin/main.go:258-285`), and conf-change apply that calls `UpsertPeer` (`applyConfigChange`, `engine.go:2456`).

**Rejected as the *bootstrap* mechanism** (consistent with #953 OQ-9 "option (b) stays for live expansion, not bootstrap"):
- It needs a designated first node that is up and leader before any `AddVoter` lands, plus an orchestration sequence (add each voter, wait for it to catch up, repeat) — fragile under etcd's randomized elections, exactly the failure `cmd/server/demo.go:204-219` calls out for the old `joinCluster` approach it deleted.
- Every test harness and every operator runbook would have to replay that dance to get a multi-voter group, the per-test cost OQ-9 explicitly wanted to avoid.
- It produces a *transient* single-voter window at startup where the group has no fault tolerance and no other transfer target — the opposite of what the leader-balance scheduler needs to test against.

**Kept as the live-expansion path.** Growing an *already-running* group (add a 4th node to a 3-voter group, replace a dead node) is precisely what `AddVoter`/`RemoveServer` are for, and this design does not touch them. The persisted-peers file is rewritten by conf-change apply, so a node added via `AddVoter` and then restarted reloads the grown set (`factory.go:43-47`) — bootstrap and live-expansion compose cleanly. After the first committed membership change, `--raftGroupPeers` is no longer compared as desired membership; the persisted Raft configuration is authoritative until another RaftAdmin membership change commits.

## 6. Rollout / testing

### 6.1 Unit
- **Flag parsing** (`shard_config.go`, table-driven, co-located `*_test.go`): `--raftGroupPeers` grammar — multiple groups (`;`-separated), `raftID@host:port` members, whitespace, empty ⇒ nil; every validation rule of §3.1 (unknown group, missing-group-when-non-empty, local-node-absent, local-addr-mismatch, duplicate raftID, homogeneity violation, mutual-exclusion with `--raftBootstrapMembers`). Pure-function determinism: same input ⇒ identical sorted `map[uint64][]Server`.
- **Per-group bootstrap-server resolution**: the new `map[uint64][]raftengine.Server` carries each group's own list; the empty-flag path returns today's behavior unchanged (regression-locks back-compat).
- **Restart idempotency** (engine-level, `internal/raftengine/etcd/`): re-open a group dir with the same list ⇒ no re-bootstrap; re-open with a divergent flag-supplied list while membership is still bootstrap-era ⇒ PR-B's explicit bootstrap-seed validation returns `errClusterMismatch`; re-open after an `AddVoter`/`RemoveServer` conf-change with the original bootstrap flag still present ⇒ accepts the committed persisted peer set (add the multi-group-dir cases).

### 6.2 Integration — 3-node × 2-group in-process harness
Stand up **3 nodes, 2 groups, every group a 3-voter Raft**, in one test process (extend `cmd/server/demo.go`'s pattern, or a new `internal/`-level harness so it is `go test`-runnable without the binary). Assertions:
- Each group's `Configuration()` reports 3 **voter** members on 3 distinct node IDs (the smoke #953 PR0 calls for: "a group has voters on ≥2 distinct nodes").
- `TransferLeadershipToServer` between two nodes of the same group **succeeds** (the capability the leader-balance scheduler is blocked on).
- Restart one node: it reloads persisted peers and rejoins both groups without re-bootstrap.
- Kill a minority (1 of 3) in a group: the group keeps a leader and serves; the killed node rejoins on restart.
- Negative case: start one fresh node with a divergent peer list and assert the harness fails health/leader convergence rather than producing an apparently healthy split configuration; document recovery as wipe-fresh-dir + restart with the shared list.

This harness is the concrete deliverable that unblocks #953's convergence test (which "requires a topology where each of N groups has voters on ≥2 of 3 nodes — which only exists after PR0", #953 §5).

### 6.3 Jepsen
Extend the multi-node story to Jepsen as a **later milestone** (noted, not v1): generalize `scripts/run-jepsen-local.sh` / `run-jepsen-m5-local.sh` from "one process hosting single-member groups" (`run-jepsen-m5-local.sh:5-22`) to **separate processes per node, each hosting all M groups as multi-voter Raft**, so partition/kill nemeses can isolate one node from a group's quorum (impossible under the single-process layout, `run-jepsen-m5-local.sh:16-18`). Acceptance bar: existing Redis/DynamoDB workloads show no new anomalies on the true multi-node multi-group topology. This is the M-script work the existing comment defers to "M6+".

### 6.4 Milestone / PR breakdown

| PR | Scope | Tests | Shippable alone? |
|---|---|---|---|
| **PR-A** | Flag + parse + validation: `--raftGroupPeers`, the §3.1 grammar and all validation rules, mutual-exclusion with `--raftBootstrapMembers`. No wiring change yet (parsed result unused). | Unit (§6.1) flag-parse table tests. | Yes — pure parsing, zero behavior change (result unconsumed). |
| **PR-B** | Wiring: add the `--raftGroupPeers` resolver alongside the existing `resolveBootstrapServers` path, preserving the `--raftBootstrapMembers` single-group guard; thread the static per-group peer map through `buildShardGroups`/`buildRuntimeForGroup`; replace the process-wide `bootstrap bool` decision with per-group bootstrap derived from whether that group has a resolved peer list; validate the flag-supplied bootstrap seed against persisted bootstrap-era peers before adopting persisted state, but preserve live-expanded persisted peers after conf changes. Each group now opens multi-voter. | Unit (§6.1) per-group resolution + restart idempotency; smoke that a 2-group config opens 2 transports. | After PR-A — the core capability. |
| **PR-C** | In-process 3-node × 2-group integration harness (§6.2) + the leader-transfer-between-nodes smoke. | Integration (§6.2). | After PR-B — the deliverable #953 PR0 / hotspot-M2 need. |
| **PR-D (later)** | Jepsen: true multi-node multi-group runner (§6.3). | Existing workloads, no-new-anomalies bar. | After PR-C; the "M6+" item. |

Each PR carries the five-lens self-review (CLAUDE.md). Lens highlights for this change: **data loss** — restart must never re-bootstrap over committed data (existing-WAL path bypasses bootstrap, and PR-B validates the bootstrap seed before adopting bootstrap-era persisted peers, §3.2); **concurrency/distributed** — any node-start order must form each group (all-same-list model, §3.2), partial-quorum bootstrap recovers; **data consistency** — a divergent `--raftGroupPeers` before live membership changes fails fast, while committed RaftAdmin membership changes remain authoritative on restart.

### 6.5 Doc lifecycle
`*_proposed_*` → `*_partial_*` after PR-B (the topology is deployable) → `*_implemented_*` after PR-C (integration harness lands). `git mv`, propose date fixed.

## 7. Cross-doc impact (explicit)

- **Unblocks leader-balance PR2/PR3 (#953).** #953 §1.1a names this as PR0 and §5/§4 state PR2/PR3 are blocked on "a topology where each of N groups has voters on ≥2 of 3 nodes." PR-C's harness is precisely the convergence-test topology #953 §5 requires; PR-B delivers the `TransferLeadershipToServer`-has-a-target precondition #953 §1.1a calls out. #953 PR1 (observe-only) is **not** blocked on this and can ship independently.
- **Unblocks hotspot-split Milestone 2 cross-group migration testing (#945).** Cross-group range migration is only meaningfully testable when the source and destination groups each have voters on ≥2 nodes (so migration races real replication, not a single in-process voter). The §6.2 harness provides that; M2's migration tests can build on it.
- **No change to leader-balance's own design.** This doc resolves #953 OQ-9 with option (a) as #953 recommended; it does not alter the scheduler's policy, transfer mechanism, or proto extension.

## 8. Open Questions

1. **OQ-1 — Flag spelling: `--raftGroupPeers` companion flag vs. extending `--raftGroups` entries.** §3.1 recommends the companion flag (clean separation of "my listener" vs. "the voter set"; mirrors single-group `--raftBootstrapMembers`). Confirm before PR-A, since it fixes the operator-facing surface.
2. **OQ-2 — Should `--raftBootstrap` be *required* alongside `--raftGroupPeers`, or implied?** §3.2 recommends implied (non-empty `--raftGroupPeers` ⇒ bootstrap=true per group, matching `bootstrap = *raftBootstrap || len(bootstrapServers) > 0`, `main.go:534`). Alternative: require `--raftBootstrap` explicitly for symmetry with single-group. Confirm before PR-B.
3. **OQ-3 — Per-group bootstrap-server carrier: `map[uint64][]Server`.** Resolved here: use a static map built during startup validation and thread that through `buildShardGroups`/`buildRuntimeForGroup` (`main.go:777`, `multiraft_runtime.go:234`). The group set is fixed at startup, so a function provider adds no value until a runtime update path exists.
4. **OQ-4 — Heterogeneous group membership (groups on a subset of nodes).** v1 enforces homogeneity (§3.1 rule 4) to match #953 §2.2. The `raftID@host:port` member syntax already expresses arbitrary per-group sets, so relaxing rule 4 later needs no grammar change — but #953's observation/forward paths assume homogeneity, so we keep the guard until a consumer needs otherwise. Should the validator's homogeneity check be a hard error (v1) or a warning that allows heterogeneous sets for advanced operators? (Recommendation: hard error in v1.)
5. **OQ-5 — Mixed bootstrap + learner start.** `--raftJoinAsLearner` (`buildRuntimeForGroup`'s `joinAsLearner`, `multiraft_runtime.go:238`) lets a node join an existing cluster as a learner. Should `--raftGroupPeers` interoperate with a per-group learner bootstrap (some members start as learners, promoted later), or is learner-join strictly a live-expansion concern (§5)? (Recommendation: learners are live-expansion only in v1; `--raftGroupPeers` declares voters.)
6. **OQ-6 — Single shared raft listener multiplexing all groups.** §3.4 keeps one listener per group (the `5005{1..6}` convention). Is the per-group-port model acceptable at the target scale, or is a single multiplexed raft listener (demux by group ID) worth the transport change for very high M? (Recommendation: per-group ports for v1; revisit only if port count becomes an operational problem.)

## 9. Lifecycle

This document begins as `*_proposed_*`. Per CLAUDE.md / `docs/design/README.md`:
- Rename to `*_partial_*` after PR-B (multi-voter groups deployable at startup), recording which PRs shipped.
- Rename to `*_implemented_*` after PR-C (in-process integration harness landed), with the Jepsen runner (PR-D) tracked as a follow-on.

Use `git mv` so history follows the rename. The propose date (2026-06-12) and slug stay fixed.
