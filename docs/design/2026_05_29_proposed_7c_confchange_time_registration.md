# Stage 7c — ConfChange-time writer registration

| Field | Value |
|---|---|
| Status | proposed |
| Date | 2026-05-29 |
| Parent designs | [`2026_04_29_partial_data_at_rest_encryption.md`](2026_04_29_partial_data_at_rest_encryption.md) (§4.1 writer registry, §5.2 RotateDEK apply), [`2026_05_26_proposed_7a_process_start_registration.md`](2026_05_26_proposed_7a_process_start_registration.md) (§4 forward-looking 7c sketch), [`2026_05_28_implemented_7b_runtime_reregistration.md`](2026_05_28_implemented_7b_runtime_reregistration.md), [`2026_05_28_implemented_7b_prime_runtime_reregistration_rotation.md`](2026_05_28_implemented_7b_prime_runtime_reregistration_rotation.md) |
| Builds on | 7a (process-start propose path), 7a-2 (storage-layer `Registered()` gate), 7b/7b' (runtime watcher) |

## 0. Why this slice exists

7a/7b/7b' cover every registration trigger that fires **on an existing
cluster member** — process start, runtime `EnableStorageEnvelope`,
runtime `RotateDEK`. They do not cover the case where a **brand-new
node joins** the cluster via a Raft `AddVoter` / `AddLearner`
configuration change. The new node has no writer-registry row when the
conf-change commits; its first encrypted write would fail closed on
the 7a-2 `Registered()` gate until 7a's process-start propose
completes — a window that can be operationally significant on a slow
catalog bootstrap or under partition.

Worse, the 7a propose path itself is only safe under the §4.1 case-1
first-seen invariant: if the new node's `(uint16(NodeID), local_epoch)`
collides with another node's existing row (the §6.1 uint16-collision
case), the §4.1 case-4 halt-apply fires AFTER the conf-change has
already added the new member to the cluster — leaving the cluster
permanently bricked at FSM apply.

Both gaps are closed by **pairing the new node's RegisterEncryptionWriter
with the conf-change at the admin-RPC layer**: the leader proposes
registration first, waits for it to commit (or fail), and only then
proposes the conf-change. The §6.1 collision halt apply, if it fires,
fires BEFORE the conf-change exists in the log.

## 1. The problem

After an `AddVoter` / `AddLearner` ConfChange commits and the new node
starts:

- The cluster's writer registry has no row for the new node under the
  currently-active storage DEK.
- The new node's 7a process-start path eventually proposes a row, but
  there is a window between "new node starts accepting writes" and "7a
  propose commits". During this window the new node's writes fail
  closed on `ErrWriterNotRegistered` (7a-2 storage gate). This is the
  safe posture per §2.3 of the parent design, but it surfaces a
  user-visible latency spike on every node addition.
- If the new node's `uint16(NodeID16)` collides with an existing
  node's, 7a's first propose halts the FSM with case-4 (different
  FullNodeID at same uint16). The conf-change is already durable;
  rolling the cluster back requires operator intervention.

## 2. Architecture choice — two viable options

(a) **Pre-register from the admin RPC handler** *(recommended)*.
   `AddVoter` / `AddLearner` handlers gain a pre-step:
   1. Read the new node's `full_node_id` from the request (derived
      from the supplied raftID — already passed in
      `RaftAdminAddVoterRequest.Id`).
   2. Read the cluster's currently-active storage DEK from the
      sidecar (the leader runs this code, so its local sidecar is
      authoritative as of the leader's last apply).
   3. Propose `RegisterEncryptionWriter(new_node_id, active_dek, 0)`
      via the existing 0x03 entry path. Wait for commit (or refuse
      to proceed on error).
   4. Only on registration success, propose the conf-change.
   - **Pro**: No FSM-layer changes; reuses 7a's existing 0x03 entry
     and §4.1 case-1/4 apply logic. The §6.1 uint16-collision halt-
     apply fires BEFORE the conf-change exists in the log. If the
     admin RPC handler crashes between steps (3) and (4), the
     registry has a row for a member that doesn't exist yet —
     benign: the row is dormant, becomes meaningful when the same
     node_id is added later.
   - **Con**: The pre-register step is admin-side; an operator who
     adds members through a path other than the admin RPC (e.g.,
     direct ConfChange via a debugging tool) bypasses the gate.
     7a's process-start path is the safety net in that case.

(b) **FSM-side hook in `applyConfChange`**. The Raft engine's
   apply-side ConfChange handler inserts a registry row inline.
   - **Pro**: Atomic with the conf-change; cannot be bypassed.
   - **Con**: Layering violation — the Raft engine should not know
     about encryption registry semantics. Per-shard FSMs apply
     ConfChanges, but the writer registry lives in the default
     group's storage; cross-group state mutation from a non-default
     group's apply hook is not currently a thing. Also the new
     node's `local_epoch` is not knowable at apply time on other
     members (it is set on the new node's first boot, not on the
     cluster-wide ConfChange apply).

**Recommendation: option (a).** Smaller surface, reuses the existing
proven 0x03 entry path, and the §4.1 collision halt-apply fires before
durable harm. Option (b) is materially larger and re-opens the
"FSM-layer cross-group state" question that the Stage 6 design
deliberately avoided.

## 3. Design (option a)

### 3.1 Layering: interceptor pattern at the admin-RPC boundary

A naive implementation would thread `kv.ShardedCoordinator`,
`kv.ShardGroup`, `encryption.StateCache`, AND
`etcdraftengine.DeriveNodeID` directly into `raftadmin.Server`. That
collapses the Raft-admin / KV / encryption layering — `internal/raftadmin`
is currently engine-generic (built against `raftengine.Engine` and
`raftengine.Admin` interfaces) and has no concrete dependency on the
KV or encryption packages (gemini medium #1 + #2 on PR #868).

Instead, 7c introduces a **`MembershipChangeInterceptor`** interface in
`internal/raftadmin` that the KV/encryption layer implements and
injects at construction time. `raftadmin.Server` stays generic; all
KV/encryption knowledge lives behind the interface:

```go
// internal/raftadmin/interceptor.go  (new file, ~15 LOC)
type MembershipChangeInterceptor interface {
    // PreAddMember runs before AddVoter/AddLearner proposes the
    // conf-change. A non-nil error aborts the membership change.
    // Implementations propose RegisterEncryptionWriter for the new
    // node and wait for commit; see Stage 7c §3.2-§3.4.
    PreAddMember(ctx context.Context, raftID string) error
}
```

```go
// internal/raftadmin/server.go  (modified)
type Server struct {
    admin       Admin
    engine      Engine
    interceptor MembershipChangeInterceptor  // nil = no pre-step
    ...
}

func (s *Server) AddVoter(ctx context.Context, req *pb.RaftAdminAddVoterRequest) (*pb.RaftAdminConfigurationChangeResponse, error) {
    if s.interceptor != nil {
        if err := s.interceptor.PreAddMember(ctx, req.Id); err != nil {
            return nil, err
        }
    }
    index, err := s.admin.AddVoter(ctx, req.Id, req.Address, req.PreviousIndex)
    ...
}
```

The encryption-aware implementation lives in a thin adapter that
`main.go` (or a small new package like `internal/encryption/raftadmin`)
wires up. The adapter holds the `*kv.ShardedCoordinator`,
`*kv.ShardGroup`, `*encryption.StateCache`, AND the `DeriveNodeID`
function — keeping all KV/encryption coupling in one place:

```go
// Pseudocode (adapter, lives outside internal/raftadmin)
type encryptionPreRegister struct {
    coordinate   *kv.ShardedCoordinator
    defaultGroup *kv.ShardGroup
    cache        *encryption.StateCache
    deriveNodeID func(raftID string) uint64  // injected; usually etcdraftengine.DeriveNodeID
}

func (e *encryptionPreRegister) PreAddMember(ctx context.Context, raftID string) error {
    activeDEK, ok := e.cache.ActiveStorageKeyID()
    if !ok {
        return nil // cluster not bootstrapped — no registry to gate on
    }
    newNodeFullID := e.deriveNodeID(raftID)
    entry := registrationEntry(activeDEK, newNodeFullID, 0)
    req := registrationRequest(activeDEK, newNodeFullID, 0)
    return proposeWriterRegistration(ctx, e.coordinate, e.defaultGroup.Engine, entry, req)
}
```

`DeriveNodeID` is passed in as a function value so the adapter remains
neutral with respect to the concrete Raft engine — main.go supplies
`etcdraftengine.DeriveNodeID` today, but a future engine swap is a
one-line wiring change in main.go, not a refactor of the encryption
adapter. (Defining `DeriveNodeID` on `raftengine.Engine` itself was a
considered alternative; rejected because the derivation is a pure
function of the raftID string, not a method that requires engine
state.)

When the operator runs an `AddVoter`/`AddLearner` against an
encryption-unaware build (or against a cluster with encryption
disabled), `main.go` passes a nil interceptor and the conf-change path
runs exactly as today.

### 3.2 Why `local_epoch = 0`

The new node has not yet started, so its `w.epoch` is unknown. Writing
`local_epoch = 0` as the §4.1 case-1 first-seen row gives the new node
the entire `uint16` space for its `BumpLocalEpoch` advances — on first
boot, 7a runs `BumpLocalEpoch(activeDEK)` → `0 → 1` → proposes
`RegisterEncryptionWriter(new_node, activeDEK, 1)` → §4.1 case-2
monotonic advance from `0` to `1`. Subsequent restarts walk `1 → 2 →
3 → ...` cleanly.

Critically, `local_epoch = 0` for a *first-seen* row is correct under
§4.1: case 1 inserts a fresh row at `(FirstSeen, LastSeen) = (0, 0)`,
and any subsequent 7a propose at `epoch > 0` advances `LastSeen`
monotonically. No brick scenario.

### 3.3 §6.1 collision: case-4 halt apply fires PRE-conf-change

If the new node's `NodeID16(FullNodeID)` collides with an existing
member's `NodeID16(FullNodeID)`, the pre-register step's 0x03 apply
hits §4.1 case 4 (different FullNodeID at same uint16) and halts the
FSM. The conf-change has NOT been proposed yet, so the cluster's
membership is unchanged — recovery is "stop the operator command, run
the existing `ErrNodeIDCollision` check, choose a non-colliding raftID,
retry." The 7a doc's `ErrNodeIDCollision` startup membership pre-check
is the documented recovery path; 7c does not need to add anything new
for this case.

Without 7c, the same case-4 halt would fire AFTER the conf-change had
already committed — irrecoverable without manual intervention.

### 3.4 Fail-open posture: the catch-up safety net is 7a, NOT 7c

7c is **not** the only path that registers a new node. 7a's
process-start path on the new node's first boot is the catch-up:

- If an operator adds a member through a non-admin-RPC path (direct
  ConfChange via raftadmin grpc or a debugging tool), 7c's
  pre-register step is bypassed.
- The new node still starts up, 7a runs, proposes
  `RegisterEncryptionWriter`. The first-seen insert succeeds (the
  cluster never registered the node).

7c is therefore an **optimization + collision-safety** layer, not the
sole defense. The 7a-2 storage gate (`Registered()`) remains the
last-line correctness defense for any node that hasn't registered
yet, no matter how it joined.

### 3.5 Leader-only RPC handling

`AddVoter` / `AddLearner` are leader-only — the existing handler
already returns `FailedPrecondition` to non-leaders. The
pre-register step runs in the same goroutine on the leader and uses
the same `coordinate` that the conf-change will use, so the propose
naturally routes to the leader's own engine. No additional
leader-checks needed.

If leadership flips between the pre-register propose and the conf-
change propose, the second call returns the existing not-leader
error and the operator retries. The pre-registered row is durable
across the leader change — the next leader sees it and accepts the
retry's conf-change normally.

## 4. Out of scope (deferred slices)

- **Bootstrap-cohort ConfChange (cluster init)**. The initial member
  set is established via `--raftBootstrap`, not via runtime
  ConfChange. Bootstrap-cohort registration is handled by the §5.2
  `BatchRegistry` payload on the bootstrap entry — already shipped in
  Stage 5.
- **Raft membership removal (`RemoveServer`)**. Removing a member
  does not require de-registering them (the row is harmless when the
  node is gone; if the node ID is reused later, §4.1 case-2 monotonic
  advance handles it cleanly). 7c does NOT propose a registry
  retraction on remove.
- **`local_epoch` recovery if the new node crashes mid-first-boot.**
  Covered by 7a's `BumpLocalEpoch` + §9.1 startup guard already.

## 5. Verification action items (for the implementation PR)

1. New `internal/raftadmin/server_test.go` cases (with a fake
   `MembershipChangeInterceptor`):
   - `TestAddVoter_InvokesInterceptorBeforeConfChange`: a recording
     fake interceptor that returns nil is invoked before
     `s.admin.AddVoter`; assert order and raftID propagation.
   - `TestAddVoter_InterceptorErrorAbortsConfChange`: a fake
     interceptor that returns an error aborts the RPC; assert
     `s.admin.AddVoter` is NOT called.
   - `TestAddVoter_NilInterceptorSkipsPreStep`: with no interceptor
     installed (e.g., encryption-disabled build), AddVoter proceeds
     directly to `s.admin.AddVoter` as today.
   - Symmetric tests for `AddLearner`.
2. New encryption-adapter tests (location: alongside the adapter — if
   it lives in `main`, then a new `main_encryption_confchange_test.go`;
   if it lives in `internal/encryption/raftadmin/`, that package's
   tests):
   - `TestEncryptionPreRegister_ProposesCorrectEntry`: with the
     state cache reporting `activeStorageDEK=X`, calling
     `PreAddMember(raftID=N)` proposes a 0x03 entry for
     `(X, deriveNodeID(N), 0)`. Use a recording fake `Coordinate.Propose`
     and a stub `deriveNodeID` that returns a known sentinel.
   - `TestEncryptionPreRegister_PreBootstrapSkips`: when
     `ActiveStorageKeyID()` reports `(0, false)`, `PreAddMember`
     returns nil without proposing. Encryption-disabled clusters
     and pre-bootstrap clusters share this path.
   - `TestEncryptionPreRegister_ProposeFailureSurfaces`: propose
     errors (simulated §4.1 case-4 halt apply, ctx timeout, propose
     gate refusal) propagate to the caller verbatim — the
     `raftadmin.Server` will then abort the conf-change.
3. New `main_encryption_e2e_test.go` test or extension:
   - `TestEncryption_E2E_ConfChange_NewMemberPreRegistration`: end-
     to-end test driving the production `AddVoter` handler in a
     2-node test cluster (with the encryption interceptor wired),
     then asserting the new node's writer-registry row exists at
     `(activeDEK, newNode, 0)` immediately after the AddVoter call
     returns.
3. Self-review (5-lens) for the implementation PR — particular
   attention to:
   - **Concurrency**: the pre-register step uses the same
     `coordinate` as the conf-change; race between leader flip
     mid-step → second propose fails not-leader, operator retries
     (idempotent via §4.1 case-2).
   - **Data consistency**: the pre-register's `local_epoch=0` row is
     §4.1 case-1 first-seen; subsequent 7a propose at the new
     node's `BumpLocalEpoch(activeDEK)`-advanced epoch is case-2
     monotonic. No brick scenario.

## 6. Rollout / migration

7c is purely additive on the leader side — no on-disk format change,
no FSM apply semantics change, no wire-format change beyond the
existing 0x03 entry shape. Mixed-version cluster:

- **Pre-7c leader, mixed members**: ConfChanges work as today; new
  nodes catch up via 7a's process-start propose. No regression.
- **7c leader, mixed members**: ConfChanges run the pre-register
  step first; the propose lands on every member (including pre-7c
  members) because the 0x03 entry is already a known FSM opcode in
  the pre-7c codebase (it shipped in Stage 6A). Pre-7c members
  apply the 0x03 entry normally.
- **Rollback to pre-7c**: a leader that rolls back skips the
  pre-register step; conf-changes work as today. Already-inserted
  registry rows from prior 7c-leader pre-register calls remain in
  the catalog — benign (the row is for a real node that
  subsequently registered itself via 7a at a higher epoch via
  case-2 monotonic advance).

The 6.1 rolling-upgrade mitigation strategies from 7b' §6.1 (admin-
RPC capability probe etc.) do NOT apply to 7c — the change is
additive and backward compatible with pre-7c FSM apply behavior.

## 7. After 7c

With 7c shipped, every node-registration trigger in the production
deployment topology is covered:

| Trigger | Covered by |
|---|---|
| Bootstrap-cohort init | Stage 5 `BatchRegistry` |
| Process restart (steady state) | 7a |
| Runtime `EnableStorageEnvelope` cutover | 7b |
| Runtime `RotateDEK` | 7b' |
| Runtime `AddVoter` / `AddLearner` | **7c** |
| Direct ConfChange (non-admin-RPC) | 7a (catch-up) + 7a-2 gate |

This closes Stage 7. Stage 8 (snapshot header v2) and Stage 9 (KMS +
compress + rotation/retire/rewrite + Jepsen) follow.
