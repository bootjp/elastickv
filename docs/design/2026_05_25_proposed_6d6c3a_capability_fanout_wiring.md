# Stage 6D-6c-3a — capability fan-out closure wiring

| Field | Value |
|---|---|
| Status | proposed |
| Date | 2026-05-25 |
| Parent designs | [`2026_05_18_partial_6d_enable_storage_envelope.md`](2026_05_18_partial_6d_enable_storage_envelope.md) (6D-6c-3 milestone; §4 capability fan-out), [`2026_04_29_partial_data_at_rest_encryption.md`](2026_04_29_partial_data_at_rest_encryption.md) (§7.1 rollout) |
| Builds on | 6D-3 (`internal/admin.CapabilityFanout` helper), 6D-6a (`adapter.WithEncryptionAdminCapabilityFanout` option) |

## 0. Why this doc exists

The `EnableStorageEnvelope` cutover RPC (6D-6a) runs the §4 capability
fan-out before proposing the cutover — but only when a
`CapabilityFanoutFn` closure is wired via
`WithEncryptionAdminCapabilityFanout`. `main.go` does **not** wire it
today, so `s.capabilityFanout == nil` and the cutover RPC refuses with
the §4 "fan-out not wired" `FailedPrecondition`. 6D-6c-3a builds and
wires that closure; 6D-6c-3b adds the end-to-end test on top.

The fan-out helper (`admin.CapabilityFanout(ctx, routes, dial,
timeout)`) and the RPC option already exist and are tested. This slice
is pure wiring of those existing pieces into `main.go` — no new RPC, no
wire-format change.

## 1. Scope (6D-6c-3a)

- `buildCapabilityFanoutFn(runtimes, connCache, timeout)
  adapter.CapabilityFanoutFn` in a new `main_encryption_fanout.go`:
  - **Snapshot builder** — for every runtime, call
    `rt.engine.Configuration(ctx)` and map each `raftengine.Server`
    to an `admin.RouteMember{FullNodeID: etcd.DeriveNodeID(srv.ID),
    Address: srv.Address}`, splitting on `srv.Suffrage`
    (`SuffrageVoter` → Voters, else Learners) into one
    `admin.RouteGroup{GroupID: rt.spec.id}`. The §4.1 contract is
    "every (voter ∪ learner) of **every** Raft group", so the
    snapshot spans **all** runtimes, not just the cutover RPC's group.
  - **DialFunc** — `connCache.ConnFor(addr)` → `pb.NewEncryptionAdminClient(conn)`,
    with a **no-op cleanup** (the cache owns the conn lifecycle and
    reuses it across probes; closing per-probe would defeat pooling).
  - The closure runs `admin.CapabilityFanout(ctx, snapshot, dial,
    timeout)`; a snapshot-build error (any `engine.Configuration`
    failure) is returned as the closure error so the cutover RPC
    fails closed (§4 maps it to a refusal).
- A **dedicated** `kv.GRPCConnCache` for the fan-out, created in
  `startRaftServers` and closed via the cleanup stack. Not the
  admin-forward cache: that one is gated on `--adminEnabled`, but the
  cutover fan-out must dial whenever encryption mutators are enabled,
  independent of the admin HTTP surface. `kv.GRPCConnCache.ConnFor`
  already uses the shared `internalutil.GRPCDialOptions()` so the
  fan-out dials with the same transport posture as every other
  intra-cluster gRPC client.
- Wire the closure into `registerEncryptionAdminServer` via
  `WithEncryptionAdminCapabilityFanout`, gated on the same
  `enableMutators` boolean that gates the Proposer/LeaderView (the
  fan-out is only meaningful when the cutover mutator is reachable).
- Fan-out timeout: a `const` in `main.go` (start at 5s — generous for
  a small cluster GetCapability round-trip; the helper bounds the
  whole fan-out by it regardless of member count).

### Out of scope (6D-6c-3b)

The end-to-end integration test (single-node cluster: Bootstrap →
EnableStorageEnvelope → Put → read-back-via-envelope) lands in 3b on
top of this wiring.

## 2. Fail-closed posture

- **Snapshot build error** (`engine.Configuration` fails on any
  group) → closure returns the error → cutover RPC refuses. Never
  propose a cutover against a membership view we could not fully
  enumerate.
- **Unreachable / not-capable member** → handled inside
  `CapabilityFanout` (verdict `Reachable=false` / `EncryptionCapable=false`
  → `OK=false`) → cutover refuses. No partial success (§4.3).
- **Closure not wired** (encryption mutators disabled) → unchanged
  existing behavior: `s.capabilityFanout == nil` → cutover refuses
  with the §4 "not wired" `FailedPrecondition`.

## 3. Why a dedicated conn cache (not the admin-forward one)

The admin-forward `connCache` is constructed only when `--adminEnabled`
(it backs the follower→leader admin write forwarder). The cutover
capability fan-out is orthogonal: it must dial every member's
`EncryptionAdmin` endpoint whenever the operator has enabled
encryption mutators, regardless of whether the admin HTTP API is
served. Coupling the two would make the fan-out silently inert on a
`--encryption-enabled` cluster that left `--adminEnabled` off. A
separate cache keeps the lifecycles independent and is cheap (one
idle `*grpc.ClientConn` per peer, closed on shutdown).

## 4. Self-review checklist (for the implementation PR)

- **Data loss / consistency** — read-only control-plane wiring; issues
  no writes and changes no apply path. The cutover it gates is
  unchanged (6D-6a).
- **Concurrency** — `CapabilityFanout` is already concurrent + timeout
  bounded; the snapshot builder runs per-RPC, single-shot; the conn
  cache is already concurrency-safe.
- **Performance** — fan-out runs once per cutover RPC (a rare operator
  action), not on the data path.
- **Test coverage** — unit tests for the snapshot builder (Server →
  RouteMember mapping, voter/learner split, multi-group, Configuration
  error → closure error) and the DialFunc (cache reuse, nil-conn
  guard). The full e2e is 3b.

## 5. Open questions

1. Fan-out timeout value — 5s proposed; revisit if large clusters need
   more headroom (the helper bounds the whole fan-out, not per-probe).
2. Should learners that are mid-snapshot-catchup be probed? Yes — the
   §4.1 contract is unconditional (voter ∪ learner); an unreachable
   learner is a hard refusal, matching the parent design §8.
