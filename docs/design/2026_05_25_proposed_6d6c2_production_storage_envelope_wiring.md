# Stage 6D-6c-2 — production storage-envelope write-path wiring

| Field | Value |
|---|---|
| Status | proposed |
| Date | 2026-05-25 |
| Parent designs | [`2026_04_29_partial_data_at_rest_encryption.md`](2026_04_29_partial_data_at_rest_encryption.md) (§4.1 nonce, §5.1 sidecar, §7.1 rollout), [`2026_05_18_partial_6d_enable_storage_envelope.md`](2026_05_18_partial_6d_enable_storage_envelope.md) (6D-6c milestone breakdown) |
| Pulls forward | The deterministic-nonce core of Stage 7 (§4.1 `write_count` / `local_epoch` lifecycle). The full Raft-replicated writer-registry **registration-before-first-write** gate stays in Stage 7. |

## 0. Why this doc exists

6D-6c-1 landed the `StateCache` accessors (`ActiveStorageKeyID`,
`StorageEnvelopeActive`) but left the storage layer's encryption
options unwired in `main.go`. The remaining 6D-6c-2 milestone — as
written in the 6D doc — is "build `encryption.NewCipher(keystore)` and
thread `store.WithEncryption` + `store.WithStorageEnvelopeGate` into
each shard's PebbleStore."

Wiring `store.WithEncryption(cipher, nonceFactory, activeKeyID)`
naively is **unsafe**: the only `NonceFactory` in the tree today is
the test-only `store.CounterNonceFactory`, whose `write_count` atomic
resets to `0` on every process load. Pinned to a fixed `local_epoch`,
that recycles the nonce `node_id ‖ local_epoch ‖ {0,1,2,…}` after a
restart — a catastrophic AES-GCM `(DEK, nonce)` reuse (§4.1).

So a *correct* production write path needs three things the tree does
not yet have, plus the wiring:

1. **Keystore hydration from the sidecar at startup.** The cipher
   reads DEK bytes from the shared `*Keystore`. Today the keystore is
   populated only by FSM apply of the `OpBootstrap` / `OpRotation`
   entries. After a restart where those entries fall behind a Raft-log
   compaction window, the keystore comes up **empty** and the cipher
   cannot decrypt existing envelopes. The sidecar already holds every
   wrapped DEK; startup must unwrap them under the KEK and install
   them.

2. **A `local_epoch` bump-and-fsync on process start.** Per §4.1 the
   `write_count` reset to `0` per process load is only safe because
   `local_epoch` is bumped and fsync'd *before the first encryption
   write*. The tree has the `ErrLocalEpochExhausted` (==0xFFFF) and
   `ErrLocalEpochRollback` (sidecar <= registry) startup guards, but
   nothing that actually performs the bump.

3. **A production deterministic `NonceFactory`** pinned to the bumped
   `local_epoch`, living in a non-test file.

This doc pins the as-implemented design for all four pieces and draws
the scope boundary against the remaining Stage 7 work.

## 1. Scope

### In scope (6D-6c-2)

- `encryption.HydrateKeystoreFromSidecar(ks, kek, sidecarPath)` — unwrap
  every `sidecar.Keys[*].Wrapped` under the KEK and `keystore.Set` it.
  Runs once at startup, after `CheckStartupGuards` (which already
  proves each DEK unwraps cleanly via the `ErrKEKMismatch` check) and
  before `buildShardGroups`.
- `encryption.BumpLocalEpoch(sidecarPath, dekID) (uint16, error)` —
  read-modify-write the active storage DEK's `LocalEpoch`, refuse at
  `0xFFFF` with `ErrLocalEpochExhausted`, fsync via the existing
  crash-durable `WriteSidecar`, and return the new value. Idempotency
  is **not** required: every process load consumes exactly one epoch.
- `encryption.NonceFactory` (production) — deterministic
  `node_id ‖ local_epoch ‖ write_count`, constructed from
  `uint16(DeriveNodeID(raftID))` and the bumped epoch. The byte layout
  is identical to `store.CounterNonceFactory`; the difference is
  provenance (the epoch came from a durable bump, not a test literal).
- `main.go` wiring: build the cipher, the single process-wide
  `StateCache`, and the nonce factory; thread `WithStateCache` into
  every per-shard `Applier`; thread `WithEncryption` +
  `WithStorageEnvelopeGate` (reading `cache.ActiveStorageKeyID` /
  `cache.StorageEnvelopeActive`) into every shard's `PebbleStore`.

### Out of scope (stays in Stage 7)

- **Registration-before-first-write coordinator gate** (§5.2
  process-start path, §4.1 ConfChange-time path). A node bumping its
  `local_epoch` locally is nonce-safe *for itself* across restarts; the
  Raft-replicated `RegisterEncryptionWriter` propose is what makes the
  `ErrLocalEpochRollback` guard's registry anchor advance and what
  detects cross-node 16-bit `node_id` collisions at registration-apply
  time. Cross-node collision is still covered in 6D-6c-2 by the
  existing startup membership pre-check (`ErrNodeIDCollision`) and the
  registry-apply-time collision check shipped in 6A; the
  propose-before-write *gate* (block the coordinator's first encrypted
  write until registration commits) is deferred.
- KMS providers, compression, DEK retirement/rewrite (Stages 9).
- The capability fan-out closure + multi-node e2e — that is 6D-6c-3.

### Why this boundary is safe to ship

The write path only ever emits an envelope when **both**
`StorageEnvelopeActive()` is true (operator ran `EnableStorageEnvelope`)
**and** `ActiveStorageKeyID()` returns a DEK (bootstrap committed). A
freshly-built binary with no bootstrap writes cleartext exactly as
today. The single-node e2e (6D-6c-3) exercises the full
Bootstrap → cutover → Put → read-back loop on one process load where
the registration gate is moot (the node is the only writer and is
registered by the §5.6 bootstrap batch). Multi-node deployments that
add a writer after bootstrap are protected by the startup guards until
Stage 7 lands the propose-before-write gate; this doc does **not**
claim multi-node-churn nonce safety beyond those guards.

## 2. Startup ordering

`run()` today:

```
loadKEKAndRunStartupGuards()   // KEK load + CheckStartupGuards (6C-1/2)
keystore := NewKeystore()      // empty
buildShardGroups(... keystore, sidecarPath ...)  // per-shard stores + appliers
chainEncryptionStartupGuard()  // 6C-2d gap guard (post-engine)
```

6D-6c-2 inserts two steps between the guard load and
`buildShardGroups`, gated on `encryptionEnabled && Active.Storage != 0`:

```
kekWrapper := loadKEKAndRunStartupGuards()
keystore   := NewKeystore()
stateCache := NewStateCache()

if encryptionActive(sidecar) {                 // Active.Storage != 0
    HydrateKeystoreFromSidecar(keystore, kekWrapper, sidecarPath)   // (1)
    epoch := BumpLocalEpoch(sidecarPath, sidecar.Active.Storage)    // (2)
    cipher := NewCipher(keystore)                                   // (3)
    nonceFactory := NewNonceFactory(uint16(DeriveNodeID(raftID)), epoch)
}                                                                   // (4)
buildShardGroups(... keystore, stateCache, cipher, nonceFactory ...)
```

Ordering rationale:

- **Hydrate before bump.** The bump's `ErrLocalEpochExhausted` /
  fsync only matters once DEKs exist; hydrating first keeps the
  "encryption active" branch self-contained and lets the cipher see
  the DEK the bumped epoch refers to.
- **Bump before any store opens.** No `PebbleStore` can serve a write
  (and therefore issue a nonce) until `buildShardGroups` returns, so
  performing the durable bump before that call guarantees the fsync
  precedes the first nonce — the §4.1 invariant.
- **StateCache before buildShardGroups.** The per-shard appliers need
  the shared cache pointer (6D-6c-1's P1 fix), and the per-shard stores
  need `cache.ActiveStorageKeyID` / `cache.StorageEnvelopeActive` as
  their closures. `NewApplier` primes the cache from the sidecar, so by
  the time the first store opens the cache reflects on-disk state.

## 3. Crash / restart correctness

- **Bump fsync vs. first write.** `BumpLocalEpoch` calls the existing
  `WriteSidecar` (write-temp + fsync + rename + dir-sync). A crash
  after the bump fsync but before any write simply consumes an epoch
  (the next start bumps again); a crash before the fsync leaves the old
  epoch and the next start retries the bump — no nonce was issued
  either way.
- **Hydration is read-only** w.r.t. durable state — it only mutates the
  in-memory keystore. Idempotent across restarts.
- **Keystore.Set conflict.** Hydration installs the same DEK bytes FSM
  apply would; `Set` is idempotent for matching bytes and returns
  `ErrKeyConflict` only if the KEK-unwrap produced different bytes for
  the same id — a halt condition surfaced as a startup failure.

## 4. Self-review checklist (to satisfy on the implementation PR)

- **Data loss** — hydration/bump never delete a DEK or a committed
  write; bump consumes epochs monotonically.
- **Concurrency** — nonce factory is `atomic.Uint64`; StateCache is
  atomic; hydration/bump run single-threaded at startup before any
  store opens.
- **Performance** — startup-only cost; hot path is one atomic add per
  nonce.
- **Data consistency** — the write path stays cleartext unless both
  gate signals are on; AAD binding unchanged; epoch monotonicity
  preserved across restarts.
- **Test coverage** — unit tests for hydration (multi-DEK, KEK
  mismatch, empty sidecar), bump (increment, 0xFFFF refusal, fsync
  durability across a re-read), nonce factory (layout, monotonic
  write_count, distinct epochs); main-wiring smoke test that a
  non-bootstrapped binary stays cleartext.

## 5. Open questions for review

1. Should `BumpLocalEpoch` also bump the **raft** DEK's epoch, or only
   the storage DEK? (Raft envelope is §4.2; this PR wires only the
   storage write path, so the proposal bumps storage only and leaves
   raft-epoch lifecycle to the raft-envelope wiring.)
2. Is hydrating **all** sidecar DEKs (not just the active one) the
   right call? (Yes — reads of pre-rotation versions need historical
   DEKs; the cipher must hold every unretired DEK, matching
   `Cipher.LoadedKeyIDs`.)
