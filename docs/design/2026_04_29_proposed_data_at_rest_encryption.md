# Data-at-rest encryption for elastickv

Status: Proposed
Author: bootjp
Date: 2026-04-29

---

## 1. Goal

Guarantee that, **without possession of a dedicated key, the persisted
state of an elastickv cluster cannot be decrypted**. Specifically, an
attacker who walks away with:

- a powered-off node's disks,
- a Pebble data directory copied off the host,
- a Raft data directory (WAL + snapshots) copied off the host,
- an etcd raft snapshot file streamed during recovery,
- any backup or export blob,

must obtain only ciphertext for every user value the cluster has ever
written. Recovering plaintext requires the cluster's externally-held
key material; nothing on the persistent media is sufficient on its own.

This proposal does **not** address transport encryption (TLS), in-memory
secret extraction from a running process, side-channel attacks,
authentication / authorization, or FIPS attestation. Those are separate
threat surfaces and are explicitly out of scope.

---

## 2. Threat model

### 2.1 In scope

| Attack | Protection |
|---|---|
| Disk theft / decommissioned drive recovery | All user values on disk are ciphertext. |
| Backup or snapshot file leak | Snapshot streams already carry ciphertext (because they replay the storage layer's encrypted bytes); backup tooling inherits the same property. |
| Raft WAL leak | WAL records carry ciphertext payloads (see §4.2). |
| Cross-node snapshot transfer over Raft transport | Already ciphertext in payload; transport TLS is orthogonal. |

### 2.2 Out of scope (documented, not solved here)

- **In-memory keys.** DEKs sit in process memory while the node is
  running. A root-equivalent attacker on a live host can dump them.
  No software-only encryption-at-rest scheme defends against this.
- **User-key (lookup-key) confidentiality.** Pebble must be able to
  range-scan keys, and `ShardRouter` must be able to hash them for
  routing; both require plaintext keys. Pebble SSTs and Raft proposals
  therefore expose the **set of keys ever written** and their lengths,
  even though every value is encrypted. Tenants encoding sensitive
  data into key bytes (e.g., user IDs as part of the key) must be
  warned in the operator docs.
- **Per-version write history.** MVCC encodes `commit_ts` into the
  Pebble key (`encodeKey(user_key, commit_ts)`), not into the value
  body, so the **timestamp of every write** for every key is
  visible from raw SST inspection regardless of value encryption.
  This in turn exposes write-rate-per-key and per-key version
  counts (a rough access pattern), even though the actual values
  remain ciphertext. Same compliance call-out as the lookup-key
  bullet above.
- **TTL and tombstone bits.** The 9-byte value header
  (§6.2: `[tombstone+enc_state(1)] [expireAt(8)]`) is intentionally
  cleartext so the GC sweep and TTL expiry can run without
  decrypting every value. This leaks "this key has TTL T" and
  "this key was deleted at version V". Accepted given the lookup-
  key admission above; flagged here for completeness.
- **Pebble metadata.** Manifest, bloom filters, OPTIONS file, and
  block index expose row counts, key-size distributions, and
  approximate cardinality. They are not encrypted in v1.
- **Network sniffing.** Use the existing TLS knobs
  (`--adminTLSCertFile`, gRPC TLS); this doc does not duplicate them.
- **KEK theft.** If the KEK leaks, all DEKs unwrap and all data is
  exposed. KEK custody is the operator's responsibility; we provide
  KMS integrations so the KEK can stay outside the host.

### 2.3 Explicit non-goals

- Reversing the migration (encrypted → cleartext) on a live cluster.
  Operators must dump-and-reload to disable encryption.
- Order-preserving / searchable encryption of values. We do not pretend
  to support range queries on encrypted value bytes.

---

## 3. Where the encryption boundary lives

There are three plausible boundaries; we pick **(b)** below and explain
why. The choice is load-bearing — it determines what is ciphertext on
disk and how invasive the change is.

### (a) Pebble VFS wrapper

Wrap `vfs.Default` with an encrypting `vfs.FS` and let Pebble open every
SST / WAL / MANIFEST through it. CockroachDB and TiKV do this.

- **Pro:** Single integration point; encrypts SST + Pebble WAL +
  manifest in one go.
- **Con:** etcd raft's WAL package (`go.etcd.io/etcd/server/v3/storage/wal`)
  uses `os.OpenFile` directly with no FS injection point. A VFS
  wrapper covers Pebble but **not** the Raft WAL or Raft snapshots.
  Patching upstream etcd raft is out of scope.
- **Con:** Block-aligned random reads against an SST require
  per-block encryption (CTR/XTS) plus a separate integrity tag, which
  is more complex than the per-value envelope in (b) and gives us
  authenticated-encryption only at the block layer, not the value
  layer.

### (b) Storage value boundary + Raft proposal envelope (chosen)

Encrypt every user value exactly once at the storage layer (§4.1).
Wrap the entire Raft proposal `Data []byte` — operation tag, lookup
key, and cleartext value — with a separate raft DEK envelope (§4.2).
Cleartext user values never touch disk: the WAL holds the raft
envelope, Pebble holds the storage envelope, and the two are
**different** ciphertexts of the same plaintext under different
DEKs (see §4.2 for why this is one encrypt/decrypt per write at
apply time, not two layers of stored ciphertext).

- **Pro:** Exactly one value-encryption site (the storage layer);
  no "is this already encrypted?" branch in the write path.
- **Pro:** AES-256-GCM at the value boundary gives authenticated
  encryption per value: a tampered SST block is rejected on read with
  a clear error, not silently returned as garbage.
- **Pro:** No upstream patches; everything is in elastickv's tree.
- **Con:** Pebble metadata (manifest, bloom, OPTIONS) and lookup keys
  remain plaintext. Documented in §2.2.
- **Con:** Pebble compression (Snappy/Zstd) becomes a no-op on the
  value bytes because ciphertext is high-entropy. We compress
  **before** encrypting at the storage layer to recover most of the
  ratio (see §6.4).

### (c) Application-layer field encryption

Encrypt selected fields (e.g., DynamoDB attribute values) at the
adapter. Rejected: it leaks Redis values, S3 object bodies, and SQS
message bodies — exactly the data the user wants protected — unless we
implement the same scheme four times in four adapters. Boundary (b)
does it once.

---

## 4. What gets encrypted

### 4.1 Pebble values (storage layer)

Every value handed to `MVCCStore.Put` (and the `Set/Hash/Stream/...`
helpers in `store/`) is wrapped in an authenticated envelope before
hitting Pebble. Reads in `MVCCStore.Get` / scans / snapshots unwrap on
the way out.

Envelope format (single byte stream, stored as the Pebble value):

```text
+--------+------+---------+----------+----------------+--------+
| 0x01   | flag | key_id  | nonce    | ciphertext     | tag    |
| 1 byte | 1 B  | 4 bytes | 12 bytes | N bytes        | 16 B   |
+--------+------+---------+----------+----------------+--------+
```

- `0x01` — envelope-version byte. Future authenticated formats reserve
  `0x02..0x0F` (see §11.3). The byte itself is **not** used to
  discriminate cleartext from ciphertext on the read path — that
  decision lives in MVCC metadata, not in the value bytes (see §7.1).
- `flag` — bit 0: `1` if `ciphertext` is the encryption of a
  Snappy-compressed plaintext, `0` if the plaintext was stored
  uncompressed. Bits 1–7 are reserved (`0`).
- `key_id` — 32-bit identifier of the DEK that produced this
  ciphertext. Required so a rotated DEK can decrypt entries written
  before the rotation (see §5.2).
- `nonce` — 12-byte AES-GCM nonce, structured as **three
  deterministic fields** to eliminate the birthday bound entirely.
  No bits of the nonce are random; nonce uniqueness is by
  construction across `(node, process_load, write)`:

  ```text
  +-------------+----------------+----------------+
  | node_id(2B) | local_epoch(2B)| write_count(8B)|
  +-------------+----------------+----------------+
  ```

  - `node_id` — the 16-bit Raft member ID assigned at cluster
    bootstrap. Never reused for a different node within the
    cluster's lifetime; verified at process start against the
    membership snapshot.
  - `local_epoch` — 16-bit per-DEK process-load counter,
    persisted in the local sidecar (§5.1). Incremented and
    fsync'd **before** the new process performs any encryption,
    so a crash between increment and the first write still leaves
    the on-disk counter ahead of any nonce ever used. Wraps at
    65,536 process restarts per node per DEK lifetime; rotation
    triggers (§5.2) reset it to 0 with the new DEK. The
    encryption package logs a warning when `local_epoch >
    0xff00` so an operator has a 256-restart cushion to rotate
    before wrap.
  - `write_count` — 64-bit `atomic.Uint64`, incremented per
    write within the current process. Resets to 0 on process
    start (the `local_epoch` bump is what makes that safe).
    Wrap is unreachable in any realistic deployment (2⁶⁴ ≈
    1.8 × 10¹⁹ writes per process-load).

  An earlier draft used a 32-bit CSPRNG-drawn prefix for the
  high half of the nonce; that has been retracted because a
  random 32-bit value across N process loads carries an N²/2³³
  birthday-collision probability (~1 in 10⁷ at N=30 over a
  multi-year DEK lifetime), and a single nonce collision under
  one DEK is a catastrophic AES-GCM failure (key-recovery + XOR
  of two plaintexts). The deterministic construction above has
  zero collision probability under its preconditions.
- `ciphertext` — AES-256-GCM(plaintext, nonce, AAD = envelope_version
  ‖ flag ‖ key_id). AAD binds the ciphertext to the **entire**
  envelope header — including the compression flag — so a
  header-rewrite attack (re-tagging a ciphertext to a different DEK,
  or flipping the compression bit so the decrypted plaintext is
  passed back through Snappy and crashes the decompressor) is
  rejected on decrypt.
- `tag` — 16-byte GCM authentication tag; mismatched tag → read
  returns a typed `ErrEncryptedReadIntegrity` error, **never** silent
  zero or empty bytes.

Per-value overhead is 34 bytes (1 version + 1 flag + 4 key_id + 12
nonce + 16 tag). For typical workloads (KV values >256 B, Redis
blobs in the kilobytes, S3 object bodies in the megabytes) this is
in the noise.

### 4.2 Raft proposal payloads

There is exactly **one** value-encryption site in the system, and it
is the storage layer (§4.1 / §6.2). The coordinator does **not**
pre-encrypt values before building a proposal, and the FSM does
**not** call a "raw" Pebble write that bypasses storage encryption.
Pre-encryption was floated in an earlier draft and has been
retracted because it forces a "skip-on-already-encrypted" branch in
`lsm_store.PutAt` and makes the storage-layer Keystore a sometimes-
optional dependency, both of which are easy to get wrong.

What the Raft layer adds on top is a separate **proposal envelope**
that wraps the **entire `Data []byte`** of each Raft entry — the
operation tag, the lookup key bytes, and the cleartext value — using
a distinct `raft` DEK (held in the same Keystore as the storage DEK,
but with a different `purpose`). On apply, the FSM unwraps the
proposal envelope, recovers the cleartext operation, and dispatches
to the existing storage handlers; the storage layer then encrypts
the value with the storage DEK on the way to Pebble.

Why a separate raft envelope at all, given the value will be
encrypted again at the storage layer? Because the proposal `Data`
carries **cleartext lookup keys** and operation tags. We
deliberately do not protect lookup keys at the storage layer
(§2.2), but we *can* protect them in the WAL, where the key history
is dense and easy to inspect. Wrapping the proposal payload means
the etcd raft WAL on disk is opaque except for Raft metadata (term,
index, type).

This costs one decrypt + one encrypt per write on the apply path
(decrypt the raft envelope; encrypt the value into the storage
envelope). It does **not** mean two layers of value encryption are
written to disk — the WAL stores only the raft envelope; Pebble
stores only the storage envelope. The two ciphertexts are
**different** byte sequences with different DEKs and different
nonces; they share a plaintext but never share a representation. If
you are tracing a value through the system, expect distinct
ciphertexts at the WAL boundary and at the Pebble boundary —
phrases like "the same ciphertext flows through Raft → WAL → Pebble"
that appeared in earlier drafts of this doc were imprecise. The
correct invariant is: **every persistent surface holds ciphertext;
no persistent surface holds cleartext user values.**

Benchmarks in §6 must confirm the apply-path cost stays inside the
existing FSM apply budget.

### 4.3 etcd raft WAL files

No direct file-level wrapping (etcd raft's WAL package opens files
through `os.OpenFile` and we are not patching upstream). Protection
comes entirely from §4.2: every Raft entry's `Data` is ciphertext, so
the WAL contains only encrypted payloads plus Raft metadata.

### 4.4 etcd raft snapshots and FSM snapshots

The etcd raft snapshot files (`snap/*.snap`) carry serialized Raft
state plus a pointer into the FSM snapshot. The FSM snapshot itself
streams `pebbleSnapshot` (`store/snapshot_pebble.go`), which iterates
the live Pebble database. Because every value in Pebble is already
ciphertext from §4.1, the FSM snapshot stream is ciphertext by
construction. No additional wrapping is required at the snapshot
layer.

The snapshot file header (the `pebbleSnapshotMagic` 8-byte magic plus
`lastCommitTS`) stays cleartext so a snapshot reader can identify the
format before attempting decryption. This is metadata, not user data.

### 4.5 Distribution catalog and HLC ceiling entries

Route catalog entries (`distribution/`) and the periodic HLC ceiling
proposal both flow through the same Raft path. They get the §4.2
envelope for free. The catalog values stored in Pebble are also
encrypted by §4.1.

This is intentional: the route catalog is part of "the persisted
state of the cluster" the threat model promises to protect, even
though its contents are operator-side metadata rather than user data.

### 4.6 What stays cleartext on disk

For absolute clarity, the following remain unencrypted:

- Lookup keys in Pebble SSTs (required for indexing/scans).
- Pebble manifest, OPTIONS, bloom filters, block index.
- Raft metadata: term, index, entry type, configuration changes.
  (Membership changes carry node IDs and addresses, which are
  topology, not user data.)
- The `pebbleSnapshotMagic` header on FSM snapshot streams.
- The encryption sidecar file itself (§5.1) — it stores **wrapped**
  DEKs only; the wrap key (KEK) is held externally.

The threat model in §2 calls these out so operators can decide
whether their compliance regime accepts them. None of them reveal
user values.

---

## 5. Key management

### 5.1 Hierarchy and on-disk layout

Two-tier hierarchy:

- **KEK (Key Encryption Key).** Held outside the cluster, never on
  the cluster's disks. Sources, in order of preference:
  1. AWS KMS: `--kekUri=aws-kms://arn:aws:kms:...`. The KMS key never
     leaves AWS; we call `Encrypt` / `Decrypt` to wrap/unwrap DEKs.
  2. GCP KMS: `--kekUri=gcp-kms://projects/.../keys/...`. Same shape.
  3. HashiCorp Vault Transit: `--kekUri=vault-transit://...`.
  4. Static file: `--kekFile=/etc/elastickv/kek.bin` — 32 bytes raw.
     Recommended only when the file lives on a tmpfs or sealed
     volume that is not part of the elastickv data dir.
  5. Env var: `ELASTICKV_KEK_BASE64=<base64>`. Strongly discouraged
     (leaks via `/proc/<pid>/environ`, `ps eww`, container env
     inspection); supported only for tests and CI.

  No default. If `--encryption-enabled` is set without a KEK source,
  the process refuses to start.

- **DEK (Data Encryption Key).** 32-byte AES key generated locally
  with `crypto/rand`. Two DEKs are issued in v1: `dek_storage` (used
  by §4.1) and `dek_raft` (used by §4.2). Both are wrapped by the KEK
  and persisted in a sidecar file:

  ```text
  <dataDir>/encryption/keys.json
  ```

  Sidecar contents (illustrative):

  ```json
  {
    "version": 1,
    "raft_applied_index": 184273,
    "storage_envelope_active": true,
    "raft_envelope_cutover_index": 184201,
    "active": { "storage": 305419896, "raft": 2596069104 },
    "keys": {
      "305419896":  { "purpose": "storage", "wrapped": "<base64>",
                      "created": "2026-04-29T10:00:00Z",
                      "local_epoch": 7 },
      "2596069104": { "purpose": "raft",    "wrapped": "<base64>",
                      "created": "2026-04-29T10:00:00Z",
                      "local_epoch": 7 }
    }
  }
  ```

  - `storage_envelope_active` mirrors the §7.1 Phase 1 cluster
    flag once the FSM has seen the `enable-storage-envelope`
    entry.
  - `raft_envelope_cutover_index` records the apply index at
    which §4.2 became active for this cluster. `0` means
    Phase 2 has not started; any non-zero value is the
    dispatch boundary used by FSM apply and WAL replay to
    decide which Raft entries are envelopes.

  - **`key_id` is a 32-bit unsigned integer**, the same value that
    appears in the §4.1 envelope `key_id` field. It is generated by
    a CSPRNG draw on the leader at the moment a new DEK is created;
    the leader retries on the (negligibly rare) collision with an
    existing `key_id` so the FSM apply on every node observes the
    same ID. The sidecar uses the decimal string form of the ID as
    the JSON object key — JSON object keys must be strings, but the
    on-disk envelope and the in-memory keystore always work in the
    binary uint32 form. Decode is a single `strconv.ParseUint`.
  - `raft_applied_index` records the Raft log index of the most
    recent rotation entry that has been persisted into this
    sidecar. It is the load-bearing field for the
    sidecar/log-index reconciliation protocol in §5.5.
  - `local_epoch` is the per-DEK process-load counter consumed by
    the §4.1 nonce construction. Bumped and durably persisted on
    every process start before any encryption happens with that
    DEK; reset to 0 when the DEK is created.

  **Crash-durable write protocol.** `os.Rename` is atomic for
  visibility but not crash-durable on its own — a power loss after
  the rename can roll back the file via the file system's metadata
  journal, leaving stale wrapped DEKs on disk while the rotation's
  Raft entry is already committed. To avoid stranding ciphertext
  written under a DEK whose wrap is then lost, the sidecar write
  protocol is:

  1. Write the new contents to `<dataDir>/encryption/keys.json.tmp`.
  2. `file.Sync()` on the temp file (fsync the data + metadata).
  3. `os.Rename` to `keys.json`.
  4. `dir.Sync()` on `<dataDir>/encryption/` (fsync the directory
     entry so the rename is durable).
  5. Only after step 4 does the FSM acknowledge the rotation entry
     as applied (and update `raft_applied_index` in memory; that
     value is then persisted on the next sidecar write).

  Skipping step 2 or 4 turns a power loss into permanent data loss
  for any value written under the new DEK; the §10 self-review
  treats sidecar non-durability as a data-loss-class bug. On
  filesystems that lack `dir.Sync()` semantics (NFS, some FUSE
  mounts) the encryption package refuses to start with
  `ErrUnsupportedFilesystem` rather than silently degrading the
  durability guarantee.

  The sidecar is **safe to leak**: every entry in `keys` is wrapped
  by the KEK. Without the KEK, the file unwraps to nothing.

  The sidecar lives inside the data dir so backups capture it
  alongside the data — restoring data without its sidecar would leave
  values unrecoverable. The KEK is **not** included in any backup.

### 5.2 Rotation

Two rotations to support; both are operator-driven, no automatic
rotation in v1.

- **DEK rotation.** `elastickv-admin encryption rotate-dek
  --purpose=storage|raft`. The admin client RPCs into the leader,
  which:
  1. Generates a new 32-byte DEK locally.
  2. Wraps it under the current KEK.
  3. Proposes a "new DEK" entry through Raft so every node persists
     the new wrapped DEK in its sidecar atomically.
  4. Marks the new DEK active for new writes; old DEKs stay loaded
     in memory for decrypt-only.

  Existing data is **not** re-encrypted eagerly. Cold values keep
  their old `key_id` until the next rewrite (compaction does not
  re-encrypt — it just shuffles ciphertext). A separate
  `elastickv-admin encryption rewrite` job can sweep the keyspace
  over time to retire old DEKs (see §5.4 for the MVCC-history
  interaction that controls when retirement is actually safe); until
  that job has completed and the retired DEK has zero references,
  the operator must keep the old DEK loaded.

  Rotation cadence is bounded by **two** triggers, whichever fires
  first:

  1. **Time:** every 90 days or on suspicion of compromise.
  2. **Writes-per-DEK:** a hard ceiling of 2³² writes per `(DEK,
     process-load)` pair, in line with NIST SP 800-38D §8.3 for
     authenticated encryption. The per-DEK write counter is exported
     as `elastickv_encryption_writes_per_dek{key_id}`; admission
     control refuses new writes once 90% of the ceiling is reached
     and the cluster auto-proposes a `rotate-dek` entry. With
     counter-based nonces (see §4.1) the cryptographic safety budget
     is far higher than 2³², but we keep the conservative limit so
     we are not relying on a single number being correct everywhere
     in the codebase. (Earlier drafts cited the 2⁴⁸ random-nonce
     birthday bound; that figure was wrong — it is the
     50%-collision boundary, not a safe operating point — and has
     been retracted in favour of the counter-nonce + 2³² ceiling
     design above.)

- **KEK rotation.** Performed entirely outside elastickv via the KMS
  provider. The cluster sees no change because the wrapped DEKs in
  the sidecar are only re-wrapped during the next DEK rotation. To
  force re-wrapping under the new KEK without rotating DEKs, the
  operator runs `elastickv-admin encryption rewrap-deks` — same Raft
  path as rotate-dek but only changes the wrap, not the key bytes.

### 5.3 Multi-tenant / per-shard keys

Out of scope for v1. The DEK pair is cluster-wide. A future revision
can split DEKs per Raft group or per logical tenant; the envelope
already carries `key_id` so the on-disk format does not change.

### 5.4 MVCC history, rewrite job, and DEK retirement

The rewrite job (`elastickv-admin encryption rewrite`) is not a
single-pass conversion of "the live value of every key" — Pebble
holds **MVCC history**, and the snapshot/lease-read paths can read
back any version newer than `minRetainedTS`. A naive rewrite that
only touches the live version would leave older versions encrypted
under the retiring DEK and quietly break snapshot reads as soon as
that DEK is unloaded. The rewrite must therefore be MVCC-aware:

1. **Iteration unit is `(user_key, version_ts)`, not `user_key`.**
   The job scans every retained MVCC version and re-encrypts those
   whose `key_id` matches the retiring DEK (or whose MVCC metadata
   bit says cleartext, during the cleartext→encrypted migration of
   §7.1). Tombstones do not carry value bytes and are skipped.
2. **Re-encryption is a same-`commit_ts` rewrite, not a new
   version.** The job opens a Pebble batch, writes the new ciphertext
   at the **same** internal key (preserving `commit_ts`), and
   commits — no new MVCC version, no OCC conflict, no visible
   change to readers. Readers that picked up the old ciphertext
   before the batch landed continue to decrypt under the still-loaded
   old DEK; readers after see the new ciphertext.
3. **Bounded write amplification.** The job's `--rate=N MiB/s`
   throttle sets a Pebble write-rate budget; the implementation
   yields between batches when the rate is exceeded. Compaction is
   left alone — Pebble already amplifies the rewritten bytes
   exactly as if a normal write had landed.
4. **DEK retirement criterion.** A DEK is safe to unload only when
   **both** of the following are true cluster-wide:
   - The rewrite cursor for that DEK has reached the end of the
     keyspace AND `elastickv_encryption_values_per_dek{key_id} == 0`
     across every node (verified by `encryption status --verify`).
   - `minRetainedTS` on every node is greater than the largest
     `commit_ts` ever written under the retiring DEK. Until this
     holds, a snapshot or lease read can still legitimately ask for
     a version that was written under the old DEK.

   The admin command `encryption retire-dek --key-id=...` checks
   both conditions and refuses to unload the DEK otherwise. There
   is no override flag — overriding is silently equivalent to
   "lose data on the next snapshot read."
5. **Bridge / proxy mode is unnecessary.** The mixed-format read
   path in §4.1 already handles "some versions encrypted under
   DEK_old, some under DEK_new, some still cleartext" without any
   client-visible cutover. The rewrite job runs as a background
   convergence step; reads and writes continue throughout. The
   only operator-visible event is the eventual `retire-dek`, which
   is a no-op for clients.
6. **Crash safety.** The rewrite cursor (a Pebble key under
   `!encryption|rewrite|cursor|<key_id>`) is updated in the same
   Pebble batch as the rewritten value, so a crash mid-batch
   either re-runs that batch on restart or skips it cleanly. There
   is no window in which the cursor advances past values that
   were not actually rewritten.
7. **Rewrite cursor key bootstrapping.** The cursor is itself a
   user-keyspace value, so it goes through §4.1 / §6.2 like any
   other write — it carries a `key_id` and is encrypted under the
   active storage DEK. To avoid the chicken-and-egg case where the
   job cannot read its own cursor because the cursor's `key_id`
   references a DEK that has just been retired, the
   `encryption retire-dek` precondition in step 4 also requires
   that **no rewrite cursor key references the DEK being retired**.
   In practice the rewrite job rewrites its cursor key under the
   active DEK every checkpoint, so this is automatically true once
   the job has made any progress under the new active DEK; the
   precondition just prevents an operator from racing
   `retire-dek` against a stalled job.
8. **Reserved-key namespace.** All encryption-internal keys live
   under the reserved prefix `!encryption|...`. The implementation
   PR must check this prefix against the existing reserved
   namespaces in `distribution/` (route catalog), `store/` (Redis
   collection helpers — `!redis|stream|...`, `!redis|hash|...`,
   etc.), and the HLC ceiling key, and pick a different prefix if
   any of them already use the same root. The expected outcome is
   no collision (`!redis|...` and `!encryption|...` are
   disjoint), but it must be confirmed in code review, not
   assumed.

The same machinery is what powers the cleartext→encrypted
migration in §7.1; the only difference is that the source DEK is
the synthetic "no DEK / cleartext" sentinel rather than a real
retiring DEK.

### 5.5 Sidecar / Raft-log reconciliation

The DEK rotation flow in §5.2 says the FSM apply persists the new
wrapped DEK into `keys.json` on every node. That apply is two
operations on the local node — a Pebble write of the rotation log
entry, then a sidecar file rewrite — and they are **not** atomic
with respect to crash. A node that crashes between the two
restarts with the rotation entry committed in its Raft log but the
sidecar still on the previous generation, which would make a
freshly-active DEK unloadable on that node.

To detect and repair this:

1. The sidecar carries `raft_applied_index` (§5.1), updated in the
   same `os.WriteFile` + `os.Rename` that persists the new wrapped
   DEKs.
2. On startup, the encryption package reads the sidecar's
   `raft_applied_index`, then reads the raftengine's persisted
   applied index. If the raftengine has progressed past the
   sidecar's index AND the gap covers any rotation entries, the
   node refuses to start with `ErrSidecarBehindRaftLog`,
   pointing at the operator runbook.
3. Recovery rewraps **every unretired DEK**, not just the active
   one. A node that missed multiple rotations needs *every*
   intermediate `key_id` to decrypt MVCC history that still
   carries those IDs (per §5.4 retirement criterion #4: a DEK
   stays loaded until both the rewrite cursor has passed the
   keyspace AND `minRetainedTS` has advanced past the youngest
   `commit_ts` written under that DEK). Recovery comes in two
   shapes:
   - **Automatic on the leader** — the leader's keystore already
     holds every unretired DEK in memory. It re-proposes a
     `rewrap-deks` entry that serialises **all** unretired
     wrapped DEKs (active + retiring), bringing every node's
     sidecar to the full set in one apply.
   - **Manual on a stuck follower** — operator runs
     `elastickv-admin encryption resync-sidecar`. The command
     replays *all* rotation and `rewrap-deks` entries between
     the sidecar's `raft_applied_index` and the FSM's applied
     index into the local sidecar (not just the most recent
     one), then exits. Replaying only the active DEK would
     leave intermediate `key_id`s missing and silently break
     historical reads on that node — explicitly rejected.
   Refusing to start until recovery completes is deliberate:
   silently serving with an incomplete sidecar would let the
   node write under one `key_id` while failing to decrypt
   historical reads under another, which is exactly the
   split-brain key state DEK rotation is designed to prevent.
4. The reverse case — sidecar ahead of the raft log — cannot
   happen because the sidecar is only written from inside an FSM
   apply; an apply implies the entry is already in the Raft log.
   The startup check still asserts this invariant and aborts on
   violation as a defence against on-disk corruption.

Note: this is local-node integrity, not a substitute for KEK
custody (§9.3). A lost KEK still makes data unrecoverable; this
section only protects against the narrower failure of "DEK was
rotated cluster-wide but this one node missed the sidecar
rewrite."

---

## 6. Implementation plan

### 6.1 New package: `internal/encryption/`

- `cipher.go` — `Encrypter` / `Decrypter` interfaces; AES-GCM
  implementation; envelope format constants.
- `keystore.go` — in-memory DEK map, lookup by `key_id`; `Active()`
  for the current write key.
- `kek/` subpackage — pluggable KEK providers (`file.go`, `awskms.go`,
  `gcpkms.go`, `vault.go`, `env.go`). Each implements
  `Wrap(dek []byte) (wrapped []byte, error)` and `Unwrap(wrapped
  []byte) (dek []byte, error)`.
- `sidecar.go` — atomic read/write of `keys.json` (write to
  `keys.json.tmp` + `os.Rename`).

### 6.2 Hooks into the storage layer

This is the **only** value-encryption site in the system. The
coordinator does not pre-encrypt; the FSM does not bypass this path.

- `store/lsm_store.go` — `pebbleStore` gains an optional
  `*encryption.Keystore`. `Put` paths wrap the cleartext value (and
  set the per-version `encryption_state` bit, see §7.1) before
  `pebble.Batch.Set`; `Get` / iterators dispatch on
  `encryption_state` and unwrap when the bit says encrypted.
- `store/mvcc_store.go` — same hook applied to MVCC value bytes; the
  MVCC `commit_ts` and version metadata are **not** encrypted
  (they're keys, not values, and are needed for visibility
  computation — see §2.2 and §11.6 on what this exposes).
- `store/lsm_store.go` value-header layout extension — the existing
  9-byte header
  (`[tombstone(1)] [expireAt(8)] [user_value...]`, see
  `encodeValue`) is extended so the leading byte encodes both the
  tombstone bit (bit 0) and the encryption-state bits (bits 1–2),
  matching the 2-bit field in §7.1. Existing builds use only bit 0,
  so an old build reading a value written by an encryption-enabled
  build sees an unrecognised tombstone byte and refuses to serve it
  rather than returning ciphertext as cleartext. `valueHeaderSize`
  stays at 9 bytes; no on-disk layout shift.
- `store/snapshot_pebble.go` — no change. It iterates Pebble values
  byte-for-byte, which are already ciphertext (with the
  `encryption_state` bit travelling in the value header).

### 6.3 Hooks into the Raft path

- `kv/sharded_coordinator.go` and `kv/coordinator.go` — wrap the
  proposal `Data []byte` (operation tag + key + cleartext value)
  before submitting to `raftengine`. The coordinator does **not**
  pre-encrypt the value with the storage DEK; that happens
  exclusively at §6.2.
- `kv/fsm.go` — unwrap the raft envelope on apply, recover the
  cleartext operation, and dispatch to the existing storage
  handlers (which then encrypt the value via §6.2). On unwrap
  failure, return an error from `Apply` so the entry is not
  silently skipped (consistency invariant — see CLAUDE.md self-
  review item 1).
- `internal/raftengine/etcd/engine.go` — no changes. It transports
  opaque bytes; whether they are cleartext or ciphertext is
  invisible to it.

### 6.4 Compress-then-encrypt

Add a Snappy compression pass in front of the encryption envelope
inside `store/`. Order matters: ciphertext is high-entropy and
compresses to ~1.0×; cleartext compresses normally. The envelope's
`flag` byte (§4.1) records whether the plaintext was Snappy-encoded
before encryption, so the decrypt path can decide whether to run
Snappy on the way out. Plaintexts that grow under Snappy (already
compressed media — images, video, gzip'd archives, pre-compressed
S3 objects) are stored uncompressed and the flag is left at zero,
so we never pay CPU twice. Because the flag is part of the AES-GCM
AAD (§4.1), an attacker cannot flip it post-hoc to crash the
decompressor.

This is also the reason we cannot rely on Pebble's built-in block
compression alone: by the time bytes reach Pebble's block writer they
are already ciphertext, so Pebble's compression is wasted CPU. We
disable Pebble's per-block compression (`Levels[i].Compression =
NoCompression`) when encryption is enabled.

### 6.5 New flags

```text
--encryption-enabled                    Default: false
--kekUri=...                            Mutually exclusive with --kekFile
--kekFile=/etc/elastickv/kek.bin        32 bytes raw
--encryption-rotate-on-startup=false    Request a DEK rotation at boot
                                        (used by ops scripts; not for
                                        normal restarts)
```

Existing clusters keep working unchanged because `--encryption-enabled`
defaults off.

`--encryption-rotate-on-startup` is a **request**, not a guarantee:
the rotation can only be proposed by the leader. A node booting
with the flag set:

1. Waits for `raftengine` to report a stable leader (with the
   existing leader-election timeout; no extra waiting loop).
2. If this node is the leader, proposes the rotation entry and
   blocks until it commits before the gRPC / Redis / DynamoDB
   listeners open.
3. If this node is a follower, **does not block**. The flag is
   recorded as a pending request in memory; once a leader change
   makes this node leader within the same uptime window, the
   pending request is auto-fired. If leadership never moves to
   this node, the request quietly expires at process exit. This
   keeps `--encryption-rotate-on-startup` safe inside a rolling
   restart loop where every node is briefly started with the flag
   — only the eventual leader actually rotates.

### 6.6 New admin commands (in `cmd/elastickv-admin/`)

```text
elastickv-admin encryption status
elastickv-admin encryption rotate-dek --purpose=storage|raft
elastickv-admin encryption rewrap-deks
elastickv-admin encryption rewrite --rate=10MiB/s
elastickv-admin encryption retire-dek --key-id=<uint32>
elastickv-admin encryption resync-sidecar    # §5.5 follower repair
elastickv-admin encryption enable-storage-envelope
                                              # §7.1 Phase 1 cutover;
                                              # refuses unless every
                                              # voting member reports
                                              # encryption_capable
elastickv-admin encryption enable-raft-envelope
                                              # §7.1 Phase 2 cutover;
                                              # same capability gate;
                                              # records cutover index
                                              # in the sidecar
elastickv-admin encryption disable           # refuses; documents the
                                              # dump-and-reload path
elastickv-admin backup verify --backup-dir=...
                                              # rejects backups
                                              # missing the sidecar
                                              # (§7.3)
```

`status` reports active DEK ids per purpose, count of values per DEK
(via a Pebble scan that samples the `key_id` byte), and the last
rotation timestamp.

---

## 7. Migration

### 7.1 Cleartext → encrypted on a running cluster

#### Why the read path cannot dispatch on a value byte

An earlier draft proposed using a leading version byte (`0x00`
cleartext, `0x01` encrypted) to discriminate legacy values from
encrypted envelopes on read. That is unsafe: pre-encryption values
on disk are arbitrary user payloads with no header, and any legacy
value whose first byte happens to equal `0x01` would be
mis-parsed as an envelope, fail GCM verification (or worse, decode
into garbage that happens to verify with probability 2⁻¹²⁸), and
take the value offline. The same applies to any other in-band
discriminator — Redis blobs, S3 object bodies, and DynamoDB
attribute values are all "arbitrary bytes from the client."

The discriminator therefore lives **out of band**, in MVCC
metadata, never in the value bytes.

#### Per-version `encryption_state` bit in MVCC metadata

Each MVCC version already carries a small metadata header (commit
ts, deletion bit, etc.) alongside the value bytes. We add a 2-bit
`encryption_state` field to that header:

| Value | Meaning |
|---|---|
| `0b00` | Cleartext. Value bytes are the user payload verbatim. Pre-encryption versions are read as this even though they have no flag on disk, because absent header bits decode to zero. |
| `0b01` | Encrypted under the envelope format of §4.1. Value bytes are an envelope. |
| `0b10`–`0b11` | Reserved. Read path errors out (forward-compat trip wire). |

The MVCC encoder/decoder are the only call sites that touch this
field. The bit is **inside** the MVCC metadata header, so the
attacker scenario (a legacy value happening to look like an
envelope) is impossible by construction — the header layout is
fixed and the encryption state is read before the value bytes are
ever interpreted.

#### Rolling enablement: three phases, not one

Both the §4.1 storage envelope and the §4.2 raft envelope create
mixed-binary safety problems that have to be ruled out **before**
any encrypted byte hits disk. The rollout is therefore explicitly
three-phase:

**Why an "obvious" rolling restart with `--encryption-enabled` is
unsafe.** A naive plan — flip the flag node-by-node and rely on
the per-version `encryption_state` bit — is broken at two layers:

- *At the storage layer:* the `lsm_store.go` value header today is
  9 bytes `[tombstone(1)] [expireAt(8)]`, and `decodeValue` reads
  the first byte as `tombstone := data[0] != 0`. The §6.2 plan
  packs `encryption_state` into bits 1–2 of that same byte. An
  encrypted live value (`tombstone=0`, `encryption_state=0b01`)
  becomes `data[0] = 0b00000010 = 2`. A node still on the old
  binary that ingests this value via Raft replication or snapshot
  catch-up reads `data[0] != 0` → "tombstone" → drops the value
  from reads. Silent data loss, not a clean refusal.
- *At the Raft layer:* §4.2 wraps the entire `Data []byte` per
  Raft entry. Apply is deterministic across replicas; a
  non-upgraded follower without the KEK fails every
  raft-envelope entry on apply, drifts behind, and is eventually
  removed.

Both problems share one fix: **gate the encryption-active state on
a Raft-replicated cluster flag, not on a per-node startup flag.**
The cluster-flag flip happens only after a membership-snapshot
check confirms every voting member is running an
encryption-capable binary. The per-node `--encryption-enabled`
flag becomes a *capability* assertion, not a *behaviour* trigger.

**Phase 0 — Capability rollout.**

0. Operator provisions the KEK in the KMS.
1. Operator restarts each node with `--encryption-enabled
   --kekUri=...`. Rolling restart is safe in this phase because
   the new binary still writes cleartext values (storage
   envelope is gated on the cluster flag below) and proposes
   cleartext Raft entries (raft envelope is gated on a separate
   flag in Phase 2).
2. Each upgraded node advertises `encryption_capable = true` in
   its periodic Raft heartbeat metadata (a new field on the
   peer-metadata payload added by `peer_metadata.go`). The flag
   is purely informational at this point.
3. When the membership snapshot of every Raft group shows
   `encryption_capable = true` for every voting member, Phase 0
   is complete. `encryption status` reports the capability
   coverage so the operator can wait for it before moving on.

**Phase 1 — Storage envelope cluster-flag flip.**

4. Operator runs `elastickv-admin encryption enable-storage-envelope`.
   The leader rechecks the membership-snapshot capability gate
   from step 3 and then proposes a single Raft entry with the
   cluster-wide flag `storage_envelope_active = true`. This
   entry's `Data []byte` is the **legacy framing** (i.e., the
   existing `kv/fsm.go` first-byte tag space; see Phase 2's note
   below) so every replica — even one that somehow missed the
   capability advert — can still decode and apply it.
5. From the apply index of that flag entry onward, every storage
   layer Put writes `encryption_state = 0b01` and the §4.1
   envelope. MVCC versions written before that index keep
   `encryption_state = 0b00` (cleartext) and are read back through
   the dispatch path; mixing within a single key is safe per §5.4.
6. Operator runs `elastickv-admin encryption rewrite` to convert
   the remaining cleartext MVCC versions in place (per §5.4).
   Phase 1 is complete when `encryption status --verify` reports
   zero `encryption_state = 0b00` versions across every node
   (excluding tombstones) AND `minRetainedTS` has advanced past
   the youngest cleartext `commit_ts`.

**Phase 2 — Raft envelope cluster-flag flip.**

7. Operator runs `elastickv-admin encryption enable-raft-envelope`.
   The leader rechecks the same capability gate, then proposes a
   single Raft entry with the cluster-wide flag
   `raft_envelope_active = true`. This entry itself uses the
   legacy first-byte tag space so it remains decodable by any
   replica that has applied entries up to it.
8. From the apply index of that flag entry onward, every leader
   wraps new proposal `Data []byte` with the raft DEK (§4.2).
   **There is no in-band format tag in the proposal payload.**
   Replicas dispatch on the **Raft log index** of the entry
   relative to the persisted `raft_envelope_cutover_index` (the
   apply index of the flag entry, recorded in the local sidecar
   on apply). Entries below the cutover index are decoded via
   the existing first-byte tag space (`0x00` single-request,
   `0x01` batch, `0x02` HLC-lease, etc.); entries at-or-above
   the cutover index are unwrapped through the raft envelope
   first. This explicitly avoids re-using `kv/fsm.go`'s existing
   first-byte tag values for the "is this an envelope?"
   discriminator — that re-use would make pre-cutover batch
   entries (`0x01`) ambiguous during WAL replay.
9. Snapshots taken during Phase 2 carry the cutover index in
   their metadata header so a fresh follower joining
   mid-Phase-2 reconstructs the same dispatch boundary on
   ingest.

#### Why §4.2 cannot be turned off again

Once `raft_envelope_active = true` has been committed, the WAL on
every node interleaves cleartext (pre-flag) and raft-envelope
(post-flag) entries. Disabling §4.2 would require rewriting WAL
entries, which etcd raft does not support. The flag is therefore
one-way; the only way back is dump-and-reload (§7.2).

#### Compatibility with snapshot streaming during migration

A leader streaming a Pebble snapshot to a new follower mid-Phase-1
ships a mix of `encryption_state = 0b00` and `0b01` MVCC versions;
the receiving follower ingests both correctly because the
metadata travels per-version. A snapshot taken in Phase 2 ships
only encryption-state-`0b01` MVCC versions plus a Phase-2-flagged
header so the receiver knows to expect raft envelopes from there
on.

### 7.2 Why we will not support encrypted → cleartext

Toggling encryption off would require rewriting every value while
keeping the cluster online and dealing with a half-encrypted state in
which the security guarantee is silently broken. The same outcome is
better served by a dump-and-reload: export the data with the cluster's
DEK loaded, restore into a fresh cluster started without
`--encryption-enabled`. Documented in the operator runbook.

### 7.3 Backup / restore compatibility

A backup file is just a stream of Pebble values, which are already
ciphertext. Restoring requires either:

- The same KEK (so the sidecar's wrapped DEKs unwrap), **or**
- A fresh KEK plus the original DEK bytes provided out-of-band
  (rare; for forensic recovery).

Backup tooling must capture the `<dataDir>/encryption/keys.json`
sidecar alongside the data dir. Without it, even the right KEK cannot
recover the data because the wrapped DEKs are gone.

To make this hard to get wrong:

- `elastickv-admin backup verify --backup-dir=...` (added in §6.6)
  rejects a backup that contains encrypted SST files but no
  sidecar, with `ErrBackupMissingSidecar`. Operators should run
  this in their backup-pipeline post-step.
- On restore, the startup checks in §9.1 already refuse to open a
  data dir whose sidecar is absent or whose wrapped DEKs do not
  unwrap under the configured KEK. A backup restored without its
  sidecar therefore fails fast at process start, not silently
  later when a read attempts to decrypt.

---

## 8. Performance

### 8.1 CPU

The numbers below are for hosts with **AES-NI** (or ARMv8 Crypto
Extensions), which is the deployment target. Go's `crypto/aes` uses
the hardware path automatically on those CPUs; `runtime/cpu`
exposes detection via `cpu.X86.HasAES` / `cpu.ARM64.HasAES` and the
encryption package logs the detected backend at startup.

- **AES-NI / ARMv8 Crypto path:** AES-256-GCM runs at 3–5 GB/s per
  core. For a typical write workload (~10k writes/s of ~1 KiB
  values per node = 10 MiB/s) the encryption CPU is well under 1%
  of one core. The compress-then-encrypt path (§6.4) doubles that,
  still negligible.
- **Software-fallback path:** older ARM, RISC-V, or virtualised
  hosts that do not surface AES instructions to the guest get the
  Go software AES — typically 200–400 MB/s per core, a 10–15×
  penalty. The §8.3 benchmark gate (≤ 10% throughput regression)
  cannot be met on those hosts; deployments that target them must
  re-benchmark and either tighten the throughput budget or accept
  the regression. The startup log line lets operators see which
  path they are on without re-reading the kernel cpuflags.

The write-amplification path (Pebble compaction) does **not** touch
encryption — it shuffles ciphertext around. Compaction CPU is
unaffected.

The read path pays one decrypt per value returned. Block-cache hits
in Pebble return ciphertext from the cache; the decrypt cost lands
above the cache. Cache effectiveness is unchanged.

### 8.2 Disk

Per-value overhead: 34 bytes per stored value
(1 version + 1 flag + 4 key_id + 12 nonce + 16 tag), per the §4.1
diagram. The 9-byte `lsm_store.go` value header (tombstone /
encryption-state / `expireAt`) is unchanged. For Redis hashes /
streams stored as one blob per key this is one envelope per blob,
not per element.

Compression ratio on text-shaped workloads (logs, JSON) drops from
~3× (Pebble-side Snappy) to ~2.5× (storage-side Snappy on cleartext
before encryption). Acceptable.

### 8.3 Benchmarks required before merge

- `store/lsm_store_sync_mode_benchmark_test.go` — extend with an
  encrypted variant; assert ≤ 10% throughput regression.
- `kv/` end-to-end with 1 KiB values; assert leader-write p99 ≤ 110%
  of pre-encryption baseline.
- The Redis adapter `XADD` hot path
  (`adapter/redis_compat_commands.go`); assert that the compress
  pass does not regress small-value writes by more than 5%.

### 8.4 Jepsen

Run the existing Redis and DynamoDB workloads against an encrypted
cluster. Encryption is consistency-transparent (same input bytes,
different output bytes; FSM apply still deterministic), so no new
Jepsen workload is required. A pass under the existing suite is the
acceptance gate.

---

## 9. Operational concerns

### 9.1 Refusing to start

The process refuses to start if any of the following hold:

- `--encryption-enabled` set, but no KEK source provided.
- `--encryption-enabled` set, but the data dir contains a sidecar
  whose wrapped DEKs do not unwrap under the configured KEK
  (mismatched KEK; almost certainly an operator error).
- `--encryption-enabled` **not** set, but the data dir contains a
  sidecar (refusing prevents accidental downgrade to cleartext
  reads, which would silently bypass encryption on new writes).
- The sidecar's `raft_applied_index` is behind the raftengine's
  persisted applied index AND the gap covers any rotation entries
  (see §5.5: `ErrSidecarBehindRaftLog`, recovered via
  `encryption resync-sidecar`).
- The sidecar is on a filesystem whose `dir.Sync()` does not
  guarantee crash durability of the rename (`ErrUnsupportedFilesystem`,
  see §5.1's crash-durable write protocol).
- The encryption package's startup membership check fails to
  resolve a stable 16-bit `node_id` (the §4.1 nonce construction
  needs one; without it nonce uniqueness cannot be guaranteed).
- The local sidecar's `raft_envelope_cutover_index` disagrees
  with the value carried in the most-recent ingested snapshot
  header (`ErrEnvelopeCutoverDivergence`). This catches a node
  that joined mid-Phase-2 from a snapshot taken before Phase 2
  was enabled, then later replayed Raft entries that crossed
  the cutover; resolved by `encryption resync-sidecar`.

Each refusal logs a single, unambiguous error pointing at the
relevant flag and runbook section.

### 9.2 Observability

New metrics:

- `elastickv_encryption_active_dek_id{purpose}` — gauge, label is
  storage/raft.
- `elastickv_encryption_decrypt_failures_total{reason}` — counter;
  `reason` is `tag_mismatch`, `unknown_key_id`, `truncated`,
  `bad_version`. Any non-zero value is a paging-grade signal.
- `elastickv_encryption_writes_per_dek{key_id}` — counter; drives
  the §5.2 writes-based rotation trigger.
- `elastickv_encryption_value_overhead_bytes` — histogram of
  `payload_size - plaintext_size` per write. **Note:** when
  compression is enabled (§6.4) this histogram leaks information
  about plaintext compressibility, which in turn correlates with
  plaintext size. Deployments where that correlation is itself
  considered sensitive (high-security tenants) should suppress the
  metric via the existing Prometheus relabel config rather than
  ship it to a shared collector.
- `elastickv_encryption_kek_unwrap_seconds` — KMS round-trip
  histogram; alerting threshold for KMS outages.
- `elastickv_encryption_sidecar_raft_index` — gauge of the
  sidecar's `raft_applied_index`. A persistent gap between this
  and the FSM applied index is the §5.5 split signal.

Logging: structured `slog` with `key_id` (never the key bytes),
`purpose`, and `data_dir`. Never log the DEK or KEK material.

### 9.3 Recovery

If the KEK is permanently lost, the data is permanently
unrecoverable. This is the design goal. The operator runbook must
state this in bold and recommend a tested KEK custody scheme (KMS
multi-region replication, sealed envelopes, etc.) before
encryption is enabled in production.

---

## 10. Self-review of the design

Per CLAUDE.md, five lenses on the proposal itself before
implementation begins. None replaces the same five lenses on the
eventual code change.

1. **Data loss.** The dangerous path is "decrypt fails →
   value disappears." The design forbids silent skips: any decrypt
   failure inside `MVCCStore.Get` returns a typed error;
   `kv/fsm.go::Apply` propagates decrypt failures rather than
   returning `nil`. Snapshot restore validates the envelope on
   ingest, not just on read. The migration path dispatches on the
   per-version `encryption_state` bit in MVCC metadata (not on the
   value bytes — see §7.1 on why a leading byte is unsafe), so a
   half-migrated database stays fully readable and a legacy value
   that happens to start with `0x01` cannot be misclassified as an
   envelope. Two specific data-loss-class failure modes are
   addressed by hard preconditions rather than by recovery code:
   (a) sidecar non-durability — the §5.1 write protocol fsyncs
   the file *and* the parent directory before the rotation is
   acknowledged, so a power loss cannot strand ciphertext under a
   wrap that has rolled back; (b) AES-GCM nonce reuse — the §4.1
   nonce is built from `node_id ‖ local_epoch ‖ write_count`,
   each field deterministic, the epoch persisted-and-fsynced
   before any encryption, so even a crash-restart loop cannot
   cause two writes under one DEK to share a nonce. The remaining
   open risk is a buggy `compress-then-encrypt` path that
   mis-frames the compressed payload; mitigation is a round-trip
   property test in `store/` using `pgregory.net/rapid` over
   arbitrary byte slices.

2. **Concurrency / distributed failures.** DEK rotation goes through
   Raft so every replica observes the new DEK at the same log index.
   A leader change mid-rotation is benign because the proposal
   either commits or it doesn't; partial state is impossible.
   The keystore is read on every Get/Put — implement as a
   copy-on-write `atomic.Pointer[map[uint32][]byte]` to avoid
   contending a mutex on the hot path. KMS round-trips happen only
   at boot and on rotation, never on the data path, so KMS latency
   does not enter steady-state. Snapshot transfer between nodes is
   unaffected because the bytes are already ciphertext; the
   receiving node's keystore must contain the relevant DEK before
   it can read the ingested data, which is guaranteed by the Raft
   ordering of the rotation entry. The three-phase rollout in §7.1
   is the load-bearing piece for "node not yet upgraded" cases:
   Phase 0 (capability rollout) gets every node onto the new
   binary while still writing cleartext so the storage-layer
   `data[0] != 0` tombstone test on old code can never see a
   value with `encryption_state` bits set; Phase 1 (storage
   cluster-flag) only flips after a membership-snapshot check
   confirms every voting member is `encryption_capable`; Phase 2
   (raft cluster-flag) reuses the same gate. The membership
   check is **not** advisory — skipping it would let one
   upgraded leader produce raft envelopes that lock every
   non-upgraded follower out of apply, or produce
   `encryption_state = 0b01` values that an old follower
   silently drops as tombstones. Sidecar recovery in §5.5 is
   correspondingly load-bearing for the "missed multiple
   rotations" case: replaying only the active DEK would strand
   intermediate `key_id`s and silently break historical reads.

3. **Performance.** AES-NI puts encryption CPU below the existing
   FSM apply CPU. The compress pass recovers most of Pebble's lost
   compression. The single new allocation per Put (the envelope
   buffer) reuses a `sync.Pool`-backed slice. Block-cache hit rate
   is unchanged because Pebble caches the ciphertext, not the
   plaintext. The risk is a hidden N+1 decrypt on `HGETALL` /
   `XREAD` when a single Pebble value contains many logical
   sub-elements; that decrypts once per Pebble value (cheap), not
   once per sub-element.

4. **Data consistency.** Encryption is invariant under MVCC and
   OCC: same input bytes produce different ciphertext only because
   of the random nonce, but visibility decisions are made on the
   key/timestamp pair, which is unencrypted. HLC ceiling proposals
   carry only timestamps in their payload; they encrypt and decrypt
   like any other proposal. The route catalog watcher
   (`distribution/`) reads ciphertext through the same storage
   path, so version bumps propagate normally. Lease reads pick up
   the active DEK before serving — the lease itself does not touch
   key material, so no new staleness window.

5. **Test coverage.** New tests required:
   - `internal/encryption/cipher_test.go` — envelope round-trip,
     tag-tamper rejection, AAD-tamper rejection (including
     compress-flag flip), version-byte rejection, unknown-`key_id`
     rejection, counter-nonce monotonicity.
   - `internal/encryption/sidecar_test.go` — atomic write,
     concurrent rotate, corrupted-sidecar refusal,
     `raft_applied_index` reconciliation per §5.5.
   - `internal/encryption/kek/*_test.go` — one per provider, with
     KMS faked at the SDK boundary.
   - `store/lsm_store_encryption_test.go` — Put/Get round trip,
     rotation mid-write, mixed cleartext/ciphertext via the MVCC
     `encryption_state` bit, including a legacy value whose first
     byte is `0x01` (regression test for the migration P1).
   - `kv/fsm_encryption_test.go` — apply with valid raft envelope,
     apply with truncated envelope (must error, not skip), apply
     across a rotation boundary, follower with stale sidecar
     refuses to start.
   - Jepsen Redis + DynamoDB suites against an encrypted 3-node
     cluster as the acceptance gate.

---

## 11. Open questions

1. **Per-shard DEKs.** v1 is cluster-wide. Splitting per Raft group
   would let an operator quarantine a compromised shard. The
   envelope already carries `key_id`, so the on-disk format is
   forward-compatible. Defer until there is a concrete request.

2. **Pebble VFS layer for defense-in-depth.** Even with §4.1+§4.2, an
   attacker reading raw SST blocks sees lookup keys and metadata. A
   future revision can wrap `vfs.Default` (boundary (a) from §3) on
   top of value-level encryption, accepting the complexity tax. Not
   needed for the v1 threat model.

3. **Envelope-format versioning.** v1 ships envelope version `0x01`.
   We will reserve `0x02..0x0F` for future authenticated formats
   (e.g., AES-256-GCM-SIV if nonce-misuse resistance matters,
   ChaCha20-Poly1305 for non-AES-NI hosts). The decrypt path
   dispatches on the version byte; anything unknown errors loudly.
   Note: this dispatch only runs when the MVCC `encryption_state`
   bit (§7.1) already says the version is encrypted — the version
   byte is **not** how we tell cleartext from ciphertext.

4. **Audit / compliance hooks.** Some compliance regimes (PCI, HIPAA,
   FIPS-140) require attestation of the cryptographic module. We use
   Go's standard library `crypto/aes` + `crypto/cipher`, which is
   not FIPS-validated by default. A future PR can compile against
   `GOEXPERIMENT=boringcrypto` for FIPS-validated AES; out of scope
   here.

5. **Interaction with `lua_commit_batching`
   (`docs/design/2026_04_22_implemented_lua_commit_batching.md`).**
   Lua batches multiple Puts into one Raft proposal. With encryption
   on, each Put inside the batch carries its own envelope; the
   outer proposal envelope wraps them all. Verify the batch encoder
   does not assume the inner bytes are compressible/cleartext.

6. **TTL / expiry visibility.** The expiry timestamp is stored
   alongside the value in MVCC metadata. We deliberately leave it
   cleartext so the GC sweep can decide what to drop without
   decrypting every value. This leaks "this key has TTL T", which
   is accepted by the threat model (§2.2 already accepts that the
   set of keys is visible).
