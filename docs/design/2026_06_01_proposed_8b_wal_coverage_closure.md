# Stage 8b — WAL coverage analysis and closure

| Field | Value |
|---|---|
| Status | proposed (closure-style) |
| Date | 2026-06-01 |
| Parent design | [`2026_04_29_partial_data_at_rest_encryption.md`](2026_04_29_partial_data_at_rest_encryption.md) (§4.2 raft envelope, §4.3 etcd raft WAL files, §4.6 cleartext residuals) |
| Related slices | Stage 6E (raft envelope cutover) — open; Stage 8a (snapshot header v2) — shipped |
| Sibling status | This is the closure analysis for the "WAL coverage" half of the parent's Stage 8 row. **No implementation work follows.** |

## 0. Why this slice exists

The parent design's Stage 8 row was labeled "Snapshot header v2 + WAL coverage". Stage 8a (#886) shipped the snapshot header v2. The "WAL coverage" half has been **partially settled in §4.3** ("No direct file-level wrapping … protection comes entirely from §4.2") but never formally closed:

- The 8a design doc's `(§4.5)` shorthand for 8b was a stale parent-doc label — §4.5 actually covers route catalog / HLC ceiling entries, not WAL files (corrected in PR #877 round-3).
- The Stage 8 row in the parent doc still reads "Snapshot header v2 + WAL coverage" with no pointer to where the WAL-coverage analysis lives.
- Operators and reviewers reasonably ask "where does the design address WAL-at-rest?" and the answer is currently scattered across §4.3 + §4.6.

This slice consolidates the analysis, affirms the existing decision, and closes the Stage 8 row.

## 1. Out of scope

- Code changes. This is a documentation-only closure analysis.
- New cryptographic primitives. The existing §4.2 raft envelope is the only on-disk protection 8b acknowledges; any future file-level wrapping is explicitly deferred.
- Patching upstream etcd-raft. The parent §4.3 already rules this out and that decision stands.
- High-compliance overlays (e.g., FIPS 140-3 module isolation for WAL records). Out of scope; tracked under Stage 9 if ever needed.

## 2. What the §4.2 raft envelope covers on disk

After Stage 6E ships (open at the time of this writing), every Raft entry's payload — the `Data []byte` carried inside the proposal envelope — is AEAD-wrapped under the cluster's `dek_raft`. The etcd-raft WAL stores those entries verbatim, so the WAL file contents look like:

```text
+-----------------------------------------------------+
| WAL record framing (cleartext, etcd-raft owned)     |
|   - CRC, type, length                               |
+-----------------------------------------------------+
| Raft entry framing (cleartext)                      |
|   - term, index, type, conf-change marker           |
+-----------------------------------------------------+
| Proposal envelope header (cleartext, engine-owned)  |
|   - proposalEnvelopeVersion (1B, 0x01)              |
|   - proposal ID (8B uint64, monotonic per leader)   |
+-----------------------------------------------------+
| AEAD envelope header (cleartext, encryption-owned)  |
|   §4.2 / internal/encryption/envelope.go::Envelope: |
|   - Version (1B)                                    |
|   - Flag (1B)                                       |
|   - KeyID (4B uint32, the DEK id)                   |
|   - Nonce (12B): node_id(2B) ‖ local_epoch(2B) ‖    |
|     write_count(8B) — all big-endian, see           |
|     internal/encryption/nonce_factory.go:13-15      |
+-----------------------------------------------------+
| AEAD body (CIPHERTEXT post-cutover, §4.2)           |
|   - ciphertext + 16B GCM tag                        |
|   - underlying plaintext: protobuf-encoded FSM      |
|     request — operations, keys, values, txn meta    |
+-----------------------------------------------------+
```

Two envelopes nest on disk: the **outer proposal envelope** is owned
by the etcd-raft engine and carries the per-proposal ID handoff that
`resolveProposal` depends on; the **inner AEAD envelope** is owned
by §4.2 and is what 6E activates the wrap-on-propose / unwrap-on-
apply boundary against. Both envelopes have cleartext headers; only
the AEAD body is encrypted.

The **user-data half** (keys, values, operation type, transaction
metadata) is ciphertext post-cutover. The **framing half** (CRC,
term, index, type, proposal ID, AEAD version/flag/key-id/nonce)
stays cleartext on disk.

## 3. Residual cleartext on disk (re-stated from §4.6 for completeness)

For a high-fidelity threat-model picture, an adversary with raw filesystem access to a WAL directory can observe:

1. **Number of entries** (file size divided by average framing).
2. **Entry index gaps** (presence of compaction / snapshot installs).
3. **Term changes** (visible from raft-entry framing).
4. **Entry types** — normal entry vs `ConfChangeV2` / `ConfChange`. ConfChange entries carry node IDs and addresses (topology, not user data).
5. **Monotonic counter leakage** — two independent counters are
   visible per entry on disk:
   - **Proposal ID** (8B uint64 in the proposal envelope header,
     monotonic **per process load** — NOT per leader). Sourced
     from `Engine.nextID()` (`engine.go:3134`), which is a
     process-wide `atomic.Uint64.Add(1)` shared by `Propose`,
     linearizable reads, and admin conf changes. It does NOT
     reset across leadership transitions, and it is incremented
     by non-WAL paths too (reads, admin), so the on-disk WAL
     sequence shows gaps relative to the in-process counter
     (codex P2 #2 on PR #897 — earlier wording claimed
     per-leader resets, which was wrong). What an adversary
     observes on disk is therefore process-local request IDs
     with gaps from non-WAL traffic — useful for throughput
     estimation but NOT a reliable leader-flip signal.
   - **`write_count`** component within the 12B AEAD nonce
     (constructed as `node_id ‖ local_epoch ‖ write_count`).
     Leaks per-writer throughput rate; tracks per-DEK writes per
     node so the rate is observable independently of the proposal
     ID. Both counters move together under steady state but
     diverge in informative ways during leader flips and DEK
     rotations.
6. **DEK rotation events** (KeyID changes between consecutive
   entries — the inner AEAD header's `KeyID` field is on disk as
   cleartext and rotates when a new DEK becomes active).
7. **Snapshot boundaries** (which entries triggered snapshot install).

What the adversary **cannot** observe (because §4.2 wraps the payload):

- User keys, values, operation type (PUT/DEL/GET/COMMIT/ABORT).
- Transaction grouping or two-phase commit metadata.
- Backup or admin operation contents.

**Important caveat — the cleartext window for pre-cutover entries**
(codex P2 #1 + claude review #1 on PR #897). The above list applies
ONLY to entries with `index > raftEnvelopeCutoverIndex`. On any
cluster that enables Stage 6E AFTER existing traffic, the WAL
interleaves cleartext pre-cutover entries with post-cutover
ciphertext entries until the WAL segments holding the pre-cutover
entries are removed by etcd-raft's log compaction (snapshot install
followed by segment purge). During that window an adversary with
raw WAL access can still read user data from the pre-cutover
segments in cleartext. Operators should treat the §4.2 guarantee
as **"eventually true post-compaction"** rather than **"immediately
true post-cutover"**. This is the same engine semantics the 6E
design's strict-`>` dispatch (`entry.Index > raftCutoverIndex`)
documents — the cutover entry itself and everything before it is
plaintext on disk by construction; only later entries are wrapped.

## 4. Threat-model justification for accepting the residual

The threat model in §2 of the parent design protects "the persisted state of the cluster" against an adversary with disk access. The residual cleartext above does not reveal user data; it reveals **traffic-analysis metadata**:

- Cluster throughput (entries per WAL segment ≈ ops per epoch).
- Cluster topology changes (ConfChange entries).
- Leader-flip cadence (proposal-ID resets, term increments).

For the deployment classes elastickv targets — internal clusters with infrastructure-level FS encryption (LUKS, EBS encryption, GCE persistent-disk encryption) handling the file-level layer — the residual is acceptable. The application-level §4.2 envelope provides defense-in-depth against an adversary who bypasses the FS-encryption layer (e.g., live-memory exfiltration of a decrypted WAL segment held in OS page cache).

For deployment classes requiring application-level file-encryption (no infrastructure FS encryption available, or compliance regimes that mandate application-layer WAL coverage — e.g., FedRAMP High, certain healthcare regulators), the parent §4.3 decision explicitly excludes that option from the current design. Operators in that bucket should either:

- Rely on FS-layer encryption (the supported answer).
- Defer adoption until Stage 9 considers a high-compliance overlay (no current commitment).

## 5. Why patching etcd-raft was ruled out

Three reasons, all from §4.3 + parent design constraints:

1. **Upstream maintenance burden**. etcd-raft's WAL package opens files through `os.OpenFile` and writes records via internal calls. Wrapping those would require a fork or a heavy `io.Writer` shim that intercepts every `os.File` operation — substantial code surface to maintain and verify against upstream changes.
2. **Crash-safety semantics**. The WAL's correctness depends on precise `fsync` boundaries, record-length CRC alignment, and idempotent recovery on partial-tail records. Any encryption layer must preserve those exactly; a bug here would corrupt the cluster's recovery story.
3. **Performance**. WAL writes are on the commit hot path. Per-record AEAD adds CPU + allocation overhead that compounds at high throughput. The current §4.2 envelope already covers the payload; an additional WAL-level wrap would re-encrypt bytes that are already ciphertext.

The parent §4.3 decision predates Stage 6E but its reasoning still holds.

## 6. Forward-compat hook (deferred, not committed)

If a future deployment class requires application-level WAL file encryption, the natural operator control surface lives in `internal/raftengine/etcd/wal_store.go` (the file-creation/open/rotate sites the elastickv engine actually controls). **The actual wrapping mechanism would still need to take one of the approaches §5 rules out** — the WAL bytes are written by the upstream `go.etcd.io/etcd/server/v3/storage/wal` package, which manages `*os.File` internally and exposes no external `io.Writer` intercept surface to callers. A future implementation would therefore have to either:

- Fork or vendor the upstream WAL package to inject the encryption layer (the maintenance burden §5 calls out), or
- Move file-level encryption into a lower layer (e.g., a FUSE/eBPF-style interposition or a dedicated wrapping filesystem) that the upstream package can ignore — at which point it stops being an in-process design and crosses into the infrastructure-FS-encryption answer §4 already documents as supported.

Either path puts §6's architectural concerns back in scope (block-boundary alignment with the 64KB segment size, random-access reads during recovery, `fsync` + partial-tail crash-safety equivalence, KEK source for the WAL-DEK). This enumeration is here so a future operator request has a known starting surface; the forward-compat hook is purely informational and **not committed** by Stage 8b.

## 7. Closure: Stage 8 row update

After this doc lands, the parent design's Stage 8 row should read:

> | 8 | Snapshot header v2 (§4.4) | **shipped** (#886 + 8a closure doc) |

with a footnote pointing to this Stage 8b closure for the WAL-coverage half. The previous "Snapshot header v2 + WAL coverage" framing is replaced by:

- 8a — snapshot header v2 — **shipped via #886**.
- 8b — WAL coverage — **decided in §4.3, formally closed by this doc; no implementation work**.

The parent doc's stage table can be updated in the same PR that lands this closure, or in a follow-up; mechanical edit, not architectural.

## 8. Verification action items

None for the implementation side — this slice ships no code. Verification is reviewer-side:

- **Reviewers check that** §4.3's existing "no file wrapping" decision still stands after Stage 6E's raft envelope ships (it does — the envelope covers the payload, not the file framing, exactly as §4.3 analyzed).
- **Reviewers check that** §4.6's enumeration of residual cleartext matches §3 of this doc (it does — §3 is the same enumeration with traffic-analysis framing added).
- **Reviewers check that** no compliance regime elastickv targets requires file-level WAL encryption that the deferred §6 hook does not cover (operator-side judgment; documented in §4 for future re-evaluation).

## 9. After 8b

- Stage 6E ships (open at the time of this writing). Once 6E lands, §4.2's coverage is on every WAL entry post-cutover, and §4.3's "no file wrapping" decision rests on its full intended foundation.
- Stage 9 — KMS-backed wrappers, compression, rotation/retire/rewrite, Jepsen. Stage 9 may revisit the §6 forward-compat hook if any KMS provider's compliance regime demands it; current commitment is none.
