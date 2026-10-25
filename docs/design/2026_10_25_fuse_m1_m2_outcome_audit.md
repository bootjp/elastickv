# FUSE operation outcome audit (M1/M2)

The strict operation journal is a prerequisite for the filesystem integration
in M3. Existing FUSE mutations still use their old dispatch path. A mounted
FUSE test cannot exercise the new journal until M3 routes mutations through it.

## Outcome rules

| Observation | Attempt outcome | Next action |
| --- | --- | --- |
| Terminal committed record and every participant receipt is confirmed by its current leader | Committed | Return the stored result. |
| Terminal rejected/cancelled record | Aborted | Return the stored semantic result or cancellation. |
| A different generation is durably visible after a successful transaction on the record key | Aborted for the old generation | A new generation may be admitted only after an authoritative decision resolver exists; `AdvanceGeneration` currently fails closed. |
| Pending or absent record, timeout, lost response, unsupported exact-version RPC, unavailable participant, or unverified effect | Unknown | Preserve the attempt identity; resolve or return an unknown error. |

All publication attempts mutate both the record and session rows, even when
the user effect is in another shard. The record is the generation fence. A
committed generation change or cancellation must conflict with an old pending
publication; an old publication that wins makes the record terminal. The
journal does not interpret a missing row, missing file, or an RPC timeout as
proof of abort. It never resends a logical effect after an unknown result.

Single-group publication is one Raft-applied transaction. For multi-group
publication, strict dispatch reports success only after all secondary commit
responses. A primary decision followed by a missing, failed, or empty
secondary response is Unknown. A committed record is resolved by checking a
unique, single-version receipt in each effect group through its current
leader. Each receipt commits in the same transaction as that group's effects
and is removed with the operation record at ACK. Later writes and MVCC
compaction of effect keys cannot erase an unacknowledged receipt. An old leader
without the exact-version response field cannot supply this proof.
Existing transaction lock resolution remains responsible for bringing delayed
secondaries to the primary decision; the journal does not synthesize a second
transaction to repair them.

Admission, attempts, results, session fences, and retired-ID watermarks live in
the same replicated keyspace as filesystem data. The versioned record codec
rejects malformed or incompatible rows. Snapshot/restore retains pending and
terminal records. Acknowledgement deletes a terminal result only while
advancing its durable reject watermark or gap set in the same transaction.

## Evidence and remaining integration work

The deterministic backend tests inject a lost publication reply, a delayed old
commit, duplicate publication, cancellation, session fencing, restart, and
snapshot restore. The shard coordinator tests cover strict secondary errors,
missing participants, and empty decisions; the RPC tests cover leader-routed
exact-version proofs and older-server failure. M3 must provide typed filesystem
planners, stable configured owner authorization, and mounted operation tests.
M4 must provide predecessor ordering and fsync receipts. Until those gates pass,
strict admissions are not enabled for FUSE requests.

Receipt rows are pinned to the group that applied their effects. M3 migration
work must retain or transfer those rows before a group can be retired; if a
participant group is unavailable, resolution stays Unknown. The finite MVCC
retention window does not replace this participant proof.
