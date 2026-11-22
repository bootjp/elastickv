# Five review passes for the fixed Sync correction

Reviewed source: d0ee76e0a1268d9af39e64560ba3e78814e2fb06 against
18a509addeef3f10cf36c7751f26e87a4887b477. This is a self-review, not a claim
of independent external approval. The final measurement remains separate from
the parallel-append-only intermediate and instrumented diagnostics.

1. **Data loss.** Followed Ready through dispatchParallelAppend, persistReady,
   applyAndDispatchReady and Advance. The original Ready and durability work
   are unchanged. Early dispatch only clones MsgApp on an unprotected legacy
   leader with no new HardState/snapshot/committed/configuration entry. Save
   still blocks the owner before processing follower replies, FSM application,
   read/proposal completion or Advance. A held actual Save and injected Save
   failure prohibit early ACK/applied publication. Queue sharing uses the
   existing TransactionManager apply/results path; no Sync option changes.
2. **Concurrency and distributed failure.** Traced the borrowed port from the
   server factory through the group-owned LeaderProxy/lease wrapper, matching
   migration group and configured manager. It cannot Close the group manager.
   Dynamic payload wrapping remains late-bound. Native owned requests retain
   the original receiver context and bypass cross-request batching as before.
   Cancellation and unknown errors do not create retries or noncommit proof.
   Fallback remains for missing/custom/unbound transactions. Existing raw queue
   locking, bounded pending items, whole caller batches and result offsets are
   reused, with focused race coverage of the production factory and KV ports.
3. **Performance.** Profiles/stacks established two independent flushers per
   engine; kernel and queue counters show synchronous I/O waits under load.
   Sharing removes the avoidable batch fragmentation. No new gather delay,
   batch limit, goroutine, global registry, mount change or weaker durability
   is introduced. The first parallel-append-only ABBA did not meet the goal;
   the final fixed candidate receives a fresh, profile-free ABBA. All offered
   drops, actual elapsed denominators, full-stage and common-window results
   must be reported, including regressions and shared physical disk limits.
4. **Consistency.** Internal.Forward still checks leadership/phase, stamps
   timestamps and timestamp admission before Commit. The borrowed port skips
   only this already-completed timestamp preparation, preserving the receiver
   epoch rather than clearing/restamping it. Local Commit retains preparation.
   The common commit path retains absence guards, sequential transaction phases,
   final migration revalidation, per-request verdicts and original ambiguity
   rules. ReadIndex, MVCC publication guards, HLC, ownership/routing and snapshot
   logic are unchanged. Tests explicitly check receiver epochs and local clones.
5. **Coverage.** The real factory test is RED for separate queues and GREEN for
   shared proposals, with no ACK before durable completion. Related tests cover
   timestamp preparation, dynamic encryption wrapping, owner lifetime, fallback,
   original owned context and terminal Unknown. Real WAL gate/failure, clone,
   exactly-once dispatch and all eligibility exclusions cover the retained
   parallel append. Full native Go, focused races, full lint, root Jepsen and
   real three-node Raw/transactional histories are recorded separately. Final
   ABBA must check every ACK at all three ingresses and five post-Done paths.

No remaining blocking issue was found in these scoped passes. The measured
physical disk is shared and two runs per variant do not establish statistical
significance or a failure-free availability guarantee.
