# Jepsen write-skew coverage

On November 21, 2026, the DynamoDB fixture produced a serializable explanation for the recorded operations. Two of the three contention scenarios produced the required witness. The two-table, single-group scenario returned HTTP 500 even for uncontended writes. The overall Jepsen result was `:unknown` and the runner exited with status 1.

## Workload and results

The server was built from `d3ba4b8f4`. One process hosted two single-member Raft groups with the M5 table split, encryption off and no nemesis. For each round, two clients used atomic TransactGetItems to read the same two absent keys. A barrier released their conditional writes only after both pre-reads returned. Each client read the other client's key and appended a unique marker to its own. A successful race requires one acknowledged append, one predicate-specific rejection and an atomic read showing only the winner. Two successive uncontended transactions and a final atomic read check that ordinary writes can still make progress.

| Keys | Routes observed in the catalog | Race and first readback | Uncontended controls and final readback |
| --- | --- | --- | --- |
| 0, 8 in table 1 | 1, 1 | 102 won, 101 was rejected. Readback was `[nil [102]]`. | Both committed. Final readback was `[[111] [102 112]]`. |
| 4, 2 in tables 1 and 3 | 1, 2 | 201 won, 202 was rejected. Readback was `[[201] nil]`. | Both committed. Final readback was `[[201 211] [212]]`. |
| 12, 13 in tables 1 and 2 | 1, 1 | Both writes returned `InternalServerError`. Readback was `[nil nil]`. | Both writes again returned `InternalServerError`. Final readback was `[nil nil]`. |

The successful response and subsequent readback identify the committed client operation. The following entries in the stopped server's committed WAL establish its protocol path. Indexes are local to each group.

| Observation | Transaction start timestamp | Group 1 | Group 2 |
| --- | ---: | --- | --- |
| Race winner 102 | 117653485706870784 | `NONE` at 12 with item value `[102]` (1PC) | No item mutation |
| Race winner 201 | 117653485706870791 | `PREPARE` at 19 with `[201]`, `COMMIT` at 20 | No item mutation |
| Last serial control, marker 212 | 117653485706870796 | `PREPARE` at 26 with `[201 211]`, `COMMIT` at 27 | `PREPARE` at 15 with `[212]`, `COMMIT` at 16 |

The last serial control witnesses two durable write participants at the same start timestamp. The race winner with marker 201 has just one participant with item mutations in the WAL dump. Group ownership of a condition key alone cannot establish that the group committed a write.

A separate diagnostic build logged the underlying error for the two-table case. All four HTTP 500 responses traced to `beginTxnWriteTimestamp` exhausting its retry deadline while establishing the snapshot seal. The same failure occurred on the ordinary server build. The successful reads and indeterminate writes permit a consistent history, while the missing commit and predicate rejection prevent this round from certifying coverage. This run covers normal operation. Recovery between prepare and commit, network partitions and transactional range predicates require separate histories.

## Reproduction

Run from a checkout with the Go and Jepsen dependencies installed. The M5 script starts and stops its dedicated fixture. It stores Jepsen output under `jepsen/store/elastickv-dynamodb-write-skew/` and the server data under `/tmp/elastickv-m5-test-run/`.

```sh
./scripts/run-jepsen-m5-local.sh --write-skew
go run ./jepsen/tools/write-skew-wal /tmp/elastickv-m5-test-run/n1/n1
cd jepsen
lein run -m elastickv.elle-checker write-skew \
  store/elastickv-dynamodb-write-skew/latest/history.edn target/write-skew-replay
```

A fresh run can elect the opposite winner. Match the returned token and its readback to the first corresponding item value in its own WAL, then join phases on the transaction start timestamp. The WAL reader expects an uncompacted, unencrypted fixture starting at its bootstrap snapshot. Jepsen's offline checker uses the saved calls, replies, snapshots and route observations to report the consistency and coverage verdicts.
