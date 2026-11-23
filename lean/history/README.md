# Checking adapter operations across process crashes

`make lean-check` checks the general theorems. To verify the calls in a specific hosted Jepsen kill run, download its workload artifact and pass its archive to the checker. The command requires the project's Lean toolchain, Python 3.9 or later, Java and the Jepsen Leiningen dependencies.

```sh
bash lean/history/check.sh grpc-raw /path/to/jepsen-grpc-raw-kill/store.tar.gz
```

If the archive contains several histories, the checker lists their member paths. Repeat the command with the chosen full member path as its third argument.

Keep the hosted artifact's `parameters.txt` beside `store.tar.gz`. The checker matches the selected history to the workload and fault. Both S3 modes use the same history name, so their checks require the parameters file to identify the adapter.

| Workload | Sequential specification used for the recorded operations |
| --- | --- |
| gRPC RawKV and TransactionalKV | One current-value register per key for timestamp-free Put/Get. All keys share a single real-time order. |
| Redis | Atomic list append/read transactions, including reads of a preceding append in the same transaction. |
| DynamoDB | The list-append workload's point and transactional item updates and reads, with one order across all items. |
| S3, including blob offload | PUT replaces one object’s contents and GET returns its latest value. Both storage modes use the same register model. |
| Filesystem overwrite and rename | Whole fixed-size workload writes and atomic rename publication are observed as register updates. Mounted readers must observe a complete value. The initial file contains −1. |
| SQS Standard and high-throughput FIFO | Per-message send, delivery, redelivery and confirmed deletion. Empty receive is permitted. The Standard drain requires every acknowledged send to be received and deleted. FIFO additionally checks raw delivery order within each group against the observed send intervals. |

For S3 offload coverage, use the same hosted artifact as the history. Check that `parameters.txt` records `adapter=s3-offload`, that `exit-code.txt` contains zero, and that the `n1.prom`, `n2.prom` and `n3.prom` files collectively contain a positive `elastickv_s3_object_duration_seconds_count` with `mode="offload"` and `outcome="success"` for each of `operation="put"` and `operation="get"`. The hosted runner checks these counters separately from the Lean register certificate.

The exporter uses Clojure's EDN reader. The Python program pairs every client invocation and response and checks the fault and final-read evidence. It proposes a completion and order, preserving micro-operations within transactions. For append workloads the final lists help find the order. Lean independently executes the entire proposed order from the specified initial state, checks every returned read, and places each operation within its real-time interval. Every successful call must appear, and each failed call must be omitted. A call with an indeterminate response may take effect after its caller timed out. Known component results from partially completed queue receives remain mandatory. A failed witness search is inconclusive until its constraints and the original EDN are inspected.

`verified_linearization`, `selected_perm` and `respects_real_time` establish the sequential and real-time claims for the normalized history. `recovery_of_verified_execution` establishes that replaying whole committed operations from any complete prefix checkpoint reconstructs the same state. This replay theorem assumes an intact committed suffix and a checkpoint consistent with its prefix. The actual kill workloads exercise process restarts with persistent volumes and a surviving quorum. Permanently losing the quorum or power-failing every disk requires a different fault model.

`BlobDurability.lean` also gives a deployment-conditional guarantee for offloaded PUT. When each planned member produces one push result, all required copies acknowledge, and distinct endpoints identify distinct persistent stores, losing any one endpoint leaves a copy. The Go regression test exercises duplicate IDs and duplicate address strings through HTTP PUT, including the absence of published metadata after rejection. Different addresses that alias one physical store remain a deployment assumption.

A successful certificate is specific to its recorded history and operation specification. The EDN-to-model projection, Raft implementation, filesystem sync semantics, and the correspondence between durable media and logical checkpoints remain trust boundaries. Historical RawGet explicitly selects an earlier snapshot. Redis Pub/Sub subscriptions are process-local. The [adapter transition proofs](../adapter/README.md#expiry-script-upload-and-receipt-transitions) cover relative TTL, normalized Lua string traces, multipart publication and SQS receipt visibility under their stated assumptions. Other Redis commands and arbitrary Lua programs require further refinement proofs.
