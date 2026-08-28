# Design: gRPC Streaming Transport for Raft Messages

Status: Implemented
Author: bootjp
Date: 2026-04-18

The per-peer dispatch channel foundation from PR #522 remains in place.
Regular Raft messages now use a long-lived client-streaming gRPC connection
per peer when the remote node supports `SendStream`, with unary `Send`
retained as the mixed-version fallback.

## 1. Background and motivation

### Problem

Before this implementation, the Raft transport used a unary gRPC RPC (`Send`)
for every outbound message:

```text
dispatchRegular:
  msg.Marshal() → client.Send(ctx, req) → wait ACK → return
```

Each call pays a full network round-trip before the next message can be sent.
With `defaultMaxInflightMsg = 256`, the leader can generate up to 256 MsgApp
messages per follower before receiving ACK. A single dispatch worker serialises
those 256 sends: at 1 ms RTT, throughput is capped at ~1 000 msg/s per peer
regardless of bandwidth.

PR #522 introduced per-peer dispatch channels and two dispatch workers per peer
(one goroutine for normal messages, one dedicated to heartbeats) to eliminate
cross-peer head-of-line blocking. The remaining bottleneck is the
per-message RTT of unary gRPC.

### Goal

Replace per-message unary RPCs with a **long-lived client-streaming gRPC stream per peer**
so that the sender does not block waiting for an individual ACK between messages.
Expected outcome: throughput per peer scales with available bandwidth rather than
being RTT-limited.

---

## 2. Previous architecture

```text
Engine run loop
  └─ sendMessages()
       └─ enqueueDispatchMessage()
            ├─ regular messages   → peerDispatchers[nodeID].normal    (chan, cap MaxInflightMsg)
            │                           ↓
            │                    runDispatchWorker (1 goroutine/peer)
            │                           ↓
            │                    GRPCTransport.dispatchRegular()
            │                           ↓
            │                    client.Send(ctx, EtcdRaftMessage)   ← unary, blocks on RTT
            │                           ↓
            │                    receive EtcdRaftAck
            │
            └─ heartbeat messages → peerDispatchers[nodeID].heartbeat (chan, cap MaxInflightMsg)
                                        ↓
                                 runDispatchWorker (1 goroutine/peer, dedicated)
                                        ↓
                                 GRPCTransport.dispatchRegular()
```

Snapshot messages already used client streaming (`SendSnapshot`); regular
messages now use `SendStream` when the peer supports it.

---

## 3. Implemented design

### 3.1 Protocol change

`etcd_raft.proto` adds a client-streaming RPC:

```protobuf
service EtcdRaft {
  rpc Send(EtcdRaftMessage)              returns (EtcdRaftAck) {}        // kept for compat
  rpc SendStream(stream EtcdRaftMessage) returns (EtcdRaftAck) {}        // new: client-streaming
  rpc SendSnapshot(stream EtcdRaftSnapshotChunk) returns (EtcdRaftAck) {}
}
```

`SendStream` is **client-streaming**: the client sends a sequence of messages and
the server replies with a single `EtcdRaftAck` when the stream closes or on error.
Per-message ACKs are intentionally omitted: Raft's own MsgAppResp / MsgHeartbeatResp
handle application-level acknowledgement; the transport only needs delivery
confirmation at stream granularity.

### 3.2 Sender side (`GRPCTransport`)

`GRPCTransport` keeps a stream cache protected by `mu`:

```go
type peerStream struct {
    mu     sync.Mutex
    stream pb.EtcdRaft_SendStreamClient
    cancel context.CancelFunc
    done   chan struct{}
}

type GRPCTransport struct {
    // existing fields ...
    streams             map[string]*peerStream // peer address -> active stream
    streamSupported     map[string]bool
    streamUnsupported   map[string]bool
    streamUnsupportedAt map[string]time.Time
}
```

`streamFor(address, client)` returns the existing stream or opens a new one.
Stream establishment is serialized with singleflight per peer address so
concurrent dispatch workers do not open duplicate streams during first contact
or reconnect.

On error (network drop, server restart), a reader goroutine observes the
terminal stream result, the stream is torn down, and the next send attempt
reopens it. `RemovePeer`, address replacement, and
`GRPCTransport.Close` cancel the stream context and delete the stream entry.
If an in-flight dispatch context is cancelled or times out while `Send` is
blocked in gRPC flow control, the dispatch closes the cached stream to unblock
the sender.

`dispatchRegular` changes from:

```go
_, err = client.Send(ctx, &pb.EtcdRaftMessage{Message: raw})
```

to:

```go
stream, err := t.streamFor(ctx, peer.Address, client)
if err != nil { return err }
err = stream.Send(&pb.EtcdRaftMessage{Message: raw})
```

`stream.Send()` returns as soon as the message enters the gRPC send buffer
(non-blocking under normal conditions). Priority control traffic such as
heartbeats and votes keeps using unary `Send` so it does not wait behind a
large replication send on the stream mutex.

> **Concurrency constraint**: gRPC-go's `stream.Send` / `SendMsg` is **not**
> goroutine-safe. Each per-peer stream must be written by exactly one goroutine.
>
> PR #522 runs **two** dispatch workers per peer (one for normal messages, one
> for heartbeats in the default layout; four lanes when
> `ELASTICKV_RAFT_DISPATCHER_LANES=1` is enabled). The implementation preserves
> that worker model and uses a per-stream `sync.Mutex` to satisfy gRPC's
> single-writer requirement. This keeps the change scoped to the transport layer
> and avoids reshaping the dispatcher scheduler in the same rollout.

### 3.3 Receiver side (`GRPCTransport`)

Add a `SendStream` server handler:

```go
func (t *GRPCTransport) SendStream(stream pb.EtcdRaft_SendStreamServer) error {
    for {
        req, err := stream.Recv()
        if err == io.EOF {
            return stream.SendAndClose(&pb.EtcdRaftAck{})
        }
        if err != nil {
            return err
        }
        var msg raftpb.Message
        if err := msg.Unmarshal(req.Message); err != nil {
            return err
        }
        if err := t.handle(stream.Context(), msg); err != nil {
            return err
        }
    }
}
```

### 3.4 Stream lifecycle

| Event | Action |
|---|---|
| First message to peer | Probe `SendStream`, open stream, store in `streams[address]` |
| `stream.Send()` error | Close and delete stream; caller retries via `streamFor` |
| `removePeer(nodeID)` / peer address replacement | Cancel stream context, delete from `streams` |
| `GRPCTransport.Close()` | Cancel all stream contexts, close all streams |
| Peer restart / network partition | Stream monitor observes terminal error → cached stream closes → reconnect on next stream send |
| Dispatch context cancelled while `Send` is blocked | Close cached stream to unblock the dispatch worker |

### 3.5 Backward compatibility

Nodes that have not yet upgraded only register `Send()`. The sender opens an
empty `SendStream` probe and calls `CloseAndRecv()` before caching support.
If the peer returns `codes.Unimplemented`, the address is marked unsupported
and `dispatchRegular` falls back to the existing unary `Send()` path. A
successful probe marks the address supported and subsequent non-priority sends
reuse a long-lived stream. Unsupported markers expire after a short reprobe
interval so rolling upgrades can move from unary fallback to `SendStream`
without waiting for a membership change or process restart.

---

## 4. Trade-offs and risks

| Factor | Notes |
|---|---|
| **Throughput** | Removes per-message RTT; limited by bandwidth and gRPC flow control window |
| **Latency** | Messages reach the receiver faster (no ACK wait) |
| **Head-of-line blocking** | Still present within a single stream; mitigated by per-peer isolation from PR #522 |
| **Stream reconnect gap** | During reconnect, messages are dropped and Raft retransmits — identical to current drop behaviour |
| **Memory** | gRPC stream send buffer (typically 32 KB) replaces per-peer channel as primary buffer; channel can be reduced |
| **Complexity** | Stream state machine in `GRPCTransport`; backward-compat fallback path |
| **Ordering** | Single stream per peer preserves FIFO — safe for Raft |
| **Heartbeat starvation** | Priority control traffic stays on unary `Send`, preserving the dedicated heartbeat lane through the transport boundary. |

### 3.6 Heartbeat handling

The implementation preserves the existing dispatcher lanes. Heartbeats and
other priority control messages still have a dedicated channel and worker
before entering the transport, and they use unary `Send` at the transport
boundary. Normal replication traffic uses the cached stream and `peerStream.mu`
serializes `stream.Send` calls. If the stream breaks, the transport closes the
cached stream and lets Raft's built-in retransmission recover any message lost
during the reconnect window, matching the previous drop semantics.

---

## 5. Implementation status

| Step | Scope | Status |
|---|---|---|
| 1. Proto: add `SendStream` RPC | `proto/etcd_raft.proto` + regenerated Go stubs | Implemented |
| 2. Receiver handler | `GRPCTransport.SendStream` | Implemented |
| 3. Stream open/close/reconnect | `streamFor`, `peerStream` lifecycle, peer removal cleanup | Implemented |
| 4. Sender: swap unary → stream in `dispatchRegular` | Streaming default with unary fallback | Implemented |
| 5. Operational kill switch | `ELASTICKV_RAFT_SEND_STREAM=false` forces unary `Send` without unregistering `SendStream` | Implemented |
| 6. Backward-compat fallback | Empty-stream probe plus `codes.Unimplemented` cache | Implemented |
| 7. Tests | Server handler, streaming dispatch, legacy-peer fallback, kill switch | Implemented |
| 8. Multi-group soak evidence | Concurrent TCP groups, disconnect/reconnect, flow-control timeout, recovery, snapshot traffic, and a fail-closed verifier | Implemented |

---

## 6. Rollout and migration strategy

Because `SendStream` is additive, the rollout is zero-downtime by design:

| Phase | Action |
|---|---|
| **Deploy new server binary** | All nodes register both `Send` (unary) and `SendStream`. Existing peers that have not yet upgraded continue to use `Send` — no behaviour change. |
| **Gradual upgrade** | As each peer is upgraded, the sender's empty-stream probe detects `SendStream` availability and opens a stream. Unary `Send` remains the fallback for un-upgraded peers. |
| **Full cutover** | Once all peers run the new binary, every peer-to-peer path uses streaming. The unary `Send` handler is kept indefinitely for backward compatibility. |
| **Emergency rollback** | Set `ELASTICKV_RAFT_SEND_STREAM=false` on every node and restart. The node still serves `SendStream` for upgraded peers, but its own outbound regular Raft messages use unary `Send`. |

No dual-write or blue/green deployment is required: the `codes.Unimplemented` probe
(§3.5) makes the switch per-peer and per-connection atomically.

---

## 7. Out of scope

- Batch encoding (multiple `EtcdRaftMessage` payloads in one proto message) — independent optimisation.
- Replacing the current per-lane workers with one biased-select multiplexing worker — an optional scheduler optimization on top of this transport protocol.
- Snapshot protocol changes — already implemented. The soak below exercises the existing snapshot stream without changing its wire format.

---

## 8. Multi-node, multi-group soak evidence

### 8.1 Deterministic transport soak

`cmd/elastickv-raft-stream-soak` starts real loopback TCP gRPC servers rather
than an in-memory mock. Each configured Raft group has its own three-node
transport mesh. All groups run concurrently through these phases:

1. every directed peer link sends regular `MsgApp` traffic through `SendStream`;
2. one receiver per group is stopped, the sender observes the broken stream,
   the receiver restarts on the same address, and a recovery sentinel arrives;
3. another receiver stops consuming a 1 MiB stream payload until gRPC flow
   control blocks the sender and its dispatch context expires, then a second
   recovery sentinel arrives;
4. every node sends one existing-format `MsgSnap` to its next peer.

The schema-v2 verifier fails unless every group has at least three nodes, every
directed sender-to-receiver link delivered its configured steady message count,
a stream reopened, both fault classes were observed, both uniquely indexed
recovery sentinels reached the receiver handler, and per-node snapshot
send/receive counts and bytes agree with the configured minimum. Re-run the
checked-in evidence exactly with:

```bash
mkdir -p .cache/go .cache/tmp docs/evidence
GOCACHE="$(pwd)/.cache/go" GOTMPDIR="$(pwd)/.cache/tmp" \
  go run ./cmd/elastickv-raft-stream-soak \
    --groups 3 \
    --nodes 3 \
    --messages-per-link 8 \
    --regular-payload-bytes 1024 \
    --backpressure-payload-bytes 1048576 \
    --snapshot-payload-bytes 65536 \
    --timeout 90s \
    --output docs/evidence/raft_streaming_multigroup_soak.json

GOCACHE="$(pwd)/.cache/go" GOTMPDIR="$(pwd)/.cache/tmp" \
  go run ./cmd/elastickv-raft-stream-soak \
    --verify docs/evidence/raft_streaming_multigroup_soak.json
```

The committed evidence records a passing `3 groups x 3 nodes` run. Each group
opened all six directed peer streams, recorded at least one successful reopen,
observed both disconnect and backpressure failures, delivered two recovery
sentinels, and acknowledged three 64 KiB snapshot streams. Per-sender receive
counts, TCP ports, and ordered UTC timestamps are retained in the JSON so the
artifact describes and verifies the actual run.

Focused regression tests:

```bash
GOCACHE="$(pwd)/.cache/go" GOTMPDIR="$(pwd)/.cache/tmp" \
  go test ./internal/raftengine/etcd/transportsoak \
          ./internal/raftengine/etcd \
          ./monitoring \
          ./cmd/elastickv-raft-stream-soak -count=1
```

### 8.2 Full Raft/Jepsen soak

The deterministic command isolates transport lifecycle behavior. The existing
DynamoDB multi-table Jepsen workload remains the full Raft correctness gate.
On a standard Jepsen control host with `n1` through `n5` reachable over SSH,
run:

```bash
TIME_LIMIT=600 RATE=50 CONCURRENCY=30 FAULT_INTERVAL=15 SNAPSHOT_COUNT=64 \
  ./scripts/run-jepsen-raft-streaming-multigroup-soak.sh
```

The script deploys five processes, each hosting groups `1`, `2`, and `3`, and
routes the four workload tables across all three groups. It enables streaming
and dispatcher lanes, lowers the snapshot threshold, and applies partition,
kill, and pause faults. The combined nemesis final generator heals outstanding
faults before teardown. Jepsen's `LogFiles` collection preserves each server
log and `elastickv-transport-metrics.prom`. The metrics file appends one final
counter snapshot for each process lifetime before a kill, plus the final
process snapshot collected by Jepsen. This preserves evidence when counters
reset after a kill/restart cycle instead of overwriting the previous lifetime.

The script then runs
`scripts/verify-raft-streaming-multigroup-metrics.sh` over the newly collected
node snapshots. It rejects malformed values, missing identity labels, duplicate
node IDs or addresses, and groups without stream activity from at least three
distinct nodes. For every expected group it also requires non-zero stream opens,
successful reconnects, streamed messages, snapshot sends and bytes, and at
least one backpressure signal (`DeadlineExceeded`, `ResourceExhausted`, a
dispatcher drop, or a full inbound step queue). The Jepsen history must also be
valid; the workload process exits non-zero before metrics verification when
Elle reports an anomaly.

The following Prometheus counters make the evidence independent of log wording:

| Metric | Meaning |
|---|---|
| `elastickv_raft_send_stream_opens_total` | Successful outbound stream opens, including reopens |
| `elastickv_raft_send_stream_reconnects_total` | Successful opens for an address that previously had a stream |
| `elastickv_raft_send_stream_messages_total` | Regular Raft messages accepted by the stream send path |
| `elastickv_raft_snapshot_stream_sends_total` | Snapshot streams acknowledged by the peer |
| `elastickv_raft_snapshot_stream_payload_bytes_total` | Payload bytes in acknowledged snapshot streams |

These counters are observational. They are updated only after successful
transport operations and are never read by dispatch, fallback, retry, or Raft
state-machine decisions, so this evidence work does not alter protocol
semantics.
