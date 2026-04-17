# Design: gRPC Streaming Transport for Raft Messages

> **Status: proposed — not yet implemented.**
> PR #522 delivers the per-peer dispatch channel foundation described in §2.
> This document specifies the next step: replacing per-message unary RPCs with
> a long-lived client-streaming gRPC connection per peer.

## 1. Background and motivation

### Problem

The current Raft transport uses a unary gRPC RPC (`Send`) for every outbound message:

```text
dispatchRegular:
  msg.Marshal() → client.Send(ctx, req) → wait ACK → return
```

Each call pays a full network round-trip before the next message can be sent.
With `defaultMaxInflightMsg = 256`, the leader can generate up to 256 MsgApp
messages per follower before receiving ACK. A single dispatch worker serialises
those 256 sends: at 1 ms RTT, throughput is capped at ~1 000 msg/s per peer
regardless of bandwidth.

PR #522 introduced per-peer dispatch channels and `defaultDispatchWorkersPerPeer = 2`
(one goroutine for normal messages, one dedicated to heartbeats) to eliminate
cross-peer head-of-line blocking. The remaining bottleneck is the
per-message RTT of unary gRPC.

### Goal

Replace per-message unary RPCs with a **long-lived client-streaming gRPC stream per peer**
so that the sender does not block waiting for an individual ACK between messages.
Expected outcome: throughput per peer scales with available bandwidth rather than
being RTT-limited.

---

## 2. Current architecture

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

Snapshot messages already use client streaming (`SendSnapshot`); regular messages do not.

---

## 3. Proposed design

### 3.1 Protocol change

Add a client-streaming RPC to `etcd_raft.proto`:

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

Add a `peerStream` map protected by `mu`:

```go
type peerStream struct {
    stream pb.EtcdRaft_SendStreamClient
    cancel context.CancelFunc
}

type GRPCTransport struct {
    // existing fields ...
    streams map[uint64]*peerStream   // nodeID → active stream
}
```

`getOrOpenStream(nodeID)` returns the existing stream or opens a new one.
On error (network drop, server restart), the stream is torn down and a new
one is opened on the next send attempt.

`dispatchRegular` changes from:

```go
_, err = client.Send(ctx, &pb.EtcdRaftMessage{Message: raw})
```

to:

```go
stream, err := t.getOrOpenStream(msg.To)
if err != nil { return err }
err = stream.Send(&pb.EtcdRaftMessage{Message: raw})
```

`stream.Send()` returns as soon as the message enters the gRPC send buffer
(non-blocking under normal conditions). The dispatch worker can immediately
pick up the next message from the per-peer channel.

> **Concurrency constraint**: gRPC-go's `stream.Send` / `SendMsg` is **not**
> goroutine-safe. Each per-peer stream must be written by exactly one goroutine.
>
> PR #522 runs **two** dispatch workers per peer (one for normal messages, one for
> heartbeats). When streaming is introduced, these two workers cannot share a
> single `SendStream` stream without external synchronization. Two approaches:
>
> **Option A — single multiplexing worker**: merge both channels into one goroutine
> that reads from either channel via `select` and writes to the stream. Preserves
> stream ownership with no locking.
>
> **Option B — stream-level mutex**: keep the two-worker model and protect
> `stream.Send` with a `sync.Mutex` per peer. Simpler to implement but adds
> a lock on the hot path.
>
> Option A is preferred: it eliminates the lock on the hot path, and within-peer
> ordering is already guaranteed by the single multiplexing worker. `getOrOpenStream`
> uses `mu` only to serialize map access; the per-peer stream itself is then written
> by exactly one goroutine (the multiplexing worker), satisfying gRPC's
> single-writer requirement.

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
| First message to peer | Open stream, store in `streams[nodeID]` |
| `stream.Send()` error | Close and delete stream; caller retries via `getOrOpenStream` |
| `removePeer(nodeID)` | Cancel stream context, delete from `streams` |
| `GRPCTransport.Close()` | Cancel all stream contexts, close all streams |
| Peer restart / network partition | Server closes stream → sender gets error on next `Send()` → reconnect |

### 3.5 Backward compatibility

Nodes that have not yet upgraded only register `Send()`. The sender detects
`codes.Unimplemented` on `SendStream` and falls back to the existing unary
`Send()` path. A version negotiation flag (e.g., a field in peer metadata) can
be used to skip the probe after the first successful stream is established.

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
| **Heartbeat starvation** | Under a burst of log entries, heartbeats could be delayed in the send buffer. Mitigated via biased-select (see below). |

### 3.6 Heartbeat starvation mitigation — biased select

When the multiplexing dispatch worker (Option A from §3.2) reads from both
channels, a sustained `MsgApp` burst can delay heartbeats. The mitigation is
a **biased select**: check the priority channel non-blocking first, fall back
to either channel:

```go
for {
    // Drain the priority channel before accepting normal messages.
    select {
    case req := <-pd.heartbeat:
        stream.Send(req)
        continue
    default:
    }
    // No priority message pending; wait on either.
    select {
    case req := <-pd.heartbeat:
        stream.Send(req)
    case req := <-pd.normal:
        stream.Send(req)
    case <-ctx.Done():
        return
    }
}
```

This guarantees heartbeats are flushed before the next log entry is written
to the stream, bounding heartbeat delay to one normal-message send time.

---

## 5. Implementation plan

| Step | Scope | Estimate |
|---|---|---|
| 1. Proto: add `SendStream` RPC | `proto/etcd_raft.proto` + regenerate | 0.5 day |
| 2. Receiver handler | `GRPCTransport.SendStream` | 0.5 day |
| 3. Stream open/close/reconnect | `getOrOpenStream`, `peerStream` lifecycle | 1 day |
| 4. Sender: swap unary → stream in `dispatchRegular` | `GRPCTransport.dispatchRegular` | 0.5 day |
| 5. Backward-compat fallback | Unimplemented detection, version flag | 1 day |
| 6. Tests | Unit + conformance | 1 day |
| **Total** | | **~4.5 days** |

---

## 6. Rollout and migration strategy

Because `SendStream` is additive, the rollout is zero-downtime by design:

| Phase | Action |
|---|---|
| **Deploy new server binary** | All nodes register both `Send` (unary) and `SendStream`. Existing peers that have not yet upgraded continue to use `Send` — no behaviour change. |
| **Gradual upgrade** | As each peer is upgraded, the sender detects `SendStream` availability (no `codes.Unimplemented`) and opens a stream. Unary `Send` remains the fallback for un-upgraded peers. |
| **Full cutover** | Once all peers run the new binary, every peer-to-peer path uses streaming. The unary `Send` handler is kept indefinitely for backward compatibility. |

No dual-write or blue/green deployment is required: the `codes.Unimplemented` probe
(§3.5) makes the switch per-peer and per-connection atomically.

---

## 7. Out of scope

- Batch encoding (multiple `EtcdRaftMessage` payloads in one proto message) — independent optimisation.
- Priority queuing for heartbeats vs log entries — orthogonal to transport protocol.
- Snapshot streaming changes — already implemented.
