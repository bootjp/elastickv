package etcd

import (
	"context"
	"log/slog"

	internalutil "github.com/bootjp/elastickv/internal"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type removalNotification struct {
	peer     Peer
	messages []raftpb.Message
}

// sendReadyMessages preserves Raft's final commit notification across the
// immediate transport/dispatcher teardown performed by applyRemovedPeer.
func (e *Engine) sendReadyMessages(rd etcdraft.Ready) error {
	removed, err := committedRemovalIndexes(rd.CommittedEntries)
	if err != nil {
		return err
	}
	if len(removed) == 0 || e.transport == nil {
		return e.sendMessages(messageValues(rd.Messages))
	}
	notifications := make(map[uint64]removalNotification)
	for _, msg := range messageValues(rd.Messages) {
		if peer, ok := e.removalNotificationPeer(msg, removed); ok {
			notification := notifications[msg.GetTo()]
			notification.peer = peer
			notification.messages = append(notification.messages, cloneDispatchMessage(msg))
			notifications[msg.GetTo()] = notification
			continue
		}
		if err := e.sendMessages([]raftpb.Message{msg}); err != nil {
			return err
		}
	}
	for _, notification := range notifications {
		e.startRemovalNotification(notification)
	}
	return nil
}

func (e *Engine) removalNotificationPeer(msg raftpb.Message, removed map[uint64]uint64) (Peer, bool) {
	index, removing := removed[msg.GetTo()]
	if !removing || msg.GetType() != raftpb.MsgApp || msg.GetCommit() < index || msg.GetTo() == e.nodeID {
		return Peer{}, false
	}
	peer, err := e.transport.peerFor(msg.GetTo())
	return peer, err == nil
}

func committedRemovalIndexes(entries []*raftpb.Entry) (map[uint64]uint64, error) {
	var removed map[uint64]uint64
	for _, entry := range entries {
		if entry.GetType() != raftpb.EntryConfChange && entry.GetType() != raftpb.EntryConfChangeV2 {
			continue
		}
		if removed == nil {
			removed = make(map[uint64]uint64)
		}
		if err := recordCommittedRemovals(entry, removed); err != nil {
			return nil, err
		}
	}
	return removed, nil
}

func recordCommittedRemovals(entry *raftpb.Entry, removed map[uint64]uint64) error {
	if entry.GetType() == raftpb.EntryConfChange {
		var change raftpb.ConfChange
		if err := proto.Unmarshal(entry.GetData(), &change); err != nil {
			return errors.Wrap(err, "decode committed peer removal")
		}
		if change.GetType() == raftpb.ConfChangeRemoveNode {
			removed[change.GetNodeId()] = entry.GetIndex()
		}
		return nil
	}
	var change raftpb.ConfChangeV2
	if err := proto.Unmarshal(entry.GetData(), &change); err != nil {
		return errors.Wrap(err, "decode committed peer removals")
	}
	for _, single := range change.GetChanges() {
		if single.GetType() == raftpb.ConfChangeRemoveNode {
			removed[single.GetNodeId()] = entry.GetIndex()
		}
	}
	return nil
}

func (e *Engine) startRemovalNotification(notification removalNotification) {
	ctx := e.dispatchCtx
	if ctx == nil {
		ctx = context.Background()
	}
	e.dispatchWG.Add(1)
	go func() {
		defer e.dispatchWG.Done()
		if err := dispatchRemovalNotification(ctx, notification); err != nil {
			slog.Warn("etcd raft removal notification failed", "node_id", e.nodeID,
				"to", notification.peer.NodeID, "err", err)
		}
	}()
}

// This connection deliberately bypasses the transport cache: removing or
// replacing a peer must not cancel the final notification, and its cleanup
// must never close a replacement peer's connection. Unary Send acknowledges
// receiver admission, unlike the buffered SendStream send operation.
func dispatchRemovalNotification(ctx context.Context, notification removalNotification) error {
	ctx, cancel := context.WithTimeout(ctx, defaultDispatchTimeout)
	defer cancel()
	conn, err := grpcNewClient(notification.peer.Address, internalutil.GRPCDialOptions()...)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			slog.Warn("etcd raft removal notification connection close failed", "err", err)
		}
	}()
	client := pb.NewEtcdRaftClient(conn)
	for _, msg := range notification.messages {
		raw, err := proto.Marshal(&msg)
		if err != nil {
			return errors.WithStack(err)
		}
		if _, err := client.Send(ctx, &pb.EtcdRaftMessage{Message: raw}, grpc.WaitForReady(true)); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}
