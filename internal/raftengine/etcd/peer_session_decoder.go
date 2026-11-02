package etcd

import (
	"bytes"
	"slices"
	"sync"

	"github.com/bootjp/elastickv/internal/peersession"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// PeerSessionMethods builds decoders for one listener's trusted group and applied
// learner roster. Neither is present in the wire envelope. Rebuild these methods
// and revoke sessions when that projection changes. Decoders do not authenticate
// unary identities: the session owner checks their decoded routes against TLS.
func PeerSessionMethods(group uint64, learners []uint64) []peersession.Method {
	learner := make(map[uint64]bool, len(learners))
	for _, id := range learners {
		learner[id] = true
	}
	decode := func(raw []byte) (peersession.Message, error) {
		return peerDecodeRoute(group, raw, func(id uint64) bool { return learner[id] })
	}
	request := func(v any) (peersession.Message, error) {
		return peerMessageRequest(v, decode)
	}
	return []peersession.Method{
		{Name: pb.EtcdRaft_Send_FullMethodName, Request: request, Reply: peerRaftAck},
		{Name: pb.EtcdRaft_SendStream_FullMethodName, Stream: func(b peersession.Binding) (peersession.StreamDecoders, error) {
			return peerMessageStream(group, b, request)
		}},
		{Name: pb.EtcdRaft_SendSnapshot_FullMethodName, Stream: func(b peersession.Binding) (peersession.StreamDecoders, error) {
			return peerSnapshotStream(group, b, decode)
		}},
	}
}

func peerDecodeRoute(group uint64, raw []byte, learner func(uint64) bool) (peersession.Message, error) {
	msg, err := peerDecodeRaft(raw)
	if err != nil {
		return peersession.Message{}, peersession.ErrFormat
	}
	traffic, err := peerRaftTraffic(msg.GetType())
	if err != nil {
		return peersession.Message{}, err
	}
	if traffic == peersession.Replication && (learner(msg.GetFrom()) || learner(msg.GetTo())) {
		if !peerCatchupKind(msg.GetType()) {
			return peersession.Message{}, peersession.ErrFormat
		}
		traffic = peersession.Catchup
	}
	return peersession.Message{Group: group, From: msg.GetFrom(), To: msg.GetTo(), Traffic: traffic}, nil
}

// PeerSessionMethodsForOwner reads the same applied projection used by session
// generation invalidation. No learner roster survives a decode. Session handles
// still validate generation at authorization/enqueue boundaries.
func PeerSessionMethodsForOwner(group uint64, owner *peersession.Owner) []peersession.Method {
	decode := func(v any) (peersession.Message, error) {
		g, err := peerAppliedProjection(owner, group)
		if err != nil {
			return peersession.Message{}, err
		}
		learners := peerProjectionLearners(g)
		return peerMessageRequest(v, func(raw []byte) (peersession.Message, error) {
			return peerDecodeRoute(group, raw, func(id uint64) bool { return slices.Contains(learners, id) })
		})
	}
	ack := func(req peersession.Message, v any) (peersession.Message, error) {
		m, err := peerRaftAck(req, v)
		if err != nil {
			return peersession.Message{}, err
		}
		return peerRefreshTraffic(owner, group, m)
	}
	return []peersession.Method{
		{Name: pb.EtcdRaft_Send_FullMethodName, Request: decode, Reply: ack},
		{Name: pb.EtcdRaft_SendStream_FullMethodName, Stream: func(b peersession.Binding) (peersession.StreamDecoders, error) {
			s, err := peerMessageStream(group, b, decode)
			return peerDynamicStream(owner, group, s), err
		}},
		{Name: pb.EtcdRaft_SendSnapshot_FullMethodName, Stream: func(b peersession.Binding) (peersession.StreamDecoders, error) {
			s, err := peerSnapshotStream(group, b, func(raw []byte) (peersession.Message, error) {
				return decode(&pb.EtcdRaftMessage{Message: raw})
			})
			return peerDynamicStream(owner, group, s), err
		}},
	}
}

func peerAppliedProjection(owner *peersession.Owner, group uint64) (peersession.Group, error) {
	if owner == nil {
		return peersession.Group{}, peersession.ErrDenied
	}
	g, ok := owner.GroupProjection(group)
	if !ok || g.Pending != peersession.PendingClear {
		return peersession.Group{}, peersession.ErrDenied
	}
	return g, nil
}

func peerProjectionLearners(g peersession.Group) []uint64 {
	var learners []uint64
	s := g.Configuration.State
	for _, id := range s.Learners {
		if !slices.Contains(s.Voters, id) && !slices.Contains(s.VotersOutgoing, id) && !slices.Contains(s.LearnersNext, id) {
			learners = append(learners, id)
		}
	}
	return learners
}

func peerRefreshTraffic(owner *peersession.Owner, group uint64, m peersession.Message) (peersession.Message, error) {
	g, err := peerAppliedProjection(owner, group)
	if err != nil {
		return peersession.Message{}, err
	}
	if m.Traffic == peersession.Vote {
		return m, nil
	}
	m.Traffic = peersession.Replication
	learners := peerProjectionLearners(g)
	if slices.Contains(learners, m.From) || slices.Contains(learners, m.To) {
		m.Traffic = peersession.Catchup
	}
	return m, nil
}

func peerDynamicStream(owner *peersession.Owner, group uint64, s peersession.StreamDecoders) peersession.StreamDecoders {
	return peersession.StreamDecoders{Request: func(v any) (peersession.Message, error) {
		// Check before mutating per-stream parsing state as well as afterwards.
		if _, err := peerAppliedProjection(owner, group); err != nil {
			return peersession.Message{}, err
		}
		m, err := s.Request(v)
		if err != nil {
			return peersession.Message{}, errors.WithStack(err)
		}
		return peerRefreshTraffic(owner, group, m)
	}, Reply: func(req peersession.Message, v any) (peersession.Message, error) {
		if _, err := peerAppliedProjection(owner, group); err != nil {
			return peersession.Message{}, err
		}
		m, err := s.Reply(req, v)
		if err != nil {
			return peersession.Message{}, errors.WithStack(err)
		}
		return peerRefreshTraffic(owner, group, m)
	}}
}

func peerMessageRequest(v any, decode func([]byte) (peersession.Message, error)) (peersession.Message, error) {
	msg, ok := v.(*pb.EtcdRaftMessage)
	if !ok || msg == nil || !peerKnownFields(msg.ProtoReflect()) {
		return peersession.Message{}, peersession.ErrFormat
	}
	return decode(msg.Message)
}

func peerDecodeRaft(raw []byte) (*raftpb.Message, error) {
	var msg raftpb.Message
	if len(raw) == 0 || proto.Unmarshal(raw, &msg) != nil || !peerKnownFields(msg.ProtoReflect()) {
		return nil, peersession.ErrFormat
	}
	canonical, err := proto.Marshal(&msg)
	if err != nil || !bytes.Equal(raw, canonical) || msg.Type == nil || !peerValidIDs(msg.GetFrom(), msg.GetTo()) {
		return nil, peersession.ErrFormat
	}
	if !peerValidSnapshot(&msg) {
		return nil, peersession.ErrFormat
	}
	return &msg, nil
}

func peerValidIDs(from, to uint64) bool { return from != 0 && to != 0 && from != to }

func peerValidSnapshot(msg *raftpb.Message) bool {
	if msg.GetType() != raftpb.MsgSnap {
		return msg.Snapshot == nil
	}
	return msg.Snapshot != nil && msg.Snapshot.Metadata != nil
}

func peerCatchupKind(kind raftpb.MessageType) bool {
	return kind == raftpb.MsgApp || kind == raftpb.MsgAppResp || kind == raftpb.MsgHeartbeat ||
		kind == raftpb.MsgHeartbeatResp || kind == raftpb.MsgSnap
}

func peerRaftTraffic(kind raftpb.MessageType) (peersession.Traffic, error) {
	switch kind {
	case raftpb.MsgVote, raftpb.MsgVoteResp, raftpb.MsgPreVote, raftpb.MsgPreVoteResp:
		return peersession.Vote, nil
	case raftpb.MsgApp, raftpb.MsgAppResp, raftpb.MsgHeartbeat, raftpb.MsgHeartbeatResp, raftpb.MsgSnap,
		raftpb.MsgTransferLeader, raftpb.MsgTimeoutNow, raftpb.MsgReadIndex, raftpb.MsgReadIndexResp:
		return peersession.Replication, nil
	case raftpb.MsgProp, raftpb.MsgHup, raftpb.MsgBeat, raftpb.MsgUnreachable, raftpb.MsgSnapStatus,
		raftpb.MsgCheckQuorum, raftpb.MsgStorageAppend, raftpb.MsgStorageAppendResp,
		raftpb.MsgStorageApply, raftpb.MsgStorageApplyResp, raftpb.MsgForgetLeader:
		return 0, peersession.ErrFormat
	default: // Local-only, storage-worker and unknown message types are not peer RPCs.
		return 0, peersession.ErrFormat
	}
}

// The protobuf runtime accepts unknown fields, duplicates and concatenated
// records. Nested unknowns are rejected here; canonical re-encoding additionally
// rejects duplicate/noncanonical inner records, including valid appended fields.
func peerKnownFields(m protoreflect.Message) bool {
	if len(m.GetUnknown()) != 0 {
		return false
	}
	valid := true
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Kind() == protoreflect.EnumKind && fd.Enum().Values().ByNumber(v.Enum()) == nil {
			valid = false
		}
		if fd.Kind() == protoreflect.MessageKind {
			if fd.IsList() {
				list := v.List()
				for i := 0; i < list.Len(); i++ {
					valid = valid && peerKnownFields(list.Get(i).Message())
				}
			} else {
				valid = peerKnownFields(v.Message())
			}
		}
		return valid
	})
	return valid
}

func peerRaftAck(req peersession.Message, v any) (peersession.Message, error) {
	ack, ok := v.(*pb.EtcdRaftAck)
	if !ok || ack == nil || !peerKnownFields(ack.ProtoReflect()) || req.From == 0 || req.To == 0 {
		return peersession.Message{}, peersession.ErrFormat
	}
	req.From, req.To = req.To, req.From
	return req, nil
}

func peerBoundRoute(b peersession.Binding, m peersession.Message) bool {
	return m.Group == b.Group && ((m.From == b.Local.Member && m.To == b.Remote.Member) ||
		(m.From == b.Remote.Member && m.To == b.Local.Member))
}

func peerMessageStream(group uint64, b peersession.Binding, decode peersession.Decoder) (peersession.StreamDecoders, error) {
	if !peerValidBinding(group, b) {
		return peersession.StreamDecoders{}, peersession.ErrFormat
	}
	var mu sync.Mutex
	var last peersession.Message
	var replied bool
	return peersession.StreamDecoders{Request: func(v any) (peersession.Message, error) {
		mu.Lock()
		defer mu.Unlock()
		m, err := decode(v)
		if err != nil || replied || !peerBoundRoute(b, m) || !peerSameStreamRoute(last, m) {
			return peersession.Message{}, peersession.ErrFormat
		}
		last = m
		return m, nil
	}, Reply: func(_ peersession.Message, v any) (peersession.Message, error) {
		mu.Lock()
		defer mu.Unlock()
		if replied {
			return peersession.Message{}, peersession.ErrFormat
		}
		m, err := peerRaftAck(last, v)
		if err == nil {
			replied = true
		}
		return m, err
	}}, nil
}

func peerValidBinding(group uint64, b peersession.Binding) bool {
	return b.Group == group && peerValidIDs(b.Local.Member, b.Remote.Member)
}

func peerSameStreamRoute(last, next peersession.Message) bool {
	return last.From == 0 || (last.From == next.From && last.To == next.To)
}

func peerSnapshotStream(group uint64, b peersession.Binding, decode func([]byte) (peersession.Message, error)) (peersession.StreamDecoders, error) {
	if !peerValidBinding(group, b) {
		return peersession.StreamDecoders{}, peersession.ErrFormat
	}
	var mu sync.Mutex
	var route peersession.Message
	var final, replied bool
	return peersession.StreamDecoders{Request: func(v any) (peersession.Message, error) {
		mu.Lock()
		defer mu.Unlock()
		chunk, err := peerSnapshotChunk(v)
		if err != nil || final {
			return peersession.Message{}, peersession.ErrFormat
		}
		if route.From == 0 {
			m, err := peerSnapshotRoute(b, chunk.Metadata, decode)
			if err != nil {
				return peersession.Message{}, peersession.ErrFormat
			}
			route = m
		} else if len(chunk.Metadata) != 0 {
			return peersession.Message{}, peersession.ErrFormat
		}
		final = chunk.Final
		return route, nil
	}, Reply: func(_ peersession.Message, v any) (peersession.Message, error) {
		mu.Lock()
		defer mu.Unlock()
		if !final || replied {
			return peersession.Message{}, peersession.ErrFormat
		}
		m, err := peerRaftAck(route, v)
		if err == nil {
			replied = true
		}
		return m, err
	}}, nil
}

func peerSnapshotChunk(v any) (*pb.EtcdRaftSnapshotChunk, error) {
	chunk, ok := v.(*pb.EtcdRaftSnapshotChunk)
	if !ok || chunk == nil || !peerKnownFields(chunk.ProtoReflect()) {
		return nil, peersession.ErrFormat
	}
	return chunk, nil
}

func peerSnapshotRoute(b peersession.Binding, raw []byte, decode func([]byte) (peersession.Message, error)) (peersession.Message, error) {
	m, err := decode(raw)
	if err != nil || !peerBoundRoute(b, m) {
		return peersession.Message{}, peersession.ErrFormat
	}
	var header raftpb.Message
	if proto.Unmarshal(raw, &header) != nil || header.GetType() != raftpb.MsgSnap || len(header.Snapshot.Data) != 0 {
		return peersession.Message{}, peersession.ErrFormat
	}
	return m, nil
}
