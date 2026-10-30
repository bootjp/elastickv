package etcd

import (
	"github.com/cockroachdb/errors"
	etcdstorage "go.etcd.io/etcd/server/v3/storage"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var errInvalidAsyncStorageAppend = errors.New("invalid asynchronous storage append")

// asyncStorageAppendWork retains the complete local message, including opaque
// responses and snapshot tokens, until the owner retires its disk completion.
// Callers must transfer ownership without mutating the message after submission.
func asyncStorageAppendWork(persist etcdstorage.Storage, message *raftpb.Message) (storageWork, error) {
	if !validAsyncStorageAppend(persist, message) {
		return storageWork{}, errors.WithStack(errInvalidAsyncStorageAppend)
	}
	size := proto.Size(message)
	if size < 0 {
		return storageWork{}, errors.WithStack(errInvalidAsyncStorageAppend)
	}
	return storageWork{
		token:         message,
		retainedBytes: uint64(size),
		run: func() (any, error) {
			return nil, persistAsyncStorageAppend(persist, message)
		},
	}, nil
}

func validAsyncStorageAppend(persist etcdstorage.Storage, message *raftpb.Message) bool {
	return persist != nil && message != nil && message.GetType() == raftpb.MsgStorageAppend && message.GetTo() == etcdraft.LocalAppendThread
}

// persistAsyncStorageAppend is only the disk phase. The owner still retains
// the message and any snapshot resources until installation/publication barriers
// complete. A nil error authorizes its opaque responses only after this prefix
// is durable; it is not itself an FSM or snapshot-installation receipt.
func persistAsyncStorageAppend(persist etcdstorage.Storage, message *raftpb.Message) error {
	if !validAsyncStorageAppend(persist, message) {
		return errors.WithStack(errInvalidAsyncStorageAppend)
	}
	ready := etcdraft.Ready{
		HardState: &raftpb.HardState{Term: message.Term, Vote: message.Vote, Commit: message.Commit},
		Entries:   message.Entries,
		Snapshot:  message.Snapshot,
	}
	if err := persistReadyToWAL(persist, ready); err != nil {
		return errors.Wrap(err, "persist asynchronous storage append")
	}
	// Save may leave commit-only writes unsynced. A response-only append must
	// also flush preceding unsynced work, not silently bypass this barrier.
	if len(message.Responses) > 0 {
		if err := persist.Sync(); err != nil {
			return errors.Wrap(err, "sync asynchronous storage prefix")
		}
	}
	return nil
}
