package kv

import (
	"sync"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
)

var _ raft.FSMSnapshot = (*kvFSMSnapshot)(nil)

type kvFSMSnapshot struct {
	snapshot store.Snapshot
	once     sync.Once
	err      error
}

func (f *kvFSMSnapshot) Persist(sink raft.SnapshotSink) (err error) {
	defer func() {
		err = errors.CombineErrors(err, f.closeSnapshot())
	}()

	if _, err = f.snapshot.WriteTo(sink); err != nil {
		cancelErr := sink.Cancel()
		return errors.WithStack(errors.CombineErrors(errors.WithStack(err), errors.WithStack(cancelErr)))
	}
	return errors.WithStack(sink.Close())
}

func (f *kvFSMSnapshot) Release() {
	_ = f.closeSnapshot()
}

func (f *kvFSMSnapshot) Close() error {
	return f.closeSnapshot()
}

func (f *kvFSMSnapshot) closeSnapshot() error {
	if f == nil {
		return nil
	}
	f.once.Do(func() {
		if f.snapshot != nil {
			f.err = errors.WithStack(f.snapshot.Close())
			f.snapshot = nil
		}
	})
	return f.err
}
