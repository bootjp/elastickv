package kv

import (
	"io"

	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
)

var _ raft.FSMSnapshot = (*kvFSMSnapshot)(nil)

type kvFSMSnapshot struct {
	io.ReadWriter
}

func (f *kvFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()
	_, err := io.Copy(sink, f)
	return errors.WithStack(err)
}

func (f *kvFSMSnapshot) Release() {
}

func (f *kvFSMSnapshot) Close() error {
	return nil
}
