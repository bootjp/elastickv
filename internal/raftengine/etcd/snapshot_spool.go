package etcd

import (
	"io"
	"os"

	"github.com/cockroachdb/errors"
)

var (
	// The current raftpb snapshot APIs still materialize payloads as []byte, so
	// the prototype cannot stream snapshots end-to-end yet. Keep the payload on
	// disk while assembling it and fail fast before unbounded growth.
	maxSnapshotPayloadBytes int64 = 1 << 30 // 1 GiB

	errSnapshotPayloadTooLarge = errors.New("etcd raft snapshot payload exceeds limit")
)

type snapshotSpool struct {
	file *os.File
	path string
	size int64
}

func newSnapshotSpool() (*snapshotSpool, error) {
	file, err := os.CreateTemp("", "elastickv-etcd-snapshot-*")
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &snapshotSpool{file: file, path: file.Name()}, nil
}

func (s *snapshotSpool) Write(p []byte) (int, error) {
	if int64(len(p))+s.size > maxSnapshotPayloadBytes {
		return 0, errors.Wrapf(errSnapshotPayloadTooLarge, "%d > %d", int64(len(p))+s.size, maxSnapshotPayloadBytes)
	}
	n, err := s.file.Write(p)
	s.size += int64(n)
	if err != nil {
		return n, errors.WithStack(err)
	}
	return n, nil
}

func (s *snapshotSpool) Bytes() ([]byte, error) {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return nil, errors.WithStack(err)
	}
	data, err := io.ReadAll(s.file)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return data, nil
}

func (s *snapshotSpool) Close() error {
	if s == nil {
		return nil
	}
	var err error
	if s.file != nil {
		err = errors.CombineErrors(err, errors.WithStack(s.file.Close()))
	}
	if s.path != "" {
		err = errors.CombineErrors(err, errors.WithStack(os.Remove(s.path)))
	}
	return errors.WithStack(err)
}
