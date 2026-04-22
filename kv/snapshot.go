package kv

import (
	"encoding/binary"
	"io"
	"sync"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// hlcSnapshotMagic is an 8-byte sentinel written at the start of every FSM
// snapshot to indicate that the snapshot includes an HLC physical ceiling.
// Old snapshots that lack this header are still readable (backward compat).
var hlcSnapshotMagic = [8]byte{'E', 'K', 'V', 'T', 'H', 'L', 'C', '1'}

// hlcSnapshotHeaderLen is the total header size: 8 magic + 8 ceiling ms.
const hlcSnapshotHeaderLen = 16 //nolint:mnd

var _ raftengine.Snapshot = (*kvFSMSnapshot)(nil)

type kvFSMSnapshot struct {
	snapshot  store.Snapshot
	ceilingMs int64
	once      sync.Once
	err       error
}

func (f *kvFSMSnapshot) WriteTo(w io.Writer) (int64, error) {
	// Write the 16-byte header: magic (8 bytes) + ceiling ms (8 bytes).
	var hdr [hlcSnapshotHeaderLen]byte
	copy(hdr[:8], hlcSnapshotMagic[:])
	binary.BigEndian.PutUint64(hdr[8:], uint64(f.ceilingMs)) //nolint:gosec // ceiling is a Unix ms timestamp, always positive
	n, err := w.Write(hdr[:])
	if err != nil {
		return int64(n), errors.WithStack(err)
	}

	m, err := f.snapshot.WriteTo(w)
	total := int64(n) + m
	if err != nil {
		return total, errors.WithStack(err)
	}
	return total, nil
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
