package kv

import (
	"bytes"
	"context"
	"io"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const tsoControlSnapshotMagic = "EKVTSO06"

// InitializeLegacyControl imports the selected legacy snapshot during the
// explicitly authorized first enablement, before admission opens. The native
// owner supplies its verified snapshot index and baseline metadata recipe.
// Ordinary Restore never silently downgrades a backed group to legacy state.
func (f *TSOStateMachine) InitializeLegacyControl(ctx context.Context, index, configuration uint64, r io.Reader, stage TSOControlStage) error {
	if f == nil || f.backing == nil || index == 0 || configuration > index {
		return errors.WithStack(ErrTSODurableBacking)
	}
	f.controlMu.Lock()
	defer f.controlMu.Unlock()
	_, err := f.backing.ReadRaftControlMetadata(ctx, []byte(tsoControlStateKey))
	if !errors.Is(err, store.ErrKeyNotFound) {
		return errors.Join(ErrTSODurableBacking, err)
	}
	candidate := NewTSOStateMachine(nil)
	if err := candidate.Restore(r); err != nil {
		return err
	}
	return f.commitControlState(ctx, index, candidate, configuration, stage)
}

type tsoControlSnapshot struct{ store.Snapshot }

func (s *tsoControlSnapshot) WriteTo(w io.Writer) (int64, error) {
	if w == nil {
		return 0, errors.New("tso control snapshot: nil writer")
	}
	n, err := io.WriteString(w, tsoControlSnapshotMagic)
	if err != nil {
		return int64(n), errors.WithStack(err)
	}
	if n != len(tsoControlSnapshotMagic) {
		return int64(n), errors.WithStack(io.ErrShortWrite)
	}
	m, err := s.Snapshot.WriteTo(w)
	return int64(n) + m, errors.WithStack(err)
}

func (f *TSOStateMachine) controlSnapshot() (raftengine.Snapshot, error) {
	f.controlMu.Lock()
	defer f.controlMu.Unlock()
	if _, err := f.backing.ReadRaftControlMetadata(context.Background(), []byte(tsoControlStateKey)); err != nil {
		return nil, errors.Wrap(err, "snapshot tso control scalars")
	}
	snapshot, err := f.backing.Snapshot()
	if err != nil {
		return nil, errors.Wrap(err, "capture tso control snapshot")
	}
	return &tsoControlSnapshot{Snapshot: snapshot}, nil
}

// restoreControlSnapshot replaces the complete scalar/control image, never a
// monotonic merge with a newer retained DB. Native recovery must still verify
// manifest/ConfState/selected WAL provenance before reopening admission or skip.
func (f *TSOStateMachine) restoreControlSnapshot(r io.Reader) error {
	if r == nil {
		return errors.WithStack(ErrTSODurableBacking)
	}
	f.controlMu.Lock()
	defer f.controlMu.Unlock()
	var magic [len(tsoControlSnapshotMagic)]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return errors.WithStack(err)
	}
	if !bytes.Equal(magic[:], []byte(tsoControlSnapshotMagic)) {
		return errors.Wrap(ErrTSODurableBacking, "backed TSO requires complete control snapshot")
	}
	var state *TSOStateMachine
	var configuration uint64
	err := f.backing.RestoreRaftControlSnapshot(context.Background(), r, func(read func([]byte) ([]byte, error)) error {
		raw, err := read([]byte(tsoControlStateKey))
		if err != nil {
			return err
		}
		state, _, configuration, err = decodeTSOControlState(raw)
		return err
	})
	if err != nil {
		return errors.Wrap(err, "restore tso control snapshot")
	}
	f.publishControlState(state, configuration)
	return nil
}
