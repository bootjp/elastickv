package main

import (
	"bufio"
	"io"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

func sidecarGapStartFromSnapshot(engine encryptionGapEngine, sc *encryption.Sidecar) (uint64, error) {
	start := sc.RaftAppliedIndex
	source, ok := engine.(interface {
		EncryptionSnapshot() (io.ReadCloser, uint64, error)
	})
	if !ok {
		return start, nil
	}
	payload, index, err := source.EncryptionSnapshot()
	if err != nil {
		return 0, errors.Wrap(err, "encryption startup guard: verify snapshot checkpoint")
	}
	if payload == nil {
		return start, nil
	}
	defer payload.Close()
	if index <= start {
		return start, nil
	}
	if index > engine.AppliedIndex() {
		return 0, errors.Wrap(encryption.ErrSidecarBehindRaftLog, "snapshot checkpoint exceeds applied index")
	}
	return verifySidecarSnapshotCheckpoint(payload, index, sc)
}

func verifySidecarSnapshotCheckpoint(payload io.Reader, index uint64, sc *encryption.Sidecar) (uint64, error) {
	_, cutover, checkpoint, err := kv.ReadSnapshotHeaderWithCheckpoint(bufio.NewReader(payload))
	if err != nil {
		return 0, errors.Wrap(err, "encryption startup guard: read snapshot checkpoint")
	}
	if checkpoint.Index == 0 {
		// Old snapshots carry no proof about encryption changes in the
		// compacted prefix. Keep the original complete-scan requirement.
		return sc.RaftAppliedIndex, nil
	}
	want, err := encryption.SidecarCheckpoint(sc)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if checkpoint.Index > index || checkpoint != want || cutover != sc.RaftEnvelopeCutoverIndex {
		return 0, errors.Wrap(encryption.ErrSidecarBehindRaftLog, "snapshot checkpoint disagrees with sidecar")
	}
	return index, nil
}
