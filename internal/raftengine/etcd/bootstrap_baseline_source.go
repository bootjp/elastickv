package etcd

import (
	"bytes"
	"context"
	"io"
	"path/filepath"

	"github.com/cockroachdb/errors"
	"go.etcd.io/raft/v3/raftpb"
)

// OfflineBaselineReplayEntry includes every committed slot after the selected
// snapshot. Normal Payload is the proposal-envelope-decoded FSM payload; the
// consumer MUST reject unsupported/encrypted application formats, not guess.
type OfflineBaselineReplayEntry struct {
	Index, Term uint64
	Type        int32
	Payload     []byte
}

type OfflineBaselineWALSource struct {
	HardState                                                                              *raftpb.HardState
	SnapshotIndex, SnapshotTerm, CommitIndex, CommitTerm, LastLogIndex, ConfigurationIndex uint64
	Configuration                                                                          *raftpb.ConfState
	Replay                                                                                 []OfflineBaselineReplayEntry
	Snapshot                                                                               io.ReadCloser
}

// ReadOfflineBaselineWALSource provides the independent native cut needed by a
// legacy TSO scalar reconstructor before Store evidence exists. It requires a
// complete native configuration proof, and never creates/repairs/restores state.
// Caller retains ownership through consumption and closes Snapshot.
func ReadOfflineBaselineWALSource(ctx context.Context, dataDir string, ownership io.Closer) (OfflineBaselineWALSource, error) {
	var result OfflineBaselineWALSource
	if _, err := verifyBaselineOwnership(OfflineBaselineOptions{DataDir: dataDir, Ownership: ownership}); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, errors.WithStack(err)
	}
	image, err := readBaselineWALPayloads(filepath.Join(dataDir, walDirName), true)
	if err != nil {
		return result, err
	}
	if err := verifyWALIdentity(image.metadata, nil); err != nil {
		return result, err
	}
	snapshot, err := selectBaselineSnapshot(dataDir, image)
	if err != nil {
		return result, err
	}
	return baselineWALReplaySource(dataDir, image, snapshot)
}

func baselineWALReplaySource(dataDir string, image *baselineWALImage, snapshot *raftpb.Snapshot) (OfflineBaselineWALSource, error) {
	var result OfflineBaselineWALSource
	commit, index := image.hardState.GetCommit(), snapshot.GetMetadata().GetIndex()
	if !baselineSourceCutComplete(image, index, commit) {
		return result, errors.Wrap(ErrOfflineBaseline, "TSO source must have a complete committed cut without pending tail")
	}
	conf, n, err := baselineReplayedConfiguration(image, snapshot, BaselineStoreProof{})
	if err != nil {
		return result, err
	}
	result = OfflineBaselineWALSource{HardState: image.hardState, SnapshotIndex: index, SnapshotTerm: snapshot.GetMetadata().GetTerm(), CommitIndex: commit,
		LastLogIndex: image.lastIndex(), ConfigurationIndex: n, Configuration: conf}
	result.CommitTerm = snapshot.GetMetadata().GetTerm()
	if entry := image.entries[commit]; entry != nil {
		result.CommitTerm = entry.GetTerm()
	}
	if result.CommitTerm == 0 || result.CommitTerm > image.hardState.GetTerm() {
		return result, errors.Wrap(ErrOfflineBaseline, "invalid source commit term")
	}
	result.Replay, err = baselineSourceReplayEntries(image, index, commit)
	if err != nil {
		return result, err
	}
	result.Snapshot, err = openBaselineSnapshotPayload(dataDir, snapshot)
	return result, err
}

func baselineSourceCutComplete(image *baselineWALImage, index, commit uint64) bool {
	return commit != 0 && commit != ^uint64(0) && index <= commit && image.lastIndex() == commit && commit-index <= uint64(len(image.entries))
}

func baselineSourceReplayEntries(image *baselineWALImage, index, commit uint64) ([]OfflineBaselineReplayEntry, error) {
	var replay []OfflineBaselineReplayEntry
	for i := index + 1; i <= commit; i++ {
		entry := image.entries[i]
		if entry == nil {
			return nil, errors.Wrap(ErrOfflineBaseline, "missing source replay entry")
		}
		payload, err := baselineReplayPayload(entry)
		if err != nil {
			return nil, err
		}
		replay = append(replay, OfflineBaselineReplayEntry{Index: i, Term: entry.GetTerm(), Type: int32(entry.GetType()), Payload: payload})
	}
	return replay, nil
}

func baselineReplayPayload(entry *raftpb.Entry) ([]byte, error) {
	if entry.GetType() != raftpb.EntryNormal {
		return bytes.Clone(entry.Data), nil
	}
	if len(entry.Data) == 0 {
		return nil, nil
	}
	_, payload, ok := decodeProposalEnvelope(entry.Data)
	if !ok {
		return nil, errors.Wrap(ErrOfflineBaseline, "unsupported source proposal envelope")
	}
	return bytes.Clone(payload), nil
}

func openBaselineSnapshotPayload(dir string, snapshot *raftpb.Snapshot) (io.ReadCloser, error) {
	if !isSnapshotToken(snapshot.Data) {
		return io.NopCloser(bytes.NewReader(bytes.Clone(snapshot.Data))), nil
	}
	token, err := decodeSnapshotToken(snapshot.Data)
	if err != nil {
		return nil, err
	}
	path := fsmSnapPath(filepath.Join(dir, fsmSnapDirName), token.Index)
	if err := verifyFSMSnapshotFileWithToken(path, token.CRC32C, true); err != nil {
		return nil, err
	}
	file, size, err := openPersistedFSMSnapshotForExport(path, token.CRC32C)
	if err != nil {
		return nil, err
	}
	return &baselinePayloadReader{Reader: io.LimitReader(file, size), Closer: file}, nil
}

type baselinePayloadReader struct {
	io.Reader
	io.Closer
}
