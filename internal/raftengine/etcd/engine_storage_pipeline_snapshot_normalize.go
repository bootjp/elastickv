package etcd

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"

	"github.com/cockroachdb/errors"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	pipelineNormalizationScratch     = 4*pipelineControlBytes + 2*pipelineSnapshotCopyBuffer
	pipelineSnapshotDescriptorCopies = 3
)

// Source fields are owner-captured before queueing. Only the disk actor writes
// snapshot.Data; the owner reads it after the actual completion. Recovery never
// captures the selected inline body: its existing Raft file is streamed instead.
type pipelineSnapshotNormalization struct {
	snapshot *raftpb.Snapshot
	inline   []byte
	path     string
	env      pipelineConfigurationEnvironment
	files    *pipelineSnapshotFiles
	plan     *pipelineSnapshotPlan
	permit   *storageWorkPermit
	index    uint64
	install  bool
	queued   bool
	wasToken bool
}

func (e *Engine) newPipelineNormalization(source *raftpb.Snapshot, env pipelineConfigurationEnvironment,
	plan *pipelineSnapshotPlan, recovered bool,
) (*pipelineSnapshotNormalization, error) {
	if err := validatePipelineConfigurationState(source.GetMetadata().GetConfState()); err != nil {
		return nil, err
	}
	metadata, err := pipelineNormalizationMetadata(source)
	if err != nil {
		return nil, err
	}
	projection := pipelineSnapshotAccounting(&metadata, true)
	if projection > pipelineControlBytes {
		return nil, errors.WithStack(errStorageWorkInvalidReservation)
	}
	size := pipelineAddBytes(projection*pipelineSnapshotDescriptorCopies, pipelineTaskOverhead)
	if !recovered && !isSnapshotToken(source.Data) {
		size = pipelineAddBytes(size, uint64(cap(source.Data)))
	}
	permit, err := e.storagePipeline.snapshotBudget.tryAcquire(size)
	if err != nil {
		return nil, errors.Wrap(errStoragePipelineResource, "snapshot normalization source")
	}
	e.initPipelineSnapshotFiles()
	n := &pipelineSnapshotNormalization{snapshot: proto.CloneOf(&metadata), env: pipelineSnapshotEnvironment(env),
		files: e.storagePipeline.snapshotFiles, plan: plan, permit: permit, index: source.GetMetadata().GetIndex(), install: recovered, wasToken: isSnapshotToken(source.Data)}
	if recovered {
		n.path = filepath.Join(env.dataDir, snapDirName, fmt.Sprintf("%016x-%016x.snap", source.GetMetadata().GetTerm(), n.index))
	} else if !isSnapshotToken(source.Data) {
		n.inline = source.Data
	}
	if e.storagePipeline.normalizations == nil {
		e.storagePipeline.normalizations = make(map[*pipelineSnapshotNormalization]bool)
	}
	e.storagePipeline.normalizations[n] = true
	n.files.pin(n.index)
	return n, nil
}

func pipelineNormalizationMetadata(source *raftpb.Snapshot) (raftpb.Snapshot, error) {
	metadata := *source
	metadata.Data = nil
	if isSnapshotToken(source.Data) {
		if _, err := decodeSnapshotToken(source.Data); err != nil {
			return metadata, err
		}
		metadata.Data = source.Data
	}
	return metadata, nil
}

func (e *Engine) queuePipelineNormalization(n *pipelineSnapshotNormalization) error {
	p := e.storagePipeline
	permit, err := p.diskBulk.tryAcquire(pipelineNormalizationScratch)
	if err != nil {
		return errors.Wrap(errStoragePipelineResource, "snapshot normalization scratch")
	}
	persist := p.persist
	task := &pipelineTask{permit: permit, normalization: n}
	task.work = storageWork{token: task, retainedBytes: pipelineNormalizationScratch, run: func() (any, error) {
		if err := p.firstFailure(nil); err != nil {
			return nil, err
		}
		_, err := runStorageWork(func() (any, error) {
			if err := writePipelineNormalizedSnapshot(n); err != nil {
				return nil, err
			}
			if n.install && !n.wasToken {
				return nil, persist.SaveSnap(n.snapshot)
			}
			return nil, nil
		})
		if err != nil {
			err = p.firstFailure(err)
		}
		return nil, err
	}}
	p.diskIssued++
	task.diskSequence = p.diskIssued
	p.diskDeferred = append(p.diskDeferred, task)
	p.diskOutstanding++
	n.queued = true
	return nil
}

type pipelineInlineSnapshot []byte

func (s pipelineInlineSnapshot) WriteTo(w io.Writer) (int64, error) {
	n, err := io.Copy(w, bytes.NewReader(s))
	return n, errors.WithStack(err)
}

func (pipelineInlineSnapshot) Close() error { return nil }

func writePipelineNormalizedSnapshot(n *pipelineSnapshotNormalization) error {
	if n.env.fsmSnapDir == "" {
		return errors.Wrap(errInvalidStorageWork, "snapshot normalization requires a data directory")
	}
	if isSnapshotToken(n.snapshot.Data) {
		token, err := decodeSnapshotToken(n.snapshot.Data)
		if err != nil {
			return err
		}
		reader, err := n.files.openReceived(n.env, token.Index, token.CRC32C)
		if err != nil {
			return err
		}
		return errors.WithStack(reader.Close())
	}
	if err := n.files.prepare(n.env, n.index); err != nil {
		return err
	}
	var source Snapshot = pipelineInlineSnapshot(n.inline)
	if n.path != "" {
		source = &pipelineRaftFileSnapshot{path: n.path, expected: n.snapshot.GetMetadata()}
	}
	n.files.mu.Lock()
	defer n.files.mu.Unlock()
	crc, err := writeFSMSnapshotFile(source, n.env.fsmSnapDir, n.index)
	if err != nil {
		return err
	}
	n.snapshot.Data = encodeSnapshotToken(n.index, crc)
	return nil
}

func (e *Engine) completePipelineNormalization(n *pipelineSnapshotNormalization, err error) error {
	if failure := e.storagePipeline.firstFailure(nil); failure != nil {
		return failure
	}
	if err != nil {
		return err
	}
	defer e.retirePipelineNormalization(n)
	if n.install {
		if err := e.publishPipelineNormalization(n); err != nil {
			return err
		}
		if e.storagePipeline.recoveryNormalization == n {
			e.storagePipeline.recoveryNormalization = nil
		}
		return nil
	}
	plan := n.plan
	plan.snapshot, plan.message.Snapshot = n.snapshot, n.snapshot
	return e.queuePipelineSnapshotApply(plan, pipelineReceivedRestore)
}

func (e *Engine) publishPipelineNormalization(n *pipelineSnapshotNormalization) error {
	if n.wasToken {
		e.storagePipeline.snapshotView.publish(n.snapshot, true)
		return nil
	}
	return e.installPipelineNormalizedStorage(n.snapshot)
}

func (e *Engine) installPipelineNormalizedStorage(snapshot *raftpb.Snapshot) error {
	if err := e.pipelineMemoryStorageMutable(); err != nil {
		return err
	}
	entries, err := e.pipelineStableBase()
	if err != nil {
		return err
	}
	size := pipelineAddBytes(pipelineArrayBytes(len(entries)+1, pipelinePointerBytes*outboundCapacityFactor), pipelineSnapshotAccounting(snapshot, true))
	permit, err := e.storagePipeline.diskBulk.tryAcquire(size)
	if err != nil {
		return errors.Wrap(errStoragePipelineResource, "normalized storage replacement")
	}
	defer permit.Release()
	state, _, err := e.storage.InitialState()
	if err != nil {
		return errors.WithStack(err)
	}
	replacement := etcdraft.NewMemoryStorage()
	if err := replacement.ApplySnapshot(snapshot); err != nil {
		return errors.WithStack(err)
	}
	if err := replacement.Append(entries); err != nil {
		return errors.WithStack(err)
	}
	if err := replacement.SetHardState(state); err != nil {
		return errors.WithStack(err)
	}
	e.storage = replacement
	e.storagePipeline.snapshotView.MemoryStorage = replacement
	e.storagePipeline.snapshotView.publish(snapshot, true)
	return nil
}

func (e *Engine) retirePipelineNormalization(n *pipelineSnapshotNormalization) {
	delete(e.storagePipeline.normalizations, n)
	n.files.unpin(n.index)
	permit := n.permit
	*n = pipelineSnapshotNormalization{}
	permit.Release()
}

func (e *Engine) retirePipelineNormalizations() {
	for n := range e.storagePipeline.normalizations {
		e.retirePipelineNormalization(n)
	}
	e.storagePipeline.recoveryNormalization = nil
}
