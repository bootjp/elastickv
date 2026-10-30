package etcd

import (
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/errors"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type pipelineSnapshotFinalization struct{}

// Resource bookkeeping has no storage I/O under its mutex. Disk finalization
// runs only after the apply actor has returned and all earlier disk work ended.
type pipelineSnapshotResources struct {
	mu     sync.Mutex
	leases map[*pipelineSnapshotLease]bool
	closed bool
}

type pipelineSnapshotLease struct {
	snapshot Snapshot
	reader   io.ReadCloser
	closed   bool // disk actor only; read by owner after a real completion
	retire   func()
}

func (r *pipelineSnapshotResources) add(retire func()) (*pipelineSnapshotLease, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.WithStack(errStorageWorkerClosed)
	}
	if r.leases == nil {
		r.leases = make(map[*pipelineSnapshotLease]bool)
	}
	lease := &pipelineSnapshotLease{retire: retire}
	r.leases[lease] = true
	return lease, nil
}

func (r *pipelineSnapshotResources) forget(lease *pipelineSnapshotLease) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.leases, lease)
}

func (l *pipelineSnapshotLease) close() error {
	if l.closed {
		return nil
	}
	l.closed = true
	if l.retire != nil {
		defer l.retire()
	}
	var err error
	if l.snapshot != nil {
		err = l.snapshot.Close()
		l.snapshot = nil
	}
	if l.reader != nil {
		err = errors.CombineErrors(err, l.reader.Close())
		l.reader = nil
	}
	return errors.WithStack(err)
}

func (r *pipelineSnapshotResources) closeAll() error {
	r.mu.Lock()
	leases := r.leases
	r.leases = nil
	r.closed = true
	r.mu.Unlock()
	var combined error
	for lease := range leases {
		_, err := runStorageWork(func() (any, error) { return nil, lease.close() })
		combined = errors.CombineErrors(combined, err)
	}
	return errors.WithStack(combined)
}

func (p *engineStoragePipeline) snapshotIngressLease(message *raftpb.Message) (*pipelineSnapshotLease, error) {
	if message.GetType() != raftpb.MsgSnap || !isSnapshotToken(message.GetSnapshot().GetData()) {
		return nil, nil
	}
	index, files := message.GetSnapshot().GetMetadata().GetIndex(), p.snapshotFiles
	return p.snapshotResources.add(func() { files.releaseReceived(index) })
}

// File exclusion is shared only with transport/API I/O threads, never acquired
// by the owner. Owner pins use a separate, I/O-free mutex.
type pipelineSnapshotFiles struct {
	mu        *sync.Mutex
	protected map[uint64]int
	pinsMu    sync.Mutex
	pins      map[uint64]int
}

func (f *pipelineSnapshotFiles) pin(index uint64) {
	f.pinsMu.Lock()
	defer f.pinsMu.Unlock()
	if f.pins == nil {
		f.pins = make(map[uint64]int)
	}
	f.pins[index]++
}

func (f *pipelineSnapshotFiles) unpin(index uint64) {
	f.pinsMu.Lock()
	defer f.pinsMu.Unlock()
	if f.pins[index] <= 1 {
		delete(f.pins, index)
	} else {
		f.pins[index]--
	}
}

func (f *pipelineSnapshotFiles) protectedIndexes() map[uint64]bool {
	indexes := make(map[uint64]bool)
	for index, count := range f.protected {
		if count > 0 {
			indexes[index] = true
		}
	}
	f.pinsMu.Lock()
	defer f.pinsMu.Unlock()
	for index, count := range f.pins {
		if count > 0 {
			indexes[index] = true
		}
	}
	return indexes
}

func (f *pipelineSnapshotFiles) prepare(env pipelineConfigurationEnvironment, index uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return prepareFSMSnapshotWriteProtected(filepath.Join(env.dataDir, snapDirName), env.fsmSnapDir, index, f.protectedIndexes())
}

func (f *pipelineSnapshotFiles) purge(env pipelineConfigurationEnvironment) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return errors.WithStack(errors.CombineErrors(purgeOldSnapshotFilesProtected(filepath.Join(env.dataDir, snapDirName), env.fsmSnapDir, f.protectedIndexes()),
		purgeOldWALFiles(filepath.Join(env.dataDir, walDirName), env.maxWALFiles)))
}

func (e *Engine) initPipelineSnapshotFiles() {
	p := e.storagePipeline
	if p.snapshotFiles != nil {
		return
	}
	if e.protectedReceivedFSMSnaps == nil {
		e.protectedReceivedFSMSnaps = make(map[uint64]int)
	}
	p.snapshotFiles = &pipelineSnapshotFiles{mu: &e.snapshotMu, protected: e.protectedReceivedFSMSnaps}
}

func (f *pipelineSnapshotFiles) releaseReceived(index uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.protected[index] <= 1 {
		delete(f.protected, index)
	} else {
		f.protected[index]--
	}
}

func (f *pipelineSnapshotFiles) openReceived(env pipelineConfigurationEnvironment, index uint64, crc uint32) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	file, err := os.Open(fsmSnapPath(env.fsmSnapDir, index))
	if err != nil {
		return nil, statFSMFileError(err)
	}
	if err := verifyEncryptionSnapshotDescriptor(file, file.Name(), crc); err != nil {
		_ = file.Close()
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, errors.WithStack(err)
	}
	return &limitedReadCloser{Reader: io.LimitReader(file, info.Size()-fsmFooterSize), Closer: file}, nil
}
