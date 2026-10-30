package etcd

import (
	"crypto/sha256"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type pipelineReadOwnership struct {
	permit        *storageWorkPermit
	term          uint64
	ready         bool
	pendingCommit bool
}

type pipelineNetworkReadOwnership struct {
	permits       []*storageWorkPermit
	pendingCommit bool
}

type pipelineNetworkReadKey struct {
	from, term uint64
	context    [sha256.Size]byte
}

func (e *Engine) admitPipelineAdmin(request adminRequest) {
	if contextErr(request.ctx) != nil || e.State() != raftengine.StateLeader ||
		(request.prevIndex != 0 && request.prevIndex != e.currentConfigIndex()) {
		e.handleAdmin(request)
		return
	}
	// encodeConfChangeContext bounds both strings by uint16 and uses a fixed
	// binary header. Reserve their copies plus protobuf/owner descriptors before
	// handleAdmin can call ProposeConfChange. No business-entry limit is raised.
	bytes := pipelineTaskOverhead*outboundCapacityFactor + pipelineConfigurationMemberBytes +
		uint64(len(request.peer.ID)+len(request.peer.Address))*outboundCapacityFactor*outboundCapacityFactor
	permit, err := e.storagePipeline.controlOwnership.tryAcquire(bytes)
	if err != nil {
		request.done <- adminResult{err: err}
		return
	}
	defer permit.Release()
	e.handleAdmin(request)
	// Do not release the control envelope's owner before emitted local work
	// has its independent reservations. The main turn observes the first cause.
	if err := e.foldPipelineReady(); err != nil {
		_ = e.storagePipeline.firstFailure(err)
	}
}

// ReadIndex retains a context in RawNode even after a caller cancels. Keeping
// its control reservation until actual ReadState publication, a proven readOnly
// reset, or disposal prevents cancellation loops from hiding retained messages.
func (e *Engine) admitPipelineRead(request readRequest) {
	p := e.storagePipeline
	if err := contextErr(request.ctx); err != nil {
		request.done <- readResult{err: err}
		return
	}
	if e.State() != raftengine.StateLeader {
		request.done <- readResult{err: errors.WithStack(errNotLeader)}
		return
	}
	status := e.rawNode.BasicStatus()
	if status.LeadTransferee != 0 {
		request.done <- readResult{err: errors.WithStack(errLeadershipTransferInProgress)}
		return
	}
	permit, err := p.controlOwnership.tryAcquire(pipelineTaskOverhead)
	if err != nil {
		request.done <- readResult{err: err}
		return
	}
	if p.readOwnership == nil {
		p.readOwnership = make(map[uint64]*pipelineReadOwnership)
	}
	p.readOwnership[request.id] = &pipelineReadOwnership{permit: permit, term: status.GetTerm(),
		pendingCommit: !e.pipelineCurrentTermCommitted()}
	e.storePendingRead(request)
	e.rawNode.ReadIndex(encodeReadContext(request.id))
}

func pipelineNetworkReadIdentity(message *raftpb.Message, term uint64) pipelineNetworkReadKey {
	var context []byte
	if len(message.Entries) > 0 && message.Entries[0] != nil {
		context = message.Entries[0].Data
	}
	return pipelineNetworkReadKey{from: message.GetFrom(), term: term, context: sha256.Sum256(context)}
}

func (e *Engine) retainPipelineNetworkRead(input *pipelineIngress) {
	if input.message.GetType() != raftpb.MsgReadIndex || len(input.message.Entries) == 0 {
		return
	}
	status := e.rawNode.BasicStatus()
	if status.RaftState != etcdraft.StateLeader {
		return
	}
	p := e.storagePipeline
	if p.networkReadOwnership == nil {
		p.networkReadOwnership = make(map[pipelineNetworkReadKey]*pipelineNetworkReadOwnership)
	}
	key := pipelineNetworkReadIdentity(&input.message, status.GetTerm())
	owned := p.networkReadOwnership[key]
	if owned == nil {
		owned = &pipelineNetworkReadOwnership{pendingCommit: !e.pipelineCurrentTermCommitted()}
		p.networkReadOwnership[key] = owned
	}
	owned.permits = append(owned.permits, input.permit)
	input.permit = nil
}

func (e *Engine) retirePipelineNetworkRead(message *raftpb.Message) {
	if message.GetType() != raftpb.MsgReadIndexResp {
		return
	}
	key := pipelineNetworkReadIdentity(message, e.rawNode.BasicStatus().GetTerm())
	key.from = message.GetTo()
	p := e.storagePipeline
	if owned := p.networkReadOwnership[key]; owned != nil {
		p.readyReadRetire = append(p.readyReadRetire, owned.permits...)
	}
	delete(p.networkReadOwnership, key)
}

func (p *engineStoragePipeline) retireReadyReads() {
	for _, permit := range p.readyReadRetire {
		permit.Release()
	}
	p.readyReadRetire = nil
}

func (p *engineStoragePipeline) releaseNetworkRead(key pipelineNetworkReadKey) {
	for _, permit := range p.networkReadOwnership[key].permits {
		permit.Release()
	}
	delete(p.networkReadOwnership, key)
}

func (e *Engine) retirePipelineReadOwnership() {
	p := e.storagePipeline
	var term uint64
	leader := false
	if e.rawNode != nil {
		status := e.rawNode.BasicStatus()
		term, leader = status.GetTerm(), status.RaftState == etcdraft.StateLeader
	}
	e.retirePipelineLocalReads(term, leader)
	for key, owned := range p.networkReadOwnership {
		if e.rawNode == nil || (!owned.pendingCommit && (!leader || key.term != term)) {
			p.releaseNetworkRead(key)
		}
	}
}

func (e *Engine) retirePipelineLocalReads(term uint64, leader bool) {
	p := e.storagePipeline
	for id, owned := range p.readOwnership {
		if owned.pendingCommit && e.rawNode != nil {
			continue
		}
		if !leader || owned.term != term {
			owned.ready = true
		}
		_, pending := e.pendingRead(id)
		if owned.ready && (!pending || e.rawNode == nil) {
			owned.permit.Release()
			delete(p.readOwnership, id)
		}
	}
}

// Unlike readOnly, raft v3.7's pendingReadIndexMessages survives reset(term).
// A new leader's actual current-term commit transfers that queue to readOnly.
// Rebind BEFORE routing that mutation's Ready so only a real ReadState/response,
// a subsequent readOnly reset, or RawNode disposal can retire the last owner.
func (e *Engine) rebindPipelinePendingReads() {
	if !e.pipelineCurrentTermCommitted() {
		return
	}
	p := e.storagePipeline
	term := e.rawNode.BasicStatus().GetTerm()
	for _, owned := range p.readOwnership {
		if owned.pendingCommit {
			owned.term, owned.pendingCommit, owned.ready = term, false, false
		}
	}
	for key, owned := range p.networkReadOwnership {
		if !owned.pendingCommit {
			continue
		}
		owned.pendingCommit = false
		if key.term == term {
			continue
		}
		delete(p.networkReadOwnership, key)
		key.term = term
		if existing := p.networkReadOwnership[key]; existing != nil {
			existing.permits = append(existing.permits, owned.permits...)
		} else {
			p.networkReadOwnership[key] = owned
		}
	}
}

func (e *Engine) pipelineCurrentTermCommitted() bool {
	if e.rawNode == nil {
		return false
	}
	status := e.rawNode.BasicStatus()
	if status.RaftState != etcdraft.StateLeader {
		return false
	}
	// The unstable suffix can replace a still-published older term at the same
	// index, so consult the owner shadow before the durable MemoryStorage prefix.
	for _, entry := range e.storagePipeline.ledger.entries {
		if entry.index == status.GetCommit() {
			return entry.term == status.GetTerm()
		}
	}
	if e.storage != nil {
		if term, err := e.storage.Term(status.GetCommit()); err == nil {
			return term == status.GetTerm()
		}
	}
	return false
}
