package etcd

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bootjp/elastickv/internal/monoclock"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	etcdstorage "go.etcd.io/etcd/server/v3/storage"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/status"
)

const (
	defaultTickInterval  = 10 * time.Millisecond
	defaultHeartbeatTick = 10  // 100ms at 10ms interval
	defaultElectionTick  = 100 // 1s at 10ms interval (10x heartbeat, etcd/raft recommended ratio)
	// leaseSafetyMargin is subtracted from electionTimeout when computing the
	// duration of a leader-local read lease. It absorbs goroutine scheduling
	// delay between heartbeat ack and lease refresh, GC pauses on the leader,
	// and bounded wall-clock skew between the leader and a partition's new
	// leader candidate. See docs/design/2026_04_20_implemented_lease_read.md for the safety argument.
	leaseSafetyMargin = 300 * time.Millisecond
	// defaultMaxInflightMsg controls how many in-flight MsgApp messages Raft
	// allows per peer before waiting for an ACK. It also sizes the inbound
	// stepCh, dispatchReportCh, and the per-peer outbound "normal" dispatch
	// queue. Total buffered memory is bounded by
	// O(numPeers × MaxInflightMsg × avgMsgSize).
	//
	// Raised from 256 → 1024 to absorb short CPU bursts without forcing
	// peers to reject with "etcd raft inbound step queue is full".
	// Under production congestion we observed the 256-slot inbound
	// stepCh on followers filling up while their event loop was held
	// up by adapter-side pebble seek storms (PRs #560, #562, #563,
	// #565 removed most of that CPU); 1024 is a 4× safety margin.
	// Note that with the current defaultMaxSizePerMsg of 1 MiB, the
	// true worst-case bound can be much larger (up to roughly 1 GiB
	// per peer if every slot held a max-sized message). In practice,
	// typical MsgApp payloads are far smaller, so expected steady-state
	// memory remains much lower than that worst-case bound.
	defaultMaxInflightMsg = 1024
	defaultMaxSizePerMsg  = 1 << 20
	// defaultHeartbeatBufPerPeer is the capacity of the priority dispatch channel.
	// It carries low-frequency control traffic: heartbeats, votes, read-index,
	// leader-transfer, and their corresponding response messages
	// (MsgHeartbeatResp, MsgReadIndexResp, MsgVoteResp, MsgPreVoteResp).
	// MsgAppResp is intentionally kept in the normal channel: followers — the
	// only senders of MsgAppResp — do not send MsgApp, so there is no
	// head-of-line blocking risk there.
	//
	// Raised from 64 → 512 after the leader logged heartbeat drops
	// totalling 1.6M+ (dispatchDropCount) while the transport drained
	// slower than heartbeat tick issuance. Heartbeats are tiny
	// (< ~100 B), so 512 × numPeers is ≪ 1 MB total memory; the
	// upside is that a ~5 s transient pause (election-timeout scale)
	// no longer drops heartbeats and forces the peers' lease to expire.
	defaultHeartbeatBufPerPeer = 512
	// defaultSnapshotLaneBufPerPeer sizes the per-peer MsgSnap lane when the
	// 4-lane dispatcher mode is enabled (see ELASTICKV_RAFT_DISPATCHER_LANES).
	// MsgSnap is rare and bulky; 4 is enough to absorb a retry or two without
	// holding up MsgApp replication behind a multi-MiB payload.
	defaultSnapshotLaneBufPerPeer = 4
	// defaultOtherLaneBufPerPeer sizes the per-peer fallback lane for message
	// types not classified as heartbeat/replication/snapshot (e.g. surprise
	// locally-addressed control types). Small buffer: traffic volume is tiny.
	defaultOtherLaneBufPerPeer = 16
	// dispatcherLanesEnvVar toggles the 4-lane dispatcher (heartbeat /
	// replication / snapshot / other). When unset or "0", the legacy
	// 2-lane layout (heartbeat + normal) is used. Opt-in by design: the
	// raft hot path is high blast radius and a regression here can cause
	// cluster-wide elections.
	dispatcherLanesEnvVar = "ELASTICKV_RAFT_DISPATCHER_LANES"
	// defaultSnapshotEvery is the fallback trigger threshold: take an FSM
	// snapshot once the applied index has advanced this many entries past
	// the last snapshot's index. etcd/raft itself uses 10_000 as a default,
	// but with fat proposal payloads (e.g. Lua scripts) this can produce a
	// multi-GiB WAL between snapshots. Operators can lower via
	// ELASTICKV_RAFT_SNAPSHOT_COUNT without a rebuild.
	defaultSnapshotEvery     = 10_000
	snapshotEveryEnvVar      = "ELASTICKV_RAFT_SNAPSHOT_COUNT"
	defaultSnapshotQueueSize = 1
	defaultAdminPollInterval = 10 * time.Millisecond
	defaultMaxPendingConfigs = 64
	unknownLastContact       = time.Duration(-1)

	proposalEnvelopeVersion  = byte(0x01)
	readContextVersion       = byte(0x02)
	confChangeContextVersion = byte(0x03)
	envelopeHeaderSize       = 9
	confChangeFixedSize      = 21
)

var (
	errNilEngine                    = errors.New("raft engine is not configured")
	errClosed                       = errors.New("etcd raft engine is closed")
	errNotLeader                    = errors.Mark(errors.New("etcd raft engine is not leader"), raftengine.ErrNotLeader)
	errNodeIDRequired               = errors.New("etcd raft node id is required")
	errDataDirRequired              = errors.New("etcd raft data dir is required")
	errStateMachineUnset            = errors.New("etcd raft state machine is not configured")
	errSnapshotRequired             = errors.New("etcd raft snapshot payload is required")
	errStepQueueFull                = errors.New("etcd raft inbound step queue is full")
	errClusterMismatch              = errors.New("etcd raft persisted cluster does not match configured peers")
	errConfigIndexMismatch          = errors.New("etcd raft configuration index does not match")
	errConfChangeContextTooLarge    = errors.New("etcd raft conf change context is too large")
	errLeadershipTransferTarget     = errors.New("etcd raft leadership transfer target is required")
	errLeadershipTransferNotReady   = errors.New("etcd raft leadership transfer target is not available")
	errLeadershipTransferAborted    = errors.New("etcd raft leadership transfer aborted")
	errLeadershipTransferRejected   = errors.New("etcd raft leadership transfer was rejected by raft (target is not a voter)")
	errLeadershipTransferNotLeader  = errors.Mark(errors.New("etcd raft leadership transfer requires the local node to be leader"), raftengine.ErrNotLeader)
	errLeadershipTransferInProgress = errors.Mark(errors.New("etcd raft leadership transfer is in progress"), raftengine.ErrLeadershipTransferInProgress)
	errTooManyPendingConfigs        = errors.New("etcd raft engine has too many pending config changes")
	errPromoteLearnerNotLearner     = errors.New("etcd raft promote-learner target is not a learner")
	errPromoteLearnerNoProgress     = errors.New("etcd raft promote-learner target has no leader-side progress entry")
	errPromoteLearnerNotCaughtUp    = errors.New("etcd raft promote-learner target has not caught up to min_applied_index")
	errPromoteLearnerMinAppliedZero = errors.New("etcd raft promote-learner requires min_applied_index>0 unless skip_min_applied_check is set")
)

// joinRoleViolationCount counts the number of times the join-as-learner
// alarm has fired across all engine instances in this process. Surfaced
// via JoinRoleViolationCount() so callers (monitoring) can read it
// without holding any engine lock; the latch on Engine.joinAlarmFired
// keeps each individual engine from contributing more than once per
// process lifetime.
var joinRoleViolationCount atomic.Uint64

// JoinRoleViolationCount returns the cumulative count of
// --raftJoinAsLearner alarms that have fired since process start. See
// docs/design/2026_04_26_proposed_raft_learner.md §4.5.
func JoinRoleViolationCount() uint64 {
	return joinRoleViolationCount.Load()
}

// Snapshot is an alias for the shared raftengine.Snapshot interface.
type Snapshot = raftengine.Snapshot

// StateMachine is an alias for the shared raftengine.StateMachine interface.
type StateMachine = raftengine.StateMachine

type OpenConfig struct {
	NodeID       uint64
	LocalID      string
	LocalAddress string
	DataDir      string
	Peers        []Peer
	Bootstrap    bool
	// JoinAsLearner mirrors raftengine.FactoryConfig.JoinAsLearner. The
	// engine watches every post-apply ConfState and emits an
	// ERROR-level alarm whenever the local node is in Voters while
	// JoinAsLearner is true. The check is post-apply only; the node
	// continues running.
	JoinAsLearner bool
	Transport     *GRPCTransport
	TickInterval  time.Duration
	ElectionTick  int
	HeartbeatTick int
	StateMachine  StateMachine
	MaxSizePerMsg uint64
	// MaxInflightMsg controls how many MsgApp messages Raft may have in-flight
	// per peer before waiting for an acknowledgement (Raft-level flow control).
	// It also sets the per-peer dispatch channel capacity, so total buffered
	// memory is bounded by O(numPeers * MaxInflightMsg * avgMsgSize).
	// Default: 256. Increase for deeper pipelining on high-bandwidth links;
	// lower in memory-constrained clusters.
	MaxInflightMsg int
}

type Engine struct {
	nodeID       uint64
	localID      string
	localAddress string
	dataDir      string
	fsmSnapDir   string
	tickInterval time.Duration
	electionTick int
	// joinAsLearner is captured from OpenConfig; consulted by
	// alarmIfJoinedAsVoter on every post-apply ConfState. See §4.5 of
	// the learner design doc.
	joinAsLearner bool
	// joinAlarmFired latches the join-as-learner alarm so the
	// structured ERROR log fires once per process lifetime. Without
	// the latch, every conf change after the misbehaving join would
	// re-emit the log even though the operator has already been
	// alerted by the metric counter. Stored as int32 for atomic CAS.
	joinAlarmFired atomic.Bool

	storage   *etcdraft.MemoryStorage
	rawNode   *etcdraft.RawNode
	persist   etcdstorage.Storage
	fsm       StateMachine
	peers     map[uint64]Peer
	transport *GRPCTransport

	nextRequestID atomic.Uint64

	proposeCh        chan proposalRequest
	readCh           chan readRequest
	adminCh          chan adminRequest
	stepCh           chan raftpb.Message
	dispatchReportCh chan dispatchReport
	peerDispatchers  map[uint64]*peerQueues
	perPeerQueueSize int
	// dispatcherLanesEnabled toggles the 4-lane dispatcher layout. Captured
	// once at Open from ELASTICKV_RAFT_DISPATCHER_LANES so the run-time code
	// path is branch-free per message and does not need to re-read env vars.
	dispatcherLanesEnabled bool
	// snapshotEvery is the FSM-snapshot trigger threshold captured once at
	// Open from ELASTICKV_RAFT_SNAPSHOT_COUNT. maybePersistLocalSnapshot
	// runs on every Ready-drain pass, so re-parsing the env var (and
	// potentially emitting a warning on malformed input) on every call
	// would flood logs and burn CPU on the raft hot path. Cache it once.
	snapshotEvery uint64
	// maxWALFiles is the WAL retention cap captured once at Open from
	// ELASTICKV_RAFT_MAX_WAL_FILES. Purges are relatively rare (only after
	// snapshot release) but caching avoids a second warning-per-invalid
	// call site and keeps the knob consistent across the engine lifetime.
	maxWALFiles    int
	dispatchStopCh chan struct{}
	dispatchCtx    context.Context
	dispatchCancel context.CancelFunc
	snapshotReqCh  chan snapshotRequest
	snapshotResCh  chan snapshotResult
	snapshotStopCh chan struct{}
	closeCh        chan struct{}
	doneCh         chan struct{}
	startedCh      chan struct{}

	leaderReady  chan struct{}
	leaderOnce   sync.Once
	startOnce    sync.Once
	closeOnce    sync.Once
	dispatchOnce sync.Once
	snapshotOnce sync.Once
	dispatchWG   sync.WaitGroup
	snapshotWG   sync.WaitGroup

	mu      sync.RWMutex
	pending sync.Mutex
	status  raftengine.Status
	config  raftengine.Configuration
	runErr  error
	closed  bool
	applied uint64
	// voterCount is the count of nodes in the latest applied
	// ConfState.Voters (including self if voter). isLearnerNode is the
	// matching set membership lookup keyed by node ID. Both are written
	// only from the apply loop after rawNode.ApplyConfChange returns
	// the new ConfState; both are read under e.mu on the lease-read /
	// quorum-ack hot path. The cache exists to keep
	// followerQuorumForClusterSize and the singleNodeLeaderAckMonoNs
	// fast path from inflating the denominator with learners. See
	// docs/design/2026_04_26_proposed_raft_learner.md §4.6. Rebuilt
	// from scratch on each conf change — never patched — so promotion
	// of a learner cannot leak a stale isLearnerNode[id]==true entry.
	voterCount    int
	isLearnerNode map[uint64]bool
	// appliedIndex mirrors the current applied-entry index for
	// lock-free readers on the lease-read fast path. Writers inside
	// the Raft run loop update both `applied` (protected by the run
	// loop's single-writer invariant) and `appliedIndex.Store(...)`.
	// AppliedIndex() reads via atomic.Load so it does not contend
	// with refreshStatus's write lock.
	appliedIndex atomic.Uint64
	// configIndex tracks the highest configuration index durably published to
	// local raft snapshot state and peer metadata.
	configIndex atomic.Uint64

	lastLeaderContactAt   time.Time
	lastLeaderContactFrom uint64

	// Restore swaps the underlying store state and must not race with the short
	// critical section that publishes a newly persisted local snapshot.
	snapshotMu sync.Mutex

	dispatchDropCount  atomic.Uint64
	dispatchErrorCount atomic.Uint64
	// dispatchErrorByCode subdivides dispatchErrorCount by the grpc
	// status code returned from the transport (e.g. "Unavailable",
	// "DeadlineExceeded"). Used to tell whether dispatch failures are
	// network / backpressure / follower apply stalls. Surfaced to
	// Prometheus via DispatchErrorCountsByCode() and the
	// DispatchCollector poll loop.
	dispatchErrorByCodeMu sync.Mutex
	dispatchErrorByCode   map[string]uint64
	// stepQueueFullCount tracks the number of inbound raft messages
	// (from remote peers and local handlers) that were dropped because
	// stepCh was full. Surfaced to Prometheus as
	// elastickv_raft_step_queue_full_total so operators can correlate
	// seek-storm goroutine spikes with raft backpressure.
	stepQueueFullCount atomic.Uint64

	// ackTracker records per-peer last-response times on the leader and
	// publishes the majority-ack instant via quorumAckMonoNs. It is
	// read lock-free from LastQuorumAck() on the hot lease-read path
	// and updated inside the single event-loop goroutine from
	// handleStep when a follower response arrives.
	ackTracker quorumAckTracker
	// singleNodeLeaderAckMonoNs short-circuits LastQuorumAck on the
	// single-node leader path: self IS the quorum, so there are no
	// follower responses to observe. refreshStatus keeps this value
	// current (set to monoclock.Now().Nanos() each tick while leader
	// and cluster size is 1; cleared otherwise) so the lease-read hot
	// path never has to acquire e.mu to check peer count or leader
	// state. Stored as int64 nanoseconds on the CLOCK_MONOTONIC_RAW
	// scale so the lease comparison is immune to NTP rate adjustment
	// and wall-clock steps (see internal/monoclock).
	singleNodeLeaderAckMonoNs atomic.Int64
	// isLeader mirrors status.State == StateLeader for lock-free reads
	// on the hot path. refreshStatus writes it on every tick;
	// recordQuorumAck reads it before admitting a follower response
	// into ackTracker (so late MsgAppResp / MsgHeartbeatResp arriving
	// after a step-down cannot repopulate the tracker), and
	// LastQuorumAck reads it to honor the LeaseProvider contract
	// ("zero time when the local node is not the leader").
	isLeader atomic.Bool

	// leaderLossCbsMu guards the slice of callbacks invoked when the node
	// transitions out of the leader role (graceful transfer, partition
	// step-down, shutdown). Callbacks fire synchronously from the
	// leader-loss handling path and MUST be non-blocking; a slow
	// callback would hold up refreshStatus / shutdown / fail. See
	// RegisterLeaderLossCallback for the full contract. Each entry
	// carries a sentinel pointer so that the deregister closure
	// returned by RegisterLeaderLossCallback can identify THIS
	// specific registration even if the same fn is registered
	// multiple times.
	leaderLossCbsMu sync.Mutex
	leaderLossCbs   []leaderLossSlot

	// leaderAcquiredCbsMu guards the slice of callbacks invoked when
	// the node transitions INTO the leader role. Callbacks fire
	// synchronously from refreshStatus on the previous!=Leader →
	// status==Leader edge. The MUST-be-non-blocking contract is the
	// same as leaderLossCbs — a slow callback would stall every
	// other holder of the same engine. See
	// RegisterLeaderAcquiredCallback for the full contract.
	//
	// The acquired-side mirror exists so per-shard policy hooks
	// (SQS HT-FIFO leadership-refusal in §8 of the split-queue FIFO
	// design) can audit "we just became leader, are we still
	// allowed to be?" without polling. Pairing the slot with a
	// sentinel pointer mirrors the leader-loss design and lets
	// deregister identify THIS registration when the same fn is
	// registered multiple times.
	leaderAcquiredCbsMu sync.Mutex
	leaderAcquiredCbs   []leaderAcquiredSlot

	pendingProposals map[uint64]proposalRequest
	pendingReads     map[uint64]readRequest
	pendingConfigs   map[uint64]adminRequest
	snapshotInFlight bool
	dispatchFn       func(context.Context, dispatchRequest) error
}

type adminAction byte

const (
	adminActionAddVoter adminAction = iota + 1
	adminActionRemoveServer
	adminActionTransferLeadership
	adminActionAddLearner
	adminActionPromoteLearner
)

type adminRequest struct {
	ctx context.Context
	id  uint64
	// minAppliedIndex is the PromoteLearner precondition: the learner's
	// Progress.Match on the leader must reach this index before the
	// promotion proposal is submitted.
	minAppliedIndex uint64
	// skipMinAppliedCheck explicitly opts out of the precondition.
	// Required when minAppliedIndex == 0; otherwise the engine
	// rejects the request with errPromoteLearnerMinAppliedZero so a
	// missing argument cannot silently disable the safety check.
	skipMinAppliedCheck bool
	action              adminAction
	peer                Peer
	prevIndex           uint64
	done                chan adminResult
}

type adminResult struct {
	index uint64
	peer  Peer
	err   error
}

type proposalRequest struct {
	ctx     context.Context
	id      uint64
	payload []byte
	done    chan proposalResult
}

type proposalResult struct {
	result *raftengine.ProposalResult
	err    error
}

type readRequest struct {
	ctx         context.Context
	id          uint64
	done        chan readResult
	target      uint64
	waitApplied bool
}

type readResult struct {
	index uint64
	err   error
}

type snapshotRequest struct {
	index    uint64
	snapshot Snapshot
}

type snapshotResult struct {
	index uint64
	err   error
}

type dispatchRequest struct {
	msg raftpb.Message
}

// peerQueues holds separate dispatch channels per peer so that heartbeats
// are never blocked behind large log-entry RPCs.
//
// Legacy 2-lane layout (default): heartbeat + normal.
//
// 4-lane layout (opt-in via ELASTICKV_RAFT_DISPATCHER_LANES=1): heartbeat +
// replication (MsgApp/MsgAppResp) + snapshot (MsgSnap) + other. Each lane
// gets its own goroutine so a bulky MsgSnap transfer cannot stall MsgApp
// replication and vice versa. Per-peer ordering within a given message type
// is preserved because a single peer's MsgApp stream all share one lane and
// one worker.
type peerQueues struct {
	normal      chan dispatchRequest
	heartbeat   chan dispatchRequest
	replication chan dispatchRequest // 4-lane mode only; nil otherwise
	snapshot    chan dispatchRequest // 4-lane mode only; nil otherwise
	other       chan dispatchRequest // 4-lane mode only; nil otherwise
	ctx         context.Context
	cancel      context.CancelFunc
}

type preparedOpenState struct {
	cfg   OpenConfig
	peers []Peer
	disk  *diskState
}

// Open starts the etcd/raft backend.
//
// Single-node bootstrap waits for local leadership so callers can use the
// engine immediately. Multi-node startup returns after the local node is
// running; leadership is established asynchronously through raft transport.
func Open(ctx context.Context, cfg OpenConfig) (*Engine, error) {
	prepared, err := prepareOpenState(cfg)
	if err != nil {
		return nil, err
	}

	rawNode, err := newRawNode(prepared.cfg, prepared.disk.Storage)
	if err != nil {
		_ = closePersist(prepared.disk.Persist)
		return nil, err
	}

	peerMap := make(map[uint64]Peer, len(prepared.peers))
	for _, peer := range prepared.peers {
		peerMap[peer.NodeID] = peer
	}
	var dispatchCtx context.Context
	var dispatchCancel context.CancelFunc
	if prepared.cfg.Transport != nil {
		dispatchCtx, dispatchCancel = context.WithCancel(context.Background())
	}
	opened := false
	defer func() {
		if opened || dispatchCancel == nil {
			return
		}
		dispatchCancel()
	}()

	engine := &Engine{
		nodeID:           prepared.cfg.NodeID,
		localID:          prepared.cfg.LocalID,
		localAddress:     prepared.cfg.LocalAddress,
		dataDir:          prepared.cfg.DataDir,
		fsmSnapDir:       filepath.Join(prepared.cfg.DataDir, fsmSnapDirName),
		tickInterval:     prepared.cfg.TickInterval,
		electionTick:     prepared.cfg.ElectionTick,
		joinAsLearner:    prepared.cfg.JoinAsLearner,
		storage:          prepared.disk.Storage,
		rawNode:          rawNode,
		persist:          prepared.disk.Persist,
		fsm:              prepared.cfg.StateMachine,
		peers:            peerMap,
		transport:        prepared.cfg.Transport,
		proposeCh:        make(chan proposalRequest),
		readCh:           make(chan readRequest),
		adminCh:          make(chan adminRequest),
		stepCh:           make(chan raftpb.Message, defaultMaxInflightMsg),
		dispatchReportCh: make(chan dispatchReport, defaultMaxInflightMsg),
		closeCh:          make(chan struct{}),
		doneCh:           make(chan struct{}),
		startedCh:        make(chan struct{}),
		leaderReady:      make(chan struct{}),
		config:           configurationFromConfState(peerMap, prepared.disk.LocalSnap.Metadata.ConfState),
		voterCount:       len(prepared.disk.LocalSnap.Metadata.ConfState.Voters),
		isLearnerNode:    learnerSetFromConfState(prepared.disk.LocalSnap.Metadata.ConfState),
		applied:          maxAppliedIndex(prepared.disk.LocalSnap),
		dispatchCtx:      dispatchCtx,
		dispatchCancel:   dispatchCancel,
		pendingProposals: map[uint64]proposalRequest{},
		pendingReads:     map[uint64]readRequest{},
		pendingConfigs:   map[uint64]adminRequest{},
		// Parse env-tunable retention/snapshot knobs once at Open. Both
		// are consulted on hot paths (maybePersistLocalSnapshot runs per
		// Ready-drain; purge runs after each snapshot persist) and the
		// underlying env parsers emit warnings on malformed input, so
		// re-reading them would flood logs under a misconfiguration.
		snapshotEvery: snapshotEveryFromEnv(),
		maxWALFiles:   maxWALFilesFromEnv(),
	}
	engine.configIndex.Store(maxAppliedIndex(prepared.disk.LocalSnap))
	engine.appliedIndex.Store(maxAppliedIndex(prepared.disk.LocalSnap))
	engine.initTransport(prepared.cfg)
	engine.initSnapshotWorker()
	engine.refreshStatus()
	// Fire the join-as-learner alarm against the persisted snapshot's
	// ConfState. Without this, a node that mis-joined as voter and is
	// then restarted with --raftJoinAsLearner=true would never emit
	// the alarm: applyConfigChange / applyConfigChangeV2 /
	// applyReadySnapshot are the only other call sites and a clean
	// open path bypasses all three (the snapshot is loaded from disk,
	// not replayed). The alarm latch
	// (joinAlarmFired.CompareAndSwap) guarantees it fires at most
	// once per process even if a later snapshot or conf change
	// reapplies the same role assignment. See learner design doc §4.5.
	engine.alarmIfJoinedAsVoter(prepared.disk.LocalSnap.Metadata.ConfState)
	// Surface a misconfiguration where the tick settings produce a
	// non-positive lease window: lease reads would never hit the fast
	// path. Don't fail Open -- the engine is still functional via the
	// slow LinearizableRead path -- but make the degradation visible.
	if lease := engine.LeaseDuration(); lease <= 0 {
		slog.Warn("etcd raft engine: lease read disabled (non-positive LeaseDuration)",
			slog.Duration("tick_interval", engine.tickInterval),
			slog.Int("election_tick", engine.electionTick),
			slog.Duration("lease_safety_margin", leaseSafetyMargin),
			slog.Duration("computed_lease", lease),
		)
	}

	go engine.run()

	openedEngine, err := waitForOpen(ctx, engine, len(prepared.peers) == 1)
	if err != nil {
		return nil, err
	}
	opened = true
	return openedEngine, nil
}

func prepareOpenState(cfg OpenConfig) (preparedOpenState, error) {
	cfg, persistedPeers, persistedPeersOK, err := normalizeOpenConfig(cfg)
	if err != nil {
		return preparedOpenState{}, err
	}
	if err := validateConfig(cfg); err != nil {
		return preparedOpenState{}, err
	}

	localPeer, peers, err := normalizePeers(cfg.NodeID, cfg.LocalID, cfg.LocalAddress, cfg.Peers, persistedPeersOK, cfg.Bootstrap)
	if err != nil {
		return preparedOpenState{}, err
	}
	cfg.NodeID = localPeer.NodeID
	cfg.LocalID = localPeer.ID
	cfg.LocalAddress = localPeer.Address

	disk, err := openDiskState(cfg, peers)
	if err != nil {
		return preparedOpenState{}, err
	}
	if err := validateOpenPeers(disk.LocalSnap, peers, persistedPeers, persistedPeersOK); err != nil {
		_ = closePersist(disk.Persist)
		return preparedOpenState{}, err
	}
	// Annotate suffrage from the persisted snapshot's ConfState before
	// saving the peers file. The operator-supplied peer list has no
	// suffrage info; without this, savePersistedPeers would write
	// every peer as a voter, then a learner's restart would land on
	// a peers file that contradicts the snapshot's ConfState.Learners
	// and validateConfState would reject the cluster on next open.
	annotatePeerSuffrageInSlice(peers, disk.LocalSnap.Metadata.ConfState)
	if err := savePersistedPeers(cfg.DataDir, maxUint64(maxAppliedIndex(disk.LocalSnap), persistedPeers.Index), peers); err != nil {
		_ = closePersist(disk.Persist)
		return preparedOpenState{}, err
	}
	return preparedOpenState{
		cfg:   cfg,
		peers: peers,
		disk:  disk,
	}, nil
}

func (e *Engine) initTransport(cfg OpenConfig) {
	if e.transport == nil {
		return
	}
	// Transport listeners may already be accepting RPCs when Open is called.
	// Gate inbound delivery on startedCh so messages are not enqueued until the
	// local run loop has completed startup.
	e.peerDispatchers = make(map[uint64]*peerQueues, len(e.peers))
	// Size the per-peer dispatch buffer to match the Raft inflight limit so that
	// the channel never drops messages that Raft's flow-control would permit.
	e.perPeerQueueSize = cfg.MaxInflightMsg
	e.dispatcherLanesEnabled = dispatcherLanesEnabledFromEnv()
	e.dispatchStopCh = make(chan struct{})
	e.transport.SetSpoolDir(cfg.DataDir)
	e.transport.SetFSMSnapDir(e.fsmSnapDir)
	e.transport.SetFSMPayloadReader(e.readFSMPayloadLocked)
	e.transport.SetFSMPayloadOpener(e.openFSMPayloadLocked)
	e.transport.SetHandler(e.handleTransportMessage)
	e.startDispatchWorkers()
}

func (e *Engine) initSnapshotWorker() {
	if e.persist == nil {
		return
	}
	// Local snapshot persistence is only wired for disk-backed engines. When
	// persist is nil the prototype intentionally leaves snapshot workers nil
	// and maybePersistLocalSnapshot becomes a no-op.
	e.snapshotReqCh = make(chan snapshotRequest, defaultSnapshotQueueSize)
	e.snapshotResCh = make(chan snapshotResult, 1)
	e.snapshotStopCh = make(chan struct{})
	e.startSnapshotWorker()
}

func newRawNode(cfg OpenConfig, storage *etcdraft.MemoryStorage) (*etcdraft.RawNode, error) {
	rawNode, err := etcdraft.NewRawNode(&etcdraft.Config{
		ID:                        cfg.NodeID,
		ElectionTick:              cfg.ElectionTick,
		HeartbeatTick:             cfg.HeartbeatTick,
		Storage:                   storage,
		MaxSizePerMsg:             cfg.MaxSizePerMsg,
		MaxCommittedSizePerReady:  cfg.MaxSizePerMsg,
		MaxInflightMsgs:           cfg.MaxInflightMsg,
		CheckQuorum:               true,
		PreVote:                   true,
		ReadOnlyOption:            etcdraft.ReadOnlySafe,
		DisableProposalForwarding: true,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return rawNode, nil
}

func waitForOpen(ctx context.Context, engine *Engine, waitForLeader bool) (*Engine, error) {
	select {
	case <-ctx.Done():
		_ = engine.Close()
		return nil, errors.WithStack(ctx.Err())
	case <-engine.openReady(waitForLeader):
		return engine, nil
	case <-engine.doneCh:
		if err := engine.currentError(); err != nil {
			return nil, err
		}
		return nil, errors.WithStack(errClosed)
	}
}

func (e *Engine) Close() error {
	if e == nil {
		return nil
	}
	e.closeOnce.Do(func() {
		close(e.closeCh)
	})
	<-e.doneCh
	if err := e.currentError(); err != nil && !errors.Is(err, errClosed) {
		return err
	}
	return nil
}

func (e *Engine) Propose(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	if err := contextErr(ctx); err != nil {
		return nil, err
	}
	if e == nil {
		return nil, errors.WithStack(errNilEngine)
	}

	req := proposalRequest{
		ctx:     ctx,
		id:      e.nextID(),
		payload: append([]byte(nil), data...),
		done:    make(chan proposalResult, 1),
	}

	select {
	case <-ctx.Done():
		return nil, errors.WithStack(ctx.Err())
	case <-e.doneCh:
		return nil, e.currentErrorOrClosed()
	case e.proposeCh <- req:
	}

	select {
	case <-ctx.Done():
		e.cancelPendingProposal(req.id)
		return nil, errors.WithStack(ctx.Err())
	case res := <-req.done:
		if res.err != nil {
			return nil, res.err
		}
		return res.result, nil
	}
}

func (e *Engine) State() raftengine.State {
	if e == nil {
		return raftengine.StateUnknown
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.status.State
}

func (e *Engine) Leader() raftengine.LeaderInfo {
	if e == nil {
		return raftengine.LeaderInfo{}
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.status.Leader
}

func (e *Engine) VerifyLeader(ctx context.Context) error {
	_, err := e.submitRead(ctx, false)
	return err
}

func (e *Engine) CheckServing(ctx context.Context) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	if e == nil {
		return errors.WithStack(errNilEngine)
	}
	if e.State() != raftengine.StateLeader {
		return errors.WithStack(errNotLeader)
	}
	return nil
}

func (e *Engine) LinearizableRead(ctx context.Context) (uint64, error) {
	return e.submitRead(ctx, true)
}

// LeaseDuration returns the time during which a lease holder can serve
// reads from local state without re-confirming leadership via ReadIndex.
// It is bounded by electionTimeout - leaseSafetyMargin so that the lease
// expires before a successor leader could realistically be elected and
// accept new writes elsewhere.
func (e *Engine) LeaseDuration() time.Duration {
	if e == nil {
		return 0
	}
	tick := e.tickInterval
	if tick <= 0 {
		tick = defaultTickInterval
	}
	election := e.electionTick
	if election <= 0 {
		election = defaultElectionTick
	}
	d := time.Duration(election)*tick - leaseSafetyMargin
	if d < 0 {
		return 0
	}
	return d
}

// AppliedIndex returns the highest log index applied to the local FSM.
// Suitable for callers that need a non-blocking read fence equivalent
// to what LinearizableRead would have returned, paired with an
// external quorum confirmation (e.g. a valid lease).
//
// Lock-free: reads the mirrored atomic.Uint64 written by the run
// loop's apply path (and by Restore's snapshot installation), so the
// lease-read fast path does not contend with refreshStatus's write
// lock under high read concurrency.
func (e *Engine) AppliedIndex() uint64 {
	if e == nil {
		return 0
	}
	return e.appliedIndex.Load()
}

// LastQuorumAck returns the monotonic-raw instant by which a majority
// of followers most recently responded to the leader, or the zero
// Instant when no such observation exists (follower / candidate /
// startup).
//
// Lock-free: reads atomic.Int64 values published by recordQuorumAck
// (multi-node cluster) or refreshStatus (single-node cluster keeps
// singleNodeLeaderAckMonoNs alive with monoclock.Now() while leader,
// so the hot lease-read path performs zero lock work). The monotonic-
// raw source keeps the lease safe against NTP-slew / wall-clock-step
// events; see raftengine.LeaseProvider for the correctness contract.
func (e *Engine) LastQuorumAck() monoclock.Instant {
	if e == nil {
		return monoclock.Zero
	}
	// Honor the LeaseProvider contract that non-leaders always return
	// the zero Instant. Without this guard a late MsgAppResp that
	// sneaks past recordQuorumAck (or a tracker entry that survived a
	// brief step-down/step-up window) could leak stale liveness into
	// the caller's fast-path validation.
	if !e.isLeader.Load() {
		return monoclock.Zero
	}
	if ns := e.singleNodeLeaderAckMonoNs.Load(); ns != 0 {
		return monoclock.FromNanos(ns)
	}
	return e.ackTracker.load()
}

// DispatchDropCount returns the total number of outbound raft messages
// dropped before hitting the transport because the per-peer normal or
// heartbeat channel was full. Monotonic across the life of the engine.
// Surfaced to Prometheus via the monitoring package so the hot-path
// dashboard can graph stepCh saturation alongside LinearizableRead
// rate (see the "Hot Path" row in monitoring/grafana/dashboards/elastickv-redis-summary.json).
func (e *Engine) DispatchDropCount() uint64 {
	if e == nil {
		return 0
	}
	return e.dispatchDropCount.Load()
}

// DispatchErrorCount returns the total number of outbound raft
// dispatches that reached the transport but failed (network errors,
// remote shutdown, etc.). Monotonic across the life of the engine.
func (e *Engine) DispatchErrorCount() uint64 {
	if e == nil {
		return 0
	}
	return e.dispatchErrorCount.Load()
}

// DispatchErrorCountsByCode returns a snapshot of dispatch-error
// counts keyed by grpc status code ("Unavailable",
// "DeadlineExceeded", "ResourceExhausted", ...). Sum of values
// equals DispatchErrorCount(). A separate breakdown is needed to
// tell whether failures are peer-down (Unavailable), leader under
// load (DeadlineExceeded), or flow-control (ResourceExhausted).
// Returns an empty map when e is nil. Safe for concurrent callers;
// the returned map is a copy.
func (e *Engine) DispatchErrorCountsByCode() map[string]uint64 {
	if e == nil {
		return map[string]uint64{}
	}
	e.dispatchErrorByCodeMu.Lock()
	defer e.dispatchErrorByCodeMu.Unlock()
	if len(e.dispatchErrorByCode) == 0 {
		return map[string]uint64{}
	}
	out := make(map[string]uint64, len(e.dispatchErrorByCode))
	for k, v := range e.dispatchErrorByCode {
		out[k] = v
	}
	return out
}

// recordDispatchErrorCode atomically increments both the aggregate
// dispatchErrorCount and the per-code bucket. code should be the
// grpc status code string from the error; callers that cannot
// extract one pass "Unknown".
func (e *Engine) recordDispatchErrorCode(code string) uint64 {
	count := e.dispatchErrorCount.Add(1)
	e.dispatchErrorByCodeMu.Lock()
	if e.dispatchErrorByCode == nil {
		e.dispatchErrorByCode = map[string]uint64{}
	}
	e.dispatchErrorByCode[code]++
	e.dispatchErrorByCodeMu.Unlock()
	return count
}

// StepQueueFullCount returns the total number of inbound raft messages
// that could not be enqueued into stepCh because the channel was at
// capacity. This is the "etcd raft inbound step queue is full" signal
// from the task description: a spike indicates the local raft loop
// is starved, usually by something blocking the apply path such as
// the pre-#560 rawKeyTypeAt seek storm.
func (e *Engine) StepQueueFullCount() uint64 {
	if e == nil {
		return 0
	}
	return e.stepQueueFullCount.Load()
}

// RegisterLeaderLossCallback registers fn to fire every time the local
// node's Raft state transitions out of leader (CheckQuorum step-down,
// graceful transfer completion, partition-induced demotion) and also
// on shutdown() while the node was still leader. Callbacks are NOT
// fired at the moment a transfer starts (LeadTransferee != 0); they
// only fire once the transfer completes and state flips to follower.
// Lease-read callers use this to invalidate cached lease state so the
// next read takes the slow path.
//
// Callbacks run synchronously from refreshStatus / shutdown / fail
// and MUST be non-blocking (each should be a fast, lock-free
// invalidation). A panic inside a callback is contained and logged
// so a bug in one holder cannot crash the engine or break other
// callbacks. LeaseRead also guards its fast path on
// engine.State() == StateLeader so the small window between the
// transition and this callback completing cannot serve stale reads.
//
// The returned deregister function removes this specific registration
// and is safe to call multiple times. Long-lived callers (coordinators
// whose lifetime matches the engine's) may ignore it; shorter-lived
// callers MUST invoke it to avoid accumulating dead callbacks in the
// engine's slice.
func (e *Engine) RegisterLeaderLossCallback(fn func()) (deregister func()) {
	if e == nil || fn == nil {
		return func() {}
	}
	return registerLeaderCallback(&e.leaderLossCbsMu, &e.leaderLossCbs, fn)
}

// leaderLossSlot pairs a registered callback with an id-only sentinel
// pointer so deregister can distinguish identical fn values. Aliased
// to leaderCallbackSlot so the leader-loss and leader-acquired slices
// share the same in-memory shape — the register / fire helpers are
// generic over this single slot type.
type leaderLossSlot = leaderCallbackSlot

// fireLeaderLossCallbacks invokes all registered callbacks
// synchronously. The registered-callback contract requires each fn
// to be non-blocking (a lock-free lease-invalidate flag flip), so
// inline execution is safe and avoids spawning an unbounded number
// of goroutines per leader-loss event when many shards / coordinators
// are registered.
//
// A panicking callback is still contained (see
// invokeLeaderLossCallback) so a bug in one holder cannot break
// subsequent callbacks or crash the process.
func (e *Engine) fireLeaderLossCallbacks() {
	for _, fn := range gatherLeaderCallbacks(&e.leaderLossCbsMu, &e.leaderLossCbs) {
		e.invokeLeaderLossCallback(fn)
	}
}

// leaderCallbackSlot is the shared on-disk shape for leader-loss
// and leader-acquired callback registrations. The id pointer is a
// per-registration sentinel so deregister can target THIS specific
// entry even when the same fn is registered multiple times.
type leaderCallbackSlot struct {
	id *struct{ fn func() }
	fn func()
}

// registerLeaderCallback installs fn into the (mu, cbs) callback
// slice and returns a deregister closure. Shared by leader-loss
// and leader-acquired registration so the slot-management /
// dangling-reference / sync.Once-deregister logic lives in one
// place. The two callback families differ only in which (mu, slice)
// pair they target — the firing semantics, the sentinel-pointer
// disambiguation, and the GC-safe slice truncation are identical.
func registerLeaderCallback(mu *sync.Mutex, cbs *[]leaderCallbackSlot, fn func()) (deregister func()) {
	// Allocate a unique sentinel pointer so the deregister closure
	// can identify THIS specific registration even if the same fn
	// is registered multiple times.
	slot := &struct{ fn func() }{fn: fn}
	mu.Lock()
	*cbs = append(*cbs, leaderCallbackSlot{id: slot, fn: fn})
	mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			mu.Lock()
			defer mu.Unlock()
			for i, c := range *cbs {
				if c.id != slot {
					continue
				}
				// Remove without leaving a dangling reference at
				// the tail of the underlying array. The removed
				// slot's fn typically captures a *Coordinate; a
				// plain `append(cbs[:i], cbs[i+1:]...)` would keep
				// the old backing cell alive and prevent GC of the
				// associated Coordinate until the engine itself is
				// dropped.
				last := len(*cbs) - 1
				copy((*cbs)[i:], (*cbs)[i+1:])
				(*cbs)[last] = leaderCallbackSlot{}
				*cbs = (*cbs)[:last]
				return
			}
		})
	}
}

// gatherLeaderCallbacks copies the live fn list out from under the
// mutex so callers can fire them without holding the lock.
// Mirrors the snapshot-then-fire pattern used by the per-callback
// invoke helpers.
//
// Takes a *pointer* to the slice (not the slice itself) so the
// header (pointer, length, capacity) is read INSIDE the locked
// section. Passing the slice by value would dereference the
// header at the call site — i.e. before mu.Lock() — racing with
// any concurrent registerLeaderCallback. The pointer parameter
// closes that race (codex P1 / gemini high on PR #723).
func gatherLeaderCallbacks(mu *sync.Mutex, cbs *[]leaderCallbackSlot) []func() {
	mu.Lock()
	defer mu.Unlock()
	out := make([]func(), len(*cbs))
	for i, c := range *cbs {
		out[i] = c.fn
	}
	return out
}

// RegisterLeaderAcquiredCallback registers fn to fire every time
// the local node's Raft state transitions INTO leader (initial
// election win, re-election after partition heal, leadership
// transfer target completion). Callbacks fire on the
// previous!=Leader → status==Leader edge in refreshStatus, after
// e.isLeader has been published, so a callback that reads
// engine.State() observes StateLeader.
//
// Use case: per-shard policy that needs to audit a freshly-acquired
// leadership ("am I still allowed to be leader of this group?").
// SQS HT-FIFO leadership-refusal (§8 of the split-queue FIFO
// design) hangs off this hook to TransferLeadership when the
// binary lacks the htfifo capability but a partitioned queue is
// mapped to this Raft group.
//
// Callbacks run synchronously from refreshStatus and MUST be
// non-blocking — same contract as RegisterLeaderLossCallback. A
// callback wanting to do real work (e.g. enumerate the catalog,
// call TransferLeadership) MUST offload to a goroutine.
//
// A panic inside a callback is contained and logged so a bug in
// one holder cannot crash the engine or break other callbacks.
//
// The returned deregister function removes this specific
// registration and is safe to call multiple times.
func (e *Engine) RegisterLeaderAcquiredCallback(fn func()) (deregister func()) {
	if e == nil || fn == nil {
		return func() {}
	}
	return registerLeaderCallback(&e.leaderAcquiredCbsMu, &e.leaderAcquiredCbs, fn)
}

// leaderAcquiredSlot is the leader-acquired companion to
// leaderLossSlot — both alias the shared leaderCallbackSlot so a
// single set of register / fire helpers serves both transitions.
type leaderAcquiredSlot = leaderCallbackSlot

// fireLeaderAcquiredCallbacks invokes all registered callbacks
// synchronously. Same panic-containment + non-blocking contract
// as fireLeaderLossCallbacks.
func (e *Engine) fireLeaderAcquiredCallbacks() {
	for _, fn := range gatherLeaderCallbacks(&e.leaderAcquiredCbsMu, &e.leaderAcquiredCbs) {
		e.invokeLeaderAcquiredCallback(fn)
	}
}

func (e *Engine) invokeLeaderAcquiredCallback(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			// Mirror the leader-loss panic log shape so operator
			// triage of either family produces the same fields.
			// Without node identity and the stack, an SQS
			// leadership-refusal hook panicking in production
			// would leave only the recovered value to grep on
			// (gemini medium / claude finding 1 on PR #723).
			slog.Error("etcd raft engine: leader-acquired callback panicked",
				slog.String("node_id", e.localID),
				slog.Uint64("raft_node_id", e.nodeID),
				slog.Any("panic", r),
				slog.String("stack", string(debug.Stack())),
			)
		}
	}()
	fn()
}

func (e *Engine) invokeLeaderLossCallback(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			// A buggy lease holder must not crash the node. Log the
			// recovery so operators can see lease-invalidation hooks
			// misbehaving in production; swallow the panic so the
			// engine status loop / shutdown path continues.
			//
			// Note: if a callback panics before it invalidates its
			// lease, fast-path reads on that lease keep succeeding
			// until wall-clock expiry. Safety is then bounded by the
			// lease duration (strictly shorter than electionTimeout),
			// not by the slow-path re-verification. The slow path
			// re-verifies leadership only once the lease has
			// naturally expired.
			slog.Error("etcd raft engine: leader-loss callback panicked",
				slog.String("node_id", e.localID),
				slog.Uint64("raft_node_id", e.nodeID),
				slog.Any("panic", r),
				slog.String("stack", string(debug.Stack())),
			)
		}
	}()
	fn()
}

func (e *Engine) submitRead(ctx context.Context, waitApplied bool) (uint64, error) {
	if err := contextErr(ctx); err != nil {
		return 0, err
	}
	if e == nil {
		return 0, errors.WithStack(errNilEngine)
	}
	req := readRequest{
		ctx:         ctx,
		id:          e.nextID(),
		done:        make(chan readResult, 1),
		waitApplied: waitApplied,
	}

	select {
	case <-ctx.Done():
		return 0, errors.WithStack(ctx.Err())
	case <-e.doneCh:
		return 0, e.currentErrorOrClosed()
	case e.readCh <- req:
	}

	select {
	case <-ctx.Done():
		e.cancelPendingRead(req.id)
		return 0, errors.WithStack(ctx.Err())
	case res := <-req.done:
		if res.err != nil {
			return 0, res.err
		}
		return res.index, nil
	}
}

func (e *Engine) Status() raftengine.Status {
	if e == nil {
		return raftengine.Status{State: raftengine.StateUnknown}
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.status
}

func (e *Engine) Configuration(ctx context.Context) (raftengine.Configuration, error) {
	if err := contextErr(ctx); err != nil {
		return raftengine.Configuration{}, err
	}
	if e == nil {
		return raftengine.Configuration{}, errors.WithStack(errNilEngine)
	}
	return e.currentConfiguration(), nil
}

func (e *Engine) AddVoter(ctx context.Context, id string, address string, prevIndex uint64) (uint64, error) {
	peer, err := e.resolveAdminPeer(id, address)
	if err != nil {
		return 0, err
	}
	result, err := e.submitAdmin(ctx, adminActionAddVoter, peer, prevIndex)
	if err != nil {
		return 0, err
	}
	return result.index, nil
}

// AddLearner attaches a non-voting replica. The learner is added via
// V1 ConfChangeAddLearnerNode and starts receiving log entries
// immediately, but does not contribute to the voter quorum until
// promoted with PromoteLearner.
func (e *Engine) AddLearner(ctx context.Context, id string, address string, prevIndex uint64) (uint64, error) {
	peer, err := e.resolveAdminPeer(id, address)
	if err != nil {
		return 0, err
	}
	result, err := e.submitAdmin(ctx, adminActionAddLearner, peer, prevIndex)
	if err != nil {
		return 0, err
	}
	return result.index, nil
}

// PromoteLearner promotes an existing learner to voter via V1
// ConfChangeAddNode. The minAppliedIndex precondition rejects the
// call if the learner's leader-side Progress.Match has not reached
// that index yet, so an operator who promotes too eagerly gets a
// clean FailedPrecondition error instead of a silent quorum stall.
//
// minAppliedIndex == 0 is rejected unless skipMinAppliedCheck is
// also true. Set skipMinAppliedCheck only when the catch-up has
// been confirmed out-of-band.
func (e *Engine) PromoteLearner(ctx context.Context, id string, prevIndex uint64, minAppliedIndex uint64, skipMinAppliedCheck bool) (uint64, error) {
	if id == "" {
		return 0, errors.WithStack(errPeerIDRequired)
	}
	if minAppliedIndex == 0 && !skipMinAppliedCheck {
		return 0, errors.WithStack(errPromoteLearnerMinAppliedZero)
	}
	result, err := e.submitAdminEx(ctx, adminRequest{
		action:              adminActionPromoteLearner,
		peer:                Peer{ID: id},
		prevIndex:           prevIndex,
		minAppliedIndex:     minAppliedIndex,
		skipMinAppliedCheck: skipMinAppliedCheck,
	})
	if err != nil {
		return 0, err
	}
	return result.index, nil
}

func (e *Engine) RemoveServer(ctx context.Context, id string, prevIndex uint64) (uint64, error) {
	result, err := e.submitAdmin(ctx, adminActionRemoveServer, Peer{ID: id}, prevIndex)
	if err != nil {
		return 0, err
	}
	return result.index, nil
}

func (e *Engine) TransferLeadership(ctx context.Context) error {
	result, err := e.submitAdmin(ctx, adminActionTransferLeadership, Peer{}, 0)
	if err != nil {
		return err
	}
	return e.waitForLeadershipTransfer(ctx, result.peer)
}

func (e *Engine) TransferLeadershipToServer(ctx context.Context, id string, address string) error {
	result, err := e.submitAdmin(ctx, adminActionTransferLeadership, Peer{ID: id, Address: address}, 0)
	if err != nil {
		return err
	}
	return e.waitForLeadershipTransfer(ctx, result.peer)
}

func (e *Engine) submitAdmin(ctx context.Context, action adminAction, peer Peer, prevIndex uint64) (adminResult, error) {
	return e.submitAdminEx(ctx, adminRequest{
		action:    action,
		peer:      peer,
		prevIndex: prevIndex,
	})
}

// submitAdminEx is the underlying admin submit path. The caller
// populates action, peer, prevIndex, and (for PromoteLearner)
// minAppliedIndex; ctx, id, and done are filled in here so the
// request integrates with the engine event loop the same way the
// existing AddVoter / RemoveServer paths do.
func (e *Engine) submitAdminEx(ctx context.Context, req adminRequest) (adminResult, error) {
	if err := contextErr(ctx); err != nil {
		return adminResult{}, err
	}
	if e == nil {
		return adminResult{}, errors.WithStack(errNilEngine)
	}
	req.ctx = ctx
	req.id = e.nextID()
	req.done = make(chan adminResult, 1)

	select {
	case <-ctx.Done():
		return adminResult{}, errors.WithStack(ctx.Err())
	case <-e.doneCh:
		return adminResult{}, e.currentErrorOrClosed()
	case e.adminCh <- req:
	}

	select {
	case <-ctx.Done():
		e.cancelPendingConfig(req.id)
		return adminResult{}, errors.WithStack(ctx.Err())
	case res := <-req.done:
		if res.err != nil {
			return adminResult{}, res.err
		}
		return res, nil
	}
}

func (e *Engine) waitForLeadershipTransfer(ctx context.Context, target Peer) error {
	if target.NodeID == 0 {
		return errors.WithStack(errLeadershipTransferTarget)
	}
	ticker := time.NewTicker(defaultAdminPollInterval)
	defer ticker.Stop()

	// etcd/raft sets leadTransferee synchronously inside TransferLeader, so by
	// the time we observe the first Status snapshot after submitting the admin
	// request it should be non-zero. If we then observe it drop back to zero
	// while we are still leader, raft aborted the transfer (electionTimeout
	// elapsed) — surface that as an error instead of polling indefinitely.
	sawTransferPending := false
	for {
		done, err := e.checkLeadershipTransfer(ctx, target, &sawTransferPending)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case <-e.doneCh:
			return e.currentErrorOrClosed()
		case <-ticker.C:
		}
	}
}

// checkLeadershipTransfer returns (done, err). done==true means the transfer
// succeeded; a non-nil error indicates either context cancellation or that
// raft aborted the transfer.
func (e *Engine) checkLeadershipTransfer(ctx context.Context, target Peer, sawPending *bool) (bool, error) {
	if err := contextErr(ctx); err != nil {
		return false, err
	}
	status := e.Status()
	if status.State != raftengine.StateLeader {
		if status.Leader.ID == target.ID {
			return true, nil
		}
		// We stepped down but a different node is leader — transfer landed on
		// the wrong peer (e.g., target lost election and another peer won).
		// Treat as aborted so the caller doesn't spin until its deadline.
		if status.Leader.ID != "" {
			return false, errors.WithStack(errLeadershipTransferAborted)
		}
		// No known leader yet; keep polling until one is elected.
		return false, nil
	}
	// Match the transferee against the specific target to avoid tracking a
	// stale or unrelated transfer that was initiated by a different caller.
	if status.LeadTransferee == target.NodeID {
		*sawPending = true
		return false, nil
	}
	if *sawPending {
		return false, errors.WithStack(errLeadershipTransferAborted)
	}
	return false, nil
}

func (e *Engine) run() {
	defer close(e.doneCh)

	ticker := time.NewTicker(e.tickInterval)
	defer ticker.Stop()

	if err := e.startup(); err != nil {
		e.fail(err)
		return
	}
	e.markStarted()

	for {
		if err := e.drainReady(); err != nil {
			e.fail(err)
			return
		}

		ok, err := e.handleEvent(ticker.C)
		if err != nil {
			e.fail(err)
			return
		}
		if !ok {
			e.shutdown()
			return
		}
	}
}

func (e *Engine) startup() error {
	if err := e.drainReady(); err != nil {
		return err
	}
	if e.State() == raftengine.StateLeader {
		return nil
	}
	if !e.shouldCampaignSingleNode() {
		return nil
	}
	if err := e.rawNode.Campaign(); err != nil {
		return errors.WithStack(err)
	}
	return e.drainReady()
}

func (e *Engine) handleEvent(tick <-chan time.Time) (bool, error) {
	select {
	case <-e.closeCh:
		return false, nil
	case <-tick:
		e.rawNode.Tick()
	case req := <-e.proposeCh:
		e.handleProposal(req)
	case req := <-e.readCh:
		e.handleRead(req)
	case req := <-e.adminCh:
		e.handleAdmin(req)
	case msg := <-e.stepCh:
		e.handleStep(msg)
	case report := <-e.dispatchReportCh:
		e.handleDispatchReport(report)
	case result := <-e.snapshotResCh:
		if err := e.handleSnapshotResult(result); err != nil {
			return false, err
		}
	}
	return true, nil
}

// dispatchReport is posted by the dispatch workers when a transport send
// to a peer fails; the engine goroutine drains these and informs etcd/raft
// via rawNode so follower Progress leaves StateReplicate / StateSnapshot on
// unreachable peers and does not silently stall.
type dispatchReport struct {
	to      uint64
	msgType raftpb.MessageType
}

func (e *Engine) handleDispatchReport(report dispatchReport) {
	if e.rawNode == nil {
		return
	}
	// MsgSnap requires the distinct SnapshotFailure path: raft tracks
	// PendingSnapshot in Progress, and only ReportSnapshot clears it.
	// All other message types use ReportUnreachable, which transitions the
	// peer from StateReplicate to StateProbe so the next heartbeat response
	// drives a fresh sendAppend attempt.
	if report.msgType == raftpb.MsgSnap {
		e.rawNode.ReportSnapshot(report.to, etcdraft.SnapshotFailure)
		return
	}
	e.rawNode.ReportUnreachable(report.to)
}

// postDispatchReport delivers a dispatch failure to the event loop without
// blocking the worker. If the channel is full (unlikely — the buffer is
// sized to MaxInflightMsg), the report is dropped and logged; this is
// acceptable because raft will retry on the next tick and we only need
// eventual consistency between transport state and Progress state.
func (e *Engine) postDispatchReport(report dispatchReport) {
	select {
	case e.dispatchReportCh <- report:
	case <-e.closeCh:
	default:
		slog.Warn("etcd raft dispatch report dropped (channel full)",
			"to", report.to,
			"type", report.msgType.String(),
		)
	}
}

func (e *Engine) handleProposal(req proposalRequest) {
	if err := contextErr(req.ctx); err != nil {
		req.done <- proposalResult{err: err}
		return
	}
	if e.State() != raftengine.StateLeader {
		req.done <- proposalResult{err: errors.WithStack(errNotLeader)}
		return
	}
	// etcd/raft drops proposals while a leadership transfer is in flight
	// (LeadTransferee != 0) and returns ErrProposalDropped. Map that to
	// the shared ErrLeadershipTransferInProgress sentinel so callers
	// (lease-read invalidation, proxy retry, etc.) can recognise it via
	// errors.Is instead of getting a generic dropped-proposal error.
	if e.rawNode.BasicStatus().LeadTransferee != 0 {
		req.done <- proposalResult{err: errors.WithStack(errLeadershipTransferInProgress)}
		return
	}
	e.storePendingProposal(req)
	if err := e.rawNode.Propose(encodeProposalEnvelope(req.id, req.payload)); err != nil {
		e.cancelPendingProposal(req.id)
		req.done <- proposalResult{err: errors.WithStack(err)}
	}
}

func (e *Engine) handleRead(req readRequest) {
	if err := contextErr(req.ctx); err != nil {
		req.done <- readResult{err: err}
		return
	}
	if e.State() != raftengine.StateLeader {
		req.done <- readResult{err: errors.WithStack(errNotLeader)}
		return
	}
	// etcd/raft silently drops MsgReadIndex while a leadership transfer
	// is in flight (LeadTransferee != 0) -- ReadIndex does not return an
	// error on drop, so without this fast-fail the caller would block on
	// req.done until ctx deadline. Surface the drop as
	// ErrLeadershipTransferInProgress so LeaseRead falls through to the
	// new leader instead of stalling ~electionTimeout.
	if e.rawNode.BasicStatus().LeadTransferee != 0 {
		req.done <- readResult{err: errors.WithStack(errLeadershipTransferInProgress)}
		return
	}
	e.storePendingRead(req)
	e.rawNode.ReadIndex(encodeReadContext(req.id))
}

func (e *Engine) handleAdmin(req adminRequest) {
	if err := contextErr(req.ctx); err != nil {
		req.done <- adminResult{err: err}
		return
	}
	if e.State() != raftengine.StateLeader {
		req.done <- adminResult{err: errors.WithStack(errNotLeader)}
		return
	}
	currentIndex := e.currentConfigIndex()
	if req.prevIndex != 0 && req.prevIndex != currentIndex {
		req.done <- adminResult{err: errors.Wrapf(errConfigIndexMismatch, "got %d want %d", req.prevIndex, currentIndex)}
		return
	}

	e.dispatchAdminAction(req)
}

func (e *Engine) dispatchAdminAction(req adminRequest) {
	switch req.action {
	case adminActionAddVoter:
		e.handleAddVoter(req)
	case adminActionAddLearner:
		e.handleAddLearner(req)
	case adminActionPromoteLearner:
		e.handlePromoteLearner(req)
	case adminActionRemoveServer:
		e.handleRemoveServer(req)
	case adminActionTransferLeadership:
		e.handleTransferLeadership(req)
	default:
		req.done <- adminResult{err: errors.New("unknown admin action")}
	}
}

func (e *Engine) handleAddVoter(req adminRequest) {
	e.proposeMembershipChange(req, raftpb.ConfChangeAddNode, req.peer)
}

// handleAddLearner submits a ConfChangeAddLearnerNode for a fresh
// non-voting peer. It runs on the single-threaded admin loop.
func (e *Engine) handleAddLearner(req adminRequest) {
	e.proposeMembershipChange(req, raftpb.ConfChangeAddLearnerNode, req.peer)
}

// handlePromoteLearner promotes an existing learner to voter via
// ConfChangeAddNode. The two preconditions both run synchronously
// here, before the propose, so a failed precondition does not enter
// the log:
//  1. the peer must currently be a learner (in ConfState.Learners,
//     not in ConfState.Voters);
//  2. when minAppliedIndex is non-zero, the leader-side
//     Progress[nodeID].Match must already be >= minAppliedIndex.
func (e *Engine) handlePromoteLearner(req adminRequest) {
	peer, ok := e.peerForID(req.peer.ID)
	if !ok {
		req.done <- adminResult{err: errors.Wrapf(errTransportPeerUnknown, "id=%q", req.peer.ID)}
		return
	}
	e.mu.RLock()
	isLearner := e.isLearnerNode[peer.NodeID]
	e.mu.RUnlock()
	if !isLearner {
		req.done <- adminResult{err: errors.Wrapf(errPromoteLearnerNotLearner, "id=%q", req.peer.ID)}
		return
	}
	// PromoteLearner gates on a minAppliedIndex catch-up check by
	// default; only an explicit skipMinAppliedCheck disables it. The
	// engine-side guard in PromoteLearner already rejects
	// minAppliedIndex == 0 without skipMinAppliedCheck, so by the
	// time we reach the apply-loop handler one of these is true:
	//   * minAppliedIndex > 0  -> verify against Progress.Match
	//   * skipMinAppliedCheck  -> caller has confirmed out-of-band
	if !req.skipMinAppliedCheck {
		// rawNode.Status() is safe here: handlePromoteLearner runs on
		// the same goroutine that owns rawNode, so no clone or lock
		// is required.
		status := e.rawNode.Status()
		progress, ok := status.Progress[peer.NodeID]
		if !ok {
			req.done <- adminResult{err: errors.Wrapf(errPromoteLearnerNoProgress, "id=%q node=%d", peer.ID, peer.NodeID)}
			return
		}
		if progress.Match < req.minAppliedIndex {
			req.done <- adminResult{err: errors.Wrapf(errPromoteLearnerNotCaughtUp, "id=%q match=%d want>=%d", peer.ID, progress.Match, req.minAppliedIndex)}
			return
		}
	}
	e.proposeMembershipChange(req, raftpb.ConfChangeAddNode, peer)
}

// proposeMembershipChange wraps the encode + storePendingConfig +
// ProposeConfChange dance shared by AddVoter / AddLearner /
// PromoteLearner. The caller has already validated the leader and
// prevIndex preconditions in handleAdmin.
func (e *Engine) proposeMembershipChange(req adminRequest, changeType raftpb.ConfChangeType, peer Peer) {
	contextBytes, err := encodeConfChangeContext(req.id, peer)
	if err != nil {
		req.done <- adminResult{err: err}
		return
	}
	cc := raftpb.ConfChange{
		Type:    changeType,
		NodeID:  peer.NodeID,
		Context: contextBytes,
	}
	if err := e.storePendingConfig(req); err != nil {
		req.done <- adminResult{err: err}
		return
	}
	if err := e.rawNode.ProposeConfChange(cc); err != nil {
		e.cancelPendingConfig(req.id)
		req.done <- adminResult{err: errors.WithStack(err)}
	}
}

func (e *Engine) handleRemoveServer(req adminRequest) {
	peer, ok := e.peerForID(req.peer.ID)
	if !ok {
		req.done <- adminResult{err: errors.Wrapf(errTransportPeerUnknown, "id=%q", req.peer.ID)}
		return
	}
	contextBytes, err := encodeConfChangeContext(req.id, peer)
	if err != nil {
		req.done <- adminResult{err: err}
		return
	}
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeRemoveNode,
		NodeID:  peer.NodeID,
		Context: contextBytes,
	}
	if err := e.storePendingConfig(req); err != nil {
		req.done <- adminResult{err: err}
		return
	}
	if err := e.rawNode.ProposeConfChange(cc); err != nil {
		e.cancelPendingConfig(req.id)
		req.done <- adminResult{err: errors.WithStack(err)}
	}
}

func (e *Engine) handleTransferLeadership(req adminRequest) {
	target, err := e.resolveTransferTarget(req.peer)
	if err != nil {
		req.done <- adminResult{err: err}
		return
	}
	// Reject transfer requests when the local node is not leader — etcd/raft
	// would silently drop the MsgTransferLeader in any non-leader state,
	// leaving the caller to block until the deadline. handleTransferLeadership
	// runs on the single-threaded event loop, so rawNode state reads here are
	// not racy with other rawNode mutations.
	if e.rawNode.BasicStatus().RaftState != etcdraft.StateLeader {
		req.done <- adminResult{err: errors.WithStack(errLeadershipTransferNotLeader)}
		return
	}
	e.rawNode.TransferLeader(target.NodeID)
	// TransferLeader is processed synchronously inside rawNode.TransferLeader.
	// If raft accepted the request, r.leadTransferee now equals target.NodeID.
	// If it was silently dropped (e.g. target has no progress entry, is a
	// learner, or equals the local node), leadTransferee is still zero or
	// unchanged — surface that as an immediate error rather than letting the
	// caller poll until its deadline.
	if e.rawNode.BasicStatus().LeadTransferee != target.NodeID {
		req.done <- adminResult{err: errors.Wrapf(errLeadershipTransferRejected,
			"target id=%d addr=%s", target.NodeID, target.Address)}
		return
	}
	req.done <- adminResult{peer: target}
}

func (e *Engine) drainReady() error {
	for e.rawNode.HasReady() {
		rd := e.rawNode.Ready()
		if err := e.persistReady(rd); err != nil {
			return err
		}
		if err := e.sendMessages(rd.Messages); err != nil {
			return err
		}
		if err := e.applyCommitted(rd.CommittedEntries); err != nil {
			return err
		}
		e.handleReadStates(rd.ReadStates)
		e.rawNode.Advance(rd)
		if err := e.maybePersistLocalSnapshot(); err != nil {
			return err
		}
		e.refreshStatus()
		e.resolveReadyReads()
	}
	e.refreshStatus()
	return nil
}

func (e *Engine) persistReady(rd etcdraft.Ready) error {
	if !readyNeedsPersistence(rd) {
		return nil
	}
	if err := e.applyReady(rd); err != nil {
		return err
	}
	if e.persist == nil {
		return nil
	}
	return persistReadyToWAL(e.persist, rd)
}

func (e *Engine) applyReady(rd etcdraft.Ready) error {
	if err := e.applyReadySnapshot(rd.Snapshot); err != nil {
		return err
	}
	if err := e.applyReadyEntries(rd.Entries); err != nil {
		return err
	}
	return e.applyReadyHardState(rd.HardState)
}

func (e *Engine) handleStep(msg raftpb.Message) {
	if e.rawNode == nil {
		return
	}
	e.recordLeaderContact(msg)
	e.recordQuorumAck(msg)
	if err := e.rawNode.Step(msg); err != nil {
		if errors.Is(err, etcdraft.ErrStepPeerNotFound) {
			return
		}
		e.fail(errors.WithStack(err))
	}
}

// recordQuorumAck updates the per-peer last-response time when msg is
// a follower -> leader response, so LastQuorumAck() reflects ongoing
// majority liveness without requiring a fresh ReadIndex.
//
// Called inside the event-loop goroutine (single writer to e.peers
// and to the raft state), so the e.peers read is race-free.
//
// Gated on the atomic isLeader mirror: a transport-level MsgAppResp /
// MsgHeartbeatResp can land shortly after a step-down (reset() has
// already cleared ackTracker); admitting it here would repopulate
// the tracker and leak a stale liveness instant into the next
// re-election as a non-zero LastQuorumAck(). isLeader is written by
// refreshStatus on every tick, which catches every role transition
// before the next handleStep runs.
func (e *Engine) recordQuorumAck(msg raftpb.Message) {
	if !isFollowerResponse(msg.Type) {
		return
	}
	if msg.From == 0 || msg.From == e.nodeID {
		return
	}
	if !e.isLeader.Load() {
		return
	}
	// Reject acks from peers not in the current membership. Without
	// this filter, a late MsgAppResp from a just-removed peer (which
	// rawNode.Step will immediately reject with ErrStepPeerNotFound)
	// would still land an ack in the tracker -- resurrecting the
	// "ghost" entry that removePeer just pruned. Since we run on the
	// event-loop goroutine (the sole writer to e.peers), the map read
	// here is race-free.
	if _, ok := e.peers[msg.From]; !ok {
		return
	}
	// Reject acks from learners. A learner does not vote and must not
	// contribute to the voter-majority denominator on the lease-read
	// fast path. See docs/design/2026_04_26_proposed_raft_learner.md
	// §4.6. Both e.peers and e.isLearnerNode are written from the
	// apply loop (single writer for both), so reading here under the
	// run-loop goroutine is race-free for the same reason as e.peers
	// above.
	if e.isLearnerNode[msg.From] {
		return
	}
	voterCount := e.voterCount
	if voterCount <= 1 {
		return
	}
	e.ackTracker.recordAck(msg.From, followerQuorumForClusterSize(voterCount))
}

// followerQuorumForClusterSize returns the number of non-self peer
// acks required to form a Raft majority for a cluster of the given
// size. Centralising the formula keeps ackTracker callers (handleStep
// and removePeer) consistent and avoids scattered //nolint:mnd
// suppressions. clusterSize is the total voter count INCLUDING self;
// the result is floor((clusterSize - 1) / 2) + 1 − 1 = clusterSize / 2
// for odd sizes (3 → 1, 5 → 2, 7 → 3) and clusterSize / 2 for even
// sizes (4 → 2, 6 → 3) where a strict majority still requires
// (N/2)+1 voters total, i.e. (N/2) followers beyond self.
func followerQuorumForClusterSize(clusterSize int) int {
	if clusterSize <= 1 {
		return 0
	}
	// The Raft majority for a cluster of size N is floor(N/2)+1 voters
	// INCLUDING self, which means the leader needs N/2 OTHER acks.
	return clusterSize / 2 //nolint:mnd
}

// isFollowerResponse reports whether a Raft message type represents a
// follower acknowledging the leader. We use only the two response
// types that ALL committed replication traffic passes through:
// MsgAppResp (log append ack) and MsgHeartbeatResp (passive heartbeat
// ack). Either one is proof that the peer's election timer has been
// reset, which is what the lease relies on.
func isFollowerResponse(t raftpb.MessageType) bool {
	return t == raftpb.MsgAppResp || t == raftpb.MsgHeartbeatResp
}

func (e *Engine) sendMessages(messages []raftpb.Message) error {
	for _, msg := range messages {
		if e.skipDispatchMessage(msg) {
			continue
		}
		if err := e.enqueueDispatchMessage(msg); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) skipDispatchMessage(msg raftpb.Message) bool {
	if msg.To == 0 || msg.To == e.nodeID || etcdraft.IsLocalMsg(msg.Type) {
		return true
	}
	return e.transport == nil || e.peerDispatchers == nil
}

// enqueueDispatchMessage routes msg to the per-peer channel. It is called
// exclusively from the engine event-loop goroutine, which is the same goroutine
// that calls removePeer. The delete-then-close ordering in removePeer therefore
// guarantees that no send can race with a channel close.
func (e *Engine) enqueueDispatchMessage(msg raftpb.Message) error {
	pd, ok := e.peerDispatchers[msg.To]
	if !ok {
		e.recordDroppedDispatch(msg)
		return nil
	}
	ch := e.selectDispatchLane(pd, msg.Type)
	// Avoid the expensive deep-clone in prepareDispatchRequest when the channel
	// is already full. The len/cap check is safe here because this function is
	// only ever called from the single engine event-loop goroutine.
	if len(ch) >= cap(ch) {
		e.recordDroppedDispatch(msg)
		return nil
	}
	dispatchReq := prepareDispatchRequest(msg)
	select {
	case ch <- dispatchReq:
		return nil
	default:
		_ = dispatchReq.Close()
		e.recordDroppedDispatch(msg)
		return nil
	}
}

// isPriorityMsg returns true for small, low-frequency control messages that
// must not be queued behind large MsgApp payloads in the normal channel.
// MsgAppResp is intentionally excluded: it is sent by followers, which never
// send MsgApp, so it faces no head-of-line blocking in the normal channel.
// Keeping it out of the priority queue preserves the low-frequency invariant
// that justifies defaultHeartbeatBufPerPeer = 64.
func isPriorityMsg(t raftpb.MessageType) bool {
	return t == raftpb.MsgHeartbeat || t == raftpb.MsgHeartbeatResp ||
		t == raftpb.MsgReadIndex || t == raftpb.MsgReadIndexResp ||
		t == raftpb.MsgVote || t == raftpb.MsgVoteResp ||
		t == raftpb.MsgPreVote || t == raftpb.MsgPreVoteResp ||
		t == raftpb.MsgTimeoutNow
}

// selectDispatchLane picks the per-peer channel for msgType. In the legacy
// 2-lane layout it returns pd.heartbeat for priority control traffic and
// pd.normal for everything else. In the 4-lane layout it additionally
// partitions the non-heartbeat traffic so that MsgApp/MsgAppResp and MsgSnap
// do not share a goroutine and cannot block each other.
func (e *Engine) selectDispatchLane(pd *peerQueues, msgType raftpb.MessageType) chan dispatchRequest {
	// Priority control traffic (heartbeats, votes, read-index, timeout-now)
	// always rides the heartbeat lane in both layouts so it keeps its
	// low-latency treatment and is never stuck behind MsgApp payloads.
	if isPriorityMsg(msgType) {
		return pd.heartbeat
	}
	if !e.dispatcherLanesEnabled {
		return pd.normal
	}
	// Only types that can actually reach this point are listed. Everything
	// filtered by skipDispatchMessage (etcdraft.IsLocalMsg: MsgHup, MsgBeat,
	// MsgUnreachable, MsgSnapStatus, MsgCheckQuorum, MsgStorageAppend/Resp,
	// MsgStorageApply/Resp) is dropped before this switch is reached.
	// Priority control traffic (MsgHeartbeat/Resp, votes, read-index,
	// MsgTimeoutNow) is short-circuited by the isPriorityMsg branch above.
	// MsgProp is not listed: DisableProposalForwarding=true ensures no
	// outbound MsgProp from this engine; if that assumption ever breaks the
	// message falls through to the pd.other return below.
	switch msgType { //nolint:exhaustive // see skipDispatchMessage / DisableProposalForwarding
	case raftpb.MsgApp, raftpb.MsgAppResp:
		return pd.replication
	case raftpb.MsgSnap:
		return pd.snapshot
	case raftpb.MsgTransferLeader, raftpb.MsgForgetLeader:
		return pd.other
	}
	// Fallback for any raftpb.MessageType added upstream that slips past
	// skipDispatchMessage and isPriorityMsg. Routing unknown non-priority
	// traffic onto pd.other keeps runtime behaviour compatible with the
	// pre-lanes default branch.
	return pd.other
}

func (e *Engine) applyReadySnapshot(snapshot raftpb.Snapshot) error {
	if etcdraft.IsEmptySnap(snapshot) {
		return nil
	}
	// etcdraft.IsEmptySnap only validates the raft metadata. This backend also
	// requires FSM payload bytes so it can restore local state before applying
	// the metadata snapshot to MemoryStorage.
	if len(snapshot.Data) == 0 {
		return errors.WithStack(errSnapshotRequired)
	}
	// Snapshot application is intentionally synchronous with the raft loop: the
	// local FSM must reflect the incoming raft snapshot before Ready can advance
	// and later committed entries can be applied safely.
	e.snapshotMu.Lock()
	defer e.snapshotMu.Unlock()

	if isSnapshotToken(snapshot.Data) {
		tok, err := decodeSnapshotToken(snapshot.Data)
		if err != nil {
			return errors.Wrapf(err, "decode snapshot token index=%d", snapshot.Metadata.Index)
		}
		if err := openAndRestoreFSMSnapshot(e.fsm, fsmSnapPath(e.fsmSnapDir, tok.Index), tok.CRC32C); err != nil {
			return errors.Wrapf(err, "restore fsm snapshot file index=%d crc=%08x", tok.Index, tok.CRC32C)
		}
	} else {
		// Legacy format: full FSM payload in snapshot.Data.
		if err := e.fsm.Restore(bytes.NewReader(snapshot.Data)); err != nil {
			return errors.Wrapf(err, "restore fsm from legacy snapshot payload index=%d", snapshot.Metadata.Index)
		}
	}

	if err := e.storage.ApplySnapshot(snapshot); err != nil {
		return errors.Wrapf(err, "apply snapshot to raft storage index=%d term=%d",
			snapshot.Metadata.Index, snapshot.Metadata.Term)
	}
	e.applied = snapshot.Metadata.Index
	e.appliedIndex.Store(snapshot.Metadata.Index)
	// Refresh the voter-cache from the snapshot's ConfState so
	// downstream apply-loop reads (recordQuorumAck, refreshStatus,
	// removePeer) see the post-snapshot voter set even before any
	// further conf-change entries arrive. Mirrors the apply-loop
	// sequence in applyConfigChange. The order matches §4.6:
	// voterCache first, then config.Servers.
	e.refreshVoterCache(snapshot.Metadata.ConfState)
	e.setConfigurationFromConfState(snapshot.Metadata.ConfState, snapshot.Metadata.Index)
	// Persist the post-snapshot peers file with suffrage drawn from
	// the snapshot's ConfState so a learner that received catch-up
	// state via snapshot (the common case for fresh joiners) writes
	// a peers file that round-trips correctly across restart. The
	// snapshot path bypasses the apply-loop's nextPeersAfterConfigChange
	// hook, so without this the joiner's peers file would carry the
	// pre-AddLearner suffrage forever. See learner design doc §4.3.
	if err := e.savePeersFileForSnapshot(snapshot); err != nil {
		return err
	}
	e.alarmIfJoinedAsVoter(snapshot.Metadata.ConfState)
	return nil
}

// savePeersFileForSnapshot writes the v2 peers file with suffrage
// drawn from the snapshot's ConfState. Idempotent: savePersistedPeers
// short-circuits when the on-disk index already covers `index`.
func (e *Engine) savePeersFileForSnapshot(snapshot raftpb.Snapshot) error {
	e.mu.RLock()
	peers := sortedPeerList(e.peers)
	e.mu.RUnlock()
	if len(peers) == 0 {
		return nil
	}
	annotatePeerSuffrageInSlice(peers, snapshot.Metadata.ConfState)
	if err := savePersistedPeers(e.dataDir, snapshot.Metadata.Index, peers); err != nil {
		return errors.Wrapf(err, "save peers file from snapshot index=%d", snapshot.Metadata.Index)
	}
	return nil
}

func (e *Engine) applyReadyEntries(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	return errors.WithStack(e.storage.Append(entries))
}

func (e *Engine) applyReadyHardState(hardState raftpb.HardState) error {
	if etcdraft.IsEmptyHardState(hardState) {
		return nil
	}
	return errors.WithStack(e.storage.SetHardState(hardState))
}

func (e *Engine) maybePersistLocalSnapshot() error {
	if e.applied == 0 || e.snapshotInFlight {
		return nil
	}
	if e.persist == nil || e.snapshotReqCh == nil {
		// Snapshot persistence is optional in this prototype; engines without a
		// disk-backed etcd storage simply skip local snapshot publication.
		return nil
	}

	current, err := e.storage.Snapshot()
	if err != nil {
		return errors.WithStack(err)
	}
	if e.applied <= current.Metadata.Index || e.applied-current.Metadata.Index < e.snapshotThreshold() {
		return nil
	}
	snapshot, err := e.fsm.Snapshot()
	if err != nil {
		return errors.WithStack(err)
	}

	req := snapshotRequest{
		index:    e.applied,
		snapshot: snapshot,
	}
	return e.enqueueSnapshotRequest(req)
}

func (e *Engine) enqueueSnapshotRequest(req snapshotRequest) error {
	select {
	case <-e.doneCh:
		if req.snapshot != nil {
			_ = req.snapshot.Close()
		}
		return e.currentErrorOrClosed()
	case <-e.snapshotStopCh:
		if req.snapshot != nil {
			_ = req.snapshot.Close()
		}
		return e.currentErrorOrClosed()
	case e.snapshotReqCh <- req:
		e.snapshotInFlight = true
		return nil
	}
}

func (e *Engine) applyCommitted(entries []raftpb.Entry) error {
	for _, entry := range entries {
		switch entry.Type {
		case raftpb.EntryNormal:
			response := e.applyNormalEntry(entry)
			e.setApplied(entry.Index)
			e.resolveProposal(entry.Index, entry.Data, response)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(entry.Data); err != nil {
				return errors.WithStack(err)
			}
			confState := e.rawNode.ApplyConfChange(cc)
			nextPeers := e.nextPeersAfterConfigChange(cc.Type, cc.NodeID, cc.Context, *confState)
			if err := e.persistConfigState(entry.Index, *confState, nextPeers); err != nil {
				return err
			}
			e.applyConfigChange(cc.Type, cc.NodeID, cc.Context, entry.Index, *confState)
			e.setApplied(entry.Index)
		case raftpb.EntryConfChangeV2:
			var cc raftpb.ConfChangeV2
			if err := cc.Unmarshal(entry.Data); err != nil {
				return errors.WithStack(err)
			}
			confState := e.rawNode.ApplyConfChange(cc)
			nextPeers := e.nextPeersAfterConfigChangeV2(cc, *confState)
			if err := e.persistConfigState(entry.Index, *confState, nextPeers); err != nil {
				return err
			}
			e.applyConfigChangeV2(cc, entry.Index, *confState)
			e.setApplied(entry.Index)
		default:
			e.setApplied(entry.Index)
		}
	}
	return nil
}

// setApplied advances both the run-loop-owned `applied` field and the
// lock-free atomic mirror in a single place. Called exclusively from
// the Raft run loop, so no synchronization between the two writes is
// required beyond the single-writer invariant.
func (e *Engine) setApplied(index uint64) {
	e.applied = index
	e.appliedIndex.Store(index)
}

func (e *Engine) applyNormalEntry(entry raftpb.Entry) any {
	if len(entry.Data) == 0 {
		return nil
	}
	_, payload, ok := decodeProposalEnvelope(entry.Data)
	if !ok {
		return nil
	}
	return e.fsm.Apply(payload)
}

func (e *Engine) resolveProposal(commitIndex uint64, data []byte, response any) {
	id, _, ok := decodeProposalEnvelope(data)
	if !ok {
		return
	}
	req, ok := e.popPendingProposal(id)
	if !ok {
		return
	}
	req.done <- proposalResult{
		result: &raftengine.ProposalResult{
			CommitIndex: commitIndex,
			Response:    response,
		},
	}
}

// applyConfigChange runs the four-step apply-loop sequence from
// docs/design/2026_04_26_proposed_raft_learner.md §4.2 edit 4:
//  1. recompute e.voterCount, e.isLearnerNode from confState
//  2. upsertPeer / removePeer (mutates e.peers; may read e.voterCount)
//  3. rebuild e.config.Servers via configurationFromConfState(e.peers, confState)
//  4. configIndex / pending-config bookkeeping
//
// All four steps run on the single-threaded apply loop, so no extra
// synchronization is needed.
func (e *Engine) applyConfigChange(changeType raftpb.ConfChangeType, nodeID uint64, context []byte, index uint64, confState raftpb.ConfState) {
	e.refreshVoterCache(confState)
	e.applyConfigPeerChange(changeType, nodeID, context)
	e.refreshConfigServers(confState)
	e.setConfigIndex(index)
	e.resolveConfigChange(index, context)
	e.alarmIfJoinedAsVoter(confState)
}

func (e *Engine) applyConfigChangeV2(cc raftpb.ConfChangeV2, index uint64, confState raftpb.ConfState) {
	e.refreshVoterCache(confState)
	for _, change := range cc.Changes {
		e.applyConfigPeerChange(change.Type, change.NodeID, cc.Context)
	}
	e.refreshConfigServers(confState)
	e.setConfigIndex(index)
	e.resolveConfigChange(index, cc.Context)
	e.alarmIfJoinedAsVoter(confState)
}

// alarmIfJoinedAsVoter implements the operator-alarm semantics from
// docs/design/2026_04_26_proposed_raft_learner.md §4.5: when the
// operator passed --raftJoinAsLearner but a post-apply ConfState
// lists this node as a voter, surface a one-shot ERROR-level
// structured log and bump a metric. The node continues running --
// shutting down would shrink the cluster's effective fault tolerance
// since by the time the conf change has applied, the local node
// already counts toward the voter quorum.
func (e *Engine) alarmIfJoinedAsVoter(confState raftpb.ConfState) {
	if !e.joinAsLearner {
		return
	}
	if !nodeInVoters(confState, e.nodeID) {
		return
	}
	if !e.joinAlarmFired.CompareAndSwap(false, true) {
		return
	}
	joinRoleViolationCount.Add(1)
	slog.Error("etcd raft join-as-learner alarm: local node was added as voter, expected learner",
		slog.String("local_id", e.localID),
		slog.Uint64("node_id", e.nodeID),
		slog.Any("voters", confState.Voters),
		slog.Any("learners", confState.Learners),
	)
}

func nodeInVoters(confState raftpb.ConfState, nodeID uint64) bool {
	for _, id := range confState.Voters {
		if id == nodeID {
			return true
		}
	}
	return false
}

// refreshVoterCache rebuilds the voterCount / isLearnerNode cache
// from the post-change ConfState. Called from the apply loop only.
// The map is rebuilt from scratch — never patched — so promotion of a
// learner cannot leak a stale isLearnerNode[id]==true entry. See
// docs/design/2026_04_26_proposed_raft_learner.md §4.6.
func (e *Engine) refreshVoterCache(confState raftpb.ConfState) {
	e.mu.Lock()
	e.voterCount = len(confState.Voters)
	e.isLearnerNode = learnerSetFromConfState(confState)
	e.mu.Unlock()
}

// refreshConfigServers rebuilds e.config.Servers from the post-change
// ConfState and the current address-cache map. Called after the
// per-peer cleanup helpers (upsertPeer / removePeer) so that
// configurationFromConfState observes the post-cleanup e.peers map.
func (e *Engine) refreshConfigServers(confState raftpb.ConfState) {
	e.mu.Lock()
	e.config = configurationFromConfState(e.peers, confState)
	e.mu.Unlock()
}

func (e *Engine) applyConfigPeerChange(changeType raftpb.ConfChangeType, nodeID uint64, context []byte) {
	peer, ok := decodeConfChangePeerContext(nodeID, context)
	switch changeType {
	case raftpb.ConfChangeAddNode:
		// Promotion of an existing learner also lands here as
		// ConfChangeAddNode for a node already in e.peers; the upsert
		// is idempotent on (nodeID -> addr) so it is safe to take
		// the same path. The voter / learner role swap is reflected
		// in the post-change ConfState, which the apply loop has
		// already used to refresh the voter cache and which it will
		// use to rebuild e.config.Servers.
		e.applyAddedPeer(nodeID, peer, ok)
	case raftpb.ConfChangeRemoveNode:
		e.applyRemovedPeer(nodeID, peer, ok)
	case raftpb.ConfChangeUpdateNode:
		e.applyUpdatedPeer(peer, ok)
	case raftpb.ConfChangeAddLearnerNode:
		// A learner joins the address cache the same way a voter does;
		// suffrage is an attribute of ConfState, not of e.peers. See
		// docs/design/2026_04_26_proposed_raft_learner.md §4.2.
		e.applyAddedPeer(nodeID, peer, ok)
	}
}

func (e *Engine) applyAddedPeer(nodeID uint64, peer Peer, ok bool) {
	if ok {
		e.upsertPeer(peer)
		return
	}
	if nodeID == 0 || e.hasPeer(nodeID) {
		return
	}
	e.upsertPeer(Peer{
		NodeID: nodeID,
		ID:     strconv.FormatUint(nodeID, 10),
	})
}

func (e *Engine) applyRemovedPeer(nodeID uint64, peer Peer, ok bool) {
	if ok && peer.NodeID != 0 {
		e.removePeer(peer.NodeID)
		if peer.NodeID == e.nodeID {
			e.requestShutdown()
		}
		return
	}
	if nodeID != 0 {
		e.removePeer(nodeID)
		if nodeID == e.nodeID {
			e.requestShutdown()
		}
	}
}

func (e *Engine) applyUpdatedPeer(peer Peer, ok bool) {
	if ok {
		e.upsertPeer(peer)
	}
}

func applyConfigPeerChangeToMap(peers map[uint64]Peer, changeType raftpb.ConfChangeType, nodeID uint64, context []byte) {
	peer, ok := decodeConfChangePeerContext(nodeID, context)
	switch changeType {
	case raftpb.ConfChangeAddNode:
		applyAddedPeerToMap(peers, nodeID, peer, ok)
	case raftpb.ConfChangeRemoveNode:
		applyRemovedPeerToMap(peers, nodeID, peer, ok)
	case raftpb.ConfChangeUpdateNode:
		applyUpdatedPeerToMap(peers, peer, ok)
	case raftpb.ConfChangeAddLearnerNode:
		// Persist learner metadata if it appears in a replayed log so a future
		// learner-aware restart path still has the full peer inventory on disk.
		// The current runtime still rejects learner conf states during startup.
		applyAddedPeerToMap(peers, nodeID, peer, ok)
	}
}

func decodeConfChangePeerContext(nodeID uint64, context []byte) (Peer, bool) {
	_, peer, ok := decodeConfChangeContext(context)
	if !ok {
		return Peer{}, false
	}
	if nodeID != 0 && peer.NodeID != 0 && peer.NodeID != nodeID {
		return Peer{}, false
	}
	return peer, true
}

func applyAddedPeerToMap(peers map[uint64]Peer, nodeID uint64, peer Peer, ok bool) {
	if ok {
		upsertPeerInMap(peers, peer)
		return
	}
	if nodeID != 0 && !hasPeerInMap(peers, nodeID) {
		upsertPeerInMap(peers, Peer{
			NodeID: nodeID,
			ID:     strconv.FormatUint(nodeID, 10),
		})
	}
}

func applyRemovedPeerToMap(peers map[uint64]Peer, nodeID uint64, peer Peer, ok bool) {
	if ok && peer.NodeID != 0 {
		removePeerFromMap(peers, peer.NodeID)
		return
	}
	if nodeID != 0 {
		removePeerFromMap(peers, nodeID)
	}
}

func applyUpdatedPeerToMap(peers map[uint64]Peer, peer Peer, ok bool) {
	if ok {
		upsertPeerInMap(peers, peer)
	}
}

func (e *Engine) persistConfigSnapshot(index uint64, confState raftpb.ConfState) error {
	// Fast path (lock-free): avoid unnecessary FSM snapshot when config is already current.
	if upToDate, err := e.configSnapshotUpToDate(index, confState); err != nil {
		return err
	} else if upToDate {
		return nil
	}

	e.snapshotMu.Lock()
	defer e.snapshotMu.Unlock()

	payload, err := e.snapshotPayload(index)
	if err != nil {
		return err
	}
	return e.persistConfigSnapshotPayloadLocked(index, confState, payload)
}

func (e *Engine) persistConfigState(index uint64, confState raftpb.ConfState, peers []Peer) error {
	// Config changes are committed in strictly increasing raft index order. Use
	// the cached durable config index to avoid re-reading peer metadata from
	// disk on this hot path.
	if e.currentConfigIndex() >= index {
		return nil
	}

	// Keep peer metadata publication, FSM snapshot serialization, and raft
	// snapshot persistence serialized with snapshot restores. Restart logic can
	// tolerate peer metadata leading the persisted raft snapshot, but not an
	// interleaving restore that swaps in newer FSM state halfway through the
	// publication sequence.
	e.snapshotMu.Lock()
	defer e.snapshotMu.Unlock()

	if e.currentConfigIndex() >= index {
		return nil
	}
	if err := writeCurrentPersistedPeers(e.dataDir, index, peers); err != nil {
		return err
	}
	if upToDate, err := e.configSnapshotUpToDate(index, confState); err != nil {
		return err
	} else if upToDate {
		e.setConfigIndex(index)
		return nil
	}

	payload, err := e.snapshotPayload(index)
	if err != nil {
		return err
	}
	if err := e.persistConfigSnapshotPayloadLocked(index, confState, payload); err != nil {
		return err
	}
	e.setConfigIndex(index)
	return nil
}

func (e *Engine) persistConfigSnapshotPayloadLocked(index uint64, confState raftpb.ConfState, payload []byte) error {
	if upToDate, err := e.configSnapshotUpToDate(index, confState); err != nil {
		return err
	} else if upToDate {
		return nil
	}

	snap, err := e.createConfigSnapshot(index, confState, payload)
	if err != nil {
		return err
	}
	if err := e.persistCreatedSnapshot(snap); err != nil {
		return err
	}
	return nil
}

// readFSMPayloadLocked reads the FSM snapshot payload for the given index while
// holding snapshotMu, preventing concurrent purge from removing the file.
func (e *Engine) readFSMPayloadLocked(index uint64) ([]byte, error) {
	e.snapshotMu.Lock()
	defer e.snapshotMu.Unlock()
	return readFSMSnapshotPayload(fsmSnapPath(e.fsmSnapDir, index))
}

// openFSMPayloadLocked opens the .fsm payload file for the given index, holding
// snapshotMu only for the duration of os.Open. Once the file descriptor is
// obtained, Unix semantics guarantee it remains readable even after
// purgeOldSnapshotFiles unlinks the file, so the lock can be released before
// the fstat call. The caller must close the returned ReadCloser.
func (e *Engine) openFSMPayloadLocked(index uint64) (io.ReadCloser, error) {
	path := fsmSnapPath(e.fsmSnapDir, index)
	e.snapshotMu.Lock()
	f, err := openFSMSnapshotFile(path)
	e.snapshotMu.Unlock()
	if err != nil {
		return nil, err
	}
	return openFSMPayloadFromFD(f)
}

// snapshotPayload takes a FSM snapshot for the given index, writes it to the
// .fsm file on disk, and returns the 17-byte token for raftpb.Snapshot.Data.
// If fsmSnapDir is not set (e.g., engines created directly in unit tests),
// falls back to the legacy in-memory []byte path.
func (e *Engine) snapshotPayload(index uint64) ([]byte, error) {
	if e.fsmSnapDir == "" {
		snapshot, err := e.fsm.Snapshot()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return snapshotBytesAndClose(snapshot, e.dataDir)
	}
	snapshot, err := e.fsm.Snapshot()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	crc32c, writeErr := writeFSMSnapshotFile(snapshot, e.fsmSnapDir, index)
	closeErr := snapshot.Close()
	if writeErr != nil {
		return nil, writeErr
	}
	if closeErr != nil {
		return nil, errors.WithStack(closeErr)
	}
	return encodeSnapshotToken(index, crc32c), nil
}

func (e *Engine) configSnapshotUpToDate(index uint64, confState raftpb.ConfState) (bool, error) {
	currentIndex := e.currentConfigIndex()
	if currentIndex > index {
		return true, nil
	}

	current, err := e.storage.Snapshot()
	if err != nil {
		return false, errors.WithStack(err)
	}
	if current.Metadata.Index > index {
		return true, nil
	}
	if current.Metadata.Index < index {
		return false, nil
	}
	return equalConfState(current.Metadata.ConfState, confState), nil
}

func (e *Engine) createConfigSnapshot(index uint64, confState raftpb.ConfState, payload []byte) (raftpb.Snapshot, error) {
	snap, err := e.storage.CreateSnapshot(index, &confState, payload)
	if err == nil {
		return snap, nil
	}
	switch {
	case errors.Is(err, etcdraft.ErrSnapOutOfDate):
		return raftpb.Snapshot{}, nil
	case errors.Is(err, etcdraft.ErrCompacted):
		return raftpb.Snapshot{}, nil
	default:
		return raftpb.Snapshot{}, errors.WithStack(err)
	}
}

func (e *Engine) persistCreatedSnapshot(snap raftpb.Snapshot) error {
	if etcdraft.IsEmptySnap(snap) || e.persist == nil {
		return nil
	}
	if err := e.persist.SaveSnap(snap); err != nil {
		return errors.WithStack(err)
	}
	if err := e.persist.Release(snap); err != nil {
		return errors.WithStack(err)
	}

	snapDir := filepath.Join(e.dataDir, snapDirName)
	if purgeErr := purgeOldSnapshotFiles(snapDir, e.fsmSnapDir); purgeErr != nil {
		slog.Warn("failed to purge old snap files", "error", purgeErr)
	}
	walDir := filepath.Join(e.dataDir, walDirName)
	if purgeErr := purgeOldWALFiles(walDir, e.walRetention()); purgeErr != nil {
		slog.Warn("failed to purge old wal files", "error", purgeErr)
	}
	return nil
}

func (e *Engine) handleReadStates(states []etcdraft.ReadState) {
	for _, state := range states {
		id, ok := decodeReadContext(state.RequestCtx)
		if !ok {
			continue
		}
		req, ok := e.pendingRead(id)
		if !ok {
			continue
		}
		req.target = state.Index
		if !req.waitApplied {
			e.popPendingRead(id)
			req.done <- readResult{index: state.Index}
			continue
		}
		e.storePendingRead(req)
	}
}

func (e *Engine) resolveReadyReads() {
	for _, req := range e.readyReads(e.applied) {
		req.done <- readResult{index: req.target}
	}
}

func (e *Engine) refreshStatus() {
	previous := e.Status().State
	basic := e.rawNode.BasicStatus()
	lastLogIndex, _ := e.storage.LastIndex()
	snapshot, _ := e.storage.Snapshot()
	config := e.currentConfiguration()

	state := translateState(basic.RaftState)
	leader := e.leaderInfo(basic.Lead)

	status := raftengine.Status{
		State:             state,
		Leader:            leader,
		Term:              basic.Term,
		CommitIndex:       basic.Commit,
		AppliedIndex:      e.applied,
		LastLogIndex:      lastLogIndex,
		LastSnapshotIndex: snapshot.Metadata.Index,
		FSMPending:        pendingEntries(basic.Commit, e.applied),
		NumPeers:          numRemoteServers(config.Servers, e.localID),
		LastContact:       lastContactFor(state, basic.Lead, e.lastLeaderContactFrom, e.lastLeaderContactAt),
		LeadTransferee:    basic.LeadTransferee,
	}

	e.mu.Lock()
	e.status = status
	if e.closed {
		e.status.State = raftengine.StateShutdown
	}
	// Use the voter count, NOT len(e.peers), so a 1-voter + N-learner
	// cluster correctly hits the leader-of-1 fast path: a learner does
	// not vote and is not part of the lease-read denominator. See
	// docs/design/2026_04_26_proposed_raft_learner.md §4.6.
	voterCount := e.voterCount
	e.mu.Unlock()

	// Keep the lock-free single-node fast path in sync with the current
	// role: populate while leader of a 1-node cluster, clear otherwise
	// (including on leader loss, so LastQuorumAck transitions to the
	// multi-node tracker or zero time atomically).
	// Publish leader state atomically so recordQuorumAck / LastQuorumAck
	// can gate on it without acquiring e.mu. MUST run before the
	// single-node ack store below, otherwise a brand-new leader tick
	// could publish a ack instant while isLeader is still false.
	e.isLeader.Store(status.State == raftengine.StateLeader)

	if status.State == raftengine.StateLeader && voterCount <= 1 {
		e.singleNodeLeaderAckMonoNs.Store(monoclock.Now().Nanos())
	} else {
		e.singleNodeLeaderAckMonoNs.Store(0)
	}

	if status.State == raftengine.StateLeader {
		e.leaderOnce.Do(func() { close(e.leaderReady) })
	}
	if previous != raftengine.StateLeader && status.State == raftengine.StateLeader {
		// Edge: the node has just acquired leadership. Fire the
		// leader-acquired callbacks so per-shard policy hooks
		// (SQS HT-FIFO leadership-refusal §8) can audit the
		// transition before any client request lands. Same
		// non-blocking contract as fireLeaderLossCallbacks.
		e.fireLeaderAcquiredCallbacks()
	}
	if previous == raftengine.StateLeader && status.State != raftengine.StateLeader {
		e.failPending(errors.WithStack(errNotLeader))
		// Drop the per-peer ack map so a future re-election cannot
		// surface a stale majority-ack instant before the new term's
		// heartbeats have actually confirmed liveness.
		e.ackTracker.reset()
		// Notify lease holders so they invalidate any cached lease;
		// without this hook, a former leader keeps serving fast-path
		// reads from local state for up to LeaseDuration after a
		// successor leader is already accepting writes.
		e.fireLeaderLossCallbacks()
	}
}

func (e *Engine) markStarted() {
	e.startOnce.Do(func() { close(e.startedCh) })
}

func (e *Engine) openReady(waitForLeader bool) <-chan struct{} {
	if waitForLeader {
		return e.leaderReady
	}
	return e.startedCh
}

func (e *Engine) requestShutdown() {
	e.closeOnce.Do(func() {
		close(e.closeCh)
	})
}

func (e *Engine) shutdown() {
	e.mu.Lock()
	wasLeader := e.status.State == raftengine.StateLeader
	e.closed = true
	e.status.State = raftengine.StateShutdown
	e.mu.Unlock()
	e.stopDispatchWorkers()
	e.stopSnapshotWorker()
	_ = closePersist(e.persist)
	if err := e.transport.Close(); err != nil {
		slog.Warn("etcd raft engine: transport close",
			slog.String("node_id", e.localID),
			slog.Any("err", err),
		)
	}
	e.failPending(errors.WithStack(errClosed))
	// LeaseProvider contract promises callbacks fire on shutdown too.
	// refreshStatus only fires them on the leader -> non-leader edge,
	// which can be missed when shutdown short-circuits the status loop.
	// Always fire here so lease holders invalidate even on engine close
	// initiated while still leader, on shutdown after fail(), or via
	// Close() racing against the run loop.
	if wasLeader {
		e.fireLeaderLossCallbacks()
	}
}

func (e *Engine) fail(err error) {
	// Idempotency guard: fail can be invoked from handleStep, which runs inside
	// the main run loop and does not return on error. Without this guard a
	// persistent step error would re-enter fail on every loop iteration,
	// producing a log storm and redundant teardown calls.
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		// Make sure the run loop still observes the shutdown signal even if a
		// previous fail path did not reach requestShutdown (e.g. concurrent
		// invocation racing on the closed flag).
		e.requestShutdown()
		return
	}
	e.mu.Unlock()

	// Log before mutating state so observability is preserved even if the
	// downstream stopDispatch/closePersist calls themselves block or panic.
	// Without this, the engine quietly transitions to StateShutdown and the
	// only signal to the operator is a "shutdown" gauge — which is what
	// happened in the production rolling-update incident where a snapshot
	// restore failed silently and left the process running but unusable.
	if err != nil {
		slog.Error("etcd raft engine shutting down due to error",
			slog.String("node_id", e.localID),
			slog.Uint64("raft_node_id", e.nodeID),
			slog.Any("err", err),
		)
	}
	e.mu.Lock()
	wasLeader := e.status.State == raftengine.StateLeader
	if err != nil {
		e.runErr = err
	}
	e.closed = true
	e.status.State = raftengine.StateShutdown
	e.mu.Unlock()
	// Signal the run loop so that callers which invoke fail without returning
	// from run themselves (notably handleStep) still exit on the next select.
	e.requestShutdown()
	e.stopDispatchWorkers()
	e.stopSnapshotWorker()
	_ = closePersist(e.persist)
	if err := e.transport.Close(); err != nil {
		slog.Warn("etcd raft engine: transport close",
			slog.String("node_id", e.localID),
			slog.Any("err", err),
		)
	}
	e.failPending(e.currentErrorOrClosed())
	// LeaseProvider contract: fire leader-loss callbacks on shutdown if
	// we were leader. fail() is the error-shutdown twin of shutdown();
	// without firing here, a run() -> fail() path that bypasses
	// refreshStatus's Leader -> non-Leader edge leaves lease holders
	// serving fast-path reads from stale state for up to LeaseDuration.
	if wasLeader {
		e.fireLeaderLossCallbacks()
	}
}

func (e *Engine) failPending(err error) {
	proposals, reads, configs := e.drainPending()
	for _, req := range proposals {
		req.done <- proposalResult{err: err}
	}
	for _, req := range reads {
		req.done <- readResult{err: err}
	}
	for _, req := range configs {
		req.done <- adminResult{err: err}
	}
}

func (e *Engine) storePendingProposal(req proposalRequest) {
	e.pending.Lock()
	defer e.pending.Unlock()
	e.pendingProposals[req.id] = req
}

func (e *Engine) popPendingProposal(id uint64) (proposalRequest, bool) {
	e.pending.Lock()
	defer e.pending.Unlock()
	req, ok := e.pendingProposals[id]
	if ok {
		delete(e.pendingProposals, id)
	}
	return req, ok
}

func (e *Engine) cancelPendingProposal(id uint64) {
	if id == 0 {
		return
	}
	e.pending.Lock()
	delete(e.pendingProposals, id)
	e.pending.Unlock()
}

func (e *Engine) storePendingRead(req readRequest) {
	e.pending.Lock()
	defer e.pending.Unlock()
	e.pendingReads[req.id] = req
}

func (e *Engine) pendingRead(id uint64) (readRequest, bool) {
	e.pending.Lock()
	defer e.pending.Unlock()
	req, ok := e.pendingReads[id]
	return req, ok
}

func (e *Engine) popPendingRead(id uint64) (readRequest, bool) {
	e.pending.Lock()
	defer e.pending.Unlock()
	req, ok := e.pendingReads[id]
	if ok {
		delete(e.pendingReads, id)
	}
	return req, ok
}

func (e *Engine) cancelPendingRead(id uint64) {
	if id == 0 {
		return
	}
	e.pending.Lock()
	delete(e.pendingReads, id)
	e.pending.Unlock()
}

func (e *Engine) storePendingConfig(req adminRequest) error {
	e.pending.Lock()
	defer e.pending.Unlock()
	if len(e.pendingConfigs) >= defaultMaxPendingConfigs {
		return errors.WithStack(errTooManyPendingConfigs)
	}
	e.pendingConfigs[req.id] = req
	return nil
}

func (e *Engine) popPendingConfig(id uint64) (adminRequest, bool) {
	e.pending.Lock()
	defer e.pending.Unlock()
	req, ok := e.pendingConfigs[id]
	if ok {
		delete(e.pendingConfigs, id)
	}
	return req, ok
}

func (e *Engine) cancelPendingConfig(id uint64) {
	if id == 0 {
		return
	}
	e.pending.Lock()
	delete(e.pendingConfigs, id)
	e.pending.Unlock()
}

func (e *Engine) readyReads(applied uint64) []readRequest {
	e.pending.Lock()
	defer e.pending.Unlock()

	ready := make([]readRequest, 0)
	for id, req := range e.pendingReads {
		if req.target == 0 || req.target > applied {
			continue
		}
		delete(e.pendingReads, id)
		ready = append(ready, req)
	}
	return ready
}

func (e *Engine) drainPending() ([]proposalRequest, []readRequest, []adminRequest) {
	e.pending.Lock()
	defer e.pending.Unlock()

	proposals := make([]proposalRequest, 0, len(e.pendingProposals))
	for id, req := range e.pendingProposals {
		delete(e.pendingProposals, id)
		proposals = append(proposals, req)
	}
	reads := make([]readRequest, 0, len(e.pendingReads))
	for id, req := range e.pendingReads {
		delete(e.pendingReads, id)
		reads = append(reads, req)
	}
	configs := make([]adminRequest, 0, len(e.pendingConfigs))
	for id, req := range e.pendingConfigs {
		delete(e.pendingConfigs, id)
		configs = append(configs, req)
	}
	return proposals, reads, configs
}

func (e *Engine) currentError() error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.runErr
}

func (e *Engine) currentErrorOrClosed() error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.runErr != nil {
		return e.runErr
	}
	return errors.WithStack(errClosed)
}

func (e *Engine) currentConfiguration() raftengine.Configuration {
	e.mu.RLock()
	defer e.mu.RUnlock()
	cfg := raftengine.Configuration{
		Servers: append([]raftengine.Server(nil), e.config.Servers...),
	}
	return cfg
}

func (e *Engine) currentConfigIndex() uint64 {
	return e.configIndex.Load()
}

func (e *Engine) shouldCampaignSingleNode() bool {
	// Count VOTERS in the current configuration, not all servers --
	// a learner does not vote and must not block the single-voter
	// fast-path Campaign(). After learner support landed,
	// len(cfg.Servers) includes learners, so the previous check
	// would skip Campaign() in a 1-voter + N-learner cluster on
	// restart and incur a full electionTimeout latency for no
	// reason. See docs/design/2026_04_26_proposed_raft_learner.md
	// §4.6 / §3.3 (the audit of len(e.peers) overloads).
	cfg := e.currentConfiguration()
	var soleVoter raftengine.Server
	voterCount := 0
	for _, s := range cfg.Servers {
		if s.Suffrage != SuffrageVoter {
			continue
		}
		voterCount++
		soleVoter = s
	}
	return voterCount == 1 && soleVoter.ID == e.localID
}

func (e *Engine) resolveAdminPeer(id string, address string) (Peer, error) {
	if e.transport != nil {
		if peer, ok := e.transport.peerByIdentity(id, address); ok {
			return peer, nil
		}
	}
	e.mu.RLock()
	for _, peer := range e.peers {
		if id != "" && peer.ID != id {
			continue
		}
		if address != "" && peer.Address != address {
			continue
		}
		e.mu.RUnlock()
		return peer, nil
	}
	e.mu.RUnlock()
	return normalizePeer(Peer{ID: id, Address: address})
}

func (e *Engine) setConfigIndex(index uint64) {
	for {
		current := e.configIndex.Load()
		if index <= current {
			return
		}
		if e.configIndex.CompareAndSwap(current, index) {
			return
		}
	}
}

func (e *Engine) setConfigurationFromConfState(conf raftpb.ConfState, index uint64) {
	e.mu.Lock()
	e.config = configurationFromConfState(e.peers, conf)
	e.mu.Unlock()
	e.setConfigIndex(index)
}

func (e *Engine) nextID() uint64 {
	return e.nextRequestID.Add(1)
}

func normalizeOpenConfig(cfg OpenConfig) (OpenConfig, persistedPeers, bool, error) {
	cfg = normalizeIdentity(cfg)
	peers, ok, err := loadPersistedPeersState(cfg.DataDir)
	if err != nil {
		return OpenConfig{}, persistedPeers{}, false, err
	}
	if ok {
		cfg.Peers = peers.Peers
	}
	cfg = normalizePeersConfig(cfg)
	cfg = normalizeTimingConfig(cfg)
	cfg = normalizeLimitConfig(cfg)
	return cfg, peers, ok, nil
}

func validateOpenPeers(snapshot raftpb.Snapshot, peers []Peer, persisted persistedPeers, persistedOK bool) error {
	if len(peers) == 0 {
		return nil
	}
	if snapshot.Metadata.Index == 0 || len(snapshot.Metadata.ConfState.Voters) == 0 {
		return nil
	}
	if !persistedOK {
		return validateConfState(snapshot.Metadata.ConfState, peers)
	}
	if persisted.Index > snapshot.Metadata.Index {
		return nil
	}
	return validateConfState(snapshot.Metadata.ConfState, peers)
}

func normalizeIdentity(cfg OpenConfig) OpenConfig {
	if cfg.NodeID == 0 && cfg.LocalID != "" {
		cfg.NodeID = DeriveNodeID(cfg.LocalID)
	}
	if cfg.LocalID == "" && cfg.NodeID != 0 {
		cfg.LocalID = strconv.FormatUint(cfg.NodeID, 10)
	}
	return cfg
}

func normalizePeersConfig(cfg OpenConfig) OpenConfig {
	// Only fill cfg.Peers with a single-peer self-list when an explicit
	// Bootstrap flag was passed. Historically we defaulted to a local-only
	// peer list whenever cfg.Peers was empty, but that silently bypassed the
	// self-bootstrap guard in normalizePeers: a node with a wiped data dir
	// and no persisted peers would arrive here with cfg.Peers empty, get a
	// self-only list injected, and then sail past the len(peers) == 0 guard
	// — the exact split-brain scenario the guard is supposed to prevent.
	// When Bootstrap is false and no persisted peers were loaded,
	// normalizePeers will reject the empty list via errNoPeersConfigured.
	if cfg.Bootstrap && len(cfg.Peers) == 0 && cfg.LocalAddress != "" && cfg.LocalID != "" {
		cfg.Peers = []Peer{{
			NodeID:  cfg.NodeID,
			ID:      cfg.LocalID,
			Address: cfg.LocalAddress,
		}}
	}
	return cfg
}

func normalizeTimingConfig(cfg OpenConfig) OpenConfig {
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = defaultTickInterval
	}
	if cfg.HeartbeatTick <= 0 {
		cfg.HeartbeatTick = defaultHeartbeatTick
	}
	if cfg.ElectionTick <= 0 {
		cfg.ElectionTick = defaultElectionTick
	}
	return cfg
}

func normalizeLimitConfig(cfg OpenConfig) OpenConfig {
	if cfg.MaxInflightMsg <= 0 {
		cfg.MaxInflightMsg = defaultMaxInflightMsg
	}
	if cfg.MaxSizePerMsg == 0 {
		cfg.MaxSizePerMsg = defaultMaxSizePerMsg
	}
	return cfg
}

func validateConfig(cfg OpenConfig) error {
	switch {
	case cfg.NodeID == 0:
		return errors.WithStack(errNodeIDRequired)
	case cfg.DataDir == "":
		return errors.WithStack(errDataDirRequired)
	case cfg.StateMachine == nil:
		return errors.WithStack(errStateMachineUnset)
	case cfg.ElectionTick <= cfg.HeartbeatTick:
		return errors.New("election tick must be greater than heartbeat tick")
	default:
		return nil
	}
}

func translateState(state etcdraft.StateType) raftengine.State {
	switch state {
	case etcdraft.StateFollower:
		return raftengine.StateFollower
	case etcdraft.StatePreCandidate:
		return raftengine.StateCandidate
	case etcdraft.StateCandidate:
		return raftengine.StateCandidate
	case etcdraft.StateLeader:
		return raftengine.StateLeader
	default:
		return raftengine.StateUnknown
	}
}

func pendingEntries(commitIndex uint64, appliedIndex uint64) uint64 {
	if commitIndex <= appliedIndex {
		return 0
	}
	return commitIndex - appliedIndex
}

func maxUint64(left uint64, right uint64) uint64 {
	if left >= right {
		return left
	}
	return right
}

func configurationFromConfState(peers map[uint64]Peer, conf raftpb.ConfState) raftengine.Configuration {
	if len(conf.Voters) == 0 && len(conf.Learners) == 0 {
		return raftengine.Configuration{}
	}

	servers := make([]raftengine.Server, 0, len(conf.Voters)+len(conf.Learners))
	for _, nodeID := range conf.Voters {
		servers = append(servers, configServerForNode(peers, nodeID, SuffrageVoter))
	}
	for _, nodeID := range conf.Learners {
		servers = append(servers, configServerForNode(peers, nodeID, SuffrageLearner))
	}
	return raftengine.Configuration{Servers: servers}
}

func configServerForNode(peers map[uint64]Peer, nodeID uint64, suffrage string) raftengine.Server {
	if peer, ok := peers[nodeID]; ok {
		return raftengine.Server{
			ID:       peer.ID,
			Address:  peer.Address,
			Suffrage: suffrage,
		}
	}
	return raftengine.Server{
		ID:       strconv.FormatUint(nodeID, 10),
		Address:  "",
		Suffrage: suffrage,
	}
}

// learnerSetFromConfState builds the apply-loop-owned isLearnerNode
// cache from a ConfState. Returns nil for an empty / voter-only conf
// state (callers treat nil as "no learners").
func learnerSetFromConfState(conf raftpb.ConfState) map[uint64]bool {
	if len(conf.Learners) == 0 {
		return nil
	}
	out := make(map[uint64]bool, len(conf.Learners))
	for _, nodeID := range conf.Learners {
		out[nodeID] = true
	}
	return out
}

func numRemoteServers(servers []raftengine.Server, localID string) uint64 {
	var count uint64
	for _, server := range servers {
		if server.ID == localID {
			continue
		}
		count++
	}
	return count
}

func lastContactFor(state raftengine.State, leaderNodeID uint64, lastFrom uint64, lastAt time.Time) time.Duration {
	if state == raftengine.StateLeader {
		return 0
	}
	if state != raftengine.StateFollower {
		return unknownLastContact
	}
	if lastAt.IsZero() {
		return unknownLastContact
	}
	if leaderNodeID != 0 && lastFrom != leaderNodeID {
		return unknownLastContact
	}
	return time.Since(lastAt)
}

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func readyNeedsPersistence(rd etcdraft.Ready) bool {
	return !etcdraft.IsEmptySnap(rd.Snapshot) || len(rd.Entries) > 0 || !etcdraft.IsEmptyHardState(rd.HardState)
}

func maxAppliedIndex(snapshot raftpb.Snapshot) uint64 {
	return snapshot.Metadata.Index
}

func (e *Engine) enqueueStep(ctx context.Context, msg raftpb.Message) error {
	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-e.doneCh:
		return e.currentErrorOrClosed()
	case e.stepCh <- msg:
		return nil
	default:
		e.stepQueueFullCount.Add(1)
		return errors.WithStack(errStepQueueFull)
	}
}

func (e *Engine) handleTransportMessage(ctx context.Context, msg raftpb.Message) error {
	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-e.doneCh:
		return e.currentErrorOrClosed()
	case <-e.startedCh:
		return e.enqueueStep(ctx, msg)
	}
}

func (e *Engine) recordLeaderContact(msg raftpb.Message) {
	if !isLeaderContactMessage(msg.Type) || msg.From == 0 {
		return
	}
	e.lastLeaderContactFrom = msg.From
	e.lastLeaderContactAt = time.Now()
}

func isLeaderContactMessage(msgType raftpb.MessageType) bool {
	return msgType == raftpb.MsgApp ||
		msgType == raftpb.MsgHeartbeat ||
		msgType == raftpb.MsgSnap ||
		msgType == raftpb.MsgReadIndexResp
}

func (e *Engine) startDispatchWorkers() {
	if e.peerDispatchers == nil {
		return
	}
	for nodeID := range e.peers {
		if nodeID == e.nodeID {
			continue
		}
		e.startPeerDispatcher(nodeID)
	}
}

func (e *Engine) startPeerDispatcher(nodeID uint64) {
	if _, exists := e.peerDispatchers[nodeID]; exists {
		return
	}
	size := e.perPeerQueueSize
	if size <= 0 {
		size = defaultMaxInflightMsg
	}
	baseCtx := e.dispatchCtx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	ctx, cancel := context.WithCancel(baseCtx)
	pd := &peerQueues{
		heartbeat: make(chan dispatchRequest, defaultHeartbeatBufPerPeer),
		ctx:       ctx,
		cancel:    cancel,
	}
	var workers []chan dispatchRequest
	if e.dispatcherLanesEnabled {
		// 4-lane layout: split MsgApp/MsgAppResp (replication), MsgSnap
		// (snapshot), and misc (other) onto independent goroutines so a
		// bulky snapshot transfer cannot stall replication. Each channel
		// still serves a single peer, so within-type ordering (the raft
		// invariant we care about for MsgApp) is preserved.
		pd.replication = make(chan dispatchRequest, size)
		pd.snapshot = make(chan dispatchRequest, defaultSnapshotLaneBufPerPeer)
		pd.other = make(chan dispatchRequest, defaultOtherLaneBufPerPeer)
		workers = []chan dispatchRequest{pd.heartbeat, pd.replication, pd.snapshot, pd.other}
	} else {
		pd.normal = make(chan dispatchRequest, size)
		workers = []chan dispatchRequest{pd.normal, pd.heartbeat}
	}
	e.peerDispatchers[nodeID] = pd
	e.dispatchWG.Add(len(workers))
	for _, w := range workers {
		go e.runDispatchWorker(ctx, w)
	}
}

// walRetention returns the WAL segment retention cap for this engine. As
// with snapshotThreshold, the value is cached at Open so the purge path
// does not re-parse ELASTICKV_RAFT_MAX_WAL_FILES on every snapshot release.
// A zero cache (tests that bypass Open) falls back to defaultMaxWALFiles.
func (e *Engine) walRetention() int {
	if e.maxWALFiles == 0 {
		return defaultMaxWALFiles
	}
	return e.maxWALFiles
}

// snapshotThreshold returns the FSM-snapshot trigger threshold for this
// engine. The value is cached once at Open via snapshotEveryFromEnv so the
// hot path (maybePersistLocalSnapshot runs per Ready-drain) does not re-parse
// the env var or re-emit warnings on every loop iteration. When the engine is
// constructed directly (e.g. in unit tests that bypass Open) snapshotEvery is
// zero; fall back to defaultSnapshotEvery rather than treating zero as "never
// snapshot", which would silently break those tests.
func (e *Engine) snapshotThreshold() uint64 {
	if e.snapshotEvery == 0 {
		return defaultSnapshotEvery
	}
	return e.snapshotEvery
}

// snapshotEveryFromEnv returns the FSM-snapshot trigger threshold (in applied
// raft entries past the last snapshot). Operators can override via
// ELASTICKV_RAFT_SNAPSHOT_COUNT; invalid or missing values fall back to
// defaultSnapshotEvery. Values < 1 are clamped to 1 rather than rejected so a
// misconfiguration errs on the side of MORE-frequent snapshots (smaller WAL
// footprint) instead of disabling snapshotting entirely.
func snapshotEveryFromEnv() uint64 {
	v := strings.TrimSpace(os.Getenv(snapshotEveryEnvVar))
	if v == "" {
		return defaultSnapshotEvery
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		slog.Warn("invalid ELASTICKV_RAFT_SNAPSHOT_COUNT; using default",
			"value", v, "default", defaultSnapshotEvery)
		return defaultSnapshotEvery
	}
	if n < 1 {
		return 1
	}
	return n
}

// dispatcherLanesEnabledFromEnv returns true when the 4-lane dispatcher has
// been explicitly opted into via ELASTICKV_RAFT_DISPATCHER_LANES. The value
// is parsed with strconv.ParseBool, which accepts the standard tokens
// (1, t, T, TRUE, true, True enable; 0, f, F, FALSE, false, False disable).
// An empty string or any unrecognized value disables the feature.
func dispatcherLanesEnabledFromEnv() bool {
	v := strings.TrimSpace(os.Getenv(dispatcherLanesEnvVar))
	enabled, err := strconv.ParseBool(v)
	if err != nil {
		return false
	}
	return enabled
}

// closePeerLanes closes every non-nil dispatch channel on pd so that the
// drain loops in runDispatchWorker exit. It is safe to call with either the
// 2-lane or 4-lane layout because unused lanes are nil.
func closePeerLanes(pd *peerQueues) {
	for _, ch := range []chan dispatchRequest{pd.heartbeat, pd.normal, pd.replication, pd.snapshot, pd.other} {
		if ch != nil {
			close(ch)
		}
	}
}

// runDispatchWorker drains ch until the channel is closed, the engine stops,
// or the per-peer context is cancelled (e.g. by removePeer). The ctx.Done()
// arm ensures old workers exit promptly when a peer is removed and
// immediately re-added, preventing stale goroutines from shadowing new ones.
func (e *Engine) runDispatchWorker(ctx context.Context, ch chan dispatchRequest) {
	defer e.dispatchWG.Done()
	for {
		select {
		case <-e.dispatchStopCh:
			return
		case <-ctx.Done():
			return
		case req, ok := <-ch:
			if !ok {
				return
			}
			if ctx.Err() != nil {
				if err := req.Close(); err != nil {
					slog.Error("etcd raft dispatch: failed to close request", "err", err)
				}
				return
			}
			e.handleDispatchRequest(ctx, req)
		}
	}
}

func (e *Engine) handleDispatchRequest(ctx context.Context, req dispatchRequest) {
	dispatchErr := e.dispatchTransport(ctx, req)
	if err := req.Close(); err != nil {
		slog.Error("etcd raft dispatch: failed to close request", "err", err)
	}
	if dispatchErr == nil || errors.Is(dispatchErr, ctx.Err()) {
		return
	}
	code := dispatchErrorCodeOf(dispatchErr)
	count := e.recordDispatchErrorCode(code)
	if shouldLogDispatchEvent(count) {
		slog.Warn("etcd raft outbound dispatch failed",
			"node_id", e.nodeID,
			"to", req.msg.To,
			"type", req.msg.Type.String(),
			"dispatch_error_count", count,
			"code", code,
			"err", dispatchErr,
		)
	}
	// Inform etcd/raft that the peer is unreachable so Progress transitions
	// out of StateReplicate / StateSnapshot. Without this the leader keeps
	// Progress stuck and never retries sendAppend/sendSnap for the peer,
	// leaving the follower indefinitely stale even after heartbeats resume.
	e.postDispatchReport(dispatchReport{to: req.msg.To, msgType: req.msg.Type})
}

func (e *Engine) stopDispatchWorkers() {
	e.dispatchOnce.Do(func() {
		if e.dispatchCancel != nil {
			e.dispatchCancel()
		}
		if e.dispatchStopCh != nil {
			close(e.dispatchStopCh)
		}
		e.dispatchWG.Wait()
	})
}

func (e *Engine) startSnapshotWorker() {
	if e.snapshotReqCh == nil {
		return
	}
	e.snapshotWG.Add(1)
	go e.runSnapshotWorker()
}

func (e *Engine) runSnapshotWorker() {
	defer e.snapshotWG.Done()
	for {
		select {
		case <-e.snapshotStopCh:
			return
		case req := <-e.snapshotReqCh:
			result := snapshotResult{
				index: req.index,
				err:   e.persistLocalSnapshot(req),
			}
			select {
			case <-e.snapshotStopCh:
				return
			case e.snapshotResCh <- result:
			}
		}
	}
}

func (e *Engine) stopSnapshotWorker() {
	e.snapshotOnce.Do(func() {
		if e.snapshotStopCh != nil {
			close(e.snapshotStopCh)
		}
		e.snapshotWG.Wait()
	})
}

func (e *Engine) leaderInfo(leaderNodeID uint64) raftengine.LeaderInfo {
	if leaderNodeID == 0 {
		return raftengine.LeaderInfo{}
	}
	e.mu.RLock()
	peer, ok := e.peers[leaderNodeID]
	e.mu.RUnlock()
	if ok {
		return raftengine.LeaderInfo{ID: peer.ID, Address: peer.Address}
	}
	return raftengine.LeaderInfo{ID: strconv.FormatUint(leaderNodeID, 10)}
}

func (e *Engine) resolveConfigChange(index uint64, context []byte) {
	id, peer, ok := decodeConfChangeContext(context)
	if !ok {
		return
	}
	req, ok := e.popPendingConfig(id)
	if !ok {
		return
	}
	req.done <- adminResult{
		index: index,
		peer:  peer,
	}
}

func (e *Engine) peerForID(id string) (Peer, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, peer := range e.peers {
		if peer.ID == id {
			return peer, true
		}
	}
	if id == e.localID {
		return Peer{
			NodeID:  e.nodeID,
			ID:      e.localID,
			Address: e.localAddress,
		}, true
	}
	for _, server := range e.config.Servers {
		if server.ID != id {
			continue
		}
		return Peer{
			NodeID:  DeriveNodeID(server.ID),
			ID:      server.ID,
			Address: server.Address,
		}, true
	}
	return Peer{}, false
}

func (e *Engine) resolveTransferTarget(target Peer) (Peer, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if len(e.config.Servers) == 0 {
		return Peer{}, errors.WithStack(errLeadershipTransferNotReady)
	}
	if target.ID == "" && target.Address == "" {
		return e.defaultTransferTargetLocked()
	}
	return e.namedTransferTargetLocked(target)
}

func (e *Engine) defaultTransferTargetLocked() (Peer, error) {
	for _, server := range e.config.Servers {
		if server.ID == e.localID {
			continue
		}
		return peerFromServer(e.peers, server), nil
	}
	return Peer{}, errors.WithStack(errLeadershipTransferNotReady)
}

func (e *Engine) namedTransferTargetLocked(target Peer) (Peer, error) {
	for _, server := range e.config.Servers {
		if target.ID != "" && server.ID != target.ID {
			continue
		}
		if target.Address != "" && server.Address != target.Address {
			continue
		}
		if server.ID == e.localID {
			return Peer{}, errors.WithStack(errLeadershipTransferTarget)
		}
		return peerFromServer(e.peers, server), nil
	}
	if target.ID != "" {
		return Peer{}, errors.Wrapf(errTransportPeerUnknown, "id=%q", target.ID)
	}
	return Peer{}, errors.Wrapf(errTransportPeerUnknown, "address=%q", target.Address)
}

// upsertPeer updates the address cache for a peer. It deliberately
// does NOT touch e.config.Servers — suffrage is determined by the
// post-change ConfState which the apply loop threads through
// refreshConfigServers. Today's call sites are the apply-loop helpers
// applyAddedPeer / applyUpdatedPeer (both reached from
// applyConfigPeerChange) which run before refreshConfigServers within
// the same apply iteration.
func (e *Engine) upsertPeer(peer Peer) {
	if peer.NodeID == 0 {
		peer.NodeID = DeriveNodeID(peer.ID)
	}
	if peer.ID == "" {
		peer.ID = strconv.FormatUint(peer.NodeID, 10)
	}

	e.mu.Lock()
	if e.peers == nil {
		e.peers = make(map[uint64]Peer)
	}
	e.peers[peer.NodeID] = peer
	e.mu.Unlock()

	if e.transport != nil {
		e.transport.UpsertPeer(peer)
	}
	if e.peerDispatchers != nil && peer.NodeID != e.nodeID {
		e.startPeerDispatcher(peer.NodeID)
	}
}

func (e *Engine) nextPeersAfterConfigChange(changeType raftpb.ConfChangeType, nodeID uint64, context []byte, confState raftpb.ConfState) []Peer {
	e.mu.RLock()
	next := clonePeerMap(e.peers)
	e.mu.RUnlock()
	applyConfigPeerChangeToMap(next, changeType, nodeID, context)
	annotatePeerSuffrageFromConfState(next, confState)
	return sortedPeerList(next)
}

func (e *Engine) nextPeersAfterConfigChangeV2(cc raftpb.ConfChangeV2, confState raftpb.ConfState) []Peer {
	e.mu.RLock()
	next := clonePeerMap(e.peers)
	e.mu.RUnlock()
	for _, change := range cc.Changes {
		applyConfigPeerChangeToMap(next, change.Type, change.NodeID, cc.Context)
	}
	annotatePeerSuffrageFromConfState(next, confState)
	return sortedPeerList(next)
}

// annotatePeerSuffrageInSlice is the slice form of
// annotatePeerSuffrageFromConfState used at bootstrap-time, where
// `peers` is a sorted operator-provided slice rather than the apply
// loop's clone of e.peers.
func annotatePeerSuffrageInSlice(peers []Peer, conf raftpb.ConfState) {
	if len(peers) == 0 {
		return
	}
	learners := make(map[uint64]bool, len(conf.Learners))
	for _, id := range conf.Learners {
		learners[id] = true
	}
	voters := make(map[uint64]bool, len(conf.Voters))
	for _, id := range conf.Voters {
		voters[id] = true
	}
	for i := range peers {
		switch {
		case learners[peers[i].NodeID]:
			peers[i].Suffrage = SuffrageLearner
		case voters[peers[i].NodeID]:
			peers[i].Suffrage = SuffrageVoter
		}
	}
}

// annotatePeerSuffrageFromConfState stamps each peer's Suffrage from
// the post-change ConfState so the v2 peers file written by
// persistConfigState round-trips suffrage across restarts. Without
// this, every Peer in the persist path carries Suffrage="" (the
// ConfChange context bytes do not encode suffrage; e.peers
// deliberately stores only nodeID/ID/address), and
// persistedSuffrageByte("") writes voter — which on restart causes
// validateConfState to see a learner-as-voter peer list and reject
// startup with errClusterMismatch. See learner design doc §4.3
// "Authoritative source of suffrage during recovery".
//
// The map is mutated in place. Peers not present in either
// confState.Voters or confState.Learners (transient state during a
// removal) keep whatever Suffrage they had — they will fall out of
// the persisted peers list on the next conf change anyway.
func annotatePeerSuffrageFromConfState(peers map[uint64]Peer, conf raftpb.ConfState) {
	if len(peers) == 0 {
		return
	}
	learners := make(map[uint64]bool, len(conf.Learners))
	for _, id := range conf.Learners {
		learners[id] = true
	}
	voters := make(map[uint64]bool, len(conf.Voters))
	for _, id := range conf.Voters {
		voters[id] = true
	}
	for id, peer := range peers {
		switch {
		case learners[id]:
			peer.Suffrage = SuffrageLearner
		case voters[id]:
			peer.Suffrage = SuffrageVoter
		default:
			continue
		}
		peers[id] = peer
	}
}

func (e *Engine) removePeer(nodeID uint64) {
	if nodeID == 0 {
		return
	}

	e.mu.Lock()
	delete(e.peers, nodeID)
	// e.voterCount has already been refreshed for the post-change
	// ConfState by refreshVoterCache earlier in the apply iteration
	// (see applyConfigChange / applyConfigChangeV2). The ack tracker
	// quorum threshold below uses the voter count, not the address
	// cache size, because a learner ack must not contribute to the
	// voter-majority denominator. See
	// docs/design/2026_04_26_proposed_raft_learner.md §4.6.
	postRemovalVoterCount := e.voterCount
	e.mu.Unlock()

	// Drop the peer's recorded ack so a reconfiguration cannot leave a
	// stale entry that falsely satisfies the new cluster's majority.
	// followerQuorum is computed against the POST-removal voter count; a
	// shrink to <=1 would otherwise pass 0 here, which
	// quorumAckTracker.removePeer treats as "keep the current instant"
	// and would surface stale liveness to LastQuorumAck if the cluster
	// subsequently grew back. Clear the tracker explicitly in that
	// case so any future multi-node membership starts fresh.
	if postRemovalVoterCount <= 1 {
		e.ackTracker.reset()
	} else {
		e.ackTracker.removePeer(nodeID, followerQuorumForClusterSize(postRemovalVoterCount))
	}

	if e.transport != nil {
		e.transport.RemovePeer(nodeID)
	}
	if e.peerDispatchers != nil {
		if pd, ok := e.peerDispatchers[nodeID]; ok {
			delete(e.peerDispatchers, nodeID)
			pd.cancel() // cancel any in-flight RPC for this peer immediately
			closePeerLanes(pd)
		}
	}
}

func (e *Engine) hasPeer(nodeID uint64) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	_, ok := e.peers[nodeID]
	return ok
}

func encodeProposalEnvelope(id uint64, payload []byte) []byte {
	out := make([]byte, envelopeHeaderSize+len(payload))
	out[0] = proposalEnvelopeVersion
	binary.BigEndian.PutUint64(out[1:envelopeHeaderSize], id)
	copy(out[envelopeHeaderSize:], payload)
	return out
}

func decodeProposalEnvelope(data []byte) (uint64, []byte, bool) {
	if len(data) < envelopeHeaderSize || data[0] != proposalEnvelopeVersion {
		return 0, nil, false
	}
	return binary.BigEndian.Uint64(data[1:envelopeHeaderSize]), data[envelopeHeaderSize:], true
}

func shouldLogDispatchEvent(count uint64) bool {
	return count == 1 || count%128 == 0
}

func (e *Engine) recordDroppedDispatch(msg raftpb.Message) {
	count := e.dispatchDropCount.Add(1)
	if shouldLogDispatchEvent(count) {
		slog.Warn("dropping etcd raft outbound message",
			"node_id", e.nodeID,
			"to", msg.To,
			"type", msg.Type.String(),
			"drop_count", count,
		)
	}
}

// dispatchErrorCodeOf extracts the grpc status code name from err, or
// returns a synthetic bucket ("Canceled" / "DeadlineExceeded" when the
// Go stdlib error matches, "Unknown" otherwise). Keeps the label set
// bounded to grpc's ~15 canonical codes + 1 fallback.
func dispatchErrorCodeOf(err error) string {
	if err == nil {
		return "OK"
	}
	if s, ok := status.FromError(err); ok {
		return s.Code().String()
	}
	if errors.Is(err, context.Canceled) {
		return "Canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "DeadlineExceeded"
	}
	return "Unknown"
}

func (e *Engine) dispatchTransport(ctx context.Context, req dispatchRequest) error {
	if e.dispatchFn != nil {
		return e.dispatchFn(ctx, req)
	}
	return e.transport.Dispatch(ctx, req.msg)
}

func prepareDispatchRequest(msg raftpb.Message) dispatchRequest {
	return dispatchRequest{msg: cloneDispatchMessage(msg)}
}

func (r dispatchRequest) Close() error {
	return nil
}

func cloneDispatchMessage(msg raftpb.Message) raftpb.Message {
	cloned := msg
	cloned.Entries = cloneDispatchEntries(msg.Entries)
	cloned.Snapshot = cloneDispatchSnapshot(msg.Snapshot)
	cloned.Context = append([]byte(nil), msg.Context...)
	cloned.Responses = cloneDispatchMessages(msg.Responses)
	return cloned
}

func cloneDispatchMessages(messages []raftpb.Message) []raftpb.Message {
	if len(messages) == 0 {
		return nil
	}
	cloned := make([]raftpb.Message, len(messages))
	for i, msg := range messages {
		cloned[i] = cloneDispatchMessage(msg)
	}
	return cloned
}

func cloneDispatchEntries(entries []raftpb.Entry) []raftpb.Entry {
	if len(entries) == 0 {
		return nil
	}
	cloned := make([]raftpb.Entry, len(entries))
	for i, entry := range entries {
		cloned[i] = entry
		cloned[i].Data = append([]byte(nil), entry.Data...)
	}
	return cloned
}

func cloneDispatchSnapshot(snapshot *raftpb.Snapshot) *raftpb.Snapshot {
	if snapshot == nil {
		return nil
	}
	cloned := *snapshot
	cloned.Data = append([]byte(nil), snapshot.Data...)
	cloned.Metadata.ConfState = cloneDispatchConfState(snapshot.Metadata.ConfState)
	return &cloned
}

func cloneDispatchConfState(conf raftpb.ConfState) raftpb.ConfState {
	conf.Voters = append([]uint64(nil), conf.Voters...)
	conf.Learners = append([]uint64(nil), conf.Learners...)
	conf.VotersOutgoing = append([]uint64(nil), conf.VotersOutgoing...)
	conf.LearnersNext = append([]uint64(nil), conf.LearnersNext...)
	return conf
}

func (e *Engine) handleSnapshotResult(result snapshotResult) error {
	e.snapshotInFlight = false
	if result.err != nil {
		return result.err
	}
	return e.maybePersistLocalSnapshot()
}

func (e *Engine) persistLocalSnapshot(req snapshotRequest) error {
	if e.fsmSnapDir == "" {
		// No fsmSnapDir set (in-memory or test engine): use the legacy path.
		payload, err := snapshotBytesAndClose(req.snapshot, e.dataDir)
		if err != nil {
			return err
		}
		return e.persistLocalSnapshotPayload(req.index, payload)
	}
	crc32c, writeErr := writeFSMSnapshotFile(req.snapshot, e.fsmSnapDir, req.index)
	closeErr := req.snapshot.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return errors.WithStack(closeErr)
	}
	token := encodeSnapshotToken(req.index, crc32c)
	return e.persistLocalSnapshotPayload(req.index, token)
}

func (e *Engine) persistLocalSnapshotPayload(index uint64, payload []byte) error {
	// Keep restore and local snapshot publication serialized end-to-end so a
	// follower snapshot cannot swap in newer FSM state while an older local
	// snapshot is still being published to raft storage and disk.
	e.snapshotMu.Lock()
	defer e.snapshotMu.Unlock()

	current, err := e.storage.Snapshot()
	if err != nil {
		return errors.WithStack(err)
	}
	if index <= current.Metadata.Index {
		return nil
	}

	_, err = persistLocalSnapshotPayload(e.storage, e.persist, index, payload)
	switch {
	case err == nil:
		snapDir := filepath.Join(e.dataDir, snapDirName)
		if purgeErr := purgeOldSnapshotFiles(snapDir, e.fsmSnapDir); purgeErr != nil {
			slog.Warn("failed to purge old snap files", "error", purgeErr)
		}
		walDir := filepath.Join(e.dataDir, walDirName)
		if purgeErr := purgeOldWALFiles(walDir, e.walRetention()); purgeErr != nil {
			slog.Warn("failed to purge old wal files", "error", purgeErr)
		}
		return nil
	case errors.Is(err, etcdraft.ErrCompacted):
		return nil
	case errors.Is(err, etcdraft.ErrUnavailable):
		return nil
	case errors.Is(err, etcdraft.ErrSnapOutOfDate):
		return nil
	default:
		return err
	}
}

func encodeReadContext(id uint64) []byte {
	out := make([]byte, envelopeHeaderSize)
	out[0] = readContextVersion
	binary.BigEndian.PutUint64(out[1:envelopeHeaderSize], id)
	return out
}

func decodeReadContext(data []byte) (uint64, bool) {
	if len(data) != envelopeHeaderSize || data[0] != readContextVersion {
		return 0, false
	}
	return binary.BigEndian.Uint64(data[1:envelopeHeaderSize]), true
}

func encodeConfChangeContext(id uint64, peer Peer) ([]byte, error) {
	idLen, err := uint16Len(len(peer.ID))
	if err != nil {
		return nil, err
	}
	addressLen, err := uint16Len(len(peer.Address))
	if err != nil {
		return nil, err
	}

	out := make([]byte, confChangeFixedSize+len(peer.ID)+len(peer.Address))
	out[0] = confChangeContextVersion
	binary.BigEndian.PutUint64(out[1:9], id)
	binary.BigEndian.PutUint64(out[9:17], peer.NodeID)
	binary.BigEndian.PutUint16(out[17:19], idLen)
	binary.BigEndian.PutUint16(out[19:21], addressLen)
	offset := confChangeFixedSize
	copy(out[offset:offset+len(peer.ID)], peer.ID)
	offset += len(peer.ID)
	copy(out[offset:offset+len(peer.Address)], peer.Address)
	return out, nil
}

func decodeConfChangeContext(data []byte) (uint64, Peer, bool) {
	if len(data) < confChangeFixedSize || data[0] != confChangeContextVersion {
		return 0, Peer{}, false
	}
	idLen := int(binary.BigEndian.Uint16(data[17:19]))
	addressLen := int(binary.BigEndian.Uint16(data[19:21]))
	if len(data) != confChangeFixedSize+idLen+addressLen {
		return 0, Peer{}, false
	}
	offset := confChangeFixedSize
	peerID := string(data[offset : offset+idLen])
	offset += idLen
	address := string(data[offset : offset+addressLen])
	return binary.BigEndian.Uint64(data[1:9]), Peer{
		NodeID:  binary.BigEndian.Uint64(data[9:17]),
		ID:      peerID,
		Address: address,
	}, true
}

func uint16Len(n int) (uint16, error) {
	if n < 0 || n > 0xffff {
		return 0, errors.WithStack(errConfChangeContextTooLarge)
	}
	return uint16(n), nil
}

func peerFromServer(peers map[uint64]Peer, server raftengine.Server) Peer {
	if nodeID, ok := configServerNodeID(peers, server); ok {
		if peer, ok := peers[nodeID]; ok {
			return peer
		}
		return Peer{
			NodeID:  nodeID,
			ID:      server.ID,
			Address: server.Address,
		}
	}
	return Peer{
		NodeID:  DeriveNodeID(server.ID),
		ID:      server.ID,
		Address: server.Address,
	}
}

func configServerNodeID(peers map[uint64]Peer, server raftengine.Server) (uint64, bool) {
	for nodeID, peer := range peers {
		if server.ID != "" && peer.ID == server.ID {
			return nodeID, true
		}
		if server.Address != "" && peer.Address == server.Address {
			return nodeID, true
		}
	}
	return 0, false
}

func equalConfState(left raftpb.ConfState, right raftpb.ConfState) bool {
	return equalUint64s(left.Voters, right.Voters) &&
		equalUint64s(left.Learners, right.Learners) &&
		equalUint64s(left.VotersOutgoing, right.VotersOutgoing) &&
		equalUint64s(left.LearnersNext, right.LearnersNext) &&
		left.AutoLeave == right.AutoLeave
}

func equalUint64s(left []uint64, right []uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
