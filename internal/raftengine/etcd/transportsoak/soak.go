package transportsoak

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	etcdtransport "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/cockroachdb/errors"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
)

const EvidenceSchemaVersion = 2

const (
	evidenceFileMode               = os.FileMode(0o600)
	minimumGroups                  = 2
	minimumNodesPerGroup           = 3
	defaultGroups                  = 3
	defaultNodesPerGroup           = 3
	defaultMessagesPerLink         = 16
	defaultRegularPayloadBytes     = 1024
	defaultBackpressurePayloadSize = 1 << 20
	defaultSnapshotPayloadBytes    = 256 << 10
	minimumBackpressurePayloadSize = 64 << 10
	rebindAttempts                 = 50
	backpressureAttempts           = 8
	minimumRecoveryMessages        = 2
	rebindRetryDelay               = 20 * time.Millisecond
	steadyDispatchTimeout          = 3 * time.Second
	disconnectObservationTimeout   = 2 * time.Second
	disconnectAttemptTimeout       = 200 * time.Millisecond
	backpressureAttemptTimeout     = 150 * time.Millisecond
	recoveryDispatchTimeout        = 5 * time.Second
	deliveryPollInterval           = 10 * time.Millisecond
	dispatchAttemptTimeout         = 500 * time.Millisecond
	dispatchRetryDelay             = 25 * time.Millisecond
	disconnectRecoverySequence     = ^uint64(0) - 1
	backpressureRecoverySequence   = ^uint64(0) - 2
)

type regularMessageKey struct {
	sender   uint64
	sequence uint64
}

type Config struct {
	Groups                   int `json:"groups"`
	NodesPerGroup            int `json:"nodes_per_group"`
	MessagesPerLink          int `json:"messages_per_link"`
	RegularPayloadBytes      int `json:"regular_payload_bytes"`
	BackpressurePayloadBytes int `json:"backpressure_payload_bytes"`
	SnapshotPayloadBytes     int `json:"snapshot_payload_bytes"`
}

func DefaultConfig() Config {
	return Config{
		Groups:                   defaultGroups,
		NodesPerGroup:            defaultNodesPerGroup,
		MessagesPerLink:          defaultMessagesPerLink,
		RegularPayloadBytes:      defaultRegularPayloadBytes,
		BackpressurePayloadBytes: defaultBackpressurePayloadSize,
		SnapshotPayloadBytes:     defaultSnapshotPayloadBytes,
	}
}

type NodeEvidence struct {
	Node                    uint64            `json:"node"`
	Address                 string            `json:"address"`
	RegularReceived         uint64            `json:"regular_received"`
	RegularReceivedBySender map[uint64]uint64 `json:"regular_received_by_sender"`
	SnapshotsReceived       uint64            `json:"snapshots_received"`
	SendStreamOpens         uint64            `json:"send_stream_opens"`
	SendStreamReconnects    uint64            `json:"send_stream_reconnects"`
	SendStreamMessages      uint64            `json:"send_stream_messages"`
	SnapshotStreamSends     uint64            `json:"snapshot_stream_sends"`
	SnapshotPayloadBytes    uint64            `json:"snapshot_payload_bytes"`
}

type GroupEvidence struct {
	GroupID            int            `json:"group_id"`
	Nodes              []NodeEvidence `json:"nodes"`
	DisconnectErrors   uint64         `json:"disconnect_errors"`
	BackpressureErrors uint64         `json:"backpressure_errors"`
	RecoveryMessages   uint64         `json:"recovery_messages"`
}

type Evidence struct {
	SchemaVersion int             `json:"schema_version"`
	StartedAt     time.Time       `json:"started_at"`
	FinishedAt    time.Time       `json:"finished_at"`
	Config        Config          `json:"config"`
	Groups        []GroupEvidence `json:"groups"`
	Result        string          `json:"result"`
}

type soakNode struct {
	id        uint64
	address   string
	transport *etcdtransport.GRPCTransport

	serverMu sync.Mutex
	server   *grpc.Server
	listener net.Listener

	gateMu sync.RWMutex
	gate   chan struct{}

	receivedMu              sync.Mutex
	regularReceivedBySender map[uint64]uint64
	regularReceivedByKey    map[regularMessageKey]uint64
	snapshotsReceived       atomic.Uint64
}

type soakGroup struct {
	id                 int
	nodes              []*soakNode
	disconnectErrors   atomic.Uint64
	backpressureErrors atomic.Uint64
	recoveryMessages   atomic.Uint64
}

func Run(ctx context.Context, cfg Config) (Evidence, error) {
	if err := validateConfig(cfg); err != nil {
		return Evidence{}, err
	}
	evidence := Evidence{SchemaVersion: EvidenceSchemaVersion, StartedAt: time.Now().UTC(), Config: cfg}

	groups, err := startGroups(ctx, cfg)
	if err != nil {
		return Evidence{}, err
	}
	defer closeGroups(groups)

	if err := runSteadyTraffic(ctx, groups, cfg); err != nil {
		return Evidence{}, errors.Wrap(err, "steady concurrent traffic")
	}
	if err := runDisconnectTraffic(ctx, groups); err != nil {
		return Evidence{}, errors.Wrap(err, "disconnect/reconnect traffic")
	}
	if err := runBackpressureTraffic(ctx, groups, cfg); err != nil {
		return Evidence{}, errors.Wrap(err, "backpressure traffic")
	}
	if err := runSnapshotTraffic(ctx, groups, cfg); err != nil {
		return Evidence{}, errors.Wrap(err, "snapshot traffic")
	}

	evidence.FinishedAt = time.Now().UTC()
	evidence.Groups = collectEvidence(groups)
	evidence.Result = "pass"
	if err := Verify(evidence); err != nil {
		return Evidence{}, errors.Wrap(err, "verify generated evidence")
	}
	return evidence, nil
}

func validateConfig(cfg Config) error {
	switch {
	case cfg.Groups < minimumGroups:
		return errors.New("streaming soak requires at least two groups")
	case cfg.NodesPerGroup < minimumNodesPerGroup:
		return errors.New("streaming soak requires at least three nodes per group")
	case cfg.MessagesPerLink < 1:
		return errors.New("messages per link must be positive")
	case cfg.RegularPayloadBytes < 1:
		return errors.New("regular payload bytes must be positive")
	case cfg.BackpressurePayloadBytes < minimumBackpressurePayloadSize:
		return errors.New("backpressure payload must be at least 64 KiB")
	case cfg.SnapshotPayloadBytes < 1:
		return errors.New("snapshot payload bytes must be positive")
	default:
		return nil
	}
}

func positiveIntToUint64(value int) uint64 {
	if value <= 0 {
		return 0
	}
	return uint64(value) //nolint:gosec // callers validate or construct a strictly positive int.
}

func startGroups(ctx context.Context, cfg Config) ([]*soakGroup, error) {
	groups := make([]*soakGroup, 0, cfg.Groups)
	for groupID := 1; groupID <= cfg.Groups; groupID++ {
		group, err := startGroup(ctx, groupID, cfg.NodesPerGroup)
		if err != nil {
			closeGroups(groups)
			return nil, errors.Wrapf(err, "start group %d", groupID)
		}
		groups = append(groups, group)
	}
	return groups, nil
}

func startGroup(ctx context.Context, groupID int, nodeCount int) (*soakGroup, error) {
	group := &soakGroup{id: groupID, nodes: make([]*soakNode, nodeCount)}
	for i := range nodeCount {
		listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
		if err != nil {
			closeGroup(group)
			return nil, errors.WithStack(err)
		}
		group.nodes[i] = &soakNode{
			id:                      positiveIntToUint64(i + 1),
			address:                 listener.Addr().String(),
			listener:                listener,
			regularReceivedBySender: make(map[uint64]uint64, nodeCount-1),
			regularReceivedByKey:    make(map[regularMessageKey]uint64),
		}
	}
	for i, node := range group.nodes {
		peers := make([]etcdtransport.Peer, 0, nodeCount-1)
		for j, peer := range group.nodes {
			if i == j {
				continue
			}
			peers = append(peers, etcdtransport.Peer{NodeID: peer.id, ID: fmt.Sprintf("g%d-n%d", groupID, j+1), Address: peer.address})
		}
		node.transport = etcdtransport.NewGRPCTransport(peers)
		node.transport.SetSendStreamEnabled(true)
		node.transport.SetHandler(node.handle)
		node.startServerWithListener(node.listener)
	}
	return group, nil
}

func (n *soakNode) handle(ctx context.Context, msg raftpb.Message) error {
	n.gateMu.RLock()
	gate := n.gate
	n.gateMu.RUnlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		}
	}
	if msg.GetType() == raftpb.MsgSnap {
		n.snapshotsReceived.Add(1)
	} else {
		n.receivedMu.Lock()
		n.regularReceivedBySender[msg.GetFrom()]++
		n.regularReceivedByKey[regularMessageKey{sender: msg.GetFrom(), sequence: msg.GetIndex()}]++
		n.receivedMu.Unlock()
	}
	return nil
}

func (n *soakNode) regularReceivedFrom(sender uint64) uint64 {
	n.receivedMu.Lock()
	defer n.receivedMu.Unlock()
	return n.regularReceivedBySender[sender]
}

func (n *soakNode) regularReceivedAt(sender, sequence uint64) uint64 {
	n.receivedMu.Lock()
	defer n.receivedMu.Unlock()
	return n.regularReceivedByKey[regularMessageKey{sender: sender, sequence: sequence}]
}

func (n *soakNode) regularReceivedSnapshot() (uint64, map[uint64]uint64) {
	n.receivedMu.Lock()
	defer n.receivedMu.Unlock()
	out := make(map[uint64]uint64, len(n.regularReceivedBySender))
	var total uint64
	for sender, count := range n.regularReceivedBySender {
		out[sender] = count
		total += count
	}
	return total, out
}

func (n *soakNode) startServerWithListener(listener net.Listener) {
	n.serverMu.Lock()
	defer n.serverMu.Unlock()
	server := grpc.NewServer()
	n.transport.Register(server)
	n.server = server
	n.listener = listener
	go func() { _ = server.Serve(listener) }()
}

func (n *soakNode) stopServer() {
	n.serverMu.Lock()
	defer n.serverMu.Unlock()
	if n.server != nil {
		n.server.Stop()
		n.server = nil
		n.listener = nil
	}
}

func (n *soakNode) restartServer(ctx context.Context) error {
	var lastErr error
	for range rebindAttempts {
		listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", n.address)
		if err == nil {
			n.startServerWithListener(listener)
			return nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case <-time.After(rebindRetryDelay):
		}
	}
	return errors.Wrap(lastErr, "rebind receiver address")
}

func (n *soakNode) block() {
	n.gateMu.Lock()
	defer n.gateMu.Unlock()
	if n.gate == nil {
		n.gate = make(chan struct{})
	}
}

func (n *soakNode) unblock() {
	n.gateMu.Lock()
	defer n.gateMu.Unlock()
	if n.gate != nil {
		close(n.gate)
		n.gate = nil
	}
}

func closeGroups(groups []*soakGroup) {
	for _, group := range groups {
		closeGroup(group)
	}
}

func closeGroup(group *soakGroup) {
	if group == nil {
		return
	}
	for _, node := range group.nodes {
		if node == nil {
			continue
		}
		node.unblock()
		node.stopServer()
		if node.listener != nil {
			_ = node.listener.Close()
		}
		if node.transport != nil {
			_ = node.transport.Close()
		}
	}
}

func runSteadyTraffic(ctx context.Context, groups []*soakGroup, cfg Config) error {
	payload := make([]byte, cfg.RegularPayloadBytes)
	var wg sync.WaitGroup
	errCh := make(chan error, cfg.Groups*cfg.NodesPerGroup*cfg.NodesPerGroup)
	for _, group := range groups {
		for _, from := range group.nodes {
			for _, to := range group.nodes {
				if from == to {
					continue
				}
				wg.Add(1)
				go func(groupID int, from, to *soakNode) {
					defer wg.Done()
					for sequence := 1; sequence <= cfg.MessagesPerLink; sequence++ {
						msg := regularMessage(from.id, to.id, positiveIntToUint64(sequence), payload)
						if err := dispatchWithRetry(ctx, from.transport, msg, steadyDispatchTimeout); err != nil {
							errCh <- errors.Wrapf(err, "group %d node %d -> %d sequence %d", groupID, from.id, to.id, sequence)
							return
						}
					}
				}(group.id, from, to)
			}
		}
	}
	wg.Wait()
	close(errCh)
	if err := combineErrors(errCh); err != nil {
		return err
	}
	return waitForSteadyDeliveries(ctx, groups, cfg)
}

func waitForSteadyDeliveries(ctx context.Context, groups []*soakGroup, cfg Config) error {
	for _, group := range groups {
		for _, receiver := range group.nodes {
			for _, sender := range group.nodes {
				if sender == receiver {
					continue
				}
				if err := waitForRegularDelivery(ctx, receiver, sender.id, positiveIntToUint64(cfg.MessagesPerLink)); err != nil {
					return errors.Wrapf(err, "group %d steady delivery node %d -> %d", group.id, sender.id, receiver.id)
				}
			}
		}
	}
	return nil
}

func runDisconnectTraffic(ctx context.Context, groups []*soakGroup) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(groups))
	for _, group := range groups {
		wg.Add(1)
		go func(group *soakGroup) {
			defer wg.Done()
			sender, receiver := group.nodes[0], group.nodes[1]
			receiver.stopServer()
			failureCtx, failureCancel := context.WithTimeout(ctx, disconnectObservationTimeout)
			for sequence := uint64(1); ; sequence++ {
				attemptCtx, attemptCancel := context.WithTimeout(failureCtx, disconnectAttemptTimeout)
				err := sender.transport.Dispatch(attemptCtx, regularMessage(sender.id, receiver.id, sequence, []byte("disconnect")))
				attemptCancel()
				if err != nil {
					break
				}
				select {
				case <-failureCtx.Done():
					failureCancel()
					errCh <- errors.Errorf("group %d did not observe receiver disconnect", group.id)
					return
				case <-time.After(deliveryPollInterval):
				}
			}
			failureCancel()
			group.disconnectErrors.Add(1)
			if err := receiver.restartServer(ctx); err != nil {
				errCh <- errors.Wrapf(err, "group %d restart receiver", group.id)
				return
			}
			if err := dispatchWithRetry(ctx, sender.transport, regularMessage(sender.id, receiver.id, disconnectRecoverySequence, []byte("recovered")), recoveryDispatchTimeout); err != nil {
				errCh <- errors.Wrapf(err, "group %d post-restart sentinel", group.id)
				return
			}
			if err := waitForRegularMessage(ctx, receiver, sender.id, disconnectRecoverySequence); err != nil {
				errCh <- errors.Wrapf(err, "group %d post-restart sentinel delivery", group.id)
				return
			}
			group.recoveryMessages.Add(1)
		}(group)
	}
	wg.Wait()
	close(errCh)
	return combineErrors(errCh)
}

func runBackpressureTraffic(ctx context.Context, groups []*soakGroup, cfg Config) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(groups))
	payload := make([]byte, cfg.BackpressurePayloadBytes)
	for _, group := range groups {
		wg.Add(1)
		go func(group *soakGroup) {
			defer wg.Done()
			sender, receiver := group.nodes[0], group.nodes[2]
			receiver.block()
			defer receiver.unblock()
			observed := false
			for sequence := 1; sequence <= backpressureAttempts; sequence++ {
				sendCtx, cancel := context.WithTimeout(ctx, backpressureAttemptTimeout)
				err := sender.transport.Dispatch(sendCtx, regularMessage(sender.id, receiver.id, positiveIntToUint64(sequence), payload))
				cancel()
				if err != nil {
					observed = true
					group.backpressureErrors.Add(1)
					break
				}
			}
			if !observed {
				errCh <- errors.Errorf("group %d did not observe flow-control timeout", group.id)
				return
			}
			receiver.unblock()
			if err := dispatchWithRetry(ctx, sender.transport, regularMessage(sender.id, receiver.id, backpressureRecoverySequence, []byte("backpressure-recovered")), recoveryDispatchTimeout); err != nil {
				errCh <- errors.Wrapf(err, "group %d post-backpressure sentinel", group.id)
				return
			}
			if err := waitForRegularMessage(ctx, receiver, sender.id, backpressureRecoverySequence); err != nil {
				errCh <- errors.Wrapf(err, "group %d post-backpressure sentinel delivery", group.id)
				return
			}
			group.recoveryMessages.Add(1)
		}(group)
	}
	wg.Wait()
	close(errCh)
	return combineErrors(errCh)
}

func waitForRegularDelivery(ctx context.Context, receiver *soakNode, sender, minimum uint64) error {
	ticker := time.NewTicker(deliveryPollInterval)
	defer ticker.Stop()
	for {
		if receiver.regularReceivedFrom(sender) >= minimum {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForRegularMessage(ctx context.Context, receiver *soakNode, sender, sequence uint64) error {
	ticker := time.NewTicker(deliveryPollInterval)
	defer ticker.Stop()
	for {
		if receiver.regularReceivedAt(sender, sequence) > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case <-ticker.C:
		}
	}
}

func runSnapshotTraffic(ctx context.Context, groups []*soakGroup, cfg Config) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(groups)*cfg.NodesPerGroup)
	payload := make([]byte, cfg.SnapshotPayloadBytes)
	for _, group := range groups {
		for i, sender := range group.nodes {
			receiver := group.nodes[(i+1)%len(group.nodes)]
			wg.Add(1)
			go func(groupID int, sender, receiver *soakNode) {
				defer wg.Done()
				if err := dispatchWithRetry(ctx, sender.transport, snapshotMessage(sender.id, receiver.id, payload), recoveryDispatchTimeout); err != nil {
					errCh <- errors.Wrapf(err, "group %d snapshot node %d -> %d", groupID, sender.id, receiver.id)
				}
			}(group.id, sender, receiver)
		}
	}
	wg.Wait()
	close(errCh)
	return combineErrors(errCh)
}

func dispatchWithRetry(parent context.Context, transport *etcdtransport.GRPCTransport, msg raftpb.Message, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	var lastErr error
	for {
		attemptCtx, attemptCancel := context.WithTimeout(ctx, dispatchAttemptTimeout)
		err := transport.Dispatch(attemptCtx, msg)
		attemptCancel()
		if err == nil {
			return nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return errors.Wrap(lastErr, "dispatch retry deadline")
		case <-time.After(dispatchRetryDelay):
		}
	}
}

func regularMessage(from, to, sequence uint64, payload []byte) raftpb.Message {
	messageType := raftpb.MsgApp
	term := uint64(1)
	entryType := raftpb.EntryNormal
	return raftpb.Message{
		Type:  &messageType,
		From:  &from,
		To:    &to,
		Term:  &term,
		Index: &sequence,
		Entries: []*raftpb.Entry{{
			Type:  &entryType,
			Term:  &term,
			Index: &sequence,
			Data:  payload,
		}},
	}
}

func snapshotMessage(from, to uint64, payload []byte) raftpb.Message {
	messageType := raftpb.MsgSnap
	index, term := uint64(1), uint64(1)
	return raftpb.Message{
		Type: &messageType,
		From: &from,
		To:   &to,
		Snapshot: &raftpb.Snapshot{
			Data: payload,
			Metadata: &raftpb.SnapshotMetadata{
				Index: &index,
				Term:  &term,
			},
		},
	}
}

func combineErrors(errCh <-chan error) error {
	var combined error
	for err := range errCh {
		combined = errors.CombineErrors(combined, err)
	}
	if combined != nil {
		return errors.WithStack(combined)
	}
	return nil
}

func collectEvidence(groups []*soakGroup) []GroupEvidence {
	out := make([]GroupEvidence, 0, len(groups))
	for _, group := range groups {
		groupEvidence := GroupEvidence{
			GroupID:            group.id,
			DisconnectErrors:   group.disconnectErrors.Load(),
			BackpressureErrors: group.backpressureErrors.Load(),
			RecoveryMessages:   group.recoveryMessages.Load(),
			Nodes:              make([]NodeEvidence, 0, len(group.nodes)),
		}
		for _, node := range group.nodes {
			stats := node.transport.Stats()
			regularReceived, regularReceivedBySender := node.regularReceivedSnapshot()
			groupEvidence.Nodes = append(groupEvidence.Nodes, NodeEvidence{
				Node:                    node.id,
				Address:                 node.address,
				RegularReceived:         regularReceived,
				RegularReceivedBySender: regularReceivedBySender,
				SnapshotsReceived:       node.snapshotsReceived.Load(),
				SendStreamOpens:         stats.SendStreamOpens,
				SendStreamReconnects:    stats.SendStreamReconnects,
				SendStreamMessages:      stats.SendStreamMessages,
				SnapshotStreamSends:     stats.SnapshotStreamSends,
				SnapshotPayloadBytes:    stats.SnapshotPayloadBytes,
			})
		}
		out = append(out, groupEvidence)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GroupID < out[j].GroupID })
	return out
}

func Verify(evidence Evidence) error {
	if err := verifyEvidenceHeader(evidence); err != nil {
		return err
	}
	seenGroups := make(map[int]struct{}, len(evidence.Groups))
	seenAddresses := make(map[string]struct{}, evidence.Config.Groups*evidence.Config.NodesPerGroup)
	for _, group := range evidence.Groups {
		if err := verifyGroupEvidence(evidence.Config, group, seenGroups, seenAddresses); err != nil {
			return err
		}
	}
	return nil
}

func verifyEvidenceHeader(evidence Evidence) error {
	if evidence.SchemaVersion != EvidenceSchemaVersion {
		return errors.Errorf("unsupported evidence schema %d", evidence.SchemaVersion)
	}
	if err := validateConfig(evidence.Config); err != nil {
		return err
	}
	if evidence.Result != "pass" {
		return errors.Errorf("evidence result is %q", evidence.Result)
	}
	if evidence.StartedAt.IsZero() || evidence.FinishedAt.IsZero() || evidence.FinishedAt.Before(evidence.StartedAt) {
		return errors.New("evidence timestamps are missing or out of order")
	}
	if len(evidence.Groups) != evidence.Config.Groups {
		return errors.Errorf("evidence has %d groups, want %d", len(evidence.Groups), evidence.Config.Groups)
	}
	return nil
}

type groupTotals struct {
	received          uint64
	opens             uint64
	reconnects        uint64
	streamed          uint64
	snapshotSends     uint64
	snapshotBytes     uint64
	snapshotsReceived uint64
}

func verifyGroupEvidence(cfg Config, group GroupEvidence, seenGroups map[int]struct{}, seenAddresses map[string]struct{}) error {
	if group.GroupID < 1 || group.GroupID > cfg.Groups {
		return errors.Errorf("group id %d is outside expected range 1..%d", group.GroupID, cfg.Groups)
	}
	if _, duplicate := seenGroups[group.GroupID]; duplicate {
		return errors.Errorf("duplicate group %d", group.GroupID)
	}
	seenGroups[group.GroupID] = struct{}{}
	if len(group.Nodes) != cfg.NodesPerGroup {
		return errors.Errorf("group %d has %d nodes, want %d", group.GroupID, len(group.Nodes), cfg.NodesPerGroup)
	}
	if group.DisconnectErrors == 0 {
		return errors.Errorf("group %d has no disconnect error evidence", group.GroupID)
	}
	if group.BackpressureErrors == 0 {
		return errors.Errorf("group %d has no backpressure error evidence", group.GroupID)
	}
	if group.RecoveryMessages < minimumRecoveryMessages {
		return errors.Errorf("group %d has only %d recovery messages", group.GroupID, group.RecoveryMessages)
	}

	seenNodes := make(map[uint64]struct{}, len(group.Nodes))
	var totals groupTotals
	for _, node := range group.Nodes {
		if err := verifyNodeEvidence(cfg, group.GroupID, node, seenNodes, seenAddresses); err != nil {
			return err
		}
		totals.add(node)
	}
	return verifyGroupTotals(cfg, group.GroupID, totals)
}

func verifyNodeEvidence(cfg Config, groupID int, node NodeEvidence, seenNodes map[uint64]struct{}, seenAddresses map[string]struct{}) error {
	if err := verifyNodeIdentity(cfg, groupID, node, seenNodes, seenAddresses); err != nil {
		return err
	}
	return verifyNodeTransport(cfg, groupID, node)
}

func verifyNodeIdentity(cfg Config, groupID int, node NodeEvidence, seenNodes map[uint64]struct{}, seenAddresses map[string]struct{}) error {
	nodeCount := positiveIntToUint64(cfg.NodesPerGroup)
	if node.Node < 1 || node.Node > nodeCount {
		return errors.Errorf("group %d node id %d is outside expected range 1..%d", groupID, node.Node, cfg.NodesPerGroup)
	}
	if _, duplicate := seenNodes[node.Node]; duplicate {
		return errors.Errorf("group %d has duplicate node %d", groupID, node.Node)
	}
	seenNodes[node.Node] = struct{}{}
	if node.Address == "" {
		return errors.Errorf("group %d node %d has an empty address", groupID, node.Node)
	}
	if _, duplicate := seenAddresses[node.Address]; duplicate {
		return errors.Errorf("duplicate transport address %q", node.Address)
	}
	seenAddresses[node.Address] = struct{}{}
	return nil
}

func verifyNodeTransport(cfg Config, groupID int, node NodeEvidence) error {
	nodeCount := positiveIntToUint64(cfg.NodesPerGroup)
	messagesPerLink := positiveIntToUint64(cfg.MessagesPerLink)
	if err := verifyDirectedDeliveries(groupID, node, nodeCount, messagesPerLink); err != nil {
		return err
	}
	minimumPerNode := (nodeCount - 1) * messagesPerLink
	if node.SendStreamOpens < nodeCount-1 {
		return errors.Errorf("group %d node %d opened only %d peer streams", groupID, node.Node, node.SendStreamOpens)
	}
	if node.SendStreamMessages < minimumPerNode {
		return errors.Errorf("group %d node %d streamed %d regular messages, want at least %d", groupID, node.Node, node.SendStreamMessages, minimumPerNode)
	}
	if node.SnapshotStreamSends < 1 || node.SnapshotsReceived < 1 || node.SnapshotPayloadBytes < positiveIntToUint64(cfg.SnapshotPayloadBytes) {
		return errors.Errorf("group %d node %d snapshot evidence incomplete: sends=%d received=%d bytes=%d", groupID, node.Node, node.SnapshotStreamSends, node.SnapshotsReceived, node.SnapshotPayloadBytes)
	}
	return nil
}

func verifyDirectedDeliveries(groupID int, node NodeEvidence, nodeCount, messagesPerLink uint64) error {
	var bySenderTotal uint64
	for sender, count := range node.RegularReceivedBySender {
		if sender < 1 || sender > nodeCount || sender == node.Node {
			return errors.Errorf("group %d node %d has unexpected sender %d", groupID, node.Node, sender)
		}
		bySenderTotal += count
	}
	for sender := uint64(1); sender <= nodeCount; sender++ {
		if sender == node.Node {
			continue
		}
		count := node.RegularReceivedBySender[sender]
		if count < messagesPerLink {
			return errors.Errorf("group %d link %d -> %d delivered %d regular messages, want at least %d", groupID, sender, node.Node, count, messagesPerLink)
		}
	}
	if node.RegularReceived != bySenderTotal {
		return errors.Errorf("group %d node %d regular_received=%d differs from sender total %d", groupID, node.Node, node.RegularReceived, bySenderTotal)
	}
	return nil
}

func (totals *groupTotals) add(node NodeEvidence) {
	totals.received += node.RegularReceived
	totals.opens += node.SendStreamOpens
	totals.reconnects += node.SendStreamReconnects
	totals.streamed += node.SendStreamMessages
	totals.snapshotSends += node.SnapshotStreamSends
	totals.snapshotBytes += node.SnapshotPayloadBytes
	totals.snapshotsReceived += node.SnapshotsReceived
}

func verifyGroupTotals(cfg Config, groupID int, totals groupTotals) error {
	nodeCount := positiveIntToUint64(cfg.NodesPerGroup)
	minimumSteady := nodeCount * (nodeCount - 1) * positiveIntToUint64(cfg.MessagesPerLink)
	if totals.received < minimumSteady {
		return errors.Errorf("group %d received %d regular messages, want at least %d", groupID, totals.received, minimumSteady)
	}
	if totals.opens < nodeCount*(nodeCount-1) {
		return errors.Errorf("group %d opened only %d peer streams", groupID, totals.opens)
	}
	if totals.reconnects == 0 {
		return errors.Errorf("group %d has no successful stream reconnect", groupID)
	}
	if totals.streamed < minimumSteady {
		return errors.Errorf("group %d streamed %d regular messages, want at least %d", groupID, totals.streamed, minimumSteady)
	}
	if totals.snapshotSends < nodeCount || totals.snapshotsReceived < nodeCount {
		return errors.Errorf("group %d snapshot evidence incomplete: sends=%d received=%d", groupID, totals.snapshotSends, totals.snapshotsReceived)
	}
	minimumSnapshotBytes := nodeCount * positiveIntToUint64(cfg.SnapshotPayloadBytes)
	if totals.snapshotBytes < minimumSnapshotBytes {
		return errors.Errorf("group %d snapshot bytes=%d, want at least %d", groupID, totals.snapshotBytes, minimumSnapshotBytes)
	}
	return nil
}

func WriteEvidence(path string, evidence Evidence) error {
	data, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		return errors.WithStack(err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, evidenceFileMode); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func ReadEvidence(path string) (Evidence, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Evidence{}, errors.WithStack(err)
	}
	var evidence Evidence
	if err := json.Unmarshal(data, &evidence); err != nil {
		return Evidence{}, errors.WithStack(err)
	}
	return evidence, nil
}
