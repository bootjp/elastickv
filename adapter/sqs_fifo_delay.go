package adapter

import (
	"bytes"
	"cmp"
	"context"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	sqsFifoDelayPageSize  = 16
	sqsFifoDelayByteLimit = 1 << 20
)

// The cursor and each page's data/index changes commit together. No new
// enqueue or initial receive may cross the queue metadata fence while set.
type sqsFifoDelayReindex struct {
	Partition uint32 `json:"partition"`
	After     []byte `json:"after,omitempty"`
}

func checkFifoDelayReady(meta *sqsQueueMeta) error {
	if meta.DelayReindex != nil {
		return newSQSAPIError(http.StatusServiceUnavailable, sqsErrServiceUnavailable,
			"FIFO delay update is in progress; retry the request")
	}
	return nil
}

func (s *SQSServer) prepareFifoDelayAttributes(ctx context.Context, meta *sqsQueueMeta, attrs map[string]string, readTS uint64) error {
	if err := checkFifoDelayReady(meta); err != nil {
		return err
	}
	previousDelay := meta.DelaySeconds
	if err := applyAndValidateSetAttributes(meta, attrs); err != nil {
		return err
	}
	if err := s.validateRedrivePolicyTarget(ctx, meta, readTS); err != nil {
		return err
	}
	_, delayRequested := attrs[sqsAttrDelaySeconds]
	if meta.IsFIFO && delayRequested && (previousDelay != meta.DelaySeconds || !meta.FifoOrderIndexed) {
		if err := s.validateFifoDelayActivation(ctx, meta); err != nil {
			return err
		}
		meta.DelayReindex = &sqsFifoDelayReindex{}
	}
	return nil
}

func (s *SQSServer) validateFifoDelayActivation(ctx context.Context, meta *sqsQueueMeta) error {
	if meta.PartitionCount > 1 {
		if err := s.validateHTFIFORoutingCoverage(meta); err != nil {
			return err
		}
	}
	return s.validateFifoDelayCapability(ctx)
}

func (s *SQSServer) validateFifoDelayCapability(ctx context.Context) error {
	peers, members, err := s.fifoDelayCapabilityPeers(ctx)
	if err != nil {
		return err
	}
	report := PollSQSHTFIFOCapability(ctx, peers, PollerConfig{})
	for _, peer := range report.Peers {
		if peer.Error != "" || !slices.Contains(peer.Capabilities, sqsCapabilityFifoRetroactiveDelay) {
			return newSQSAPIError(http.StatusServiceUnavailable, sqsErrServiceUnavailable,
				"FIFO delay changes require every SQS peer to support retroactive delay")
		}
	}
	afterPeers, afterMembers, err := s.fifoDelayCapabilityPeers(ctx)
	if err != nil {
		return err
	}
	if !slices.Equal(peers, afterPeers) || !slices.Equal(members, afterMembers) {
		return newSQSAPIError(http.StatusServiceUnavailable, sqsErrServiceUnavailable,
			"Raft membership changed during FIFO delay capability validation; retry the request")
	}
	return nil
}

func (s *SQSServer) fifoDelayCapabilityPeers(ctx context.Context) ([]string, []kv.RaftMember, error) {
	provider, ok := s.coordinator.(kv.RaftMembershipCoordinator)
	if !ok {
		return nil, nil, fifoDelayMembershipUnavailable()
	}
	members, err := provider.RaftMembers(ctx)
	if err != nil || len(members) == 0 {
		return nil, nil, fifoDelayMembershipUnavailable()
	}
	for _, member := range members {
		if member.NodeID == "" || strings.TrimSpace(member.Address) == "" {
			return nil, nil, fifoDelayMembershipUnavailable()
		}
		if len(members) > 1 && strings.TrimSpace(s.leaderSQS[member.Address]) == "" {
			return nil, nil, fifoDelayMembershipUnavailable()
		}
	}
	slices.SortFunc(members, func(a, b kv.RaftMember) int {
		return cmp.Or(cmp.Compare(a.NodeID, b.NodeID), cmp.Compare(a.Address, b.Address), cmp.Compare(a.Suffrage, b.Suffrage))
	})
	return s.collectSQSPeers(), members, nil
}

func fifoDelayMembershipUnavailable() error {
	return newSQSAPIError(http.StatusServiceUnavailable, sqsErrServiceUnavailable,
		"FIFO delay changes require known Raft membership and SQS addresses for every cluster member")
}

// Only configuration requests drain a job synchronously. Receive never scans
// the backlog to repair it. The reaper advances one bounded page per tick.
func (s *SQSServer) completeFifoDelayReindex(ctx context.Context, queueName string) error {
	for {
		if err := ctx.Err(); err != nil {
			return errors.WithStack(err)
		}
		done, err := s.advanceFifoDelayPageWithRetry(ctx, queueName)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

func (s *SQSServer) advanceFifoDelayPageWithRetry(ctx context.Context, queueName string) (bool, error) {
	backoff := transactRetryInitialBackoff
	deadline := time.Now().Add(transactRetryMaxDuration)
	for range transactRetryMaxAttempts {
		done, err := s.advanceFifoDelayReindex(ctx, queueName)
		if err == nil {
			return done, nil
		}
		if errors.Is(err, kv.ErrTransactionOutcomeUnknown) || !isRetryableTransactWriteError(err) {
			return false, err
		}
		if err := waitRetryWithDeadline(ctx, deadline, backoff); err != nil {
			return false, errors.WithStack(err)
		}
		backoff = nextTransactRetryBackoff(backoff)
	}
	return false, newSQSAPIError(http.StatusInternalServerError, sqsErrInternalFailure, "FIFO delay page retry attempts exhausted")
}

func fifoDelayByAgePrefix(meta *sqsQueueMeta, partition uint32) []byte {
	if meta.PartitionCount > 1 {
		return sqsPartitionedMsgByAgePrefixForPartition(meta.Name, partition, meta.Generation)
	}
	return appendU64(sqsMsgByAgePrefixAllGenerations(meta.Name), meta.Generation)
}

func (s *SQSServer) advanceFifoDelayReindex(ctx context.Context, queueName string) (bool, error) {
	stamp, err := s.beginTxnReadTimestamp(ctx, "sqs FIFO delay update: begin read timestamp")
	if err != nil {
		return false, errors.WithStack(err)
	}
	ctx = stamp.WithDispatchVoucher(ctx)
	readTS := stamp.Timestamp()
	meta, exists, err := s.loadQueueMetaAt(ctx, queueName, readTS)
	if err != nil {
		return false, err
	}
	if !exists || meta.DelayReindex == nil {
		return true, nil
	}
	state := meta.DelayReindex
	prefix := fifoDelayByAgePrefix(meta, state.Partition)
	start := prefix
	if len(state.After) != 0 {
		start = nextScanCursorAfter(state.After)
	}
	page, groupID, err := s.scanVisibleIndexAt(ctx, start, prefixScanEnd(prefix), sqsFifoDelayPageSize, readTS)
	if err != nil {
		return false, err
	}
	req := &kv.OperationGroup[kv.OP]{IsTxn: true, StartTS: readTS,
		ReadKeys: [][]byte{sqsQueueMetaKey(queueName), sqsQueueGenKey(queueName)}}
	processed, err := s.appendFifoDelayPage(ctx, meta, page, groupID, req)
	if err != nil {
		return false, err
	}
	advanceFifoDelayCursor(meta, page, processed)
	encoded, err := encodeSQSQueueMeta(meta)
	if err != nil {
		return false, err
	}
	req.Elems = append(req.Elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: sqsQueueMetaKey(queueName), Value: encoded})
	if _, err := kv.DispatchWithReadTimestamp(ctx, s.coordinator, req); err != nil {
		return false, errors.WithStack(err)
	}
	return meta.DelayReindex == nil, nil
}

func advanceFifoDelayCursor(meta *sqsQueueMeta, page []*store.KVPair, processed int) {
	state := meta.DelayReindex
	if processed > 0 {
		state.After = bytes.Clone(page[processed-1].Key)
	}
	if processed == len(page) && len(page) < sqsFifoDelayPageSize {
		state.Partition++
		state.After = nil
		if state.Partition >= effectivePartitionCount(meta) {
			meta.DelayReindex = nil
			meta.FifoOrderIndexed = true
		}
	}
}

func (s *SQSServer) appendFifoDelayPage(ctx context.Context, meta *sqsQueueMeta, page []*store.KVPair, groupID uint64, req *kv.OperationGroup[kv.OP]) (int, error) {
	bytesWritten := 0
	for i, entry := range page {
		cand := sqsMsgCandidate{partition: meta.DelayReindex.Partition, groupID: groupID, messageID: string(entry.Value)}
		rec, dataKey, gone, err := s.loadCandidateRecord(ctx, meta.Name, meta, meta.Generation, cand, req.StartTS)
		if err != nil {
			return 0, err
		}
		if gone {
			continue
		}
		elems, cost, err := fifoDelayRecordOps(meta, cand.partition, rec, dataKey)
		if err != nil {
			return 0, err
		}
		if bytesWritten > 0 && bytesWritten+cost > sqsFifoDelayByteLimit {
			return i, nil
		}
		bytesWritten += cost
		req.ReadKeys = append(req.ReadKeys, dataKey)
		req.Elems = append(req.Elems, elems...)
	}
	return len(page), nil
}

func fifoDelayRecordOps(meta *sqsQueueMeta, partition uint32, rec *sqsMessageRecord, dataKey []byte) ([]*kv.Elem[kv.OP], int, error) {
	if rec.MessageGroupId == "" || rec.SequenceNumber == 0 {
		return nil, 0, errors.New("FIFO delay backfill found an invalid FIFO record")
	}
	orderKey := sqsFifoOrderKey(meta, meta.Name, partition, meta.Generation, rec)
	elems := []*kv.Elem[kv.OP]{{Op: kv.Put, Key: orderKey, Value: []byte(rec.MessageID)}}
	cost := len(orderKey) + len(rec.MessageID)
	if rec.ReceiveCount != 0 {
		return elems, cost, nil
	}
	oldVis := sqsMsgVisKeyDispatch(meta, meta.Name, partition, meta.Generation, rec.VisibleAtMillis, rec.MessageID)
	visibleAt := rec.SendTimestampMillis + meta.DelaySeconds*sqsMillisPerSecond
	if rec.VisibleAtMillis == visibleAt && rec.AvailableAtMillis == visibleAt {
		return elems, cost, nil
	}
	rec.AvailableAtMillis, rec.VisibleAtMillis = visibleAt, visibleAt
	encoded, err := encodeSQSMessageRecord(rec)
	if err != nil {
		return nil, 0, err
	}
	newVis := sqsMsgVisKeyDispatch(meta, meta.Name, partition, meta.Generation, visibleAt, rec.MessageID)
	elems = append(elems,
		&kv.Elem[kv.OP]{Op: kv.Del, Key: oldVis},
		&kv.Elem[kv.OP]{Op: kv.Put, Key: dataKey, Value: encoded},
		&kv.Elem[kv.OP]{Op: kv.Put, Key: newVis, Value: []byte(rec.MessageID)})
	return elems, cost + len(encoded) + len(dataKey) + len(oldVis) + len(newVis) + len(rec.MessageID), nil
}
