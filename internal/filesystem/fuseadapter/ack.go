package fuseadapter

import (
	"cmp"
	"context"
	"log/slog"
	"slices"
	"sort"
	"time"

	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/cockroachdb/errors"
)

const (
	ackRetryInterval = 5 * time.Second
	ackTimeout       = 5 * time.Second
	ackBatchSize     = 64
	ackClassCount    = 2
	ackDrainInterval = 10 * time.Millisecond
)

type replyIdentityKey struct{}
type backgroundOperationKey struct{}

func withReplyIdentity(ctx context.Context, unique uint64) context.Context {
	if unique == 0 {
		return ctx
	}
	return context.WithValue(ctx, replyIdentityKey{}, unique)
}

func withBackgroundOperation(ctx context.Context) context.Context {
	return context.WithValue(ctx, backgroundOperationKey{}, true)
}

func (a *Adapter) queueAcknowledgement(id filesystem.OperationID) {
	a.replyMu.Lock()
	if a.ackPending == nil {
		a.ackPending = make(map[filesystem.OperationID]struct{})
	}
	a.ackPending[id] = struct{}{}
	a.replyMu.Unlock()
	if a.ackWake != nil {
		select {
		case a.ackWake <- struct{}{}:
		default:
		}
	}
}

func (a *Adapter) recordReplyIdentity(ctx context.Context, id filesystem.OperationID) {
	unique, ok := ctx.Value(replyIdentityKey{}).(uint64)
	if !ok || unique == 0 {
		return
	}
	a.replyMu.Lock()
	if a.replyIDs == nil {
		a.replyIDs = make(map[uint64][]filesystem.OperationID)
	}
	a.replyIDs[unique] = append(a.replyIDs[unique], id)
	a.replyMu.Unlock()
}

// replyAttempted is called only after go-fuse has tried writing the reply.
// A failed device write is still a reply attempt; suppressed replies are not.
func (a *Adapter) replyAttempted(unique uint64) {
	a.replyMu.Lock()
	ids := a.replyIDs[unique]
	delete(a.replyIDs, unique)
	if a.ackPending == nil {
		a.ackPending = make(map[filesystem.OperationID]struct{})
	}
	for _, id := range ids {
		a.ackPending[id] = struct{}{}
	}
	a.replyMu.Unlock()
	if len(ids) > 0 && a.ackWake != nil {
		select {
		case a.ackWake <- struct{}{}:
		default:
		}
	}
}

// replySuppressed is the terminal boundary for a request that go-fuse will
// never answer, such as an interrupted request after the connection died.
func (a *Adapter) replySuppressed(unique uint64) {
	a.replyAttempted(unique)
}

func (a *Adapter) retryAcknowledgements(ctx context.Context) {
	ticker := time.NewTicker(ackRetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-a.ackWake:
		}
		// select picks among ready cases at random: a queued wake-up can win
		// over a cancelled context, so check it before starting a batch.
		if ctx.Err() != nil {
			return
		}
		if a.processAcknowledgementBatch(ctx) {
			select {
			case a.ackWake <- struct{}{}:
			default:
			}
		}
	}
}

func (a *Adapter) processAcknowledgementBatch(ctx context.Context) bool {
	progress := false
	for _, id := range a.pendingAcknowledgements() {
		attemptCtx, cancel := context.WithTimeout(ctx, ackTimeout)
		err := a.acknowledgeReceipt(attemptCtx, id)
		cancel()
		if err != nil {
			if logger := slog.Default(); logger.Enabled(context.Background(), slog.LevelWarn) {
				logger.Warn("filesystem FUSE operation acknowledgement deferred", "sequence", id.Sequence, "error", err.Error())
			}
			continue
		}
		a.replyMu.Lock()
		delete(a.ackPending, id)
		keys := a.unknownOperations[id]
		delete(a.unknownOperations, id)
		progress = true
		a.replyMu.Unlock()
		for _, key := range keys {
			a.mutations.resolveUnknown(key)
		}
	}
	a.replyMu.Lock()
	remaining := len(a.ackPending)
	a.replyMu.Unlock()
	return progress && remaining > 0
}

func (a *Adapter) pendingAcknowledgements() []filesystem.OperationID {
	a.replyMu.Lock()
	defer a.replyMu.Unlock()
	ids := make([]filesystem.OperationID, 0, len(a.ackPending))
	for id := range a.ackPending {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(left, right filesystem.OperationID) int {
		return cmp.Compare(left.Sequence, right.Sequence)
	})
	if len(ids) == 0 {
		return ids
	}
	selected := make([]filesystem.OperationID, 0, min(len(ids), ackBatchSize))
	seen := make(map[filesystem.OperationID]struct{}, ackBatchSize)
	// An unknown mutation blocks subsequent operations on its inode until its
	// acknowledgement resolves. Visit those receipts before ordinary ACKs so
	// a full batch of slow or contended retirements cannot delay recovery.
	// Reserve half of a full batch for ordinary ACKs when both classes have
	// work. Rotate each class independently so an ordinary ACK cannot pin the
	// unknown cursor ahead of unknown mutations that still need a retry.
	appendMatching := func(wantUnknown bool, limit int, cursor *uint64) {
		start := sort.Search(len(ids), func(i int) bool { return ids[i].Sequence > *cursor })
		for i := 0; i < len(ids) && len(selected) < limit; i++ {
			id := ids[(start+i)%len(ids)]
			_, unknown := a.unknownOperations[id]
			if unknown == wantUnknown {
				if _, alreadySelected := seen[id]; alreadySelected {
					continue
				}
				selected = append(selected, id)
				seen[id] = struct{}{}
				*cursor = id.Sequence
			}
		}
	}
	appendMatching(true, ackBatchSize/ackClassCount, &a.unknownAckCursor)
	appendMatching(false, ackBatchSize, &a.ackCursor)
	appendMatching(true, ackBatchSize, &a.unknownAckCursor)
	return selected
}

func (a *Adapter) acknowledgeReceipt(ctx context.Context, id filesystem.OperationID) error {
	acker, ok := a.core.(interface {
		AcknowledgeOperation(context.Context, filesystem.OperationID) error
	})
	if !ok {
		return filesystem.ErrUnsupported
	}
	a.receiptMu.Lock()
	hasReceipt := a.hasReceiptLocked(id)
	a.receiptMu.Unlock()
	if hasReceipt {
		verifier, ok := a.core.(operationReceiptVerifier)
		if !ok {
			return filesystem.ErrUnsupported
		}
		if err := verifier.VerifyOperationReceipt(ctx, id); err != nil && !errors.Is(err, filesystem.ErrOperationRetired) {
			return errors.Wrap(err, "verify operation before acknowledgement")
		}
	}
	if err := acker.AcknowledgeOperation(ctx, id); err != nil {
		return errors.Wrap(err, "acknowledge operation after reply")
	}
	a.receiptMu.Lock()
	for inode, ids := range a.receipts {
		delete(ids, id)
		if len(ids) == 0 {
			delete(a.receipts, inode)
		}
	}
	a.receiptMu.Unlock()
	return nil
}

func (a *Adapter) hasReceiptLocked(id filesystem.OperationID) bool {
	for _, ids := range a.receipts {
		if _, ok := ids[id]; ok {
			return true
		}
	}
	return false
}

func (a *Adapter) finishStrictSession(ctx context.Context) error {
	if !a.strictMode {
		return nil
	}
	closer, ok := a.core.(interface {
		CloseOperationSession(context.Context, filesystem.OperationSessionID) error
	})
	if !ok {
		return filesystem.ErrUnsupported
	}
	ticker := time.NewTicker(ackDrainInterval)
	defer ticker.Stop()
	for {
		a.replyMu.Lock()
		pending := len(a.ackPending) + len(a.replyIDs)
		a.replyMu.Unlock()
		if pending == 0 {
			return errors.Wrap(closer.CloseOperationSession(ctx, a.operationSession), "close strict operation session")
		}
		select {
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "wait for strict acknowledgements")
		case <-ticker.C:
		}
	}
}
