package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// recordingBatchCoordinator records the mutation keys of each dispatch and
// fails the FIRST dispatch with a retryable WriteConflict — modelling a
// committed-but-conflicted attempt under leader churn (the apply may have
// landed, but the dispatch surfaced WriteConflict). The second dispatch
// succeeds. It embeds stubAdapterCoordinator for the Coordinator surface
// (IsLeader, Clock, ...) and only overrides Dispatch.
type recordingBatchCoordinator struct {
	stubAdapterCoordinator
	dispatchKeys [][]string
	// beforeDispatch, if set, runs at the start of each Dispatch with the
	// 1-based dispatch number — lets a test mutate queue state between attempts.
	beforeDispatch func(n int)
}

func (c *recordingBatchCoordinator) Dispatch(_ context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	if c.beforeDispatch != nil {
		c.beforeDispatch(len(c.dispatchKeys) + 1)
	}
	keys := make([]string, 0, len(req.Elems))
	for _, e := range req.Elems {
		keys = append(keys, string(e.Key))
	}
	c.dispatchKeys = append(c.dispatchKeys, keys)
	if len(c.dispatchKeys) == 1 {
		return nil, store.ErrWriteConflict
	}
	return &kv.CoordinateResponse{}, nil
}

func seedStandardQueue(t *testing.T, st store.MVCCStore, name string) {
	t.Helper()
	meta := &sqsQueueMeta{
		Name:               name,
		Generation:         1,
		MaximumMessageSize: 262144,
		IsFIFO:             false,
	}
	body, err := encodeSQSQueueMeta(meta)
	require.NoError(t, err)
	require.NoError(t, st.PutAt(context.Background(), sqsQueueMetaKey(name), body, 1, 0))
}

// TestSendMessageBatchStandard_RetryReusesStableKeys pins the standard-queue
// batch-send dedup fix: when the first dispatch commits-then-conflicts under
// leader churn, the retry must reuse the SAME storage keys (stable MessageIDs +
// timestamps minted once before the loop) so the committed attempt is
// overwritten idempotently. Without the fix the retry re-mints fresh
// MessageIDs, writing a SECOND copy of every entry at new keys — the
// :duplicate-elements double-send (same class as DynamoDB PR #920).
func TestSendMessageBatchStandard_RetryReusesStableKeys(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	seedStandardQueue(t, st, "q")

	coord := &recordingBatchCoordinator{stubAdapterCoordinator: stubAdapterCoordinator{clock: kv.NewHLC()}}
	srv := &SQSServer{store: st, coordinator: coord}

	entries := []sqsSendMessageBatchEntryInput{
		{Id: "a", MessageBody: "alpha"},
		{Id: "b", MessageBody: "bravo"},
	}
	successful, failed, err := srv.sendMessageBatchWithRetry(ctx, "q", entries)
	require.NoError(t, err)
	require.Empty(t, failed)
	require.Len(t, successful, 2)

	require.Len(t, coord.dispatchKeys, 2, "attempt 1 conflicts (committed-but-conflicted), attempt 2 retries")
	require.NotEmpty(t, coord.dispatchKeys[0])
	require.Equal(t, coord.dispatchKeys[0], coord.dispatchKeys[1],
		"the retry MUST reuse the same storage keys; fresh MessageIDs would double-send every entry under leader churn")
}

// TestSendMessageBatchStandard_VisKeyStableAcrossDelayChange pins codex P2:
// if a SetQueueAttributes changes the queue's DelaySeconds between a
// committed-but-conflicted attempt and its retry, the retry must still write
// the SAME vis/by-age keys (AvailableAtMillis is captured in the identity at
// first mint, not recomputed from the changed delay) — otherwise the first
// attempt's vis index entry is orphaned and can redeliver the message.
func TestSendMessageBatchStandard_VisKeyStableAcrossDelayChange(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	seedStandardQueue(t, st, "q") // DelaySeconds = 0

	coord := &recordingBatchCoordinator{stubAdapterCoordinator: stubAdapterCoordinator{clock: kv.NewHLC()}}
	coord.beforeDispatch = func(n int) {
		if n != 2 {
			return
		}
		// SetQueueAttributes raises DelaySeconds to 900 before the retry.
		meta := &sqsQueueMeta{Name: "q", Generation: 1, MaximumMessageSize: 262144, DelaySeconds: 900}
		body, err := encodeSQSQueueMeta(meta)
		require.NoError(t, err)
		require.NoError(t, st.PutAt(ctx, sqsQueueMetaKey("q"), body, 100, 0))
	}
	srv := &SQSServer{store: st, coordinator: coord}

	entries := []sqsSendMessageBatchEntryInput{{Id: "a", MessageBody: "alpha"}} // no per-message DelaySeconds
	successful, _, err := srv.sendMessageBatchWithRetry(ctx, "q", entries)
	require.NoError(t, err)
	require.Len(t, successful, 1)

	require.Len(t, coord.dispatchKeys, 2)
	require.Equal(t, coord.dispatchKeys[0], coord.dispatchKeys[1],
		"vis/by-age keys must stay stable across a mid-retry DelaySeconds change (cached AvailableAtMillis)")
}

// TestSendMessageBatchStandard_ReturnedMessageIdsStableAcrossRetry pins that
// the MessageIds reported to the client are the ones actually stored on the
// retry (not a discarded first-attempt set), so a consumer cannot observe a
// MessageId the client never received.
func TestSendMessageBatchStandard_ReturnedMessageIdsStableAcrossRetry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	seedStandardQueue(t, st, "q")

	coord := &recordingBatchCoordinator{stubAdapterCoordinator: stubAdapterCoordinator{clock: kv.NewHLC()}}
	srv := &SQSServer{store: st, coordinator: coord}

	entries := []sqsSendMessageBatchEntryInput{{Id: "a", MessageBody: "alpha"}}
	successful, _, err := srv.sendMessageBatchWithRetry(ctx, "q", entries)
	require.NoError(t, err)
	require.Len(t, successful, 1)

	// The data key embeds the MessageID; both attempts must carry the
	// returned MessageID so the client's id matches what is stored.
	wantDataKey := string(sqsMsgDataKeyDispatch(&sqsQueueMeta{Name: "q", Generation: 1}, "q", 0, 1, successful[0].MessageId))
	for i, keys := range coord.dispatchKeys {
		require.Contains(t, keys, wantDataKey, "dispatch %d must write the returned MessageID's data key", i)
	}
}
