package adapter

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

func legacyEvidenceRequest(primary []byte) *pb.Request {
	return &pb.Request{IsTxn: true, Phase: pb.Phase_COMMIT, Mutations: []*pb.Mutation{{
		Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: primary, CommitTS: 60}),
	}}}
}

type primaryEvidenceBatchSpy struct {
	singleCalls int
	batchCalls  int
	keys        [][]byte
}

func (s *primaryEvidenceBatchSpy) PrimaryCommitEvidence(context.Context, []byte, uint64) (kv.PrimaryCommitEvidence, error) {
	s.singleCalls++
	return kv.PrimaryCommitEvidence{}, nil
}

func (s *primaryEvidenceBatchSpy) PrimaryCommitEvidenceBatch(_ context.Context, keys [][]byte, _ uint64) ([]kv.PrimaryCommitEvidence, error) {
	s.batchCalls++
	s.keys = keys
	results := make([]kv.PrimaryCommitEvidence, len(keys))
	for index := range results {
		results[index] = kv.PrimaryCommitEvidence{Readable: true, Found: true, CommitTS: 60}
	}
	return results, nil
}

func TestLegacyCommitEvidenceDeduplicatesBeforeBatchRead(t *testing.T) {
	t.Parallel()
	reader := &primaryEvidenceBatchSpy{}
	i := NewInternalWithEngine(nil, nil, nil, nil, WithInternalPrimaryCommitEvidence(reader))
	var reqs []*pb.Request
	for range 256 {
		reqs = append(reqs, legacyEvidenceRequest([]byte("first")), legacyEvidenceRequest([]byte("second")))
	}
	require.NoError(t, i.verifyLegacyCommitEvidence(t.Context(), reqs, 50, 60))
	require.Zero(t, reader.singleCalls)
	require.Equal(t, 1, reader.batchCalls)
	require.Equal(t, [][]byte{[]byte("first"), []byte("second")}, reader.keys)
}

func TestLegacyCommitEvidenceRejectsTooManyKeysBeforeReading(t *testing.T) {
	t.Parallel()
	reader := &primaryEvidenceBatchSpy{}
	i := NewInternalWithEngine(nil, nil, nil, nil, WithInternalPrimaryCommitEvidence(reader))
	var reqs []*pb.Request
	for index := range kv.MaxPrimaryCommitEvidenceKeys + 1 {
		reqs = append(reqs, legacyEvidenceRequest([]byte{byte(index + 1)}))
	}
	require.ErrorIs(t, i.verifyLegacyCommitEvidence(t.Context(), reqs, 50, 60), kv.ErrInvalidRequest)
	require.Zero(t, reader.singleCalls)
	require.Zero(t, reader.batchCalls)
}

func TestLegacyCommitEvidenceFallbackSharesOneBudget(t *testing.T) {
	t.Parallel()
	var deadlines []time.Time
	reads := 0
	reader := primaryCommitEvidenceFunc(func(ctx context.Context, _ []byte, _ uint64) (kv.PrimaryCommitEvidence, error) {
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.LessOrEqual(t, time.Until(deadline), legacyCommitEvidenceTimeout)
		deadlines = append(deadlines, deadline)
		reads++
		return kv.PrimaryCommitEvidence{Readable: true, Found: true, CommitTS: 60}, nil
	})
	i := NewInternalWithEngine(nil, nil, nil, nil, WithInternalPrimaryCommitEvidence(reader))
	require.NoError(t, i.verifyLegacyCommitEvidence(t.Context(), []*pb.Request{
		legacyEvidenceRequest([]byte("first")), legacyEvidenceRequest([]byte("first")), legacyEvidenceRequest([]byte("second")),
	}, 50, 60))
	require.Equal(t, 2, reads)
	require.True(t, deadlines[0].Equal(deadlines[1]))
}
