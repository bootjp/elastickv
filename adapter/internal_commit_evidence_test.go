package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func recordedPrimaryCommitEvidence(commitTS uint64) kv.PrimaryCommitEvidenceReader {
	return primaryCommitEvidenceFunc(func(context.Context, []byte, uint64) (kv.PrimaryCommitEvidence, error) {
		return kv.PrimaryCommitEvidence{Readable: true, Found: true, CommitTS: commitTS}, nil
	})
}

type primaryCommitEvidenceFunc func(context.Context, []byte, uint64) (kv.PrimaryCommitEvidence, error)

func (f primaryCommitEvidenceFunc) PrimaryCommitEvidence(ctx context.Context, key []byte, startTS uint64) (kv.PrimaryCommitEvidence, error) {
	return f(ctx, key, startTS)
}

func TestFillForwardedTxnCommitTSEnforcesPrimaryEvidence(t *testing.T) {
	t.Parallel()
	readFailure := errors.New("primary read failed")
	for _, test := range []struct {
		name      string
		phase     pb.Phase
		commitTS  uint64
		evidence  kv.PrimaryCommitEvidence
		readErr   error
		wantErr   error
		wantReads int
	}{
		{name: "matching durable decision", phase: pb.Phase_COMMIT, commitTS: 60, evidence: kv.PrimaryCommitEvidence{Readable: true, Found: true, CommitTS: 60}, wantReads: 1},
		{name: "missing local record", phase: pb.Phase_COMMIT, commitTS: 60, evidence: kv.PrimaryCommitEvidence{Readable: true}, wantErr: kv.ErrPrePhaseDCommitUnproven, wantReads: 1},
		{name: "mismatched decision", phase: pb.Phase_COMMIT, commitTS: 60, evidence: kv.PrimaryCommitEvidence{Readable: true, Found: true, CommitTS: 61}, wantErr: kv.ErrPrePhaseDCommitUnproven, wantReads: 1},
		{name: "remote primary", phase: pb.Phase_COMMIT, commitTS: 60, wantReads: 1},
		{name: "abort needs no commit record", phase: pb.Phase_ABORT, commitTS: 60},
		{name: "post phase D needs no legacy evidence", phase: pb.Phase_COMMIT, commitTS: 150},
		{name: "read error refuses commit", phase: pb.Phase_COMMIT, commitTS: 60, readErr: readFailure, wantErr: readFailure, wantReads: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			reads := 0
			reader := primaryCommitEvidenceFunc(func(_ context.Context, key []byte, startTS uint64) (kv.PrimaryCommitEvidence, error) {
				reads++
				require.Equal(t, []byte("primary"), key)
				require.Equal(t, uint64(50), startTS)
				return test.evidence, test.readErr
			})
			i := NewInternalWithEngine(nil, nil, nil, nil,
				WithInternalTimestampAllocator(&phaseDForwardAllocator{floor: 100}),
				WithInternalPrimaryCommitEvidence(reader))
			reqs := []*pb.Request{{IsTxn: true, Phase: test.phase, Mutations: []*pb.Mutation{{
				Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("primary"), CommitTS: test.commitTS}),
			}}}}
			got, err := i.fillForwardedTxnCommitTS(context.Background(), reqs, 50)
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.commitTS, got)
			}
			require.Equal(t, test.wantReads, reads)
		})
	}
}

func TestInternalForwardRejectsUnprovenLegacyCommitBeforePropose(t *testing.T) {
	t.Parallel()
	txn := &forwardObserverTxn{}
	i := NewInternalWithEngine(txn, forwardObserverLeader{}, nil, nil,
		WithInternalTimestampAllocator(&phaseDForwardAllocator{floor: 100}),
		WithInternalPrimaryCommitEvidence(primaryCommitEvidenceFunc(func(context.Context, []byte, uint64) (kv.PrimaryCommitEvidence, error) {
			return kv.PrimaryCommitEvidence{Readable: true}, nil
		})))
	response, err := i.Forward(context.Background(), &pb.ForwardRequest{IsTxn: true, Requests: []*pb.Request{{
		Ts: 50, IsTxn: true, Phase: pb.Phase_COMMIT, Mutations: []*pb.Mutation{{
			Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("primary"), CommitTS: 60}),
		}},
	}}})
	require.ErrorIs(t, err, kv.ErrPrePhaseDCommitUnproven)
	require.False(t, response.Success)
	require.Empty(t, txn.reqs)
}

func TestFillForwardedTxnCommitTSChecksEveryPrimary(t *testing.T) {
	t.Parallel()
	var readKeys []string
	i := NewInternalWithEngine(nil, nil, nil, nil,
		WithInternalTimestampAllocator(&phaseDForwardAllocator{floor: 100}),
		WithInternalPrimaryCommitEvidence(primaryCommitEvidenceFunc(func(_ context.Context, key []byte, _ uint64) (kv.PrimaryCommitEvidence, error) {
			readKeys = append(readKeys, string(key))
			return kv.PrimaryCommitEvidence{Readable: true, Found: string(key) == "first", CommitTS: 60}, nil
		})))
	var reqs []*pb.Request
	for _, key := range []string{"first", "second"} {
		reqs = append(reqs, &pb.Request{IsTxn: true, Phase: pb.Phase_COMMIT, Mutations: []*pb.Mutation{{
			Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte(key), CommitTS: 60}),
		}}})
	}
	_, err := i.fillForwardedTxnCommitTS(context.Background(), reqs, 50)
	require.ErrorIs(t, err, kv.ErrPrePhaseDCommitUnproven)
	require.Equal(t, []string{"first", "second"}, readKeys)
}

func TestInternalForwardAllowsPreparedFirstPrimaryCommitAcrossActivation(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name          string
		key           string
		prepared      bool
		found         bool
		decision      uint64
		proofTS       uint64
		legacyAllowed bool
		wantErr       bool
	}{
		{name: "unproven legacy primary cannot cross completed activation", key: "primary", prepared: true, wantErr: true},
		{name: "existing active cluster drains old primary before protocol seal", key: "primary", prepared: true, legacyAllowed: true},
		{name: "first primary with durable timestamp", key: "primary", prepared: true, proofTS: 60},
		{name: "first primary timestamp differs from prepare", key: "primary", prepared: true, proofTS: 61, wantErr: true},
		{name: "unprepared primary", key: "primary", wantErr: true},
		{name: "secondary still requires decision", key: "secondary", prepared: true, wantErr: true},
		{name: "prepared primary cannot replace existing decision", key: "primary", prepared: true, found: true, decision: 61, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			txn := &forwardObserverTxn{}
			i := NewInternalWithEngine(txn, forwardObserverLeader{}, nil, nil,
				WithInternalTimestampAllocator(&phaseDForwardAllocator{floor: 100}),
				WithInternalPrimaryCommitEvidence(primaryCommitEvidenceFunc(func(context.Context, []byte, uint64) (kv.PrimaryCommitEvidence, error) {
					return kv.PrimaryCommitEvidence{Readable: true, Found: test.found, CommitTS: test.decision, PrimaryPrepared: test.prepared, PreparedCommitTS: test.proofTS, LegacyPreparedAllowed: test.legacyAllowed}, nil
				})))
			response, err := i.Forward(context.Background(), &pb.ForwardRequest{IsTxn: true, Requests: []*pb.Request{{
				Ts: 50, IsTxn: true, Phase: pb.Phase_COMMIT, Mutations: []*pb.Mutation{
					{Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("primary"), CommitTS: 60})},
					{Op: pb.Op_PUT, Key: []byte(test.key)},
				},
			}}})
			if test.wantErr {
				require.ErrorIs(t, err, kv.ErrPrePhaseDCommitUnproven)
				require.Empty(t, txn.reqs)
				return
			}
			require.NoError(t, err)
			require.True(t, response.Success)
			require.Equal(t, uint64(60), response.CommitTs)
			require.Len(t, txn.reqs, 1)
		})
	}
}
