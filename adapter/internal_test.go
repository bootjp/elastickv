package adapter

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestStampTxnTimestamps_RejectsMaxStartTS(t *testing.T) {
	t.Parallel()

	i := &Internal{}
	reqs := []*pb.Request{
		{
			IsTxn: true,
			Phase: pb.Phase_COMMIT,
			Ts:    ^uint64(0),
			Mutations: []*pb.Mutation{
				{
					Op:    pb.Op_PUT,
					Key:   []byte(kv.TxnMetaPrefix),
					Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("k"), CommitTS: 0}),
				},
			},
		},
	}

	_, err := i.stampTxnTimestamps(context.Background(), reqs)
	require.ErrorIs(t, err, ErrTxnTimestampOverflow)
}

func TestFillForwardedTxnCommitTS_RejectsOverflow(t *testing.T) {
	t.Parallel()

	i := &Internal{}
	reqs := []*pb.Request{
		{
			IsTxn: true,
			Phase: pb.Phase_COMMIT,
			Mutations: []*pb.Mutation{
				{
					Op:    pb.Op_PUT,
					Key:   []byte(kv.TxnMetaPrefix),
					Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("k"), CommitTS: 0}),
				},
			},
		},
	}

	_, err := i.fillForwardedTxnCommitTS(context.Background(), reqs, ^uint64(0))
	require.ErrorIs(t, err, ErrTxnTimestampOverflow)
}

func TestFillForwardedTxnCommitTS_AssignsCommitTS(t *testing.T) {
	t.Parallel()

	i := &Internal{}
	startTS := uint64(10)
	reqs := []*pb.Request{
		{
			IsTxn: true,
			Phase: pb.Phase_COMMIT,
			Mutations: []*pb.Mutation{
				{
					Op:    pb.Op_PUT,
					Key:   []byte(kv.TxnMetaPrefix),
					Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("k"), CommitTS: 0}),
				},
			},
		},
	}

	commitTS, err := i.fillForwardedTxnCommitTS(context.Background(), reqs, startTS)
	require.NoError(t, err)

	meta, err := kv.DecodeTxnMeta(reqs[0].Mutations[0].Value)
	require.NoError(t, err)
	require.Equal(t, startTS+1, meta.CommitTS)
	require.Equal(t, meta.CommitTS, commitTS)
}

func TestFillForwardedTxnCommitTS_PreservesExistingCommitTS(t *testing.T) {
	t.Parallel()

	i := &Internal{}
	reqs := []*pb.Request{
		{
			IsTxn: true,
			Phase: pb.Phase_COMMIT,
			Mutations: []*pb.Mutation{
				{
					Op:    pb.Op_PUT,
					Key:   []byte(kv.TxnMetaPrefix),
					Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("k"), CommitTS: 42}),
				},
			},
		},
	}

	commitTS, err := i.fillForwardedTxnCommitTS(context.Background(), reqs, 10)
	require.NoError(t, err)
	meta, err := kv.DecodeTxnMeta(reqs[0].Mutations[0].Value)
	require.NoError(t, err)
	require.Equal(t, uint64(42), meta.CommitTS)
	require.Equal(t, meta.CommitTS, commitTS)
}

func TestFillForwardedTxnCommitTS_AssignsCommitTSForOnePhaseTxn(t *testing.T) {
	t.Parallel()

	i := &Internal{}
	startTS := uint64(10)
	reqs := []*pb.Request{
		{
			IsTxn: true,
			Phase: pb.Phase_NONE,
			Mutations: []*pb.Mutation{
				{
					Op:    pb.Op_PUT,
					Key:   []byte(kv.TxnMetaPrefix),
					Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("k"), CommitTS: 0}),
				},
				{
					Op:    pb.Op_PUT,
					Key:   []byte("k"),
					Value: []byte("v"),
				},
			},
		},
	}

	commitTS, err := i.fillForwardedTxnCommitTS(context.Background(), reqs, startTS)
	require.NoError(t, err)

	meta, err := kv.DecodeTxnMeta(reqs[0].Mutations[0].Value)
	require.NoError(t, err)
	require.Equal(t, startTS+1, meta.CommitTS)
	require.Equal(t, meta.CommitTS, commitTS)
}

func TestFillForwardedTxnCommitTS_StampsCommitTSValueOffset(t *testing.T) {
	t.Parallel()

	i := &Internal{}
	startTS := uint64(10)
	value := make([]byte, 16)
	reqs := []*pb.Request{
		{
			IsTxn: true,
			Phase: pb.Phase_NONE,
			Mutations: []*pb.Mutation{
				{
					Op:    pb.Op_PUT,
					Key:   []byte(kv.TxnMetaPrefix),
					Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("k"), CommitTS: 0}),
				},
				{
					Op:                  pb.Op_PUT,
					Key:                 []byte("k"),
					Value:               value,
					CommitTsValueOffset: 4,
				},
			},
		},
	}

	commitTS, err := i.fillForwardedTxnCommitTS(context.Background(), reqs, startTS)
	require.NoError(t, err)
	require.Equal(t, startTS+1, commitTS)
	require.Equal(t, commitTS, binary.BigEndian.Uint64(value[4:12]))
	require.Zero(t, reqs[0].Mutations[1].CommitTsValueOffset)
}

func TestFillForwardedTxnCommitTS_PrepareAllowsAlreadyStampedOffsets(t *testing.T) {
	t.Parallel()

	i := &Internal{}
	value := make([]byte, 16)
	mut := &pb.Mutation{
		Op:                  pb.Op_PUT,
		Key:                 []byte("k"),
		Value:               value,
		CommitTsValueOffset: 4,
	}
	require.NoError(t, kv.StampMutationCommitTS([]*pb.Mutation{mut}, 42))

	reqs := []*pb.Request{
		{
			IsTxn:     true,
			Phase:     pb.Phase_PREPARE,
			Ts:        10,
			Mutations: []*pb.Mutation{mut},
		},
	}

	commitTS, err := i.fillForwardedTxnCommitTS(context.Background(), reqs, 10)
	require.NoError(t, err)
	require.Zero(t, commitTS)
	require.Equal(t, uint64(42), binary.BigEndian.Uint64(value[4:12]))
	require.Zero(t, mut.CommitTsValueOffset)
}

func TestStampTxnTimestamps_UsesSingleTxnStartTS(t *testing.T) {
	t.Parallel()

	i := &Internal{}
	prepare := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_PREPARE,
		Ts:    0,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")},
		},
	}
	commit := &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_COMMIT,
		Ts:    9,
		Mutations: []*pb.Mutation{
			{
				Op:    pb.Op_PUT,
				Key:   []byte(kv.TxnMetaPrefix),
				Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("k"), CommitTS: 0}),
			},
			{Op: pb.Op_PUT, Key: []byte("k")},
		},
	}
	reqs := []*pb.Request{prepare, commit}

	commitTS, err := i.stampTxnTimestamps(context.Background(), reqs)
	require.NoError(t, err)
	require.Equal(t, uint64(9), prepare.Ts)
	require.Equal(t, uint64(9), commit.Ts)

	meta, err := kv.DecodeTxnMeta(commit.Mutations[0].Value)
	require.NoError(t, err)
	require.Greater(t, meta.CommitTS, uint64(9))
	require.Equal(t, meta.CommitTS, commitTS)
}

// recordingWriteGate stands in for the sharded coordinator's route-floor check.
type recordingWriteGate struct {
	floor     uint64
	calls     int
	lastTS    uint64
	lastMuts  int
	rejectErr error
}

func (g *recordingWriteGate) EnsureMutationsWriteAllowed(muts []*pb.Mutation, commitTS uint64) error {
	g.calls++
	g.lastTS = commitTS
	g.lastMuts = len(muts)
	if commitTS != 0 && commitTS <= g.floor {
		return g.rejectErr
	}
	return nil
}

// A follower cannot stamp a raw write (its stamping path bails out when the
// group engine is not leader), so the leader assigns the timestamp here. Before
// the gate existed, that write went to Raft with no route-floor check at all.
func TestStampRawTimestamps_AppliesRouteFloorToForwardedWrites(t *testing.T) {
	t.Parallel()

	rejected := errors.New("route min_write_ts_exclusive rejects commit_ts")

	tests := []struct {
		name      string
		floor     uint64
		requestTS uint64
		wantErr   bool
	}{
		{
			name:      "unstamped write above the floor is admitted",
			floor:     0,
			requestTS: 0,
			wantErr:   false,
		},
		{
			// Any timestamp the leader can mint is at or below this floor.
			name:      "unstamped write at or below the floor is rejected",
			floor:     ^uint64(0) - 1,
			requestTS: 0,
			wantErr:   true,
		},
		{
			name:      "pre-stamped write below the floor is rejected too",
			floor:     100,
			requestTS: 42,
			wantErr:   true,
		},
		{
			name:      "pre-stamped write above the floor is admitted",
			floor:     5,
			requestTS: 42,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gate := &recordingWriteGate{floor: tt.floor, rejectErr: rejected}
			i := &Internal{clock: kv.NewHLC(), writeGate: gate}
			reqs := []*pb.Request{{
				IsTxn: false,
				Phase: pb.Phase_NONE,
				Ts:    tt.requestTS,
				Mutations: []*pb.Mutation{
					{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")},
				},
			}}

			err := i.stampRawTimestamps(context.Background(), reqs)
			if tt.wantErr {
				require.ErrorIs(t, err, rejected)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, gate.calls)
			require.Equal(t, 1, gate.lastMuts)
			require.NotZero(t, gate.lastTS, "the gate must see the stamped timestamp, not zero")
			if tt.requestTS != 0 {
				require.Equal(t, tt.requestTS, gate.lastTS)
			}
		})
	}
}

// Deployments without a route table (the single-group coordinator) leave the
// gate unset; stamping must keep working there.
func TestStampRawTimestamps_WithoutWriteGate(t *testing.T) {
	t.Parallel()

	i := &Internal{}
	reqs := []*pb.Request{{
		IsTxn:     false,
		Phase:     pb.Phase_NONE,
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")}},
	}}

	require.NoError(t, i.stampRawTimestamps(context.Background(), reqs))
	require.NotZero(t, reqs[0].Ts)
}
