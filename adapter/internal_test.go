package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

type fixedInternalTimestampAllocator uint64

func (a fixedInternalTimestampAllocator) Next(context.Context) (uint64, error) {
	return uint64(a), nil
}

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

	err := i.stampTxnTimestamps(context.Background(), reqs)
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

	err := i.fillForwardedTxnCommitTS(context.Background(), reqs, ^uint64(0))
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

	require.NoError(t, i.fillForwardedTxnCommitTS(context.Background(), reqs, startTS))

	meta, err := kv.DecodeTxnMeta(reqs[0].Mutations[0].Value)
	require.NoError(t, err)
	require.Equal(t, startTS+1, meta.CommitTS)
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

	require.NoError(t, i.fillForwardedTxnCommitTS(context.Background(), reqs, 10))
	meta, err := kv.DecodeTxnMeta(reqs[0].Mutations[0].Value)
	require.NoError(t, err)
	require.Equal(t, uint64(42), meta.CommitTS)
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

	require.NoError(t, i.fillForwardedTxnCommitTS(context.Background(), reqs, startTS))

	meta, err := kv.DecodeTxnMeta(reqs[0].Mutations[0].Value)
	require.NoError(t, err)
	require.Equal(t, startTS+1, meta.CommitTS)
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

	require.NoError(t, i.stampTxnTimestamps(context.Background(), reqs))
	require.Equal(t, uint64(9), prepare.Ts)
	require.Equal(t, uint64(9), commit.Ts)

	meta, err := kv.DecodeTxnMeta(commit.Mutations[0].Value)
	require.NoError(t, err)
	require.Greater(t, meta.CommitTS, uint64(9))
}

func TestStampRawTimestampsRejectsRouteWriteFloor(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{{
			RouteID:             1,
			Start:               []byte(""),
			End:                 nil,
			GroupID:             1,
			State:               distribution.RouteStateActive,
			MinWriteTSExclusive: 100,
		}},
	}))
	i := &Internal{
		tsAllocator: fixedInternalTimestampAllocator(100),
		routeEngine: engine,
	}
	reqs := []*pb.Request{{
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")}},
	}}

	err := i.stampRawTimestamps(context.Background(), reqs)
	require.ErrorIs(t, err, kv.ErrRouteWriteTimestampTooLow)
}

func TestStampRawTimestampsRejectsPreStampedRouteWriteFloor(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{{
			RouteID:             1,
			Start:               []byte(""),
			End:                 nil,
			GroupID:             1,
			State:               distribution.RouteStateActive,
			MinWriteTSExclusive: 100,
		}},
	}))
	i := &Internal{routeEngine: engine}
	reqs := []*pb.Request{{
		Ts:        100,
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")}},
	}}

	err := i.stampRawTimestamps(context.Background(), reqs)
	require.ErrorIs(t, err, kv.ErrRouteWriteTimestampTooLow)
}

func TestStampRawTimestampsRejectsPreStampedDelPrefixRouteWriteFloor(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{
				RouteID:             1,
				Start:               []byte(""),
				End:                 []byte("m"),
				GroupID:             1,
				State:               distribution.RouteStateActive,
				MinWriteTSExclusive: 0,
			},
			{
				RouteID:             2,
				Start:               []byte("m"),
				End:                 nil,
				GroupID:             2,
				State:               distribution.RouteStateActive,
				MinWriteTSExclusive: 100,
			},
		},
	}))
	i := &Internal{routeEngine: engine}
	reqs := []*pb.Request{{
		Ts:        100,
		Mutations: []*pb.Mutation{{Op: pb.Op_DEL_PREFIX, Key: nil}},
	}}

	err := i.stampRawTimestamps(context.Background(), reqs)
	require.ErrorIs(t, err, kv.ErrRouteWriteTimestampTooLow)
}

func TestStampTxnTimestampsRejectsRouteWriteFloor(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{{
			RouteID:             1,
			Start:               []byte(""),
			End:                 nil,
			GroupID:             1,
			State:               distribution.RouteStateActive,
			MinWriteTSExclusive: 100,
		}},
	}))
	i := &Internal{routeEngine: engine}
	reqs := []*pb.Request{{
		IsTxn: true,
		Phase: pb.Phase_NONE,
		Ts:    50,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("k"), CommitTS: 100})},
			{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")},
		},
	}}

	err := i.stampTxnTimestamps(context.Background(), reqs)
	require.ErrorIs(t, err, kv.ErrRouteWriteTimestampTooLow)
}

func TestStampTxnTimestampsIgnoresMetadataForRouteWriteFloor(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{
				RouteID:             1,
				Start:               []byte(""),
				End:                 []byte("m"),
				GroupID:             1,
				State:               distribution.RouteStateActive,
				MinWriteTSExclusive: 100,
			},
			{
				RouteID: 2,
				Start:   []byte("m"),
				End:     nil,
				GroupID: 2,
				State:   distribution.RouteStateActive,
			},
		},
	}))
	i := &Internal{routeEngine: engine}
	reqs := []*pb.Request{{
		IsTxn: true,
		Phase: pb.Phase_NONE,
		Ts:    50,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("z"), CommitTS: 100})},
			{Op: pb.Op_PUT, Key: []byte("z"), Value: []byte("v")},
		},
	}}

	require.NoError(t, i.stampTxnTimestamps(context.Background(), reqs))
}
