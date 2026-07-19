package adapter

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/s3keys"
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

func TestStampRawTimestampsIgnoresRawRouteFloorForS3BucketAuxiliaryWrite(t *testing.T) {
	t.Parallel()

	const bucket = "bucket-a"
	key := s3keys.BucketMetaKey(bucket)
	auxStart := s3keys.RoutePrefixForBucketAnyGeneration(bucket)
	auxEnd := prefixScanEnd(auxStart)
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), End: auxStart, GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 2, Start: auxStart, End: auxEnd, GroupID: 2, State: distribution.RouteStateActive},
			{RouteID: 3, Start: auxEnd, End: nil, GroupID: 1, State: distribution.RouteStateActive, MinWriteTSExclusive: ^uint64(0)},
		},
	}))
	i := &Internal{routeEngine: engine}
	reqs := []*pb.Request{{
		Ts:        100,
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: key, Value: []byte("meta")}},
	}}

	require.NoError(t, i.stampRawTimestamps(context.Background(), reqs))
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
