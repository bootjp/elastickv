package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestFSMPreparedCommitDrainsMigrationFences(t *testing.T) {
	t.Parallel()
	for _, secondary := range []bool{false, true} {
		for _, fence := range []string{"catalog", "durable", "both"} {
			t.Run(fence+map[bool]string{false: "/primary", true: "/secondary"}[secondary], func(t *testing.T) {
				t.Parallel()
				ctx := context.Background()
				engine := distribution.NewEngine()
				route := distribution.RouteDescriptor{RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 1}
				applyComposed1Snapshot(t, engine, 1, []distribution.RouteDescriptor{route})
				fsm := newComposed1FSM(t, engine, 1)
				primary := []byte("n")
				if secondary {
					primary = []byte("b")
				}
				meta := &pb.Mutation{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{
					PrimaryKey: primary, CommitTS: 60, LockTTLms: defaultTxnLockTTLms,
				})}
				mutation := &pb.Mutation{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("prepared")}
				require.NoError(t, fsm.handleTxnRequest(ctx, &pb.Request{IsTxn: true, Phase: pb.Phase_PREPARE, Ts: 40,
					Mutations: []*pb.Mutation{meta, mutation}}, 40))
				if fence != "durable" {
					route.State = distribution.RouteStateWriteFenced
					applyComposed1Snapshot(t, engine, 2, []distribution.RouteDescriptor{route})
				}
				applyTargetReadinessToFSM(t, fsm, store.TargetStagedReadinessState{
					JobID: 11, MigrationJobID: 11, RouteStart: []byte("m"), RouteEnd: []byte("z"),
					Armed: true, SourceWriteFence: fence != "catalog", TrackWrites: true,
					MinWriteTSExclusive: 1, RetentionPinTS: 1,
				})
				// Lock resolution supplies neither a pinned route version nor bypass keys,
				// and must apply the stored intent, not the caller's mutation value.
				request := &pb.Request{IsTxn: true, Phase: pb.Phase_COMMIT, Ts: 40,
					Mutations: []*pb.Mutation{meta, {Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("untrusted")}}}
				require.NoError(t, fsm.handleTxnRequest(ctx, request, 60))
				value, err := fsm.store.GetAt(ctx, []byte("n"), 60)
				require.NoError(t, err)
				require.Equal(t, []byte("prepared"), value)
				_, exists, err := fsm.txnLockAtKey(ctx, txnLockKey([]byte("n")))
				require.NoError(t, err)
				require.False(t, exists)
				require.Equal(t, uint64(60), migrationTrackerMinimum(t, fsm))
				require.NoError(t, fsm.handleTxnRequest(ctx, request, 60), "resolved retries remain idempotent")
			})
		}
	}
}

func TestFSMFencedCommitAuthenticatesLiveLock(t *testing.T) {
	t.Parallel()
	for _, mismatch := range []string{"start", "primary", "missing"} {
		t.Run(mismatch, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			fsm := newMigrationWriteTrackerFSM(t)
			meta := &pb.Mutation{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{
				PrimaryKey: []byte("n"), CommitTS: 60, LockTTLms: defaultTxnLockTTLms,
			})}
			mutation := &pb.Mutation{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("prepared")}
			if mismatch != "missing" {
				require.NoError(t, fsm.handleTxnRequest(ctx, &pb.Request{IsTxn: true, Phase: pb.Phase_PREPARE, Ts: 40,
					Mutations: []*pb.Mutation{meta, mutation}}, 40))
			}
			applySourceMigrationControlToFSM(t, fsm, false)
			request := &pb.Request{IsTxn: true, Phase: pb.Phase_COMMIT, Ts: 40, Mutations: []*pb.Mutation{meta, mutation}}
			if mismatch == "start" {
				request.Ts = 41
			}
			if mismatch == "primary" {
				request.Mutations[0] = &pb.Mutation{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix),
					Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("b"), CommitTS: 60})}
			}
			require.Error(t, fsm.handleTxnRequest(ctx, request, 60))
			_, err := fsm.store.GetAt(ctx, []byte("n"), 60)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		})
	}
}
