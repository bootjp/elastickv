package adapter

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func derivedTwoGroupRPCManifest() *pb.TwoGroupPrepareManifest {
	manifest := twoGroupRPCManifest()
	primaryKey := []byte("home-key")
	h := sha256.New()
	_, _ = h.Write([]byte("elastickv/two-group-prepare/id/v1\x00"))
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(primaryKey)))
	_, _ = h.Write(size[:])
	_, _ = h.Write(primaryKey)
	binary.BigEndian.PutUint64(size[:], manifest.GetParticipants()[0].GetPrepare().GetTs())
	_, _ = h.Write(size[:])
	manifest.OperationId = append([]byte(nil), h.Sum(nil)[:16]...)
	return manifest
}

func twoGroupTerminalRPCMethod(group uint64) peersession.Method {
	return peersession.Method{Name: pb.Internal_ProposeTwoGroupTerminal_FullMethodName,
		Request: func(v any) (peersession.Message, error) {
			if _, ok := v.(*pb.ProposeTwoGroupTerminalRequest); !ok {
				return peersession.Message{}, peersession.ErrFormat
			}
			return peersession.Message{Group: group, From: 2, To: 1, Traffic: peersession.Control}, nil
		},
		Reply: func(req peersession.Message, v any) (peersession.Message, error) {
			if _, ok := v.(*pb.ProposeTwoGroupTerminalResponse); !ok {
				return peersession.Message{}, peersession.ErrFormat
			}
			req.From, req.To = req.To, req.From
			return req, nil
		}}
}

func twoGroupVerifyRPCMethod(group uint64) peersession.Method {
	return peersession.Method{Name: pb.Internal_VerifyTwoGroupPrepare_FullMethodName,
		Request: func(v any) (peersession.Message, error) {
			if _, ok := v.(*pb.VerifyTwoGroupPrepareRequest); !ok {
				return peersession.Message{}, peersession.ErrFormat
			}
			return peersession.Message{Group: group, From: 2, To: 1, Traffic: peersession.Control}, nil
		},
		Reply: func(req peersession.Message, v any) (peersession.Message, error) {
			if _, ok := v.(*pb.VerifyTwoGroupPrepareResponse); !ok {
				return peersession.Message{}, peersession.ErrFormat
			}
			req.From, req.To = req.To, req.From
			return req, nil
		}}
}

func preparedTwoGroupHomeStore(t *testing.T, manifest *pb.TwoGroupPrepareManifest) store.RaftControlMetadataStore {
	t.Helper()
	st, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	_, err = kv.ApplyTwoGroupHomePrepare(t.Context(), st, 2, 1, manifest, func(context.Context) error { return nil })
	require.NoError(t, err)
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	return metadata
}

type terminalTestHomeLookup struct {
	metadata store.RaftControlMetadataStore
}

func (l terminalTestHomeLookup) LookupTwoGroupHome(ctx context.Context, id [16]byte) ([]byte, uint64, uint64, error) {
	raw, found, err := kv.LookupTwoGroupHomeRecord(ctx, l.metadata, id)
	if err != nil || !found {
		return nil, 0, 0, kv.ErrTwoGroupAdmission
	}
	return raw, 2, 1, nil
}

func preparedTwoGroupSecondaryStore(t *testing.T, manifest *pb.TwoGroupPrepareManifest,
	home store.RaftControlMetadataStore) store.RaftControlMetadataStore {
	t.Helper()
	st, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	observed, err := kv.ObserveTwoGroupHome(t.Context(), terminalTestHomeLookup{metadata: home}, manifest)
	require.NoError(t, err)
	_, err = kv.ApplyTwoGroupSecondaryPrepare(t.Context(), st, 2, 2, manifest, observed,
		func(context.Context) error { return nil })
	require.NoError(t, err)
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	return metadata
}

func TestTwoGroupTerminalPrivateGRPCSessionAndFence(t *testing.T) {
	manifest := derivedTwoGroupRPCManifest()
	metadataStore := preparedTwoGroupHomeStore(t, manifest)
	leader := &prepareSafetyRPCLeader{}
	proposer := &twoGroupRPCProposer{}
	service := NewInternalWithEngine(nil, leader, nil, nil,
		WithInternalTwoGroupPrepareAdmission(1, "two-group-token", proposer, nil),
		WithInternalTwoGroupTerminalAdmission(metadataStore, nil))
	serverOwner, clientOwner, conn, serverNode := newTimestampNativePeer(t, 1, twoGroupTerminalRPCMethod(1), service)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer two-group-token")
	req := &pb.ProposeTwoGroupTerminalRequest{Manifest: manifest, Decision: pb.Phase_ABORT}
	_, err := pb.NewInternalClient(conn).ProposeTwoGroupTerminal(ctx, req)
	require.Error(t, err, "mTLS and bearer without a peer session are insufficient")
	require.Zero(t, proposer.calls.Load())
	handle, err := clientOwner.Handshake(ctx, conn, 1, serverNode)
	require.NoError(t, err)
	forged := metadata.AppendToOutgoingContext(ctx, "x-elastickv-peer-session", strings.Repeat("0", 64),
		"x-elastickv-peer-proof", strings.Repeat("0", 64))
	_, err = pb.NewInternalClient(conn).ProposeTwoGroupTerminal(forged, req)
	require.Error(t, err, "forged peer-session headers must not reuse the live session")
	require.Zero(t, proposer.calls.Load())
	peer := TwoGroupTerminalPeer{Handle: handle, Conn: conn, Token: "two-group-token"}
	require.Error(t, peer.DecideTwoGroupOnLeader(ctx, 2, manifest, pb.Phase_ABORT), "wrong group must not route")
	require.Zero(t, proposer.calls.Load())
	unknown := &pb.ProposeTwoGroupTerminalRequest{Manifest: manifest, Decision: pb.Phase_ABORT}
	unknown.ProtoReflect().SetUnknown([]byte{0x78, 0x01})
	response := new(pb.ProposeTwoGroupTerminalResponse)
	require.Error(t, handle.Invoke(ctx, conn, pb.Internal_ProposeTwoGroupTerminal_FullMethodName,
		unknown, response, grpc.PerRPCCredentials(migrationNativeBearer{token: "two-group-token"})))
	require.Zero(t, proposer.calls.Load(), "unknown protobuf fields cannot reach the proposer")
	require.NoError(t, peer.DecideTwoGroupOnLeader(ctx, 1, manifest, pb.Phase_ABORT))
	require.Equal(t, int32(1), proposer.calls.Load())
	require.Error(t, peer.DecideTwoGroupOnLeader(ctx, 1, manifest, pb.Phase_COMMIT), "COMMIT needs secondary PREPARE proof")
	require.Equal(t, int32(1), proposer.calls.Load())
	serverOwner.RevokeGroup(1)
	require.Error(t, peer.DecideTwoGroupOnLeader(ctx, 1, manifest, pb.Phase_ABORT))
	require.Equal(t, int32(1), proposer.calls.Load())
}

func TestTwoGroupTerminalPrivateGRPCRejectsLeaderTransfer(t *testing.T) {
	manifest := derivedTwoGroupRPCManifest()
	metadataStore := preparedTwoGroupHomeStore(t, manifest)
	leader := &prepareSafetyRPCLeader{advanceTermOnRead: true}
	proposer := &twoGroupRPCProposer{}
	service := NewInternalWithEngine(nil, leader, nil, nil,
		WithInternalTwoGroupPrepareAdmission(1, "two-group-token", proposer, nil),
		WithInternalTwoGroupTerminalAdmission(metadataStore, nil))
	_, clientOwner, conn, serverNode := newTimestampNativePeer(t, 1, twoGroupTerminalRPCMethod(1), service)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	handle, err := clientOwner.Handshake(ctx, conn, 1, serverNode)
	require.NoError(t, err)
	peer := TwoGroupTerminalPeer{Handle: handle, Conn: conn, Token: "two-group-token"}
	require.Error(t, peer.DecideTwoGroupOnLeader(ctx, 1, manifest, pb.Phase_ABORT))
	require.Zero(t, proposer.calls.Load())
}

func TestTwoGroupPrepareVerificationPrivateGRPC(t *testing.T) {
	manifest := derivedTwoGroupRPCManifest()
	metadataStore := preparedTwoGroupHomeStore(t, manifest)
	service := NewInternalWithEngine(nil, &prepareSafetyRPCLeader{}, nil, nil,
		WithInternalTwoGroupPrepareAdmission(1, "two-group-token", &twoGroupRPCProposer{}, nil),
		WithInternalTwoGroupTerminalAdmission(metadataStore, nil))
	_, clientOwner, conn, serverNode := newTimestampNativePeer(t, 1, twoGroupVerifyRPCMethod(1), service)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	handle, err := clientOwner.Handshake(ctx, conn, 1, serverNode)
	require.NoError(t, err)
	peer := TwoGroupTerminalPeer{Handle: handle, Conn: conn, Token: "two-group-token"}
	require.NoError(t, peer.VerifyTwoGroupPrepareOnLeader(ctx, 1, manifest))
	require.Error(t, peer.VerifyTwoGroupPrepareOnLeader(ctx, 2, manifest))
	changed := derivedTwoGroupRPCManifest()
	changed.Participants[0].Prepare.Mutations[1].Value = []byte("different")
	require.Error(t, peer.VerifyTwoGroupPrepareOnLeader(ctx, 1, changed), "proof must bind the complete manifest")
}

func TestTwoGroupRemoteCommitGRPCVerifiesSecondaryFirst(t *testing.T) {
	manifest := derivedTwoGroupRPCManifest()
	home := preparedTwoGroupHomeStore(t, manifest)
	secondary := preparedTwoGroupSecondaryStore(t, manifest, home)
	secondaryService := NewInternalWithEngine(nil, &prepareSafetyRPCLeader{}, nil, nil,
		WithInternalTwoGroupPrepareAdmission(2, "two-group-token", &twoGroupRPCProposer{}, nil),
		WithInternalTwoGroupTerminalAdmission(secondary, nil))
	_, secondaryOwner, secondaryConn, secondaryNode := newTimestampNativePeer(t, 2, twoGroupVerifyRPCMethod(2), secondaryService)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	secondaryHandle, err := secondaryOwner.Handshake(ctx, secondaryConn, 2, secondaryNode)
	require.NoError(t, err)
	secondaryPeer := &TwoGroupTerminalPeer{Handle: secondaryHandle, Conn: secondaryConn, Token: "two-group-token"}
	proposer := &twoGroupRPCProposer{}
	primaryService := NewInternalWithEngine(nil, &prepareSafetyRPCLeader{}, nil, nil,
		WithInternalTwoGroupPrepareAdmission(1, "two-group-token", proposer, nil),
		WithInternalTwoGroupTerminalAdmission(home, func(group uint64) (*TwoGroupTerminalPeer, error) {
			if group != 2 {
				return nil, kv.ErrTwoGroupAdmission
			}
			return secondaryPeer, nil
		}))
	_, primaryOwner, primaryConn, primaryNode := newTimestampNativePeer(t, 1, twoGroupTerminalRPCMethod(1), primaryService)
	primaryHandle, err := primaryOwner.Handshake(ctx, primaryConn, 1, primaryNode)
	require.NoError(t, err)
	primaryPeer := TwoGroupTerminalPeer{Handle: primaryHandle, Conn: primaryConn, Token: "two-group-token"}
	require.NoError(t, primaryPeer.DecideTwoGroupOnLeader(ctx, 1, manifest, pb.Phase_COMMIT))
	require.Equal(t, int32(1), proposer.calls.Load())
	secondaryOwner.RevokeGroup(2)
	require.Error(t, primaryPeer.DecideTwoGroupOnLeader(ctx, 1, manifest, pb.Phase_COMMIT))
	require.Equal(t, int32(1), proposer.calls.Load(), "lost secondary proof must not produce another primary proposal")
}

func TestTwoGroupHomeDecisionPrivateLookupObservesAppliedRecord(t *testing.T) {
	manifest := derivedTwoGroupRPCManifest()
	st := store.NewMVCCStore()
	_, err := kv.ApplyTwoGroupHomePrepare(t.Context(), st, 2, 1, manifest, func(context.Context) error { return nil })
	require.NoError(t, err)
	metadataStore, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	applier, ok := st.(store.RaftEntryApplier)
	require.True(t, ok)
	leader := &prepareSafetyRPCLeader{}
	service := NewInternalWithEngine(nil, leader, nil, nil,
		WithInternalStore(st), WithInternalTimestampAdmission("two-group-token", nil))
	_, clientOwner, conn, serverNode := newTimestampNativePeer(t, 1, lookupTimestampPeerMethod(1), service)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	handle, err := clientOwner.Handshake(ctx, conn, 1, serverNode)
	require.NoError(t, err)
	lookup := TwoGroupHomePeerLookup{Handle: handle, Conn: conn, Token: "two-group-token"}
	var id [16]byte
	copy(id[:], manifest.GetOperationId())
	_, _, _, err = lookup.LookupTwoGroupDecision(ctx, id)
	require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
	record := &pb.TwoGroupTerminalRecord{Version: 1, Manifest: manifest, Decision: pb.Phase_ABORT,
		ResolutionTimestamp: manifest.GetPersistenceTimestamp(), AppliedIndex: 3, AppliedTerm: 1}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(record)
	require.NoError(t, err)
	require.NoError(t, applier.ApplyRaftEntry(t.Context(), 3, func(applyCtx context.Context) error {
		key := append([]byte("timestamp-binding/two-group-terminal/"), id[:]...)
		return metadataStore.StageRaftControlMetadata(applyCtx, key, raw)
	}))
	observed, index, term, err := lookup.LookupTwoGroupDecision(ctx, id)
	require.NoError(t, err)
	require.Equal(t, raw, observed)
	require.GreaterOrEqual(t, index, uint64(3))
	require.NotZero(t, term)
	leader.advanceTermOnRead = true
	_, _, _, err = lookup.LookupTwoGroupDecision(ctx, id)
	require.Error(t, err, "term transfer must invalidate the observation")
}
