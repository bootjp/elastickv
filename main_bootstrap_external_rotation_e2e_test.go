package main

import (
	"bytes"
	"context"
	"net"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const externalRotationE2EAdmin = "external-rotation-e2e-admin"

type externalRotationE2EController interface {
	RotateDEK(context.Context, *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error)
}

// Only the transport may withhold a reply. Every mutation still passes the
// actual serialized process controller, owned Rot grant and native FSM.
func externalRotationE2EClient(t *testing.T, f *bootstrapRotationE2EFixture, node *bootstrapRotationE2ENode, after func(context.Context, *pb.RotateDEKRequest)) pb.EncryptionAdminClient {
	t.Helper()
	controller, ok := any(node.supervisor).(externalRotationE2EController)
	require.True(t, ok, "production rotation supervisor must implement the external controller API")
	rpc, err := newBootstrapRotationRPC(externalRotationE2EAdmin, f.config.local.Member, func(ctx context.Context, req *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
		reply, callErr := controller.RotateDEK(ctx, req)
		if after != nil {
			after(ctx, req)
		}
		return reply, callErr
	})
	require.NoError(t, err)
	options, err := node.domain.owners[1].ServerOptions(f.config.serverTLS)
	require.NoError(t, err)
	gate := &startupPublicKVGate{domain: node.domain}
	gate.markReady()
	server := grpc.NewServer(append(options, grpc.ChainUnaryInterceptor(rpc, gate.unaryInterceptor))...)
	admin := adapter.NewEncryptionAdminServer(adapter.WithEncryptionAdminSidecarPath(f.sidecarPath),
		adapter.WithEncryptionAdminFullNodeID(f.config.local.Member), adapter.WithEncryptionAdminProposer(node.runtime.Engine),
		adapter.WithEncryptionAdminPostCutoverProposer(node.group.Proposer()), adapter.WithEncryptionAdminLeaderView(node.runtime.Engine))
	require.NoError(t, admin.Validate())
	pb.RegisterEncryptionAdminServer(server, admin)
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); require.NoError(t, <-done) })
	tls := f.config.clientTLS.Clone()
	tls.ServerName = "localhost"
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(tls)))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return pb.NewEncryptionAdminClient(conn)
}

func externalRotationE2EContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), enablementE2EWait)
	t.Cleanup(cancel)
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+externalRotationE2EAdmin)
}

func externalRotationE2ERequest(t *testing.T, f *bootstrapRotationE2EFixture, node *bootstrapRotationE2ENode) *pb.RotateDEKRequest {
	t.Helper()
	sc, err := encryption.ReadSidecar(f.sidecarPath)
	require.NoError(t, err)
	id, err := nextEncryptionRotateOnStartupKeyID(sc)
	require.NoError(t, err)
	wrapped, err := wrapFreshStartupDEK(f.wrapper)
	require.NoError(t, err)
	return &pb.RotateDEKRequest{Purpose: pb.RotateDEKRequest_PURPOSE_STORAGE, NewDekId: id, WrappedNewDek: wrapped,
		ProposerNodeId: f.config.local.Member, ProposerLocalEpoch: uint32(node.wiring.epoch)}
}

func externalRotationE2EApplied(t *testing.T, f *bootstrapRotationE2EFixture, node *bootstrapRotationE2ENode, req *pb.RotateDEKRequest) {
	t.Helper()
	require.Eventually(t, func() bool {
		sc, err := encryption.ReadSidecar(f.sidecarPath)
		if err != nil || sc.Active.Storage != req.NewDekId {
			return false
		}
		key := sc.Keys[strconv.FormatUint(uint64(req.NewDekId), 10)]
		return key.Purpose == encryption.SidecarPurposeStorage && bytes.Equal(key.Wrapped, req.WrappedNewDek) && uint32(key.LocalEpoch) == req.ProposerLocalEpoch
	}, enablementE2EWait, 10*time.Millisecond, "actual committed sidecar must match the exact external request")
	require.Eventually(t, node.runtime.PublicReady, enablementE2EWait, 10*time.Millisecond, "read-only recovery must settle responsibility and restore DataReady")
}

func TestBootstrapExternalRotationE2EEncryptedBusinessRestart(t *testing.T) {
	t.Setenv("ELASTICKV_RAFT_SNAPSHOT_COUNT", "1")
	f := newBootstrapRotationE2EFixture(t, true)
	node := f.protected(t)
	client := externalRotationE2EClient(t, f, node, nil)
	req := externalRotationE2ERequest(t, f, node)
	reply, err := client.RotateDEK(externalRotationE2EContext(t), req)
	if err != nil {
		require.Equal(t, codes.Unknown, status.Code(err), "a committed self-revoking key apply must report an ambiguous result: %v", err)
	} else {
		require.NotZero(t, reply.GetAppliedIndex())
	}
	externalRotationE2EApplied(t, f, node, req)
	// A client retry of the SAME request reconciles exact committed bytes.
	reply, err = client.RotateDEK(externalRotationE2EContext(t), req)
	require.NoError(t, err)
	require.NotZero(t, reply.GetAppliedIndex())
	_, err = node.coordinate.Dispatch(externalRotationE2EContext(t), &kv.OperationGroup[kv.OP]{Elems: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("external-rotated-business"), Value: []byte("encrypted-external-durable")}}})
	require.NoError(t, err)
	node.read(t, "external-rotated-business", "encrypted-external-durable")
	node.close()
	plain, err := store.NewPebbleStore(filepath.Join(f.dir, "fsm.db"))
	require.NoError(t, err)
	_, err = plain.GetAt(t.Context(), []byte("external-rotated-business"), ^uint64(0))
	require.ErrorContains(t, err, "no cipher configured")
	require.NoError(t, plain.Close())
	restarted := f.protected(t)
	restarted.read(t, "external-rotated-business", "encrypted-external-durable")
	restarted.read(t, "legacy-key", "original-value")
}

func TestBootstrapExternalRotationE2ERejectedRequestsDoNotMutate(t *testing.T) {
	t.Setenv("ELASTICKV_RAFT_SNAPSHOT_COUNT", "1")
	f := newBootstrapRotationE2EFixture(t, true)
	node := f.protected(t)
	client := externalRotationE2EClient(t, f, node, nil)
	req := externalRotationE2ERequest(t, f, node)
	before, err := encryption.ReadSidecar(f.sidecarPath)
	require.NoError(t, err)
	for _, tc := range []struct {
		name   string
		change func(*pb.RotateDEKRequest)
	}{
		{"unknown", func(r *pb.RotateDEKRequest) { r.ProtoReflect().SetUnknown([]byte{0x38, 1}) }},
		{"oversized", func(r *pb.RotateDEKRequest) { r.WrappedNewDek = make([]byte, 4097) }},
		{"foreign_identity", func(r *pb.RotateDEKRequest) { r.ProposerNodeId++ }},
		{"wrong_epoch", func(r *pb.RotateDEKRequest) { r.ProposerLocalEpoch++ }},
		{"key_collision", func(r *pb.RotateDEKRequest) { r.NewDekId = before.Active.Storage }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := cloneBootstrapRotationRequest(req)
			tc.change(bad)
			_, err := client.RotateDEK(externalRotationE2EContext(t), bad)
			require.Error(t, err)
		})
	}
	_, err = client.RotateDEK(t.Context(), req)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	ctx := metadata.AppendToOutgoingContext(t.Context(), "authorization", "Bearer "+externalRotationE2EAdmin, bootstrapControlHeader, f.config.startupToken)
	_, err = client.RotateDEK(ctx, req)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	after, err := encryption.ReadSidecar(f.sidecarPath)
	require.NoError(t, err)
	require.Equal(t, before.Active, after.Active)
	require.Equal(t, before.Keys, after.Keys, "rejected network requests cannot install any key material")
	require.True(t, node.runtime.PublicReady())
}

func TestBootstrapExternalRotationE2ELostReplyReconcilesExactRequest(t *testing.T) {
	t.Setenv("ELASTICKV_RAFT_SNAPSHOT_COUNT", "1")
	f := newBootstrapRotationE2EFixture(t, true)
	node := f.protected(t)
	completed := make(chan struct{})
	client := externalRotationE2EClient(t, f, node, func(ctx context.Context, _ *pb.RotateDEKRequest) {
		close(completed)
		<-ctx.Done()
	})
	req := externalRotationE2ERequest(t, f, node)
	ctx, cancel := context.WithCancel(externalRotationE2EContext(t))
	defer cancel()
	returned := make(chan error, 1)
	go func() { _, err := client.RotateDEK(ctx, req); returned <- err }()
	select {
	case <-completed:
	case <-time.After(enablementE2EWait):
		t.Fatal("actual external controller did not return")
	}
	cancel()
	require.Equal(t, codes.Canceled, status.Code(<-returned))
	externalRotationE2EApplied(t, f, node, req)
	node.read(t, "legacy-key", "original-value")
}

func TestBootstrapExternalRotationE2ERejectsReplayedSidecarMaterial(t *testing.T) {
	t.Setenv("ELASTICKV_RAFT_SNAPSHOT_COUNT", "1")
	f := newBootstrapRotationE2EFixture(t, true)
	node := f.protected(t)
	client := externalRotationE2EClient(t, f, node, nil)
	report, err := client.GetSidecarState(externalRotationE2EContext(t), &pb.Empty{})
	require.NoError(t, err, "the attack obtains existing wrapped DEKs through the real public RPC")
	before, err := encryption.ReadSidecar(f.sidecarPath)
	require.NoError(t, err)
	for _, source := range []uint32{report.ActiveStorageId, report.ActiveRaftId, e2eStorageDEKID, e2eRaftDEKID} {
		wrapped := report.WrappedDeksById[source]
		require.NotEmpty(t, wrapped, "active AND historical wrapped keys remain exposed by the sidecar report")
		for _, purpose := range []pb.RotateDEKRequest_Purpose{pb.RotateDEKRequest_PURPOSE_STORAGE, pb.RotateDEKRequest_PURPOSE_RAFT} {
			for _, rewrapped := range []bool{false, true} {
				t.Run(strconv.FormatUint(uint64(source), 10)+"/"+purpose.String()+"/rewrapped="+strconv.FormatBool(rewrapped), func(t *testing.T) {
					req := externalRotationE2ERequest(t, f, node)
					req.Purpose, req.WrappedNewDek = purpose, bytes.Clone(wrapped)
					if purpose == pb.RotateDEKRequest_PURPOSE_RAFT {
						req.ProposerLocalEpoch = uint32(node.wiring.raftEpoch)
					}
					if rewrapped {
						material, err := f.wrapper.Unwrap(wrapped)
						require.NoError(t, err)
						req.WrappedNewDek, err = f.wrapper.Wrap(material)
						zeroBytes(material)
						require.NoError(t, err)
						require.NotEqual(t, wrapped, req.WrappedNewDek)
					}
					_, err := client.RotateDEK(externalRotationE2EContext(t), req)
					require.Error(t, err)
					require.Equal(t, codes.InvalidArgument, status.Code(err), "rewrapping or changing ID/purpose cannot create a fresh nonce domain: %v", err)
				})
			}
		}
	}
	after, err := encryption.ReadSidecar(f.sidecarPath)
	require.NoError(t, err)
	require.Equal(t, before.Active, after.Active)
	require.Equal(t, before.Keys, after.Keys)
	state, err := node.domain.admission.RotationStatus()
	require.NoError(t, err)
	require.False(t, state.Responsibility, "reused material must be rejected before private Rot authority is installed")
	node.read(t, "legacy-key", "original-value")
}
