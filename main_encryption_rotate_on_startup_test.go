package main

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	pb "github.com/bootjp/elastickv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStartupPublicKVGate_BlocksMutatorsUntilReady(t *testing.T) {
	t.Parallel()
	gate := &startupPublicKVGate{}

	blockedMethods := []string{
		pb.RawKV_RawGet_FullMethodName,
		pb.TransactionalKV_Get_FullMethodName,
		pb.Internal_Forward_FullMethodName,
		pb.Internal_ApplyTargetStagedReadiness_FullMethodName,
		pb.Internal_ImportRangeVersions_FullMethodName,
		pb.Internal_PromoteStagedVersions_FullMethodName,
		pb.Internal_CleanupMigration_FullMethodName,
		pb.Internal_IssueMigrationTimestamp_FullMethodName,
		pb.AdminForward_Forward_FullMethodName,
		pb.Distribution_GetTimestamp_FullMethodName,
		pb.Distribution_SplitRange_FullMethodName,
		pb.Distribution_StartSplitMigration_FullMethodName,
		pb.Distribution_GetSplitMigrationCapability_FullMethodName,
		pb.Distribution_AttestMigrationOwnership_FullMethodName,
		pb.Distribution_AbandonSplitJob_FullMethodName,
		pb.Distribution_RetrySplitJob_FullMethodName,
		pb.RaftAdmin_AddVoter_FullMethodName,
		pb.RaftAdmin_AddLearner_FullMethodName,
		pb.RaftAdmin_PromoteLearner_FullMethodName,
		pb.RaftAdmin_RemoveServer_FullMethodName,
		pb.RaftAdmin_TransferLeadership_FullMethodName,
		pb.EncryptionAdmin_BootstrapEncryption_FullMethodName,
		pb.EncryptionAdmin_RotateDEK_FullMethodName,
		pb.EncryptionAdmin_RegisterEncryptionWriter_FullMethodName,
		pb.EncryptionAdmin_ResyncSidecar_FullMethodName,
		pb.EncryptionAdmin_EnableStorageEnvelope_FullMethodName,
		pb.EncryptionAdmin_EnableRaftEnvelope_FullMethodName,
		pb.S3BlobFetch_PushChunkBlob_FullMethodName,
	}

	for _, method := range blockedMethods {
		t.Run("blocked/"+method, func(t *testing.T) {
			handlerCalled := false
			handler := func(context.Context, interface{}) (interface{}, error) {
				handlerCalled = true
				return "ok", nil
			}
			if _, err := gate.unaryInterceptor(
				context.Background(), nil,
				&grpc.UnaryServerInfo{FullMethod: method},
				handler,
			); status.Code(err) != codes.Unavailable {
				t.Fatalf("%s before ready err=%v, want Unavailable", method, err)
			}
			if handlerCalled {
				t.Fatalf("%s handler ran before startup gate opened", method)
			}
		})
	}

	allowedMethods := []string{
		pb.Internal_RelayPublish_FullMethodName,
		pb.RaftAdmin_Status_FullMethodName,
		pb.RaftAdmin_Configuration_FullMethodName,
		pb.EncryptionAdmin_GetCapability_FullMethodName,
		pb.EncryptionAdmin_GetSidecarState_FullMethodName,
	}
	for _, method := range allowedMethods {
		t.Run("allowed/"+method, func(t *testing.T) {
			handlerCalled := false
			handler := func(context.Context, interface{}) (interface{}, error) {
				handlerCalled = true
				return "ok", nil
			}
			if _, err := gate.unaryInterceptor(
				context.Background(), nil,
				&grpc.UnaryServerInfo{FullMethod: method},
				handler,
			); err != nil {
				t.Fatalf("%s before ready must pass: %v", method, err)
			}
			if !handlerCalled {
				t.Fatalf("%s handler did not run", method)
			}
		})
	}

	gate.markReady()
	for _, method := range blockedMethods {
		t.Run("ready/"+method, func(t *testing.T) {
			handlerCalled := false
			handler := func(context.Context, interface{}) (interface{}, error) {
				handlerCalled = true
				return "ok", nil
			}
			if _, err := gate.unaryInterceptor(
				context.Background(), nil,
				&grpc.UnaryServerInfo{FullMethod: method},
				handler,
			); err != nil {
				t.Fatalf("%s after ready err=%v", method, err)
			}
			if !handlerCalled {
				t.Fatalf("%s handler did not run after startup gate opened", method)
			}
		})
	}
}

func TestStartupPublicKVGate_BlocksMutatorStreamsUntilReady(t *testing.T) {
	t.Parallel()
	gate := &startupPublicKVGate{}

	handlerCalled := false
	handler := func(interface{}, grpc.ServerStream) error {
		handlerCalled = true
		return nil
	}
	err := gate.streamInterceptor(
		nil,
		nil,
		&grpc.StreamServerInfo{FullMethod: pb.S3BlobFetch_PushChunkBlob_FullMethodName},
		handler,
	)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("PushChunkBlob before ready err=%v, want Unavailable", err)
	}
	if handlerCalled {
		t.Fatal("stream handler ran before startup gate opened")
	}

	gate.markReady()
	if err := gate.streamInterceptor(
		nil,
		nil,
		&grpc.StreamServerInfo{FullMethod: pb.S3BlobFetch_PushChunkBlob_FullMethodName},
		handler,
	); err != nil {
		t.Fatalf("PushChunkBlob after ready err=%v", err)
	}
	if !handlerCalled {
		t.Fatal("stream handler did not run after startup gate opened")
	}
}

func TestStartupPublicKVGate_BlocksAfterReadyWhenStartupRotationPending(t *testing.T) {
	t.Parallel()
	blocked := true
	gate := &startupPublicKVGate{blockMutator: func() bool { return blocked }}
	gate.markReady()

	handlerCalled := false
	handler := func(context.Context, interface{}) (interface{}, error) {
		handlerCalled = true
		return "ok", nil
	}
	if _, err := gate.unaryInterceptor(
		context.Background(), nil,
		&grpc.UnaryServerInfo{FullMethod: pb.RaftAdmin_AddVoter_FullMethodName},
		handler,
	); status.Code(err) != codes.Unavailable {
		t.Fatalf("ready gate with pending startup rotation err=%v, want Unavailable", err)
	}
	if handlerCalled {
		t.Fatal("gated handler ran while startup rotation blocker was active")
	}

	blocked = false
	if _, err := gate.unaryInterceptor(
		context.Background(), nil,
		&grpc.UnaryServerInfo{FullMethod: pb.RaftAdmin_AddVoter_FullMethodName},
		handler,
	); err != nil {
		t.Fatalf("gate after blocker cleared: %v", err)
	}
	if !handlerCalled {
		t.Fatal("gated handler did not run after blocker cleared")
	}
}

func TestStartupGatedCoordinator_BlocksAdapterDispatchDuringAsyncRotation(t *testing.T) {
	t.Parallel()
	inner := &stubStartupCoordinator{clock: kv.NewHLC()}
	blocked := true
	gate := &startupPublicKVGate{blockMutator: func() bool { return blocked }}
	gate.markReady()
	coord := newStartupGatedCoordinator(inner, gate)

	if _, err := coord.Dispatch(context.Background(), nil); status.Code(err) != codes.Unavailable {
		t.Fatalf("blocked Dispatch err=%v, want Unavailable", err)
	}
	if got := inner.dispatches.Load(); got != 0 {
		t.Fatalf("inner Dispatch calls while blocked=%d, want 0", got)
	}
	if _, err := coord.LinearizableRead(context.Background()); err != nil {
		t.Fatalf("LinearizableRead should pass through while Dispatch is blocked: %v", err)
	}

	blocked = false
	if _, err := coord.Dispatch(context.Background(), nil); err != nil {
		t.Fatalf("Dispatch after blocker cleared: %v", err)
	}
	if got := inner.dispatches.Load(); got != 1 {
		t.Fatalf("inner Dispatch calls after unblock=%d, want 1", got)
	}
}

func TestStartupGatedCoordinator_ForwardsTimestampCapabilities(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("recover failed")
	inner := &stubStartupCoordinator{clock: kv.NewHLC(), recoverErr: sentinel}
	blocked := true
	gate := &startupPublicKVGate{blockMutator: func() bool { return blocked }}
	gate.markReady()
	coord := newStartupGatedCoordinator(inner, gate)

	assertStartupGatedTimestampBlocked(t, coord)
	blocked = false
	assertStartupGatedTimestampForwarding(t, coord, inner, sentinel)
}

func assertStartupGatedTimestampBlocked(t *testing.T, coord startupGatedCoordinator) {
	t.Helper()
	if _, err := coord.Next(context.Background()); status.Code(err) != codes.Unavailable {
		t.Fatalf("blocked Next() err=%v, want Unavailable", err)
	}
	if _, err := coord.NextAfter(context.Background(), 200); status.Code(err) != codes.Unavailable {
		t.Fatalf("blocked NextAfter() err=%v, want Unavailable", err)
	}
}

func assertStartupGatedTimestampForwarding(
	t *testing.T,
	coord startupGatedCoordinator,
	inner *stubStartupCoordinator,
	recoverErr error,
) {
	t.Helper()
	ts, err := coord.Next(context.Background())
	if err != nil || ts != 101 {
		t.Fatalf("Next() after unblock = (%d, %v), want (101, nil)", ts, err)
	}
	next, err := coord.NextAfter(context.Background(), 200)
	if err != nil || next != 201 {
		t.Fatalf("NextAfter() after unblock = (%d, %v), want (201, nil)", next, err)
	}
	if err := coord.RecoverHLCLease(context.Background()); !errors.Is(err, recoverErr) {
		t.Fatalf("RecoverHLCLease() err=%v, want sentinel", err)
	}
	if got := inner.nextCalls.Load(); got != 1 {
		t.Fatalf("inner Next calls=%d, want 1", got)
	}
	if got := inner.nextAfter.Load(); got != 1 {
		t.Fatalf("inner NextAfter calls=%d, want 1", got)
	}
	if got := inner.recoverCalls.Load(); got != 1 {
		t.Fatalf("inner RecoverHLCLease calls=%d, want 1", got)
	}
	if got := inner.dispatches.Load(); got != 0 {
		t.Fatalf("timestamp capability forwarding called Dispatch %d times, want 0", got)
	}
}

func TestRuntimeServerRunner_PrepareAdminForwardServersDoesNotBindPublicListeners(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	lc := &net.ListenConfig{}
	heldDynamo, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen held dynamo: %v", err)
	}
	defer heldDynamo.Close()
	heldS3, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen held s3: %v", err)
	}
	defer heldS3.Close()

	runner := runtimeServerRunner{
		ctx:             ctx,
		lc:              lc,
		dynamoAddress:   heldDynamo.Addr().String(),
		s3Address:       heldS3.Addr().String(),
		s3PathStyleOnly: true,
		metricsRegistry: monitoring.NewRegistry("test-node", "127.0.0.1:0"),
		coordinate:      newStartupGatedCoordinator(&stubStartupCoordinator{clock: kv.NewHLC()}, nil),
	}
	if err := runner.prepareAdminForwardServers(); err != nil {
		t.Fatalf("prepareAdminForwardServers: %v", err)
	}
	if runner.dynamoServer == nil {
		t.Fatal("dynamo admin-forward server was not prepared")
	}
	if runner.s3Server == nil {
		t.Fatal("s3 admin-forward server was not prepared")
	}
	if runner.dynamoListener != nil || runner.s3Listener != nil {
		t.Fatalf("prepareAdminForwardServers bound listeners: dynamo=%v s3=%v", runner.dynamoListener, runner.s3Listener)
	}
}

func TestRuntimeServerRunner_PreparePublicServicesBindsBeforeRotation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	lc := &net.ListenConfig{}
	heldDynamo, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen held dynamo: %v", err)
	}
	defer heldDynamo.Close()

	runner := runtimeServerRunner{
		ctx:             ctx,
		lc:              lc,
		redisAddress:    "127.0.0.1:0",
		dynamoAddress:   heldDynamo.Addr().String(),
		s3PathStyleOnly: true,
		metricsAddress:  "",
		pprofAddress:    "",
		pubsubRelay:     adapter.NewRedisPubSubRelay(),
		metricsRegistry: monitoring.NewRegistry("test-node", "127.0.0.1:0"),
		coordinate:      &stubStartupCoordinator{clock: kv.NewHLC()},
	}
	if err := runner.prepareAdminForwardServers(); err != nil {
		t.Fatalf("prepareAdminForwardServers: %v", err)
	}
	err = runner.preparePublicServices()
	if err == nil || !strings.Contains(err.Error(), "failed to listen") {
		t.Fatalf("preparePublicServices err=%v, want occupied listener error before rotation", err)
	}
	runner.closePreparedExternalListeners()
}
