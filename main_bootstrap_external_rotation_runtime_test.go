package main

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBootstrapExternalRotationRuntimeUnknownDoesNotReissue(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(map[bool]string{false: "absent", true: "applied"}[applied], func(t *testing.T) {
			f := newRotationRuntimeFixture(t, raftengine.StateLeader)
			require.NoError(t, f.runtime.step(t.Context()))
			require.True(t, f.a.LocalPermissionReady())
			prior := len(f.io.calls)
			f.runtime.config.planExternal = func(ctx context.Context, request *pb.RotateDEKRequest) (*bootstrapRotationRecipe, error) {
				return newBootstrapExternalRotationRecipe(ctx, f.io.cfg, request)
			}
			f.io.admin = func(_ context.Context, request *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
				if applied {
					f.io.apply(request)
				}
				f.a.InvalidateEncryption()
				return nil, raftadmission.ErrStale
			}
			intent, err := f.a.CaptureRotationRequest(t.Context())
			require.NoError(t, err)
			defer intent.CancelAndJoin()
			_, err = f.runtime.executeExternal(bootstrapExternalRotationCall{intent: intent, request: externalRotationTestRequest(f.io)})
			require.Error(t, err)
			require.Len(t, f.io.calls, prior+1)
			// Startup rotation remains enabled; a proven absent external
			// command must restore its earlier complete recipe, not rotate anew.
			require.NoError(t, f.runtime.step(t.Context()))
			require.NoError(t, f.runtime.step(t.Context()))
			require.Len(t, f.io.calls, prior+1, "background reconciliation must never replay the external proposal")
		})
	}
}

func TestBootstrapExternalRotationUnknownOverridesNestedCanceledStatus(t *testing.T) {
	err := bootstrapExternalRotationUnknown(status.Error(codes.Canceled, "native owner invalidated"))
	require.Equal(t, codes.Unknown, status.Code(err))
	require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
}
