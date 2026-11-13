package server

import (
	"context"
	"crypto"
	"crypto/sha256"
	"crypto/tls"
	"math"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/encryptionwiring"
	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func publishBootstrapRegistrationPreparedFixture(t *testing.T, a *raftadmission.Arbiter, ticket raftadmission.RecoveryTicket, policy raftbootstrap.PolicyRecord, cert tls.Certificate) {
	t.Helper()
	digest, err := raftbootstrap.ConfigurationDigest(policy.Configuration)
	require.NoError(t, err)
	signer, ok := cert.PrivateKey.(crypto.Signer)
	require.True(t, ok)
	probe, err := raftbootstrap.SignBaselineProbe(raftbootstrap.BaselineProbe{
		BaselineContext: raftbootstrap.BaselineContext{Version: raftbootstrap.BaselineVersion,
			Identity: policy.Identity, OperationID: raftbootstrap.OperationID{1}, Configuration: policy.Configuration,
			Minimum: policy.Minimum, CommitIndex: 5, CommitTerm: 1, DataDigest: raftbootstrap.Digest{1}},
		Evidence: raftbootstrap.BaselineMemberEvidence{MemberID: 1, CommitIndex: 5, CommitTerm: 1,
			AppliedIndex: 5, LastLogIndex: 5, ConfigurationDigest: digest,
			NativeStateDigest: raftbootstrap.Digest{2}, DataDigest: raftbootstrap.Digest{1}},
	}, signer)
	require.NoError(t, err)
	require.NoError(t, a.PublishPreparedWith(ticket, probe, raftbootstrap.Digest{3},
		func(peersession.Stage, uint64) (func(), error) { return nil, nil }))
}

func TestBootstrapRegistrationExactDescriptor(t *testing.T) {
	t.Parallel()
	method := bootstrapMethodForTest(t, 17, pb.EncryptionAdmin_RegisterEncryptionWriter_FullMethodName)
	binding := bootstrapBindingForTest(17)
	req := encryptionwiring.RegistrationRequest(7, binding.From.Member, 0)
	message, err := method.BoundRequest(binding, req)
	require.NoError(t, err)
	require.Equal(t, peersession.Registration, message.Traffic)
	require.Equal(t, raftbootstrap.Digest(sha256.Sum256(encryptionwiring.RegistrationEntry(7, binding.From.Member, 0))), message.Declaration)
	reply, err := method.Reply(message, &pb.RegisterEncryptionWriterResponse{AppliedIndex: 1})
	require.NoError(t, err)
	require.Equal(t, message.From, reply.To)
	require.Equal(t, peersession.Registration, reply.Traffic)
	for _, mutate := range []func(*pb.RegisterEncryptionWriterRequest){
		func(r *pb.RegisterEncryptionWriterRequest) { r.DekId = 0 },
		func(r *pb.RegisterEncryptionWriterRequest) { r.Writers = nil },
		func(r *pb.RegisterEncryptionWriterRequest) { r.Writers = append(r.Writers, r.Writers[0]) },
		func(r *pb.RegisterEncryptionWriterRequest) { r.Writers[0] = nil },
		func(r *pb.RegisterEncryptionWriterRequest) { r.Writers[0].FullNodeId = 1<<32 | binding.From.Member },
		func(r *pb.RegisterEncryptionWriterRequest) { r.Writers[0].LocalEpoch = math.MaxUint16 + 1 },
		func(r *pb.RegisterEncryptionWriterRequest) { r.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		func(r *pb.RegisterEncryptionWriterRequest) { r.Writers[0].ProtoReflect().SetUnknown([]byte{0x78, 1}) },
	} {
		invalid, ok := proto.Clone(req).(*pb.RegisterEncryptionWriterRequest)
		require.True(t, ok)
		mutate(invalid)
		_, err := method.BoundRequest(binding, invalid)
		require.Error(t, err)
	}
	_, err = method.Reply(message, &pb.RegisterEncryptionWriterResponse{})
	require.Error(t, err)
}

func TestBootstrapRegistrationTLSIdentityReadyWithNativeHandle(t *testing.T) {
	t.Parallel()
	f := newBootstrapAuthFixture(t, 17, peersession.IdentityReady, nil, bootstrapAuthRegistrationOptions{prepared: true})
	require.True(t, f.a.NeedsActivation(17))
	entry := encryptionwiring.RegistrationEntry(7, 2, 3)
	f.service.action = func(ctx context.Context) error {
		bound, err := f.a.BindProposal(ctx, f.group, entry)
		if err != nil {
			return err
		}
		return f.a.WithNative(bound, f.group, func(scope raftengine.NativeScope) error {
			return scope.Check(bound, raftengine.NativeRequest{Operation: raftengine.NativeProposal, PayloadDigest: sha256.Sum256(entry)})
		})
	}
	ctx, cancel := bootstrapGateContext(t, bootstrapGateTestToken)
	defer cancel()
	err := f.handle.Invoke(ctx, f.cc, pb.EncryptionAdmin_RegisterEncryptionWriter_FullMethodName,
		encryptionwiring.RegistrationRequest(7, 2, 3), &pb.RegisterEncryptionWriterResponse{})
	require.NoError(t, err)
	require.Zero(t, f.authCalls.Load(), "only exact typed registration bypasses independent admin auth")
	require.EqualValues(t, 1, f.service.calls.Load())
}

func TestBootstrapRegistrationTLSMissingTokenCannotReachHandler(t *testing.T) {
	t.Parallel()
	f := newBootstrapAuthFixture(t, 17, peersession.IdentityReady, nil)
	for _, token := range []string{"", "incorrect"} {
		ctx, cancel := bootstrapGateContext(t, token)
		err := f.handle.Invoke(ctx, f.cc, pb.EncryptionAdmin_RegisterEncryptionWriter_FullMethodName,
			encryptionwiring.RegistrationRequest(7, 2, 3), &pb.RegisterEncryptionWriterResponse{})
		cancel()
		require.Error(t, err)
	}
	require.Zero(t, f.service.calls.Load())
}

func TestBootstrapRegistrationTLSEpochCannotChangeAfterCapture(t *testing.T) {
	t.Parallel()
	f := newBootstrapAuthFixture(t, 17, peersession.IdentityReady, func(payload any) any {
		original, ok := payload.(*pb.RegisterEncryptionWriterRequest)
		require.True(t, ok)
		req, ok := proto.Clone(original).(*pb.RegisterEncryptionWriterRequest)
		require.True(t, ok)
		req.Writers[0].LocalEpoch++
		return req
	})
	ctx, cancel := bootstrapGateContext(t, bootstrapGateTestToken)
	defer cancel()
	require.Error(t, f.handle.Invoke(ctx, f.cc, pb.EncryptionAdmin_RegisterEncryptionWriter_FullMethodName,
		encryptionwiring.RegistrationRequest(7, 2, 3), &pb.RegisterEncryptionWriterResponse{}))
	require.Zero(t, f.service.calls.Load())
	require.EqualValues(t, 1, f.authCalls.Load(), "changed declaration must not bypass original auth")
}

func TestBootstrapRegistrationTLSRequiresExplicitDefaultGroupReceiver(t *testing.T) {
	t.Parallel()
	otherGroup := uint64(18)
	for _, tc := range []struct {
		name    string
		options bootstrapAuthRegistrationOptions
	}{
		{"missing receiver", bootstrapAuthRegistrationOptions{omitReceiver: true}},
		{"different group", bootstrapAuthRegistrationOptions{receiverGroup: &otherGroup}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newBootstrapAuthFixture(t, 17, peersession.IdentityReady, nil, tc.options)
			ctx, cancel := bootstrapGateContext(t, bootstrapGateTestToken)
			defer cancel()
			require.Error(t, f.handle.Invoke(ctx, f.cc, pb.EncryptionAdmin_RegisterEncryptionWriter_FullMethodName,
				encryptionwiring.RegistrationRequest(7, 2, 3), &pb.RegisterEncryptionWriterResponse{}))
			require.Zero(t, f.service.calls.Load())
			require.EqualValues(t, 1, f.authCalls.Load(), "nondefault session must retain original auth")
		})
	}
}

type bootstrapRegistrationProposer struct {
	raftengine.Proposer
	action func(context.Context, []byte) (*raftengine.ProposalResult, error)
}

func (p bootstrapRegistrationProposer) Propose(ctx context.Context, entry []byte) (*raftengine.ProposalResult, error) {
	return p.action(ctx, entry)
}

func TestBootstrapRegistrationLocalProposalOwnsHandleAndCloseJoins(t *testing.T) {
	t.Parallel()
	f := newBootstrapNetworkFixture(t, false)
	ctx, err := clusterbootstrap.WithBootstrapRegistration(t.Context(), f.network, bootstrapNetworkTestGroup, f.config.Local.Member,
		func(uint32, uint16) error { return nil })
	require.NoError(t, err)
	entered := make(chan struct{})
	proposer := bootstrapRegistrationProposer{action: func(ctx context.Context, entry []byte) (*raftengine.ProposalResult, error) {
		bound, err := f.sender.Admission.BindProposal(ctx, bootstrapNetworkTestGroup, entry)
		if err != nil {
			return nil, err
		}
		err = f.sender.Admission.WithNative(bound, bootstrapNetworkTestGroup, func(scope raftengine.NativeScope) error {
			return scope.Check(bound, raftengine.NativeRequest{Operation: raftengine.NativeProposal, PayloadDigest: sha256.Sum256(entry)})
		})
		if err != nil {
			return nil, err
		}
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	coord := kv.NewShardedCoordinator(distribution.NewEngine(), map[uint64]*kv.ShardGroup{
		bootstrapNetworkTestGroup: {Engine: &recordingMembershipProposer{}},
	}, bootstrapNetworkTestGroup, kv.NewHLC(), nil)
	done := make(chan error, 1)
	go func() {
		done <- encryptionwiring.ProposeWriterRegistration(ctx, coord, proposer, nil, encryptionwiring.RegistrationEntry(7, f.config.Local.Member, 3),
			encryptionwiring.RegistrationRequest(7, f.config.Local.Member, 3))
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("registration did not enter: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("registration did not enter")
	}
	f.network.Close()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestBootstrapRegistrationPrivateForwardNeverUsesPublicFallback(t *testing.T) {
	t.Parallel()
	for _, wrongSPKI := range []bool{false, true} {
		t.Run(map[bool]string{false: "valid", true: "wrong SPKI"}[wrongSPKI], func(t *testing.T) {
			f := newBootstrapNetworkFixture(t, wrongSPKI)
			ctx, err := clusterbootstrap.WithBootstrapRegistration(t.Context(), f.network, bootstrapNetworkTestGroup, f.config.Local.Member,
				func(uint32, uint16) error { return nil })
			require.NoError(t, err)
			source := clusterbootstrap.BootstrapRegistrationFromContext(ctx)
			err = source.Forward(ctx, f.address, encryptionwiring.RegistrationRequest(7, f.config.Local.Member, 3))
			if wrongSPKI {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Zero(t, f.ordinary.Load())
			require.Error(t, source.Forward(ctx, "unprovisioned.test:9001", encryptionwiring.RegistrationRequest(7, f.config.Local.Member, 3)))
		})
	}
}

func TestBootstrapRegistrationPrivateForwardCloseCancelsAndJoins(t *testing.T) {
	t.Parallel()
	f := newBootstrapNetworkFixture(t, false)
	ctx, err := clusterbootstrap.WithBootstrapRegistration(t.Context(), f.network, bootstrapNetworkTestGroup, f.config.Local.Member,
		func(uint32, uint16) error { return nil })
	require.NoError(t, err)
	entered, handlerDone := make(chan struct{}), make(chan struct{})
	f.service.action = func(ctx context.Context) error {
		close(entered)
		defer close(handlerDone)
		<-ctx.Done()
		return ctx.Err()
	}
	done := make(chan error, 1)
	go func() {
		done <- clusterbootstrap.BootstrapRegistrationFromContext(ctx).Forward(ctx, f.address,
			encryptionwiring.RegistrationRequest(7, f.config.Local.Member, 3))
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("registration did not enter: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("registration did not enter")
	}
	f.network.Close()
	require.Error(t, <-done)
	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("receiver registration did not cancel")
	}
}

func TestBootstrapRegistrationSourcePinsEpochAndPayload(t *testing.T) {
	t.Parallel()
	f := newBootstrapNetworkFixture(t, false)
	ctx, err := clusterbootstrap.WithBootstrapRegistration(t.Context(), f.network, bootstrapNetworkTestGroup, f.config.Local.Member,
		func(dek uint32, epoch uint16) error {
			if dek != 7 || epoch != 3 {
				return errors.New("durable epoch mismatch")
			}
			return nil
		})
	require.NoError(t, err)
	source := clusterbootstrap.BootstrapRegistrationFromContext(ctx)
	req := encryptionwiring.RegistrationRequest(7, f.config.Local.Member, 3)
	entry := encryptionwiring.RegistrationEntry(7, f.config.Local.Member, 3)
	require.NoError(t, source.Validate(entry, req))
	require.Error(t, source.Validate(append(entry, 0), req))
	require.Error(t, source.Validate(encryptionwiring.RegistrationEntry(7, f.config.Local.Member, 4), encryptionwiring.RegistrationRequest(7, f.config.Local.Member, 4)))
	_, err = clusterbootstrap.WithBootstrapRegistration(t.Context(), f.network, bootstrapNetworkTestGroup, 999, source.ValidateEpoch)
	require.Error(t, err)
	_, err = clusterbootstrap.WithBootstrapRegistration(t.Context(), f.network, bootstrapNetworkTestGroup+1, f.config.Local.Member, source.ValidateEpoch)
	require.Error(t, err)
	_, err = clusterbootstrap.WithBootstrapRegistration(t.Context(), f.network, bootstrapNetworkTestGroup, f.config.Local.Member, nil)
	require.Error(t, err)
	require.Nil(t, clusterbootstrap.BootstrapRegistrationFromContext(t.Context()), "legacy profile is unchanged")
}

func TestBootstrapRegistrationTLSRevocationRetainsHandleThroughResult(t *testing.T) {
	t.Parallel()
	f := newBootstrapAuthFixture(t, 17, peersession.IdentityReady, nil)
	f.service.action = func(context.Context) error { f.server.Close(); return nil }
	// Metadata is still supplied only by the explicit startup call.
	startup, stop := bootstrapGateContext(t, bootstrapGateTestToken)
	defer stop()
	err := f.handle.Invoke(startup, f.cc, pb.EncryptionAdmin_RegisterEncryptionWriter_FullMethodName,
		encryptionwiring.RegistrationRequest(7, 2, 3), &pb.RegisterEncryptionWriterResponse{})
	require.Error(t, err)
}
