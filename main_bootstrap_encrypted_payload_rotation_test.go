package main

import (
	"bytes"
	"context"
	"math"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/encryptiontest"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/bootjp/elastickv/internal/encryptionwiring"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func bootstrapTestRotation() fsmwire.RotationPayload {
	return fsmwire.RotationPayload{SubTag: fsmwire.RotateSubRotateDEK, DEKID: 9, Purpose: fsmwire.PurposeStorage,
		Wrapped: []byte("controller-generated-wrapped-dek"), ProposerRegistration: fsmwire.RegistrationPayload{DEKID: 9, FullNodeID: 42, LocalEpoch: 7}}
}

func bootstrapTestRotationFrame(rotation fsmwire.RotationPayload) []byte {
	return append([]byte{fsmwire.OpRotation}, fsmwire.EncodeRotation(rotation)...)
}

func TestBootstrapRotationPayloadExactGrantPlainAndEncrypted(t *testing.T) {
	t.Parallel()
	for _, purpose := range []fsmwire.Purpose{fsmwire.PurposeStorage, fsmwire.PurposeRaft} {
		for _, subtag := range []byte{fsmwire.RotateSubRotateDEK, fsmwire.RotateSubEnableStorageEnvelope, fsmwire.RotateSubEnableRaftEnvelope} {
			rotation := bootstrapTestRotation()
			rotation.Purpose, rotation.SubTag = purpose, subtag
			if subtag != fsmwire.RotateSubRotateDEK {
				rotation.Wrapped = nil
			}
			if subtag == fsmwire.RotateSubEnableStorageEnvelope && purpose != fsmwire.PurposeStorage || subtag == fsmwire.RotateSubEnableRaftEnvelope && purpose != fsmwire.PurposeRaft {
				continue
			}
			assertBootstrapRotationGrant(t, bootstrapTestRotationFrame(rotation))
		}
	}
}

func assertBootstrapRotationGrant(t *testing.T, plain []byte) {
	t.Helper()
	grant, err := clusterbootstrap.NewBootstrapRotationPayloadGrant(17, plain)
	require.NoError(t, err)
	encrypted, wrap := bootstrapEncryptedPayloadFixture(t, 17, grant)
	unwrapped, err := clusterbootstrap.NewBootstrapPayloadProjection(17, 1, nil, 0, false, grant)
	require.NoError(t, err)
	final, err := wrap(plain)
	require.NoError(t, err)
	for _, fixture := range []struct {
		projection clusterbootstrap.BootstrapPayloadProjection
		payload    []byte
	}{{encrypted, final}, {unwrapped, plain}} {
		_, err := clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{1}, raftadmission.Rotation, 17, fixture.payload, fixture.projection)
		require.NoError(t, err)
		for _, lane := range []raftadmission.Kind{raftadmission.Data, raftadmission.Control, raftadmission.Registration, raftadmission.Recovery, raftadmission.Catalog, raftadmission.HLC, raftadmission.Allocator} {
			_, err := clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{1}, lane, 17, fixture.payload, fixture.projection)
			require.ErrorIs(t, err, clusterbootstrap.ErrBootstrapAdmissionPayload)
		}
		fixture.projection.RotationRecipes = nil
		_, err = clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{1}, raftadmission.Rotation, 17, fixture.payload, fixture.projection)
		require.ErrorIs(t, err, clusterbootstrap.ErrBootstrapRotationRecipe)
	}
	require.ErrorIs(t, clusterbootstrap.BootstrapPayloadValidator(raftbootstrap.UUID{1})(raftadmission.Rotation, 17, plain), clusterbootstrap.ErrBootstrapRotationRecipe,
		"legacy validator must remain closed without an explicitly owned rotation recipe")
}

func TestBootstrapRotationPayloadGrantRejectsDifferentCompleteRecipe(t *testing.T) {
	t.Parallel()
	rotation := bootstrapTestRotation()
	grant, err := clusterbootstrap.NewBootstrapRotationPayloadGrant(17, bootstrapTestRotationFrame(rotation))
	require.NoError(t, err)
	projection, wrap := bootstrapEncryptedPayloadFixture(t, 17, grant)
	for name, mutate := range map[string]func(*fsmwire.RotationPayload){
		"wrapped key": func(p *fsmwire.RotationPayload) { p.Wrapped[0] ^= 1 },
		"dek":         func(p *fsmwire.RotationPayload) { p.DEKID++; p.ProposerRegistration.DEKID++ },
		"purpose":     func(p *fsmwire.RotationPayload) { p.Purpose = fsmwire.PurposeRaft },
		"writer":      func(p *fsmwire.RotationPayload) { p.ProposerRegistration.FullNodeID++ },
		"epoch":       func(p *fsmwire.RotationPayload) { p.ProposerRegistration.LocalEpoch++ },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := rotation
			candidate.Wrapped = bytes.Clone(rotation.Wrapped)
			mutate(&candidate)
			plain := bootstrapTestRotationFrame(candidate)
			require.NoError(t, clusterbootstrap.ValidateBootstrapRotationPayload(plain), "changed recipe is still a structurally valid rotation")
			final, err := wrap(plain)
			require.NoError(t, err)
			_, err = clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{1}, raftadmission.Rotation, 17, final, projection)
			require.ErrorIs(t, err, clusterbootstrap.ErrBootstrapRotationRecipe)
		})
	}
	_, err = clusterbootstrap.NewBootstrapPayloadProjection(18, 1, nil, 0, false, grant)
	require.ErrorIs(t, err, clusterbootstrap.ErrBootstrapRotationRecipe)
	grants := []clusterbootstrap.BootstrapRotationPayloadGrant{grant}
	plainProjection, err := clusterbootstrap.NewBootstrapPayloadProjection(17, 1, nil, 0, false, grants...)
	require.NoError(t, err)
	grants[0] = clusterbootstrap.BootstrapRotationPayloadGrant{}
	_, err = clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{1}, raftadmission.Rotation, 17, bootstrapTestRotationFrame(rotation), plainProjection)
	require.NoError(t, err, "projection must not borrow a caller-mutable grant slice")
}

func TestBootstrapRotationPayloadRejectsInvalidFrames(t *testing.T) {
	t.Parallel()
	rotation := bootstrapTestRotation()
	for name, mutate := range map[string]func(*fsmwire.RotationPayload){
		"unknown subtag":        func(p *fsmwire.RotationPayload) { p.SubTag = 0xff },
		"unknown purpose":       func(p *fsmwire.RotationPayload) { p.Purpose = 0xff },
		"zero dek":              func(p *fsmwire.RotationPayload) { p.DEKID = 0; p.ProposerRegistration.DEKID = 0 },
		"mismatched registry":   func(p *fsmwire.RotationPayload) { p.ProposerRegistration.DEKID++ },
		"zero writer":           func(p *fsmwire.RotationPayload) { p.ProposerRegistration.FullNodeID = 0 },
		"missing wrapped":       func(p *fsmwire.RotationPayload) { p.Wrapped = nil },
		"cutover has wrapped":   func(p *fsmwire.RotationPayload) { p.SubTag = fsmwire.RotateSubEnableStorageEnvelope },
		"wrong cutover purpose": func(p *fsmwire.RotationPayload) { p.SubTag = fsmwire.RotateSubEnableRaftEnvelope; p.Wrapped = nil },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := rotation
			mutate(&candidate)
			_, err := clusterbootstrap.NewBootstrapRotationPayloadGrant(17, bootstrapTestRotationFrame(candidate))
			require.ErrorIs(t, err, clusterbootstrap.ErrBootstrapAdmissionPayload)
		})
	}
	valid := bootstrapTestRotationFrame(rotation)
	for length := range len(valid) {
		require.ErrorIs(t, clusterbootstrap.ValidateBootstrapRotationPayload(valid[:length]), clusterbootstrap.ErrBootstrapAdmissionPayload)
	}
	require.ErrorIs(t, clusterbootstrap.ValidateBootstrapRotationPayload(append(bytes.Clone(valid), 0)), clusterbootstrap.ErrBootstrapAdmissionPayload)
	for _, opcode := range []byte{fsmwire.OpBootstrap, fsmwire.OpRegistration, 0x06, 0xff} {
		candidate := bytes.Clone(valid)
		candidate[0] = opcode
		require.ErrorIs(t, clusterbootstrap.ValidateBootstrapRotationPayload(candidate), clusterbootstrap.ErrBootstrapAdmissionPayload)
	}
}

type bootstrapEncryptedCaptureEngine struct {
	raftengine.Engine
	final []byte
}

func (e *bootstrapEncryptedCaptureEngine) ProposeAdmin(_ context.Context, payload []byte) (*raftengine.ProposalResult, error) {
	e.final = bytes.Clone(payload)
	return &raftengine.ProposalResult{CommitIndex: 1}, nil
}

func TestBootstrapRotationRequestGrantMatchesActualAdminAndDynamicWrap(t *testing.T) {
	t.Parallel()
	request := &pb.RotateDEKRequest{NewDekId: 9, Purpose: pb.RotateDEKRequest_PURPOSE_RAFT, WrappedNewDek: []byte("fixed wrapped DEK"), ProposerNodeId: 42, ProposerLocalEpoch: 7}
	grants, err := clusterbootstrap.BootstrapRotationPayloadGrants(17, request)
	require.NoError(t, err)
	cipher := encryptiontest.NewCipher(t, bootstrapEncryptedTestKey)
	var cutover atomic.Uint64
	runtime, err := encryptionwiring.NewRaftEnvelopeRuntime(cipher, encryptiontest.NewNonceFactory(t), 1, bootstrapEncryptedTestKey, &cutover, 7, 42, nil)
	require.NoError(t, err)
	engine := &bootstrapEncryptedCaptureEngine{}
	shard := &kv.ShardGroup{Engine: engine}
	shard.Txn = kv.NewLeaderProxyForShardGroup(shard)
	runtime.AttachGroup(17, shard)
	admin := adapter.NewEncryptionAdminServer(adapter.WithEncryptionAdminProposer(shard.Proposer()), adapter.WithEncryptionAdminLeaderView(rotateStartupLeader{}))
	_, err = admin.RotateDEK(t.Context(), request)
	require.NoError(t, err)
	projection, err := clusterbootstrap.NewBootstrapPayloadProjection(17, 1, cipher, bootstrapEncryptedTestKey, true, grants...)
	require.NoError(t, err)
	_, err = clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{1}, raftadmission.Rotation, 17, engine.final, projection)
	require.NoError(t, err)
	actual, err := encryption.UnwrapRaftPayload(cipher, engine.final)
	require.NoError(t, err)
	expected, err := clusterbootstrap.BootstrapRotationPayloadFromRequest(request)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
	for _, bad := range []*pb.RotateDEKRequest{nil,
		{Purpose: pb.RotateDEKRequest_PURPOSE_RAFT, NewDekId: 1, WrappedNewDek: []byte{1}, ProposerNodeId: 42, ProposerLocalEpoch: math.MaxUint16 + 1},
		{Purpose: pb.RotateDEKRequest_Purpose(99)},
	} {
		_, err := clusterbootstrap.BootstrapRotationPayloadFromRequest(bad)
		require.Error(t, err)
	}
	unknown, ok := proto.Clone(request).(*pb.RotateDEKRequest)
	require.True(t, ok)
	unknown.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 1})
	_, err = clusterbootstrap.BootstrapRotationPayloadFromRequest(unknown)
	require.ErrorIs(t, err, clusterbootstrap.ErrBootstrapRotationRecipe)
}
