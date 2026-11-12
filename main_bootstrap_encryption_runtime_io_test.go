package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryptionwiring"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type encryptionRuntimeIOTestKEK struct{}

func (encryptionRuntimeIOTestKEK) Name() string { return "test" }
func (encryptionRuntimeIOTestKEK) Wrap(dek []byte) ([]byte, error) {
	return append(make([]byte, 16), dek...), nil
}
func (encryptionRuntimeIOTestKEK) Unwrap(wrapped []byte) ([]byte, error) {
	if len(wrapped) != 16+encryption.KeySize {
		return nil, errors.New("invalid test ciphertext")
	}
	return bytes.Clone(wrapped[16:]), nil
}

type encryptionRuntimeIOTestEngine struct {
	raftengine.Engine
	calls [][]byte
}

func (*encryptionRuntimeIOTestEngine) State() raftengine.State            { return raftengine.StateLeader }
func (*encryptionRuntimeIOTestEngine) Leader() raftengine.LeaderInfo      { return raftengine.LeaderInfo{} }
func (*encryptionRuntimeIOTestEngine) VerifyLeader(context.Context) error { return nil }
func (*encryptionRuntimeIOTestEngine) LinearizableRead(context.Context) (uint64, error) {
	return 40, nil
}
func (e *encryptionRuntimeIOTestEngine) Propose(_ context.Context, p []byte) (*raftengine.ProposalResult, error) {
	e.calls = append(e.calls, bytes.Clone(p))
	return &raftengine.ProposalResult{CommitIndex: 41}, nil
}
func (e *encryptionRuntimeIOTestEngine) ProposeAdmin(ctx context.Context, p []byte) (*raftengine.ProposalResult, error) {
	return e.Propose(ctx, p)
}

type encryptionRuntimeIOFixture struct {
	*bootstrapInstalledFixture
	io     *clusterbootstrap.BootstrapEncryptionRuntimeIO
	cfg    clusterbootstrap.BootstrapEncryptionRuntimeIOConfig
	engine *encryptionRuntimeIOTestEngine
}

func newEncryptionRuntimeIOFixture(t *testing.T) *encryptionRuntimeIOFixture {
	t.Helper()
	f := newBootstrapInstalledFixture(t)
	for id, key := range f.sidecar.Keys {
		wrapped, err := (encryptionRuntimeIOTestKEK{}).Wrap(make([]byte, encryption.KeySize))
		require.NoError(t, err)
		key.Wrapped = wrapped
		f.sidecar.Keys[id] = key
	}
	f.persist(t)
	engine := &encryptionRuntimeIOTestEngine{}
	f.group.Engine = engine
	_ = kv.NewLeaderProxyForShardGroup(f.group)
	domain := bootstrapIngressFixture(t).Domain
	w := encryptionwiring.WriteWiring{Cache: encryption.NewStateCache(), Cipher: f.runtime.Cipher, RaftEnvelope: f.runtime, Epoch: 7, RaftEpoch: 9}
	rt := &clusterbootstrap.GroupRuntime{GroupRuntime: &noderuntime.GroupRuntime{Spec: noderuntime.GroupSpec{ID: 1}, Engine: engine, Store: f.in.LocalStore}, BootstrapDomain: domain}
	cfg := clusterbootstrap.BootstrapEncryptionRuntimeIOConfig{DefaultRuntime: rt, Group: f.group, Wiring: w, SidecarPath: f.in.SidecarPath, Wrapper: encryptionRuntimeIOTestKEK{}, Domain: domain, NodeID: 123, RotationRequested: true}
	io, err := clusterbootstrap.NewBootstrapEncryptionRuntimeIO(cfg)
	require.NoError(t, err)
	return &encryptionRuntimeIOFixture{bootstrapInstalledFixture: f, io: io, cfg: cfg, engine: engine}
}

func TestBootstrapEncryptionRuntimeIOObserveDoesNotMutate(t *testing.T) {
	t.Parallel()
	f := newEncryptionRuntimeIOFixture(t)
	before, err := f.io.Observe(t.Context())
	require.NoError(t, err)
	after, err := f.io.Observe(t.Context())
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, encryptionwiring.BootstrapRotationRequestedUnproven, after.Rotation)
	gen, err := f.cfg.Domain.Admission.EncryptionFence()
	require.NoError(t, err)
	f.cfg.Wiring.Cache.MarkRegistered(1)
	changed, err := f.cfg.Domain.Admission.EncryptionFence()
	require.NoError(t, err)
	require.NotEqual(t, gen, changed, "constructor must bind storage async mutation hooks")
}

func TestBootstrapEncryptionRuntimeIORegistrationEpoch(t *testing.T) {
	t.Parallel()
	f := newEncryptionRuntimeIOFixture(t)
	require.NoError(t, f.io.ValidateEpoch(1, 7))
	require.NoError(t, f.io.ValidateEpoch(2, 9))
	require.ErrorIs(t, f.io.ValidateEpoch(1, 9), encryption.ErrLocalEpochRollback)
	require.ErrorIs(t, f.io.ValidateEpoch(3, 7), encryptionwiring.ErrBootstrapEncryptionUnsupported)
}

func TestBootstrapEncryptionRuntimeIORejectsWrongActualMaterial(t *testing.T) {
	t.Parallel()
	f := newEncryptionRuntimeIOFixture(t)
	key := f.sidecar.Keys["1"]
	key.Wrapped[16] = 1
	f.sidecar.Keys["1"] = key
	f.persist(t)
	_, err := f.io.Observe(t.Context())
	require.ErrorContains(t, err, "installed cipher material mismatch")
	require.NotErrorIs(t, err, encryptionwiring.ErrBootstrapEncryptionProjectionPending)
}

func TestBootstrapEncryptionRuntimeIORepairsWrapperOnlyOnce(t *testing.T) {
	t.Parallel()
	f := newEncryptionRuntimeIOFixture(t)
	f.group.SetRaftPayloadWrap(nil)
	_, err := f.io.Observe(t.Context())
	require.ErrorIs(t, err, encryptionwiring.ErrBootstrapEncryptionProjectionPending)
	cfg := f.io.RecipeConfig(func(context.Context) (uint64, error) { return 40, nil }, func(ctx context.Context) error { return ctx.Err() })
	require.NoError(t, cfg.InstallAndVerify(t.Context(), 2, f.sidecar.Keys["2"]))
	before, err := f.io.Observe(t.Context())
	require.NoError(t, err)
	require.NoError(t, cfg.InstallAndVerify(t.Context(), 2, f.sidecar.Keys["2"]))
	after, err := f.io.Observe(t.Context())
	require.NoError(t, err)
	require.Equal(t, before, after, "healthy verification must not reinstall/invalidate")
}

func TestBootstrapEncryptionRuntimeIOPayloadUsesActualPendingWrapper(t *testing.T) {
	t.Parallel()
	f := newEncryptionRuntimeIOFixture(t)
	f.runtime.Registration.MarkRegistered(3, 9)
	f.sidecar.Active.Raft = 3
	f.sidecar.Keys["3"] = encryption.SidecarKey{Purpose: encryption.SidecarPurposeRaft, Wrapped: make([]byte, 48), LocalEpoch: 9}
	f.persist(t)
	_, err := f.io.Observe(t.Context())
	require.ErrorIs(t, err, encryptionwiring.ErrBootstrapEncryptionProjectionPending)
	gen, err := f.cfg.Domain.Admission.EncryptionFence()
	require.NoError(t, err)
	p, err := f.io.PayloadProjection(1, gen)
	require.NoError(t, err)
	require.Equal(t, uint32(2), p.RaftKeyID, "sidecar key 3 is not the installed encoding key")
	plain := encryptionwiring.RegistrationEntry(2, 123, 9)
	encoded, err := f.group.RaftPayloadWrap()(plain)
	require.NoError(t, err)
	decoded, err := p.Plaintext(encoded)
	require.NoError(t, err)
	require.Equal(t, plain, decoded)
	f.group.SetRaftPayloadWrap(nil)
	gen, err = f.cfg.Domain.Admission.EncryptionFence()
	require.NoError(t, err)
	_, err = f.io.PayloadProjection(1, gen)
	require.ErrorIs(t, err, encryptionwiring.ErrBootstrapEncryptionProjectionPending)
}

func TestBootstrapEncryptionRuntimeIOAdminUsesWrappedProposer(t *testing.T) {
	t.Parallel()
	f := newEncryptionRuntimeIOFixture(t)
	cfg := f.io.RecipeConfig(func(context.Context) (uint64, error) { return 40, nil }, func(ctx context.Context) error { return ctx.Err() })
	wrapped, err := f.cfg.Wrapper.Wrap(make([]byte, encryption.KeySize))
	require.NoError(t, err)
	_, err = cfg.Admin.RotateDEK(t.Context(), &pb.RotateDEKRequest{Purpose: pb.RotateDEKRequest_PURPOSE_STORAGE, NewDekId: 3, WrappedNewDek: wrapped, ProposerNodeId: 123, ProposerLocalEpoch: 7})
	require.NoError(t, err)
	require.Len(t, f.engine.calls, 1)
	env, err := encryption.DecodeEnvelope(f.engine.calls[0])
	require.NoError(t, err)
	require.Equal(t, uint32(2), env.KeyID)
}

func TestBootstrapEncryptionRuntimeIOCancellationAndBinding(t *testing.T) {
	t.Parallel()
	f := newEncryptionRuntimeIOFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := f.io.Observe(ctx)
	require.ErrorIs(t, err, context.Canceled)
	bad := f.cfg
	bad.NodeID = 124
	_, err = clusterbootstrap.NewBootstrapEncryptionRuntimeIO(bad)
	require.ErrorIs(t, err, encryptionwiring.ErrBootstrapEncryptionUnsupported)
	bad = f.cfg
	bad.Group = &kv.ShardGroup{Engine: f.engine, Store: f.cfg.Group.Store}
	_, err = clusterbootstrap.NewBootstrapEncryptionRuntimeIO(bad)
	require.ErrorIs(t, err, encryptionwiring.ErrBootstrapEncryptionUnsupported)
}

func TestBootstrapEncryptionRuntimeIORejectsStaleInstallWithoutOverwriting(t *testing.T) {
	t.Parallel()
	f := newEncryptionRuntimeIOFixture(t)
	before := f.group.RaftPayloadWrap()
	key := f.sidecar.Keys["2"]
	key.Wrapped = bytes.Repeat([]byte{7}, 48)
	cfg := f.io.RecipeConfig(func(context.Context) (uint64, error) { return 40, nil }, func(ctx context.Context) error { return ctx.Err() })
	require.ErrorIs(t, cfg.InstallAndVerify(t.Context(), 2, key), encryptionwiring.ErrBootstrapEncryptionProjectionPending)
	require.NotNil(t, before)
	// A durable newer key is not authority to overwrite an installer-owned
	// old wrapper from a stale observation. Only native apply installs keys.
	require.NoError(t, f.keys.Set(3, make([]byte, encryption.KeySize)))
	f.sidecar.Active.Raft = 3
	f.sidecar.Keys["3"] = encryption.SidecarKey{Purpose: encryption.SidecarPurposeRaft, Wrapped: make([]byte, 48), LocalEpoch: 9}
	f.persist(t)
	f.register(t, 3, 123, 9)
	require.ErrorIs(t, cfg.InstallAndVerify(t.Context(), 3, f.sidecar.Keys["3"]), encryptionwiring.ErrBootstrapEncryptionProjectionPending)
	f.runtime.Mu.Lock()
	require.Equal(t, uint32(2), f.runtime.ActiveRaftDEKID)
	f.runtime.Mu.Unlock()
}

func TestBootstrapEncryptionRuntimeIOPayloadRejectsStaleGenerationAndNonce(t *testing.T) {
	t.Parallel()
	f := newEncryptionRuntimeIOFixture(t)
	gen, err := f.cfg.Domain.Admission.EncryptionFence()
	require.NoError(t, err)
	_, err = f.io.PayloadProjection(2, gen)
	require.ErrorIs(t, err, clusterbootstrap.ErrBootstrapPayloadProjection)
	f.runtime.Registration.MarkRegistered(2, 9)
	_, err = f.io.PayloadProjection(1, gen)
	require.ErrorIs(t, err, clusterbootstrap.ErrBootstrapPayloadProjection)
	gen, err = f.cfg.Domain.Admission.EncryptionFence()
	require.NoError(t, err)
	f.runtime.Mu.Lock()
	f.runtime.NonceFactory = encryption.NewDeterministicNonceFactory(123, 10)
	f.runtime.Mu.Unlock()
	_, err = f.io.PayloadProjection(1, gen)
	require.ErrorIs(t, err, encryptionwiring.ErrBootstrapEncryptionUnsupported)
}

func TestBootstrapEncryptionRuntimeIORecipeOrderRepairsLaterRaftPurpose(t *testing.T) {
	t.Parallel()
	f := newEncryptionRuntimeIOFixture(t)
	f.group.SetRaftPayloadWrap(nil)
	config := f.io.RecipeConfig(func(context.Context) (uint64, error) { return 40, nil }, func(ctx context.Context) error { return ctx.Err() })
	r := &clusterbootstrap.BootstrapRotationRecipe{Config: config, Baseline: f.sidecar.Active}
	require.NoError(t, r.VerifyActive(t.Context(), f.sidecar), "storage verification must reach the later raft repair")
	_, err := f.io.Observe(t.Context())
	require.NoError(t, err)
}
