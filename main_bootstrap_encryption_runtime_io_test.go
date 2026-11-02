package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
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
	io     *bootstrapEncryptionRuntimeIO
	cfg    bootstrapEncryptionRuntimeIOConfig
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
	domain := bootstrapIngressFixture(t).domain
	w := encryptionWriteWiring{cache: encryption.NewStateCache(), cipher: f.runtime.cipher, raftEnvelope: f.runtime, epoch: 7, raftEpoch: 9}
	rt := &raftGroupRuntime{spec: groupSpec{id: 1}, engine: engine, store: f.in.localStore, bootstrapDomain: domain}
	cfg := bootstrapEncryptionRuntimeIOConfig{DefaultRuntime: rt, Group: f.group, Wiring: w, SidecarPath: f.in.sidecarPath, Wrapper: encryptionRuntimeIOTestKEK{}, Domain: domain, NodeID: 123, RotationRequested: true}
	io, err := newBootstrapEncryptionRuntimeIO(cfg)
	require.NoError(t, err)
	return &encryptionRuntimeIOFixture{bootstrapInstalledFixture: f, io: io, cfg: cfg, engine: engine}
}

func TestBootstrapEncryptionRuntimeIOObserveDoesNotMutate(t *testing.T) {
	f := newEncryptionRuntimeIOFixture(t)
	before, err := f.io.Observe(t.Context())
	require.NoError(t, err)
	after, err := f.io.Observe(t.Context())
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, bootstrapRotationRequestedUnproven, after.rotation)
	gen, err := f.cfg.Domain.admission.EncryptionFence()
	require.NoError(t, err)
	f.cfg.Wiring.cache.MarkRegistered(1)
	changed, err := f.cfg.Domain.admission.EncryptionFence()
	require.NoError(t, err)
	require.NotEqual(t, gen, changed, "constructor must bind storage async mutation hooks")
}

func TestBootstrapEncryptionRuntimeIORegistrationEpoch(t *testing.T) {
	f := newEncryptionRuntimeIOFixture(t)
	require.NoError(t, f.io.ValidateEpoch(1, 7))
	require.NoError(t, f.io.ValidateEpoch(2, 9))
	require.ErrorIs(t, f.io.ValidateEpoch(1, 9), encryption.ErrLocalEpochRollback)
	require.ErrorIs(t, f.io.ValidateEpoch(3, 7), errBootstrapEncryptionUnsupported)
}

func TestBootstrapEncryptionRuntimeIORejectsWrongActualMaterial(t *testing.T) {
	f := newEncryptionRuntimeIOFixture(t)
	key := f.sidecar.Keys["1"]
	key.Wrapped[16] = 1
	f.sidecar.Keys["1"] = key
	f.persist(t)
	_, err := f.io.Observe(t.Context())
	require.ErrorContains(t, err, "installed cipher material mismatch")
	require.NotErrorIs(t, err, errBootstrapEncryptionProjectionPending)
}

func TestBootstrapEncryptionRuntimeIORepairsWrapperOnlyOnce(t *testing.T) {
	f := newEncryptionRuntimeIOFixture(t)
	f.group.SetRaftPayloadWrap(nil)
	_, err := f.io.Observe(t.Context())
	require.ErrorIs(t, err, errBootstrapEncryptionProjectionPending)
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
	f := newEncryptionRuntimeIOFixture(t)
	f.runtime.registration.MarkRegistered(3, 9)
	f.sidecar.Active.Raft = 3
	f.sidecar.Keys["3"] = encryption.SidecarKey{Purpose: encryption.SidecarPurposeRaft, Wrapped: make([]byte, 48), LocalEpoch: 9}
	f.persist(t)
	_, err := f.io.Observe(t.Context())
	require.ErrorIs(t, err, errBootstrapEncryptionProjectionPending)
	gen, err := f.cfg.Domain.admission.EncryptionFence()
	require.NoError(t, err)
	p, err := f.io.PayloadProjection(1, gen)
	require.NoError(t, err)
	require.Equal(t, uint32(2), p.RaftKeyID, "sidecar key 3 is not the installed encoding key")
	plain := registrationEntry(2, 123, 9)
	encoded, err := f.group.RaftPayloadWrap()(plain)
	require.NoError(t, err)
	decoded, err := p.plaintext(encoded)
	require.NoError(t, err)
	require.Equal(t, plain, decoded)
	f.group.SetRaftPayloadWrap(nil)
	gen, err = f.cfg.Domain.admission.EncryptionFence()
	require.NoError(t, err)
	_, err = f.io.PayloadProjection(1, gen)
	require.ErrorIs(t, err, errBootstrapEncryptionProjectionPending)
}

func TestBootstrapEncryptionRuntimeIOAdminUsesWrappedProposer(t *testing.T) {
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
	f := newEncryptionRuntimeIOFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := f.io.Observe(ctx)
	require.ErrorIs(t, err, context.Canceled)
	bad := f.cfg
	bad.NodeID = 124
	_, err = newBootstrapEncryptionRuntimeIO(bad)
	require.ErrorIs(t, err, errBootstrapEncryptionUnsupported)
	bad = f.cfg
	bad.Group = &kv.ShardGroup{Engine: f.engine, Store: f.cfg.Group.Store}
	_, err = newBootstrapEncryptionRuntimeIO(bad)
	require.ErrorIs(t, err, errBootstrapEncryptionUnsupported)
}

func TestBootstrapEncryptionRuntimeIORejectsStaleInstallWithoutOverwriting(t *testing.T) {
	f := newEncryptionRuntimeIOFixture(t)
	before := f.group.RaftPayloadWrap()
	key := f.sidecar.Keys["2"]
	key.Wrapped = bytes.Repeat([]byte{7}, 48)
	cfg := f.io.RecipeConfig(func(context.Context) (uint64, error) { return 40, nil }, func(ctx context.Context) error { return ctx.Err() })
	require.ErrorIs(t, cfg.InstallAndVerify(t.Context(), 2, key), errBootstrapEncryptionProjectionPending)
	require.NotNil(t, before)
	// A durable newer key is not authority to overwrite an installer-owned
	// old wrapper from a stale observation. Only native apply installs keys.
	require.NoError(t, f.keys.Set(3, make([]byte, encryption.KeySize)))
	f.sidecar.Active.Raft = 3
	f.sidecar.Keys["3"] = encryption.SidecarKey{Purpose: encryption.SidecarPurposeRaft, Wrapped: make([]byte, 48), LocalEpoch: 9}
	f.persist(t)
	f.register(t, 3, 123, 9)
	require.ErrorIs(t, cfg.InstallAndVerify(t.Context(), 3, f.sidecar.Keys["3"]), errBootstrapEncryptionProjectionPending)
	f.runtime.mu.Lock()
	require.Equal(t, uint32(2), f.runtime.activeRaftDEKID)
	f.runtime.mu.Unlock()
}

func TestBootstrapEncryptionRuntimeIOPayloadRejectsStaleGenerationAndNonce(t *testing.T) {
	f := newEncryptionRuntimeIOFixture(t)
	gen, err := f.cfg.Domain.admission.EncryptionFence()
	require.NoError(t, err)
	_, err = f.io.PayloadProjection(2, gen)
	require.ErrorIs(t, err, errBootstrapPayloadProjection)
	f.runtime.registration.MarkRegistered(2, 9)
	_, err = f.io.PayloadProjection(1, gen)
	require.ErrorIs(t, err, errBootstrapPayloadProjection)
	gen, err = f.cfg.Domain.admission.EncryptionFence()
	require.NoError(t, err)
	f.runtime.mu.Lock()
	f.runtime.nonceFactory = encryption.NewDeterministicNonceFactory(123, 10)
	f.runtime.mu.Unlock()
	_, err = f.io.PayloadProjection(1, gen)
	require.ErrorIs(t, err, errBootstrapEncryptionUnsupported)
}

func TestBootstrapEncryptionRuntimeIORecipeOrderRepairsLaterRaftPurpose(t *testing.T) {
	f := newEncryptionRuntimeIOFixture(t)
	f.group.SetRaftPayloadWrap(nil)
	config := f.io.RecipeConfig(func(context.Context) (uint64, error) { return 40, nil }, func(ctx context.Context) error { return ctx.Err() })
	r := &bootstrapRotationRecipe{config: config, baseline: f.sidecar.Active}
	require.NoError(t, r.verifyActive(t.Context(), f.sidecar), "storage verification must reach the later raft repair")
	_, err := f.io.Observe(t.Context())
	require.NoError(t, err)
}
