package main

import (
	"context"
	"io"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/encryptiontest"
	"github.com/bootjp/elastickv/internal/encryptionwiring"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// Encryption test fixtures also defined in internal/encryptionwiring's own
// tests; the wiring tests that stay in main need them too.
type recordingMembershipProposer struct {
	raftengine.Engine
	proposeCalls      int
	proposeAdminCalls int
}

func (p *recordingMembershipProposer) Propose(context.Context, []byte) (*raftengine.ProposalResult, error) {
	p.proposeCalls++
	return &raftengine.ProposalResult{CommitIndex: 1}, nil
}

func (p *recordingMembershipProposer) ProposeAdmin(context.Context, []byte) (*raftengine.ProposalResult, error) {
	p.proposeAdminCalls++
	return &raftengine.ProposalResult{CommitIndex: 1}, nil
}

func (p *recordingMembershipProposer) State() raftengine.State { return raftengine.StateLeader }

func (p *recordingMembershipProposer) Leader() raftengine.LeaderInfo {
	return raftengine.LeaderInfo{ID: "self", Address: "127.0.0.1:0"}
}

func (p *recordingMembershipProposer) VerifyLeader(context.Context) error { return nil }

func (p *recordingMembershipProposer) LinearizableRead(context.Context) (uint64, error) {
	return 0, nil
}

func (p *recordingMembershipProposer) Status() raftengine.Status {
	return raftengine.Status{State: raftengine.StateLeader}
}

func (p *recordingMembershipProposer) Configuration(context.Context) (raftengine.Configuration, error) {
	return raftengine.Configuration{}, nil
}

func (p *recordingMembershipProposer) Close() error { return nil }

// TestEncryption_E2E_BootstrapCutoverPutReadback is the Stage 6D-6c-3b
// end-to-end test: it drives the *production* main.go encryption wiring
// (buildEncryptionWriteWiring → store.WithEncryption + the shared
// StateCache) through the full §7.1 rollout — Bootstrap → cutover →
// Put → read-back — and proves that post-cutover writes land encrypted
// at rest while every version still reads back as correct plaintext.
//
// Bootstrap and the cutover are applied via the Applier (sharing the
// same keystore + StateCache the wiring built), which is exactly the
// FSM-apply effect of the BootstrapEncryption / EnableStorageEnvelope
// RPCs. The gRPC RPC + §4 capability fan-out path is covered by the
// 6D-6c-3a unit tests and the adapter EncryptionAdmin tests; this test
// validates the data path that 6D-6c-1 (shared cache) and 6D-6c-2
// (cipher/gate wiring) enable.
const (
	e2eStorageDEKID uint32 = 7
	e2eRaftDEKID    uint32 = 8
)

type bootstrapInstalledFixture struct {
	*bootstrapProjectionFixture
	keys    *encryption.Keystore
	runtime *encryptionwiring.RaftEnvelopeRuntime
	group   *kv.ShardGroup
}

func newBootstrapInstalledFixture(t *testing.T) *bootstrapInstalledFixture {
	t.Helper()
	f := newBootstrapProjectionFixture(t)
	keys := encryption.NewKeystore()
	require.NoError(t, keys.Set(1, make([]byte, encryption.KeySize)))
	require.NoError(t, keys.Set(2, make([]byte, encryption.KeySize)))
	cipher, err := encryption.NewCipher(keys)
	require.NoError(t, err)
	st, err := store.NewPebbleStore(filepath.Join(t.TempDir(), "installed"),
		store.WithEncryption(cipher, encryption.NewDeterministicNonceFactory(123, 7), func() (uint32, bool) { return 1, true }),
		store.WithStorageEnvelopeGate(func() bool { return true }),
		store.WithStorageRegistrationGate(func() bool { return true }))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	f.in.LocalStore = st
	f.registry, err = store.WriterRegistryFor(st)
	require.NoError(t, err)
	sink, ok := st.(interface {
		SaveEncryptionSnapshotCheckpoint(encryption.SnapshotCheckpoint) error
		SetDurableAppliedIndex(uint64) error
	})
	require.True(t, ok)
	f.sink = sink
	f.persist(t)
	require.NoError(t, f.sink.SetDurableAppliedIndex(40))
	f.register(t, 1, 123, 7)
	f.register(t, 2, 123, 9)
	gate := &encryptionwiring.RaftRegistrationGate{}
	gate.MarkRegistered(2, 9)
	runtime, err := encryptionwiring.NewRaftEnvelopeRuntime(cipher, encryption.NewDeterministicNonceFactory(123, 9), 20, 2, &atomic.Uint64{}, 9, 123, gate)
	require.NoError(t, err)
	group := &kv.ShardGroup{Store: st}
	runtime.AttachGroup(1, group)
	f.in = f.in.BindInstalledObservation(group, encryptionwiring.WriteWiring{RaftEnvelope: runtime}, f.in.RuntimeFence)
	f.in.RotationRequested = true
	return &bootstrapInstalledFixture{bootstrapProjectionFixture: f, keys: keys, runtime: runtime, group: group}
}

type bootstrapProjectionFixture struct {
	in       encryptionwiring.BootstrapEncryptionProjectionInput
	sidecar  *encryption.Sidecar
	registry encryption.WriterRegistryStore
	sink     interface {
		SaveEncryptionSnapshotCheckpoint(encryption.SnapshotCheckpoint) error
		SetDurableAppliedIndex(uint64) error
	}
}

func newBootstrapProjectionFixture(t *testing.T) *bootstrapProjectionFixture {
	t.Helper()
	st, err := store.NewPebbleStore(filepath.Join(t.TempDir(), "db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	reg, err := store.WriterRegistryFor(st)
	require.NoError(t, err)
	sink, ok := st.(interface {
		SaveEncryptionSnapshotCheckpoint(encryption.SnapshotCheckpoint) error
		SetDurableAppliedIndex(uint64) error
	})
	require.True(t, ok)
	f := &bootstrapProjectionFixture{
		in: encryptionwiring.BootstrapEncryptionProjectionInput{
			RuntimeFence: func() (uint64, error) { return 1, nil },
			Configured:   true, SidecarPath: filepath.Join(t.TempDir(), "keys.json"),
			LocalStore: st, FullNodeID: 123, StorageEpoch: 7, RaftEpoch: 9,
			StorageWrapperInstalled: func(id uint32, epoch uint16) bool { return id == 1 && epoch == 7 },
			RaftWrapperInstalled:    func(id uint32, epoch uint16) bool { return id == 2 && epoch == 9 },
		},
		sidecar: &encryption.Sidecar{
			Version: encryption.SidecarVersion, RaftAppliedIndex: 30,
			StorageEnvelopeActive: true, StorageEnvelopeCutoverIndex: 10, RaftEnvelopeCutoverIndex: 20,
			Active: encryption.ActiveKeys{Storage: 1, Raft: 2},
			Keys: map[string]encryption.SidecarKey{
				"1": {Purpose: encryption.SidecarPurposeStorage, Wrapped: []byte{1}, LocalEpoch: 7},
				"2": {Purpose: encryption.SidecarPurposeRaft, Wrapped: []byte{2}, LocalEpoch: 9},
			},
		},
		registry: reg, sink: sink,
	}
	f.persist(t)
	require.NoError(t, sink.SetDurableAppliedIndex(40))
	f.register(t, 1, 123, 7)
	f.register(t, 2, 123, 9)
	return f
}

func (f *bootstrapProjectionFixture) persist(t *testing.T) {
	t.Helper()
	require.NoError(t, encryption.WriteSidecar(f.in.SidecarPath, f.sidecar))
	cp, err := encryption.SidecarCheckpoint(f.sidecar)
	require.NoError(t, err)
	require.NoError(t, f.sink.SaveEncryptionSnapshotCheckpoint(cp))
}

func (f *bootstrapProjectionFixture) register(t *testing.T, id uint32, node uint64, epoch uint16) {
	t.Helper()
	require.NoError(t, f.registry.SetRegistryRow(encryption.RegistryKey(id, encryption.NodeID16(node)),
		encryption.EncodeRegistryValue(encryption.RegistryValue{
			FullNodeID: node, FirstSeenLocalEpoch: epoch, LastSeenLocalEpoch: epoch,
		})))
}

// testRegDEKID is the storage DEK id used across the registration
// tests (the wiringFor fixtures activate the same id).
const testRegDEKID uint32 = 7

func writeRegistryRow(t *testing.T, st store.MVCCStore, fullNodeID uint64, lastSeen uint16) {
	t.Helper()
	reg, err := store.WriterRegistryFor(st)
	if err != nil {
		t.Fatalf("WriterRegistryFor: %v", err)
	}
	val := encryption.EncodeRegistryValue(encryption.RegistryValue{
		FullNodeID: fullNodeID, FirstSeenLocalEpoch: lastSeen, LastSeenLocalEpoch: lastSeen,
	})
	if err := reg.SetRegistryRow(encryption.RegistryKey(testRegDEKID, encryption.NodeID16(fullNodeID)), val); err != nil {
		t.Fatalf("SetRegistryRow: %v", err)
	}
}

type stubStartupCoordinator struct {
	dispatches   atomic.Int32
	nextCalls    atomic.Int32
	nextAfter    atomic.Int32
	recoverCalls atomic.Int32
	clock        *kv.HLC
	nextErr      error
	nextAfterErr error
	recoverErr   error
}

func (s *stubStartupCoordinator) Dispatch(context.Context, *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	s.dispatches.Add(1)
	return &kv.CoordinateResponse{}, nil
}

func (s *stubStartupCoordinator) IsLeader() bool { return true }

func (s *stubStartupCoordinator) VerifyLeader(context.Context) error { return nil }

func (s *stubStartupCoordinator) LinearizableRead(context.Context) (uint64, error) { return 1, nil }

func (s *stubStartupCoordinator) RaftLeader() string { return "n1" }

func (s *stubStartupCoordinator) IsLeaderForKey([]byte) bool { return true }

func (s *stubStartupCoordinator) VerifyLeaderForKey(context.Context, []byte) error { return nil }

func (s *stubStartupCoordinator) RaftLeaderForKey([]byte) string { return "n1" }

func (s *stubStartupCoordinator) Clock() *kv.HLC {
	if s.clock == nil {
		s.clock = kv.NewHLC()
	}
	return s.clock
}

func (s *stubStartupCoordinator) Next(context.Context) (uint64, error) {
	s.nextCalls.Add(1)
	return 101, s.nextErr
}

func (s *stubStartupCoordinator) NextAfter(_ context.Context, min uint64) (uint64, error) {
	s.nextAfter.Add(1)
	return min + 1, s.nextAfterErr
}

func (s *stubStartupCoordinator) RecoverHLCLease(context.Context) error {
	s.recoverCalls.Add(1)
	return s.recoverErr
}

type fakeStartupKEK struct{}

func (fakeStartupKEK) Wrap(dek []byte) ([]byte, error) {
	if len(dek) != encryption.KeySize {
		return nil, errors.New("bad dek length")
	}
	out := make([]byte, 1, 1+len(dek))
	out[0] = 0xAA
	out = append(out, dek...)
	return out, nil
}

func (fakeStartupKEK) Unwrap(wrapped []byte) ([]byte, error) { return wrapped, nil }

func (fakeStartupKEK) Name() string { return "fake" }

type rotateStartupLeader struct {
	raftengine.Proposer
}

func (rotateStartupLeader) State() raftengine.State { return raftengine.StateLeader }

func (rotateStartupLeader) Leader() raftengine.LeaderInfo { return raftengine.LeaderInfo{} }

func (rotateStartupLeader) VerifyLeader(context.Context) error { return nil }

func (rotateStartupLeader) LinearizableRead(context.Context) (uint64, error) {
	return 0, nil
}

type stubRestoredCutoverStateMachine struct {
	cutover uint64
}

func (s *stubRestoredCutoverStateMachine) Apply([]byte) any { return nil }

func (s *stubRestoredCutoverStateMachine) Snapshot() (raftengine.Snapshot, error) {
	return nil, nil
}

func (s *stubRestoredCutoverStateMachine) Restore(io.Reader) error { return nil }

func (s *stubRestoredCutoverStateMachine) RestoredCutover() uint64 {
	return s.cutover
}

// writeMinimalSidecar writes a valid §5.1 sidecar with the
// supplied RaftAppliedIndex into a freshly created temp dir, and
// returns the sidecar path.
func writeMinimalSidecar(t *testing.T, raftAppliedIdx uint64) string {
	t.Helper()
	sc := &encryption.Sidecar{
		Version:          encryption.SidecarVersion,
		RaftAppliedIndex: raftAppliedIdx,
		Keys:             map[string]encryption.SidecarKey{},
	}
	return encryptiontest.WriteSidecar(t, sc)
}

func writeRaftCutoverSidecarForStartup(t *testing.T, activeRaft uint32, localEpoch uint16, cutover uint64) string {
	t.Helper()
	sc := &encryption.Sidecar{
		Version:                  encryption.SidecarVersion,
		RaftAppliedIndex:         cutover,
		StorageEnvelopeActive:    true,
		RaftEnvelopeCutoverIndex: cutover,
		Active:                   encryption.ActiveKeys{Storage: testRegDEKID, Raft: activeRaft},
		Keys: map[string]encryption.SidecarKey{
			strconv.FormatUint(uint64(testRegDEKID), 10): {
				Purpose:    encryption.SidecarPurposeStorage,
				Wrapped:    []byte("wrapped-storage"),
				Created:    "2026-07-07T00:00:00Z",
				LocalEpoch: localEpoch,
			},
			strconv.FormatUint(uint64(activeRaft), 10): {
				Purpose: encryption.SidecarPurposeRaft,
				Wrapped: []byte("wrapped-raft"),
				Created: "2026-07-07T00:00:00Z",
			},
		},
	}
	return encryptiontest.WriteSidecar(t, sc)
}

func writeActiveStorageSidecarForStartup(t *testing.T, localEpoch uint16) string {
	t.Helper()
	sc := &encryption.Sidecar{
		Version:               encryption.SidecarVersion,
		RaftAppliedIndex:      1,
		StorageEnvelopeActive: true,
		Active:                encryption.ActiveKeys{Storage: testRegDEKID},
		Keys: map[string]encryption.SidecarKey{
			"7": {
				Purpose:    encryption.SidecarPurposeStorage,
				Wrapped:    []byte("wrapped"),
				Created:    "2026-07-07T00:00:00Z",
				LocalEpoch: localEpoch,
			},
		},
	}
	return encryptiontest.WriteSidecar(t, sc)
}
