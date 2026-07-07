package main

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
)

func newTestRaftEnvelopeRuntimeDeps(t *testing.T) (*encryption.Cipher, *encryption.DeterministicNonceFactory) {
	t.Helper()
	const keyID uint32 = 4
	ks := encryption.NewKeystore()
	key := bytes.Repeat([]byte{byte(keyID)}, encryption.KeySize)
	if err := ks.Set(keyID, key); err != nil {
		t.Fatalf("keystore Set: %v", err)
	}
	cipher, err := encryption.NewCipher(ks)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	return cipher, encryption.NewDeterministicNonceFactory(0xCAFE, 7)
}

func TestRaftEnvelopeRuntime_StartupCutoverInstallsWrapOnAttach(t *testing.T) {
	t.Parallel()
	const keyID uint32 = 4
	cipher, nonceFactory := newTestRaftEnvelopeRuntimeDeps(t)
	var cutover atomic.Uint64
	runtime, err := newRaftEnvelopeRuntime(cipher, nonceFactory, 1234, keyID, &cutover, 7, nil)
	if err != nil {
		t.Fatalf("newRaftEnvelopeRuntime: %v", err)
	}
	if got := runtime.engineCutoverIndex(); got != 1234 {
		t.Fatalf("engineCutoverIndex = %d, want 1234", got)
	}
	sg := &kv.ShardGroup{}
	runtime.attachGroup(7, sg)
	wrap := sg.RaftPayloadWrap()
	if wrap == nil {
		t.Fatal("attached shard group has nil wrap after startup cutover")
	}
	plain := []byte("raft payload")
	wrapped, err := wrap(plain)
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}
	got, err := encryption.UnwrapRaftPayload(cipher, wrapped)
	if err != nil {
		t.Fatalf("UnwrapRaftPayload: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("unwrapped payload = %q, want %q", got, plain)
	}
}

func TestRaftEnvelopeRuntime_InactiveCutoverIsInert(t *testing.T) {
	t.Parallel()
	cipher, nonceFactory := newTestRaftEnvelopeRuntimeDeps(t)
	var cutover atomic.Uint64
	runtime, err := newRaftEnvelopeRuntime(cipher, nonceFactory, 0, 0, &cutover, 7, nil)
	if err != nil {
		t.Fatalf("newRaftEnvelopeRuntime: %v", err)
	}
	if got := runtime.engineCutoverIndex(); got != inertRaftEnvelopeCutoverIndex {
		t.Fatalf("engineCutoverIndex = %d, want inert sentinel", got)
	}
	sg := &kv.ShardGroup{}
	runtime.attachGroup(7, sg)
	if wrap := sg.RaftPayloadWrap(); wrap != nil {
		t.Fatalf("inactive cutover installed wrap %p, want nil", wrap)
	}
}

func TestRaftEnvelopeRuntime_InstallFromApplyPublishesWrap(t *testing.T) {
	t.Parallel()
	const keyID uint32 = 4
	cipher, nonceFactory := newTestRaftEnvelopeRuntimeDeps(t)
	var cutover atomic.Uint64
	runtime, err := newRaftEnvelopeRuntime(cipher, nonceFactory, 0, 0, &cutover, 7, nil)
	if err != nil {
		t.Fatalf("newRaftEnvelopeRuntime: %v", err)
	}
	sg := &kv.ShardGroup{}
	runtime.attachGroup(7, sg)
	if err := runtime.installFromApply(5678, keyID); err != nil {
		t.Fatalf("installFromApply: %v", err)
	}
	if got := runtime.engineCutoverIndex(); got != 5678 {
		t.Fatalf("engineCutoverIndex = %d, want 5678", got)
	}
	if wrap := sg.RaftPayloadWrap(); wrap == nil {
		t.Fatal("installFromApply did not publish wrap to attached group")
	}
}

func TestRaftEnvelopeRuntime_InstallRotatedRaftDEKRepublishesWrap(t *testing.T) {
	t.Parallel()
	const (
		oldKeyID  uint32 = 4
		newKeyID  uint32 = 9
		raftEpoch uint16 = 7
	)
	ks := encryption.NewKeystore()
	for _, keyID := range []uint32{oldKeyID, newKeyID} {
		key := bytes.Repeat([]byte{byte(keyID)}, encryption.KeySize)
		if err := ks.Set(keyID, key); err != nil {
			t.Fatalf("keystore Set(%d): %v", keyID, err)
		}
	}
	cipher, err := encryption.NewCipher(ks)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	var cutover atomic.Uint64
	runtime, err := newRaftEnvelopeRuntime(
		cipher,
		encryption.NewDeterministicNonceFactory(0xCAFE, 7),
		1234,
		oldKeyID,
		&cutover,
		raftEpoch,
		nil,
	)
	if err != nil {
		t.Fatalf("newRaftEnvelopeRuntime: %v", err)
	}
	sg := &kv.ShardGroup{}
	runtime.attachGroup(7, sg)
	if err := runtime.installRotatedRaftDEK(newKeyID); err != nil {
		t.Fatalf("installRotatedRaftDEK: %v", err)
	}
	wrap := sg.RaftPayloadWrap()
	if wrap == nil {
		t.Fatal("installRotatedRaftDEK did not publish wrap to attached group")
	}
	wrapped, err := wrap([]byte("raft payload"))
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}
	env, err := encryption.DecodeEnvelope(wrapped)
	if err != nil {
		t.Fatalf("DecodeEnvelope: %v", err)
	}
	if env.KeyID != newKeyID {
		t.Fatalf("wrapped key_id=%d, want rotated key_id=%d", env.KeyID, newKeyID)
	}
}

func TestRaftEnvelopeRuntime_WrapRequiresRaftWriterRegistration(t *testing.T) {
	t.Parallel()
	const keyID uint32 = 4
	const raftEpoch uint16 = 9
	cipher, nonceFactory := newTestRaftEnvelopeRuntimeDeps(t)
	var cutover atomic.Uint64
	gate := &raftRegistrationGate{}
	runtime, err := newRaftEnvelopeRuntime(cipher, nonceFactory, 0, 0, &cutover, raftEpoch, gate)
	if err != nil {
		t.Fatalf("newRaftEnvelopeRuntime: %v", err)
	}
	sg := &kv.ShardGroup{}
	runtime.attachGroup(7, sg)
	if err := runtime.installFromApply(5678, keyID); err != nil {
		t.Fatalf("installFromApply: %v", err)
	}
	wrap := sg.RaftPayloadWrap()
	if wrap == nil {
		t.Fatal("installFromApply did not publish wrap")
	}
	if _, err := wrap([]byte("raft payload")); !errors.Is(err, store.ErrWriterNotRegistered) {
		t.Fatalf("wrap before registration error = %v, want ErrWriterNotRegistered", err)
	}
	registrationPayload := append([]byte{fsmwire.OpRegistration}, fsmwire.EncodeRegistration(fsmwire.RegistrationPayload{
		DEKID:      keyID,
		FullNodeID: 0x1234,
		LocalEpoch: raftEpoch,
	})...)
	wrappedRegistration, err := wrap(registrationPayload)
	if err != nil {
		t.Fatalf("wrap registration before registration gate opens: %v", err)
	}
	unwrappedRegistration, err := encryption.UnwrapRaftPayload(cipher, wrappedRegistration)
	if err != nil {
		t.Fatalf("UnwrapRaftPayload registration: %v", err)
	}
	if !bytes.Equal(unwrappedRegistration, registrationPayload) {
		t.Fatalf("unwrapped registration = %x, want %x", unwrappedRegistration, registrationPayload)
	}
	gate.MarkRegistered(keyID, raftEpoch)
	wrapped, err := wrap([]byte("raft payload"))
	if err != nil {
		t.Fatalf("wrap after registration: %v", err)
	}
	if _, err := encryption.UnwrapRaftPayload(cipher, wrapped); err != nil {
		t.Fatalf("UnwrapRaftPayload after registration: %v", err)
	}
}

func TestRaftEnvelopeRuntime_InstallFromApplySeedsCommittedRegistration(t *testing.T) {
	t.Parallel()
	const keyID uint32 = 4
	const raftEpoch uint16 = 9
	cipher, nonceFactory := newTestRaftEnvelopeRuntimeDeps(t)
	var cutover atomic.Uint64
	gate := &raftRegistrationGate{}
	runtime, err := newRaftEnvelopeRuntime(cipher, nonceFactory, 0, 0, &cutover, raftEpoch, gate)
	if err != nil {
		t.Fatalf("newRaftEnvelopeRuntime: %v", err)
	}
	runtime.setRegistrationVerifier(func(dekID uint32, epoch uint16) bool {
		return dekID == keyID && epoch == raftEpoch
	})
	sg := &kv.ShardGroup{}
	runtime.attachGroup(7, sg)
	if err := runtime.installFromApply(5678, keyID); err != nil {
		t.Fatalf("installFromApply: %v", err)
	}
	wrap := sg.RaftPayloadWrap()
	if wrap == nil {
		t.Fatal("installFromApply did not publish wrap")
	}
	wrapped, err := wrap([]byte("raft payload"))
	if err != nil {
		t.Fatalf("wrap after verifier-seeded registration: %v", err)
	}
	if _, err := encryption.UnwrapRaftPayload(cipher, wrapped); err != nil {
		t.Fatalf("UnwrapRaftPayload: %v", err)
	}
}

func TestValidateRaftEnvelopeStartupScopeRefusesActiveMultiGroup(t *testing.T) {
	t.Parallel()
	err := validateRaftEnvelopeStartupScope(99, []groupSpec{{id: 1}, {id: 2}})
	if err == nil {
		t.Fatal("validateRaftEnvelopeStartupScope returned nil for active cutover with multiple groups")
	}
}

type nilChannelCutoverEngine struct {
	raftengine.Engine
}

func (nilChannelCutoverEngine) BeginCutoverBarrier() <-chan struct{} {
	return nil
}

func (nilChannelCutoverEngine) WaitInflightDrained(context.Context) error {
	return nil
}

func (nilChannelCutoverEngine) EndCutoverBarrier() {}

func TestRaftEnvelopeCutoverBarrier_BeginTreatsNilGroupChannelAsDrained(t *testing.T) {
	t.Parallel()
	runtime := &raftEnvelopeRuntime{
		groups: map[uint64]*kv.ShardGroup{
			7: {Engine: nilChannelCutoverEngine{}},
		},
	}
	done := (&raftEnvelopeCutoverBarrier{runtime: runtime}).Begin()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Begin blocked on nil group cutover channel")
	}
}

func TestRaftEnvelopeCutoverBarrier_ValidateCutoverScopeRefusesMultipleGroups(t *testing.T) {
	t.Parallel()
	runtime := &raftEnvelopeRuntime{
		groups: map[uint64]*kv.ShardGroup{
			7: {},
			8: {},
		},
	}
	if err := (&raftEnvelopeCutoverBarrier{runtime: runtime}).ValidateCutoverScope(); err == nil {
		t.Fatal("ValidateCutoverScope returned nil for multiple shard groups, want fail-closed error")
	}
}
