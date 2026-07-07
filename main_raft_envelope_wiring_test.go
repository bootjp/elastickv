package main

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
)

func newTestRaftEnvelopeRuntimeDeps(t *testing.T, keyID uint32) (*encryption.Cipher, *encryption.DeterministicNonceFactory) {
	t.Helper()
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
	cipher, nonceFactory := newTestRaftEnvelopeRuntimeDeps(t, keyID)
	var cutover atomic.Uint64
	runtime, err := newRaftEnvelopeRuntime(cipher, nonceFactory, 1234, keyID, &cutover)
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
	cipher, nonceFactory := newTestRaftEnvelopeRuntimeDeps(t, 4)
	var cutover atomic.Uint64
	runtime, err := newRaftEnvelopeRuntime(cipher, nonceFactory, 0, 0, &cutover)
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
	cipher, nonceFactory := newTestRaftEnvelopeRuntimeDeps(t, keyID)
	var cutover atomic.Uint64
	runtime, err := newRaftEnvelopeRuntime(cipher, nonceFactory, 0, 0, &cutover)
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
