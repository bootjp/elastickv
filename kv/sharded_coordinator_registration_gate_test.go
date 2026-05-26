package kv

import (
	"context"
	"errors"
	"testing"
	"time"
)

// mutatingGroup is an OperationGroup with one PUT (a write that would
// land encrypted once the envelope is active).
func mutatingGroup() *OperationGroup[OP] {
	return &OperationGroup[OP]{Elems: []*Elem[OP]{{Op: Put, Key: []byte("k"), Value: []byte("v")}}}
}

// readOnlyTxnGroup models a read-only transaction: ReadKeys set, no
// mutating Elems. Must stay ungated.
func readOnlyTxnGroup() *OperationGroup[OP] {
	return &OperationGroup[OP]{IsTxn: true, ReadKeys: [][]byte{[]byte("k")}}
}

func activeGatePredicates() (func() bool, func() (uint32, bool)) {
	return func() bool { return true }, func() (uint32, bool) { return 7, true }
}

func TestAwaitRegistration_NilGate_Ungated(t *testing.T) {
	t.Parallel()
	c := &ShardedCoordinator{}
	if err := c.awaitRegistration(context.Background(), mutatingGroup()); err != nil {
		t.Fatalf("nil gate should be ungated, got %v", err)
	}
}

func TestAwaitRegistration_NilBarrier_Ungated(t *testing.T) {
	t.Parallel()
	envActive, activeKey := activeGatePredicates()
	c := &ShardedCoordinator{registrationGate: &RegistrationGate{
		Barrier: nil, StorageEnvelopeActive: envActive, ActiveStorageKeyID: activeKey,
	}}
	if err := c.awaitRegistration(context.Background(), mutatingGroup()); err != nil {
		t.Fatalf("nil barrier should be ungated, got %v", err)
	}
}

func TestAwaitRegistration_ClosedBarrier_FastPath(t *testing.T) {
	t.Parallel()
	ch := make(chan struct{})
	close(ch)
	envActive, activeKey := activeGatePredicates()
	c := &ShardedCoordinator{registrationGate: &RegistrationGate{
		Barrier: ch, StorageEnvelopeActive: envActive, ActiveStorageKeyID: activeKey,
	}}
	if err := c.awaitRegistration(context.Background(), mutatingGroup()); err != nil {
		t.Fatalf("closed barrier should be ungated, got %v", err)
	}
}

func TestAwaitRegistration_OpenBarrier_ReadOnly_Ungated(t *testing.T) {
	t.Parallel()
	ch := make(chan struct{}) // open
	envActive, activeKey := activeGatePredicates()
	c := &ShardedCoordinator{registrationGate: &RegistrationGate{
		Barrier: ch, StorageEnvelopeActive: envActive, ActiveStorageKeyID: activeKey,
	}}
	// Read-only txn must not block even with an open barrier.
	if err := c.awaitRegistration(context.Background(), readOnlyTxnGroup()); err != nil {
		t.Fatalf("read-only group should be ungated, got %v", err)
	}
}

func TestAwaitRegistration_OpenBarrier_EnvelopeInactive_Ungated(t *testing.T) {
	t.Parallel()
	ch := make(chan struct{}) // open
	c := &ShardedCoordinator{registrationGate: &RegistrationGate{
		Barrier:               ch,
		StorageEnvelopeActive: func() bool { return false }, // pre-cutover
		ActiveStorageKeyID:    func() (uint32, bool) { return 7, true },
	}}
	// Pre-cutover writes are cleartext → ungated even with open barrier.
	if err := c.awaitRegistration(context.Background(), mutatingGroup()); err != nil {
		t.Fatalf("envelope-inactive write should be ungated, got %v", err)
	}
}

func TestAwaitRegistration_OpenBarrier_NoActiveDEK_Ungated(t *testing.T) {
	t.Parallel()
	ch := make(chan struct{}) // open
	c := &ShardedCoordinator{registrationGate: &RegistrationGate{
		Barrier:               ch,
		StorageEnvelopeActive: func() bool { return true },
		ActiveStorageKeyID:    func() (uint32, bool) { return 0, false }, // not bootstrapped
	}}
	if err := c.awaitRegistration(context.Background(), mutatingGroup()); err != nil {
		t.Fatalf("no-active-DEK write should be ungated, got %v", err)
	}
}

func TestAwaitRegistration_OpenBarrier_BlocksThenReleasesOnCommit(t *testing.T) {
	t.Parallel()
	ch := make(chan struct{}) // open
	envActive, activeKey := activeGatePredicates()
	c := &ShardedCoordinator{registrationGate: &RegistrationGate{
		Barrier: ch, StorageEnvelopeActive: envActive, ActiveStorageKeyID: activeKey,
	}}
	done := make(chan error, 1)
	go func() { done <- c.awaitRegistration(context.Background(), mutatingGroup()) }()

	// Must still be blocked: nothing on done yet.
	select {
	case err := <-done:
		t.Fatalf("expected block on open barrier, returned early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	// Registration commits → close barrier → write proceeds.
	close(ch)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("after commit, expected ungated, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write did not unblock after barrier close")
	}
}

func TestAwaitRegistration_OpenBarrier_CtxCancelFailsClosed(t *testing.T) {
	t.Parallel()
	ch := make(chan struct{}) // open, never closed
	envActive, activeKey := activeGatePredicates()
	c := &ShardedCoordinator{registrationGate: &RegistrationGate{
		Barrier: ch, StorageEnvelopeActive: envActive, ActiveStorageKeyID: activeKey,
	}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.awaitRegistration(ctx, mutatingGroup()) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected ctx.Canceled fail-closed, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write did not fail-closed on ctx cancel")
	}
}
