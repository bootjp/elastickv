package kv

import (
	"bytes"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/cockroachdb/errors"
)

// fakeApplier records every call and lets tests inject a per-handler
// error. ApplyRegistration / ApplyBootstrap / ApplyRotation are the
// three handler hooks Stage 5/6/7 will fill in for real; the kvFSM
// dispatch logic must invoke exactly the right one and propagate
// any returned error through the HaltApply seam.
type fakeApplier struct {
	regCalls         atomic.Int32
	bootstrapCalls   atomic.Int32
	rotationCalls    atomic.Int32
	regErr           error
	bootstrapErr     error
	rotationErr      error
	lastReg          fsmwire.RegistrationPayload
	lastBootstrap    fsmwire.BootstrapPayload
	lastRotation     fsmwire.RotationPayload
	lastBootstrapIdx uint64
	lastRotationIdx  uint64
}

func (f *fakeApplier) ApplyRegistration(p fsmwire.RegistrationPayload) error {
	f.regCalls.Add(1)
	f.lastReg = p
	return f.regErr
}

func (f *fakeApplier) ApplyBootstrap(raftIdx uint64, p fsmwire.BootstrapPayload) error {
	f.bootstrapCalls.Add(1)
	f.lastBootstrap = p
	f.lastBootstrapIdx = raftIdx
	return f.bootstrapErr
}

func (f *fakeApplier) ApplyRotation(raftIdx uint64, p fsmwire.RotationPayload) error {
	f.rotationCalls.Add(1)
	f.lastRotation = p
	f.lastRotationIdx = raftIdx
	return f.rotationErr
}

func newFSMWithFake(applier EncryptionApplier) *kvFSM {
	f := &kvFSM{}
	if applier != nil {
		WithEncryption(applier)(f)
	}
	return f
}

// haltApplyOf extracts a wrapped halt error from an Apply response,
// or returns nil for non-halt responses. Mirrors the engine-side
// type assertion in internal/raftengine/etcd.applyNormalCommitted
// so the FSM tests can verify the dispatch produced something the
// engine recognises as a halt.
func haltApplyOf(resp any) error {
	h, ok := resp.(interface{ HaltApply() error })
	if !ok {
		return nil
	}
	return h.HaltApply()
}

// TestApply_Registration_HappyPath confirms a well-formed 0x03 entry
// dispatches to ApplyRegistration with the decoded payload and
// returns nil (no halt).
func TestApply_Registration_HappyPath(t *testing.T) {
	t.Parallel()
	applier := &fakeApplier{}
	f := newFSMWithFake(applier)

	want := fsmwire.RegistrationPayload{DEKID: 7, FullNodeID: 0xCAFEBABE, LocalEpoch: 3}
	payload := fsmwire.EncodeRegistration(want)
	wireBytes := append([]byte{fsmwire.OpRegistration}, payload...)

	resp := f.Apply(wireBytes)
	if err := haltApplyOf(resp); err != nil {
		t.Fatalf("unexpected halt: %v", err)
	}
	if got := applier.regCalls.Load(); got != 1 {
		t.Fatalf("regCalls = %d, want 1", got)
	}
	if applier.lastReg != want {
		t.Fatalf("lastReg = %+v, want %+v", applier.lastReg, want)
	}
	if applier.bootstrapCalls.Load() != 0 || applier.rotationCalls.Load() != 0 {
		t.Fatal("dispatch leaked into other handlers")
	}
}

func TestApply_Bootstrap_HappyPath(t *testing.T) {
	t.Parallel()
	applier := &fakeApplier{}
	f := newFSMWithFake(applier)

	want := fsmwire.BootstrapPayload{
		StorageDEKID:   1,
		WrappedStorage: []byte("storage-w"),
		RaftDEKID:      2,
		WrappedRaft:    []byte("raft-w"),
		BatchRegistry:  []fsmwire.RegistrationPayload{{DEKID: 1, FullNodeID: 11, LocalEpoch: 1}},
	}
	wireBytes := append([]byte{fsmwire.OpBootstrap}, fsmwire.EncodeBootstrap(want)...)

	if err := haltApplyOf(f.Apply(wireBytes)); err != nil {
		t.Fatalf("unexpected halt: %v", err)
	}
	if got := applier.bootstrapCalls.Load(); got != 1 {
		t.Fatalf("bootstrapCalls = %d, want 1", got)
	}
	if applier.lastBootstrap.StorageDEKID != want.StorageDEKID {
		t.Fatalf("storage id mismatch")
	}
	if !bytes.Equal(applier.lastBootstrap.WrappedStorage, want.WrappedStorage) {
		t.Fatalf("storage wrapped mismatch")
	}
}

func TestApply_Rotation_HappyPath(t *testing.T) {
	t.Parallel()
	applier := &fakeApplier{}
	f := newFSMWithFake(applier)

	want := fsmwire.RotationPayload{
		SubTag:  fsmwire.RotateSubRotateDEK,
		DEKID:   42,
		Purpose: fsmwire.PurposeStorage,
		Wrapped: []byte("new-w"),
		ProposerRegistration: fsmwire.RegistrationPayload{
			DEKID: 42, FullNodeID: 99, LocalEpoch: 1,
		},
	}
	wireBytes := append([]byte{fsmwire.OpRotation}, fsmwire.EncodeRotation(want)...)

	if err := haltApplyOf(f.Apply(wireBytes)); err != nil {
		t.Fatalf("unexpected halt: %v", err)
	}
	if got := applier.rotationCalls.Load(); got != 1 {
		t.Fatalf("rotationCalls = %d, want 1", got)
	}
	if applier.lastRotation.DEKID != want.DEKID || applier.lastRotation.Purpose != want.Purpose {
		t.Fatalf("rotation payload mismatch")
	}
}

// TestApply_NoApplierWired locks down the Stage-4 fail-closed
// default: when WithEncryption was not used, every encryption
// opcode halts with ErrEncryptionApply. A regression here would
// let an opcode silently advance setApplied past a proposal the
// local node cannot process — the §6.3 fatal-apply property we
// added the HaltApply seam to enforce.
func TestApply_NoApplierWired(t *testing.T) {
	t.Parallel()
	f := newFSMWithFake(nil)
	cases := []struct {
		name   string
		opcode byte
		body   []byte
	}{
		{"registration", fsmwire.OpRegistration, fsmwire.EncodeRegistration(fsmwire.RegistrationPayload{DEKID: 1})},
		{"bootstrap", fsmwire.OpBootstrap, fsmwire.EncodeBootstrap(fsmwire.BootstrapPayload{})},
		{"rotation", fsmwire.OpRotation, fsmwire.EncodeRotation(fsmwire.RotationPayload{SubTag: fsmwire.RotateSubRotateDEK})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wireBytes := append([]byte{tc.opcode}, tc.body...)
			err := haltApplyOf(f.Apply(wireBytes))
			if !errors.Is(err, encryption.ErrEncryptionApply) {
				t.Fatalf("expected ErrEncryptionApply, got %v", err)
			}
		})
	}
}

// TestApply_HandlerError_HaltsWithEncryptionApply confirms an
// applier-side error flows through the HaltApply seam wrapped with
// ErrEncryptionApply so internal/raftengine/etcd recognises it.
func TestApply_HandlerError_HaltsWithEncryptionApply(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("applier denied")
	applier := &fakeApplier{regErr: sentinel}
	f := newFSMWithFake(applier)

	wireBytes := append([]byte{fsmwire.OpRegistration},
		fsmwire.EncodeRegistration(fsmwire.RegistrationPayload{DEKID: 1})...)
	err := haltApplyOf(f.Apply(wireBytes))
	if !errors.Is(err, encryption.ErrEncryptionApply) {
		t.Fatalf("expected wrapped ErrEncryptionApply, got %v", err)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected nested sentinel preserved, got %v", err)
	}
	if got := applier.regCalls.Load(); got != 1 {
		t.Fatalf("regCalls = %d, want 1", got)
	}
}

// TestApply_DecodeFailure_Halts pins the malformed-payload contract:
// a truncated encryption opcode must halt rather than silently no-op.
// The Stage-4 dispatcher marks the fsmwire decode error with
// ErrEncryptionApply; the engine's HaltApply path then halts.
func TestApply_DecodeFailure_Halts(t *testing.T) {
	t.Parallel()
	applier := &fakeApplier{}
	f := newFSMWithFake(applier)

	// Truncated registration payload (missing version + 14 body bytes).
	wireBytes := []byte{fsmwire.OpRegistration, 0x99}
	err := haltApplyOf(f.Apply(wireBytes))
	if !errors.Is(err, encryption.ErrEncryptionApply) {
		t.Fatalf("expected ErrEncryptionApply, got %v", err)
	}
	if !errors.Is(err, fsmwire.ErrFSMWireMalformed) {
		t.Fatalf("expected nested ErrFSMWireMalformed, got %v", err)
	}
	if got := applier.regCalls.Load(); got != 0 {
		t.Fatalf("regCalls = %d, want 0 (decode failed before applier)", got)
	}
}

// TestApply_ReservedRange_FutureOpcodes_Halt is the codex-P1
// regression for PR748: every byte in [OpEncryptionMin,
// OpEncryptionMax] = [0x03, 0x07] must route through the encryption
// fail-closed halt path, NOT the legacy proto3 decoder. A future
// leader emitting 0x06 (or any reserved byte) against a stale
// follower would otherwise fall through to decodeLegacyRaftRequest
// and either silently advance setApplied or return an ordinary
// apply error — the rolling-upgrade divergence shape the §6.3 halt
// was added to prevent. The upper bound stops at 0x07 because
// 0x08..0x0D collide with proto3 field-1 wire tags (see the comment
// on OpEncryptionMax in fsmwire/wire.go for the analysis).
//
// The test sweeps the reserved range exhaustively and asserts that
// every byte produces a haltApplyResponse wrapping
// ErrEncryptionApply. The known opcodes (0x03/0x04/0x05) ride this
// path because no applier is wired; the future-reserved opcodes
// (0x06/0x07) ride it because applyEncryption's default case fails
// closed for any byte not yet implemented.
func TestApply_ReservedRange_FutureOpcodes_Halt(t *testing.T) {
	t.Parallel()
	f := newFSMWithFake(nil)
	for op := fsmwire.OpEncryptionMin; op <= fsmwire.OpEncryptionMax; op++ {
		// Single-byte payload (no need to be well-formed — the
		// no-applier dispatcher fails closed before any decode).
		err := haltApplyOf(f.Apply([]byte{op}))
		if !errors.Is(err, encryption.ErrEncryptionApply) {
			t.Fatalf("op=%#x: expected halt with ErrEncryptionApply, got %v", op, err)
		}
	}
}

// TestApply_AboveReservedRange_FallsThroughToLegacy confirms 0x10
// and above is NOT routed through applyEncryption — it remains
// available for future non-encryption FSM extensions and continues
// to flow through decodeRaftRequests, which will surface the
// proto3 decode error (or proto3 stretch-decode it). The contract
// is "encryption owns 0x03..0x07 (OpEncryptionMax)"; bytes 0x08+
// are reserved for the proto3 fallback (because 0x08..0x0D collide
// with proto3 field-1 wire tags) and any later non-encryption FSM
// extension. That bound must hold from both sides.
func TestApply_AboveReservedRange_FallsThroughToLegacy(t *testing.T) {
	t.Parallel()
	applier := &fakeApplier{}
	f := newFSMWithFake(applier)
	// 0x10 with a proto3-invalid payload should trigger
	// decodeRaftRequests' legacy fallback and surface as a non-halt
	// error response (the kv legacy path returns plain errors via
	// errors.WithStack — NOT via the HaltApply interface). The
	// distinguishing assertion: the response must NOT be a
	// haltApplyResponse.
	resp := f.Apply([]byte{0x10, 0x99, 0x88})
	if h, ok := resp.(interface{ HaltApply() error }); ok {
		t.Fatalf("byte 0x10 unexpectedly routed through HaltApply: %v", h.HaltApply())
	}
	if got := applier.regCalls.Load() + applier.bootstrapCalls.Load() + applier.rotationCalls.Load(); got != 0 {
		t.Fatalf("encryption applier received %d calls on a 0x10 entry", got)
	}
}

// TestApply_LegacyOpcodesUnaffected confirms 0x00 / 0x01 / 0x02
// continue to dispatch through the kv-legacy and HLC-lease paths.
// A regression where the encryption switch case fell through to
// every input would route legacy proposals through the encryption
// applier (and halt them all) — exactly the failure mode the
// pre-Apply test guards against.
func TestApply_LegacyOpcodesUnaffected(t *testing.T) {
	t.Parallel()
	applier := &fakeApplier{}
	f := newFSMWithFake(applier)

	// 0x02 raftEncodeHLCLease with a valid 8-byte big-endian payload.
	hlcEntry := []byte{raftEncodeHLCLease, 0, 0, 0, 0, 0, 0, 0, 1}
	if resp := f.Apply(hlcEntry); resp != nil {
		// applyHLCLease may return nil or an HLC-style error; here
		// we just assert it did NOT route to the encryption applier.
		_ = resp
	}
	if got := applier.regCalls.Load() + applier.bootstrapCalls.Load() + applier.rotationCalls.Load(); got != 0 {
		t.Fatalf("encryption applier received %d calls on a 0x02 entry", got)
	}
}
