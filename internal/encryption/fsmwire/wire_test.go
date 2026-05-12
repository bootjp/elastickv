package fsmwire_test

import (
	"bytes"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/cockroachdb/errors"
)

func TestRegistration_RoundTrip(t *testing.T) {
	t.Parallel()
	cases := []fsmwire.RegistrationPayload{
		{},
		{DEKID: 1, FullNodeID: 1, LocalEpoch: 1},
		{DEKID: 0xCAFEBABE, FullNodeID: 0xDEADBEEFCAFEBABE, LocalEpoch: 0xABCD},
		{DEKID: 0xFFFFFFFF, FullNodeID: ^uint64(0), LocalEpoch: 0xFFFF},
	}
	for _, want := range cases {
		got, err := fsmwire.DecodeRegistration(fsmwire.EncodeRegistration(want))
		if err != nil {
			t.Fatalf("decode %+v: %v", want, err)
		}
		if got != want {
			t.Fatalf("round-trip mismatch: got %+v want %+v", got, want)
		}
	}
}

func TestRegistration_ByteLayoutPin(t *testing.T) {
	t.Parallel()
	got := fsmwire.EncodeRegistration(fsmwire.RegistrationPayload{
		DEKID:      0x01020304,
		FullNodeID: 0x05060708090A0B0C,
		LocalEpoch: 0x0D0E,
	})
	want := []byte{
		fsmwire.WireVersionV1,
		0x01, 0x02, 0x03, 0x04, // DEKID
		0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, // FullNodeID
		0x0D, 0x0E, // LocalEpoch
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("layout drift:\n got  %x\n want %x", got, want)
	}
}

func TestRegistration_RejectsMalformed(t *testing.T) {
	t.Parallel()
	for _, l := range []int{0, 1, 14, 16, 100} {
		_, err := fsmwire.DecodeRegistration(make([]byte, l))
		// l == 15 is the only valid length; anything else must
		// fail with ErrFSMWireMalformed (or, for l == 15 with
		// version != V1, ErrFSMWireVersion — covered separately).
		if !errors.Is(err, fsmwire.ErrFSMWireMalformed) {
			t.Fatalf("len=%d: expected ErrFSMWireMalformed, got %v", l, err)
		}
	}
}

func TestRegistration_RejectsBadVersion(t *testing.T) {
	t.Parallel()
	raw := fsmwire.EncodeRegistration(fsmwire.RegistrationPayload{DEKID: 1})
	raw[0] = 0x99 // tamper version byte
	_, err := fsmwire.DecodeRegistration(raw)
	if !errors.Is(err, fsmwire.ErrFSMWireVersion) {
		t.Fatalf("expected ErrFSMWireVersion, got %v", err)
	}
}

func TestBootstrap_RoundTrip(t *testing.T) {
	t.Parallel()
	cases := []fsmwire.BootstrapPayload{
		{
			StorageDEKID:   1,
			WrappedStorage: []byte("storage-wrapped"),
			RaftDEKID:      2,
			WrappedRaft:    []byte("raft-wrapped"),
		},
		{
			StorageDEKID:   0xAAAAAAAA,
			WrappedStorage: bytes.Repeat([]byte{0xAA}, 60),
			RaftDEKID:      0xBBBBBBBB,
			WrappedRaft:    bytes.Repeat([]byte{0xBB}, 60),
			BatchRegistry: []fsmwire.RegistrationPayload{
				{DEKID: 0xAAAAAAAA, FullNodeID: 11, LocalEpoch: 1},
				{DEKID: 0xAAAAAAAA, FullNodeID: 22, LocalEpoch: 1},
				{DEKID: 0xBBBBBBBB, FullNodeID: 11, LocalEpoch: 1},
				{DEKID: 0xBBBBBBBB, FullNodeID: 22, LocalEpoch: 1},
			},
		},
		{
			// Empty wrapped + empty batch — minimal valid bootstrap.
			StorageDEKID: 7,
			RaftDEKID:    8,
		},
	}
	for _, want := range cases {
		got, err := fsmwire.DecodeBootstrap(fsmwire.EncodeBootstrap(want))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.StorageDEKID != want.StorageDEKID || got.RaftDEKID != want.RaftDEKID {
			t.Fatalf("dek_id mismatch: got %+v want %+v", got, want)
		}
		if !bytes.Equal(got.WrappedStorage, want.WrappedStorage) {
			t.Fatalf("storage wrapped mismatch")
		}
		if !bytes.Equal(got.WrappedRaft, want.WrappedRaft) {
			t.Fatalf("raft wrapped mismatch")
		}
		if len(got.BatchRegistry) != len(want.BatchRegistry) {
			t.Fatalf("batch len mismatch: got %d want %d", len(got.BatchRegistry), len(want.BatchRegistry))
		}
		for i := range want.BatchRegistry {
			if got.BatchRegistry[i] != want.BatchRegistry[i] {
				t.Fatalf("batch[%d] mismatch: got %+v want %+v", i, got.BatchRegistry[i], want.BatchRegistry[i])
			}
		}
	}
}

func TestBootstrap_RejectsTruncated(t *testing.T) {
	t.Parallel()
	full := fsmwire.EncodeBootstrap(fsmwire.BootstrapPayload{
		StorageDEKID: 1, WrappedStorage: []byte("abcd"),
		RaftDEKID: 2, WrappedRaft: []byte("ef"),
		BatchRegistry: []fsmwire.RegistrationPayload{{DEKID: 1}},
	})
	for cut := 0; cut < len(full); cut++ {
		_, err := fsmwire.DecodeBootstrap(full[:cut])
		if err == nil {
			t.Fatalf("cut=%d: expected truncation error, got nil", cut)
		}
		if !errors.Is(err, fsmwire.ErrFSMWireMalformed) && !errors.Is(err, fsmwire.ErrFSMWireVersion) {
			t.Fatalf("cut=%d: expected malformed/version, got %v", cut, err)
		}
	}
}

// TestBootstrap_RejectsOverCapBatchCount is the codex P1 round-3
// regression for PR748: the wire-bytes check (count *
// registrationSize ≤ remaining) is necessary but NOT sufficient
// because RegistrationPayload occupies more bytes in memory (24
// on 64-bit due to alignment padding) than on the wire (14 bytes
// packed). A crafted count that fits the wire-bounds check could
// still drive a >70% larger heap allocation; for a multi-MiB raft
// entry the in-memory cost would multiply into the gigabytes.
//
// The decoder enforces an absolute cap on `count` that bounds the
// post-make slice size independent of the raft entry length. This
// test asserts a cap-exceeding count fails closed BEFORE the
// allocation, even when the wire payload would otherwise admit it.
//
// The literal 16385 below must equal `maxBootstrapBatchCount + 1`
// in wire.go (1<<14 + 1). The constant is intentionally unexported
// — it is a defensive bound, not part of the wire contract.
func TestBootstrap_RejectsOverCapBatchCount(t *testing.T) {
	t.Parallel()
	good := fsmwire.EncodeBootstrap(fsmwire.BootstrapPayload{
		StorageDEKID: 1, RaftDEKID: 2,
	})
	const countOffset = 1 + 4 + 4 + 0 + 4 + 4 + 0
	if got := len(good); got < countOffset+4 {
		t.Fatalf("encoded bootstrap shorter than expected: %d bytes, want >= %d", got, countOffset+4)
	}
	// Overwrite count with maxBootstrapBatchCount + 1 = 16385.
	// The cap check fires before the wire-bytes check, so we do
	// NOT need to extend the payload with 16385 * 14 trailing
	// bytes — the decoder must reject on the cap regardless.
	good[countOffset+0] = 0x00
	good[countOffset+1] = 0x00
	good[countOffset+2] = 0x40 // 0x4001 = 16385 = (1<<14) + 1
	good[countOffset+3] = 0x01
	_, err := fsmwire.DecodeBootstrap(good)
	if !errors.Is(err, fsmwire.ErrFSMWireMalformed) {
		t.Fatalf("expected ErrFSMWireMalformed for over-cap batch count, got %v", err)
	}
}

// TestBootstrap_RejectsHugeBatchCount is the regression for the
// codex P1 / gemini security-high finding: a malformed bootstrap
// payload that names `count = 0xffffffff` would, before the guard,
// trigger `make([]RegistrationPayload, count)` (4G × 14 bytes ≈
// 56 GiB) and OOM the process before the loop tried to read the
// first row. The decoder must fail closed via ErrFSMWireMalformed
// before any allocation that exceeds the remaining payload.
func TestBootstrap_RejectsHugeBatchCount(t *testing.T) {
	t.Parallel()
	// Hand-roll a payload whose batch count is huge but which has
	// only a tiny tail to serve registration rows from. We start
	// from the encoded form of a minimal valid bootstrap, then
	// rewrite the count prefix in place.
	good := fsmwire.EncodeBootstrap(fsmwire.BootstrapPayload{
		StorageDEKID: 1, RaftDEKID: 2,
	})
	// Locate the batch-count prefix: after [ver(1)] [storage_dek_id(4)]
	// [storage_wrapped_len(4)] [storage_wrapped(0)] [raft_dek_id(4)]
	// [raft_wrapped_len(4)] [raft_wrapped(0)] = offset 17.
	const countOffset = 1 + 4 + 4 + 0 + 4 + 4 + 0
	if got := len(good); got < countOffset+4 {
		t.Fatalf("encoded bootstrap shorter than expected: %d bytes, want >= %d", got, countOffset+4)
	}
	// Overwrite count with 0xFFFFFFFF (4G entries). Pre-fix this
	// would `make([]RegistrationPayload, 4_294_967_295)` and OOM.
	good[countOffset+0] = 0xFF
	good[countOffset+1] = 0xFF
	good[countOffset+2] = 0xFF
	good[countOffset+3] = 0xFF
	_, err := fsmwire.DecodeBootstrap(good)
	if !errors.Is(err, fsmwire.ErrFSMWireMalformed) {
		t.Fatalf("expected ErrFSMWireMalformed for huge batch count, got %v", err)
	}
}

func TestBootstrap_RejectsTrailingBytes(t *testing.T) {
	t.Parallel()
	good := fsmwire.EncodeBootstrap(fsmwire.BootstrapPayload{StorageDEKID: 1, RaftDEKID: 2})
	// Build a fresh slice with one extra byte rather than appending
	// to `good` (which gocritic flags as "append result not assigned
	// to the same slice"). The intent is "good followed by 0x99".
	corrupted := make([]byte, 0, len(good)+1)
	corrupted = append(corrupted, good...)
	corrupted = append(corrupted, 0x99)
	_, err := fsmwire.DecodeBootstrap(corrupted)
	if !errors.Is(err, fsmwire.ErrFSMWireMalformed) {
		t.Fatalf("expected ErrFSMWireMalformed, got %v", err)
	}
}

func TestRotation_RoundTrip(t *testing.T) {
	t.Parallel()
	want := fsmwire.RotationPayload{
		SubTag:  fsmwire.RotateSubRotateDEK,
		DEKID:   0xCAFEBABE,
		Purpose: fsmwire.PurposeStorage,
		Wrapped: []byte("new-wrapped-DEK"),
		ProposerRegistration: fsmwire.RegistrationPayload{
			DEKID:      0xCAFEBABE,
			FullNodeID: 0xDEADBEEFCAFEBABE,
			LocalEpoch: 7,
		},
	}
	got, err := fsmwire.DecodeRotation(fsmwire.EncodeRotation(want))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.SubTag != want.SubTag || got.DEKID != want.DEKID || got.Purpose != want.Purpose {
		t.Fatalf("header mismatch: got %+v", got)
	}
	if !bytes.Equal(got.Wrapped, want.Wrapped) {
		t.Fatalf("wrapped mismatch")
	}
	if got.ProposerRegistration != want.ProposerRegistration {
		t.Fatalf("proposer reg mismatch: got %+v want %+v", got.ProposerRegistration, want.ProposerRegistration)
	}
}

func TestRotation_RejectsUnknownSubTag(t *testing.T) {
	t.Parallel()
	raw := fsmwire.EncodeRotation(fsmwire.RotationPayload{
		SubTag: fsmwire.RotateSubRotateDEK, DEKID: 1, Purpose: fsmwire.PurposeStorage,
	})
	raw[1] = 0x99 // sub-tag byte (after version)
	_, err := fsmwire.DecodeRotation(raw)
	if !errors.Is(err, fsmwire.ErrFSMWireSubtag) {
		t.Fatalf("expected ErrFSMWireSubtag, got %v", err)
	}
}

// TestRotation_RejectsUnknownPurpose is the codex P2 regression for
// PR748: the decoder previously cast the raw purpose byte to
// `Purpose` without validating, so values outside the §5.1 enum
// (storage=1, raft=2) — for example 0x00 or 0xFF — were accepted as
// syntactically valid and forwarded to the applier. That weakens the
// fail-closed wire contract: an applier that did not independently
// re-check purpose could advance setApplied past a malformed entry.
// Validate at decode time and surface ErrFSMWireMalformed.
func TestRotation_RejectsUnknownPurpose(t *testing.T) {
	t.Parallel()
	for _, bad := range []byte{0x00, 0x03, 0x7F, 0xFF} {
		raw := fsmwire.EncodeRotation(fsmwire.RotationPayload{
			SubTag: fsmwire.RotateSubRotateDEK, DEKID: 1, Purpose: fsmwire.PurposeStorage,
		})
		// Tamper purpose byte. Layout: [ver(1)] [subtag(1)] [dek_id(4)] [purpose(1)] ...
		const purposeOffset = 1 + 1 + 4
		if got := len(raw); got <= purposeOffset {
			t.Fatalf("encoded rotation shorter than expected: %d bytes, want > %d", got, purposeOffset)
		}
		raw[purposeOffset] = bad
		_, err := fsmwire.DecodeRotation(raw)
		if !errors.Is(err, fsmwire.ErrFSMWireMalformed) {
			t.Fatalf("purpose=%#x: expected ErrFSMWireMalformed, got %v", bad, err)
		}
	}
}

// TestRotation_AcceptsKnownPurposes is the positive control for the
// purpose-validation guard above: storage=1 and raft=2 must continue
// to round-trip cleanly. A regression that reject-listed too
// aggressively would also break Stage 6's enable-storage-envelope
// and enable-raft-envelope paths.
func TestRotation_AcceptsKnownPurposes(t *testing.T) {
	t.Parallel()
	for _, p := range []fsmwire.Purpose{fsmwire.PurposeStorage, fsmwire.PurposeRaft} {
		raw := fsmwire.EncodeRotation(fsmwire.RotationPayload{
			SubTag: fsmwire.RotateSubRotateDEK, DEKID: 9, Purpose: p,
		})
		got, err := fsmwire.DecodeRotation(raw)
		if err != nil {
			t.Fatalf("purpose=%d: %v", p, err)
		}
		if got.Purpose != p {
			t.Fatalf("purpose round-trip: got %d want %d", got.Purpose, p)
		}
	}
}

func TestRotation_RejectsBadVersion(t *testing.T) {
	t.Parallel()
	raw := fsmwire.EncodeRotation(fsmwire.RotationPayload{SubTag: fsmwire.RotateSubRotateDEK})
	raw[0] = 0x99
	_, err := fsmwire.DecodeRotation(raw)
	if !errors.Is(err, fsmwire.ErrFSMWireVersion) {
		t.Fatalf("expected ErrFSMWireVersion, got %v", err)
	}
}

// TestOpcodes_DistinctFromKVOpcodes pins that the new opcodes do not
// collide with kv/fsm.go's existing raftEncode* tags (0x00-0x02).
// A drift here would route an FSM dispatch through the wrong handler.
func TestOpcodes_DistinctFromKVOpcodes(t *testing.T) {
	t.Parallel()
	if fsmwire.OpRegistration <= 0x02 || fsmwire.OpBootstrap <= 0x02 || fsmwire.OpRotation <= 0x02 {
		t.Fatalf("opcode collision with kv legacy: 0x%02x / 0x%02x / 0x%02x",
			fsmwire.OpRegistration, fsmwire.OpBootstrap, fsmwire.OpRotation)
	}
	if fsmwire.OpRegistration == fsmwire.OpBootstrap ||
		fsmwire.OpBootstrap == fsmwire.OpRotation ||
		fsmwire.OpRegistration == fsmwire.OpRotation {
		t.Fatal("opcodes are not distinct")
	}
}
