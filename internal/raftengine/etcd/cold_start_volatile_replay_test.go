package etcd

import (
	"bytes"
	"io"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// volatileTagFakeFSM is a StateMachine that classifies payloads by
// their leading byte: 0x02 (kv.raftEncodeHLCLease) is volatile, every
// other tag is data-mutating. Mirrors the kvFSM contract closely
// enough that the engine's cold-start duplicate guard can be tested
// without pulling in the full kv package.
type volatileTagFakeFSM struct {
	calls       atomic.Int32
	lastPayload []byte
}

func (f *volatileTagFakeFSM) Apply(data []byte) any {
	f.calls.Add(1)
	cp := make([]byte, len(data))
	copy(cp, data)
	f.lastPayload = cp
	return nil
}

func (f *volatileTagFakeFSM) Snapshot() (Snapshot, error) { return nil, nil }
func (f *volatileTagFakeFSM) Restore(_ io.Reader) error   { return nil }

func (f *volatileTagFakeFSM) IsVolatileOnlyPayload(payload []byte) bool {
	return len(payload) > 0 && payload[0] == 0x02
}

var _ raftengine.VolatileEntryClassifier = (*volatileTagFakeFSM)(nil)

// TestApplyNormalCommitted_VolatileEntryReplayedOnDuplicate pins the
// codex P1 #934 round 7 fix: after the cold-start skip gate seeds
// e.applied at the WAL committed tail, entries delivered by Raft at
// indices <= e.applied that are volatile-only (HLC lease, tag 0x02)
// MUST still reach fsm.Apply so the post-snapshot ceiling raise is
// reconstructed. Data-mutating duplicates (any other tag) MUST NOT
// reach fsm.Apply (OCC re-validation; ceiling regression). A
// regression would either silently drop the lease (the bug) or
// silently re-execute KV writes (idempotency violation).
func TestApplyNormalCommitted_VolatileEntryReplayedOnDuplicate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		tag       byte
		wantApply bool
	}{
		{"volatile HLC lease (tag 0x02) replays past e.applied", 0x02, true},
		{"data-mutating single KV (tag 0x00) is dropped", 0x00, false},
		{"data-mutating batch KV (tag 0x01) is dropped", 0x01, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			fsm := &volatileTagFakeFSM{}
			e := newTestEngine(fsm, nil, nil)
			e.applied = 200

			payload := []byte{c.tag, 0xde, 0xad, 0xbe, 0xef, 0x00, 0x00, 0x00, 0x00}
			entry := raftpb.Entry{
				Type:  entryTypePtr(raftpb.EntryNormal),
				Index: uint64Ptr(150), // <= e.applied → duplicate path
				Data:  encodeProposalEnvelope(42, payload),
			}

			if err := e.applyNormalCommitted(entry); err != nil {
				t.Fatalf("applyNormalCommitted: %v", err)
			}

			got := fsm.calls.Load()
			switch {
			case c.wantApply && got != 1:
				t.Fatalf("volatile duplicate: fsm.Apply call count = %d, want 1 (lost in-memory effect)", got)
			case !c.wantApply && got != 0:
				t.Fatalf("data-mutating duplicate: fsm.Apply call count = %d, want 0 (re-applied; OCC will re-validate against post-tail state)", got)
			}

			// In every case e.applied must NOT advance — the entry's
			// index is below the gate-seeded value and resolveProposal
			// is intentionally not called either.
			if e.applied != 200 {
				t.Fatalf("e.applied advanced to %d, want it pinned at 200 for duplicate-path entries", e.applied)
			}
		})
	}
}

// TestApplyNormalCommitted_FreshEntryAlwaysAppliesAndAdvances pins the
// non-duplicate path: entries past e.applied always reach fsm.Apply
// regardless of the volatile/data classification, and e.applied
// advances. Locks in the asymmetry — the classifier ONLY gates the
// duplicate arm.
func TestApplyNormalCommitted_FreshEntryAlwaysAppliesAndAdvances(t *testing.T) {
	t.Parallel()
	for _, tag := range []byte{0x00, 0x01, 0x02} {
		t.Run("tag=0x0"+string("0123"[tag])+"_fresh", func(t *testing.T) {
			t.Parallel()
			fsm := &volatileTagFakeFSM{}
			e := newTestEngine(fsm, nil, nil)
			e.applied = 100

			entry := raftpb.Entry{
				Type:  entryTypePtr(raftpb.EntryNormal),
				Index: uint64Ptr(150),
				Data:  encodeProposalEnvelope(7, []byte{tag, 0x99}),
			}

			if err := e.applyNormalCommitted(entry); err != nil {
				t.Fatalf("applyNormalCommitted: %v", err)
			}
			if got := fsm.calls.Load(); got != 1 {
				t.Fatalf("fsm.Apply call count = %d, want 1 (fresh entry, all tags)", got)
			}
			if e.applied != 150 {
				t.Fatalf("e.applied = %d, want 150 (fresh entry must advance)", e.applied)
			}
		})
	}
}

// TestApplyNormalCommitted_VolatileDuplicate_PostCutoverEncrypted pins
// the post-Stage-8a cutover path: encrypted HLC lease entries past
// e.applied MUST be decrypted FIRST, then classified as volatile, then
// replayed for their in-memory effect. The wire-format reality is
// that a post-cutover HLC lease's `payload[0]` is encrypted bytes;
// only the cleartext (after WrapRaftPayload unwrap) carries the 0x02
// tag, so the classifier must see the cleartext or the lease drops.
// Claude #934 round 7 finding R7-F2 — pre-cutover-only coverage was
// insufficient.
func TestApplyNormalCommitted_VolatileDuplicate_PostCutoverEncrypted(t *testing.T) {
	t.Parallel()
	c, kid := raftCipherFixture(t)
	const cutover uint64 = 100
	fsm := &volatileTagFakeFSM{}
	e := newTestEngine(fsm, c, func() uint64 { return cutover })
	e.applied = 200

	// HLC lease cleartext: tag 0x02 + 8-byte big-endian ceiling.
	plain := []byte{0x02, 0, 0, 0, 0, 0, 0, 0, 0x01}
	// Index above cutover → triggers WrapRaftPayload path inside
	// applyNormalEntry; index below e.applied → duplicate arm.
	entry := envelopeEntry(t, c, kid, 150, plain)

	if err := e.applyNormalCommitted(entry); err != nil {
		t.Fatalf("applyNormalCommitted: %v", err)
	}
	if got := fsm.calls.Load(); got != 1 {
		t.Fatalf("encrypted volatile duplicate: fsm.Apply call count = %d, want 1 — decryption must run before classification or the lease drops", got)
	}
	if !bytes.Equal(fsm.lastPayload, plain) {
		t.Fatalf("FSM received %x, want cleartext %x — classifier must see post-decrypt bytes", fsm.lastPayload, plain)
	}
	if e.applied != 200 {
		t.Fatalf("e.applied advanced to %d, want pinned at 200 for duplicate-arm replay", e.applied)
	}
}

// TestApplyNormalCommitted_DuplicateWithoutClassifier_DropsAll guards
// the FSM-doesn't-opt-in path: a StateMachine that does NOT implement
// VolatileEntryClassifier (existing FSMs, third-party engines) must
// keep the pre-PR behavior — every duplicate is dropped, including
// any that happen to be volatile. The strictly-additive opt-in keeps
// the engine compatible with FSMs that have not been updated.
func TestApplyNormalCommitted_DuplicateWithoutClassifier_DropsAll(t *testing.T) {
	t.Parallel()
	// fakeStateMachine (from encryption_test.go) does NOT implement
	// VolatileEntryClassifier — that absence is the contract under
	// test.
	fsm := &fakeStateMachine{}
	if _, ok := any(fsm).(raftengine.VolatileEntryClassifier); ok {
		t.Fatal("fakeStateMachine unexpectedly implements VolatileEntryClassifier; test contract broken")
	}
	e := newTestEngine(fsm, nil, nil)
	e.applied = 200

	entry := raftpb.Entry{
		Type:  entryTypePtr(raftpb.EntryNormal),
		Index: uint64Ptr(150),
		Data:  encodeProposalEnvelope(13, []byte{0x02, 0xff}), // would-be volatile
	}

	if err := e.applyNormalCommitted(entry); err != nil {
		t.Fatalf("applyNormalCommitted: %v", err)
	}
	if got := fsm.calls.Load(); got != 0 {
		t.Fatalf("FSM without classifier: fsm.Apply call count = %d, want 0 (drop-all on duplicate, no opt-in)", got)
	}
	if e.applied != 200 {
		t.Fatalf("e.applied advanced unexpectedly: %d, want 200", e.applied)
	}
}
