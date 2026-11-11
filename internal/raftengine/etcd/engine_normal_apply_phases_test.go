package etcd

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type normalPhaseFSM struct {
	testStateMachine
	apply       func([]byte) any
	indexes     []uint64
	calls       int
	classified  [][]byte
	applyEvents []string
}

func (f *normalPhaseFSM) SetApplyIndex(index uint64) {
	f.indexes = append(f.indexes, index)
	f.applyEvents = append(f.applyEvents, "index")
}

func (f *normalPhaseFSM) Apply(payload []byte) any {
	f.calls++
	f.applyEvents = append(f.applyEvents, "apply")
	return f.apply(payload)
}

func (f *normalPhaseFSM) IsVolatileOnlyPayload(payload []byte) bool {
	f.classified = append(f.classified, bytes.Clone(payload))
	return len(payload) > 0 && payload[0] == 0x02
}

type normalPhaseFixture struct {
	engine    *Engine
	done      chan proposalResult
	adminDone chan adminResult
}

func newNormalPhaseFixture(fsm StateMachine, applied uint64) normalPhaseFixture {
	e := newTestEngine(fsm, nil, nil)
	e.applied = applied
	e.appliedIndex.Store(applied)
	e.status.AppliedIndex = applied
	e.configIndex.Store(90)
	e.markPendingConfChange(120)
	done := make(chan proposalResult, 1)
	adminDone := make(chan adminResult, 1)
	e.pendingProposals = map[uint64]proposalRequest{7: {id: 7, done: done}}
	e.pendingConfigs = map[uint64]adminRequest{17: {id: 17, done: adminDone}}
	return normalPhaseFixture{engine: e, done: done, adminDone: adminDone}
}

func normalPhaseEntry(index, id uint64, payload []byte) raftpb.Entry {
	return raftpb.Entry{
		Type: entryTypePtr(raftpb.EntryNormal), Index: uint64Ptr(index),
		Data: encodeProposalEnvelope(id, payload),
	}
}

func (f normalPhaseFixture) requireUnpublished(t *testing.T, applied uint64) {
	t.Helper()
	require.Equal(t, applied, f.engine.applied)
	require.Equal(t, applied, f.engine.AppliedIndex())
	require.Equal(t, applied, f.engine.Status().AppliedIndex)
	require.Contains(t, f.engine.pendingProposals, uint64(7))
	require.Empty(t, f.done, "execution must not deliver a proposal receipt")
	f.requireAdminUnchanged(t)
}

func (f normalPhaseFixture) requireAdminUnchanged(t *testing.T) {
	t.Helper()
	require.Equal(t, uint64(90), f.engine.currentConfigIndex())
	require.Equal(t, uint64(120), f.engine.pendingConfChangeIndex.Load())
	require.Contains(t, f.engine.pendingConfigs, uint64(17))
	require.Empty(t, f.adminDone, "normal apply must not deliver an admin receipt")
}

func requireNormalPhaseProposal(t *testing.T, done <-chan proposalResult, index uint64, response any) {
	t.Helper()
	select {
	case result := <-done:
		require.NoError(t, result.err)
		require.NotNil(t, result.result)
		require.Equal(t, index, result.result.CommitIndex)
		require.Equal(t, response, result.result.Response)
	default:
		t.Fatal("owner publication did not deliver a proposal receipt")
	}
	require.Empty(t, done, "proposal receipt must be delivered exactly once")
}

func TestEngineNormalApplyPhasesDelayedCompletion(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	workerDone := make(chan struct{})
	completed := make(chan normalApplyReceipt, 1)
	response := errors.New("ordinary committed FSM verdict")
	fsm := &normalPhaseFSM{apply: func([]byte) any {
		close(entered)
		<-release
		return response
	}}
	f := newNormalPhaseFixture(fsm, 100)
	entry := normalPhaseEntry(101, 7, []byte("payload"))
	input := f.engine.prepareNormalApply(entry, false)
	// Execution owns captured references/scalars, not later Engine field values
	// or the protobuf's pointer-backed index. Keep the borrowed bytes immutable.
	f.engine.fsm = nil
	f.engine.raftCutoverIndex = func() uint64 { panic("read Engine during execution") }
	*entry.Index = 999
	go func() {
		defer close(workerDone)
		completed <- executeNormalApply(input)
	}()
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		<-workerDone
	})
	awaitStorageSignal(t, entered)
	f.requireUnpublished(t, 100)
	require.Empty(t, completed, "held FSM must not produce an execution receipt")
	releaseOnce.Do(func() { close(release) })
	awaitStorageSignal(t, workerDone)
	receipt := <-completed
	require.NoError(t, receipt.err)
	require.Equal(t, []uint64{101}, fsm.indexes)
	require.Equal(t, []string{"index", "apply"}, fsm.applyEvents)
	f.requireUnpublished(t, 100) // Completed execution is still not publication.
	require.NoError(t, f.engine.publishNormalApply(receipt))
	require.Equal(t, uint64(101), f.engine.applied)
	require.Equal(t, uint64(101), f.engine.AppliedIndex())
	require.NotContains(t, f.engine.pendingProposals, uint64(7))
	requireNormalPhaseProposal(t, f.done, 101, response)
	f.requireAdminUnchanged(t)
}

func TestEngineNormalApplyPhasesReplayAndEmpty(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		for _, duplicate := range []bool{false, true} {
			for _, kind := range []string{"empty", "short", "bad-version", "empty-payload", "data", "volatile"} {
				t.Run(normalPhaseCaseName(encrypted, duplicate, kind), func(t *testing.T) {
					testNormalPhaseReplayAndEmpty(t, encrypted, duplicate, kind)
				})
			}
		}
	}
}

func normalPhaseCaseName(encrypted, duplicate bool, kind string) string {
	return fmt.Sprintf("encrypted=%t/duplicate=%t/%s", encrypted, duplicate, kind)
}

func testNormalPhaseReplayAndEmpty(t *testing.T, encrypted, duplicate bool, kind string) {
	t.Helper()
	response := &haltResponse{} // A nil HaltApply error is still a success.
	fsm := &normalPhaseFSM{apply: func([]byte) any { return response }}
	base := uint64(100)
	if duplicate {
		base = 200
	}
	f := newNormalPhaseFixture(fsm, base)
	entry, payload, decoded := normalPhaseReplayEntry(t, f, encrypted, kind)
	receipt := executeNormalApply(f.engine.prepareNormalApply(entry, duplicate))
	require.NoError(t, receipt.err)
	f.requireUnpublished(t, base)
	requireNormalPhaseExecution(t, fsm, receipt, duplicate, kind, decoded, payload, response)
	require.NoError(t, f.engine.publishNormalApply(receipt))
	if duplicate {
		f.requireUnpublished(t, base)
	} else {
		require.Equal(t, uint64(101), f.engine.AppliedIndex())
		if decoded {
			requireNormalPhaseProposal(t, f.done, 101, receipt.response)
		} else {
			require.Contains(t, f.engine.pendingProposals, uint64(7))
			require.Empty(t, f.done)
		}
		f.requireAdminUnchanged(t)
	}
}

func normalPhaseReplayEntry(t *testing.T, f normalPhaseFixture, encrypted bool, kind string) (raftpb.Entry, []byte, bool) {
	t.Helper()
	payload := normalPhaseReplayPayload(encrypted, kind)
	entry := normalPhaseEntry(101, 7, payload)
	decoded := kind == "data" || kind == "volatile" || kind == "empty-payload"
	if encrypted {
		cipher, kid := raftCipherFixture(t)
		f.engine.raftCipher = cipher
		f.engine.raftCutoverIndex = func() uint64 { return 90 }
		entry = envelopeEntry(t, cipher, kid, 101, payload)
	}
	switch kind {
	case "empty":
		entry.Data = nil
	case "short":
		entry.Data = []byte{proposalEnvelopeVersion}
	case "bad-version":
		entry.Data[0] = 0xff
	}
	if !decoded {
		f.engine.raftCutoverIndex = func() uint64 { panic("invalid envelope read cutover") }
	}
	return entry, payload, decoded
}

func normalPhaseReplayPayload(encrypted bool, kind string) []byte {
	switch kind {
	case "volatile":
		return []byte{0x02, 0x99}
	case "empty-payload":
		if encrypted {
			// GCM.Open returns nil for empty plaintext; envelope slicing
			// returns a non-nil empty slice on the unencrypted path.
			return nil
		}
		return []byte{}
	default:
		return []byte{0x00, 0x99}
	}
}

func requireNormalPhaseExecution(t *testing.T, fsm *normalPhaseFSM, receipt normalApplyReceipt, duplicate bool,
	kind string, decoded bool, payload []byte, response *haltResponse,
) {
	t.Helper()
	wantApply := decoded && (!duplicate || kind == "volatile")
	if wantApply {
		require.Equal(t, 1, fsm.calls)
		require.Same(t, response, receipt.response)
	} else {
		require.Zero(t, fsm.calls)
		require.Nil(t, receipt.response)
	}
	if duplicate && decoded {
		require.Equal(t, [][]byte{payload}, fsm.classified, "classify decrypted bytes, including empty payloads")
	} else {
		require.Empty(t, fsm.classified)
	}
	if wantApply && !duplicate {
		require.Equal(t, []uint64{101}, fsm.indexes)
		require.Equal(t, []string{"index", "apply"}, fsm.applyEvents)
	} else {
		require.Empty(t, fsm.indexes, "never deliver a stale apply index on replay")
	}
}

func TestEngineNormalApplyPhasesFailedReceipt(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		for _, failure := range []string{"halt", "tampered", "missing-cipher"} {
			t.Run(normalPhaseCaseName(true, duplicate, failure), func(t *testing.T) {
				fsm := &normalPhaseFSM{apply: func([]byte) any { return &haltResponse{err: ErrEncryptionApply} }}
				base := uint64(100)
				if duplicate {
					base = 200
				}
				f := newNormalPhaseFixture(fsm, base)
				cipher, kid := raftCipherFixture(t)
				f.engine.raftCipher = cipher
				f.engine.raftCutoverIndex = func() uint64 { return 90 }
				payload := []byte{0x02, 0x99}
				if failure == "tampered" {
					payload[0] = 0x00 // Even a dropped data duplicate must unwrap first.
				}
				entry := envelopeEntry(t, cipher, kid, 101, payload)
				wantErr := ErrRaftUnwrapFailed
				switch failure {
				case "halt":
					wantErr = ErrEncryptionApply
				case "tampered":
					entry.Data[len(entry.Data)-1] ^= 0xff
				case "missing-cipher":
					f.engine.raftCipher = nil
				}
				receipt := executeNormalApply(f.engine.prepareNormalApply(entry, duplicate))
				require.Truef(t, errors.Is(receipt.err, wantErr), "receipt error = %v; want %v", receipt.err, wantErr)
				f.requireUnpublished(t, base)
				publicationErr := f.engine.publishNormalApply(receipt)
				require.Truef(t, errors.Is(publicationErr, wantErr), "publication error = %v; want %v", publicationErr, wantErr)
				f.requireUnpublished(t, base)
				if failure == "halt" {
					require.Equal(t, 1, fsm.calls)
				} else {
					require.Zero(t, fsm.calls)
					require.Empty(t, fsm.indexes)
					require.Empty(t, fsm.classified)
				}
			})
		}
	}
}

func TestEngineNormalApplyPhasesSynchronousPrefix(t *testing.T) {
	for _, failure := range []string{"halt", "unwrap"} {
		t.Run(failure, func(t *testing.T) {
			var seen []string
			verdict := errors.New("committed request conflict")
			fsm := &normalPhaseFSM{apply: func(payload []byte) any {
				seen = append(seen, string(payload))
				if string(payload) == "bad" {
					return &haltResponse{err: ErrEncryptionApply}
				}
				return verdict
			}}
			f := newNormalPhaseFixture(fsm, 100)
			badDone, neverDone := make(chan proposalResult, 1), make(chan proposalResult, 1)
			f.engine.pendingProposals[8] = proposalRequest{id: 8, done: badDone}
			f.engine.pendingProposals[9] = proposalRequest{id: 9, done: neverDone}
			cipher, kid := raftCipherFixture(t)
			f.engine.raftCipher = cipher
			f.engine.raftCutoverIndex = func() uint64 { return 100 }
			entries := []raftpb.Entry{
				normalPhaseWrappedEntry(t, cipher, kid, 101, 7, []byte("good")),
				normalPhaseWrappedEntry(t, cipher, kid, 102, 8, []byte("bad")),
				normalPhaseWrappedEntry(t, cipher, kid, 103, 9, []byte("never")),
			}
			wantErr := ErrEncryptionApply
			if failure == "unwrap" {
				entries[1].Data[len(entries[1].Data)-1] ^= 0xff
				wantErr = ErrRaftUnwrapFailed
			}
			err := f.engine.applyCommitted(entries)
			require.Truef(t, errors.Is(err, wantErr), "apply error = %v; want %v", err, wantErr)
			require.Equal(t, uint64(101), f.engine.applied)
			require.Equal(t, uint64(101), f.engine.AppliedIndex())
			requireNormalPhaseProposal(t, f.done, 101, verdict)
			require.Contains(t, f.engine.pendingProposals, uint64(8))
			require.Contains(t, f.engine.pendingProposals, uint64(9))
			require.Empty(t, badDone)
			require.Empty(t, neverDone)
			if failure == "halt" {
				require.Equal(t, []string{"good", "bad"}, seen)
			} else {
				require.Equal(t, []string{"good"}, seen)
			}
			f.requireAdminUnchanged(t)
		})
	}
}

func normalPhaseWrappedEntry(t *testing.T, cipher *encryption.Cipher, kid uint32, index, id uint64, payload []byte) raftpb.Entry {
	t.Helper()
	entry := envelopeEntry(t, cipher, kid, index, payload)
	_, wrapped, ok := decodeProposalEnvelope(entry.GetData())
	require.True(t, ok)
	entry.Data = encodeProposalEnvelope(id, wrapped)
	return entry
}

func TestEngineNormalApplyPhasesOrderedCutover(t *testing.T) {
	for _, queued := range []bool{false, true} {
		name := "synchronous"
		if queued {
			name = "captured-before-cutover"
		}
		t.Run(name, func(t *testing.T) {
			cipher, kid := raftCipherFixture(t)
			cutover := inertRaftCutoverIndex()
			var seen []string
			fsm := &normalPhaseFSM{apply: func(payload []byte) any {
				seen = append(seen, string(payload))
				if string(payload) == "enable" {
					cutover = 100
				}
				return nil
			}}
			f := newNormalPhaseFixture(fsm, 99)
			f.engine.raftCipher = cipher
			reads := 0
			f.engine.raftCutoverIndex = func() uint64 { reads++; return cutover }
			flag := normalPhaseEntry(100, 7, []byte("enable"))
			wrapped := envelopeEntry(t, cipher, kid, 101, []byte("encrypted"))
			if queued {
				first := f.engine.prepareNormalApply(flag, false)
				next := f.engine.prepareNormalApply(wrapped, false)
				require.Zero(t, reads, "capture must not evaluate the apply-ordered cutover")
				firstReceipt := executeNormalApply(first)
				require.NoError(t, firstReceipt.err)
				f.requireUnpublished(t, 99)
				// Next execution sees the FSM cutover even before owner publication.
				nextReceipt := executeNormalApply(next)
				require.NoError(t, nextReceipt.err)
				f.requireUnpublished(t, 99)
				require.NoError(t, f.engine.publishNormalApply(firstReceipt))
				require.NoError(t, f.engine.publishNormalApply(nextReceipt))
			} else {
				require.NoError(t, f.engine.applyCommitted([]raftpb.Entry{flag, wrapped}))
			}
			require.Equal(t, []string{"enable", "encrypted"}, seen)
			require.Equal(t, []uint64{100, 101}, fsm.indexes)
			require.Equal(t, 2, reads)
			require.Equal(t, uint64(101), f.engine.AppliedIndex())
			requireNormalPhaseProposal(t, f.done, 100, nil)
		})
	}
}

func TestEngineNormalApplyPhasesPlaintextBorrowsPayload(t *testing.T) {
	entry := normalPhaseEntry(101, 7, []byte("payload"))
	aliased := false
	fsm := &normalPhaseFSM{apply: func(payload []byte) any {
		aliased = &payload[0] == &entry.Data[envelopeHeaderSize]
		return nil
	}}
	f := newNormalPhaseFixture(fsm, 100)
	require.NoError(t, f.engine.applyNormalCommitted(entry))
	require.True(t, aliased, "normal plaintext execution must not add a payload copy")
	requireNormalPhaseProposal(t, f.done, 101, nil)
}

func TestEngineNormalApplyPhasesEntryOnlyCompatibility(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		for _, kind := range []string{"data", "volatile"} {
			t.Run(normalPhaseCaseName(false, duplicate, kind), func(t *testing.T) {
				response := &haltResponse{err: ErrEncryptionApply}
				fsm := &normalPhaseFSM{apply: func([]byte) any { return response }}
				f := newNormalPhaseFixture(fsm, 100)
				entry, _, _ := normalPhaseReplayEntry(t, f, false, kind)
				got, err := f.engine.applyNormalEntry(entry, duplicate)
				require.NoError(t, err, "entry-only helper returns HaltApply responses without interpreting them")
				if duplicate && kind == "data" {
					require.Nil(t, got)
					require.Zero(t, fsm.calls)
				} else {
					require.Same(t, response, got)
					require.Equal(t, 1, fsm.calls)
				}
				f.requireUnpublished(t, 100)
			})
		}
	}
}

var _ raftengine.ApplyIndexAware = (*normalPhaseFSM)(nil)
