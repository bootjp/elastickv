package etcd

import (
	"log/slog"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// normalApplyInput captures owner decisions without retaining Engine or
// protobuf scalar pointers. data borrows the immutable committed bytes, which
// the caller must retain through publication. FSM/cipher lifetime must cover
// execution, ordered with config/restore. The immutable cutover callback is
// evaluated during execution, after earlier FSM effects, not during capture.
// Async integration must also derive duplicate from the apply actor's ordered
// cursor, not a stale owner-published applied index. Current capture is SYNC.
type normalApplyInput struct {
	index     uint64
	data      []byte
	duplicate bool
	cutover   RaftCutoverIndex
	cipher    *encryption.Cipher
	fsm       StateMachine
}

// normalApplyReceipt is execution's verdict, not a published applied index or
// client receipt. Even a successful volatile replay must not resolve a proposal.
type normalApplyReceipt struct {
	index     uint64
	data      []byte
	duplicate bool
	response  any
	err       error
}

// prepareNormalApply runs on the owner. Copy the configured references, not the
// cutover's current value: a preceding queued apply can enable encryption.
func (e *Engine) prepareNormalApply(entry raftpb.Entry, duplicate bool) normalApplyInput {
	return normalApplyInput{
		index: entry.GetIndex(), data: entry.GetData(), duplicate: duplicate,
		cutover: e.raftCutoverIndex, cipher: e.raftCipher, fsm: e.fsm,
	}
}

// executeNormalApply has no Engine/RawNode access and never completes proposals.
// HaltApply is checked even on duplicate volatile replay. Ordinary FSM verdicts
// (including errors without HaltApply) remain committed responses.
func executeNormalApply(input normalApplyInput) normalApplyReceipt {
	response, err := input.applyNormalEntry()
	if err == nil {
		if h, ok := response.(interface{ HaltApply() error }); ok {
			if halt := h.HaltApply(); halt != nil {
				slog.Error("encryption FSM apply requested halt; not advancing setApplied",
					slog.Uint64("entry_index", input.index), slog.Any("err", halt))
				err = errors.Wrap(halt, "raftengine/etcd: FSM-requested apply halt")
			}
		}
	}
	return normalApplyReceipt{
		index: input.index, data: input.data, duplicate: input.duplicate,
		response: response, err: err,
	}
}

func (input normalApplyInput) applyNormalEntry() (any, error) {
	_, payload, decoded := decodeProposalEnvelope(input.data)
	if !decoded {
		return nil, nil
	}
	// The cutover entry itself is plaintext. Only its successors are wrapped;
	// read after all earlier applies, then unwrap before classifying duplicates,
	// including ones we will drop. Never cache this value at enqueue time.
	cutover := input.cutover()
	if input.index > cutover {
		if input.cipher == nil {
			slog.Error("raft envelope cutover active but no cipher configured; halting apply",
				slog.Uint64("entry_index", input.index), slog.Uint64("cutover_index", cutover))
			return nil, errors.Wrap(ErrRaftUnwrapFailed,
				"raftengine/etcd: entry past raft envelope cutover but no raft cipher wired")
		}
		plain, err := unwrapRaftPayload(input.cipher, payload)
		if err != nil {
			slog.Error("raft envelope unwrap failed; halting apply",
				slog.Uint64("entry_index", input.index), slog.Any("err", err))
			return nil, err
		}
		payload = plain
	}
	if input.duplicate && !isVolatilePayload(input.fsm, payload) {
		return nil, nil
	}
	// A volatile replay has an old index; never feed it to a durability sink.
	if !input.duplicate {
		if aware, ok := input.fsm.(raftengine.ApplyIndexAware); ok {
			aware.SetApplyIndex(input.index)
		}
	}
	// Without encryption payload aliases committed data; decrypt allocates its
	// own plaintext. In either case the FSM must copy if it retains bytes after
	// Apply returns. No per-user-payload copy is added at this boundary.
	return input.fsm.Apply(payload), nil
}

// publishNormalApply runs only on the owner, in committed-entry order. Failure
// leaves pending callers for the existing fatal path, never a false verdict.
func (e *Engine) publishNormalApply(receipt normalApplyReceipt) error {
	if receipt.err != nil {
		return receipt.err
	}
	if receipt.duplicate {
		return nil
	}
	e.setApplied(receipt.index)
	e.resolveProposal(receipt.index, receipt.data, receipt.response)
	return nil
}
