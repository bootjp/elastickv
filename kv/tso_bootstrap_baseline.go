package kv

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
)

var ErrTSOBaseline = errors.New("tso: offline scalar baseline unavailable or inconsistent")

// TSOBaselineCut comes exclusively from the native offline owner after selected
// snapshot/WAL validation. These fields are not an operator-supplied proof.
type TSOBaselineCut struct {
	SnapshotIndex, SnapshotTerm      uint64
	CommitIndex, CommitTerm          uint64
	LastLogIndex, ConfigurationIndex uint64
}

// TSOBaselineEntry includes EVERY retained committed slot after the selected
// snapshot, including no-ops and configurations. Type uses the native Raft enum:
// 0 normal, 1 ConfChange, 2 ConfChangeV2. Native must validate configurations and
// supply plaintext normal commands; this decoder never decrypts envelopes.
type TSOBaselineEntry struct {
	Index, Term uint64
	Type        int32
	Payload     []byte
}

const (
	TSOBaselineNormalEntry int32 = iota
	TSOBaselineConfChangeEntry
	TSOBaselineConfChangeV2Entry
)

type tsoBaselineCursor struct {
	state                      *TSOStateMachine
	cut                        TSOBaselineCut
	index, term, configuration uint64
	failed                     error
}

type TSOBaselineReplay func(yield func(TSOBaselineEntry) error) error

type TSOBaselineState struct {
	ScalarSnapshot                              []byte
	DataDigest                                  raftbootstrap.Digest
	AppliedIndex, ConfigurationIndex, Watermark uint64
}

// ReconstructTSOBaseline is read-only and has no backing DB. The native caller
// must retain exclusive offline ownership for both snapshot and suffix reads.
// It reconstructs a new agreed cut, not proof of historical correctness.
func ReconstructTSOBaseline(ctx context.Context, snapshot io.Reader, cut TSOBaselineCut, replay TSOBaselineReplay) (TSOBaselineState, error) {
	var zero TSOBaselineState
	if err := validateTSOBaselineCut(cut); err != nil {
		return zero, err
	}
	state := NewTSOStateMachine(nil)
	if err := restoreTSOBaselineSnapshot(ctx, state, snapshot, cut.SnapshotIndex); err != nil {
		return zero, err
	}
	cursor := tsoBaselineCursor{state: state, cut: cut, index: cut.SnapshotIndex, term: cut.SnapshotTerm}
	if err := cursor.replay(ctx, replay); err != nil {
		return zero, err
	}
	if err := ctx.Err(); err != nil {
		return zero, errors.WithStack(err)
	}
	if cursor.index != cut.CommitIndex || cursor.term != cut.CommitTerm || !tsoBaselineConfigurationCovered(cut, cursor.configuration) {
		return zero, errors.WithStack(ErrTSOBaseline)
	}
	return state.scalarBaseline(cut.CommitIndex, cut.ConfigurationIndex)
}

func (c *tsoBaselineCursor) replay(ctx context.Context, replay TSOBaselineReplay) error {
	if replay == nil {
		if c.index != c.cut.CommitIndex {
			return errors.WithStack(ErrTSOBaseline)
		}
		return nil
	}
	err := replay(func(entry TSOBaselineEntry) error { return c.apply(ctx, entry) })
	if err != nil || c.failed != nil {
		return errors.Join(ErrTSOBaseline, err, c.failed)
	}
	return nil
}

func (c *tsoBaselineCursor) apply(ctx context.Context, entry TSOBaselineEntry) error {
	if c.failed != nil {
		return c.failed
	}
	c.failed = applyTSOBaselineEntry(ctx, c.state, c.cut, entry, c.index, c.term)
	if c.failed != nil {
		return c.failed
	}
	c.index, c.term = entry.Index, entry.Term
	if entry.Type != TSOBaselineNormalEntry {
		c.configuration = entry.Index
	}
	return nil
}

func validateTSOBaselineCut(cut TSOBaselineCut) error {
	if cut.CommitIndex == 0 || cut.CommitTerm == 0 || cut.LastLogIndex != cut.CommitIndex || cut.SnapshotIndex > cut.CommitIndex {
		return errors.WithStack(ErrTSOBaseline)
	}
	if cut.ConfigurationIndex == 0 || cut.ConfigurationIndex > cut.CommitIndex || (cut.SnapshotIndex == 0) != (cut.SnapshotTerm == 0) {
		return errors.WithStack(ErrTSOBaseline)
	}
	return nil
}

func restoreTSOBaselineSnapshot(ctx context.Context, state *TSOStateMachine, snapshot io.Reader, index uint64) error {
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	if snapshot == nil {
		if index == 0 {
			return nil
		}
		return errors.WithStack(ErrTSOBaseline)
	}
	// Dedicated legacy scalar snapshots are at most V5. In particular, do not
	// drain/ignore a KV body or import a protected EKVTSO06 control snapshot.
	payload, err := io.ReadAll(io.LimitReader(snapshot, tsoSnapshotV5Len+1))
	if err != nil {
		return errors.WithStack(err)
	}
	if index == 0 {
		if len(payload) != 0 {
			return errors.WithStack(ErrTSOBaseline)
		}
		return nil
	}
	_, _, _, _, _, _, err = readTSOSnapshotState(tsoSnapshotReader(bytes.NewReader(payload)))
	if err != nil {
		return errors.Join(ErrTSOBaseline, err)
	}
	return state.Restore(bytes.NewReader(payload))
}

func applyTSOBaselineEntry(ctx context.Context, state *TSOStateMachine, cut TSOBaselineCut, entry TSOBaselineEntry, previous, term uint64) error {
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	if !validTSOBaselineEntry(cut, entry, previous, term) {
		return errors.WithStack(ErrTSOBaseline)
	}
	switch entry.Type {
	case TSOBaselineNormalEntry:
		return applyTSOBaselinePayload(state, entry.Payload)
	case TSOBaselineConfChangeEntry, TSOBaselineConfChangeV2Entry:
		if len(entry.Payload) == 0 {
			return errors.WithStack(ErrTSOBaseline)
		}
		return nil // Native owns exact ConfChange decoding and roles/roster proof.
	default:
		return errors.WithStack(ErrTSOBaseline)
	}
}

func validTSOBaselineEntry(cut TSOBaselineCut, entry TSOBaselineEntry, previous, term uint64) bool {
	return previous < cut.CommitIndex && entry.Index == previous+1 && entry.Term != 0 && entry.Term >= term && entry.Term <= cut.CommitTerm
}

func applyTSOBaselinePayload(state *TSOStateMachine, payload []byte) error {
	if len(payload) == 0 {
		return nil
	}
	response := state.Apply(payload)
	if response == nil {
		return nil
	}
	if halt, ok := response.(*haltApplyResponse); ok {
		return errors.Join(ErrTSOBaseline, halt.HaltApply())
	}
	return errors.WithStack(ErrTSOBaseline)
}

func tsoBaselineConfigurationCovered(cut TSOBaselineCut, suffixConfiguration uint64) bool {
	if suffixConfiguration != 0 {
		return suffixConfiguration == cut.ConfigurationIndex
	}
	return cut.ConfigurationIndex <= cut.SnapshotIndex
}

// ScalarBaseline is also the destination check after explicit migration. It
// excludes P/preparation/journals, which the migration owner verifies separately.
func (f *TSOStateMachine) ScalarBaseline(applied, configuration uint64) (TSOBaselineState, error) {
	if f == nil {
		return TSOBaselineState{}, errors.WithStack(ErrTSOBaseline)
	}
	f.controlMu.Lock()
	defer f.controlMu.Unlock()
	return f.scalarBaseline(applied, configuration)
}

func (f *TSOStateMachine) scalarBaseline(applied, configuration uint64) (TSOBaselineState, error) {
	var zero TSOBaselineState
	if applied == 0 || configuration == 0 || configuration > applied || f.PendingPhaseDFloor() != 0 {
		return zero, errors.WithStack(ErrTSOBaseline)
	}
	// Force the full V4 scalar layout even for lease-only state: shortening to
	// V1 would synthesize a lease-derived allocation floor during import.
	payload := encodeTSOControlState(f, applied, configuration)[:tsoSnapshotV5Len]
	if f.pendingPhaseDFloor.Load() == 0 {
		payload = payload[:tsoSnapshotV4Len]
	}
	h := sha256.New()
	_, _ = h.Write([]byte("elastickv/tso-baseline/scalars/v1\x00"))
	_, _ = h.Write(payload)
	var digest raftbootstrap.Digest
	copy(digest[:], h.Sum(nil))
	return TSOBaselineState{ScalarSnapshot: payload, DataDigest: digest, AppliedIndex: applied, ConfigurationIndex: configuration, Watermark: f.AllocationFloor()}, nil
}
