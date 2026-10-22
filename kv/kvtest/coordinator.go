// Package kvtest supplies coordinators for tests outside package kv.
package kvtest

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"sync"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// NopCoordinator implements only Coordinator; it always reports local leadership.
// The zero value is ready for use, including concurrent Clock calls.
type NopCoordinator struct {
	once  sync.Once
	clock *kv.HLC
}

func (*NopCoordinator) Dispatch(context.Context, *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	return &kv.CoordinateResponse{}, nil
}
func (*NopCoordinator) IsLeader() bool                                   { return true }
func (*NopCoordinator) VerifyLeader(context.Context) error               { return nil }
func (*NopCoordinator) LinearizableRead(context.Context) (uint64, error) { return 0, nil }
func (*NopCoordinator) RaftLeader() string                               { return "local" }
func (*NopCoordinator) IsLeaderForKey([]byte) bool                       { return true }
func (*NopCoordinator) VerifyLeaderForKey(context.Context, []byte) error { return nil }
func (*NopCoordinator) RaftLeaderForKey([]byte) string                   { return "local" }
func (c *NopCoordinator) Clock() *kv.HLC {
	c.once.Do(func() { c.clock = kv.NewHLC() })
	return c.clock
}

type PrefixMode uint8

const (
	PrefixReject PrefixMode = iota
	// PrefixApply permits standalone, non-transactional prefix deletes.
	PrefixApply
	PrefixIgnore
)

type TimestampSource uint8

const (
	TimestampHLC TimestampSource = iota
	TimestampStoreIncrement
	TimestampRequest
)

type OCCMode uint8

const (
	OCCDisabled OCCMode = iota
	OCCTransactions
	OCCAllGroups
)

// LocalOptions explicitly selects behaviours that historically differed among
// local fakes. Zero values reject prefix deletes, use the HLC, skip OCC and do
// not patch values. OCC-disabled writes are applied sequentially, like the old
// adapter fake; OCC-enabled groups go through the store's atomic validation.
type LocalOptions struct {
	DelPrefix        PrefixMode
	CommitTimestamp  TimestampSource
	OCC              OCCMode
	ValidateReadKeys bool
	PatchCommitTS    bool
	// AdvanceCommitTS raises caller-supplied timestamps to at least store+1
	// and, for transactions, StartTS+1 (the filesystem fake's policy).
	AdvanceCommitTS bool
}

func (o LocalOptions) valid() bool {
	return o.DelPrefix <= PrefixIgnore && o.CommitTimestamp <= TimestampRequest && o.OCC <= OCCAllGroups
}

type LocalCoordinator struct {
	NopCoordinator
	mu      sync.Mutex
	st      store.MVCCStore
	options LocalOptions
}

func NewLocalCoordinator(st store.MVCCStore, options LocalOptions) *LocalCoordinator {
	return &LocalCoordinator{st: st, options: options}
}

func (c *LocalCoordinator) LinearizableRead(ctx context.Context) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.st.LastCommitTS(), errors.WithStack(ctx.Err())
}

func (c *LocalCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, errors.WithStack(err)
	}
	if req == nil {
		return &kv.CoordinateResponse{}, nil
	}
	if err := c.validate(req); err != nil {
		return nil, err
	}
	ts, err := c.commitTS(req)
	if err != nil {
		return nil, err
	}
	elems, err := c.copyElems(req.Elems, ts)
	if err != nil {
		return nil, err
	}
	if c.options.OCC == OCCAllGroups || (c.options.OCC == OCCTransactions && req.IsTxn) {
		err = c.applyOCC(ctx, req, elems, ts)
	} else {
		err = c.applySequential(ctx, elems, ts)
	}
	if err != nil {
		return nil, err
	}
	return &kv.CoordinateResponse{CommitTS: ts, CommitIndex: ts}, nil
}

func (c *LocalCoordinator) validate(req *kv.OperationGroup[kv.OP]) error {
	if !c.options.valid() {
		return errors.Wrap(kv.ErrInvalidRequest, "invalid local coordinator options")
	}
	prefixes, points, err := operationShape(req.Elems)
	if err != nil {
		return err
	}
	if !prefixes || c.options.DelPrefix == PrefixIgnore {
		return nil
	}
	if c.options.DelPrefix == PrefixReject {
		return errors.WithStack(store.ErrUnknownOp)
	}
	if req.IsTxn || points || c.options.OCC == OCCAllGroups {
		return errors.Wrap(kv.ErrInvalidRequest, "prefix deletes must be standalone and non-transactional")
	}
	return nil
}

func operationShape(elems []*kv.Elem[kv.OP]) (bool, bool, error) {
	var prefixes, points bool
	for _, elem := range elems {
		if elem == nil {
			continue
		}
		switch elem.Op {
		case kv.Put, kv.Del:
			points = true
		case kv.DelPrefix:
			prefixes = true
		default:
			return false, false, errors.WithStack(store.ErrUnknownOp)
		}
	}
	return prefixes, points, nil
}

func (c *LocalCoordinator) commitTS(req *kv.OperationGroup[kv.OP]) (uint64, error) {
	if req.IsTxn && req.StartTS == math.MaxUint64 {
		return 0, errors.WithStack(kv.ErrTxnCommitTSRequired)
	}
	ts, err := c.requestCommitTS(req)
	if err != nil {
		return 0, err
	}
	if c.options.AdvanceCommitTS {
		ts, err = c.raiseCommitTS(req, ts)
		if err != nil {
			return 0, err
		}
	}
	if req.IsTxn && ts <= req.StartTS {
		return 0, errors.WithStack(kv.ErrTxnCommitTSRequired)
	}
	c.Clock().Observe(ts)
	return ts, nil
}

func (c *LocalCoordinator) requestCommitTS(req *kv.OperationGroup[kv.OP]) (uint64, error) {
	if req.CommitTS != 0 {
		return req.CommitTS, nil
	}
	switch c.options.CommitTimestamp {
	case TimestampHLC:
		if req.IsTxn {
			c.Clock().Observe(req.StartTS)
		}
		return c.Clock().Next(), nil
	case TimestampStoreIncrement:
		return incrementTimestamp(c.st.LastCommitTS())
	case TimestampRequest:
		return 0, errors.WithStack(kv.ErrTxnCommitTSRequired)
	default:
		return 0, errors.WithStack(kv.ErrInvalidRequest)
	}
}

func (c *LocalCoordinator) raiseCommitTS(req *kv.OperationGroup[kv.OP], ts uint64) (uint64, error) {
	floor := c.st.LastCommitTS()
	if req.IsTxn && req.StartTS > floor {
		floor = req.StartTS
	}
	if ts <= floor {
		return incrementTimestamp(floor)
	}
	return ts, nil
}

func incrementTimestamp(ts uint64) (uint64, error) {
	if ts == math.MaxUint64 {
		return 0, errors.WithStack(kv.ErrTxnCommitTSRequired)
	}
	return ts + 1, nil
}

func (c *LocalCoordinator) copyElems(input []*kv.Elem[kv.OP], ts uint64) ([]*kv.Elem[kv.OP], error) {
	if c.options.PatchCommitTS {
		if err := kv.ValidateElemCommitTSPatches(input, ts); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	out := make([]*kv.Elem[kv.OP], 0, len(input))
	for _, elem := range input {
		if elem == nil {
			continue
		}
		copied := *elem
		copied.Key, copied.Value = bytes.Clone(elem.Key), bytes.Clone(elem.Value)
		if c.options.PatchCommitTS && copied.CommitTSValueOffset != 0 {
			binary.BigEndian.PutUint64(copied.Value[copied.CommitTSValueOffset:], ts)
		}
		out = append(out, &copied)
	}
	return out, nil
}

func (c *LocalCoordinator) applyOCC(ctx context.Context, req *kv.OperationGroup[kv.OP], elems []*kv.Elem[kv.OP], ts uint64) error {
	mutations := make([]*store.KVPairMutation, 0, len(elems))
	for _, elem := range elems {
		switch elem.Op {
		case kv.Put:
			mutations = append(mutations, &store.KVPairMutation{Op: store.OpTypePut, Key: elem.Key, Value: elem.Value})
		case kv.Del:
			mutations = append(mutations, &store.KVPairMutation{Op: store.OpTypeDelete, Key: elem.Key})
		case kv.DelPrefix:
			// PrefixIgnore was validated before entering this path.
		}
	}
	var reads [][]byte
	if c.options.ValidateReadKeys {
		reads = req.ReadKeys
	}
	return errors.Wrap(c.st.ApplyMutations(ctx, mutations, reads, req.StartTS, ts), "apply local OCC group")
}

func (c *LocalCoordinator) applySequential(ctx context.Context, elems []*kv.Elem[kv.OP], ts uint64) error {
	for _, elem := range elems {
		var err error
		switch elem.Op {
		case kv.Put:
			err = c.st.PutAt(ctx, elem.Key, elem.Value, ts, 0)
		case kv.Del:
			err = c.st.DeleteAt(ctx, elem.Key, ts)
		case kv.DelPrefix:
			if c.options.DelPrefix == PrefixApply {
				err = c.st.DeletePrefixAt(ctx, elem.Key, nil, ts)
			}
		}
		if err != nil {
			return errors.Wrap(err, "apply local operation")
		}
	}
	return nil
}

var (
	_ kv.Coordinator = (*NopCoordinator)(nil)
	_ kv.Coordinator = (*LocalCoordinator)(nil)
)
