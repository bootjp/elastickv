package filesystem

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type operationPublicationConflictDispatcher struct {
	inner     *testCoordinator
	conflicts int
}

type operationMetadataConflictDispatcher struct {
	inner     *testCoordinator
	remaining int
	match     func(*kv.OperationGroup[kv.OP]) bool
}

func (d *operationMetadataConflictDispatcher) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	if d.remaining > 0 && d.match(req) {
		d.remaining--
		return nil, store.ErrWriteConflict
	}
	return d.inner.Dispatch(ctx, req)
}

func TestOperationMetadataConflictsDoNotReportQuota(t *testing.T) {
	ctx := t.Context()
	svc := newTestServiceWithOptions(t, []uint64{2})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	id := testWriteOperationID(t, svc, 1)
	inner, ok := svc.dispatch.(*testCoordinator)
	require.True(t, ok)
	fault := &operationMetadataConflictDispatcher{inner: inner, remaining: 8, match: func(req *kv.OperationGroup[kv.OP]) bool {
		for _, elem := range req.Elems {
			if elem != nil && fskeys.IsOperationKey(elem.Key) && validOperationHeader(elem.Value) &&
				OperationState(elem.Value[5]) == OperationPending {
				return true
			}
		}
		return false
	}}
	svc.dispatch = fault
	journal, err := NewOperationJournal(svc)
	require.NoError(t, err)
	_, err = journal.Admit(ctx, id, []byte("intent"), 1, 1)
	require.NoError(t, err)
	require.Zero(t, fault.remaining)
	svc.dispatch = inner
	_, err = journal.CancelWithResult(ctx, id, []byte{1})
	require.NoError(t, err)
	fault = &operationMetadataConflictDispatcher{inner: inner, remaining: 8, match: func(req *kv.OperationGroup[kv.OP]) bool {
		for _, elem := range req.Elems {
			if elem != nil && elem.Op == kv.Del && fskeys.IsOperationKey(elem.Key) {
				return true
			}
		}
		return false
	}}
	svc.dispatch = fault
	require.NoError(t, journal.Acknowledge(ctx, id))
	require.Zero(t, fault.remaining)
}

func (d *operationPublicationConflictDispatcher) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	for _, elem := range req.Elems {
		if elem == nil || !fskeys.IsOperationKey(elem.Key) {
			continue
		}
		if validOperationHeader(elem.Value) &&
			(OperationState(elem.Value[5]) == OperationCommitted || OperationState(elem.Value[5]) == OperationRejected) {
			d.conflicts++
			return nil, store.ErrWriteConflict
		}
	}
	return d.inner.Dispatch(ctx, req)
}

func TestHandleContentionIsTerminalAndReplayable(t *testing.T) {
	tests := []struct {
		name string
		run  func(*Service, context.Context, OperationID, uint64, uint64, []byte) error
	}{
		{"lease", (*Service).RefreshOpenHandleLeaseWithOperation},
		{"release", (*Service).ReleaseWithOperation},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			svc := newTestServiceWithOptions(t, []uint64{2, 3})
			require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
			file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
			require.NoError(t, err)
			clientID := []byte("mount")
			fh, err := svc.Open(ctx, file.Inode, clientID)
			require.NoError(t, err)
			id := testWriteOperationID(t, svc, 1)
			inner, ok := svc.dispatch.(*testCoordinator)
			require.True(t, ok)
			fault := &operationPublicationConflictDispatcher{inner: inner}
			svc.dispatch = fault
			require.ErrorIs(t, tt.run(svc, ctx, id, file.Inode, fh, clientID), ErrContentionLimit)
			require.Equal(t, writeConflictRetryLimit, fault.conflicts)
			svc.dispatch = inner
			require.ErrorIs(t, tt.run(svc, ctx, id, file.Inode, fh, clientID), ErrContentionLimit)
			journal, err := NewOperationJournal(svc)
			require.NoError(t, err)
			record, err := journal.Resolve(ctx, id)
			require.NoError(t, err)
			require.Equal(t, OperationCancelled, record.State)
			require.NoError(t, journal.CloseSession(ctx, OperationSessionID{Filesystem: id.Filesystem, Session: id.Session}))
		})
	}
}

func TestOperationContentionResultsRemainTyped(t *testing.T) {
	createResult, err := json.Marshal(createOperationResult{Version: createOperationVersion, Error: createResultContention})
	require.NoError(t, err)
	openResult, err := json.Marshal(openOperationResult{Version: openOperationVersion, Error: openResultContention})
	require.NoError(t, err)
	setAttrResult, err := json.Marshal(setAttrOperationResult{Version: setAttrOperationVersion, Error: setAttrResultContention})
	require.NoError(t, err)
	tests := []struct {
		name   string
		result []byte
		decode func(operationRecord) error
	}{
		{"create", createResult, func(r operationRecord) error { _, err := decodeCreateOperationResult(r); return err }},
		{"open", openResult, func(r operationRecord) error { _, err := decodeOpenOperationResult(r); return err }},
		{"setattr", setAttrResult, func(r operationRecord) error { _, err := decodeSetAttrOperationResult(r); return err }},
		{"rename", []byte{renameResultContention}, decodeRenameOperationResult},
		{"unlink", []byte{unlinkResultContention}, decodeUnlinkOperationResult},
		{"lease", []byte{leaseResultContention}, decodeLeaseOperationResult},
		{"release", []byte{releaseResultContention}, decodeReleaseOperationResult},
		{"write", encodeWriteOperationResult(0, writeResultPlanContention), func(r operationRecord) error { _, err := decodeWriteOperationResult(r); return err }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := operationRecord{State: OperationCancelled, Result: tt.result}
			require.ErrorIs(t, tt.decode(record), ErrContentionLimit)
			record.State = OperationRejected
			require.ErrorIs(t, tt.decode(record), ErrOperationRecordCorrupt)
		})
	}
}
