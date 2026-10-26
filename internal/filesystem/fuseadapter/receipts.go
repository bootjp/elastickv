package fuseadapter

import (
	"context"
	"log/slog"
	"syscall"

	"github.com/bootjp/elastickv/internal/filesystem"
)

type operationReceiptVerifier interface {
	VerifyOperationReceipt(context.Context, filesystem.OperationID) error
}

func (a *Adapter) recordReceipt(inode uint64, id filesystem.OperationID) {
	a.receiptMu.Lock()
	defer a.receiptMu.Unlock()
	if a.receipts == nil {
		a.receipts = make(map[uint64]map[filesystem.OperationID]struct{})
	}
	if a.receipts[inode] == nil {
		a.receipts[inode] = make(map[filesystem.OperationID]struct{})
	}
	a.receipts[inode][id] = struct{}{}
}

// retainLatestLeaseReceipt drops older lease receipts only after their
// participant proofs have been checked. Failed checks leave them for fsync.
func (a *Adapter) retainLatestLeaseReceipt(ctx context.Context, inode, fh uint64, id filesystem.OperationID) {
	a.leaseReceiptMu.Lock()
	defer a.leaseReceiptMu.Unlock()
	key := openHandleKey{inode: inode, fh: fh}
	if a.leaseReceipts == nil {
		a.leaseReceipts = make(map[openHandleKey][]filesystem.OperationID)
	}
	previous := a.leaseReceipts[key]
	a.leaseReceipts[key] = append(previous, id)
	verifier, ok := a.core.(operationReceiptVerifier)
	if !ok {
		return
	}
	for _, old := range previous {
		if err := verifier.VerifyOperationReceipt(ctx, old); err != nil {
			slog.WarnContext(ctx, "filesystem lease receipt verification failed", "inode", inode, "fh", fh, "error", err.Error())
			return
		}
	}
	a.clearReceipts(inode, previous)
	a.leaseReceipts[key] = []filesystem.OperationID{id}
}

func (a *Adapter) verifyReceipts(ctx context.Context, inode uint64) ([]filesystem.OperationID, syscall.Errno) {
	a.receiptMu.Lock()
	receipts := make([]filesystem.OperationID, 0, len(a.receipts[inode]))
	for id := range a.receipts[inode] {
		receipts = append(receipts, id)
	}
	a.receiptMu.Unlock()
	if len(receipts) == 0 {
		return receipts, 0
	}
	verifier, ok := a.core.(operationReceiptVerifier)
	if !ok {
		return nil, syscall.EOPNOTSUPP
	}
	for _, id := range receipts {
		if err := verifier.VerifyOperationReceipt(ctx, id); err != nil {
			return nil, Errno(err)
		}
	}
	return receipts, 0
}

func (a *Adapter) clearReceipts(inode uint64, receipts []filesystem.OperationID) {
	a.receiptMu.Lock()
	defer a.receiptMu.Unlock()
	for _, id := range receipts {
		delete(a.receipts[inode], id)
	}
	if len(a.receipts[inode]) == 0 {
		delete(a.receipts, inode)
	}
}
