package filesystem

import (
	"encoding/hex"
	"log/slog"
	"os"
	"slices"
	"sync/atomic"
)

const operationAccountingTraceWindow = 2048

var (
	operationAccountingTrace = os.Getenv("ELASTICKV_FS_ACCOUNTING_TRACE") == "1"
	operationTraceNext       atomic.Uint64
	operationTraceDumped     atomic.Bool
	operationTraceSlots      [operationAccountingTraceWindow]atomic.Pointer[operationAccountingEvent]
)

type operationAccountingEvent struct {
	Order         uint64
	Kind          string
	Session       [16]byte
	Sequence      uint64
	Generation    uint64
	State         OperationState
	Reservation   uint64
	Fences        int
	RetainedCount uint32
	RetainedBytes uint64
	Pending       uint32
	RejectBefore  uint64
	AckGaps       int
	SnapshotTS    uint64
	CommitTS      uint64
}

func traceOperationAccounting(kind string, snapshotTS, commitTS uint64, record operationRecord, session operationSession) {
	order := operationTraceNext.Add(1)
	event := &operationAccountingEvent{
		Order: order, Kind: kind, Session: session.ID.Session,
		Sequence: record.ID.Sequence, Generation: record.Generation, State: record.State,
		Reservation: record.Reservation, Fences: len(record.FencedAttempts),
		RetainedCount: session.RetainedCount, RetainedBytes: session.RetainedBytes,
		Pending: session.Pending, RejectBefore: session.RejectBefore, AckGaps: len(session.AckGaps),
		SnapshotTS: snapshotTS, CommitTS: commitTS,
	}
	operationTraceSlots[order%operationAccountingTraceWindow].Store(event)
}

func dumpOperationAccountingTrace(id OperationSessionID) {
	if !operationTraceDumped.CompareAndSwap(false, true) {
		return
	}
	events := make([]*operationAccountingEvent, 0, operationAccountingTraceWindow)
	for index := range operationTraceSlots {
		event := operationTraceSlots[index].Load()
		if event != nil && event.Session == id.Session {
			events = append(events, event)
		}
	}
	slices.SortFunc(events, func(a, b *operationAccountingEvent) int {
		switch {
		case a.Order < b.Order:
			return -1
		case a.Order > b.Order:
			return 1
		default:
			return 0
		}
	})
	slog.Warn("filesystem journal accounting trace begin", "session", hex.EncodeToString(id.Session[:]), "events", len(events))
	for _, event := range events {
		slog.Warn("filesystem journal accounting trace", "order", event.Order, "kind", event.Kind,
			"session", hex.EncodeToString(event.Session[:]), "sequence", event.Sequence,
			"generation", event.Generation, "state", event.State,
			"reservation", event.Reservation, "fences", event.Fences,
			"retained_count", event.RetainedCount, "retained_bytes", event.RetainedBytes,
			"pending", event.Pending, "reject_before", event.RejectBefore, "ack_gaps", event.AckGaps,
			"snapshot_ts", event.SnapshotTS, "commit_ts", event.CommitTS)
	}
	slog.Warn("filesystem journal accounting trace end", "session", hex.EncodeToString(id.Session[:]))
}
