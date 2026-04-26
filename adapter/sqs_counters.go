package adapter

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"time"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// sqsApproxCounters holds the per-state message counts AWS returns
// from GetQueueAttributes. AWS itself documents these as approximate;
// this implementation caps the visibility-index scan at
// sqsApproxCounterScanBudget records — beyond that, the counts are a
// lower bound and Truncated is true so callers can log the breach.
type sqsApproxCounters struct {
	Visible    int
	NotVisible int
	Delayed    int
	Truncated  bool
}

const (
	// Budget on how many vis-index entries one GetQueueAttributes call
	// touches. Tuned to keep the call well under a 100 ms wall-clock
	// budget on Pebble: each entry is one cheap key parse plus
	// (rarely) one point GetAt for the in-flight / delayed
	// disambiguation.
	sqsApproxCounterScanBudget = 5000
	sqsApproxCounterPageLimit  = 1024
)

// computeApproxCounters classifies every visibility-index entry of a
// queue at the supplied snapshot read TS into the three AWS states:
// visible (ready), not-visible (in-flight), delayed.
//
// Strategy:
//   - The vis-index key embeds the current visibility deadline as the
//     last fixed-width u64 segment, so visible_at can be extracted
//     from the key alone (no GetAt) for the common ready-message case.
//   - For entries with visible_at > now we need to disambiguate
//     "delayed (never received)" from "in-flight (received, vis
//     bumped)" by inspecting the data record's available_at_millis.
//     Both fields start equal on send and only diverge when receive
//     bumps visible_at, so available_at vs now is the distinguishing
//     signal.
//
// The function bounds the cost by capping at sqsApproxCounterScanBudget
// total entries inspected. Above that, Truncated=true and the caller
// can log a warning; AWS's "approximate" contract makes the lower
// bound legal.
func (s *SQSServer) computeApproxCounters(ctx context.Context, queueName string, gen uint64, readTS uint64) (sqsApproxCounters, error) {
	var out sqsApproxCounters
	now := time.Now().UnixMilli()
	prefix := sqsMsgVisPrefixForQueue(queueName, gen)
	upper := prefixScanEnd(prefix)
	visAtOffset := len(prefix)

	start := bytes.Clone(prefix)
	visited := 0
	for visited < sqsApproxCounterScanBudget {
		page, err := s.store.ScanAt(ctx, start, upper, sqsApproxCounterPageLimit, readTS)
		if err != nil {
			return out, errors.WithStack(err)
		}
		if len(page) == 0 {
			return out, nil
		}
		done, newVisited, err := s.classifyApproxCounterPage(ctx, queueName, gen, page, visAtOffset, now, readTS, visited, &out)
		if err != nil {
			return out, err
		}
		visited = newVisited
		if done {
			return out, nil
		}
		if len(page) < sqsApproxCounterPageLimit {
			return out, nil
		}
		start = nextScanCursorAfter(page[len(page)-1].Key)
		if bytes.Compare(start, upper) >= 0 {
			return out, nil
		}
	}
	out.Truncated = true
	return out, nil
}

// classifyApproxCounterPage processes one ScanAt page. Returns
// done=true when the per-call budget is exhausted (caller should mark
// Truncated and stop) or when the page itself is short. Split out so
// computeApproxCounters stays under the cyclop budget.
func (s *SQSServer) classifyApproxCounterPage(
	ctx context.Context,
	queueName string,
	gen uint64,
	page []*store.KVPair,
	visAtOffset int,
	now int64,
	readTS uint64,
	visited int,
	out *sqsApproxCounters,
) (bool, int, error) {
	for _, kvp := range page {
		if visited >= sqsApproxCounterScanBudget {
			out.Truncated = true
			return true, visited, nil
		}
		visited++
		if len(kvp.Key) < visAtOffset+8 {
			// Malformed key (shouldn't happen for keys we wrote).
			// Skip rather than fail the whole counter call.
			continue
		}
		visAtRaw := binary.BigEndian.Uint64(kvp.Key[visAtOffset : visAtOffset+8])
		// vis_at is encoded as uint64 wall-clock millis. Clamp on the
		// off chance a corrupted key carries a value past MaxInt64
		// before the int64 conversion — otherwise a hostile / corrupt
		// row would wrap to a negative value and silently land in the
		// "visible" bucket. Real timestamps stay well under MaxInt64
		// for the next ~292 million years, so this is purely
		// defence in depth.
		visAt := int64(math.MaxInt64)
		if visAtRaw <= math.MaxInt64 {
			visAt = int64(visAtRaw)
		}
		if visAt <= now {
			out.Visible++
			continue
		}
		// visible_at > now: either Delayed or NotVisible. Load the
		// data record so we can distinguish on available_at.
		state, err := s.classifyHiddenCandidate(ctx, queueName, gen, string(kvp.Value), now, readTS)
		if err != nil {
			return true, visited, err
		}
		switch state {
		case approxCounterDelayed:
			out.Delayed++
		case approxCounterNotVisible:
			out.NotVisible++
		case approxCounterSkipped:
			// race: data record gone between scan and load — skip.
		}
	}
	return false, visited, nil
}

type approxCounterClass int

const (
	approxCounterSkipped approxCounterClass = iota
	approxCounterDelayed
	approxCounterNotVisible
)

// classifyHiddenCandidate disambiguates a vis-index entry whose
// visible_at > now. Loads the data record once and decides based on
// its available_at:
//   - available_at > now → Delayed (never received, just not yet
//     eligible).
//   - available_at <= now → NotVisible (received, vis was bumped).
//
// On a vis/data race (data already deleted) we return Skipped so the
// counter does not double-charge a record the storage no longer
// agrees exists.
func (s *SQSServer) classifyHiddenCandidate(ctx context.Context, queueName string, gen uint64, messageID string, now int64, readTS uint64) (approxCounterClass, error) {
	dataKey := sqsMsgDataKey(queueName, gen, messageID)
	raw, err := s.store.GetAt(ctx, dataKey, readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return approxCounterSkipped, nil
		}
		return approxCounterSkipped, errors.WithStack(err)
	}
	rec, err := decodeSQSMessageRecord(raw)
	if err != nil {
		// A genuinely corrupt record on disk should *surface* through
		// the GetQueueAttributes call (a 500 the operator can act on)
		// rather than be silently bucketed into "skipped". The reaper
		// also catches this lazily, but here we have a chance to
		// alert immediately.
		return approxCounterSkipped, errors.Wrapf(err, "decode message record %s", messageID)
	}
	if rec.AvailableAtMillis > now {
		return approxCounterDelayed, nil
	}
	return approxCounterNotVisible, nil
}
