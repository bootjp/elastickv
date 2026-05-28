package keyviz

// Space-Saving sketch (Metwally et al.) — Misra–Gries-equivalent
// heavy-hitter detector backing the per-cell hot-key drill-down (Phase
// 2-A++; see docs/design/2026_05_28_proposed_keyviz_hot_key_topk.md
// §3). Maintains at most `capacity` (= `m`) entries; a key with
// frequency f over the observed stream of length N is reported with
// error ≤ N/m, and any key with f > N/m is guaranteed in the tracked
// set (over that stream). v1 keys the bookkeeping on the byte-string
// form of the key; an O(m) scan finds the min on eviction, which is
// cheap at the target m ≤ 256 and avoids a separate heap structure.
//
// NOT thread-safe: the hot-keys aggregator goroutine
// (`hot_keys.go::aggregator`) is the single writer and the only
// caller of observe / reset / snapshot. The published deep-copied
// `keyvizHotKeysSnapshot` is what drill-down readers see, lock-free
// via the slot's atomic.Pointer.

type spaceSaving struct {
	capacity int
	entries  map[string]*ssEntry
}

type ssEntry struct {
	// Key bytes owned by this sketch — independent of the pool buffer
	// the hot path used to ferry the key into the aggregator, so the
	// pool buffer can be released as soon as observe returns.
	key   []byte
	count uint64
}

// ssEntrySnap is the deep-copied form returned by snapshot. The Key
// slice is freshly allocated, so a subsequent reset / eviction on the
// live sketch cannot mutate a published snapshot (Gemini medium on the
// design's round-1).
type ssEntrySnap struct {
	Key   []byte
	Count uint64
}

func newSpaceSaving(capacity int) *spaceSaving {
	if capacity < 1 {
		capacity = 1
	}
	return &spaceSaving{
		capacity: capacity,
		entries:  make(map[string]*ssEntry, capacity),
	}
}

// observe records one occurrence of key. The aggregator passes the
// pool-borrowed buffer; this method copies the bytes into an
// SS-owned slice on insert/replace so the caller can release the
// pool buffer immediately afterwards (design §4 pool-vs-snapshot).
func (s *spaceSaving) observe(key []byte) {
	// map[string([]byte)] lookup is a compiler-special-cased zero-alloc
	// access; only the insert path actually allocates a string key.
	if e, ok := s.entries[string(key)]; ok {
		e.count++
		return
	}
	kc := append([]byte(nil), key...) // SS-owned copy
	if len(s.entries) < s.capacity {
		s.entries[string(kc)] = &ssEntry{key: kc, count: 1}
		return
	}
	// Capacity reached: evict the entry with the smallest counter and
	// reuse it for the new key. The new counter is min_counter + 1
	// (Space-Saving's overestimate-on-insert rule, which bounds error
	// at N/m).
	var minE *ssEntry
	var minK string
	for k, e := range s.entries {
		if minE == nil || e.count < minE.count {
			minE = e
			minK = k
		}
	}
	delete(s.entries, minK)
	minE.key = kc
	minE.count++
	s.entries[string(kc)] = minE
}

// snapshot returns a deep-copied list of every tracked (key, count)
// pair. The aggregator calls it under its tick before publishing; the
// returned slice never aliases the live sketch's storage, so an
// eviction or reset after publish is safe.
func (s *spaceSaving) snapshot() []ssEntrySnap {
	if len(s.entries) == 0 {
		return nil
	}
	out := make([]ssEntrySnap, 0, len(s.entries))
	for _, e := range s.entries {
		out = append(out, ssEntrySnap{
			Key:   append([]byte(nil), e.key...),
			Count: e.count,
		})
	}
	return out
}

// reset clears every entry. Called by the aggregator immediately after
// snapshot publish so the next keyvizStep window starts empty
// (design §4 sketch reset; Codex P2 L186). Capacity is preserved.
func (s *spaceSaving) reset() {
	if len(s.entries) == 0 {
		return
	}
	s.entries = make(map[string]*ssEntry, s.capacity)
}

// len returns the current number of tracked entries (≤ capacity).
// Test helper; not used by production code.
func (s *spaceSaving) len() int { return len(s.entries) }
