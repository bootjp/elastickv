package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"math"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

const BootstrapBaselineProbeVersion uint32 = 1

var (
	ErrBootstrapBaselineUnsupported = errors.New("store: unsupported baseline probe")
	ErrBootstrapBaselinePending     = errors.New("store: baseline contains unresolved transaction or maintenance state")
	ErrBootstrapBaselineInvalid     = errors.New("store: invalid baseline image")
)

// BootstrapBaselineProbe describes ONE local retained image, not historical
// correctness, quorum agreement, native WAL provenance or admission authority.
// Missing configuration/watermark projections are explicit; never infer their
// values from a local maximum. The native probe must independently match the
// applied cut and configuration before anyone signs baseline evidence.
type BootstrapBaselineProbe struct {
	Version                   uint32
	DataDigest                [sha256.Size]byte
	Watermark                 uint64
	WatermarkPresent          bool
	AppliedIndex              uint64
	ConfigurationIndex        uint64
	ConfigurationIndexPresent bool
	MVCCVersions              uint64
	MetadataRecords           uint64
}

type baselineTxnHead struct {
	ts        uint64
	tombstone bool
}

type baselineProbeReader struct {
	result        BootstrapBaselineProbe
	hash          hash.Hash
	txnHeads      map[string]baselineTxnHead
	configuration baselineTxnHead
}

type baselineProbeIterator interface {
	First() bool
	Next() bool
	Key() []byte
	ValueAndErr() ([]byte, error)
	Error() error
}

// ProbeBootstrapBaseline requires caller-owned quiescence and exclusive store
// ownership, spanning BOTH this probe and the native WAL/snapshot probe. It does
// not open/replay a database, sync, flush, compact, write a checkpoint, or repair
// anything. Use an already-open, read-only offline store; opening it is outside
// this function's contract. Only the in-tree Pebble backend is supported.
//
// Every retained MVCC version, tombstone and absolute TTL participates, including
// expired versions and transaction history. ALL raw metadata participates too:
// retention, migration, applied index, protected control records, writer registry
// and encryption checkpoint. The exclusion allowlist is deliberately EMPTY.
// Writer registry rows are replicated, and the checkpoint witnesses replicated
// encryption state; neither is proven node-local. Sidecar nonce counters are
// outside the DB image. Encrypted stores/values are explicitly unsupported, not
// compared by nondeterministic ciphertext or silently omitted.
func ProbeBootstrapBaseline(ctx context.Context, st MVCCStore) (BootstrapBaselineProbe, error) {
	if ctx == nil {
		return BootstrapBaselineProbe{}, errors.WithStack(ErrBootstrapBaselineInvalid)
	}
	if err := ctx.Err(); err != nil {
		return BootstrapBaselineProbe{}, errors.WithStack(err)
	}
	s, ok := st.(*pebbleStore)
	if !ok || s == nil {
		return BootstrapBaselineProbe{}, errors.WithStack(ErrBootstrapBaselineUnsupported)
	}
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if s.db == nil || s.cipher != nil {
		return BootstrapBaselineProbe{}, errors.WithStack(ErrBootstrapBaselineUnsupported)
	}
	snapshot := s.db.NewSnapshot()
	result, err := ProbeBootstrapBaselineSnapshot(ctx, snapshot)
	if err = errors.Join(err, snapshot.Close()); err != nil {
		return BootstrapBaselineProbe{}, errors.WithStack(err)
	}
	return result, nil
}

// ProbeBootstrapBaselineSnapshot reads the entire pinned Pebble image. The
// caller owns the snapshot/DB lifetime and the same offline exclusivity contract
// as ProbeBootstrapBaseline. It neither opens the DB nor closes the snapshot.
// This permits a native offline reader to supply a snapshot from a read-only
// Pebble open without constructing a write-capable MVCCStore. Encrypted rows
// remain unsupported. No metadata (including encryption metadata) is excluded.
func ProbeBootstrapBaselineSnapshot(ctx context.Context, snapshot *pebble.Snapshot) (BootstrapBaselineProbe, error) {
	if ctx == nil || snapshot == nil {
		return BootstrapBaselineProbe{}, errors.WithStack(ErrBootstrapBaselineInvalid)
	}
	if err := ctx.Err(); err != nil {
		return BootstrapBaselineProbe{}, errors.WithStack(err)
	}
	reader := baselineProbeReader{
		result: BootstrapBaselineProbe{Version: BootstrapBaselineProbeVersion},
		hash:   sha256.New(), txnHeads: make(map[string]baselineTxnHead),
	}
	_, _ = reader.hash.Write([]byte("elastickv/offline-full-mvcc-baseline/v1\x00"))
	iter, err := snapshot.NewIter(nil)
	if err != nil {
		return BootstrapBaselineProbe{}, errors.WithStack(err)
	}
	err = reader.scan(ctx, iter)
	if err = errors.Join(err, iter.Close()); err != nil {
		return BootstrapBaselineProbe{}, errors.WithStack(err)
	}
	if err := reader.finish(ctx); err != nil {
		return BootstrapBaselineProbe{}, err
	}
	copy(reader.result.DataDigest[:], reader.hash.Sum(nil))
	return reader.result, nil
}

func (r *baselineProbeReader) scan(ctx context.Context, iter baselineProbeIterator) error {
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return errors.WithStack(err)
		}
		key := iter.Key()
		value, err := iter.ValueAndErr()
		if err != nil {
			return errors.Wrap(err, "baseline iterator value")
		}
		if isPebbleMetaKey(key) {
			if err := r.metadata(key, value); err != nil {
				return err
			}
			r.result.MetadataRecords++
		} else {
			if err := r.version(key, value); err != nil {
				return err
			}
			r.result.MVCCVersions++
		}
		// Pebble bytewise iteration and length framing give a layout-independent,
		// unambiguous stream. Do not hash SST bytes or filter via GetAt/ScanAt.
		r.field(key)
		r.field(value)
	}
	return errors.WithStack(iter.Error())
}

func (r *baselineProbeReader) field(value []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = r.hash.Write(size[:])
	_, _ = r.hash.Write(value)
}

func (r *baselineProbeReader) metadata(key, value []byte) error {
	switch string(key) {
	case metaAppliedIndex:
		index, err := baselineUint64(value)
		if err != nil || index == 0 {
			return errors.Wrap(ErrBootstrapBaselineInvalid, "missing or malformed durable applied index")
		}
		r.result.AppliedIndex = index
	case metaLastCommitTS:
		watermark, err := baselineUint64(value)
		if err != nil || watermark == math.MaxUint64 {
			return errors.Wrap(ErrBootstrapBaselineInvalid, "malformed global watermark")
		}
		r.result.Watermark, r.result.WatermarkPresent = watermark, true
	case metaMinRetainedTS, metaPendingMinRetainedTS:
		return baselineRetention(key, value)
	}
	return nil
}

func baselineUint64(value []byte) (uint64, error) {
	if len(value) != timestampSize {
		return 0, errors.Wrap(ErrBootstrapBaselineInvalid, "invalid metadata uint64")
	}
	return binary.LittleEndian.Uint64(value), nil
}

func baselineRetention(key, value []byte) error {
	boundary, err := baselineUint64(value)
	if err != nil {
		return err
	}
	if string(key) == metaPendingMinRetainedTS && boundary != 0 {
		return errors.Wrap(ErrBootstrapBaselinePending, "pending retention boundary")
	}
	return nil
}

func (r *baselineProbeReader) version(encodedKey, value []byte) error {
	if len(encodedKey) < timestampSize {
		return errors.Wrap(ErrBootstrapBaselineInvalid, "truncated MVCC key")
	}
	key, ts := decodeKeyView(encodedKey)
	decoded, err := decodeValueView(value)
	if err != nil {
		return errors.Wrap(err, "baseline MVCC value")
	}
	if decoded.EncState != encStateCleartext {
		return errors.Wrap(ErrBootstrapBaselineUnsupported, "encrypted MVCC value")
	}
	if bytes.Equal(key, MigrationConfigurationIndexKey()) {
		return r.configurationVersion(ts, decoded)
	}
	return r.transactionVersion(key, ts, decoded.Tombstone)
}

func (r *baselineProbeReader) configurationVersion(ts uint64, value storedValue) error {
	if r.result.ConfigurationIndexPresent && ts <= r.configuration.ts {
		return nil
	}
	if value.Tombstone || value.ExpireAt != 0 {
		return errors.Wrap(ErrBootstrapBaselineInvalid, "deleted or expiring configuration projection")
	}
	if len(value.Value) != timestampSize {
		return errors.Wrap(ErrBootstrapBaselineInvalid, "malformed configuration projection")
	}
	index := binary.BigEndian.Uint64(value.Value)
	if index == 0 {
		return errors.Wrap(ErrBootstrapBaselineInvalid, "malformed configuration projection")
	}
	r.configuration = baselineTxnHead{ts: ts}
	r.result.ConfigurationIndex, r.result.ConfigurationIndexPresent = index, true
	return nil
}

func (r *baselineProbeReader) transactionVersion(key []byte, ts uint64, tombstone bool) error {
	if baselinePendingKey(key) {
		// Encoded keys with prefix-related logical names need not be contiguous.
		// Track the true newest version, rather than treating First as latest.
		name := string(key)
		if old, exists := r.txnHeads[name]; !exists || ts > old.ts {
			r.txnHeads[name] = baselineTxnHead{ts: ts, tombstone: tombstone}
		}
		return nil
	}
	if !bytes.HasPrefix(key, txnInternalKeyPrefix) {
		return nil
	}
	// TxnMeta is a wire-only synthetic mutation, not persisted transaction state.
	// Do not guess future/unknown transaction codecs or interpret them as settled.
	for _, prefix := range []string{"!txn|cmt|", "!txn|rb|", "!txn|ok|", "!txn|backup|"} {
		if bytes.HasPrefix(key, []byte(prefix)) {
			return nil
		}
	}
	return errors.Wrap(ErrBootstrapBaselineUnsupported, "unknown persisted transaction namespace")
}

func baselinePendingKey(key []byte) bool {
	// Migration shadow rows may contain prepared locks/intents. Without the
	// migration owner's resolution evidence, refuse live shadow state too.
	for _, prefix := range []string{"!txn|lock|", "!txn|int|", "!dist|migstage|", "!migstage|"} {
		if bytes.HasPrefix(key, []byte(prefix)) {
			return true
		}
	}
	return false
}

func (r *baselineProbeReader) finish(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	if r.result.AppliedIndex == 0 || r.result.ConfigurationIndex > r.result.AppliedIndex {
		return errors.Wrap(ErrBootstrapBaselineInvalid, "absent applied proof or configuration beyond applied cut")
	}
	for _, head := range r.txnHeads {
		if err := ctx.Err(); err != nil {
			return errors.WithStack(err)
		}
		if !head.tombstone {
			return errors.Wrap(ErrBootstrapBaselinePending, "live prepared lock or intent (TTL is not resolution evidence)")
		}
	}
	return nil
}
