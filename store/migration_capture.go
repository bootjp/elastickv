package store

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"

	"github.com/cockroachdb/errors"
)

// MaxMigrationCaptureRequests bounds the explicit point-entry diagnostic.
const MaxMigrationCaptureRequests = 256

const (
	migrationCaptureVersion        = 1
	migrationCaptureDefaultBytes   = 256 << 10
	migrationCaptureMaxEffects     = 4096
	migrationCaptureEffectBytes    = 64
	migrationCaptureMinBytes       = 1024
	migrationCaptureMetadataPrefix = RaftControlReceiptKeyPrefix + "migration-capture/v1/"
)

var (
	ErrMigrationCaptureInput      = errors.New("migration capture: invalid entry")
	ErrMigrationCaptureConflict   = errors.New("migration capture: entry identity reused with different input")
	ErrMigrationCaptureStaleIndex = errors.New("migration capture: missing record for an already applied entry")
	ErrMigrationCaptureCorrupt    = errors.New("migration capture: invalid record")
)

// MigrationCaptureIdentity identifies one source entry, not a continuous stream
// or an ownership/activation witness. A new Raft lineage must use a new Lineage.
type MigrationCaptureIdentity struct {
	SourceGroupID uint64
	JobID         uint64
	Generation    uint64
	Index         uint64
	Lineage       [32]byte
}

// MigrationCaptureRequest is an already timestamped store-level point mutation
// batch. Callers must perform protocol and ownership admission before applying
// it. Transaction commands and prefix operations are deliberately not accepted.
type MigrationCaptureRequest struct {
	Mutations []*KVPairMutation
	ReadKeys  [][]byte
	StartTS   uint64
	CommitTS  uint64
	Fenced    bool
}

type MigrationCaptureLimits struct {
	MaxBytes   uint64
	MaxEffects uint32
}

type MigrationCaptureOutcome string

const (
	MigrationCaptureApplied  MigrationCaptureOutcome = "applied"
	MigrationCaptureConflict MigrationCaptureOutcome = "conflict"
	MigrationCaptureRejected MigrationCaptureOutcome = "rejected"
)

type MigrationCaptureInvalidation string

const (
	MigrationCaptureByteLimit   MigrationCaptureInvalidation = "byte_limit"
	MigrationCaptureEffectLimit MigrationCaptureInvalidation = "effect_limit"
)

type MigrationCaptureEffect struct {
	Request   uint64
	Ordinal   uint64
	Key       []byte
	Value     []byte
	CommitTS  uint64
	ExpireAt  uint64
	Tombstone bool
}

// MigrationCaptureEntry contains only the effects of this explicit point-entry
// apply. Complete does not attest to intervening entries, transfer families,
// session admission, or a base snapshot. Invalidation preserves the user write
// but makes the entry unusable as migration coverage.
type MigrationCaptureEntry struct {
	Version       uint32
	Identity      MigrationCaptureIdentity
	CommandDigest [32]byte
	Limits        MigrationCaptureLimits
	Complete      bool
	Invalidation  MigrationCaptureInvalidation
	LogicalBytes  uint64
	Outcomes      []MigrationCaptureOutcome
	Effects       []MigrationCaptureEffect
	Digest        [32]byte
}

type migrationCaptureBackend interface {
	MVCCStore
	RaftEntryApplier
	RaftControlMetadataStore
	LastAppliedIndex() (uint64, bool, error)
}

// ApplyMigrationCaptureEntry atomically applies point batches and their capture
// record using the existing synced, encrypted Raft control metadata path.
// It is an explicit shadow primitive, not a server feature flag: no continuous
// session or online-Split capability is activated. The caller must serialize
// Raft entries and must not mutate requests until this call returns.
func ApplyMigrationCaptureEntry(ctx context.Context, st MVCCStore, id MigrationCaptureIdentity, requests []MigrationCaptureRequest, limits MigrationCaptureLimits) (MigrationCaptureEntry, error) {
	limits, err := normalizeMigrationCaptureInput(ctx, id, requests, limits)
	if err != nil {
		return MigrationCaptureEntry{}, err
	}
	digest := migrationCaptureCommandDigest(requests, limits)
	return CaptureMigrationRaftEntry(ctx, st, id, digest, limits, func(tx context.Context, writer *MigrationCaptureWriter) error {
		return applyMigrationCaptureRequests(tx, st, id.Index, requests, writer)
	})
}

// CaptureMigrationRaftEntry lets the FSM retain its normal ownership and
// protocol checks inside the atomic point-entry capture. It does not activate
// a continuous capture session. All data writes must be made inside Request.
func CaptureMigrationRaftEntry(ctx context.Context, st MVCCStore, id MigrationCaptureIdentity, digest [32]byte, limits MigrationCaptureLimits, apply func(context.Context, *MigrationCaptureWriter) error) (MigrationCaptureEntry, error) {
	backend, ok := st.(migrationCaptureBackend)
	if !ok {
		return MigrationCaptureEntry{}, errors.WithStack(ErrNotSupported)
	}
	limits, err := normalizeMigrationCaptureInput(ctx, id, nil, limits)
	if err != nil {
		return MigrationCaptureEntry{}, err
	}
	if apply == nil || digest == [32]byte{} {
		return MigrationCaptureEntry{}, errors.WithStack(ErrMigrationCaptureInput)
	}
	entry := MigrationCaptureEntry{Version: migrationCaptureVersion, Identity: id, CommandDigest: digest, Limits: limits, Complete: true}
	err = backend.ApplyRaftEntry(ctx, id.Index, func(tx context.Context) error {
		return entry.applyInsideEntry(tx, backend, apply)
	})
	if err != nil {
		return MigrationCaptureEntry{}, errors.WithStack(err)
	}
	return entry, nil
}

func (entry *MigrationCaptureEntry) applyInsideEntry(ctx context.Context, backend migrationCaptureBackend, apply func(context.Context, *MigrationCaptureWriter) error) error {
	stored, err := ReadMigrationCaptureEntry(ctx, backend, entry.Identity)
	if err == nil {
		if stored.CommandDigest != entry.CommandDigest || stored.Limits != entry.Limits {
			return errors.WithStack(ErrMigrationCaptureConflict)
		}
		*entry = stored
		return nil
	}
	if !errors.Is(err, ErrKeyNotFound) {
		return err
	}
	if err := checkMigrationCaptureIndex(backend, entry.Identity.Index); err != nil {
		return err
	}
	return entry.captureFreshEntry(ctx, backend, apply)
}

func (entry *MigrationCaptureEntry) captureFreshEntry(ctx context.Context, backend migrationCaptureBackend, apply func(context.Context, *MigrationCaptureWriter) error) error {
	scope, ok := ctx.Value(raftEntryScopeKey{}).(*raftEntryScope)
	if !ok || scope == nil || scope.capture != nil {
		return errors.WithStack(ErrRaftControlScopeRequired)
	}
	scope.capture = &migrationCaptureCollector{entry: entry}
	writer := &MigrationCaptureWriter{collector: scope.capture, active: true}
	defer func() { writer.active = false }()
	if err := apply(ctx, writer); err != nil {
		return err
	}
	if writer.failed != nil {
		return writer.failed
	}
	if !validMigrationCaptureRecord(*entry) {
		return errors.WithStack(ErrMigrationCaptureCorrupt)
	}
	encoded, err := encodeMigrationCaptureEntry(entry)
	if err != nil {
		return err
	}
	return errors.WithStack(backend.StageRaftControlMetadata(ctx, migrationCaptureKey(entry.Identity), encoded))
}

func checkMigrationCaptureIndex(backend migrationCaptureBackend, incoming uint64) error {
	index, present, err := backend.LastAppliedIndex()
	if err != nil {
		return errors.WithStack(err)
	}
	if present && index >= incoming {
		return errors.WithStack(ErrMigrationCaptureStaleIndex)
	}
	return nil
}

func normalizeMigrationCaptureInput(ctx context.Context, id MigrationCaptureIdentity, requests []MigrationCaptureRequest, limits MigrationCaptureLimits) (MigrationCaptureLimits, error) {
	if err := ctx.Err(); err != nil {
		return limits, errors.WithStack(err)
	}
	if !validMigrationCaptureIdentity(id) || ctx.Value(raftEntryScopeKey{}) != nil || ctx.Value(memoryEntryKey{}) != nil {
		return limits, errors.WithStack(ErrMigrationCaptureInput)
	}
	limits, err := normalizeMigrationCaptureLimits(limits)
	if err != nil {
		return limits, err
	}
	return limits, validateMigrationCaptureRequests(requests)
}

func normalizeMigrationCaptureLimits(limits MigrationCaptureLimits) (MigrationCaptureLimits, error) {
	if limits.MaxBytes == 0 {
		limits.MaxBytes = migrationCaptureDefaultBytes
	}
	if limits.MaxEffects == 0 {
		limits.MaxEffects = migrationCaptureMaxEffects
	}
	if limits.MaxBytes < migrationCaptureMinBytes || limits.MaxBytes > migrationCaptureDefaultBytes || limits.MaxEffects > migrationCaptureMaxEffects {
		return limits, errors.WithStack(ErrMigrationCaptureInput)
	}
	return limits, nil
}

func validateMigrationCaptureRequests(requests []MigrationCaptureRequest) error {
	if len(requests) > MaxMigrationCaptureRequests {
		return errors.WithStack(ErrMigrationCaptureInput)
	}
	for _, request := range requests {
		if request.CommitTS == 0 {
			return errors.WithStack(ErrMigrationCaptureInput)
		}
		for _, mutation := range request.Mutations {
			if mutation == nil || len(mutation.Key) == 0 {
				return errors.WithStack(ErrMigrationCaptureInput)
			}
		}
	}
	return nil
}

func validMigrationCaptureIdentity(id MigrationCaptureIdentity) bool {
	return id.SourceGroupID != 0 && id.JobID != 0 && id.Generation != 0 && id.Index != 0 && id.Lineage != [32]byte{}
}

func migrationCaptureKey(id MigrationCaptureIdentity) []byte {
	key := []byte(migrationCaptureMetadataPrefix)
	key = hex.AppendEncode(key, id.Lineage[:])
	for _, n := range []uint64{id.SourceGroupID, id.JobID, id.Generation, id.Index} {
		key = binary.BigEndian.AppendUint64(key, n)
	}
	return key
}

func applyMigrationCaptureRequests(ctx context.Context, st MVCCStore, index uint64, requests []MigrationCaptureRequest, writer *MigrationCaptureWriter) error {
	for _, request := range requests {
		if err := writer.Request(func() (MigrationCaptureOutcome, error) {
			return applyMigrationCaptureRequest(ctx, st, index, request)
		}); err != nil {
			return err
		}
	}
	return nil
}

func applyMigrationCaptureRequest(ctx context.Context, st MVCCStore, index uint64, request MigrationCaptureRequest) (MigrationCaptureOutcome, error) {
	var err error
	if request.Fenced {
		err = st.ApplyMutationsRaftAtFenced(ctx, request.Mutations, request.ReadKeys, request.StartTS, request.CommitTS, index)
	} else {
		err = st.ApplyMutationsRaftAt(ctx, request.Mutations, request.ReadKeys, request.StartTS, request.CommitTS, index)
	}
	outcome, terminal := migrationCaptureVerdict(err)
	if !terminal {
		return "", errors.WithStack(err)
	}
	return outcome, nil
}

var migrationCaptureSecondary = errors.WithSecondaryError(ErrUnknownOp, ErrUnknownOp)

func migrationCaptureVerdict(err error) (MigrationCaptureOutcome, bool) {
	if err == nil {
		return MigrationCaptureApplied, true
	}
	if errors.HasType(err, migrationCaptureSecondary) {
		return "", false
	}
	for current := err; current != nil; current = errors.UnwrapOnce(current) {
		if _, branched := current.(interface{ Unwrap() []error }); branched {
			return "", false
		}
	}
	if !IsApplyVerdict(err) {
		return "", false
	}
	if errors.Is(err, ErrWriteConflict) {
		return MigrationCaptureConflict, true
	}
	return MigrationCaptureRejected, true
}

func (entry *MigrationCaptureEntry) invalidate(reason MigrationCaptureInvalidation) {
	entry.Complete = false
	entry.Invalidation = reason
	entry.Effects = nil
	entry.LogicalBytes = 0
}

func migrationCaptureCommandDigest(requests []MigrationCaptureRequest, limits MigrationCaptureLimits) [32]byte {
	h := sha256.New()
	captureHashNumber(h, limits.MaxBytes)
	captureHashNumber(h, uint64(limits.MaxEffects))
	captureHashNumber(h, uint64(len(requests)))
	for _, request := range requests {
		captureHashNumber(h, request.StartTS)
		captureHashNumber(h, request.CommitTS)
		captureHashBool(h, request.Fenced)
		captureHashNumber(h, uint64(len(request.ReadKeys)))
		for _, key := range request.ReadKeys {
			captureHashBytes(h, key)
		}
		captureHashNumber(h, uint64(len(request.Mutations)))
		for _, mutation := range request.Mutations {
			var opcode [binary.MaxVarintLen64]byte
			n := binary.PutVarint(opcode[:], int64(mutation.Op))
			_, _ = h.Write(opcode[:n])
			captureHashBytes(h, mutation.Key)
			captureHashBytes(h, mutation.Value)
			captureHashBool(h, mutation.RequireAbsent)
			captureHashNumber(h, mutation.ExpireAt)
		}
	}
	return [32]byte(h.Sum(nil))
}

func captureHashNumber(h hash.Hash, number uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], number)
	_, _ = h.Write(b[:])
}

func captureHashBytes(h hash.Hash, value []byte) {
	captureHashNumber(h, uint64(len(value)))
	_, _ = h.Write(value)
}

func captureHashBool(h hash.Hash, value bool) {
	var number uint64
	if value {
		number = 1
	}
	captureHashNumber(h, number)
}
