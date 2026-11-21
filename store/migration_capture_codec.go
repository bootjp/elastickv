package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"

	"github.com/cockroachdb/errors"
)

const migrationCaptureMaxEncodedBytes = 1 << 20

func encodeMigrationCaptureEntry(entry *MigrationCaptureEntry) ([]byte, error) {
	entry.Digest = [32]byte{}
	canonical, err := json.Marshal(entry)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	entry.Digest = sha256.Sum256(canonical)
	encoded, err := json.Marshal(entry)
	return encoded, errors.WithStack(err)
}

// ReadMigrationCaptureEntry validates one stored entry. It neither establishes
// a continuous cursor nor authorizes target activation or source log GC.
func ReadMigrationCaptureEntry(ctx context.Context, st MVCCStore, id MigrationCaptureIdentity) (MigrationCaptureEntry, error) {
	metadata, ok := st.(RaftControlMetadataStore)
	if !ok {
		return MigrationCaptureEntry{}, errors.WithStack(ErrNotSupported)
	}
	if !validMigrationCaptureIdentity(id) {
		return MigrationCaptureEntry{}, errors.WithStack(ErrMigrationCaptureInput)
	}
	encoded, err := metadata.ReadRaftControlMetadata(ctx, migrationCaptureKey(id))
	if err != nil {
		return MigrationCaptureEntry{}, errors.WithStack(err)
	}
	if len(encoded) > migrationCaptureMaxEncodedBytes {
		return MigrationCaptureEntry{}, errors.WithStack(ErrMigrationCaptureCorrupt)
	}
	return decodeMigrationCaptureEntry(encoded, id)
}

func decodeMigrationCaptureEntry(encoded []byte, id MigrationCaptureIdentity) (MigrationCaptureEntry, error) {
	var entry MigrationCaptureEntry
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&entry); err != nil {
		return MigrationCaptureEntry{}, errors.WithStack(ErrMigrationCaptureCorrupt)
	}
	digest := entry.Digest
	canonical, err := encodeMigrationCaptureEntry(&entry)
	if err != nil || !bytes.Equal(encoded, canonical) || entry.Digest != digest || entry.Identity != id || !validMigrationCaptureRecord(entry) {
		return MigrationCaptureEntry{}, errors.WithStack(ErrMigrationCaptureCorrupt)
	}
	return entry, nil
}

func validMigrationCaptureRecord(entry MigrationCaptureEntry) bool {
	return validMigrationCaptureHeader(entry) && validMigrationCaptureOutcomes(entry.Outcomes) && validMigrationCaptureEffects(entry)
}

func validMigrationCaptureHeader(entry MigrationCaptureEntry) bool {
	return entry.Version == migrationCaptureVersion && len(entry.Outcomes) <= MaxMigrationCaptureRequests &&
		len(entry.Effects) <= migrationCaptureMaxEffects && entry.Limits.MaxBytes >= migrationCaptureMinBytes &&
		entry.Limits.MaxBytes <= migrationCaptureDefaultBytes && entry.Limits.MaxEffects != 0 &&
		entry.Limits.MaxEffects <= migrationCaptureMaxEffects && validMigrationCaptureCompleteness(entry)
}

func validMigrationCaptureOutcomes(outcomes []MigrationCaptureOutcome) bool {
	for _, outcome := range outcomes {
		if outcome != MigrationCaptureApplied && outcome != MigrationCaptureConflict && outcome != MigrationCaptureRejected {
			return false
		}
	}
	return true
}

func validMigrationCaptureEffects(entry MigrationCaptureEntry) bool {
	var size uint64
	var previousRequest uint64
	for i, effect := range entry.Effects {
		if effect.Ordinal != uint64(i) || effect.Request < previousRequest || !validMigrationCaptureEffect(effect, entry.Outcomes) {
			return false
		}
		previousRequest = effect.Request
		size += uint64(len(effect.Key)) + uint64(len(effect.Value)) + migrationCaptureEffectBytes
	}
	return size == entry.LogicalBytes && size <= entry.Limits.MaxBytes && uint64(len(entry.Effects)) <= uint64(entry.Limits.MaxEffects)
}

func validMigrationCaptureEffect(effect MigrationCaptureEffect, outcomes []MigrationCaptureOutcome) bool {
	if effect.Request >= uint64(len(outcomes)) || outcomes[effect.Request] != MigrationCaptureApplied || effect.CommitTS == 0 || len(effect.Key) == 0 {
		return false
	}
	return !effect.Tombstone || (effect.ExpireAt == 0 && effect.Value == nil)
}

func validMigrationCaptureCompleteness(entry MigrationCaptureEntry) bool {
	if entry.Complete {
		return entry.Invalidation == ""
	}
	return (entry.Invalidation == MigrationCaptureByteLimit || entry.Invalidation == MigrationCaptureEffectLimit) && len(entry.Effects) == 0 && entry.LogicalBytes == 0
}
