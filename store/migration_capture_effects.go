package store

import (
	"bytes"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
	"github.com/golang/snappy"
)

// The collector is installed only on an explicit shadow apply. The normal
// mutation path pays one nil check and performs no journal lookup or allocation.
type migrationCaptureCollector struct {
	entry         *MigrationCaptureEntry
	request       uint64
	recording     bool
	stagedRequest bool
	accept        func([]byte) bool
}

func captureStagedMigrationMutations(scope *raftEntryScope, mutations []encodedMutation) error {
	if scope.capture == nil {
		return nil
	}
	if !scope.capture.recording {
		return errors.WithStack(ErrMigrationCaptureInput)
	}
	return scope.capture.staged(scope, mutations)
}

func (c *migrationCaptureCollector) staged(scope *raftEntryScope, mutations []encodedMutation) error {
	if len(mutations) != 0 {
		c.stagedRequest = true
	}
	for _, mutation := range mutations {
		if c.accept != nil {
			key, _ := decodeKeyView(mutation.key)
			if !c.accept(key) {
				continue
			}
		}
		if !c.entry.Complete {
			break
		}
		if uint64(len(c.entry.Effects)) >= uint64(c.entry.Limits.MaxEffects) {
			c.entry.invalidate(MigrationCaptureEffectLimit)
			break
		}
		effect, err := c.decodeEffect(scope, mutation)
		if err != nil {
			return err
		}
		if !c.entry.Complete {
			break
		}
		effect.Request, effect.Ordinal = c.request, uint64(len(c.entry.Effects))
		c.entry.LogicalBytes += uint64(len(effect.Key)) + uint64(len(effect.Value)) + migrationCaptureEffectBytes
		c.entry.Effects = append(c.entry.Effects, effect)
	}
	return nil
}

// Called with the entry's dbMu read lock held. Decode only actual staged
// writes; a replay skipped by the store never reaches the collector.
func (c *migrationCaptureCollector) decodeEffect(scope *raftEntryScope, mutation encodedMutation) (MigrationCaptureEffect, error) {
	key, timestamp := decodeKeyView(mutation.key)
	if len(key) == 0 || timestamp == 0 {
		return MigrationCaptureEffect{}, errors.WithStack(ErrMigrationCaptureInput)
	}
	remaining := c.entry.Limits.MaxBytes - c.entry.LogicalBytes
	keyBytes := uint64(len(key)) + migrationCaptureEffectBytes
	if keyBytes > remaining {
		c.entry.invalidate(MigrationCaptureByteLimit)
		return MigrationCaptureEffect{}, nil
	}
	sv, err := decodeValueView(mutation.value)
	if err != nil {
		return MigrationCaptureEffect{}, err
	}
	budget := remaining - keyBytes
	if uint64(len(sv.Value)) > budget+encryption.EnvelopeOverhead {
		c.entry.invalidate(MigrationCaptureByteLimit)
		return MigrationCaptureEffect{}, nil
	}
	plain, flags, err := scope.store.decryptAuthenticatedForKey(mutation.key, sv, sv.Value)
	if err != nil {
		return MigrationCaptureEffect{}, err
	}
	effect := MigrationCaptureEffect{CommitTS: timestamp, ExpireAt: sv.ExpireAt, Tombstone: sv.Tombstone}
	if !sv.Tombstone {
		plain, err = c.expandWithinBudget(plain, flags, budget)
		if err != nil {
			return MigrationCaptureEffect{}, err
		}
		if !c.entry.Complete {
			return MigrationCaptureEffect{}, nil
		}
		effect.Value = bytes.Clone(plain)
	}
	effect.Key = bytes.Clone(key)
	return effect, nil
}

func (c *migrationCaptureCollector) expandWithinBudget(plain []byte, flags byte, budget uint64) ([]byte, error) {
	size := len(plain)
	if flags&encryption.FlagCompressed != 0 {
		decodedLen, err := snappy.DecodedLen(plain)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		size = decodedLen
	}
	if uint64(size) > budget {
		c.entry.invalidate(MigrationCaptureByteLimit)
		return nil, nil
	}
	return finishAuthenticatedValue(plain, flags)
}
