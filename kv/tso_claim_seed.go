package kv

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"

	"github.com/cockroachdb/errors"
)

const (
	tsoClaimSeedMagic     = "EKVCKY01"
	tsoClaimSeedFileBytes = len(tsoClaimSeedMagic) + ed25519.SeedSize + sha256.Size
)

var (
	ErrTSOClaimSeedInvalid     = errors.New("tso: invalid claim seed file")
	ErrTSOClaimSeedUnsafe      = errors.New("tso: unsafe claim seed path or permissions")
	ErrTSOClaimSeedUnsupported = errors.New("tso: durable claim seed files require Linux or Darwin")
)

// ProvisionTSOClaimIdentity explicitly publishes the supplied seed once. Its
// existing parent directory must be private (0700); an existing file is never
// replaced. Success follows file sync, atomic publication and directory sync.
// This is local key custody, not node registration or issuance authority.
func ProvisionTSOClaimIdentity(path string, seed []byte) (*TSOClaimIdentity, error) {
	identity, err := NewTSOClaimIdentity(seed)
	if err != nil {
		return nil, err
	}
	var raw [tsoClaimSeedFileBytes]byte
	defer clear(raw[:])
	copy(raw[:], tsoClaimSeedMagic)
	copy(raw[len(tsoClaimSeedMagic):], identity.seed[:])
	hash := sha256.Sum256(raw[:len(tsoClaimSeedMagic)+ed25519.SeedSize])
	copy(raw[len(tsoClaimSeedMagic)+ed25519.SeedSize:], hash[:])
	if err := persistTSOClaimSeed(path, raw[:]); err != nil {
		return nil, err
	}
	return identity, nil
}

// LoadTSOClaimIdentity reads an existing regular, owner-only (0600) seed file
// without following symlinks. Missing or invalid files are errors, never a
// request to generate, replace or repair an identity.
func LoadTSOClaimIdentity(path string) (*TSOClaimIdentity, error) {
	raw, err := readTSOClaimSeed(path)
	defer clear(raw[:])
	if err != nil {
		return nil, err
	}
	if string(raw[:len(tsoClaimSeedMagic)]) != tsoClaimSeedMagic {
		return nil, errors.WithStack(ErrTSOClaimSeedInvalid)
	}
	end := len(tsoClaimSeedMagic) + ed25519.SeedSize
	hash := sha256.Sum256(raw[:end])
	if !bytes.Equal(raw[end:], hash[:]) {
		return nil, errors.WithStack(ErrTSOClaimSeedInvalid)
	}
	identity, err := NewTSOClaimIdentity(raw[len(tsoClaimSeedMagic):end])
	if err != nil {
		return nil, errors.WithStack(errors.Join(ErrTSOClaimSeedInvalid, err))
	}
	return identity, nil
}
