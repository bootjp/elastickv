//go:build !linux && !darwin

package kv

import "github.com/cockroachdb/errors"

func persistTSOClaimSeed(string, []byte) error {
	return errors.WithStack(ErrTSOClaimSeedUnsupported)
}

func readTSOClaimSeed(string) ([tsoClaimSeedFileBytes]byte, error) {
	return [tsoClaimSeedFileBytes]byte{}, errors.WithStack(ErrTSOClaimSeedUnsupported)
}
