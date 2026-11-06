package encryption

import (
	"bytes"
	"crypto/subtle"
	"maps"
	"slices"
	"strconv"

	"github.com/cockroachdb/errors"
)

var ErrDEKMaterialReuse = errors.New("encryption: DEK material must not be reused across identities or purposes")

// ValidateRotationDEKMaterial compares actual AES keys, not randomized KEK
// encodings. Historical inactive keys remain nonce domains and are included.
// Replaying the same identity/purpose/material is the only permitted alias.
func ValidateRotationDEKMaterial(sc *Sidecar, unwrap KEKUnwrapper, id uint32, purpose string, material []byte) error {
	if sc == nil || unwrap == nil || id == 0 || len(material) != KeySize {
		return errors.WithStack(ErrEncryptionApply)
	}
	if purpose != SidecarPurposeStorage && purpose != SidecarPurposeRaft {
		return errors.WithStack(ErrEncryptionApply)
	}
	identity := strconv.FormatUint(uint64(id), 10)
	for _, existingID := range slices.Sorted(maps.Keys(sc.Keys)) {
		key := sc.Keys[existingID]
		if err := validateRotationMaterialEntry(unwrap, identity, purpose, material, existingID, key); err != nil {
			return err
		}
	}
	return nil
}

func validateRotationMaterialEntry(unwrap KEKUnwrapper, id, purpose string, material []byte, existingID string, key SidecarKey) error {
	existing, err := unwrap.Unwrap(bytes.Clone(key.Wrapped))
	defer clear(existing)
	if err != nil {
		return errors.Wrap(err, "encryption: verify historical DEK material")
	}
	if len(existing) != KeySize {
		return errors.WithStack(ErrBadKeySize)
	}
	sameMaterial := subtle.ConstantTimeCompare(material, existing) == 1
	if id == existingID {
		if purpose == key.Purpose && sameMaterial {
			return nil
		}
		return errors.WithStack(ErrDEKMaterialReuse)
	}
	if sameMaterial {
		return errors.WithStack(ErrDEKMaterialReuse)
	}
	return nil
}
