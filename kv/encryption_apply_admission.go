package kv

import (
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/cockroachdb/errors"
)

// EncryptionApplyChangesAdmission completely decodes implemented cleartext
// encryption mutations. Even possibly redundant registrations conservatively
// revoke: equality requires applier-owned state. This grants no apply authority.
// TSO 0x07 and unimplemented reserved tags are deliberately not accepted here.
func EncryptionApplyChangesAdmission(payload []byte) (bool, error) {
	if len(payload) == 0 {
		return false, nil
	}
	var err error
	switch payload[0] {
	case fsmwire.OpRegistration:
		_, err = fsmwire.DecodeRegistration(payload[1:])
	case fsmwire.OpBootstrap:
		_, err = fsmwire.DecodeBootstrap(payload[1:])
	case fsmwire.OpRotation:
		_, err = fsmwire.DecodeRotation(payload[1:])
	default:
		return false, nil
	}
	if err != nil {
		return false, errors.Wrap(err, "decode encryption admission transition")
	}
	return true, nil
}
