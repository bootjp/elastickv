package kv

import (
	"testing"

	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/stretchr/testify/require"
)

// applyReservedOpcode dispatches on the first byte, so two features sharing a
// value would route committed entries to the wrong apply path. Branches that
// assign opcodes independently collide silently at merge time; this pins them.
func TestKVFSMReservedOpcodesAreDistinct(t *testing.T) {
	t.Parallel()

	opcodes := map[string]byte{
		"single":            raftEncodeSingle,
		"batch":             raftEncodeBatch,
		"hlc lease":         raftEncodeHLCLease,
		"migration import":  raftEncodeMigrationImport,
		"migration promote": raftEncodeMigrationPromote,
		"migration retire":  raftEncodeMigrationRetire,
		"migration cleanup": raftEncodeMigrationCleanup,
		"backup":            raftEncodeBackup,
		"target readiness":  raftEncodeTargetReadiness,
		"replicated backup": raftEncodeReplicatedBackup,
	}
	owner := make(map[byte]string, len(opcodes))
	for name, op := range opcodes {
		require.Falsef(t, op >= fsmwire.OpEncryptionMin && op <= fsmwire.OpEncryptionMax,
			"%s opcode 0x%02x is inside the encryption range", name, op)
		if prev, dup := owner[op]; dup {
			t.Fatalf("%s and %s share opcode 0x%02x", prev, name, op)
		}
		owner[op] = name
	}
}
