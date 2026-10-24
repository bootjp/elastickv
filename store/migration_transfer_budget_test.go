package store

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigrationTransferBudgetIncludesFullKeyCursor(t *testing.T) {
	key := make([]byte, 1<<20)
	value := make([]byte, MigrationPagePayloadBudget-2*len(key)-128)
	require.True(t, MigrationVersionFitsTransfer(key, value))
	require.False(t, MigrationVersionFitsTransfer(key, make([]byte, len(value)+1)))
}
