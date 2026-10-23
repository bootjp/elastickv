package distribution

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOwnershipPartitionLedgerNamespace(t *testing.T) {
	key, err := CatalogOwnershipPartitionKey(3, make([]byte, 32))
	require.NoError(t, err)
	require.True(t, IsCatalogOwnershipPartitionKey(key))
	require.False(t, IsMigrationOnlyControlKey(key), "catalog transactions must retain their typed ledger writes")
	for _, prefix := range [][]byte{nil, []byte("!dist|"), []byte("!dist|meta|ownership_partition"), key} {
		require.True(t, CatalogOwnershipPartitionPrefixIntersects(prefix))
	}
	for _, prefix := range [][]byte{[]byte("customer"), []byte("!dist|meta|ownership_partition_user")} {
		require.False(t, CatalogOwnershipPartitionPrefixIntersects(prefix))
	}
}
