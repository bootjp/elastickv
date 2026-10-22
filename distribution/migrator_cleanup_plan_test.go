package distribution_test

import (
	"encoding/base64"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

func TestPlanCleanupBracketsIncludesRoutedDynamoSealsWithoutExport(t *testing.T) {
	t.Parallel()
	table := base64.RawURLEncoding.EncodeToString([]byte("foo"))
	neighbor := base64.RawURLEncoding.EncodeToString([]byte("fooX"))
	seal := []byte(kv.DynamoSealPrefix + table + "|7|0001")
	other := []byte(kv.DynamoSealPrefix + neighbor + "|7|0001")
	start := kv.RouteKey(seal)
	end := append(append([]byte(nil), start...), 0)
	export, err := distribution.PlanExportBrackets(start, end)
	require.NoError(t, err)
	cleanup, err := distribution.PlanCleanupBrackets(start, end)
	require.NoError(t, err)
	require.Equal(t, export, cleanup[:len(export)], "existing positional cleanup cursors must retain their meaning")
	require.Len(t, cleanup, len(export)+1)
	bracket := cleanup[len(export)]
	require.Equal(t, distribution.MigrationFamilyDynamoSnapshotSeal, bracket.Family)
	require.True(t, bracket.ContainsRoutedKey(seal, start, end, kv.RouteKey))
	require.False(t, bracket.ContainsRoutedKey(other, start, end, kv.RouteKey))
	wideStart, wideEnd := []byte("!ddb|"), []byte("!ddc|")
	wide, err := distribution.PlanExportBrackets(wideStart, wideEnd)
	require.NoError(t, err)
	for _, bracket := range wide {
		require.False(t, bracket.ContainsRoutedKey(seal, wideStart, wideEnd, kv.RouteKey), "OCC seals must not be imported as snapshot data")
	}
}
