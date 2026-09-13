package admin

import (
	"encoding/json"
	"testing"

	"github.com/bootjp/elastickv/keyviz"
	"github.com/stretchr/testify/require"
)

// TestSubBucketFieldsStayOffTheWireUnlessSubDivided pins that the new
// fields are inert at the K=1 default. The keyviz response is consumed
// by an SPA that ships separately from the server, so a field that
// appeared on every row would change the payload for every existing
// deployment that never enabled sub-bucketing.
func TestSubBucketFieldsStayOffTheWireUnlessSubDivided(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		row  keyviz.MatrixRow
	}{
		{name: "K=1 route", row: keyviz.MatrixRow{RouteID: 7, SubBucketCount: 1}},
		{name: "unset count", row: keyviz.MatrixRow{RouteID: 7}},
		{name: "aggregate row", row: keyviz.MatrixRow{RouteID: 7, Aggregate: true, SubBucketCount: 4, SubBucket: 2}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			encoded, err := json.Marshal(KeyVizRow{
				BucketID:       bucketIDFor(tc.row),
				SubBucket:      subBucketIndexFor(tc.row),
				SubBucketCount: subBucketCountFor(tc.row),
			})
			require.NoError(t, err)
			require.NotContains(t, string(encoded), "sub_bucket",
				"the sub-range fields must not appear for a non-sub-divided row")
		})
	}
}

// TestSubBucketFieldsAgreeWithTheBucketID pins that the fields and the
// "#i" suffix cannot disagree about whether a row is a sub-range —
// they share one predicate, so an SPA can trust either.
func TestSubBucketFieldsAgreeWithTheBucketID(t *testing.T) {
	t.Parallel()

	subdivided := keyviz.MatrixRow{RouteID: 42, SubBucket: 3, SubBucketCount: 8}
	require.Equal(t, "route:42#3", bucketIDFor(subdivided))
	require.Equal(t, 3, subBucketIndexFor(subdivided))
	require.Equal(t, 8, subBucketCountFor(subdivided))

	encoded, err := json.Marshal(KeyVizRow{
		BucketID:       bucketIDFor(subdivided),
		SubBucket:      subBucketIndexFor(subdivided),
		SubBucketCount: subBucketCountFor(subdivided),
	})
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"sub_bucket_count":8`)

	// Bucket zero of a subdivided route: the index is legitimately 0,
	// so only the count keeps it on the wire — which is why the count
	// is what the SPA must test, not the index.
	first := keyviz.MatrixRow{RouteID: 42, SubBucket: 0, SubBucketCount: 8}
	require.Equal(t, "route:42#0", bucketIDFor(first))
	require.Equal(t, 8, subBucketCountFor(first))
}
