package main

import (
	"testing"

	"github.com/bootjp/elastickv/keyviz"
	"github.com/stretchr/testify/require"
)

func TestEffectiveKeyVizBucketsPerRouteAutoSplitImpliedDefault(t *testing.T) {
	t.Parallel()

	require.Equal(t, 16, effectiveKeyVizBucketsPerRoute(true, false, keyviz.DefaultKeyBucketsPerRoute, 16))
	require.Equal(t, 1, effectiveKeyVizBucketsPerRoute(true, true, keyviz.DefaultKeyBucketsPerRoute, 16))
	require.Equal(t, 8, effectiveKeyVizBucketsPerRoute(true, false, 8, 16))
	require.Equal(t, 1, effectiveKeyVizBucketsPerRoute(false, false, keyviz.DefaultKeyBucketsPerRoute, 16))
}
