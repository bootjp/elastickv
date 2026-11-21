package server

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRampCatalogConflictRequiresDefinitiveStatus(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		retry bool
	}{
		{"wrapped OCC", fmt.Errorf("autosplit: %w", status.Error(codes.Aborted, "catalog version conflict")), true},
		{"unknown outcome", status.Error(codes.Unknown, "catalog version conflict"), false},
		{"different abort", status.Error(codes.Aborted, "not catalog version conflict"), false},
		{"extended message", status.Error(codes.Aborted, "catalog version conflict: unknown outcome"), false},
		{"ordinary error", fmt.Errorf("catalog version conflict"), false},
		{"no error", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.retry, rampCatalogConflict(tc.err))
		})
	}
}
