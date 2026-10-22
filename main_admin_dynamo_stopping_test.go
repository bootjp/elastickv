package main

import (
	"testing"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/admin"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// An admin table or item request refused because the DynamoDB server
// already began stopping is answered like a follower's (503 with
// Retry-After), so the client retries against another node instead of
// seeing a 500.
func TestTranslateAdminDynamoErrorsMapStoppingToNotLeader(t *testing.T) {
	err := errors.Wrap(adapter.ErrDynamoServerStopping, "admin delete table")
	require.ErrorIs(t, translateAdminTablesError(err), admin.ErrTablesNotLeader)
	require.ErrorIs(t, translateAdminItemsError(err), admin.ErrItemsNotLeader)
}
