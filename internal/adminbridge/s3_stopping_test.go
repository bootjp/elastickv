package adminbridge

import (
	"testing"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/admin"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// An admin object request that reaches an S3 server which already began
// stopping is answered like a follower's (503 with Retry-After), so the
// client retries against another node instead of seeing a 500.
func TestTranslateAdminObjectsErrorMapsStoppingToNotLeader(t *testing.T) {
	t.Parallel()
	err := translateAdminObjectsError(errors.Wrap(adapter.ErrAdminS3Stopping, "admin put"))
	require.ErrorIs(t, err, admin.ErrObjectsNotLeader)
}
