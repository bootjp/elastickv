package fsmount

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

func TestRetryFilesystemStartupWaitsForLeadershipAndHLCLease(t *testing.T) {
	t.Parallel()
	attempts := 0
	err := retryFilesystemStartup(t.Context(), time.Nanosecond, func(context.Context) error {
		attempts++
		switch attempts {
		case 1:
			return kv.ErrLeaderNotFound
		case 2:
			return errors.Join(kv.ErrCeilingExpired, errors.New("lease recovery unavailable"))
		default:
			return nil
		}
	})
	require.NoError(t, err)
	require.Equal(t, 3, attempts)
}

func TestRetryFilesystemStartupRejectsNonTransientError(t *testing.T) {
	t.Parallel()
	permanent := errors.New("corrupt filesystem root")
	attempts := 0
	err := retryFilesystemStartup(t.Context(), time.Nanosecond, func(context.Context) error {
		attempts++
		return permanent
	})
	require.ErrorIs(t, err, permanent)
	require.Equal(t, 1, attempts)
}

func TestRetryFilesystemStartupStopsAtDeadline(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	err := retryFilesystemStartup(ctx, time.Millisecond, func(context.Context) error {
		return kv.ErrLeaderNotFound
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, err, kv.ErrLeaderNotFound)
}

type startupSessionRegistrar struct {
	id       filesystem.OperationSessionID
	initial  error
	retries  []filesystem.OperationSessionID
	retryErr error
}

func (r *startupSessionRegistrar) RegisterSession(context.Context, [16]byte, string) (filesystem.OperationSessionID, error) {
	return r.id, r.initial
}

func (r *startupSessionRegistrar) RegisterSessionWithID(_ context.Context, id filesystem.OperationSessionID, _ string) error {
	r.retries = append(r.retries, id)
	if len(r.retries) == 1 {
		return r.retryErr
	}
	return nil
}

func TestRegisterStrictSessionRetainsIdentityAcrossLeaseRecovery(t *testing.T) {
	t.Parallel()
	var filesystemID [16]byte
	filesystemID[0] = 1
	id := filesystem.OperationSessionID{Filesystem: filesystemID}
	id.Session[0] = 2
	registrar := &startupSessionRegistrar{
		id: id, initial: kv.ErrCeilingExpired, retryErr: kv.ErrLeaderNotFound,
	}
	session, err := registerStrictSession(t.Context(), registrar, filesystemID, "n1")
	require.NoError(t, err)
	require.Equal(t, id, session)
	require.Equal(t, []filesystem.OperationSessionID{id, id}, registrar.retries)
}

func TestRegisterStrictSessionDoesNotRetryPermanentFailure(t *testing.T) {
	t.Parallel()
	var filesystemID [16]byte
	filesystemID[0] = 1
	id := filesystem.OperationSessionID{Filesystem: filesystemID}
	id.Session[0] = 2
	permanent := errors.New("invalid operation session")
	registrar := &startupSessionRegistrar{id: id, initial: permanent}
	_, err := registerStrictSession(t.Context(), registrar, filesystemID, "n1")
	require.ErrorIs(t, err, permanent)
	require.Empty(t, registrar.retries)
}
