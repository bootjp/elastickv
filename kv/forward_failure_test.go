package kv

import (
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestIsCertifiedOnePhaseConflict(t *testing.T) {
	t.Parallel()
	require.False(t, IsCertifiedOnePhaseConflict(store.ErrWriteConflict))
	require.False(t, IsCertifiedOnePhaseConflict(&AppliedRequestRejection{cause: store.ErrWriteConflict}))
	require.True(t, IsCertifiedOnePhaseConflict(errors.WithStack(&AppliedRequestRejection{
		commitIndex: 7,
		cause:       store.ErrWriteConflict,
	})))
	require.True(t, IsCertifiedOnePhaseConflict(errors.WithStack(&ForwardApplyRejection{cause: store.ErrWriteConflict})))
}
