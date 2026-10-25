package kv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTxnLockedErrorKeepsDetailsOutOfMessage(t *testing.T) {
	t.Parallel()
	key := []byte("!fs|private-internal-key")
	err := NewTxnLockedErrorWithDetail(key, "private lock owner")
	key[0] = 'x'

	require.ErrorIs(t, err, ErrTxnLocked)
	require.Equal(t, ErrTxnLocked.Error(), err.Error())
	stored, detail, ok := TxnLockedDetails(err)
	require.True(t, ok)
	require.Equal(t, []byte("!fs|private-internal-key"), stored)
	require.Equal(t, "private lock owner", detail)
}
