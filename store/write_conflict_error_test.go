package store

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteConflictErrorKeepsKeyOutOfMessage(t *testing.T) {
	t.Parallel()
	key := []byte("!fs|private-internal-key")
	err := NewWriteConflictError(key)
	key[0] = 'x'

	require.ErrorIs(t, err, ErrWriteConflict)
	require.Equal(t, ErrWriteConflict.Error(), err.Error())
	stored, ok := WriteConflictKey(err)
	require.True(t, ok)
	require.Equal(t, []byte("!fs|private-internal-key"), stored)
}
