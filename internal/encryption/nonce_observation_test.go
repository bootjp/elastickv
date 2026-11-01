package encryption

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNonceIdentityObservationDoesNotConsumeNonce(t *testing.T) {
	f := NewDeterministicNonceFactory(123, 7)
	node, epoch, exhausted := f.NonceIdentity()
	require.Equal(t, uint16(123), node)
	require.Equal(t, uint16(7), epoch)
	require.False(t, exhausted)
	nonce, err := f.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1), binary.BigEndian.Uint64(nonce[4:]))
	f.writes.Store(^uint64(0))
	_, err = f.Next()
	require.ErrorIs(t, err, ErrWriteCountExhausted)
	_, _, exhausted = f.NonceIdentity()
	require.True(t, exhausted)
}
