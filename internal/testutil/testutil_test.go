package testutil_test

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/stretchr/testify/require"
)

type failingTB struct {
	testing.TB
}

func (failingTB) Helper()                           {}
func (failingTB) Fatalf(format string, args ...any) { panic(fmt.Sprintf(format, args...)) }

func TestWaits(t *testing.T) {
	ch := make(chan int, 1)
	ch <- 42
	require.Equal(t, 42, testutil.Recv(t, ch, time.Second, "message %s", "ready"))
	testutil.NoRecv(t, ch, time.Millisecond, "empty channel")
	close(ch)
	testutil.Closed(t, ch, time.Second, "producer finished")
}

func TestWaitFailures(t *testing.T) {
	open := make(chan int)
	closed := make(chan int)
	close(closed)
	buffered := func() <-chan int {
		ch := make(chan int, 1)
		ch <- 1
		return ch
	}
	for _, tc := range []struct {
		name string
		call func()
	}{
		{"receive timeout", func() { testutil.Recv(failingTB{}, open, time.Millisecond, "detail %d", 7) }},
		{"receive closed", func() { testutil.Recv(failingTB{}, closed, time.Second, "detail %d", 7) }},
		{"unexpected value", func() { testutil.NoRecv(failingTB{}, buffered(), time.Second, "detail %d", 7) }},
		{"unexpected close", func() { testutil.NoRecv(failingTB{}, closed, time.Second, "detail %d", 7) }},
		{"close timeout", func() { testutil.Closed(failingTB{}, open, time.Millisecond, "detail %d", 7) }},
		{"close received value", func() { testutil.Closed(failingTB{}, buffered(), time.Second, "detail %d", 7) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.PanicsWithValue(t, failureMessage(tc.name), tc.call)
		})
	}
}

func failureMessage(name string) string {
	switch name {
	case "receive timeout":
		return "receive timed out: detail 7"
	case "receive closed":
		return "channel closed: detail 7"
	case "close timeout":
		return "close timed out: detail 7"
	case "close received value":
		return "expected closed channel, received value: detail 7"
	default:
		return "unexpected receive or close: detail 7"
	}
}

func TestListenCleanup(t *testing.T) {
	var lis net.Listener
	t.Run("owner", func(t *testing.T) {
		lis = testutil.Listen(t)
		require.True(t, strings.HasPrefix(lis.Addr().String(), "127.0.0.1:"))
		dialer := net.Dialer{Timeout: time.Second}
		conn, err := dialer.DialContext(context.Background(), "tcp", lis.Addr().String())
		require.NoError(t, err)
		require.NoError(t, conn.Close())
	})
	_, err := lis.Accept()
	require.ErrorIs(t, err, net.ErrClosed)
}
