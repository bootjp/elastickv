package proxy

import (
	"bufio"
	"context"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRawRedisCommand(t *testing.T) {
	t.Parallel()

	require.Equal(t,
		[]byte("*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n"),
		rawRedisCommand("AUTH", "secret"),
	)
}

func TestPrepareRawRedisUpstreamAuthenticatesAndSelectsDB(t *testing.T) {
	t.Parallel()

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	seen := make(chan []string, 2)
	go func() {
		reader := bufio.NewReader(server)
		for i := 0; i < 2; i++ {
			cmd, err := readRawRedisTestCommand(reader)
			if err != nil {
				return
			}
			seen <- cmd
			_, _ = server.Write([]byte("+OK\r\n"))
		}
	}()

	p := &ProxyServer{cfg: ProxyConfig{PrimaryPassword: "secret", PrimaryDB: 1}}
	err := p.prepareRawRedisUpstream(client, bufio.NewReader(client))
	require.NoError(t, err)
	require.Equal(t, []string{"AUTH", "secret"}, <-seen)
	require.Equal(t, []string{"SELECT", "1"}, <-seen)
}

func readRawRedisTestCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	n, err := strconv.Atoi(strings.TrimPrefix(line, "*"))
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, n)
	for range n {
		line, err = r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(strings.TrimPrefix(strings.TrimRight(line, "\r\n"), "$"))
		if err != nil {
			return nil, err
		}
		arg := make([]byte, size+2)
		if _, err := io.ReadFull(r, arg); err != nil {
			return nil, err
		}
		out = append(out, string(arg[:size]))
	}
	return out, nil
}

// A client that writes its command and then half-closes its write side is
// valid TCP and is what one-shot clients do. Closing the whole upstream
// connection when that direction reaches EOF tears down the reverse copier
// too, so the reply never arrives.
func TestHandleRawRedisConnDeliversReplyAfterClientHalfClose(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	var lc net.ListenConfig
	upstreamLn, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer upstreamLn.Close()

	go func() {
		conn, acceptErr := upstreamLn.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		// Drain the request, then answer after the client has half-closed.
		buf := make([]byte, 64)
		if _, readErr := conn.Read(buf); readErr != nil {
			return
		}
		_, _ = conn.Write([]byte("+PONG\r\n"))
		// Hold the connection open until the proxy tears it down, so the
		// reply is the only thing the client is waiting for.
		_, _ = io.Copy(io.Discard, conn)
	}()

	clientLn, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer clientLn.Close()

	p := &ProxyServer{
		cfg:     ProxyConfig{PrimaryAddr: upstreamLn.Addr().String()},
		metrics: newTestMetrics(),
		logger:  testLogger,
	}

	served := make(chan struct{})
	go func() {
		conn, acceptErr := clientLn.Accept()
		if acceptErr != nil {
			close(served)
			return
		}
		p.handleRawRedisConn(ctx, conn)
		close(served)
	}()

	var dialer net.Dialer
	client, err := dialer.DialContext(ctx, "tcp", clientLn.Addr().String())
	require.NoError(t, err)
	defer client.Close()

	_, err = client.Write(rawRedisCommand("PING"))
	require.NoError(t, err)
	tcpClient, ok := client.(*net.TCPConn)
	require.True(t, ok)
	require.NoError(t, tcpClient.CloseWrite())

	require.NoError(t, client.SetReadDeadline(time.Now().Add(5*time.Second)))
	reply, err := bufio.NewReader(client).ReadString('\n')
	require.NoError(t, err, "the reply must survive the client's half-close")
	require.Equal(t, "+PONG\r\n", reply)

	_ = client.Close()
	<-served
}
