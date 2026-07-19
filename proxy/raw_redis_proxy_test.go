package proxy

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"

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
