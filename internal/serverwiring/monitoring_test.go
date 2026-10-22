package serverwiring

import (
	"context"
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrepareMonitoringServers(t *testing.T) {
	t.Parallel()

	prepare := map[string]func(addr, token string) (*http.Server, net.Listener, string, error){
		"metrics": func(addr, token string) (*http.Server, net.Listener, string, error) {
			return PrepareMetricsServer(context.Background(), &net.ListenConfig{}, addr, token, http.NewServeMux())
		},
		"pprof": func(addr, token string) (*http.Server, net.Listener, string, error) {
			return PreparePprofServer(context.Background(), &net.ListenConfig{}, addr, token)
		},
	}
	for name, run := range prepare {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			server, l, addr, err := run("  ", "")
			require.NoError(t, err, "a blank address disables the server")
			require.Nil(t, server)
			require.Nil(t, l)
			require.Empty(t, addr)

			// The address is checked before the token, so a typo is not
			// reported as a missing token.
			_, _, _, err = run("localhost", "")
			require.ErrorContains(t, err, `invalid `+name+`Address "localhost"`)

			_, _, _, err = run("0.0.0.0:0", "")
			require.ErrorContains(t, err, name+"Token is required")

			server, l, addr, err = run(" 127.0.0.1:0 ", "")
			require.NoError(t, err)
			require.NotNil(t, server)
			require.Equal(t, "127.0.0.1:0", addr)
			require.NoError(t, l.Close())
		})
	}
}

func TestPrepareMetricsServerWithoutHandlerIsDisabled(t *testing.T) {
	t.Parallel()

	server, l, addr, err := PrepareMetricsServer(context.Background(), &net.ListenConfig{}, "127.0.0.1:0", "", nil)
	require.NoError(t, err)
	require.Nil(t, server)
	require.Nil(t, l)
	require.Empty(t, addr)
}
