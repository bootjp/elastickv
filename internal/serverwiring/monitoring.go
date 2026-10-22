package serverwiring

import (
	"context"
	"net"
	"net/http"
	"strings"

	"github.com/bootjp/elastickv/monitoring"
	"github.com/cockroachdb/errors"
)

// PrepareMetricsServer validates the metrics address and token and binds the
// listener. An empty address or a nil handler disables metrics and returns
// zero values. It returns the server, its listener and the trimmed address.
func PrepareMetricsServer(ctx context.Context, lc *net.ListenConfig, addr, token string, handler http.Handler) (*http.Server, net.Listener, string, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" || handler == nil {
		return nil, nil, "", nil
	}
	l, err := listenMonitoring(ctx, lc, "metrics", addr, token)
	if err != nil {
		return nil, nil, "", err
	}
	return monitoring.NewMetricsServer(handler, token), l, addr, nil
}

// PreparePprofServer is PrepareMetricsServer for the pprof endpoints. An
// empty address disables pprof.
func PreparePprofServer(ctx context.Context, lc *net.ListenConfig, addr, token string) (*http.Server, net.Listener, string, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil, nil, "", nil
	}
	l, err := listenMonitoring(ctx, lc, "pprof", addr, token)
	if err != nil {
		return nil, nil, "", err
	}
	return monitoring.NewPprofServer(token), l, addr, nil
}

// listenMonitoring rejects an address that is not host:port, then a
// non-loopback address without a token, then binds. The address check runs
// first so a typo is reported as a typo rather than as a missing token.
func listenMonitoring(ctx context.Context, lc *net.ListenConfig, name, addr, token string) (net.Listener, error) {
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return nil, errors.Wrapf(err, "invalid %sAddress %q; expected host:port", name, addr)
	}
	if monitoring.AddressRequiresToken(addr) && strings.TrimSpace(token) == "" {
		return nil, errors.Errorf("%sToken is required when %sAddress is not loopback", name, name)
	}
	l, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to listen on %s", addr)
	}
	return l, nil
}
