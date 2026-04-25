package admin

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseTrustedProxies(t *testing.T) {
	nets, err := ParseTrustedProxies([]string{"10.0.0.0/8", "192.168.1.5", "::1", " "})
	require.NoError(t, err)
	require.Len(t, nets, 3)
	// /8 covers 10.x.x.x.
	require.True(t, nets[0].Contains(net4("10.0.0.1")))
	require.False(t, nets[0].Contains(net4("11.0.0.1")))
	// Bare IPv4 becomes /32.
	require.True(t, nets[1].Contains(net4("192.168.1.5")))
	require.False(t, nets[1].Contains(net4("192.168.1.6")))
	// IPv6 host route — sanity.
	require.NotNil(t, nets[2])

	_, err = ParseTrustedProxies([]string{"not-an-ip"})
	require.Error(t, err)
}

func TestClientIPWithTrust_HonoursXFFFromTrustedPeer(t *testing.T) {
	trusted, err := ParseTrustedProxies([]string{"10.0.0.0/8"})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.RemoteAddr = "10.0.0.5:9000"
	req.Header.Set("X-Forwarded-For", "203.0.113.7, 10.0.0.4")

	require.Equal(t, "203.0.113.7", clientIPWithTrust(req, trusted))
}

func TestClientIPWithTrust_IgnoresXFFFromUntrustedPeer(t *testing.T) {
	trusted, err := ParseTrustedProxies([]string{"10.0.0.0/8"})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.RemoteAddr = "203.0.113.99:9000"
	req.Header.Set("X-Forwarded-For", "1.2.3.4")

	// Untrusted peer must NOT be allowed to substitute via XFF.
	require.Equal(t, "203.0.113.99", clientIPWithTrust(req, trusted))
}

func TestClientIPWithTrust_NoTrustedFallsThroughToPeer(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.RemoteAddr = "10.0.0.5:9000"
	req.Header.Set("X-Forwarded-For", "1.2.3.4")
	require.Equal(t, "10.0.0.5", clientIPWithTrust(req, nil))
}

// net4 is a tiny helper that parses an IPv4 string into net.IP for the
// table tests above without dragging the net import into a separate file.
func net4(s string) net.IP { return net.ParseIP(s) }

func TestRateLimiter_HardCapRefusesNewIPs(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	clk := func() time.Time { return now }
	rl := newRateLimiter(5, time.Minute, clk)

	// Fill the map to the cap by bringing in distinct IPs.
	for i := 0; i < rateLimiterMaxEntries; i++ {
		ip := "10.0." + strconv.Itoa(i/256) + "." + strconv.Itoa(i%256)
		require.Truef(t, rl.allow(ip), "IP %s should be allowed when map is below cap", ip)
	}
	require.Equal(t, rateLimiterMaxEntries, len(rl.entries))

	// A brand-new IP must be refused because the cap is reached and
	// no entries have expired yet.
	require.False(t, rl.allow("192.168.1.1"))

	// Existing IPs are still accounted for (not evicted).
	require.True(t, rl.allow("10.0.0.0"))
}

func TestRateLimiter_HardCapReclaimsAfterWindow(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	clk := func() time.Time { return now }
	rl := newRateLimiter(5, time.Minute, clk)

	for i := 0; i < rateLimiterMaxEntries; i++ {
		ip := "10.0." + strconv.Itoa(i/256) + "." + strconv.Itoa(i%256)
		require.True(t, rl.allow(ip))
	}

	// Advance past the window — the next allow() call must be able to
	// sweep the expired entries and then admit the new IP.
	now = now.Add(2 * time.Minute)
	require.True(t, rl.allow("192.168.2.2"))
}
