package admin

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
