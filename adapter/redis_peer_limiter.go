package adapter

import (
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

const (
	redisPerPeerLimitEnv              = "ELASTICKV_REDIS_PER_PEER_CONNECTIONS"
	defaultRedisProxyPoolPeerCap      = 192
	defaultRedisProxyReplicasPerPeer  = 2
	defaultRedisDedicatedPeerHeadroom = 128
	defaultRedisPerPeerConnectionCap  = defaultRedisProxyPoolPeerCap*defaultRedisProxyReplicasPerPeer + defaultRedisDedicatedPeerHeadroom
	redisPeerLimitError               = "ERR max connections per client exceeded"
	unknownRedisPeer                  = "unknown"
)

type redisPeerLimiter struct {
	limit  int
	mu     sync.Mutex
	active map[string]int
}

func newDefaultRedisPeerLimiter() *redisPeerLimiter {
	n := defaultRedisPerPeerConnectionCap
	if raw := strings.TrimSpace(os.Getenv(redisPerPeerLimitEnv)); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			n = parsed
		}
	}
	return newRedisPeerLimiter(n)
}

func newRedisPeerLimiter(limit int) *redisPeerLimiter {
	if limit <= 0 {
		return nil
	}
	return &redisPeerLimiter{
		limit:  limit,
		active: make(map[string]int),
	}
}

func (l *redisPeerLimiter) accept(remoteAddr string) (string, bool) {
	peer := redisPeerKey(remoteAddr)
	if l == nil {
		return peer, true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.active[peer] >= l.limit {
		return peer, false
	}
	l.active[peer]++
	return peer, true
}

func (l *redisPeerLimiter) release(peer string) {
	if l == nil || peer == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	n := l.active[peer]
	if n <= 1 {
		delete(l.active, peer)
		return
	}
	l.active[peer] = n - 1
}

func redisPeerKey(remoteAddr string) string {
	remoteAddr = strings.TrimSpace(remoteAddr)
	if remoteAddr == "" {
		return unknownRedisPeer
	}
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil || host == "" {
		return remoteAddr
	}
	return host
}
