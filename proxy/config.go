package proxy

import "time"

const (
	defaultSecondaryTimeout = 5 * time.Second
	defaultShadowTimeout    = 3 * time.Second
)

// ProxyMode controls which backends receive reads and writes.
type ProxyMode int

const (
	ModeRedisOnly        ProxyMode = iota // Redis only (passthrough)
	ModeDualWrite                         // Write to both, read from Redis
	ModeDualWriteShadow                   // Write to both, read from Redis + shadow read from ElasticKV
	ModeElasticKVPrimary                  // Write to both, read from ElasticKV + shadow read from Redis
	ModeElasticKVOnly                     // ElasticKV only
)

var modeNames = map[string]ProxyMode{
	"redis-only":        ModeRedisOnly,
	"dual-write":        ModeDualWrite,
	"dual-write-shadow": ModeDualWriteShadow,
	"elastickv-primary": ModeElasticKVPrimary,
	"elastickv-only":    ModeElasticKVOnly,
}

var modeStrings = map[ProxyMode]string{
	ModeRedisOnly:        "redis-only",
	ModeDualWrite:        "dual-write",
	ModeDualWriteShadow:  "dual-write-shadow",
	ModeElasticKVPrimary: "elastickv-primary",
	ModeElasticKVOnly:    "elastickv-only",
}

// ParseProxyMode converts a string to ProxyMode. Returns false if unknown.
func ParseProxyMode(s string) (ProxyMode, bool) {
	m, ok := modeNames[s]
	return m, ok
}

func (m ProxyMode) String() string {
	if s, ok := modeStrings[m]; ok {
		return s
	}
	return unknownStr
}

// ProxyConfig holds all configuration for the dual-write proxy.
type ProxyConfig struct {
	ListenAddr       string
	PrimaryAddr      string
	SecondaryAddr    string
	Mode             ProxyMode
	SecondaryTimeout time.Duration
	ShadowTimeout    time.Duration
	SentryDSN        string
	SentryEnv        string
	SentrySampleRate float64
	MetricsAddr      string
}

// DefaultConfig returns a ProxyConfig with sensible defaults.
func DefaultConfig() ProxyConfig {
	return ProxyConfig{
		ListenAddr:       ":6479",
		PrimaryAddr:      "localhost:6379",
		SecondaryAddr:    "localhost:6380",
		Mode:             ModeDualWrite,
		SecondaryTimeout: defaultSecondaryTimeout,
		ShadowTimeout:    defaultShadowTimeout,
		SentrySampleRate: 1.0,
		MetricsAddr:      ":9191",
	}
}
