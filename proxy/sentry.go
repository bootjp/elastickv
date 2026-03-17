package proxy

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"
)

const (
	defaultReportCooldown = 60 * time.Second
	// maxSentryValueLen limits the length of values attached to Sentry events
	// to prevent data leakage and oversized events.
	maxSentryValueLen = 256
	// maxReportEntries caps the lastReport map to prevent unbounded growth.
	maxReportEntries = 10000
)

// SentryReporter sends anomaly events to Sentry with de-duplication.
type SentryReporter struct {
	enabled  bool
	hub      *sentry.Hub
	logger   *slog.Logger
	cooldown time.Duration

	mu         sync.Mutex
	lastReport map[string]time.Time // fingerprint → last report time
}

// NewSentryReporter initialises Sentry. If dsn is empty, reporting is disabled.
func NewSentryReporter(dsn string, environment string, sampleRate float64, logger *slog.Logger) *SentryReporter {
	if logger == nil {
		logger = slog.Default()
	}
	r := &SentryReporter{
		logger:     logger,
		cooldown:   defaultReportCooldown,
		lastReport: make(map[string]time.Time),
	}
	if dsn == "" {
		return r
	}

	err := sentry.Init(sentry.ClientOptions{
		Dsn:              dsn,
		Environment:      environment,
		SampleRate:       sampleRate,
		EnableTracing:    false,
		AttachStacktrace: true,
	})
	if err != nil {
		logger.Error("failed to init sentry", "err", err)
		return r
	}
	r.enabled = true
	r.hub = sentry.CurrentHub()
	return r
}

// CaptureException reports an error to Sentry.
func (r *SentryReporter) CaptureException(err error, operation string, args [][]byte) {
	if !r.enabled {
		return
	}
	r.hub.WithScope(func(scope *sentry.Scope) {
		scope.SetTag("operation", operation)
		if len(args) > 0 {
			scope.SetTag("command", string(args[0]))
		}
		scope.SetFingerprint([]string{operation, cmdNameFromArgs(args)})
		r.hub.CaptureException(err)
	})
}

// CaptureDivergence reports a data divergence to Sentry with cooldown-based de-duplication.
func (r *SentryReporter) CaptureDivergence(div Divergence) {
	if !r.enabled {
		return
	}
	fingerprint := fmt.Sprintf("divergence_%s_%s", div.Kind.String(), div.Command)
	if !r.ShouldReport(fingerprint) {
		return
	}
	r.hub.WithScope(func(scope *sentry.Scope) {
		scope.SetTag("command", div.Command)
		scope.SetTag("kind", div.Kind.String())
		// Omit raw key from Sentry tags to avoid leaking sensitive data;
		// only send a truncated form as an extra for debugging.
		scope.SetExtra("key", truncateValue(div.Key))
		scope.SetExtra("primary", truncateValue(div.Primary))
		scope.SetExtra("secondary", truncateValue(div.Secondary))
		scope.SetFingerprint([]string{"divergence", div.Kind.String(), div.Command})
		scope.SetLevel(sentry.LevelWarning)
		r.hub.CaptureMessage(fmt.Sprintf("data divergence: %s %s", div.Kind, div.Command))
	})
}

// ShouldReport checks if this fingerprint has been reported recently (cooldown-based).
// Evicts expired entries when the map reaches maxReportEntries to bound memory usage.
// Returns false (drops the report) if the map is still at capacity after eviction.
func (r *SentryReporter) ShouldReport(fingerprint string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()

	// Evict expired entries if map grows too large
	if len(r.lastReport) >= maxReportEntries {
		for k, t := range r.lastReport {
			if now.Sub(t) >= r.cooldown {
				delete(r.lastReport, k)
			}
		}
		// If still at capacity after eviction, drop report to prevent unbounded growth and Sentry flooding.
		if len(r.lastReport) >= maxReportEntries {
			return false
		}
	}

	if last, ok := r.lastReport[fingerprint]; ok && now.Sub(last) < r.cooldown {
		return false
	}
	r.lastReport[fingerprint] = now
	return true
}

// Flush waits for pending Sentry events.
func (r *SentryReporter) Flush(timeout time.Duration) {
	if r.enabled {
		sentry.Flush(timeout)
	}
}

func cmdNameFromArgs(args [][]byte) string {
	if len(args) > 0 {
		return string(args[0])
	}
	return unknownStr
}

// truncateValue formats a value for logging/Sentry, truncating to avoid data leakage and oversized events.
// Handles common types by slicing before formatting to avoid allocating the full string representation.
func truncateValue(v any) string {
	var s string
	switch tv := v.(type) {
	case string:
		s = tv
	case []byte:
		if len(tv) > maxSentryValueLen {
			tv = tv[:maxSentryValueLen]
		}
		s = string(tv)
	default:
		s = fmt.Sprintf("%v", v)
	}
	if len(s) > maxSentryValueLen {
		return s[:maxSentryValueLen] + "...(truncated)"
	}
	return s
}
