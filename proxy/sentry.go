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

// CaptureDivergence reports a data divergence to Sentry.
func (r *SentryReporter) CaptureDivergence(div Divergence) {
	if !r.enabled {
		return
	}
	r.hub.WithScope(func(scope *sentry.Scope) {
		scope.SetTag("command", div.Command)
		scope.SetTag("key", div.Key)
		scope.SetTag("kind", div.Kind.String())
		scope.SetExtra("primary", fmt.Sprintf("%v", div.Primary))
		scope.SetExtra("secondary", fmt.Sprintf("%v", div.Secondary))
		scope.SetFingerprint([]string{"divergence", div.Kind.String(), div.Command})
		scope.SetLevel(sentry.LevelWarning)
		r.hub.CaptureMessage(fmt.Sprintf("data divergence: %s %s (%s)", div.Kind, div.Command, div.Key))
	})
}

// ShouldReport checks if this fingerprint has been reported recently (cooldown-based).
// Periodically evicts expired entries to prevent unbounded map growth.
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
		// If still at capacity after eviction, skip tracking to prevent unbounded growth.
		if len(r.lastReport) >= maxReportEntries {
			return true
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
