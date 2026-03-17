package proxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	unknownStr                    = "unknown"
	defaultGapLogSampleRate int64 = 1000
)

// DivergenceKind classifies the nature of a shadow-read mismatch.
type DivergenceKind int

const (
	DivMigrationGap DivergenceKind = iota // Secondary nil/empty, Primary has data → expected during migration
	DivDataMismatch                       // Both have data but differ → real inconsistency
	DivExtraData                          // Primary nil, Secondary has data → unexpected
)

func (k DivergenceKind) String() string {
	switch k {
	case DivMigrationGap:
		return "migration_gap"
	case DivDataMismatch:
		return "data_mismatch"
	case DivExtraData:
		return "extra_data"
	default:
		return unknownStr
	}
}

// Divergence records a detected mismatch between primary and secondary.
type Divergence struct {
	Command    string
	Key        string
	Kind       DivergenceKind
	Primary    interface{}
	Secondary  interface{}
	DetectedAt time.Time
}

// ShadowReader compares primary and secondary read results.
type ShadowReader struct {
	secondary Backend
	metrics   *ProxyMetrics
	sentry    *SentryReporter
	logger    *slog.Logger
	timeout   time.Duration

	// Sampling counter for migration gap logs (log 1 per gapLogSampleRate).
	gapCount         atomic.Int64
	gapLogSampleRate int64
}

// NewShadowReader creates a ShadowReader.
func NewShadowReader(secondary Backend, metrics *ProxyMetrics, sentryReporter *SentryReporter, logger *slog.Logger, timeout time.Duration) *ShadowReader {
	return &ShadowReader{
		secondary:        secondary,
		metrics:          metrics,
		sentry:           sentryReporter,
		logger:           logger,
		timeout:          timeout,
		gapLogSampleRate: defaultGapLogSampleRate,
	}
}

// Compare issues the same read to the secondary and checks for divergence.
func (s *ShadowReader) Compare(ctx context.Context, cmd string, args [][]byte, primaryResp interface{}, primaryErr error) {
	sCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	iArgs := bytesArgsToInterfaces(args)
	secondaryResult := s.secondary.Do(sCtx, iArgs...)
	secondaryResp, secondaryErr := secondaryResult.Result()

	if isConsistent(primaryResp, secondaryResp, primaryErr, secondaryErr) {
		return
	}

	// --- Divergence detected ---
	kind := classifyDivergence(primaryResp, primaryErr, secondaryResp, secondaryErr)

	if kind == DivMigrationGap {
		s.metrics.MigrationGaps.WithLabelValues(cmd).Inc()
		count := s.gapCount.Add(1)
		if count%s.gapLogSampleRate == 1 {
			s.logger.Debug("migration gap (sampled)", "cmd", cmd, "key", extractKey(args))
		}
		return
	}

	div := Divergence{
		Command:    cmd,
		Key:        extractKey(args),
		Kind:       kind,
		Primary:    formatResp(primaryResp, primaryErr),
		Secondary:  formatResp(secondaryResp, secondaryErr),
		DetectedAt: time.Now(),
	}

	s.metrics.Divergences.WithLabelValues(cmd, kind.String()).Inc()
	s.logger.Warn("response divergence detected",
		"cmd", div.Command, "key", div.Key, "kind", div.Kind.String(),
		"primary", fmt.Sprintf("%v", div.Primary),
		"secondary", fmt.Sprintf("%v", div.Secondary),
	)
	s.sentry.CaptureDivergence(div)
}

// isConsistent checks whether primary and secondary responses agree.
func isConsistent(primaryResp, secondaryResp interface{}, primaryErr, secondaryErr error) bool {
	// Both are redis.Nil → consistent (key missing on both)
	if isNilError(primaryErr) && isNilError(secondaryErr) {
		return true
	}
	// Both returned the same non-nil error → consistent
	if primaryErr != nil && secondaryErr != nil && primaryErr.Error() == secondaryErr.Error() {
		return true
	}
	// Both succeeded with equal values → consistent
	return primaryErr == nil && secondaryErr == nil && responseEqual(primaryResp, secondaryResp)
}

// responseEqual compares two go-redis response values for equality.
func responseEqual(a, b interface{}) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	switch av := a.(type) {
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case int64:
		bv, ok := b.(int64)
		return ok && av == bv
	case []interface{}:
		return interfaceSliceEqual(av, b)
	default:
		return reflect.DeepEqual(a, b)
	}
}

// interfaceSliceEqual compares two []interface{} slices element-by-element.
func interfaceSliceEqual(av []interface{}, b interface{}) bool {
	bv, ok := b.([]interface{})
	if !ok || len(av) != len(bv) {
		return false
	}
	for i := range av {
		if !responseEqual(av[i], bv[i]) {
			return false
		}
	}
	return true
}

// classifyDivergence determines the kind based on primary/secondary values.
func classifyDivergence(primaryResp interface{}, primaryErr error, secondaryResp interface{}, secondaryErr error) DivergenceKind {
	primaryNil := isNilResp(primaryResp, primaryErr)
	secondaryNil := isNilResp(secondaryResp, secondaryErr)

	switch {
	case !primaryNil && secondaryNil:
		return DivMigrationGap
	case primaryNil && !secondaryNil:
		return DivExtraData
	default:
		return DivDataMismatch
	}
}

func isNilError(err error) bool {
	return errors.Is(err, redis.Nil)
}

// isNilResp checks if a response represents "no data" (nil response or redis.Nil error).
// Empty string is NOT nil — it is a valid value.
func isNilResp(resp interface{}, err error) bool {
	if errors.Is(err, redis.Nil) {
		return true
	}
	return resp == nil
}

func formatResp(resp interface{}, err error) interface{} {
	if err != nil {
		return fmt.Sprintf("error: %v", err)
	}
	return resp
}

func extractKey(args [][]byte) string {
	if len(args) > 1 {
		return string(args[1])
	}
	return ""
}

func bytesArgsToInterfaces(args [][]byte) []interface{} {
	out := make([]interface{}, len(args))
	for i, a := range args {
		out[i] = a
	}
	return out
}
