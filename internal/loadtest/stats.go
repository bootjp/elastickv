// Package loadtest provides bounded, paced RawKV load and acknowledged-write audits.
package loadtest

import (
	"errors"
	"sort"
	"time"

	"google.golang.org/grpc/status"
)

// Latency includes failed attempts; SuccessfulLatency reports only successes.
type Latency struct {
	Samples int     `json:"samples"`
	P50MS   float64 `json:"p50_ms"`
	P95MS   float64 `json:"p95_ms"`
	P99MS   float64 `json:"p99_ms"`
	MaxMS   float64 `json:"max_ms"`
}

type Metric struct {
	Attempts          int            `json:"attempts"`
	Success           int            `json:"success"`
	Errors            map[string]int `json:"errors"`
	FirstError        string         `json:"first_error,omitempty"`
	Latency           Latency        `json:"latency"`
	SuccessfulLatency Latency        `json:"successful_latency"`
}

type samples struct {
	all        []time.Duration
	success    []time.Duration
	errors     map[string]int
	firstError string
}

func (s *samples) record(elapsed time.Duration, err error) {
	s.all = append(s.all, elapsed)
	if err == nil {
		s.success = append(s.success, elapsed)
		return
	}
	if s.errors == nil {
		s.errors = make(map[string]int)
	}
	if s.firstError == "" {
		s.firstError = err.Error()
	}
	code := status.Code(err).String()
	if errors.Is(err, ErrMismatch) {
		code = "value_mismatch"
	}
	s.errors[code]++
}

func (s *samples) merge(other samples) {
	s.all = append(s.all, other.all...)
	s.success = append(s.success, other.success...)
	if s.errors == nil {
		s.errors = make(map[string]int)
	}
	if s.firstError == "" {
		s.firstError = other.firstError
	}
	for code, n := range other.errors {
		s.errors[code] += n
	}
}

func (s *samples) metric() Metric {
	return Metric{Attempts: len(s.all), Success: len(s.success), Errors: s.errors, FirstError: s.firstError,
		Latency: percentiles(s.all), SuccessfulLatency: percentiles(s.success)}
}

func percentiles(values []time.Duration) Latency {
	if len(values) == 0 {
		return Latency{}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	at := func(p int) float64 { return float64(values[(len(values)-1)*p/100]) / float64(time.Millisecond) }
	return Latency{Samples: len(values), P50MS: at(medianPercentile), P95MS: at(highPercentile), P99MS: at(tailPercentile), MaxMS: at(maxPercentile)}
}
