package loadtest

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

const (
	DefaultConcurrency    = 32
	DefaultReadPercent    = 50
	DefaultSeedKeys       = 256
	DefaultValueBytes     = 1024
	DefaultMaxRequests    = 1000000
	DefaultStageDuration  = time.Minute
	DefaultRequestTimeout = 2 * time.Second
	operationStride       = 37
	operationCycle        = 100
	medianPercentile      = 50
	highPercentile        = 95
	tailPercentile        = 99
	maxPercentile         = 100
)

func validate(c Config) error {
	for _, check := range []func(Config) error{validateSize, validateWindow, validatePlan} {
		if err := check(c); err != nil {
			return err
		}
	}
	return nil
}
func validateSize(c Config) error {
	if c.Concurrency < 1 || c.Concurrency > 1024 || c.Keys < 1 || c.Keys > 1000000 {
		return errors.New("loadtest: concurrency must be 1..1024 and seed keys 1..1000000")
	}
	if c.ReadPercent < 0 || c.ReadPercent > 100 || c.ValueBytes < 128 || c.ValueBytes > 4<<20 {
		return errors.New("loadtest: read percent must be 0..100 and value size 128 bytes..4 MiB")
	}
	return nil
}
func validateWindow(c Config) error {
	if c.RequestTimeout <= 0 || c.StageDuration <= 0 || c.StageDuration > time.Hour || len(c.Rates) == 0 {
		return errors.New("loadtest: positive timeout and stage duration (at most 1h), and rates required")
	}
	if len(c.Prefix) == 0 || len(c.Prefix) > 64 || strings.ContainsAny(c.Prefix, "/\n\r\x00") {
		return errors.New("loadtest: unique prefix must be 1..64 bytes without slash or control delimiters")
	}
	return nil
}
func validatePlan(c Config) error {
	total := 0.0
	for _, rate := range c.Rates {
		if rate < 1 || rate > 1000000 {
			return fmt.Errorf("loadtest: invalid rate %d", rate)
		}
		total += math.Ceil(c.StageDuration.Seconds() * float64(rate))
	}
	if c.MaxRequests < 1 || total > float64(c.MaxRequests) {
		return errors.New("loadtest: planned requests exceed the explicit in-memory tracking limit")
	}
	return nil
}

var keyDomains = [...]string{"a", "p", "z"}
