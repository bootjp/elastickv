package autosplit

import (
	"context"
	"sync/atomic"
)

// RuntimeSwitch is the process-local operator switch for automatic splitting.
// It is independent from the startup Enabled flag: disabling it keeps detector
// observation active while blocking all new SplitRange calls.
type RuntimeSwitch struct {
	enabled atomic.Bool
}

// NewRuntimeSwitch creates a runtime switch with the requested initial state.
func NewRuntimeSwitch(enabled bool) *RuntimeSwitch {
	s := &RuntimeSwitch{}
	s.enabled.Store(enabled)
	return s
}

// Enabled returns whether new automatic splits are allowed.
func (s *RuntimeSwitch) Enabled() bool {
	return s != nil && s.enabled.Load()
}

// SetEnabled atomically changes whether new automatic splits are allowed.
func (s *RuntimeSwitch) SetEnabled(enabled bool) {
	if s != nil {
		s.enabled.Store(enabled)
	}
}

// KillSwitch adapts RuntimeSwitch to SchedulerConfig.KillSwitch.
func (s *RuntimeSwitch) KillSwitch(context.Context) bool {
	return !s.Enabled()
}
