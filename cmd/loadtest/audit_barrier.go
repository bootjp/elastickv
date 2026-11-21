package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
)

const auditMarkerPollInterval = 100 * time.Millisecond
const defaultAuditWaitTimeout = 5 * time.Minute

func checkAuditMarker(o options) error {
	if o.AuditReadyFile == "" {
		return nil
	}
	if o.AuditWaitTimeout <= 0 {
		return errors.New("audit wait timeout must be positive")
	}
	_, err := os.Stat(o.AuditReadyFile)
	if err == nil {
		return errors.New("audit-ready marker already exists; use a fresh run directory")
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
func waitForAudit(ctx context.Context, o options, encoder *json.Encoder) error {
	if err := encoder.Encode(map[string]any{"type": "load_complete", "time": time.Now().UTC()}); err != nil {
		return err
	}
	if o.AuditReadyFile == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, o.AuditWaitTimeout)
	defer cancel()
	ticker := time.NewTicker(auditMarkerPollInterval)
	defer ticker.Stop()
	for {
		_, err := os.Stat(o.AuditReadyFile)
		if err == nil {
			return nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for Split audit barrier: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}
