package store

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

// OnlineMaintenanceGuard protects active source ranges during a replicated
// promotion or physical cleanup. The FSM supplies the sessions from its
// ordered apply context and the same key projection used by source capture.
type OnlineMaintenanceGuard struct {
	Sessions   []OnlineMigrationSession
	Intersects func([]byte, OnlineMigrationSession) bool
}

func (s *pebbleStore) stageOnlineMaintenanceGuard(batch *pebble.Batch, guard *OnlineMaintenanceGuard, rows []MVCCVersion) error {
	if guard == nil {
		return nil
	}
	for _, session := range guard.Sessions {
		for _, row := range rows {
			if !guard.Intersects(row.Key, session) {
				continue
			}
			if session.Sealed {
				return errors.WithStack(ErrOnlineMigrationSealed)
			}
			if session.Invalid == "" {
				session.Invalid = "live version changed by migration maintenance"
				if err := s.stageOnlineMetadataBatch(batch, onlineMigrationKey("source", session.Identity.JobID, 0), session); err != nil {
					return err
				}
			}
			break
		}
	}
	return nil
}
