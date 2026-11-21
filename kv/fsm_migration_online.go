package kv

import (
	"bytes"
	"context"
	"crypto/sha256"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

type onlineMigrationWriteContextKey struct{}

func (f *kvFSM) onlineMigrationSessions(ctx context.Context) ([]store.OnlineMigrationSession, error) {
	reader, ok := f.store.(store.MigrationTargetReadinessReader)
	if !ok {
		return nil, nil
	}
	states, err := reader.MigrationTargetReadinessStates(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var sessions []store.OnlineMigrationSession
	for _, state := range states {
		if !state.Armed || !state.TrackWrites {
			continue
		}
		session, err := store.ReadOnlineMigrationSession(ctx, f.store, state.JobID)
		if errors.Is(err, store.ErrKeyNotFound) || errors.Is(err, store.ErrNotSupported) {
			continue
		}
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if !session.Closed {
			sessions = append(sessions, session)
		}
	}
	return sessions, nil
}

func onlineRequestIntersects(request *pb.Request, session store.OnlineMigrationSession) bool {
	// Legacy list delta rows share their physical prefix with ordinary list
	// metadata. BASE routes them using their value, and a tombstone belongs
	// to both possible owners. Include the second candidate before deciding
	// the mutation can bypass capture or a sealed source.
	for _, mutation := range request.GetMutations() {
		if onlineLegacyKeyIntersects(mutation.GetKey(), session) {
			return true
		}
		if mutation.GetOp() == pb.Op_DEL_PREFIX && legacyListDeltaPrefixOverlap(mutation.GetKey()) {
			return true
		}
	}
	ranges, err := migrationServingFootprint(request)
	if err != nil {
		return true
	}
	for _, r := range ranges {
		if routeRangeIntersects(r.start, r.end, session.RouteStart, session.RouteEnd) {
			return true
		}
	}
	return false
}

func onlineLegacyKeyIntersects(key []byte, session store.OnlineMigrationSession) bool {
	legacy := store.ExtractLegacyListUserKeyFromDelta(key)
	return legacy != nil && routeRangeIntersects(legacy, nextScanCursor(legacy), session.RouteStart, session.RouteEnd)
}

func legacyListDeltaPrefixOverlap(prefix []byte) bool {
	legacy := []byte(store.LegacyListMetaDeltaPrefix)
	return bytes.HasPrefix(prefix, legacy) || bytes.HasPrefix(legacy, prefix)
}

func onlineCapturePointKey(key []byte) bool {
	return !distribution.IsMigrationKnownInternalKey(key) && !isTxnInternalKey(key) && bytes.Equal(routeKey(key), key)
}

func onlineCaptureKeyInRange(key []byte, session store.OnlineMigrationSession) bool {
	return bytes.Compare(key, session.RouteStart) >= 0 && (len(session.RouteEnd) == 0 || bytes.Compare(key, session.RouteEnd) < 0)
}

func onlinePointRequests(requests []*pb.Request) bool {
	if !migrationCaptureShadowEligible(requests) {
		return false
	}
	for _, request := range requests {
		for _, mutation := range request.Mutations {
			if !onlineCapturePointKey(mutation.Key) {
				return false
			}
		}
	}
	return true
}

func (f *kvFSM) applyOnlineMigrationRequests(ctx context.Context, data []byte, requests []*pb.Request) (any, bool) {
	sessions, err := f.onlineMigrationSessions(ctx)
	if err != nil {
		return haltErr(errors.Mark(err, ErrRaftEntryApply)), true
	}
	affected := affectedOnlineSessions(sessions, requests)
	if len(affected) == 0 {
		return nil, false
	}
	ctx = context.WithValue(ctx, onlineMigrationWriteContextKey{}, true)
	if len(affected) != 1 || !onlinePointRequests(requests) {
		if err := f.invalidateUnsupportedOnlineSessions(ctx, affected); err != nil {
			return haltErr(errors.Mark(err, ErrRaftEntryApply)), true
		}
		return f.applyDecodedRequests(ctx, requests), true
	}
	return f.applyOnlinePointRequests(ctx, data, requests, affected[0]), true
}

func (f *kvFSM) applyOnlinePointRequests(ctx context.Context, data []byte, requests []*pb.Request, session store.OnlineMigrationSession) any {
	if session.Invalid != "" || session.Sealed {
		return f.applyDecodedRequests(ctx, requests)
	}
	if !store.OnlineMigrationSupported(f.store) {
		return haltErr(errors.Mark(store.ErrNotSupported, ErrRaftEntryApply))
	}
	if session.ConfigurationIndex != f.AppliedConfigurationIndex() {
		if err := store.InvalidateOnlineMigration(ctx, f.store, session, "source configuration changed"); err != nil {
			return haltErr(errors.Mark(err, ErrRaftEntryApply))
		}
		return f.applyDecodedRequests(ctx, requests)
	}
	return f.captureOnlinePointRequests(ctx, data, requests, session)
}

func affectedOnlineSessions(sessions []store.OnlineMigrationSession, requests []*pb.Request) []store.OnlineMigrationSession {
	var affected []store.OnlineMigrationSession
	for _, session := range sessions {
		for _, request := range requests {
			if onlineRequestIntersects(request, session) {
				affected = append(affected, session)
				break
			}
		}
	}
	return affected
}

func (f *kvFSM) invalidateUnsupportedOnlineSessions(ctx context.Context, sessions []store.OnlineMigrationSession) error {
	for _, session := range sessions {
		if session.Invalid != "" || session.Sealed {
			continue
		}
		if err := store.InvalidateOnlineMigration(ctx, f.store, session, "unsupported data entry"); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (f *kvFSM) captureOnlinePointRequests(ctx context.Context, data []byte, requests []*pb.Request, session store.OnlineMigrationSession) any {
	results := make([]error, len(requests))
	entered := false
	f.beginDeferredApplyNotices()
	entry, err := store.CaptureOnlineMigrationEntry(ctx, f.store, session.Identity.JobID, f.pendingApplyIdx, sha256.Sum256(data), func(key []byte) bool {
		return !distribution.IsMigrationKnownInternalKey(key) && !isTxnInternalKey(key) && onlineCaptureKeyInRange(key, session)
	}, func(tx context.Context, writer *store.MigrationCaptureWriter) error {
		entered = true
		for i, request := range requests {
			if err := writer.Request(func() (store.MigrationCaptureOutcome, error) {
				results[i] = f.applyRequestErr(tx, request)
				return migrationCaptureShadowOutcome(results[i])
			}); err != nil {
				return errors.WithStack(err)
			}
		}
		return nil
	})
	if err != nil {
		f.discardDeferredApplyNotices()
		return haltErr(errors.Mark(err, ErrRaftEntryApply))
	}
	if !entered {
		results, err = migrationCaptureShadowReplay(entry, len(requests))
		if err != nil {
			f.discardDeferredApplyNotices()
			return haltErr(errors.Mark(err, ErrRaftEntryApply))
		}
	}
	f.flushDeferredApplyNotices()
	return migrationCaptureShadowResponse(results)
}

// Invalidation precedes unsupported typed writes and never advances their data
// applied index. Pure migration metadata controls remain in the ordinary path.
func (f *kvFSM) invalidateOnlineMigrationControl(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	// Imports write only job-private staged keys. Promotion and cleanup check
	// the actual affected live rows in the same batch as their changes.
	if data[0] < fsmwire.OpEncryptionMin || data[0] > fsmwire.OpEncryptionMax {
		return nil
	}
	sessions, err := f.onlineMigrationSessions(ctx)
	if err != nil {
		return err
	}
	for _, session := range sessions {
		if session.Sealed {
			return errors.WithStack(store.ErrOnlineMigrationSealed)
		}
	}
	for _, session := range sessions {
		if session.Invalid == "" {
			if err := store.InvalidateOnlineMigration(ctx, f.store, session, "unsupported control entry"); err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return nil
}

func (f *kvFSM) verifyOnlineSourceNotSealed(ctx context.Context, req *pb.Request) error {
	sessions, err := f.onlineMigrationSessions(ctx)
	if err != nil {
		return err
	}
	for _, session := range sessions {
		if session.Sealed && onlineRequestIntersects(req, session) {
			return errors.WithStack(ErrRouteWriteFenced)
		}
	}
	return nil
}
