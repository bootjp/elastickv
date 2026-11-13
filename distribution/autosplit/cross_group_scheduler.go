package autosplit

import (
	"context"
	"time"

	"github.com/bootjp/elastickv/distribution"
)

const targetEvidenceMaxAgeSteps = 2

func (s *Scheduler) prepareCrossGroupTick(prep *tickPreparation, now time.Time, runtimeEnabled bool) {
	if !runtimeEnabled || !s.cfg.CrossGroupEnabled {
		return
	}
	prep.result.Detector.Decisions = s.selectCrossGroupTargets(prep.result.Detector.Decisions, *prep, now)
}

func (s *Scheduler) excludeBusySplitJobs(ctx context.Context, prep *tickPreparation) error {
	if !s.cfg.CrossGroupEnabled || prep.stopAfterPrepare {
		return nil
	}
	busy, err := activeSplitJobRouteIDs(ctx, s.source, prep.snapshot)
	if err != nil {
		return err
	}
	for i := range prep.routes {
		if _, exists := busy[prep.routes[i].RouteID]; exists {
			prep.routes[i].State = distribution.RouteStateMigratingSource
			s.state.ResetConfidence(prep.routes[i].RouteID)
		}
	}
	return nil
}

func (s *Scheduler) selectCrossGroupTargets(decisions []Decision, prep tickPreparation, now time.Time) []Decision {
	registrations, ok := s.sampler.(RouteWindowRegistration)
	if !ok || len(prep.windows) == 0 || len(s.cfg.CandidateGroupIDs) == 0 {
		return decisions
	}
	window := prep.windows[len(prep.windows)-1].Column
	evidence := BuildLocalTargetEvidence(window, prep.snapshot.Routes, registrations, s.servedWholeWindow, s.cfg.Detector)
	maxAge := targetEvidenceMaxAgeSteps * s.sampler.Step()
	if maxAge <= 0 {
		return decisions
	}
	for i := range decisions {
		input := TargetSelectionInput{
			Enabled:           true,
			Decision:          decisions[i],
			P50LoadKnown:      isP50Decision(decisions[i].SplitOrigin),
			CandidateGroupIDs: s.cfg.CandidateGroupIDs,
			Routes:            prep.snapshot.Routes,
			Evidence:          evidence,
			WindowStart:       window.WindowStart,
			WindowEnd:         window.At,
			Now:               now,
			MaxEvidenceAge:    maxAge,
		}
		decisions[i].TargetGroupID = SelectTargetGroup(input)
		if decisions[i].TargetGroupID == 0 && rightChildEligible(input) && hasOtherCandidateGroup(input) {
			if observer, ok := s.cfg.Observer.(targetSkipObserver); ok {
				observer.ObserveTargetSkipped("load_unknown")
			}
		}
	}
	return decisions
}

func hasOtherCandidateGroup(in TargetSelectionInput) bool {
	for _, groupID := range in.CandidateGroupIDs {
		if groupID != 0 && groupID != in.Decision.RouteGroupID {
			return true
		}
	}
	return false
}

func isP50Decision(origin SplitOrigin) bool {
	switch origin {
	case SplitOriginP50Mid, SplitOriginP50LastBucketLo, SplitOriginP50FirstBucketHi:
		return true
	case SplitOriginIsolationCompound, SplitOriginIsolationSingleLowerEdge, SplitOriginIsolationSingleUpperEdge:
		return false
	}
	return false
}

func (s *Scheduler) servedWholeWindow(groupID, term uint64, windowStart, windowEnd time.Time) bool {
	state, ok := s.groupLeadership[groupID]
	return ok && state.leader && term != 0 && state.term == term &&
		!state.startedAt.IsZero() && !windowStart.Before(state.startedAt) &&
		windowStart.Before(windowEnd) && (state.watermark.IsZero() || windowEnd.After(state.watermark))
}
