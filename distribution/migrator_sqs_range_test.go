package distribution

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSplitMigrationRejectsResolverOwnedSQSRoute(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, start, end string
		reject           bool
	}{
		{"global at start", "!sqs|route|global", "!sqs|route|z", true},
		{"global interior", "!sqs|route|g", "!sqs|route|z", true},
		{"unbounded", "!sqs|route|g", "", true},
		{"exclusive end", "!sqs|route|g", "!sqs|route|global", false},
		{"global stays left", "!sqs|route|global0", "!sqs|route|z", false},
		{"ordinary user interval", "m", "z", false},
		{"SQS-looking ordinary user interval", "!sqs|foo", "!sqs|fop", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			start, end := []byte(tc.start), []byte(tc.end)
			err := ValidateSplitMigrationRouteRange(start, end)
			if tc.reject {
				require.ErrorIs(t, err, ErrMigrationResolverOwnedRange)
			} else {
				require.NoError(t, err)
			}
			source := RouteDescriptor{RouteID: 1, Start: []byte("!sqs|"), End: end, GroupID: 7}
			job := SplitJob{JobID: 1, SourceRouteID: 1, SplitKey: start, TargetGroupID: 9}
			_, err = InitializeSplitJobPlan(job, source, 1000)
			if tc.reject {
				require.ErrorIs(t, err, ErrMigrationResolverOwnedRange)
			} else {
				require.NoError(t, err)
			}
			job.TargetGroupID = source.GroupID
			_, err = AdvanceSameGroupNoop(job, source, 1000)
			require.NoError(t, err, "same-group splits do not change partition ownership")
		})
	}
}
