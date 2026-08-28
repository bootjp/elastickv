package kv

import (
	"context"
	"testing"
)

// The Redis adapter wraps its coordinator in WithKeyVizLabel, so this wrapper is
// what the read fence type-asserts against. If it stops forwarding the
// group-keyed methods, every fence target silently falls back to key resolution
// and the legacy wide-column group is dropped again.
func TestKeyVizLabeledCoordinatorForwardsGroupRouting(t *testing.T) {
	t.Parallel()

	var c any = keyVizLabeledCoordinator{}
	if _, ok := c.(GroupLeaderRoutableCoordinator); !ok {
		t.Fatal("keyVizLabeledCoordinator must satisfy GroupLeaderRoutableCoordinator")
	}
	if _, ok := c.(interface {
		LeaseReadForGroup(context.Context, uint64) (uint64, error)
	}); !ok {
		t.Fatal("keyVizLabeledCoordinator must forward LeaseReadForGroup")
	}
}
