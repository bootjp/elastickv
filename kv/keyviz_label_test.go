package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/keyviz"
	"github.com/stretchr/testify/require"
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

type localLeaderGroupsStub struct {
	Coordinator
	ids []uint64
}

func (s localLeaderGroupsStub) LocalLeaderGroupIDs() ([]uint64, bool) { return s.ids, true }

// The Redis TTL-inline migrator asks the keyviz-labeled coordinator which data
// groups this node leads. The wrapper always has the method, so it must report
// an inner coordinator without the capability as unknown, not as leading no
// group: that answer made the migrator plan no scans at all. A known empty
// answer must stay known, because it means this node leads nothing.
func TestKeyVizLabeledCoordinatorLocalLeaderGroupIDs(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		inner     Coordinator
		wantIDs   []uint64
		wantKnown bool
	}{
		{
			name:      "forwards known groups",
			inner:     localLeaderGroupsStub{ids: []uint64{2, 5}},
			wantIDs:   []uint64{2, 5},
			wantKnown: true,
		},
		{
			name:      "keeps a known empty answer",
			inner:     localLeaderGroupsStub{ids: []uint64{}},
			wantIDs:   []uint64{},
			wantKnown: true,
		},
		{
			name:  "inner without the capability is unknown",
			inner: struct{ Coordinator }{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ids, known := LocalLeaderGroupIDsThrough(WithKeyVizLabel(tc.inner, keyviz.LabelRedis))
			require.Equal(t, tc.wantKnown, known)
			require.Equal(t, tc.wantIDs, ids)
		})
	}
}
