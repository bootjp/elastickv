package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommitTSFenceRuntimesReady(t *testing.T) {
	domain := &bootstrapDomain{}
	ready := func() bool { return true }
	blocked := func() bool { return false }
	cases := []struct {
		name     string
		runtimes []*raftGroupRuntime
		want     bool
	}{
		{name: "no runtimes"},
		{name: "nil runtime", runtimes: []*raftGroupRuntime{nil}},
		{name: "legacy runtime", runtimes: []*raftGroupRuntime{{publicReady: ready}}},
		{name: "missing callback", runtimes: []*raftGroupRuntime{{bootstrapDomain: domain}}},
		{name: "not ready", runtimes: []*raftGroupRuntime{{bootstrapDomain: domain, publicReady: blocked}}},
		{name: "mixed domains", runtimes: []*raftGroupRuntime{{bootstrapDomain: domain, publicReady: ready}, {bootstrapDomain: &bootstrapDomain{}, publicReady: ready}}},
		{name: "missing group", runtimes: []*raftGroupRuntime{{bootstrapDomain: domain, publicReady: ready}, nil}},
		{name: "all protected groups ready", runtimes: []*raftGroupRuntime{{bootstrapDomain: domain, publicReady: ready}, {bootstrapDomain: domain, publicReady: ready}}, want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, commitTSFenceRuntimesReady(tc.runtimes))
		})
	}
}
