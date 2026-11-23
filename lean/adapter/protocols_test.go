package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDispatchedGroupFollowsTheAttemptRequest(t *testing.T) {
	const request = "req := &kv.OperationGroup[kv.OP]{IsTxn: true}; "
	const send = "s.dispatchSQSMutation(ctx, req)"
	for _, tc := range []struct {
		name, body string
		valid      bool
	}{
		{"one per attempt", request + send, true},
		{"same request twice", request + send + "; " + send, false},
		{"decoy literal", request + "s.dispatchSQSMutation(ctx, other)", false},
		{"mutated request", request + "req.Elems = nil; " + send, false},
		{"aliased dispatch", request + "again := s.dispatchSQSMutation; again(ctx, req)", false},
		{"nested dispatch loop", request + "for range 2 {" + send + "}", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, err := parser.ParseFile(token.NewFileSet(), "probe.go",
				"package p; func probe() { for range limit {"+tc.body+"} }", 0)
			require.NoError(t, err)
			fn, ok := f.Decls[0].(*ast.FuncDecl)
			require.True(t, ok)
			check := func() { dispatchedGroup(fn, "s.dispatchSQSMutation", "range limit") }
			if tc.valid {
				require.NotPanics(t, check)
			} else {
				require.Panics(t, check)
			}
		})
	}
}
