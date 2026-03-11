package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCleanupStackRunUsesLIFO(t *testing.T) {
	stack := CleanupStack{}
	var calls []string

	stack.Add(func() { calls = append(calls, "first") })
	stack.Add(func() { calls = append(calls, "second") })
	stack.Run()

	require.Equal(t, []string{"second", "first"}, calls)
}

func TestCleanupStackReleaseDropsCallbacks(t *testing.T) {
	stack := CleanupStack{}
	called := false

	stack.Add(func() { called = true })
	stack.Release()
	stack.Run()

	require.False(t, called)
}
