package autosplit

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRuntimeSwitchAdaptsToKillSwitch(t *testing.T) {
	t.Parallel()
	runtime := NewRuntimeSwitch(true)
	require.True(t, runtime.Enabled())
	require.False(t, runtime.KillSwitch(context.Background()))

	runtime.SetEnabled(false)
	require.False(t, runtime.Enabled())
	require.True(t, runtime.KillSwitch(context.Background()))
}
