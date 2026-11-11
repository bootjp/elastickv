package peersession

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMethodFactoryBindsConstructedOwner(t *testing.T) {
	t.Parallel()
	h, _ := controlFixture(t)
	cfg := Config{Local: h.Client, Versions: h.View.Installed, Provisioned: []Provision{{Node: h.Client, Groups: []uint64{testGroup}}}}
	var bound *Owner
	cfg.MethodFactory = func(owner *Owner) ([]Method, error) {
		bound = owner
		return []Method{{Name: sendMethod, Request: func(any) (Message, error) { return Message{}, nil },
			Reply: func(Message, any) (Message, error) { return Message{}, nil }}}, nil
	}
	owner, err := NewOwner(cfg)
	require.NoError(t, err)
	t.Cleanup(owner.Close)
	require.Same(t, bound, owner)
	_, found := owner.methods[sendMethod]
	require.True(t, found)
	cfg.Methods = []Method{{Name: sendMethod}}
	_, err = NewOwner(cfg)
	require.ErrorIs(t, err, ErrFormat)
	cfg.Methods = nil
	cfg.MethodFactory = func(*Owner) ([]Method, error) { return nil, ErrDenied }
	_, err = NewOwner(cfg)
	require.ErrorIs(t, err, ErrDenied)
}
