package admin

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func makeKey(seed byte) string {
	raw := make([]byte, sessionSigningKeyLen)
	for i := range raw {
		raw[i] = seed
	}
	return base64.StdEncoding.EncodeToString(raw)
}

func TestConfigValidate_DisabledNoOp(t *testing.T) {
	c := &Config{}
	require.NoError(t, c.Validate())
}

func TestConfigValidate_RequiresListen(t *testing.T) {
	c := &Config{
		Enabled:           true,
		SessionSigningKey: makeKey(1),
	}
	err := c.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "admin.listen must not be empty")
}

func TestConfigValidate_RequiresSigningKey(t *testing.T) {
	c := &Config{
		Enabled: true,
		Listen:  "127.0.0.1:8080",
	}
	err := c.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "session_signing_key is required")
}

func TestConfigValidate_SigningKeyWrongLength(t *testing.T) {
	short := base64.StdEncoding.EncodeToString([]byte("too short"))
	c := &Config{
		Enabled:           true,
		Listen:            "127.0.0.1:8080",
		SessionSigningKey: short,
	}
	err := c.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "must decode to 64 bytes")
}

func TestConfigValidate_SigningKeyNotBase64(t *testing.T) {
	c := &Config{
		Enabled:           true,
		Listen:            "127.0.0.1:8080",
		SessionSigningKey: "!!!not base64!!!",
	}
	err := c.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "not valid base64")
}

func TestConfigValidate_LoopbackNoTLSOK(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:8080", "[::1]:8080", "localhost:8080"} {
		c := &Config{
			Enabled:           true,
			Listen:            addr,
			SessionSigningKey: makeKey(7),
		}
		require.NoErrorf(t, c.Validate(), "loopback %s", addr)
	}
}

func TestConfigValidate_NonLoopbackRequiresTLS(t *testing.T) {
	c := &Config{
		Enabled:           true,
		Listen:            "0.0.0.0:8080",
		SessionSigningKey: makeKey(2),
	}
	err := c.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "TLS is not configured")
	require.Contains(t, err.Error(), "allow-plaintext-non-loopback")
}

func TestConfigValidate_NonLoopbackWithTLSOK(t *testing.T) {
	c := &Config{
		Enabled:           true,
		Listen:            "10.0.0.1:8443",
		TLSCertFile:       "cert.pem",
		TLSKeyFile:        "key.pem",
		SessionSigningKey: makeKey(3),
	}
	require.NoError(t, c.Validate())
}

func TestConfigValidate_NonLoopbackPlaintextOptInOK(t *testing.T) {
	c := &Config{
		Enabled:                   true,
		Listen:                    "0.0.0.0:8080",
		AllowPlaintextNonLoopback: true,
		SessionSigningKey:         makeKey(4),
	}
	require.NoError(t, c.Validate())
}

func TestConfigValidate_OverlappingRolesRejected(t *testing.T) {
	c := &Config{
		Enabled:            true,
		Listen:             "127.0.0.1:8080",
		SessionSigningKey:  makeKey(5),
		ReadOnlyAccessKeys: []string{"AKIA1", "AKIA2"},
		FullAccessKeys:     []string{"AKIA2", "AKIA3"},
	}
	err := c.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "AKIA2")
	require.Contains(t, err.Error(), "both")
}

func TestConfigValidate_RoleIndexExclusive(t *testing.T) {
	c := &Config{
		Enabled:            true,
		Listen:             "127.0.0.1:8080",
		SessionSigningKey:  makeKey(6),
		ReadOnlyAccessKeys: []string{"AKIA_RO"},
		FullAccessKeys:     []string{"AKIA_ADMIN"},
	}
	require.NoError(t, c.Validate())
	idx := c.RoleIndex()
	require.Equal(t, RoleReadOnly, idx["AKIA_RO"])
	require.Equal(t, RoleFull, idx["AKIA_ADMIN"])
	_, unknown := idx["AKIA_NOBODY"]
	require.False(t, unknown)
}

func TestConfigValidate_PreviousSigningKeyValidated(t *testing.T) {
	c := &Config{
		Enabled:                   true,
		Listen:                    "127.0.0.1:8080",
		SessionSigningKey:         makeKey(1),
		SessionSigningKeyPrevious: base64.StdEncoding.EncodeToString([]byte("short")),
	}
	err := c.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "session_signing_key_previous")
}

func TestConfigDecodedSigningKeys_Order(t *testing.T) {
	c := &Config{
		Enabled:                   true,
		Listen:                    "127.0.0.1:8080",
		SessionSigningKey:         makeKey(1),
		SessionSigningKeyPrevious: makeKey(2),
	}
	require.NoError(t, c.Validate())
	keys, err := c.DecodedSigningKeys()
	require.NoError(t, err)
	require.Len(t, keys, 2)
	require.Equal(t, byte(1), keys[0][0])
	require.Equal(t, byte(2), keys[1][0])
}

func TestRole_AllowsWrite(t *testing.T) {
	require.True(t, RoleFull.AllowsWrite())
	require.False(t, RoleReadOnly.AllowsWrite())
	require.False(t, Role("").AllowsWrite())
}

func TestAddressRequiresTLS(t *testing.T) {
	require.False(t, addressRequiresTLS("127.0.0.1:8080"))
	require.False(t, addressRequiresTLS("[::1]:8080"))
	require.False(t, addressRequiresTLS("localhost:8080"))
	require.True(t, addressRequiresTLS(":8080"))
	require.True(t, addressRequiresTLS("0.0.0.0:8080"))
	require.True(t, addressRequiresTLS("10.0.0.1:8080"))
	require.True(t, addressRequiresTLS("garbage"))
}

func TestConfigValidate_DisabledIgnoresBadFields(t *testing.T) {
	c := &Config{
		Enabled: false,
		Listen:  "not a host port",
	}
	require.NoError(t, c.Validate())
}

// TestConfigValidate_PreservesContext ensures the validation message
// includes the offending field so operators can resolve errors quickly.
func TestConfigValidate_PreservesContext(t *testing.T) {
	c := &Config{
		Enabled:           true,
		Listen:            "::::::::",
		SessionSigningKey: makeKey(8),
	}
	err := c.Validate()
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "admin.listen"))
}
