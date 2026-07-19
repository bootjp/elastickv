package kek

import (
	"bytes"
	"context"
	"encoding/base64"
	"testing"

	"github.com/hashicorp/vault/api"
	"github.com/stretchr/testify/require"
)

type fakeVaultLogical struct {
	path       string
	data       map[string]interface{}
	dek        []byte
	ciphertext string
}

func (f *fakeVaultLogical) WriteWithContext(_ context.Context, path string, data map[string]interface{}) (*api.Secret, error) {
	f.path = path
	f.data = data
	if _, ok := data["plaintext"]; ok {
		ciphertext := f.ciphertext
		if ciphertext == "" {
			ciphertext = "vault:v1:ciphertext"
		}
		return &api.Secret{Data: map[string]interface{}{"ciphertext": ciphertext}}, nil
	}
	return &api.Secret{Data: map[string]interface{}{"plaintext": base64.StdEncoding.EncodeToString(f.dek)}}, nil
}

func TestVaultTransitWrapperRequestBinding(t *testing.T) {
	dek := bytes.Repeat([]byte{0x62}, fileKEKSize)
	logical := &fakeVaultLogical{dek: dek}
	wrapper := newVaultTransitWrapper(logical, "transit", "service/key")

	wrapped, err := wrapper.Wrap(dek)
	require.NoError(t, err)
	require.Equal(t, []byte("vault:v1:ciphertext"), wrapped)
	require.Equal(t, "transit/encrypt/service/key", logical.path)
	require.Equal(t, base64.StdEncoding.EncodeToString(dek), logical.data["plaintext"])

	plain, err := wrapper.Unwrap(wrapped)
	require.NoError(t, err)
	require.Equal(t, dek, plain)
	require.Equal(t, "transit/decrypt/service/key", logical.path)
	require.Equal(t, "vault:v1:ciphertext", logical.data["ciphertext"])
	require.Equal(t, "vault-transit:transit/service/key", wrapper.Name())
}

func TestParseVaultTarget(t *testing.T) {
	mount, keyName, err := parseVaultTarget("transit/service/key")
	require.NoError(t, err)
	require.Equal(t, "transit", mount)
	require.Equal(t, "service/key", keyName)
	for _, target := range []string{"", "transit", "transit//key", "../key", "transit/../key"} {
		t.Run(target, func(t *testing.T) {
			_, _, err := parseVaultTarget(target)
			require.ErrorIsf(t, err, ErrInvalidKEKURI, "target=%q", target)
		})
	}
}

func TestVaultTransitWrapperRejectsMalformedResponses(t *testing.T) {
	logical := &fakeVaultLogical{dek: []byte("short")}
	wrapper := newVaultTransitWrapper(logical, "transit", "key")
	_, err := wrapper.Unwrap([]byte("vault:v1:ciphertext"))
	require.ErrorIs(t, err, ErrInvalidDEKLength)
	_, err = wrapper.Unwrap([]byte("not-vault"))
	require.ErrorIs(t, err, ErrInvalidProviderResponse)
	for _, ciphertext := range []string{"vault:v", "vault:v1:", "vault:v0:data", "vault:vx:data"} {
		t.Run(ciphertext, func(t *testing.T) {
			invalid := newVaultTransitWrapper(&fakeVaultLogical{dek: bytes.Repeat([]byte{1}, fileKEKSize), ciphertext: ciphertext}, "transit", "key")
			_, wrapErr := invalid.Wrap(bytes.Repeat([]byte{2}, fileKEKSize))
			require.ErrorIs(t, wrapErr, ErrInvalidProviderResponse)
			_, unwrapErr := invalid.Unwrap([]byte(ciphertext))
			require.ErrorIs(t, unwrapErr, ErrInvalidProviderResponse)
		})
	}
}
