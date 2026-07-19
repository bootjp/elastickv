package kek

import (
	"context"
	"encoding/base64"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	vaultapi "github.com/hashicorp/vault/api"
)

type vaultLogicalClient interface {
	WriteWithContext(context.Context, string, map[string]interface{}) (*vaultapi.Secret, error)
}

// VaultTransitWrapper wraps DEKs with a Vault Transit symmetric key.
type VaultTransitWrapper struct {
	logical vaultLogicalClient
	mount   string
	keyName string
	timeout time.Duration
}

// NewVaultTransitWrapper uses the standard VAULT_ADDR, VAULT_TOKEN, TLS, and
// namespace environment configuration. target is <mount>/<key-name>.
func NewVaultTransitWrapper(target string) (*VaultTransitWrapper, error) {
	mount, keyName, err := parseVaultTarget(target)
	if err != nil {
		return nil, err
	}
	config := vaultapi.DefaultConfig()
	if err := config.ReadEnvironment(); err != nil {
		return nil, errors.Wrap(err, "kek: read Vault environment")
	}
	client, err := vaultapi.NewClient(config)
	if err != nil {
		return nil, errors.Wrap(err, "kek: create Vault client")
	}
	client.SetToken(os.Getenv("VAULT_TOKEN"))
	return newVaultTransitWrapper(client.Logical(), mount, keyName), nil
}

func newVaultTransitWrapper(logical vaultLogicalClient, mount, keyName string) *VaultTransitWrapper {
	return &VaultTransitWrapper{logical: logical, mount: mount, keyName: keyName, timeout: providerRequestTimeout}
}

func parseVaultTarget(target string) (string, string, error) {
	parts := strings.Split(target, "/")
	if len(parts) < 2 || parts[0] == "" {
		return "", "", errors.Wrapf(ErrInvalidKEKURI, "invalid Vault Transit target %q", target)
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return "", "", errors.Wrapf(ErrInvalidKEKURI, "invalid Vault Transit target %q", target)
		}
	}
	return parts[0], strings.Join(parts[1:], "/"), nil
}

// Wrap base64-encodes the binary DEK for Vault's JSON API and stores Vault's
// versioned ciphertext string as the wrapped sidecar bytes.
func (w *VaultTransitWrapper) Wrap(dek []byte) ([]byte, error) {
	if w == nil || w.logical == nil {
		return nil, errors.Wrap(ErrInvalidProviderResponse, "kek: Vault client is nil")
	}
	if err := validateDEK(dek); err != nil {
		return nil, err
	}
	ctx, cancel := requestContext(w.timeout)
	defer cancel()
	secret, err := w.logical.WriteWithContext(ctx, w.mount+"/encrypt/"+w.keyName, map[string]interface{}{
		"plaintext": base64.StdEncoding.EncodeToString(dek),
	})
	if err != nil {
		return nil, errors.Wrap(err, "kek: Vault Transit encrypt")
	}
	ciphertext, ok := vaultString(secret, "ciphertext")
	if !ok || !validVaultCiphertext(ciphertext) {
		return nil, errors.WithStack(ErrInvalidProviderResponse)
	}
	return []byte(ciphertext), nil
}

// Unwrap asks Vault Transit to decrypt its versioned ciphertext and validates
// the returned base64 plaintext as a 32-byte DEK.
func (w *VaultTransitWrapper) Unwrap(wrapped []byte) ([]byte, error) {
	if w == nil || w.logical == nil {
		return nil, errors.Wrap(ErrInvalidProviderResponse, "kek: Vault client is nil")
	}
	if !validVaultCiphertext(string(wrapped)) {
		return nil, errors.Wrap(ErrInvalidProviderResponse, "kek: invalid Vault ciphertext")
	}
	ctx, cancel := requestContext(w.timeout)
	defer cancel()
	secret, err := w.logical.WriteWithContext(ctx, w.mount+"/decrypt/"+w.keyName, map[string]interface{}{
		"ciphertext": string(wrapped),
	})
	if err != nil {
		return nil, errors.Wrap(err, "kek: Vault Transit decrypt")
	}
	encoded, ok := vaultString(secret, "plaintext")
	if !ok {
		return nil, errors.WithStack(ErrInvalidProviderResponse)
	}
	plain, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, errors.Wrap(ErrInvalidProviderResponse, "kek: Vault plaintext is not base64")
	}
	if err := validateDEK(plain); err != nil {
		clear(plain)
		return nil, errors.Wrap(err, "kek: Vault plaintext")
	}
	return plain, nil
}

func validVaultCiphertext(ciphertext string) bool {
	const prefix = "vault:v"
	if !strings.HasPrefix(ciphertext, prefix) {
		return false
	}
	rest := strings.TrimPrefix(ciphertext, prefix)
	separator := strings.IndexByte(rest, ':')
	if separator <= 0 || separator == len(rest)-1 {
		return false
	}
	version, err := strconv.ParseUint(rest[:separator], 10, 64)
	return err == nil && version > 0
}

func vaultString(secret *vaultapi.Secret, field string) (string, bool) {
	if secret == nil || secret.Data == nil {
		return "", false
	}
	value, ok := secret.Data[field].(string)
	return value, ok && value != ""
}

// Name returns the provider and Transit mount/key path.
func (w *VaultTransitWrapper) Name() string { return "vault-transit:" + w.mount + "/" + w.keyName }
