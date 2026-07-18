package kek

import (
	"context"
	"hash/crc32"
	"strings"
	"time"

	gcpkms "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var gcpKMSAAD = []byte("elastickv-dek-wrap-v1")

type gcpKMSClient interface {
	Encrypt(context.Context, *kmspb.EncryptRequest) (*kmspb.EncryptResponse, error)
	Decrypt(context.Context, *kmspb.DecryptRequest) (*kmspb.DecryptResponse, error)
}

type gcpSDKClient struct {
	client *gcpkms.KeyManagementClient
}

func (c gcpSDKClient) Encrypt(ctx context.Context, req *kmspb.EncryptRequest) (*kmspb.EncryptResponse, error) {
	out, err := c.client.Encrypt(ctx, req)
	return out, errors.Wrap(err, "GCP KMS SDK Encrypt")
}

func (c gcpSDKClient) Decrypt(ctx context.Context, req *kmspb.DecryptRequest) (*kmspb.DecryptResponse, error) {
	out, err := c.client.Decrypt(ctx, req)
	return out, errors.Wrap(err, "GCP KMS SDK Decrypt")
}

// GCPKMSWrapper wraps DEKs using a Google Cloud KMS symmetric CryptoKey.
type GCPKMSWrapper struct {
	client  gcpKMSClient
	keyName string
	timeout time.Duration
}

// NewGCPKMSWrapper uses Application Default Credentials to construct a Cloud
// KMS client for keyName.
func NewGCPKMSWrapper(ctx context.Context, keyName string) (*GCPKMSWrapper, error) {
	if !validGCPKeyName(keyName) {
		return nil, errors.Wrapf(ErrInvalidKEKURI, "invalid GCP KMS CryptoKey name %q", keyName)
	}
	client, err := gcpkms.NewKeyManagementClient(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "kek: create GCP KMS client")
	}
	return newGCPKMSWrapper(gcpSDKClient{client: client}, keyName), nil
}

func newGCPKMSWrapper(client gcpKMSClient, keyName string) *GCPKMSWrapper {
	return &GCPKMSWrapper{client: client, keyName: keyName, timeout: providerRequestTimeout}
}

func validGCPKeyName(name string) bool {
	parts := strings.Split(name, "/")
	return len(parts) == 8 && parts[0] == "projects" && parts[1] != "" &&
		parts[2] == "locations" && parts[3] != "" && parts[4] == "keyRings" &&
		parts[5] != "" && parts[6] == "cryptoKeys" && parts[7] != ""
}

func crc32c(data []byte) *wrapperspb.Int64Value {
	return wrapperspb.Int64(int64(crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))))
}

func checksumMatches(data []byte, checksum *wrapperspb.Int64Value) bool {
	return checksum != nil && crc32c(data).Value == checksum.Value
}

// Wrap calls Cloud KMS Encrypt with fixed AAD and verifies request/response
// CRC32C integrity metadata.
func (w *GCPKMSWrapper) Wrap(dek []byte) ([]byte, error) {
	if w == nil || w.client == nil {
		return nil, errors.Wrap(ErrInvalidProviderResponse, "kek: GCP KMS client is nil")
	}
	if err := validateDEK(dek); err != nil {
		return nil, err
	}
	ctx, cancel := requestContext(w.timeout)
	defer cancel()
	out, err := w.client.Encrypt(ctx, &kmspb.EncryptRequest{
		Name:                              w.keyName,
		Plaintext:                         dek,
		AdditionalAuthenticatedData:       gcpKMSAAD,
		PlaintextCrc32C:                   crc32c(dek),
		AdditionalAuthenticatedDataCrc32C: crc32c(gcpKMSAAD),
	})
	if err != nil {
		return nil, errors.Wrap(err, "kek: GCP KMS Encrypt")
	}
	if out == nil || len(out.Ciphertext) == 0 || !out.VerifiedPlaintextCrc32C ||
		!out.VerifiedAdditionalAuthenticatedDataCrc32C || !checksumMatches(out.Ciphertext, out.CiphertextCrc32C) {
		return nil, errors.WithStack(ErrInvalidProviderResponse)
	}
	return append([]byte(nil), out.Ciphertext...), nil
}

// Unwrap calls Cloud KMS Decrypt and verifies the plaintext CRC32C before
// accepting the 32-byte DEK.
func (w *GCPKMSWrapper) Unwrap(wrapped []byte) ([]byte, error) {
	if w == nil || w.client == nil {
		return nil, errors.Wrap(ErrInvalidProviderResponse, "kek: GCP KMS client is nil")
	}
	if len(wrapped) == 0 {
		return nil, errors.Wrap(ErrInvalidProviderResponse, "kek: GCP KMS ciphertext is empty")
	}
	ctx, cancel := requestContext(w.timeout)
	defer cancel()
	out, err := w.client.Decrypt(ctx, &kmspb.DecryptRequest{
		Name:                              w.keyName,
		Ciphertext:                        wrapped,
		AdditionalAuthenticatedData:       gcpKMSAAD,
		CiphertextCrc32C:                  crc32c(wrapped),
		AdditionalAuthenticatedDataCrc32C: crc32c(gcpKMSAAD),
	})
	if err != nil {
		return nil, errors.Wrap(err, "kek: GCP KMS Decrypt")
	}
	if out == nil || !checksumMatches(out.Plaintext, out.PlaintextCrc32C) {
		return nil, errors.WithStack(ErrInvalidProviderResponse)
	}
	if err := validateDEK(out.Plaintext); err != nil {
		return nil, errors.Wrap(err, "kek: GCP KMS plaintext")
	}
	return append([]byte(nil), out.Plaintext...), nil
}

// Name returns the provider and configured CryptoKey resource name.
func (w *GCPKMSWrapper) Name() string { return "gcp-kms:" + w.keyName }
