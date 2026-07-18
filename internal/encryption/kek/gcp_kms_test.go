package kek

import (
	"bytes"
	"context"
	"testing"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/stretchr/testify/require"
)

type fakeGCPKMSClient struct {
	encryptInput  *kmspb.EncryptRequest
	decryptInput  *kmspb.DecryptRequest
	encryptOutput *kmspb.EncryptResponse
	decryptOutput *kmspb.DecryptResponse
	err           error
}

func (f *fakeGCPKMSClient) Encrypt(_ context.Context, input *kmspb.EncryptRequest) (*kmspb.EncryptResponse, error) {
	f.encryptInput = input
	return f.encryptOutput, f.err
}

func (f *fakeGCPKMSClient) Decrypt(_ context.Context, input *kmspb.DecryptRequest) (*kmspb.DecryptResponse, error) {
	f.decryptInput = input
	return f.decryptOutput, f.err
}

func TestGCPKMSWrapperRequestAndResponseIntegrity(t *testing.T) {
	const keyName = "projects/p/locations/global/keyRings/r/cryptoKeys/k"
	dek := bytes.Repeat([]byte{0x52}, fileKEKSize)
	ciphertext := []byte("gcp-wrapped")
	client := &fakeGCPKMSClient{
		encryptOutput: &kmspb.EncryptResponse{
			Ciphertext:              ciphertext,
			CiphertextCrc32C:        crc32c(ciphertext),
			VerifiedPlaintextCrc32C: true,
			VerifiedAdditionalAuthenticatedDataCrc32C: true,
		},
		decryptOutput: &kmspb.DecryptResponse{Plaintext: dek, PlaintextCrc32C: crc32c(dek)},
	}
	wrapper := newGCPKMSWrapper(client, keyName)

	wrapped, err := wrapper.Wrap(dek)
	require.NoError(t, err)
	require.Equal(t, ciphertext, wrapped)
	require.Equal(t, keyName, client.encryptInput.Name)
	require.Equal(t, gcpKMSAAD, client.encryptInput.AdditionalAuthenticatedData)
	require.True(t, checksumMatches(dek, client.encryptInput.PlaintextCrc32C))

	plain, err := wrapper.Unwrap(wrapped)
	require.NoError(t, err)
	require.Equal(t, dek, plain)
	require.Equal(t, gcpKMSAAD, client.decryptInput.AdditionalAuthenticatedData)
	require.True(t, checksumMatches(wrapped, client.decryptInput.CiphertextCrc32C))
	require.Equal(t, "gcp-kms:"+keyName, wrapper.Name())
}

func TestGCPKMSWrapperRejectsIntegrityFailures(t *testing.T) {
	const keyName = "projects/p/locations/global/keyRings/r/cryptoKeys/k"
	dek := bytes.Repeat([]byte{0x52}, fileKEKSize)
	client := &fakeGCPKMSClient{encryptOutput: &kmspb.EncryptResponse{
		Ciphertext:              []byte("ciphertext"),
		CiphertextCrc32C:        crc32c([]byte("different")),
		VerifiedPlaintextCrc32C: true,
		VerifiedAdditionalAuthenticatedDataCrc32C: true,
	}}
	_, err := newGCPKMSWrapper(client, keyName).Wrap(dek)
	require.ErrorIs(t, err, ErrInvalidProviderResponse)

	client.decryptOutput = &kmspb.DecryptResponse{Plaintext: dek, PlaintextCrc32C: crc32c([]byte("different"))}
	_, err = newGCPKMSWrapper(client, keyName).Unwrap([]byte("ciphertext"))
	require.ErrorIs(t, err, ErrInvalidProviderResponse)
}

func TestValidGCPKeyName(t *testing.T) {
	require.True(t, validGCPKeyName("projects/p/locations/global/keyRings/r/cryptoKeys/k"))
	require.False(t, validGCPKeyName("projects/p/keyRings/r/cryptoKeys/k"))
}
