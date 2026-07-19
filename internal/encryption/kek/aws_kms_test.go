package kek

import (
	"bytes"
	"context"
	"testing"

	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type fakeAWSKMSClient struct {
	encryptInput  *awskms.EncryptInput
	decryptInput  *awskms.DecryptInput
	encryptOutput *awskms.EncryptOutput
	decryptOutput *awskms.DecryptOutput
	err           error
}

func (f *fakeAWSKMSClient) Encrypt(_ context.Context, input *awskms.EncryptInput, _ ...func(*awskms.Options)) (*awskms.EncryptOutput, error) {
	f.encryptInput = input
	return f.encryptOutput, f.err
}

func (f *fakeAWSKMSClient) Decrypt(_ context.Context, input *awskms.DecryptInput, _ ...func(*awskms.Options)) (*awskms.DecryptOutput, error) {
	f.decryptInput = input
	return f.decryptOutput, f.err
}

func TestAWSKMSWrapperRequestBinding(t *testing.T) {
	const keyARN = "arn:aws:kms:us-east-1:123456789012:key/1234abcd"
	dek := bytes.Repeat([]byte{0x42}, fileKEKSize)
	client := &fakeAWSKMSClient{
		encryptOutput: &awskms.EncryptOutput{CiphertextBlob: []byte("wrapped")},
		decryptOutput: &awskms.DecryptOutput{Plaintext: append([]byte(nil), dek...)},
	}
	wrapper := newAWSKMSWrapper(client, keyARN)

	wrapped, err := wrapper.Wrap(dek)
	require.NoError(t, err)
	require.Equal(t, []byte("wrapped"), wrapped)
	require.Equal(t, keyARN, *client.encryptInput.KeyId)
	require.Equal(t, dek, client.encryptInput.Plaintext)
	require.Equal(t, awsEncryptionContextValue, client.encryptInput.EncryptionContext[awsEncryptionContextKey])

	plain, err := wrapper.Unwrap(wrapped)
	require.NoError(t, err)
	require.Equal(t, dek, plain)
	require.Equal(t, keyARN, *client.decryptInput.KeyId)
	require.Equal(t, awsEncryptionContextValue, client.decryptInput.EncryptionContext[awsEncryptionContextKey])
	require.Equal(t, "aws-kms:"+keyARN, wrapper.Name())
}

func TestAWSKMSWrapperRejectsProviderFailures(t *testing.T) {
	const keyARN = "arn:aws:kms:us-east-1:123456789012:key/1234abcd"
	dek := bytes.Repeat([]byte{0x42}, fileKEKSize)
	for _, tc := range []struct {
		name   string
		client *fakeAWSKMSClient
		unwrap bool
		want   error
	}{
		{name: "encrypt error", client: &fakeAWSKMSClient{err: errors.New("denied")}, want: errors.New("sentinel")},
		{name: "empty ciphertext", client: &fakeAWSKMSClient{encryptOutput: &awskms.EncryptOutput{}}, want: ErrInvalidProviderResponse},
		{name: "short plaintext", client: &fakeAWSKMSClient{decryptOutput: &awskms.DecryptOutput{Plaintext: []byte("short")}}, unwrap: true, want: ErrInvalidDEKLength},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wrapper := newAWSKMSWrapper(tc.client, keyARN)
			var err error
			if tc.unwrap {
				_, err = wrapper.Unwrap([]byte("wrapped"))
			} else {
				_, err = wrapper.Wrap(dek)
			}
			require.Error(t, err)
			if errors.Is(tc.want, ErrInvalidProviderResponse) || errors.Is(tc.want, ErrInvalidDEKLength) {
				require.ErrorIs(t, err, tc.want)
			}
		})
	}
}

func TestNewAWSKMSWrapperRejectsInvalidARNBeforeConfigLoad(t *testing.T) {
	for _, keyARN := range []string{
		"not-an-arn",
		"arn:aws:kms:us-east-1:123456789012:key/",
		"arn:aws:kms:us-east-1:123456789012:alias/current",
		"arn:aws:kms:us-east-1:123456789012:alias/",
	} {
		t.Run(keyARN, func(t *testing.T) {
			_, err := NewAWSKMSWrapper(context.Background(), keyARN)
			require.ErrorIs(t, err, ErrInvalidKEKURI)
		})
	}
}
