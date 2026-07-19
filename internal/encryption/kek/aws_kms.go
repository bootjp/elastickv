package kek

import (
	"context"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/cockroachdb/errors"
)

const awsEncryptionContextKey = "elastickv-purpose"
const awsEncryptionContextValue = "dek-wrap-v1"

type awsKMSClient interface {
	Encrypt(context.Context, *awskms.EncryptInput, ...func(*awskms.Options)) (*awskms.EncryptOutput, error)
	Decrypt(context.Context, *awskms.DecryptInput, ...func(*awskms.Options)) (*awskms.DecryptOutput, error)
}

// AWSKMSWrapper wraps DEKs using an AWS KMS symmetric ENCRYPT_DECRYPT key.
type AWSKMSWrapper struct {
	client  awsKMSClient
	keyARN  string
	timeout time.Duration
}

// NewAWSKMSWrapper loads the standard AWS credential chain and derives the KMS
// endpoint region from keyARN.
func NewAWSKMSWrapper(ctx context.Context, keyARN string) (*AWSKMSWrapper, error) {
	parsed, err := arn.Parse(keyARN)
	if err != nil || parsed.Service != "kms" || parsed.Region == "" || parsed.AccountID == "" ||
		!strings.HasPrefix(parsed.Resource, "key/") || strings.TrimPrefix(parsed.Resource, "key/") == "" {
		return nil, errors.Wrapf(ErrInvalidKEKURI, "invalid AWS KMS key ARN %q", keyARN)
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(parsed.Region))
	if err != nil {
		return nil, errors.Wrap(err, "kek: load AWS configuration")
	}
	return newAWSKMSWrapper(awskms.NewFromConfig(cfg), keyARN), nil
}

func newAWSKMSWrapper(client awsKMSClient, keyARN string) *AWSKMSWrapper {
	return &AWSKMSWrapper{client: client, keyARN: keyARN, timeout: providerRequestTimeout}
}

// Wrap calls AWS KMS Encrypt with a fixed encryption context that is required
// again by Unwrap.
func (w *AWSKMSWrapper) Wrap(dek []byte) ([]byte, error) {
	if w == nil || w.client == nil {
		return nil, errors.Wrap(ErrInvalidProviderResponse, "kek: AWS KMS client is nil")
	}
	if err := validateDEK(dek); err != nil {
		return nil, err
	}
	ctx, cancel := requestContext(w.timeout)
	defer cancel()
	out, err := w.client.Encrypt(ctx, &awskms.EncryptInput{
		KeyId:     aws.String(w.keyARN),
		Plaintext: dek,
		EncryptionContext: map[string]string{
			awsEncryptionContextKey: awsEncryptionContextValue,
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "kek: AWS KMS Encrypt")
	}
	if out == nil || len(out.CiphertextBlob) == 0 {
		return nil, errors.Wrap(ErrInvalidProviderResponse, "kek: AWS KMS returned empty ciphertext")
	}
	return append([]byte(nil), out.CiphertextBlob...), nil
}

// Unwrap calls AWS KMS Decrypt and rejects any non-32-byte plaintext response.
func (w *AWSKMSWrapper) Unwrap(wrapped []byte) ([]byte, error) {
	if w == nil || w.client == nil {
		return nil, errors.Wrap(ErrInvalidProviderResponse, "kek: AWS KMS client is nil")
	}
	if len(wrapped) == 0 {
		return nil, errors.Wrap(ErrInvalidProviderResponse, "kek: AWS KMS ciphertext is empty")
	}
	ctx, cancel := requestContext(w.timeout)
	defer cancel()
	out, err := w.client.Decrypt(ctx, &awskms.DecryptInput{
		CiphertextBlob: wrapped,
		KeyId:          aws.String(w.keyARN),
		EncryptionContext: map[string]string{
			awsEncryptionContextKey: awsEncryptionContextValue,
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "kek: AWS KMS Decrypt")
	}
	if out == nil {
		return nil, errors.Wrap(ErrInvalidProviderResponse, "kek: AWS KMS returned nil plaintext")
	}
	if err := validateDEK(out.Plaintext); err != nil {
		return nil, errors.Wrap(err, "kek: AWS KMS plaintext")
	}
	return append([]byte(nil), out.Plaintext...), nil
}

// Name returns the provider and configured key ARN.
func (w *AWSKMSWrapper) Name() string { return "aws-kms:" + w.keyARN }
