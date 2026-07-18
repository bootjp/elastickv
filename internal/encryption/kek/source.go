package kek

import (
	"context"
	"os"
	"strings"

	"github.com/cockroachdb/errors"
)

const (
	awsKMSScheme       = "aws-kms://"
	gcpKMSScheme       = "gcp-kms://"
	vaultTransitScheme = "vault-transit://"
)

var (
	// ErrMultipleKEKSources rejects ambiguous key configuration rather than
	// silently selecting one source by precedence.
	ErrMultipleKEKSources = errors.New("kek: configure exactly one of --kekFile, --kekUri, or ELASTICKV_KEK_BASE64")
	// ErrInvalidKEKURI rejects unknown providers and malformed provider targets.
	ErrInvalidKEKURI = errors.New("kek: invalid KEK URI")
)

// NewWrapperFromSources resolves exactly one file, URI, or environment-backed
// KEK. No configured source returns nil so encryption-disabled deployments keep
// their existing startup behavior.
func NewWrapperFromSources(ctx context.Context, filePath, uri string) (Wrapper, error) {
	_, envSet := os.LookupEnv(EnvVar)
	if countConfiguredSources(filePath != "", uri != "", envSet) > 1 {
		return nil, rejectSourceConflict(envSet)
	}
	switch {
	case filePath != "":
		wrapper, err := NewFileWrapper(filePath)
		if err != nil {
			return nil, errors.Wrapf(err, "kek: load file %q", filePath)
		}
		return wrapper, nil
	case uri != "":
		return newURIWrapper(ctx, uri)
	case envSet:
		return NewEnvWrapper()
	default:
		return nil, nil
	}
}

func countConfiguredSources(sources ...bool) int {
	configured := 0
	for _, present := range sources {
		if present {
			configured++
		}
	}
	return configured
}

func rejectSourceConflict(envSet bool) error {
	if envSet {
		if err := os.Unsetenv(EnvVar); err != nil {
			return errors.Wrapf(err, "kek: unset %s after source conflict", EnvVar)
		}
	}
	return errors.WithStack(ErrMultipleKEKSources)
}

func newURIWrapper(ctx context.Context, uri string) (Wrapper, error) {
	switch {
	case strings.HasPrefix(uri, awsKMSScheme):
		target := strings.TrimPrefix(uri, awsKMSScheme)
		if target == "" {
			return nil, errors.WithStack(ErrInvalidKEKURI)
		}
		return NewAWSKMSWrapper(ctx, target)
	case strings.HasPrefix(uri, gcpKMSScheme):
		target := strings.TrimPrefix(uri, gcpKMSScheme)
		if target == "" {
			return nil, errors.WithStack(ErrInvalidKEKURI)
		}
		return NewGCPKMSWrapper(ctx, target)
	case strings.HasPrefix(uri, vaultTransitScheme):
		target := strings.TrimPrefix(uri, vaultTransitScheme)
		if target == "" {
			return nil, errors.WithStack(ErrInvalidKEKURI)
		}
		return NewVaultTransitWrapper(target)
	default:
		return nil, errors.Wrapf(ErrInvalidKEKURI, "unsupported URI %q", uri)
	}
}
