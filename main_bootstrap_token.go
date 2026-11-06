package main

import (
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/cockroachdb/errors"
)

func validateBootstrapStartupToken(token string) error {
	if token == "" {
		return errors.New("protected runtime requires a dedicated startup token")
	}
	for _, path := range []string{*adminTokenFile, *splitMigrationTokenFile, *s3BlobPeerTokenFile} {
		if path == "" {
			continue
		}
		other, err := internalutil.LoadBearerTokenFile(path, adminTokenMaxBytes, "existing service token")
		if err != nil {
			return errors.Wrap(err, "verify startup token separation")
		}
		if other == token {
			return errors.New("startup token must differ from Admin, migration and S3 peer tokens")
		}
	}
	return nil
}
