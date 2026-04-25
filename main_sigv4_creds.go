package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/errors"
)

// sigV4CredentialsFile is the JSON schema both --s3CredentialsFile and
// --sqsCredentialsFile read. Sharing the schema means operators maintain one
// file per deployment regardless of which adapters are enabled.
type sigV4CredentialsFile struct {
	Credentials []sigV4CredentialEntry `json:"credentials"`
}

type sigV4CredentialEntry struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
}

// loadSigV4StaticCredentialsFile parses a credentials file and returns a
// map of access-key → secret suitable for WithS3StaticCredentials or
// WithSQSStaticCredentials. An empty path returns a nil map so the caller
// can leave authorization disabled.
//
// labelForErrors appears in error text ("s3 credentials file ...") so
// operators get context on which flag produced the problem.
func loadSigV4StaticCredentialsFile(path string, labelForErrors string) (map[string]string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer f.Close()
	file := sigV4CredentialsFile{}
	if err := json.NewDecoder(f).Decode(&file); err != nil {
		return nil, errors.WithStack(err)
	}
	out := make(map[string]string, len(file.Credentials))
	for _, cred := range file.Credentials {
		accessKeyID := strings.TrimSpace(cred.AccessKeyID)
		secretAccessKey := strings.TrimSpace(cred.SecretAccessKey)
		if accessKeyID == "" || secretAccessKey == "" {
			return nil, errors.WithStack(fmt.Errorf("%s credentials file contains an empty access key or secret key", labelForErrors))
		}
		if _, exists := out[accessKeyID]; exists {
			return nil, errors.WithStack(fmt.Errorf("%s credentials file contains duplicate access key ID: %q", labelForErrors, accessKeyID))
		}
		out[accessKeyID] = secretAccessKey
	}
	return out, nil
}
