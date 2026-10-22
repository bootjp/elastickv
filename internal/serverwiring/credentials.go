// Package serverwiring is the process wiring the server binaries share: the
// --keyviz* and --autoSplit* flag groups and what they build, the metrics and
// pprof listeners, and the SigV4 static-credentials loader. Root main and the
// cmd/server demo both register and call it, so the demo cannot drift from
// the server binary.
package serverwiring

import (
	"encoding/json"
	"os"
	"strings"

	"github.com/cockroachdb/errors"
)

// sigV4CredentialsFile is the JSON schema every SigV4 credentials flag reads
// (--s3CredentialsFile, --sqsCredentialsFile), so operators keep one file per
// deployment whichever adapters are enabled.
type sigV4CredentialsFile struct {
	Credentials []sigV4CredentialEntry `json:"credentials"`
}

type sigV4CredentialEntry struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
}

// LoadSigV4StaticCredentials parses a credentials file into an access key →
// secret map for WithS3StaticCredentials or WithSQSStaticCredentials. An
// empty path returns a nil map so authorization stays disabled. An empty
// field or an access key listed twice is an error: a second entry would
// silently replace the first. label names the adapter in errors
// ("s3 credentials file ...").
func LoadSigV4StaticCredentials(path string, label string) (map[string]string, error) {
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
			return nil, errors.Errorf("%s credentials file contains an empty access key or secret key", label)
		}
		if _, exists := out[accessKeyID]; exists {
			return nil, errors.Errorf("%s credentials file contains duplicate access key ID: %q", label, accessKeyID)
		}
		out[accessKeyID] = secretAccessKey
	}
	return out, nil
}
