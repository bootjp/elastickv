package serverwiring

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadSigV4StaticCredentials(t *testing.T) {
	t.Parallel()

	creds, err := LoadSigV4StaticCredentials(" ", "s3")
	require.NoError(t, err)
	require.Nil(t, creds, "an empty path leaves authorization disabled")

	tests := []struct {
		name    string
		body    string
		want    map[string]string
		wantErr string
	}{
		{
			name: "valid",
			body: `{"credentials":[{"access_key_id":" akid ","secret_access_key":"secret"},{"access_key_id":"other","secret_access_key":"s2"}]}`,
			want: map[string]string{"akid": "secret", "other": "s2"},
		},
		{
			name:    "empty secret",
			body:    `{"credentials":[{"access_key_id":"akid","secret_access_key":""}]}`,
			wantErr: "sqs credentials file contains an empty access key or secret key",
		},
		{
			name:    "duplicate access key",
			body:    `{"credentials":[{"access_key_id":"akid","secret_access_key":"one"},{"access_key_id":"akid","secret_access_key":"two"}]}`,
			wantErr: `sqs credentials file contains duplicate access key ID: "akid"`,
		},
		{
			name:    "malformed json",
			body:    `{"credentials":`,
			wantErr: "unexpected EOF",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "creds.json")
			require.NoError(t, os.WriteFile(path, []byte(tc.body), 0o600))
			got, err := LoadSigV4StaticCredentials(path, "sqs")
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
