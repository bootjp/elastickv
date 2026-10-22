package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/keyviz"
	"github.com/stretchr/testify/require"
)

// The server binary rejects a credentials file that names one access key
// twice (main_s3_test.go): the second secret would silently replace the
// first. The demo must load credentials the same way.
func TestDemoS3CredentialsRejectDuplicateAccessKey(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "s3-creds.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"credentials":[`+
		`{"access_key_id":"akid","secret_access_key":"one"},`+
		`{"access_key_id":"akid","secret_access_key":"two"}]}`), 0o600))

	_, err := loadS3StaticCredentials(path)
	require.ErrorContains(t, err, "duplicate access key ID")
}

// --keyvizLabelsEnabled gives each adapter its own KeyViz rows on the server
// binary. The demo must expose the flag and pass it to its sampler.
func TestDemoKeyVizSamplerHonoursLabelsFlag(t *testing.T) {
	// Not parallel: sets process-wide demo flags.
	setDemoFlagForTest(t, "keyvizEnabled", "true")
	setDemoFlagForTest(t, "keyvizLabelsEnabled", "true")

	s := buildDemoKeyVizSampler()
	require.NotNil(t, s)
	s.RegisterRoute(1, []byte("a"), []byte("z"), 1)
	s.Observe(1, []byte("k"), keyviz.OpWrite, 1, keyviz.LabelRedis)
	s.Flush()

	labels := map[keyviz.Label]bool{}
	for _, column := range s.Snapshot(time.Time{}, time.Now().Add(time.Hour)) {
		for _, row := range column.Rows {
			labels[row.Label] = true
		}
	}
	require.True(t, labels[keyviz.LabelRedis], "labelled sampler must keep a redis row; got %v", labels)
}

func setDemoFlagForTest(t *testing.T, name, value string) {
	t.Helper()
	f := flag.CommandLine.Lookup(name)
	require.NotNil(t, f, "the demo does not register --%s", name)
	previous := f.Value.String()
	require.NoError(t, flag.CommandLine.Set(name, value))
	t.Cleanup(func() { require.NoError(t, flag.CommandLine.Set(name, previous)) })
}
