package server

import (
	"flag"
	"testing"

	"github.com/bootjp/elastickv/adapter"
	"github.com/stretchr/testify/require"
)

func TestListenerFlagsDefaultsAndParsing(t *testing.T) {
	t.Parallel()
	require.Equal(t, listenerFlags{
		redisAddress:          "localhost:6379",
		redisLuaMaxIdleStates: adapter.DefaultLuaPoolMaxIdle,
		dynamoAddress:         "localhost:8000",
		s3Region:              "us-east-1",
		s3PathStyleOnly:       true,
		sqsRegion:             "us-east-1",
		metricsAddress:        "localhost:9090",
		pprofAddress:          "localhost:6060",
	}, defaultFlags[listenerFlags]())

	var f listenerFlags
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	f.register(fs)
	require.NoError(t, fs.Parse([]string{
		"--s3Address", "127.0.0.1:9000", "--s3CredentialsFile", "s3.json", "--s3PathStyleOnly=false",
		"--sqsAddress", "127.0.0.1:9324", "--sqsCredentialsFile", "sqs.json", "--pprofAddress", "",
	}))
	require.Equal(t, "127.0.0.1:9000", f.s3Address)
	require.Equal(t, "s3.json", f.s3CredentialsFile)
	require.False(t, f.s3PathStyleOnly)
	require.Equal(t, "127.0.0.1:9324", f.sqsAddress)
	require.Equal(t, "sqs.json", f.sqsCredentialsFile)
	require.Empty(t, f.pprofAddress)
}
