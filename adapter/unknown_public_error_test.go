package adapter

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestS3UnknownOutcomeResponse(t *testing.T) {
	t.Parallel()
	err := fmt.Errorf("%w: %w: private-key", kv.ErrTransactionOutcomeUnknown, store.ErrWriteConflict)
	for _, write := range []func(http.ResponseWriter, error){
		func(w http.ResponseWriter, err error) { writeS3ResponseOrInternalError(w, err) },
		func(w http.ResponseWriter, err error) { writeS3MutationError(w, err, "bucket", "object") },
		func(w http.ResponseWriter, err error) { writeS3InternalError(w, err) },
	} {
		rec := httptest.NewRecorder()
		write(rec, err)
		require.Equal(t, http.StatusConflict, rec.Code)
		require.Equal(t, "application/xml", rec.Header().Get("Content-Type"))
		var response struct {
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
			Bucket  string `xml:"BucketName"`
			Key     string `xml:"Key"`
		}
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &response))
		require.Equal(t, "OperationOutcomeUnknown", response.Code)
		require.Equal(t, "Operation outcome unknown", response.Message)
		require.Empty(t, response.Bucket)
		require.Empty(t, response.Key)
		require.NotContains(t, rec.Body.String(), "private-key")
	}
}

func TestSQSUnknownOutcomeResponse(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	writeSQSErrorFromErr(rec, fmt.Errorf("%w: private-key", kv.ErrTransactionOutcomeUnknown))
	require.Equal(t, http.StatusConflict, rec.Code)
	require.Equal(t, "application/x-amz-json-1.0", rec.Header().Get("Content-Type"))
	require.Equal(t, "OperationOutcomeUnknown", rec.Header().Get("x-amzn-ErrorType"))
	require.JSONEq(t, `{"__type":"OperationOutcomeUnknown","message":"Operation outcome unknown"}`, rec.Body.String())
	require.False(t, strings.Contains(rec.Body.String(), "private-key"))
}

func TestS3UnknownOutcomeGoSDKDoesNotReplay(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeS3MutationError(w, kv.ErrTransactionOutcomeUnknown, "bucket", "object")
	}))
	t.Cleanup(server.Close)

	cfg, err := config.LoadDefaultConfig(t.Context(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	require.NoError(t, err)
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(server.URL)
		o.UsePathStyle = true
	})
	_, err = client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("object"),
		Body:   strings.NewReader("payload"),
	})
	require.Error(t, err)
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "OperationOutcomeUnknown", apiErr.ErrorCode())
	require.Equal(t, int32(1), calls.Load())
}
