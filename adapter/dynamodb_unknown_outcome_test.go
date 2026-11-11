package adapter

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

func TestDynamoUnknownOutcomeResponse(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	writeDynamoErrorFromErr(rec, errors.New("unrelated"))
	require.Equal(t, http.StatusInternalServerError, rec.Code)

	rec = httptest.NewRecorder()
	writeDynamoErrorFromErr(rec, fmt.Errorf("%w: private-key", kv.ErrTransactionOutcomeUnknown))
	require.Equal(t, http.StatusConflict, rec.Code)
	require.Equal(t, dynamoErrOperationOutcomeUnknown, rec.Header().Get("x-amzn-ErrorType"))
	require.Equal(t, "application/x-amz-json-1.0", rec.Header().Get("Content-Type"))
	require.JSONEq(t, `{"__type":"OperationOutcomeUnknown","message":"Operation outcome unknown"}`, rec.Body.String())
	require.False(t, strings.Contains(rec.Body.String(), "private-key"))
}

func TestDynamoUnknownOutcomeGoSDKDoesNotReplay(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeDynamoErrorFromErr(w, kv.ErrTransactionOutcomeUnknown)
	}))
	t.Cleanup(server.Close)

	cfg, err := config.LoadDefaultConfig(t.Context(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	require.NoError(t, err)
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(server.URL)
	})
	_, err = client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("t"),
		Item: map[string]ddbTypes.AttributeValue{
			"pk": &ddbTypes.AttributeValueMemberS{Value: "k"},
		},
	})
	require.Error(t, err)
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, dynamoErrOperationOutcomeUnknown, apiErr.ErrorCode())
	require.Equal(t, int32(1), calls.Load())
}
