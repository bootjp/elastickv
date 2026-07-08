package adapter

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	crdberrors "github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHTTPAdaptersMapStartupGateUnavailableTo503(t *testing.T) {
	t.Parallel()
	err := crdberrors.Wrap(status.Error(codes.Unavailable, "startup rotation has not completed"), "dispatch")

	t.Run("dynamodb", func(t *testing.T) {
		t.Parallel()
		rec := httptest.NewRecorder()
		writeDynamoErrorFromErr(rec, err)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status=%d, want 503", rec.Code)
		}
		if body := rec.Body.String(); !strings.Contains(body, dynamoErrServiceUnavailable) {
			t.Fatalf("body %q does not contain %q", body, dynamoErrServiceUnavailable)
		}
	})

	t.Run("sqs", func(t *testing.T) {
		t.Parallel()
		rec := httptest.NewRecorder()
		writeSQSErrorFromErr(rec, err)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status=%d, want 503", rec.Code)
		}
		if body := rec.Body.String(); !strings.Contains(body, sqsErrServiceUnavailable) {
			t.Fatalf("body %q does not contain %q", body, sqsErrServiceUnavailable)
		}
	})

	t.Run("s3", func(t *testing.T) {
		t.Parallel()
		rec := httptest.NewRecorder()
		writeS3MutationError(rec, err, "bucket-a", "key-a")

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status=%d, want 503", rec.Code)
		}
		if body := rec.Body.String(); !strings.Contains(body, "<Code>ServiceUnavailable</Code>") {
			t.Fatalf("body %q does not contain ServiceUnavailable", body)
		}
	})
}
