package adapter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
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

	t.Run("sqs query", func(t *testing.T) {
		t.Parallel()
		rec := httptest.NewRecorder()
		writeSQSQueryError(rec, err)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status=%d, want 503", rec.Code)
		}
		if body := rec.Body.String(); !strings.Contains(body, "<Code>"+sqsErrServiceUnavailable+"</Code>") {
			t.Fatalf("body %q does not contain %q", body, sqsErrServiceUnavailable)
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

func TestHTTPAdaptersMapLeaderProxyCircuitOpenTo503(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		call func(http.ResponseWriter)
		body string
	}{
		{
			name: "dynamodb",
			call: func(w http.ResponseWriter) { writeDynamoErrorFromErr(w, kv.ErrLeaderProxyCircuitOpen) },
			body: dynamoErrServiceUnavailable,
		},
		{
			name: "sqs",
			call: func(w http.ResponseWriter) { writeSQSErrorFromErr(w, kv.ErrLeaderProxyCircuitOpen) },
			body: sqsErrServiceUnavailable,
		},
		{
			name: "sqs query",
			call: func(w http.ResponseWriter) { writeSQSQueryError(w, kv.ErrLeaderProxyCircuitOpen) },
			body: "<Code>" + sqsErrServiceUnavailable + "</Code>",
		},
		{
			name: "s3",
			call: func(w http.ResponseWriter) {
				writeS3MutationError(w, kv.ErrLeaderProxyCircuitOpen, "bucket-a", "key-a")
			},
			body: "<Code>ServiceUnavailable</Code>",
		},
		{
			name: "s3 chunk upload",
			call: func(w http.ResponseWriter) {
				(&S3Server{}).writeS3ChunkUploadError(w, nil, kv.ErrLeaderProxyCircuitOpen, "bucket-a", "key-a")
			},
			body: "<Code>ServiceUnavailable</Code>",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := httptest.NewRecorder()
			tt.call(rec)
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("status=%d, want 503", rec.Code)
			}
			if body := rec.Body.String(); !strings.Contains(body, tt.body) {
				t.Fatalf("body %q does not contain %q", body, tt.body)
			}
		})
	}
}

type availabilityErrorCoordinator struct {
	stubAdapterCoordinator
	err error
}

func (c *availabilityErrorCoordinator) Dispatch(context.Context, *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	return nil, c.err
}

func TestS3CreateMultipartUploadMapsAvailabilityErrorsTo503(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	local := newLocalAdapterCoordinator(st)
	server := NewS3Server(nil, "", st, local, nil)

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-a", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("create bucket status=%d, want 200: %s", rec.Code, rec.Body.String())
	}

	tests := []struct {
		name string
		err  error
	}{
		{name: "circuit open", err: kv.ErrLeaderProxyCircuitOpen},
		{name: "grpc unavailable", err: crdberrors.Wrap(status.Error(codes.Unavailable, "startup gate closed"), "dispatch")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server.coordinator = &availabilityErrorCoordinator{
				stubAdapterCoordinator: stubAdapterCoordinator{clock: local.Clock()},
				err:                    tt.err,
			}
			rec := httptest.NewRecorder()
			server.createMultipartUpload(
				rec,
				newS3TestRequest(http.MethodPost, "/bucket-a/key?uploads=", nil),
				"bucket-a",
				"key",
			)
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("status=%d, want 503: %s", rec.Code, rec.Body.String())
			}
			if body := rec.Body.String(); !strings.Contains(body, "<Code>ServiceUnavailable</Code>") {
				t.Fatalf("body %q does not contain ServiceUnavailable", body)
			}
		})
	}
}
