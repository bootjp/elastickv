package adapter

import (
	"net/http"

	"github.com/cockroachdb/errors"
	json "github.com/goccy/go-json"
)

type dynamoAPIError struct {
	status    int
	errorType string
	message   string
}

func (e *dynamoAPIError) Error() string {
	if e == nil {
		return ""
	}
	if e.message != "" {
		return e.message
	}
	return http.StatusText(e.status)
}

func newDynamoAPIError(status int, errorType string, message string) error {
	return &dynamoAPIError{
		status:    status,
		errorType: errorType,
		message:   message,
	}
}

func writeDynamoErrorFromErr(w http.ResponseWriter, err error) {
	var apiErr *dynamoAPIError
	if errors.As(err, &apiErr) {
		writeDynamoError(w, apiErr.status, apiErr.errorType, apiErr.message)
		return
	}
	writeDynamoError(w, http.StatusInternalServerError, dynamoErrInternal, err.Error())
}

// dynamoErrIsTransient reports whether err is a transient/internal failure
// (Pebble error, context deadline, decode failure) as opposed to a structured
// validation/malformed-input error. A *dynamoAPIError is always a deliberate
// validation result (it carries an HTTP status + error type), so it is NOT
// transient; everything else — a raw wrapped store/context error — is. The
// lease pre-pass uses this to decide whether an unresolvable item must fail
// closed (transient) or may be skipped (validation, rejected identically by
// the read path).
func dynamoErrIsTransient(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *dynamoAPIError
	return !errors.As(err, &apiErr)
}

func writeDynamoError(w http.ResponseWriter, status int, errorType string, message string) {
	if message == "" {
		message = http.StatusText(status)
	}

	resp := map[string]string{"message": message}
	if errorType != "" {
		resp["__type"] = errorType
		w.Header().Set("x-amzn-ErrorType", errorType)
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}

func writeDynamoJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(payload)
}
