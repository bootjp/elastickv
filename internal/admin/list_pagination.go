package admin

import (
	"encoding/base64"
	"errors"
	"net/http"
	"strconv"
)

// parseListPaginationParams extracts the (limit, startAfter) pair
// from a request's query string. Shared by every list endpoint —
// dynamo tables, s3 buckets, and any future paginated read — so the
// validation policy stays in one place: empty limit → caller's
// default; non-numeric / non-positive → 400 with a precise message;
// oversize → caller-supplied ceiling silently clamps; missing
// next_token → ""; non-base64url next_token → 400.
//
// On any rejection the helper writes the JSON error and returns
// ok=false so the handler can short-circuit. Returning the response
// from inside the helper keeps the call site to a single
// `if !ok { return }` line.
func parseListPaginationParams(w http.ResponseWriter, r *http.Request, defaultLimit, maxLimit int) (limit int, startAfter string, ok bool) {
	rawLimit := r.URL.Query().Get("limit")
	limit, err := parseListLimit(rawLimit, defaultLimit, maxLimit)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_limit", err.Error())
		return 0, "", false
	}
	startAfter, err = decodeListNextToken(r.URL.Query().Get("next_token"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_next_token", err.Error())
		return 0, "", false
	}
	return limit, startAfter, true
}

// parseListLimit centralises the limit-parsing rules: empty →
// default, negatives / non-numerics → typed error, oversize → silent
// clamp. Per-resource handlers wrap this with their own
// (default, max) pair so a future per-resource ceiling change does
// not have to re-thread through the shared helper.
func parseListLimit(raw string, defaultLimit, maxLimit int) (int, error) {
	if raw == "" {
		return defaultLimit, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, errors.New("limit must be an integer")
	}
	if n <= 0 {
		return 0, errors.New("limit must be positive")
	}
	if n > maxLimit {
		return maxLimit, nil
	}
	return n, nil
}

// decodeListNextToken reverses encodeListNextToken. We base64url-wrap
// the raw cursor string so the wire token is opaque from the
// client's perspective and the server can change the cursor
// representation later without breaking the API contract.
func decodeListNextToken(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return "", errors.New("next_token is not valid base64url")
	}
	return string(decoded), nil
}

// encodeListNextToken is the encoder counterpart used by every list
// handler when emitting a non-empty cursor in the response.
func encodeListNextToken(cursor string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(cursor))
}
