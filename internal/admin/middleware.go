package admin

import (
	"context"
	"crypto/subtle"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	pkgerrors "github.com/cockroachdb/errors"
)

// Cookie names used throughout the admin surface. We define them in one
// place so the login handler, CSRF middleware, and logout handler cannot
// drift.
const (
	sessionCookieName = "admin_session"
	csrfCookieName    = "admin_csrf"
	csrfHeaderName    = "X-Admin-CSRF"

	// defaultBodyLimit matches docs/design 4.4: 64 KiB upper bound for
	// every POST/PUT endpoint. DynamoDB table schemas and S3 bucket
	// metadata are each well under this bound.
	defaultBodyLimit int64 = 64 << 10
)

// contextKey is the unexported type for storing values in request
// contexts. Using a string type directly would risk collisions with other
// packages.
type contextKey int

const (
	ctxKeyPrincipal contextKey = iota + 1
)

// PrincipalFromContext returns the authenticated principal associated
// with the request context, or false if the middleware did not set one.
func PrincipalFromContext(ctx context.Context) (AuthPrincipal, bool) {
	v, ok := ctx.Value(ctxKeyPrincipal).(AuthPrincipal)
	return v, ok
}

// BodyLimit caps each request body at `limit` bytes via
// http.MaxBytesReader. Handlers that read the body are responsible for
// detecting overflow (via IsMaxBytesError / errors.As on
// *http.MaxBytesError) and calling WriteMaxBytesError to respond 413.
// We intentionally do not centralise that translation in the middleware
// chain: different handlers parse bodies with different decoders (json,
// form, multipart) and each already has a natural error path, so a
// wrapper ResponseWriter would either double-write or mask subsequent
// errors.
func BodyLimit(limit int64) func(http.Handler) http.Handler {
	if limit <= 0 {
		limit = defaultBodyLimit
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, limit)
			}
			next.ServeHTTP(w, r)
		})
	}
}

// WriteMaxBytesError is the canonical 413 response body for admin
// handlers that detected an http.MaxBytesError while reading a request
// body. It keeps the JSON error shape consistent with the rest of the
// admin surface.
func WriteMaxBytesError(w http.ResponseWriter) {
	writeJSONError(w, http.StatusRequestEntityTooLarge, "payload_too_large",
		"request body exceeds the 64 KiB admin limit")
}

// IsMaxBytesError reports whether err was produced because the client
// uploaded more bytes than BodyLimit permits.
func IsMaxBytesError(err error) bool {
	if err == nil {
		return false
	}
	var mbe *http.MaxBytesError
	return errors.As(err, &mbe)
}

// SessionAuth parses the admin_session cookie, validates it against the
// verifier, and attaches the resulting AuthPrincipal to the request
// context. Requests without a session, or with an invalid/expired one,
// are rejected with 401.
func SessionAuth(verifier *Verifier) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cookie, err := r.Cookie(sessionCookieName)
			if err != nil || strings.TrimSpace(cookie.Value) == "" {
				writeJSONError(w, http.StatusUnauthorized, "unauthenticated", "missing session cookie")
				return
			}
			principal, err := verifier.Verify(cookie.Value)
			if err != nil {
				writeJSONError(w, http.StatusUnauthorized, "unauthenticated", "invalid or expired session")
				return
			}
			ctx := context.WithValue(r.Context(), ctxKeyPrincipal, principal)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequireWriteRole blocks the handler unless the current principal may
// execute write operations. Must be composed after SessionAuth.
func RequireWriteRole() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			principal, ok := PrincipalFromContext(r.Context())
			if !ok {
				writeJSONError(w, http.StatusUnauthorized, "unauthenticated", "no principal on context")
				return
			}
			if !principal.Role.AllowsWrite() {
				writeJSONError(w, http.StatusForbidden, "forbidden",
					"this endpoint requires admin.full_access_keys membership")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// CSRFDoubleSubmit enforces the double-submit cookie rule for state
// changing methods (POST, PUT, DELETE, PATCH). The admin_csrf cookie is
// minted at login; the SPA copies its value into the X-Admin-CSRF header
// on every write. We reject the request if either the cookie or the
// header is missing or if they do not match. GET/HEAD pass through
// untouched.
func CSRFDoubleSubmit() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodGet, http.MethodHead, http.MethodOptions:
				next.ServeHTTP(w, r)
				return
			}
			cookie, err := r.Cookie(csrfCookieName)
			if err != nil || strings.TrimSpace(cookie.Value) == "" {
				writeJSONError(w, http.StatusForbidden, "csrf_missing", "admin_csrf cookie is required")
				return
			}
			header := strings.TrimSpace(r.Header.Get(csrfHeaderName))
			if header == "" {
				writeJSONError(w, http.StatusForbidden, "csrf_missing",
					"X-Admin-CSRF header is required for write operations")
				return
			}
			// Constant-time comparison on the byte contents once we
			// know the lengths match. A length mismatch is itself not
			// secret (the server mints both tokens with a fixed 32-byte
			// width, so any divergence means an attacker forged or
			// corrupted the value — the response is 403 in every case
			// anyway), so short-circuiting there does not leak anything
			// useful to a timing attacker.
			if len(cookie.Value) != len(header) ||
				subtle.ConstantTimeCompare([]byte(cookie.Value), []byte(header)) != 1 {
				writeJSONError(w, http.StatusForbidden, "csrf_mismatch", "CSRF token mismatch")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// auditRecorder is the ResponseWriter wrapper the Audit middleware uses
// to learn the final status code without forcing the handler to pass it
// back explicitly.
type auditRecorder struct {
	http.ResponseWriter
	status  int
	written bool
}

func (a *auditRecorder) WriteHeader(code int) {
	if !a.written {
		a.status = code
		a.written = true
	}
	a.ResponseWriter.WriteHeader(code)
}

func (a *auditRecorder) Write(b []byte) (int, error) {
	if !a.written {
		a.status = http.StatusOK
		a.written = true
	}
	n, err := a.ResponseWriter.Write(b)
	if err != nil {
		return n, pkgerrors.Wrap(err, "audit recorder write")
	}
	return n, nil
}

// Audit writes a structured slog line for every state-changing admin
// request, as required by docs/design Section 10. GET/HEAD requests are
// not audited (read traffic can be too loud and does not modify state).
// The logger uses the "admin_audit" key so operators can filter. Callers
// wire this middleware after SessionAuth so the principal is available.
func Audit(logger *slog.Logger) func(http.Handler) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodGet, http.MethodHead, http.MethodOptions:
				next.ServeHTTP(w, r)
				return
			}
			rec := &auditRecorder{ResponseWriter: w}
			start := time.Now()
			next.ServeHTTP(rec, r)
			principal, _ := PrincipalFromContext(r.Context())
			logger.LogAttrs(r.Context(), slog.LevelInfo, "admin_audit",
				slog.String("actor", principal.AccessKey),
				slog.String("role", string(principal.Role)),
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", rec.status),
				slog.String("remote", r.RemoteAddr),
				slog.Duration("duration", time.Since(start)),
			)
		})
	}
}
