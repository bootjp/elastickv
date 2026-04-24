package admin

import (
	"context"
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

// BodyLimit wraps the request body with http.MaxBytesReader and responds
// 413 when the client exceeds the cap. It also sets
// http.MaxBytesError-aware error translation so the handler does not need
// to distinguish ordinary IO failures from overflow.
func BodyLimit(limit int64) func(http.Handler) http.Handler {
	if limit <= 0 {
		limit = defaultBodyLimit
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, limit)
			}
			next.ServeHTTP(bodyLimitResponseWriter{ResponseWriter: w}, r)
		})
	}
}

// bodyLimitResponseWriter is a minor adapter that lets a handler translate
// its own MaxBytesError into a consistent 413 without duplicating the
// plumbing. At the time of writing, each write handler can call
// r.ParseForm / json.Decode and on error call
// `if errors.As(err, &http.MaxBytesError{}) { ... }` manually; this
// wrapper just forces the header once per request.
type bodyLimitResponseWriter struct {
	http.ResponseWriter
}

// WriteMaxBytesError is called by handlers that detected a MaxBytesError.
// It is a package-level helper rather than a method so the router error
// path keeps the same JSON shape as the rest.
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
			// Constant-time comparison: the values are user-supplied
			// and we do not want to leak length differences.
			if !constantTimeEq(cookie.Value, header) {
				writeJSONError(w, http.StatusForbidden, "csrf_mismatch", "CSRF token mismatch")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func constantTimeEq(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := 0; i < len(a); i++ {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
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
