package admin

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/goccy/go-json"
)

// CredentialStore is the read-side view of the static SigV4 credential
// table the server was configured with. It returns the secret for a
// given access key, or ("", false) if the key is unknown. Supplying the
// same map the S3/DynamoDB adapters use keeps authentication consistent
// across the protocol surface.
type CredentialStore interface {
	LookupSecret(accessKey string) (string, bool)
}

// MapCredentialStore adapts a plain map into the CredentialStore
// interface. Callers typically load this from config at startup and
// hand the same map to the S3 adapter and the admin service.
type MapCredentialStore map[string]string

// LookupSecret implements CredentialStore.
func (m MapCredentialStore) LookupSecret(accessKey string) (string, bool) {
	secret, ok := m[strings.TrimSpace(accessKey)]
	return secret, ok
}

// AuthService wires the login/logout handlers, token minting, role
// lookup, and per-IP rate limiter together. Construct it once at
// startup and reuse across the admin listener's lifetime.
type AuthService struct {
	signer       *Signer
	verifier     *Verifier
	creds        CredentialStore
	roles        map[string]Role
	limiter      *rateLimiter
	sessionTTL   time.Duration
	secureCookie bool
	cookieDomain string
	logger       *slog.Logger
}

// AuthServiceOpts covers the knobs a caller may want to vary in tests.
// Zero values fall back to production defaults.
type AuthServiceOpts struct {
	// InsecureCookie disables the Secure attribute on the issued
	// cookies. It exists only for local plaintext-loopback development
	// and is expected to stay false in any real deployment.
	InsecureCookie bool
	// CookieDomain is optional and rarely used. Empty means "host-only
	// cookie", which is the default and the safest choice.
	CookieDomain string
	// LoginLimit is the per-IP rate limit (default 5).
	LoginLimit int
	// LoginWindow is the rate-limit window (default 1 minute).
	LoginWindow time.Duration
	// Clock drives rate-limiter aging. Defaults to SystemClock.
	Clock Clock
	// Verifier lets the logout handler best-effort decode the
	// incoming session cookie and include the actor in the audit
	// log. When nil, logout events are still audited but with an
	// empty actor field.
	Verifier *Verifier
	// Logger is the slog destination for admin_audit entries emitted
	// by the login/logout handlers. nil falls back to slog.Default().
	Logger *slog.Logger
}

// NewAuthService constructs an AuthService. The signer must be primary
// (use NewSigner with the current key); token verification uses the
// Verifier passed separately to SessionAuth.
func NewAuthService(signer *Signer, creds CredentialStore, roles map[string]Role, opts AuthServiceOpts) *AuthService {
	limit := opts.LoginLimit
	if limit <= 0 {
		limit = 5
	}
	window := opts.LoginWindow
	if window <= 0 {
		window = time.Minute
	}
	if opts.Clock == nil {
		opts.Clock = SystemClock
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &AuthService{
		signer:       signer,
		verifier:     opts.Verifier,
		creds:        creds,
		roles:        roles,
		limiter:      newRateLimiter(limit, window, opts.Clock),
		sessionTTL:   sessionTTL,
		secureCookie: !opts.InsecureCookie,
		cookieDomain: opts.CookieDomain,
		logger:       logger,
	}
}

// loginRequest is the JSON body the login endpoint accepts.
type loginRequest struct {
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
}

// loginResponse is the JSON body the login endpoint returns on success.
// The CSRF token is also readable from the admin_csrf cookie; we include
// it here as a convenience for clients that want to avoid parsing the
// Set-Cookie header themselves.
type loginResponse struct {
	Role      Role      `json:"role"`
	ExpiresAt time.Time `json:"expires_at"`
}

// HandleLogin validates credentials and issues the session + CSRF cookies.
// It is safe to expose without the SessionAuth middleware because this is
// where a session first comes from; rate limiting, Content-Type validation,
// and constant-time credential comparison guard it.
//
// Login events (success and failure) emit admin_audit slog entries
// directly. The generic Audit middleware cannot do this because it runs
// before the handler knows who the caller is claiming to be.
func (s *AuthService) HandleLogin(w http.ResponseWriter, r *http.Request) {
	rec := newStatusRecorder(w)
	defer s.auditLogin(r, rec)

	if !s.preflightLogin(rec, r) {
		return
	}
	req, ok := readLoginRequest(rec, r)
	rec.claimedActor = req.AccessKey
	if !ok {
		return
	}
	principal, ok := s.authenticate(rec, req)
	if !ok {
		return
	}
	s.issueSession(rec, principal)
	rec.actor = principal.AccessKey
}

func (s *AuthService) preflightLogin(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "login requires POST")
		return false
	}
	if !s.limiter.allow(clientIP(r)) {
		w.Header().Set("Retry-After", "60")
		writeJSONError(w, http.StatusTooManyRequests, "rate_limited",
			"too many login attempts from this source; try again later")
		return false
	}
	ct := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type")))
	if !strings.HasPrefix(ct, "application/json") {
		writeJSONError(w, http.StatusUnsupportedMediaType, "unsupported_media_type",
			"login requires Content-Type: application/json")
		return false
	}
	return true
}

func readLoginRequest(w http.ResponseWriter, r *http.Request) (loginRequest, bool) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		if IsMaxBytesError(err) {
			WriteMaxBytesError(w)
			return loginRequest{}, false
		}
		writeJSONError(w, http.StatusBadRequest, "invalid_body", "failed to read body")
		return loginRequest{}, false
	}
	var req loginRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_body", "body is not valid JSON")
		return loginRequest{}, false
	}
	// Access keys are AWS-style identifiers that users sometimes copy
	// with surrounding whitespace; trimming there is harmless and
	// matches how the S3 adapter normalises its credential table at
	// load time. Secrets, by contrast, are opaque bytes — trimming
	// would accept inputs the SigV4 adapter would reject, creating a
	// cross-protocol inconsistency. Leave SecretKey untouched.
	req.AccessKey = strings.TrimSpace(req.AccessKey)
	if req.AccessKey == "" || req.SecretKey == "" {
		writeJSONError(w, http.StatusBadRequest, "missing_fields",
			"access_key and secret_key are required")
		return loginRequest{}, false
	}
	return req, true
}

// secretCompareKey is a per-process random key used to derive
// fixed-length digests of incoming and expected login secrets before a
// constant-time comparison. The key itself does not need to be secret —
// its only job is to:
//
//  1. normalise inputs to a fixed 32-byte width so subtle.ConstantTimeCompare
//     cannot leak the length of the expected secret via an early-return,
//  2. make the construction a keyed MAC rather than a naked password hash,
//     which keeps static analysis (CodeQL) aligned with the intent: this
//     is a timing-safe comparator, not a persisted password hash.
//
// We deliberately do not use bcrypt / argon2 here: nothing is persisted,
// the secret is received in plaintext over TLS at login time, and the
// rate limiter already bounds online guessing. A computationally
// expensive KDF would add latency to every login attempt without
// changing the threat model.
var (
	secretCompareKey     []byte
	secretCompareKeyOnce sync.Once
)

func initSecretCompareKey() {
	secretCompareKey = make([]byte, sha256.Size)
	if _, err := rand.Read(secretCompareKey); err != nil {
		// rand.Read never fails on supported platforms; if it did,
		// panicking is the right reaction — the admin listener
		// cannot authenticate anyone anyway.
		panic("admin: crypto/rand failure while initialising secret compare key: " + err.Error())
	}
}

// digestForCompare returns HMAC-SHA256(secretCompareKey, s). Used only
// for timing-safe comparison of login secrets; never stored.
func digestForCompare(s string) []byte {
	secretCompareKeyOnce.Do(initSecretCompareKey)
	mac := hmac.New(sha256.New, secretCompareKey)
	mac.Write([]byte(s))
	return mac.Sum(nil)
}

// unknownKeyPlaceholder is a sentinel digest we compare against when the
// caller-supplied access key is not in the credential store. Using a
// deterministic value here keeps the work done by the authenticate path
// roughly equivalent between "unknown key" and "known key, wrong
// secret", so the two branches are not distinguishable by timing.
var unknownKeyPlaceholder = func() []byte {
	secretCompareKeyOnce.Do(initSecretCompareKey)
	mac := hmac.New(sha256.New, secretCompareKey)
	mac.Write([]byte("admin-auth-unknown-key-placeholder"))
	return mac.Sum(nil)
}

func (s *AuthService) authenticate(w http.ResponseWriter, req loginRequest) (AuthPrincipal, bool) {
	providedHash := digestForCompare(req.SecretKey)
	expected, known := s.creds.LookupSecret(req.AccessKey)
	expectedHash := unknownKeyPlaceholder()
	if known {
		expectedHash = digestForCompare(expected)
	}
	match := subtle.ConstantTimeCompare(providedHash, expectedHash) == 1
	if !known || !match {
		writeJSONError(w, http.StatusUnauthorized, "invalid_credentials",
			"access_key or secret_key is invalid")
		return AuthPrincipal{}, false
	}
	role, ok := s.roles[req.AccessKey]
	if !ok {
		writeJSONError(w, http.StatusForbidden, "forbidden",
			"access_key is not authorised for admin access")
		return AuthPrincipal{}, false
	}
	return AuthPrincipal{AccessKey: req.AccessKey, Role: role}, true
}

func (s *AuthService) issueSession(w http.ResponseWriter, principal AuthPrincipal) {
	token, err := s.signer.Sign(principal)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "internal", "failed to mint session token")
		return
	}
	csrf, err := newCSRFToken()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "internal", "failed to mint csrf token")
		return
	}
	expires := time.Now().UTC().Add(s.sessionTTL)
	http.SetCookie(w, s.buildCookie(sessionCookieName, token, true))
	http.SetCookie(w, s.buildCookie(csrfCookieName, csrf, false))
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(loginResponse{Role: principal.Role, ExpiresAt: expires})
}

// HandleLogout clears both cookies. It does not require authentication —
// clearing stale cookies after a session has expired is always safe. We
// best-effort decode the incoming session cookie so the audit log can
// record who logged out; a missing or invalid cookie leaves actor empty.
func (s *AuthService) HandleLogout(w http.ResponseWriter, r *http.Request) {
	rec := newStatusRecorder(w)
	defer s.auditLogout(r, rec)
	if r.Method != http.MethodPost {
		writeJSONError(rec, http.StatusMethodNotAllowed, "method_not_allowed", "logout requires POST")
		return
	}
	if s.verifier != nil {
		if c, err := r.Cookie(sessionCookieName); err == nil && strings.TrimSpace(c.Value) != "" {
			if p, verr := s.verifier.Verify(c.Value); verr == nil {
				rec.actor = p.AccessKey
			}
		}
	}
	http.SetCookie(rec, s.buildExpiredCookie(sessionCookieName, true))
	http.SetCookie(rec, s.buildExpiredCookie(csrfCookieName, false))
	rec.Header().Set("Cache-Control", "no-store")
	rec.WriteHeader(http.StatusNoContent)
}

// statusRecorder captures the response status + writes we emit so the
// audit log can include both the final code and the claimed actor.
type statusRecorder struct {
	http.ResponseWriter
	status       int
	claimedActor string // what the caller said they were
	actor        string // what we authenticated them as (empty on failure)
}

func newStatusRecorder(w http.ResponseWriter) *statusRecorder {
	return &statusRecorder{ResponseWriter: w}
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	if err != nil {
		return n, errors.Wrap(err, "status recorder write")
	}
	return n, nil
}

func (s *AuthService) auditLogin(r *http.Request, rec *statusRecorder) {
	s.logger.LogAttrs(r.Context(), slog.LevelInfo, "admin_audit",
		slog.String("action", "login"),
		slog.String("actor", rec.actor),
		slog.String("claimed_actor", rec.claimedActor),
		slog.String("remote", r.RemoteAddr),
		slog.Int("status", nonZero(rec.status, http.StatusOK)),
	)
}

func (s *AuthService) auditLogout(r *http.Request, rec *statusRecorder) {
	s.logger.LogAttrs(r.Context(), slog.LevelInfo, "admin_audit",
		slog.String("action", "logout"),
		slog.String("actor", rec.actor),
		slog.String("remote", r.RemoteAddr),
		slog.Int("status", nonZero(rec.status, http.StatusOK)),
	)
}

func nonZero(v, fallback int) int {
	if v == 0 {
		return fallback
	}
	return v
}

func (s *AuthService) buildCookie(name, value string, httpOnly bool) *http.Cookie {
	return &http.Cookie{
		Name:     name,
		Value:    value,
		Path:     pathPrefixAdmin,
		Domain:   s.cookieDomain,
		MaxAge:   int(s.sessionTTL.Seconds()),
		Secure:   s.secureCookie,
		HttpOnly: httpOnly,
		SameSite: http.SameSiteStrictMode,
	}
}

func (s *AuthService) buildExpiredCookie(name string, httpOnly bool) *http.Cookie {
	return &http.Cookie{
		Name:     name,
		Value:    "",
		Path:     pathPrefixAdmin,
		Domain:   s.cookieDomain,
		MaxAge:   -1,
		Expires:  time.Unix(0, 0),
		Secure:   s.secureCookie,
		HttpOnly: httpOnly,
		SameSite: http.SameSiteStrictMode,
	}
}

func newCSRFToken() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", errors.Wrap(err, "read random bytes for csrf token")
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}
