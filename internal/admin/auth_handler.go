package admin

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"io"
	"net/http"
	"strings"
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
	creds        CredentialStore
	roles        map[string]Role
	limiter      *rateLimiter
	sessionTTL   time.Duration
	secureCookie bool
	cookieDomain string
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
	return &AuthService{
		signer:       signer,
		creds:        creds,
		roles:        roles,
		limiter:      newRateLimiter(limit, window, opts.Clock),
		sessionTTL:   sessionTTL,
		secureCookie: !opts.InsecureCookie,
		cookieDomain: opts.CookieDomain,
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
func (s *AuthService) HandleLogin(w http.ResponseWriter, r *http.Request) {
	if !s.preflightLogin(w, r) {
		return
	}
	req, ok := readLoginRequest(w, r)
	if !ok {
		return
	}
	principal, ok := s.authenticate(w, req)
	if !ok {
		return
	}
	s.issueSession(w, principal)
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
	req.AccessKey = strings.TrimSpace(req.AccessKey)
	req.SecretKey = strings.TrimSpace(req.SecretKey)
	if req.AccessKey == "" || req.SecretKey == "" {
		writeJSONError(w, http.StatusBadRequest, "missing_fields",
			"access_key and secret_key are required")
		return loginRequest{}, false
	}
	return req, true
}

// dummySecretLen is the length we pad the timing-safe comparison with
// when the access key is unknown. It roughly matches the length of a
// typical AWS secret-access-key.
const dummySecretLen = 40

func (s *AuthService) authenticate(w http.ResponseWriter, req loginRequest) (AuthPrincipal, bool) {
	expected, known := s.creds.LookupSecret(req.AccessKey)
	if !known {
		// Still compare against a dummy secret to keep timing
		// roughly equivalent between known and unknown keys.
		dummy := strings.Repeat("x", dummySecretLen)
		_ = subtle.ConstantTimeCompare([]byte(req.SecretKey), []byte(dummy))
		writeJSONError(w, http.StatusUnauthorized, "invalid_credentials",
			"access_key or secret_key is invalid")
		return AuthPrincipal{}, false
	}
	if subtle.ConstantTimeCompare([]byte(req.SecretKey), []byte(expected)) != 1 {
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
// clearing stale cookies after a session has expired is always safe.
func (s *AuthService) HandleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "logout requires POST")
		return
	}
	http.SetCookie(w, s.buildExpiredCookie(sessionCookieName, true))
	http.SetCookie(w, s.buildExpiredCookie(csrfCookieName, false))
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
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
