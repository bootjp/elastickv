package admin

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/goccy/go-json"
)

// Session TTL for admin JWTs. Aligns with the 1h Max-Age specified for the
// session cookie in the design doc (Section 6.1).
const sessionTTL = 1 * time.Hour

// defaultClockSkewTolerance is the slack we allow on the "issued in
// the future" check so that minor NTP drift between admin nodes does
// not produce spurious 401s. Operators in distributed environments
// with looser clock synchronisation can override the per-verifier
// value via Verifier.WithClockSkewTolerance.
const defaultClockSkewTolerance = 30 * time.Second

// jwtSegments is the fixed number of dot-separated segments in a valid
// HS256 JWT (header.payload.signature).
const jwtSegments = 3

// jwtHeader is the fixed HS256 JWT header. Admin never issues anything else.
var jwtHeaderEncoded = base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))

type jwtClaims struct {
	Sub  string `json:"sub"`
	Role string `json:"role"`
	IAT  int64  `json:"iat"`
	EXP  int64  `json:"exp"`
	JTI  string `json:"jti"`
}

// Clock is the small time abstraction used by the signer/verifier so tests
// can control token freshness without sleeping.
type Clock func() time.Time

// SystemClock returns wall-clock time and is the default for production.
func SystemClock() time.Time { return time.Now().UTC() }

// Signer issues HS256-signed JWTs using the primary admin signing key. Only
// the primary key can sign new tokens; the previous key is verify-only and
// lives on Verifier.
type Signer struct {
	key   []byte
	clock Clock
}

// NewSigner constructs a Signer; key must be exactly sessionSigningKeyLen
// bytes (validated up-front so we do not catch this inside the hot path).
func NewSigner(key []byte, clock Clock) (*Signer, error) {
	if len(key) != sessionSigningKeyLen {
		return nil, errors.WithStack(errors.Newf("signer key must be %d bytes, got %d", sessionSigningKeyLen, len(key)))
	}
	if clock == nil {
		clock = SystemClock
	}
	copied := append([]byte{}, key...)
	return &Signer{key: copied, clock: clock}, nil
}

// Sign mints a fresh JWT for principal with the admin session TTL.
func (s *Signer) Sign(principal AuthPrincipal) (string, error) {
	jti, err := randomJTI()
	if err != nil {
		return "", err
	}
	now := s.clock().UTC()
	claims := jwtClaims{
		Sub:  principal.AccessKey,
		Role: string(principal.Role),
		IAT:  now.Unix(),
		EXP:  now.Add(sessionTTL).Unix(),
		JTI:  jti,
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", errors.Wrap(err, "marshal jwt claims")
	}
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	signingInput := jwtHeaderEncoded + "." + encodedPayload
	mac := hmac.New(sha256.New, s.key)
	mac.Write([]byte(signingInput))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return signingInput + "." + sig, nil
}

// Verifier validates HS256 admin tokens. It tries the primary key first and
// falls back to the optional previous key so operators can rotate keys
// without logging everybody out at once.
type Verifier struct {
	keys           [][]byte
	clock          Clock
	clockTolerance time.Duration
}

// NewVerifier builds a verifier from keys in priority order (primary first,
// optional previous second). Zero-length keys are rejected.
func NewVerifier(keys [][]byte, clock Clock) (*Verifier, error) {
	if len(keys) == 0 {
		return nil, errors.New("verifier requires at least one key")
	}
	for i, k := range keys {
		if len(k) != sessionSigningKeyLen {
			return nil, errors.WithStack(errors.Newf("verifier key[%d] must be %d bytes, got %d", i, sessionSigningKeyLen, len(k)))
		}
	}
	copied := make([][]byte, len(keys))
	for i, k := range keys {
		copied[i] = append([]byte{}, k...)
	}
	if clock == nil {
		clock = SystemClock
	}
	return &Verifier{keys: copied, clock: clock, clockTolerance: defaultClockSkewTolerance}, nil
}

// WithClockSkewTolerance overrides the future-issuance grace window.
// Operators in distributed environments where NTP synchronisation is
// loose may want a larger value to avoid spurious 401s when the
// signer's clock leads the verifier's by more than the default.
// Negative or zero durations fall back to defaultClockSkewTolerance.
func (v *Verifier) WithClockSkewTolerance(d time.Duration) *Verifier {
	if d <= 0 {
		v.clockTolerance = defaultClockSkewTolerance
	} else {
		v.clockTolerance = d
	}
	return v
}

// ErrInvalidToken is returned for any verification failure without leaking
// which specific check failed. Callers should log the wrapped error but
// return a single 401 to clients regardless of the cause.
var ErrInvalidToken = errors.New("invalid admin session token")

// Verify parses token, checks the signature against each configured key,
// and confirms it is within its validity window. On success it returns the
// embedded AuthPrincipal.
func (v *Verifier) Verify(token string) (AuthPrincipal, error) {
	signingInput, payloadSeg, sig, err := splitSignedToken(token)
	if err != nil {
		return AuthPrincipal{}, err
	}
	if err := v.checkSignature(signingInput, sig); err != nil {
		return AuthPrincipal{}, err
	}
	claims, err := decodeClaims(payloadSeg)
	if err != nil {
		return AuthPrincipal{}, err
	}
	return v.validateClaims(claims)
}

func splitSignedToken(token string) (signingInput, payloadSeg string, sig []byte, err error) {
	parts := strings.Split(token, ".")
	if len(parts) != jwtSegments {
		return "", "", nil, errors.Wrap(ErrInvalidToken, "token does not have three segments")
	}
	if parts[0] != jwtHeaderEncoded {
		return "", "", nil, errors.Wrap(ErrInvalidToken, "unsupported jwt header")
	}
	sig, err = base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", "", nil, errors.Wrap(ErrInvalidToken, "malformed signature")
	}
	return parts[0] + "." + parts[1], parts[1], sig, nil
}

func (v *Verifier) checkSignature(signingInput string, providedSig []byte) error {
	for _, k := range v.keys {
		mac := hmac.New(sha256.New, k)
		mac.Write([]byte(signingInput))
		if subtle.ConstantTimeCompare(mac.Sum(nil), providedSig) == 1 {
			return nil
		}
	}
	return errors.Wrap(ErrInvalidToken, "signature mismatch")
}

func decodeClaims(payloadSeg string) (jwtClaims, error) {
	payload, err := base64.RawURLEncoding.DecodeString(payloadSeg)
	if err != nil {
		return jwtClaims{}, errors.Wrap(ErrInvalidToken, "malformed payload")
	}
	var claims jwtClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return jwtClaims{}, errors.Wrap(ErrInvalidToken, "payload is not json")
	}
	return claims, nil
}

func (v *Verifier) validateClaims(claims jwtClaims) (AuthPrincipal, error) {
	now := v.clock().UTC()
	if claims.EXP == 0 || now.Unix() >= claims.EXP {
		return AuthPrincipal{}, errors.Wrap(ErrInvalidToken, "token expired")
	}
	// A missing iat is treated the same as a missing exp: the admin
	// Signer always sets iat, so a token without one is either
	// malformed, produced by a foreign signer that happens to share
	// the HS256 key, or a future-version token we do not recognise.
	if claims.IAT == 0 {
		return AuthPrincipal{}, errors.Wrap(ErrInvalidToken, "missing iat")
	}
	// Compare with full Duration precision rather than truncating
	// the tolerance to whole seconds. A caller that configured
	// 500*time.Millisecond should still get half a second of slack;
	// the previous int64(.Seconds()) round-down silently dropped
	// any sub-second value to zero.
	iat := time.Unix(claims.IAT, 0).UTC()
	if now.Add(v.clockTolerance).Before(iat) {
		return AuthPrincipal{}, errors.Wrap(ErrInvalidToken, "token issued in the future")
	}
	if claims.Sub == "" {
		return AuthPrincipal{}, errors.Wrap(ErrInvalidToken, "missing sub")
	}
	role := Role(claims.Role)
	if role != RoleReadOnly && role != RoleFull {
		return AuthPrincipal{}, errors.Wrap(ErrInvalidToken, "unknown role")
	}
	return AuthPrincipal{AccessKey: claims.Sub, Role: role}, nil
}

func randomJTI() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", errors.Wrap(err, "read random bytes for jti")
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}
