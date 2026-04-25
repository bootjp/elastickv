package admin

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func fixedClock(t time.Time) Clock { return func() time.Time { return t } }

func newSignerForTest(t *testing.T, seed byte, clk Clock) *Signer {
	t.Helper()
	key := bytes.Repeat([]byte{seed}, sessionSigningKeyLen)
	s, err := NewSigner(key, clk)
	require.NoError(t, err)
	return s
}

func newVerifierForTest(t *testing.T, seeds []byte, clk Clock) *Verifier {
	t.Helper()
	keys := make([][]byte, 0, len(seeds))
	for _, seed := range seeds {
		keys = append(keys, bytes.Repeat([]byte{seed}, sessionSigningKeyLen))
	}
	v, err := NewVerifier(keys, clk)
	require.NoError(t, err)
	return v
}

func TestJWT_SignVerifyRoundTrip(t *testing.T) {
	clk := fixedClock(time.Unix(1_700_000_000, 0).UTC())
	signer := newSignerForTest(t, 1, clk)
	verifier := newVerifierForTest(t, []byte{1}, clk)

	principal := AuthPrincipal{AccessKey: "AKIA_OK", Role: RoleFull}
	token, err := signer.Sign(principal)
	require.NoError(t, err)
	require.Equal(t, 2, strings.Count(token, "."))

	got, err := verifier.Verify(token)
	require.NoError(t, err)
	require.Equal(t, principal, got)
}

func TestJWT_RejectsExpired(t *testing.T) {
	signClk := fixedClock(time.Unix(1_700_000_000, 0).UTC())
	verifyClk := fixedClock(time.Unix(1_700_000_000+int64(sessionTTL.Seconds()+1), 0).UTC())
	signer := newSignerForTest(t, 1, signClk)
	verifier := newVerifierForTest(t, []byte{1}, verifyClk)

	token, err := signer.Sign(AuthPrincipal{AccessKey: "AKIA", Role: RoleReadOnly})
	require.NoError(t, err)

	_, err = verifier.Verify(token)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidToken))
}

func TestJWT_RejectsFutureIssued(t *testing.T) {
	// Sign in the future; verifier clock is now.
	signClk := fixedClock(time.Unix(1_700_000_000+600, 0).UTC())
	verifyClk := fixedClock(time.Unix(1_700_000_000, 0).UTC())
	signer := newSignerForTest(t, 1, signClk)
	verifier := newVerifierForTest(t, []byte{1}, verifyClk)

	token, err := signer.Sign(AuthPrincipal{AccessKey: "AKIA", Role: RoleFull})
	require.NoError(t, err)

	_, err = verifier.Verify(token)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidToken))
}

// TestJWT_ClockSkewToleranceConfigurable proves WithClockSkewTolerance
// widens the future-issuance grace window so a verifier whose clock
// trails the signer by more than 30s can still accept its tokens.
func TestJWT_ClockSkewToleranceConfigurable(t *testing.T) {
	signClk := fixedClock(time.Unix(1_700_000_000+120, 0).UTC()) // 2 min ahead of verifier
	verifyClk := fixedClock(time.Unix(1_700_000_000, 0).UTC())
	signer := newSignerForTest(t, 1, signClk)

	// Default 30s tolerance: should reject (issued 120s in the future).
	defaultVerifier := newVerifierForTest(t, []byte{1}, verifyClk)
	token, err := signer.Sign(AuthPrincipal{AccessKey: "AKIA", Role: RoleFull})
	require.NoError(t, err)
	_, err = defaultVerifier.Verify(token)
	require.Error(t, err)

	// Loosen tolerance to 5 minutes: should accept.
	relaxed := newVerifierForTest(t, []byte{1}, verifyClk).WithClockSkewTolerance(5 * time.Minute)
	got, err := relaxed.Verify(token)
	require.NoError(t, err)
	require.Equal(t, "AKIA", got.AccessKey)
}

func TestJWT_RejectsWrongSignature(t *testing.T) {
	clk := fixedClock(time.Unix(1_700_000_000, 0).UTC())
	signer := newSignerForTest(t, 1, clk)
	verifier := newVerifierForTest(t, []byte{9}, clk) // different key

	token, err := signer.Sign(AuthPrincipal{AccessKey: "AKIA", Role: RoleFull})
	require.NoError(t, err)

	_, err = verifier.Verify(token)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidToken))
}

func TestJWT_PreviousKeyAccepted(t *testing.T) {
	clk := fixedClock(time.Unix(1_700_000_000, 0).UTC())
	// Previous key signs the token.
	oldSigner := newSignerForTest(t, 2, clk)
	token, err := oldSigner.Sign(AuthPrincipal{AccessKey: "AKIA_OLD", Role: RoleReadOnly})
	require.NoError(t, err)

	// Verifier has primary=new, previous=old.
	verifier := newVerifierForTest(t, []byte{1, 2}, clk)
	got, err := verifier.Verify(token)
	require.NoError(t, err)
	require.Equal(t, "AKIA_OLD", got.AccessKey)
	require.Equal(t, RoleReadOnly, got.Role)
}

func TestJWT_AfterRotationOldPreviousRejected(t *testing.T) {
	clk := fixedClock(time.Unix(1_700_000_000, 0).UTC())
	// Token minted with key seed=7.
	signer := newSignerForTest(t, 7, clk)
	token, err := signer.Sign(AuthPrincipal{AccessKey: "AKIA", Role: RoleFull})
	require.NoError(t, err)

	// After rotation completes, only seeds {1,2} are configured.
	verifier := newVerifierForTest(t, []byte{1, 2}, clk)
	_, err = verifier.Verify(token)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidToken))
}

func TestJWT_MalformedTokens(t *testing.T) {
	clk := fixedClock(time.Unix(1_700_000_000, 0).UTC())
	verifier := newVerifierForTest(t, []byte{1}, clk)

	cases := []string{
		"",
		"abc",
		"a.b",
		"a.b.c.d",
		"not-header.payload.sig",
	}
	for _, tok := range cases {
		_, err := verifier.Verify(tok)
		require.Errorf(t, err, "token %q should fail", tok)
		require.True(t, errors.Is(err, ErrInvalidToken))
	}
}

func TestJWT_RejectsUnknownRole(t *testing.T) {
	clk := fixedClock(time.Unix(1_700_000_000, 0).UTC())
	key := bytes.Repeat([]byte{3}, sessionSigningKeyLen)
	// Manually craft a token with role=admin (unsupported).
	payload := []byte(`{"sub":"AKIA","role":"admin","iat":1700000000,"exp":1700003600,"jti":"j"}`)
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	signingInput := jwtHeaderEncoded + "." + encodedPayload
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(signingInput))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	token := signingInput + "." + sig

	verifier := newVerifierForTest(t, []byte{3}, clk)
	_, err := verifier.Verify(token)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidToken))
}

func TestJWT_RejectsMissingSub(t *testing.T) {
	clk := fixedClock(time.Unix(1_700_000_000, 0).UTC())
	key := bytes.Repeat([]byte{4}, sessionSigningKeyLen)
	payload := []byte(`{"sub":"","role":"full","iat":1700000000,"exp":1700003600,"jti":"j"}`)
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	signingInput := jwtHeaderEncoded + "." + encodedPayload
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(signingInput))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	token := signingInput + "." + sig

	verifier := newVerifierForTest(t, []byte{4}, clk)
	_, err := verifier.Verify(token)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidToken))
}

func TestNewSigner_RejectsWrongKeyLength(t *testing.T) {
	_, err := NewSigner([]byte("short"), nil)
	require.Error(t, err)
}

func TestNewVerifier_RejectsEmptyKeys(t *testing.T) {
	_, err := NewVerifier(nil, nil)
	require.Error(t, err)
}
