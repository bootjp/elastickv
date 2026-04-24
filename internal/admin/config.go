package admin

import (
	"encoding/base64"
	"net"
	"strings"

	"github.com/cockroachdb/errors"
)

const (
	// sessionSigningKeyLen is the required raw byte length for the admin
	// JWT HS256 signing key.
	sessionSigningKeyLen = 64
)

// Config captures everything the admin listener needs at startup. It mirrors
// the Section 7.1 table in docs/design/2026_04_24_proposed_admin_dashboard.md
// and intentionally uses plain Go fields rather than a config library so the
// existing flag-based wiring in main.go can hand values over without a new
// dependency.
type Config struct {
	// Enabled toggles the admin listener. Default false.
	Enabled bool

	// Listen is the host:port for the admin HTTP server. Default
	// 127.0.0.1:8080 (loopback only).
	Listen string

	// TLSCertFile / TLSKeyFile enable TLS when both are set.
	TLSCertFile string
	TLSKeyFile  string

	// AllowPlaintextNonLoopback opts out of the TLS-on-non-loopback
	// requirement. Refusing to honour it is the default.
	AllowPlaintextNonLoopback bool

	// SessionSigningKey is the base64-encoded cluster-wide HS256 key. It
	// must decode to exactly 64 bytes.
	SessionSigningKey string

	// SessionSigningKeyPrevious is an optional base64-encoded previous
	// signing key, used to verify tokens issued before a key rotation.
	SessionSigningKeyPrevious string

	// ReadOnlyAccessKeys grants the GET subset of admin endpoints.
	ReadOnlyAccessKeys []string

	// FullAccessKeys grants the full CRUD surface of admin endpoints.
	FullAccessKeys []string

	// AllowInsecureDevCookie turns off the always-on Secure cookie
	// attribute. Intended only for local plaintext development; it is off
	// by default and the startup banner calls it out loudly.
	AllowInsecureDevCookie bool
}

// Validate returns the first configuration error found, if any. It does not
// try to collect every error because any of these conditions is a hard
// startup failure.
func (c *Config) Validate() error {
	if c == nil {
		return errors.New("admin config is nil")
	}
	if !c.Enabled {
		return nil
	}
	if err := c.validateListen(); err != nil {
		return err
	}
	if err := c.validateTLS(); err != nil {
		return err
	}
	if err := c.validateSigningKeys(); err != nil {
		return err
	}
	return validateAccessKeyRoles(c.ReadOnlyAccessKeys, c.FullAccessKeys)
}

func (c *Config) validateListen() error {
	listen := strings.TrimSpace(c.Listen)
	if listen == "" {
		return errors.New("-adminListen must not be empty when -adminEnabled=true")
	}
	if _, _, err := net.SplitHostPort(listen); err != nil {
		return errors.Wrapf(err, "-adminListen %q is not host:port", c.Listen)
	}
	return nil
}

func (c *Config) validateTLS() error {
	certSet := strings.TrimSpace(c.TLSCertFile) != ""
	keySet := strings.TrimSpace(c.TLSKeyFile) != ""
	if certSet != keySet {
		// A lone cert or key almost always means a typo. Silently
		// treating it as "TLS off" would downgrade transport
		// security while the operator thinks TLS is enabled; fail
		// fast so the misconfiguration is visible at startup.
		return errors.New("-adminTLSCertFile and -adminTLSKeyFile must be set together;" +
			" partial TLS configuration is not allowed")
	}
	tlsConfigured := certSet && keySet
	if tlsConfigured || !addressRequiresTLS(strings.TrimSpace(c.Listen)) || c.AllowPlaintextNonLoopback {
		return nil
	}
	return errors.WithStack(errors.Newf(
		"-adminListen %q is not loopback but TLS is not configured;"+
			" set -adminTLSCertFile + -adminTLSKeyFile, or explicitly pass"+
			" -adminAllowPlaintextNonLoopback (strongly discouraged)",
		c.Listen,
	))
}

func (c *Config) validateSigningKeys() error {
	primary, err := decodeSigningKey("-adminSessionSigningKey", c.SessionSigningKey)
	if err != nil {
		return err
	}
	if len(primary) == 0 {
		return errors.New("-adminSessionSigningKey is required when -adminEnabled=true")
	}
	if strings.TrimSpace(c.SessionSigningKeyPrevious) == "" {
		return nil
	}
	if _, err := decodeSigningKey("-adminSessionSigningKeyPrevious", c.SessionSigningKeyPrevious); err != nil {
		return err
	}
	return nil
}

// DecodedSigningKeys returns the raw HS256 keys in verification order: the
// primary signing key first, followed by an optional previous key. Validate
// must be called first.
func (c *Config) DecodedSigningKeys() ([][]byte, error) {
	primary, err := decodeSigningKey("-adminSessionSigningKey", c.SessionSigningKey)
	if err != nil {
		return nil, err
	}
	keys := [][]byte{primary}
	if strings.TrimSpace(c.SessionSigningKeyPrevious) != "" {
		prev, err := decodeSigningKey("-adminSessionSigningKeyPrevious", c.SessionSigningKeyPrevious)
		if err != nil {
			return nil, err
		}
		keys = append(keys, prev)
	}
	return keys, nil
}

// RoleIndex returns a map from access key to Role after Validate has
// succeeded. The caller must not mutate the returned map.
func (c *Config) RoleIndex() map[string]Role {
	index := make(map[string]Role, len(c.FullAccessKeys)+len(c.ReadOnlyAccessKeys))
	for _, k := range c.FullAccessKeys {
		trim := strings.TrimSpace(k)
		if trim == "" {
			continue
		}
		index[trim] = RoleFull
	}
	for _, k := range c.ReadOnlyAccessKeys {
		trim := strings.TrimSpace(k)
		if trim == "" {
			continue
		}
		// Overlap is rejected by Validate; this branch only runs for
		// keys exclusive to read_only.
		index[trim] = RoleReadOnly
	}
	return index
}

func decodeSigningKey(field, encoded string) ([]byte, error) {
	trim := strings.TrimSpace(encoded)
	if trim == "" {
		return nil, nil
	}
	raw, err := base64.StdEncoding.DecodeString(trim)
	if err != nil {
		raw, err = base64.RawStdEncoding.DecodeString(trim)
		if err != nil {
			return nil, errors.Wrapf(err, "%s is not valid base64", field)
		}
	}
	if len(raw) != sessionSigningKeyLen {
		return nil, errors.WithStack(errors.Newf(
			"%s must decode to %d bytes but got %d bytes",
			field, sessionSigningKeyLen, len(raw),
		))
	}
	return raw, nil
}

func validateAccessKeyRoles(readOnly, full []string) error {
	fullSet := make(map[string]struct{}, len(full))
	for _, k := range full {
		trim := strings.TrimSpace(k)
		if trim == "" {
			continue
		}
		fullSet[trim] = struct{}{}
	}
	for _, k := range readOnly {
		trim := strings.TrimSpace(k)
		if trim == "" {
			continue
		}
		if _, dup := fullSet[trim]; dup {
			return errors.WithStack(errors.Newf(
				"access key %q is listed in both -adminReadOnlyAccessKeys and -adminFullAccessKeys;"+
					" this would silently grant write access depending on lookup order, so it is rejected at startup",
				trim,
			))
		}
	}
	return nil
}

// addressRequiresTLS reports whether a listen address is exposed beyond
// loopback and therefore must use TLS. Mirrors monitoring.AddressRequiresToken
// so the admin package does not import monitoring.
func addressRequiresTLS(addr string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err != nil {
		return true
	}
	host = strings.TrimSpace(host)
	if host == "" || host == "0.0.0.0" || host == "::" {
		return true
	}
	if strings.EqualFold(host, "localhost") {
		return false
	}
	ip := net.ParseIP(host)
	return ip == nil || !ip.IsLoopback()
}
