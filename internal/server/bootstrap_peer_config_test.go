package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/stretchr/testify/require"
)

func bootstrapConfigFixture(t *testing.T) clusterbootstrap.BootstrapPeerJSON {
	t.Helper()
	dir := t.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	require.NoError(t, err)
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, DNSNames: []string{"localhost"}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, key)
	require.NoError(t, err)
	parsed, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	write := func(name, kind string, data []byte) string {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: data}), 0600))
		return path
	}
	digest := sha256.Sum256(parsed.RawSubjectPublicKeyInfo)
	return clusterbootstrap.BootstrapPeerJSON{Cluster: "12345678-1234-1234-1234-123456789abc", Local: 1, Members: []clusterbootstrap.BootstrapPeerMemberJSON{{ID: 1, Address: "localhost:50051", Key: hex.EncodeToString(digest[:]), Groups: []uint64{1}}}, CA: write("ca.pem", "CERTIFICATE", caDER), Cert: write("cert.pem", "CERTIFICATE", der), Key: write("key.pem", "PRIVATE KEY", keyDER)}
}

func loadBootstrapFixture(t *testing.T, data []byte) (*clusterbootstrap.BootstrapPeerConfig, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(path, data, 0600))
	return clusterbootstrap.LoadBootstrapPeerConfig(path)
}

func TestBootstrapPeerConfigValid(t *testing.T) {
	t.Parallel()
	raw := bootstrapConfigFixture(t)
	raw.Members[0].Groups = []uint64{0, 1}
	data, err := json.Marshal(raw)
	require.NoError(t, err)
	cfg, err := loadBootstrapFixture(t, data)
	require.NoError(t, err)
	require.Equal(t, uint64(1), cfg.Local.Member)
	require.Len(t, cfg.Provisions, 1)
	require.Equal(t, []uint64{0, 1}, cfg.Provisions[0].Groups)
	require.Equal(t, tls.VerifyClientCertIfGiven, cfg.ServerTLS.ClientAuth)
	require.NotNil(t, cfg.ServerTLS.ClientCAs)
	require.NotNil(t, cfg.ClientTLS.RootCAs)
	for _, config := range []*tls.Config{cfg.ServerTLS, cfg.ClientTLS} {
		require.Equal(t, uint16(tls.VersionTLS12), config.MinVersion)
		require.False(t, config.InsecureSkipVerify)
		require.Len(t, config.Certificates, 1)
	}
}

func TestBootstrapPeerConfigRejectJSON(t *testing.T) {
	t.Parallel()
	raw := bootstrapConfigFixture(t)
	data, err := json.Marshal(raw)
	require.NoError(t, err)
	valid := string(data)
	cases := []string{valid + " {}", valid + " garbage", `{"local_member":1,"local_member":2}`, `{"LOCAL_MEMBER":1}`, `{"unknown":true}`, `null`, strings.Replace(valid, `"member_id":1`, `"member_id":1,"member_id":1`, 1), strings.Repeat(" ", clusterbootstrap.BootstrapPeerConfigLimit+1)}
	for i, input := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			_, err := loadBootstrapFixture(t, []byte(input))
			require.Error(t, err)
		})
	}
	// Whitespace counts toward the boundary, even when the decoded object is small.
	padded := valid + strings.Repeat(" ", clusterbootstrap.BootstrapPeerConfigLimit-len(valid))
	_, err = loadBootstrapFixture(t, []byte(padded))
	require.NoError(t, err)
}

func TestBootstrapPeerConfigRejectIdentityAndPKI(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*clusterbootstrap.BootstrapPeerJSON){
		"zero cluster":         func(r *clusterbootstrap.BootstrapPeerJSON) { r.Cluster = "00000000-0000-0000-0000-000000000000" },
		"zero local":           func(r *clusterbootstrap.BootstrapPeerJSON) { r.Local = 0 },
		"absent local":         func(r *clusterbootstrap.BootstrapPeerJSON) { r.Local = 2 },
		"zero member":          func(r *clusterbootstrap.BootstrapPeerJSON) { r.Members[0].ID = 0 },
		"bad address":          func(r *clusterbootstrap.BootstrapPeerJSON) { r.Members[0].Address = "localhost:0" },
		"bad digest":           func(r *clusterbootstrap.BootstrapPeerJSON) { r.Members[0].Key = "bad" },
		"zero digest":          func(r *clusterbootstrap.BootstrapPeerJSON) { r.Members[0].Key = strings.Repeat("0", 64) },
		"spki mismatch":        func(r *clusterbootstrap.BootstrapPeerJSON) { r.Members[0].Key = strings.Repeat("1", 64) },
		"hostname mismatch":    func(r *clusterbootstrap.BootstrapPeerJSON) { r.Members[0].Address = "other.example:50051" },
		"IP hostname mismatch": func(r *clusterbootstrap.BootstrapPeerJSON) { r.Members[0].Address = "127.0.0.1:50051" },
		"duplicate zero group": func(r *clusterbootstrap.BootstrapPeerJSON) { r.Members[0].Groups = []uint64{0, 0} },
		"duplicate group":      func(r *clusterbootstrap.BootstrapPeerJSON) { r.Members[0].Groups = []uint64{1, 1} },
		"missing groups":       func(r *clusterbootstrap.BootstrapPeerJSON) { r.Members[0].Groups = nil },
		"duplicate member":     func(r *clusterbootstrap.BootstrapPeerJSON) { r.Members = append(r.Members, r.Members[0]) },
		"duplicate address": func(r *clusterbootstrap.BootstrapPeerJSON) {
			m := r.Members[0]
			m.ID = 2
			m.Key = strings.Repeat("1", 64)
			r.Members = append(r.Members, m)
		},
		"duplicate key": func(r *clusterbootstrap.BootstrapPeerJSON) {
			m := r.Members[0]
			m.ID = 2
			m.Address = "localhost:50052"
			r.Members = append(r.Members, m)
		},
		"missing CA":   func(r *clusterbootstrap.BootstrapPeerJSON) { r.CA += ".missing" },
		"invalid CA":   func(r *clusterbootstrap.BootstrapPeerJSON) { r.CA = r.Key },
		"missing key":  func(r *clusterbootstrap.BootstrapPeerJSON) { r.Key += ".missing" },
		"key mismatch": func(r *clusterbootstrap.BootstrapPeerJSON) { other := bootstrapConfigFixture(t); r.Key = other.Key },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			raw := bootstrapConfigFixture(t)
			mutate(&raw)
			data, err := json.Marshal(raw)
			require.NoError(t, err)
			_, err = loadBootstrapFixture(t, data)
			require.Error(t, err)
		})
	}
}
