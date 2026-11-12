package adapter

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
)

// writeSelfSignedCert writes a short-lived self-signed certificate to a
// temp dir and returns the cert / key paths. The certificate is valid
// for 127.0.0.1 only; tests that need TLS should run with it.
func writeSelfSignedCert(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err)
	tmpl := x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "elastickv-admin-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	require.NoError(t, err)
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	certOut, err := os.Create(certPath)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der}))
	require.NoError(t, certOut.Close())

	keyBytes, err := x509.MarshalECPrivateKey(priv)
	require.NoError(t, err)
	keyOut, err := os.Create(keyPath)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}))
	require.NoError(t, keyOut.Close())
	return certPath, keyPath
}

type capabilityConfigEngine struct {
	cfg    raftengine.Configuration
	leader raftengine.LeaderInfo
}

func (e capabilityConfigEngine) SnapshotEvery() uint64 { return 0 }

func (e capabilityConfigEngine) Propose(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return nil, nil
}

func (e capabilityConfigEngine) ProposeAdmin(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return nil, nil
}

func (e capabilityConfigEngine) State() raftengine.State {
	return raftengine.StateFollower
}

func (e capabilityConfigEngine) Leader() raftengine.LeaderInfo {
	return e.leader
}

func (e capabilityConfigEngine) VerifyLeader(context.Context) error {
	return nil
}

func (e capabilityConfigEngine) LinearizableRead(context.Context) (uint64, error) {
	return 0, nil
}

func (e capabilityConfigEngine) Status() raftengine.Status {
	return raftengine.Status{}
}

func (e capabilityConfigEngine) Configuration(context.Context) (raftengine.Configuration, error) {
	return e.cfg, nil
}

func (e capabilityConfigEngine) Close() error {
	return nil
}
