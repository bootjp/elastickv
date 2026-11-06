package adapter

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/peersession"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const migrationAuthNativeTimeout = 5 * time.Second

func migrationAuthTestCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "migration auth test"}, DNSNames: []string{"migration.test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: private, Leaf: leaf}
}

// Unit interceptor fixtures use the REAL AuthInfo returned by grpc TLS
// credentials, not an empty TLSInfo or a hand-written "secure" context flag.
func migrationTLSInfoFixture(t *testing.T) credentials.TLSInfo {
	t.Helper()
	cert := migrationAuthTestCertificate(t)
	roots := x509.NewCertPool()
	roots.AddCert(cert.Leaf)
	serverRaw, clientRaw := net.Pipe()
	defer serverRaw.Close()
	defer clientRaw.Close()
	require.NoError(t, serverRaw.SetDeadline(time.Now().Add(migrationAuthNativeTimeout)))
	require.NoError(t, clientRaw.SetDeadline(time.Now().Add(migrationAuthNativeTimeout)))
	serverCredentials := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	clientCredentials := credentials.NewTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS12})
	type result struct {
		conn net.Conn
		auth credentials.AuthInfo
		err  error
	}
	done := make(chan result, 1)
	go func() {
		conn, auth, err := serverCredentials.ServerHandshake(serverRaw)
		done <- result{conn, auth, err}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), migrationAuthNativeTimeout)
	defer cancel()
	client, _, err := clientCredentials.ClientHandshake(ctx, "migration.test", clientRaw)
	require.NoError(t, err)
	var server result
	select {
	case server = <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.NoError(t, server.err)
	// Closing raw pipes first avoids blocking TLS close_notify writes on net.Pipe.
	_ = clientRaw.Close()
	_ = serverRaw.Close()
	_ = client.Close()
	_ = server.conn.Close()
	info, ok := server.auth.(credentials.TLSInfo)
	require.True(t, ok)
	require.True(t, info.State.HandshakeComplete)
	require.Equal(t, credentials.PrivacyAndIntegrity, info.SecurityLevel)
	require.Empty(t, info.State.PeerCertificates, "public TLS does not require a client certificate")
	return info
}

type migrationNamedAuthInfo struct {
	credentials.CommonAuthInfo
	kind string
}

func (a migrationNamedAuthInfo) AuthType() string { return a.kind }

type migrationSpoofedTLSInfo struct{ credentials.TLSInfo }

func TestSplitMigrationTLSProtectedTransportContract(t *testing.T) {
	secure := migrationTLSInfoFixture(t)
	zeroCommon := secure
	zeroCommon.SecurityLevel = credentials.InvalidSecurityLevel
	noSecurity := secure
	noSecurity.SecurityLevel = credentials.NoSecurity
	integrity := secure
	integrity.SecurityLevel = credentials.IntegrityOnly
	unfinished := secure
	unfinished.State.HandshakeComplete = false
	oldVersion := secure
	oldVersion.State.Version = tls.VersionTLS11
	noCipher := secure
	noCipher.State.CipherSuite = 0
	for _, tc := range []struct {
		name    string
		info    credentials.AuthInfo
		allowed bool
	}{
		{"legacy native TLS", secure, true},
		{"nil", nil, false},
		{"empty legacy TLS", credentials.TLSInfo{}, false},
		{"empty wrapper", &peersession.ServerAuthInfo{}, false},
		{"unminted wrapper with copied secure TLS", &peersession.ServerAuthInfo{TLSInfo: secure}, false},
		{"nil wrapper", (*peersession.ServerAuthInfo)(nil), false},
		{"wrong client-side wrapper", &peersession.ClientAuthInfo{TLSInfo: secure}, false},
		{"fake TLS type", migrationNamedAuthInfo{kind: "tls"}, false},
		{"fake secure TLS type", migrationNamedAuthInfo{kind: "tls", CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity}}, false},
		{"fake embedded TLS", migrationSpoofedTLSInfo{TLSInfo: secure}, false},
		{"ALTS", migrationNamedAuthInfo{kind: "alts", CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity}}, false},
		{"insecure", migrationNamedAuthInfo{kind: "insecure", CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.NoSecurity}}, false},
		{"zero common", zeroCommon, false}, {"no security", noSecurity, false}, {"integrity only", integrity, false},
		{"unfinished TLS", unfinished, false}, {"obsolete TLS", oldVersion, false}, {"zero cipher", noCipher, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.allowed, splitMigrationTLSProtectedTransport(tc.info))
			ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", "Bearer migration-secret", "transport-security", "tls"))
			ctx = peer.NewContext(ctx, &peer.Peer{AuthInfo: tc.info})
			unary, stream := SplitMigrationTokenAuth("migration-secret", false, true)
			called := false
			_, err := unary(ctx, nil, &grpc.UnaryServerInfo{FullMethod: pb.Internal_CleanupMigration_FullMethodName}, func(context.Context, any) (any, error) { called = true; return nil, nil })
			assertMigrationTransportContract(t, tc.allowed, called, err)
			called = false
			err = stream(nil, &adminAuthTestServerStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: pb.Internal_ExportRangeVersions_FullMethodName}, func(any, grpc.ServerStream) error { called = true; return nil })
			assertMigrationTransportContract(t, tc.allowed, called, err)
		})
	}
	// grpc's backward-compatibility helper ALONE would accept both zero cases.
	require.NoError(t, credentials.CheckSecurityLevel(credentials.TLSInfo{}, credentials.PrivacyAndIntegrity))
	require.NoError(t, credentials.CheckSecurityLevel(&peersession.ServerAuthInfo{}, credentials.PrivacyAndIntegrity))
}

func assertMigrationTransportContract(t *testing.T, allowed, called bool, err error) {
	t.Helper()
	require.Equal(t, allowed, called)
	if allowed {
		require.NoError(t, err)
		return
	}
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.ErrorContains(t, err, "split migration token requires TLS")
}
