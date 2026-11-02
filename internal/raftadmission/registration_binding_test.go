package raftadmission

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"math/big"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// These tests exercise only A's inspection contract, not production decryption
// authority. Real AES-GCM bytes are generated locally; the controlled inspector
// recognizes those exact bytes and returns an independently known plaintext hash.
func registrationBindingCiphertext(t *testing.T, plain []byte) []byte {
	t.Helper()
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	aead, err := cipher.NewGCM(block)
	require.NoError(t, err)
	nonce := make([]byte, aead.NonceSize())
	_, err = rand.Read(nonce)
	require.NoError(t, err)
	final := aead.Seal(bytes.Clone(nonce), nonce, plain, nil)
	decoded, err := aead.Open(nil, final[:aead.NonceSize()], final[aead.NonceSize():], nil)
	require.NoError(t, err)
	require.Equal(t, plain, decoded)
	return final
}

func registrationBindingPlain(req *pb.RegisterEncryptionWriterRequest) []byte {
	// Exact test adapter for the canonical 0x03 / v1 registration frame.
	epoch := req.GetWriters()[0].GetLocalEpoch()
	if epoch > 65535 {
		return nil
	}
	plain := make([]byte, 16)
	plain[0], plain[1] = 3, 1
	binary.BigEndian.PutUint32(plain[2:6], req.GetDekId())
	binary.BigEndian.PutUint64(plain[6:14], req.GetWriters()[0].GetFullNodeId())
	binary.BigEndian.PutUint16(plain[14:], uint16(epoch))
	return plain
}

func registrationBindingCertificate(t *testing.T, serial int64) (tls.Certificate, raftbootstrap.Digest) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{SerialNumber: big.NewInt(serial), DNSNames: []string{"binding.test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: private, Leaf: leaf}, sha256.Sum256(leaf.RawSubjectPublicKeyInfo)
}

type registrationBindingService struct {
	pb.UnimplementedEncryptionAdminServer
	action func(context.Context) error
}

func (s *registrationBindingService) RegisterEncryptionWriter(ctx context.Context, _ *pb.RegisterEncryptionWriterRequest) (*pb.RegisterEncryptionWriterResponse, error) {
	if err := s.action(ctx); err != nil {
		return nil, err
	}
	return &pb.RegisterEncryptionWriterResponse{AppliedIndex: 1}, nil
}

type registrationBindingFixture struct {
	a                         *Arbiter
	server, client            *peersession.Owner
	conn                      *grpc.ClientConn
	handle                    *peersession.Handle
	service                   *registrationBindingService
	inspections, nativeChecks atomic.Int64
}

func newRegistrationBindingFixture(t *testing.T, missingDeclaration bool, traffic peersession.Traffic) *registrationBindingFixture {
	t.Helper()
	serverCert, serverKey := registrationBindingCertificate(t, 1)
	clientCert, clientKey := registrationBindingCertificate(t, 2)
	roots := x509.NewCertPool()
	roots.AddCert(serverCert.Leaf)
	roots.AddCert(clientCert.Leaf)
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	local := peersession.Node{Cluster: raftbootstrap.UUID{1}, Member: 1, Key: serverKey, Address: lis.Addr().String()}
	remote := peersession.Node{Cluster: local.Cluster, Member: 2, Key: clientKey, Address: "client.test:9001"}
	configuration := raftbootstrap.Configuration{Index: 1, State: raftbootstrap.ConfState{Voters: []uint64{1, 2}},
		Members: []raftbootstrap.Member{{ID: 1, Address: local.Address, PublicKeyDigest: local.Key}, {ID: 2, Address: remote.Address, PublicKeyDigest: remote.Key}}}
	method := registrationBindingMethod(missingDeclaration, traffic)
	provisions := []peersession.Provision{{Node: local, Groups: []uint64{1}}, {Node: remote, Groups: []uint64{1}}}
	versions := raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}
	server, err := peersession.NewOwner(peersession.Config{Local: local, Provisioned: provisions, Versions: versions, Methods: []peersession.Method{method}})
	require.NoError(t, err)
	t.Cleanup(server.Close)
	client, err := peersession.NewOwner(peersession.Config{Local: remote, Provisioned: provisions, Versions: versions, Methods: []peersession.Method{method}})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	projection := peersession.Group{ID: 1, Generation: 1, Configuration: configuration, Minimum: versions,
		State: peersession.Intact, Stage: peersession.IdentityReady, Pending: peersession.PendingClear, MarkerReady: true}
	require.NoError(t, server.UpdateGroup(projection))
	require.NoError(t, client.UpdateGroup(projection))
	f := &registrationBindingFixture{server: server, client: client, service: &registrationBindingService{}}
	options, err := server.ServerOptions(&tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{serverCert}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert})
	require.NoError(t, err)
	gs := grpc.NewServer(options...)
	server.Register(gs)
	pb.RegisterEncryptionAdminServer(gs, f.service)
	done := make(chan struct{})
	go func() { defer close(done); _ = gs.Serve(lis) }()
	t.Cleanup(func() { gs.Stop(); _ = lis.Close(); <-done })
	credentials, err := client.TransportCredentials(&tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{clientCert}, RootCAs: roots, ServerName: "binding.test"}, false)
	require.NoError(t, err)
	f.conn, err = grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(credentials))
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.conn.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	f.handle, err = client.Handshake(ctx, f.conn, 1, local)
	require.NoError(t, err)
	return f
}

func registrationBindingMethod(missing bool, traffic peersession.Traffic) peersession.Method {
	return peersession.Method{Name: pb.EncryptionAdmin_RegisterEncryptionWriter_FullMethodName,
		BoundRequest: func(binding peersession.RequestBinding, payload any) (peersession.Message, error) {
			req, ok := payload.(*pb.RegisterEncryptionWriterRequest)
			if !ok || req.GetDekId() == 0 || len(req.GetWriters()) != 1 || req.GetWriters()[0].GetFullNodeId() != binding.From.Member || req.GetWriters()[0].GetLocalEpoch() > 65535 {
				return peersession.Message{}, peersession.ErrDenied
			}
			message := peersession.Message{Group: binding.Group, From: binding.From.Member, To: binding.To.Member, Traffic: traffic}
			if !missing {
				message.Declaration = sha256.Sum256(registrationBindingPlain(req))
			}
			return message, nil
		}, Reply: func(message peersession.Message, _ any) (peersession.Message, error) {
			message.From, message.To = message.To, message.From
			return message, nil
		}}
}

func (f *registrationBindingFixture) admission(t *testing.T, inspector func(Kind, uint64, []byte, raftbootstrap.Digest) (PayloadInspection, error)) {
	t.Helper()
	var err error
	f.a, err = New(Config{Cluster: raftbootstrap.UUID{1}, DefaultGroup: 1, Groups: []uint64{1},
		ValidatePayload: func(Kind, uint64, []byte) error { return ErrClosed }, InspectPayload: inspector})
	require.NoError(t, err)
	t.Cleanup(f.a.Close)
	observeAdmission(t, f.a, 1, 1)
	probe := preparedAdmissionProbe(t)
	ticket, err := f.a.Ticket(1)
	require.NoError(t, err)
	require.NoError(t, f.a.PublishPreparedWith(ticket, probe, raftbootstrap.Digest{3}, func(peersession.Stage, uint64) (func(), error) { return nil, nil }))
	require.True(t, f.a.NeedsActivation(1))
	require.Equal(t, peersession.IdentityReady, f.a.Stage(1))
}

type registrationBindingCase struct {
	name                                                                                                              string
	plainMismatch, missingDeclaration, forged, missingSession, staleGeneration, invalidate, plaintextSeal, substitute bool
	traffic                                                                                                           peersession.Traffic
	kind                                                                                                              Kind
	accepted                                                                                                          bool
}

func TestRegistrationBindingEncryptedReceiptAndFinalSeal(t *testing.T) {
	for _, tc := range []registrationBindingCase{
		{name: "exact encrypted declaration", accepted: true},
		{name: "different plaintext declaration", plainMismatch: true},
		{name: "missing declaration", missingDeclaration: true},
		{name: "forged receipt", forged: true},
		{name: "missing session receipt", missingSession: true},
		{name: "different traffic", traffic: peersession.PolicyControl},
		{name: "registration receipt other kind", kind: Recovery},
		{name: "stale projection generation", staleGeneration: true},
		{name: "projection revoked during inspection", invalidate: true},
		{name: "plaintext cannot replace ciphertext seal", plaintextSeal: true},
		{name: "different ciphertext same plaintext", substitute: true},
	} {
		t.Run(tc.name, func(t *testing.T) { testRegistrationBindingCase(t, tc) })
	}
}

func testRegistrationBindingCase(t *testing.T, tc registrationBindingCase) {
	t.Helper()
	traffic, kind := tc.traffic, tc.kind
	if traffic == 0 {
		traffic = peersession.Registration
	}
	if kind == 0 {
		kind = Registration
	}
	f := newRegistrationBindingFixture(t, tc.missingDeclaration, traffic)
	req := &pb.RegisterEncryptionWriterRequest{DekId: 7, Writers: []*pb.WriterRegistryEntry{{FullNodeId: 2, LocalEpoch: 3}}}
	plain := registrationBindingPlain(req)
	if tc.plainMismatch {
		plain[len(plain)-1]++
	}
	final := registrationBindingCiphertext(t, plain)
	second := registrationBindingCiphertext(t, plain)
	f.admission(t, registrationBindingInspector(f, tc, final, plain))
	f.service.action = registrationBindingAction(f, tc, kind, final, plain, second)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var reply pb.RegisterEncryptionWriterResponse
	var err error
	if tc.missingSession {
		err = f.conn.Invoke(ctx, pb.EncryptionAdmin_RegisterEncryptionWriter_FullMethodName, req, &reply)
	} else {
		err = f.handle.Invoke(ctx, f.conn, pb.EncryptionAdmin_RegisterEncryptionWriter_FullMethodName, req, &reply)
	}
	assertRegistrationBindingResult(t, f, tc, err)
}

func registrationBindingAction(f *registrationBindingFixture, tc registrationBindingCase, kind Kind, final, plain, second []byte) func(context.Context) error {
	return func(ctx context.Context) error {
		if tc.forged {
			ctx = registrationForgedReceiptContext{Context: ctx}
		}
		owned, operation, err := f.a.Begin(ctx, kind)
		if err != nil {
			return err
		}
		defer operation.CancelAndJoin()
		bound, err := f.a.BindProposal(owned, 1, final)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(final)
		if tc.plaintextSeal {
			digest = sha256.Sum256(plain)
		}
		if tc.substitute {
			digest = sha256.Sum256(second)
		}
		return f.a.WithNative(bound, 1, func(scope raftengine.NativeScope) error {
			f.nativeChecks.Add(1)
			return scope.Check(bound, raftengine.NativeRequest{Operation: raftengine.NativeProposal, PayloadDigest: digest})
		})
	}
}

func assertRegistrationBindingResult(t *testing.T, f *registrationBindingFixture, tc registrationBindingCase, err error) {
	t.Helper()
	if tc.accepted {
		require.NoError(t, err)
	} else {
		require.Error(t, err)
	}
	if !tc.forged && !tc.missingSession {
		require.EqualValues(t, 1, f.inspections.Load(), "must reach the trusted inspector")
	}
	if tc.accepted || tc.plaintextSeal || tc.substitute {
		require.EqualValues(t, 1, f.nativeChecks.Load(), "must reach final native seal validation")
	} else {
		require.Zero(t, f.nativeChecks.Load())
	}
}

func registrationBindingInspector(f *registrationBindingFixture, tc registrationBindingCase, final, plain []byte) func(Kind, uint64, []byte, raftbootstrap.Digest) (PayloadInspection, error) {
	return func(_ Kind, _ uint64, payload []byte, _ raftbootstrap.Digest) (PayloadInspection, error) {
		f.inspections.Add(1)
		if !bytes.Equal(payload, final) {
			return PayloadInspection{}, ErrClosed
		}
		generation, err := f.a.EncryptionFence()
		if err != nil {
			return PayloadInspection{}, err
		}
		if tc.staleGeneration {
			generation++
		}
		if tc.invalidate {
			f.a.InvalidateEncryption()
		}
		return PayloadInspection{PlaintextDigest: sha256.Sum256(plain), ProjectionGeneration: generation}, nil
	}
}

type registrationForgedReceiptContext struct{ context.Context }

func (c registrationForgedReceiptContext) Value(key any) any {
	value := c.Context.Value(key)
	if _, ok := value.(*peersession.Receipt); ok {
		return &peersession.Receipt{}
	}
	return value
}
