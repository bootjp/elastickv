package clusterbootstrap

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"unicode/utf8"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/cockroachdb/errors"
)

const (
	BootstrapArtifactAttest   = "attest"
	BootstrapArtifactCertify  = "certify"
	bootstrapArtifactFileMode = 0o600
)

type bootstrapArtifactCommand struct {
	BootstrapBaselineCLIRequest
	PeerConfig, Output string
	Acknowledgments    []string
}

func DispatchBootstrapArtifactCommand(args []string, output io.Writer) (handled bool, err error) {
	if len(args) == 0 || (args[0] != BootstrapArtifactAttest && args[0] != BootstrapArtifactCertify) {
		return false, nil
	}
	request, err := ParseBootstrapArtifactCommand(args)
	if err != nil {
		return true, err
	}
	budget := BootstrapArtifactBudget(raftbootstrap.MaxFileSize)
	expected, manifest, err := readBootstrapArtifactContext(request, &budget)
	if err != nil {
		return true, err
	}
	if request.Command == BootstrapArtifactCertify {
		return true, certifyBootstrapArtifact(request, expected, manifest, &budget)
	}
	return true, executeBootstrapAttestCommand(request, expected, manifest, &budget, output)
}

func executeBootstrapAttestCommand(request bootstrapArtifactCommand, expected raftbootstrap.BaselineContext, manifest raftbootstrap.BaselineManifest, budget *BootstrapArtifactBudget, output io.Writer) error {
	if output == nil {
		return errors.New("attest requires an output writer")
	}
	config, err := ReadBootstrapArtifactPeer(request.PeerConfig, budget)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	ack, err := AttestBootstrapArtifact(ctx, request.BootstrapBaselineCLIRequest, expected, manifest, config)
	if err != nil {
		return err
	}
	n, err := output.Write(ack)
	if err == nil && n != len(ack) {
		err = io.ErrShortWrite
	}
	return errors.WithStack(err)
}

func ParseBootstrapArtifactCommand(args []string) (bootstrapArtifactCommand, error) {
	var request bootstrapArtifactCommand
	if err := bootstrapBaselineArgsBound(args); err != nil {
		return request, err
	}
	if args[0] != BootstrapArtifactAttest && args[0] != BootstrapArtifactCertify {
		return request, errors.New("unsupported bootstrap artifact command")
	}
	request.Command = args[0]
	operation, member, err := parseBootstrapArtifactFlags(args[1:], &request)
	if err != nil {
		return request, err
	}
	request.OperationID, err = parseBootstrapBaselineOperationID(operation)
	if err != nil {
		return request, err
	}
	if request.Command == BootstrapArtifactAttest {
		request.MemberID, err = bootstrapBaselineUint(member)
		if err != nil || request.MemberID == 0 {
			return request, errors.New("invalid attesting member")
		}
	}
	return request, validateBootstrapArtifactPaths(request)
}

func parseBootstrapArtifactFlags(args []string, request *bootstrapArtifactCommand) (operation, member string, err error) {
	for _, arg := range args {
		if arg == "--" {
			return "", "", errors.New("artifact command does not accept a trailing option separator")
		}
	}
	fs := flag.NewFlagSet("bootstrap "+request.Command, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	if request.Command == BootstrapArtifactAttest {
		request.Encryption.flags(fs)
	}
	values := map[string]*string{"expected-config": &request.ExpectedConfigPath, "manifest": &request.ManifestPath, "operation-id": &operation}
	if request.Command == BootstrapArtifactAttest {
		values["raft-data-dir"], values["member-id"], values["peer-config"] = &request.RaftDataDir, &member, &request.PeerConfig
	} else {
		values["output"] = &request.Output
		fs.Func("ack", "per-member ready acknowledgment", func(value string) error {
			request.Acknowledgments = append(request.Acknowledgments, value)
			return nil
		})
	}
	seen := map[string]bool{}
	for name, target := range values {
		fs.Func(name, "required artifact input", func(value string) error {
			if seen[name] {
				return errors.New("duplicate artifact option")
			}
			seen[name], *target = true, value
			return nil
		})
	}
	if err := fs.Parse(args); err != nil {
		return "", "", errors.New("invalid artifact options")
	}
	if fs.NArg() != 0 || len(seen) != len(values) {
		return "", "", errors.New("artifact command requires exact named inputs and no positional arguments")
	}
	return operation, member, nil
}

func validateBootstrapArtifactPaths(request bootstrapArtifactCommand) error {
	if request.Command != BootstrapArtifactAttest && request.Encryption != (BootstrapOfflineEncryptionCLI{}) {
		return errors.New("artifact-only certify does not accept encryption options")
	}
	if err := request.Encryption.validate(); err != nil {
		return err
	}
	paths := []string{request.ExpectedConfigPath, request.ManifestPath}
	paths = append(paths, request.Encryption.paths()...)
	if request.Command == BootstrapArtifactAttest {
		paths = append(paths, request.RaftDataDir, request.PeerConfig)
	} else {
		if len(request.Acknowledgments) == 0 {
			return errors.New("certify requires acknowledgment files")
		}
		paths = append(paths, request.Output)
		paths = append(paths, request.Acknowledgments...)
	}
	seen := map[string]bool{}
	for _, path := range paths {
		if !bootstrapBaselineValidPath(path) || seen[path] {
			return errors.New("artifact paths must be distinct canonical absolute paths")
		}
		seen[path] = true
	}
	return nil
}

// A single budget covers config, manifest, all ack files, and signer TLS inputs.
// Read the opened file with the remaining limit even if its stat size races.
type BootstrapArtifactBudget int

func (b *BootstrapArtifactBudget) Read(path string) ([]byte, error) {
	if !bootstrapBaselineValidPath(path) {
		return nil, errors.New("artifact input requires a canonical absolute path")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "open artifact input")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat artifact input")
	}
	if !info.Mode().IsRegular() || info.Size() > int64(*b) {
		return nil, errors.New("artifact input exceeds aggregate bound or is not regular")
	}
	data, err := io.ReadAll(io.LimitReader(f, int64(*b)+1))
	if err != nil {
		return nil, errors.Wrap(err, "read artifact input")
	}
	if len(data) > int(*b) {
		return nil, errors.New("artifact inputs exceed aggregate 16 MiB bound")
	}
	*b -= BootstrapArtifactBudget(len(data))
	return data, nil
}

func readBootstrapArtifactContext(request bootstrapArtifactCommand, budget *BootstrapArtifactBudget) (raftbootstrap.BaselineContext, raftbootstrap.BaselineManifest, error) {
	var expected raftbootstrap.BaselineContext
	var manifest raftbootstrap.BaselineManifest
	expectedRaw, err := budget.Read(request.ExpectedConfigPath)
	if err != nil {
		return expected, manifest, err
	}
	expected, err = ParseBootstrapBaselineExpectedConfig(expectedRaw, request.OperationID)
	if err != nil {
		return expected, manifest, err
	}
	manifestRaw, err := budget.Read(request.ManifestPath)
	if err != nil {
		return expected, manifest, err
	}
	manifest, err = raftbootstrap.ParseBaselineManifest(manifestRaw)
	if err != nil {
		return expected, manifest, errors.WithStack(err)
	}
	return expected, manifest, errors.WithStack(raftbootstrap.VerifyBaselineManifest(manifest, expected))
}

func AttestBootstrapArtifact(ctx context.Context, request BootstrapBaselineCLIRequest, expected raftbootstrap.BaselineContext, manifest raftbootstrap.BaselineManifest, config *BootstrapPeerConfig) (ack []byte, err error) {
	if err := raftbootstrap.VerifyBaselineManifest(manifest, expected); err != nil {
		return nil, errors.WithStack(err)
	}
	ownership, err := etcd.AcquireOfflineBaselineOwnership(request.RaftDataDir)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer func() {
		err = errors.Join(err, ownership.Close())
		if err != nil {
			ack = nil
		}
	}()
	options, err := bootstrapOfflineOptionsWithContext(ctx, request, expected, manifest, ownership)
	if err != nil {
		return nil, err
	}
	digest, err := etcd.VerifyBoundOfflineBaseline(ctx, options, manifest)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.WithStack(err)
	}
	prepared, err := SignBootstrapPreparedAck(config, manifest, request.MemberID, digest)
	if err != nil {
		return nil, err
	}
	return MarshalBootstrapPreparedAck(prepared, manifest)
}

func certifyBootstrapArtifact(request bootstrapArtifactCommand, expected raftbootstrap.BaselineContext, manifest raftbootstrap.BaselineManifest, budget *BootstrapArtifactBudget) error {
	if len(request.Acknowledgments) != len(manifest.Configuration.Members) {
		return errors.New("certify requires exactly every pinned member acknowledgment")
	}
	acks := make([][]byte, 0, len(request.Acknowledgments))
	for _, path := range request.Acknowledgments {
		raw, err := budget.Read(path)
		if err != nil {
			return err
		}
		acks = append(acks, raw)
	}
	encoded, err := AssembleBootstrapActivation(manifest, expected, acks)
	if err != nil {
		return err
	}
	return writeBootstrapArtifactExclusive(request.Output, encoded)
}

// Failed/ambiguous output is deliberately retained. Never remove or overwrite
// an operator artifact, even when a write, sync or close fails.
func writeBootstrapArtifactExclusive(path string, data []byte) (err error) {
	if !bootstrapBaselineValidPath(path) {
		return errors.New("artifact output requires a canonical absolute path")
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return errors.Wrap(err, "open artifact output directory")
	}
	defer func() { err = errors.Join(err, dir.Close()) }()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, bootstrapArtifactFileMode)
	if err != nil {
		return errors.Wrap(err, "create exclusive artifact output")
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	if _, err := io.Copy(f, bytes.NewReader(data)); err != nil {
		return errors.Wrap(err, "write artifact output")
	}
	if err := f.Sync(); err != nil {
		return errors.Wrap(err, "sync artifact output")
	}
	return errors.Wrap(dir.Sync(), "sync artifact output directory")
}

// Only load the offline signer/roster. Runtime startup tokens and activation
// files are not authority for attestation and are intentionally not read here.
func ReadBootstrapArtifactPeer(path string, budget *BootstrapArtifactBudget) (*BootstrapPeerConfig, error) {
	data, err := budget.Read(path)
	if err != nil {
		return nil, err
	}
	raw, err := ParseBootstrapArtifactPeer(data)
	if err != nil {
		return nil, err
	}
	config, err := bootstrapArtifactPeerRoster(raw)
	if err != nil {
		return nil, err
	}
	certPEM, err := budget.Read(raw.Cert)
	if err != nil {
		return nil, err
	}
	keyPEM, err := budget.Read(raw.Key)
	if err != nil {
		return nil, err
	}
	caPEM, err := budget.Read(raw.CA)
	if err != nil {
		return nil, err
	}
	config.ClientTLS, err = bootstrapArtifactSignerTLS(config, certPEM, keyPEM, caPEM)
	return config, err
}

func ParseBootstrapArtifactPeer(data []byte) (BootstrapPeerJSON, error) {
	var raw BootstrapPeerJSON
	if !utf8.Valid(data) {
		return raw, errors.New("invalid peer config encoding")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	if err := scanBootstrapJSON(d, 0); err != nil {
		return raw, err
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) {
		return raw, errors.New("trailing peer config data")
	}
	d = json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&raw); err != nil {
		return raw, errors.Wrap(err, "decode artifact peer config")
	}
	return raw, nil
}

func bootstrapArtifactPeerRoster(raw BootstrapPeerJSON) (*BootstrapPeerConfig, error) {
	cluster, err := raftbootstrap.ParseUUID(raw.Cluster)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	config := &BootstrapPeerConfig{}
	ids, addresses, keys := map[uint64]bool{}, map[string]bool{}, map[raftbootstrap.Digest]bool{}
	for _, member := range raw.Members {
		provision, err := bootstrapProvision(cluster, member)
		if err != nil {
			return nil, err
		}
		if bootstrapDuplicatePeer(ids[member.ID], addresses[member.Address], keys[provision.Node.Key]) {
			return nil, errors.New("duplicate artifact peer identity")
		}
		ids[member.ID], addresses[member.Address], keys[provision.Node.Key] = true, true, true
		config.Provisions = append(config.Provisions, provision)
		if member.ID == raw.Local {
			config.Local = provision.Node
		}
	}
	if config.Local.Member == 0 {
		return nil, errors.New("artifact local member not provisioned")
	}
	return config, nil
}

func bootstrapArtifactSignerTLS(config *BootstrapPeerConfig, certPEM, keyPEM, caPEM []byte) (*tls.Config, error) {
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, errors.Wrap(err, "load artifact signing certificate")
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if err := bootstrapVerifyLocalCertificate(leaf, config.Local); err != nil {
		return nil, err
	}
	roots, intermediates := x509.NewCertPool(), x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("artifact CA contains no certificates")
	}
	for _, der := range cert.Certificate[1:] {
		parsed, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		intermediates.AddCert(parsed)
	}
	for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth} {
		if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{usage}}); err != nil {
			return nil, errors.Wrap(err, "verify artifact signing certificate")
		}
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, RootCAs: roots}, nil
}
