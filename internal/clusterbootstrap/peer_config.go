package clusterbootstrap

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/nodeauth"
	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
)

const BootstrapPeerConfigLimit = 16 << 20
const bootstrapPeerMaxDepth = 4
const bootstrapMemberIDJSONField = "member_id"

type BootstrapPeerConfig struct {
	Local                peersession.Node
	Provisions           []peersession.Provision
	ServerTLS, ClientTLS *tls.Config
	StartupToken         string
	Activation           []byte
}

type BootstrapPeerJSON struct {
	Cluster          string                    `json:"cluster_uuid"`
	Local            uint64                    `json:"local_member"`
	Members          []BootstrapPeerMemberJSON `json:"members"`
	CA               string                    `json:"ca_file"`
	Cert             string                    `json:"cert_file"`
	Key              string                    `json:"key_file"`
	StartupTokenFile string                    `json:"startup_token_file"`
	ActivationFile   string                    `json:"activation_file"`
}

type BootstrapPeerMemberJSON struct {
	ID      uint64   `json:"member_id"`
	Address string   `json:"address"`
	Key     string   `json:"public_key_sha256"`
	Groups  []uint64 `json:"groups"`
}

func LoadBootstrapPeerConfig(path string) (*BootstrapPeerConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "open peer config")
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, BootstrapPeerConfigLimit+1))
	if err != nil {
		return nil, errors.Wrap(err, "read peer config")
	}
	if len(data) > BootstrapPeerConfigLimit {
		return nil, errors.New("peer config exceeds 16 MiB")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	if err = scanBootstrapJSON(d, 0); err != nil {
		return nil, err
	}
	if _, err = d.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing peer config data")
	}
	d = json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	var raw BootstrapPeerJSON
	if err = d.Decode(&raw); err != nil {
		return nil, errors.Wrap(err, "decode peer config")
	}
	return BuildBootstrapPeerConfig(raw)
}

// Inspect object keys before struct decoding, which otherwise accepts duplicate
// and case-insensitive field names. The schema has at most three container levels.
func scanBootstrapJSON(d *json.Decoder, depth int) error {
	if depth > bootstrapPeerMaxDepth {
		return errors.New("peer config nesting too deep")
	}
	t, err := d.Token()
	if err != nil {
		return errors.Wrap(err, "read JSON token")
	}
	delim, container := t.(json.Delim)
	if !container {
		return nil
	}
	seen := map[string]bool{}
	for d.More() {
		if delim == '{' {
			if err = bootstrapReadKey(d, seen); err != nil {
				return err
			}
		}
		if err = scanBootstrapJSON(d, depth+1); err != nil {
			return err
		}
	}
	_, err = d.Token()
	return errors.Wrap(err, "close JSON container")
}

func bootstrapReadKey(d *json.Decoder, seen map[string]bool) error {
	key, err := d.Token()
	if err != nil {
		return errors.Wrap(err, "read JSON key")
	}
	name, ok := key.(string)
	if !ok || seen[name] || !bootstrapJSONKey(name) {
		return errors.New("unknown or duplicate peer config field")
	}
	seen[name] = true
	return nil
}

func bootstrapJSONKey(key string) bool {
	switch key {
	case "cluster_uuid", "local_member", "members", "ca_file", "cert_file", "key_file", "startup_token_file", "activation_file", bootstrapMemberIDJSONField, "address", "public_key_sha256", "groups":
		return true
	default:
		return false
	}
}

func BuildBootstrapPeerConfig(raw BootstrapPeerJSON) (*BootstrapPeerConfig, error) {
	cluster, err := raftbootstrap.ParseUUID(raw.Cluster)
	if err != nil {
		return nil, errors.Wrap(err, "parse cluster UUID")
	}
	if raw.Local == 0 || len(raw.Members) == 0 {
		return nil, errors.New("missing peer identity")
	}
	out := &BootstrapPeerConfig{}
	ids := map[uint64]bool{}
	addresses := map[string]bool{}
	keys := map[raftbootstrap.Digest]bool{}
	for _, m := range raw.Members {
		p, e := bootstrapProvision(cluster, m)
		if e != nil {
			return nil, e
		}
		if bootstrapDuplicatePeer(ids[m.ID], addresses[m.Address], keys[p.Node.Key]) {
			return nil, errors.New("duplicate peer identity")
		}
		ids[m.ID], addresses[m.Address], keys[p.Node.Key] = true, true, true
		out.Provisions = append(out.Provisions, p)
		if m.ID == raw.Local {
			out.Local = p.Node
		}
	}
	if out.Local.Member == 0 {
		return nil, errors.New("local member not provisioned")
	}
	if err = out.loadRuntimeCredentials(raw); err != nil {
		return nil, err
	}
	return out, nil
}

func (out *BootstrapPeerConfig) loadRuntimeCredentials(raw BootstrapPeerJSON) error {
	if err := out.loadTLS(raw); err != nil {
		return err
	}
	if raw.StartupTokenFile != "" {
		token, err := internalutil.LoadBearerTokenFile(raw.StartupTokenFile, nodeauth.AdminTokenMaxBytes, "startup token")
		if err != nil {
			return errors.Wrap(err, "load protected startup token")
		}
		out.StartupToken = token
	}
	if raw.ActivationFile != "" {
		data, err := readBootstrapOperatorInput(raw.ActivationFile)
		if err != nil {
			return err
		}
		if _, matched, err := raftbootstrap.ParseActivationCommand(data); err != nil || !matched {
			return errors.Join(raftbootstrap.ErrInvalid, err)
		}
		out.Activation = data
	}
	return nil
}

func bootstrapDuplicatePeer(id, address, key bool) bool { return id || address || key }

func bootstrapProvision(cluster raftbootstrap.UUID, m BootstrapPeerMemberJSON) (peersession.Provision, error) {
	var p peersession.Provision
	key, err := hex.DecodeString(m.Key)
	if err != nil || len(key) != sha256.Size {
		return p, errors.New("invalid peer public key digest")
	}
	p.Node = peersession.Node{Cluster: cluster, Member: m.ID, Address: m.Address}
	copy(p.Node.Key[:], key)
	if m.ID == 0 || p.Node.Key == (raftbootstrap.Digest{}) || !bootstrapValidAddress(m.Address) {
		return p, errors.New("invalid peer identity/address")
	}
	if len(m.Groups) == 0 {
		return p, errors.New("missing peer groups")
	}
	seen := map[uint64]bool{}
	for _, group := range m.Groups {
		if seen[group] {
			return p, errors.New("duplicate peer group")
		}
		seen[group] = true
	}
	p.Groups = append([]uint64(nil), m.Groups...)
	return p, nil
}

func bootstrapValidAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	n, portErr := strconv.ParseUint(port, 10, 16)
	return err == nil && host != "" && portErr == nil && n != 0 && utf8.ValidString(address) && !strings.ContainsAny(address, " \t\r\n\x00")
}

func (c *BootstrapPeerConfig) loadTLS(raw BootstrapPeerJSON) error {
	ca, err := os.ReadFile(raw.CA)
	if err != nil {
		return errors.Wrap(err, "read peer CA")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return errors.New("peer CA contains no certificates")
	}
	cert, err := tls.LoadX509KeyPair(raw.Cert, raw.Key)
	if err != nil {
		return errors.Wrap(err, "load peer key pair")
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return errors.Wrap(err, "parse local certificate")
	}
	if err = bootstrapVerifyLocalCertificate(leaf, c.Local); err != nil {
		return err
	}
	intermediates := x509.NewCertPool()
	for _, der := range cert.Certificate[1:] {
		parsed, e := x509.ParseCertificate(der)
		if e != nil {
			return errors.Wrap(e, "parse intermediate certificate")
		}
		intermediates.AddCert(parsed)
	}
	for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth} {
		if _, err = leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{usage}}); err != nil {
			return errors.Wrap(err, "verify local certificate")
		}
	}
	c.ServerTLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, ClientCAs: roots, ClientAuth: tls.VerifyClientCertIfGiven}
	c.ClientTLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, RootCAs: roots}
	return nil
}

func bootstrapVerifyLocalCertificate(leaf *x509.Certificate, local peersession.Node) error {
	if sha256.Sum256(leaf.RawSubjectPublicKeyInfo) != local.Key {
		return errors.New("local certificate SPKI does not match provisioned identity")
	}
	host, _, err := net.SplitHostPort(local.Address)
	if err != nil {
		return errors.Wrap(err, "parse local certificate address")
	}
	return errors.Wrap(leaf.VerifyHostname(host), "verify local certificate hostname")
}
