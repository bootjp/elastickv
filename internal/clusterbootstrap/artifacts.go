package clusterbootstrap

import (
	"bytes"
	"crypto"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"io"
	"strconv"
	"unicode/utf8"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
)

const BootstrapPreparedAckLimit = 64 << 10

// The digest must come only from the trusted native final-preparation verifier.
// This pure signer neither inspects disk nor establishes source ownership.
func SignBootstrapPreparedAck(config *BootstrapPeerConfig, manifest raftbootstrap.BaselineManifest, memberID uint64, verifiedStateDigest raftbootstrap.Digest) (raftbootstrap.PreparedMemberAck, error) {
	if err := validateBootstrapPreparedSigner(config, manifest, memberID); err != nil {
		return raftbootstrap.PreparedMemberAck{}, err
	}
	signer, ok := config.ClientTLS.Certificates[0].PrivateKey.(crypto.Signer)
	if !ok {
		return raftbootstrap.PreparedMemberAck{}, errors.New("prepared acknowledgment requires a TLS crypto signer")
	}
	der, err := x509.MarshalPKIXPublicKey(signer.Public())
	if err != nil || sha256.Sum256(der) != config.Local.Key {
		return raftbootstrap.PreparedMemberAck{}, errors.New("prepared signer differs from pinned local identity")
	}
	digest, err := raftbootstrap.BaselineManifestDigest(manifest)
	if err != nil {
		return raftbootstrap.PreparedMemberAck{}, errors.WithStack(err)
	}
	ack, err := raftbootstrap.SignPreparedMemberAck(raftbootstrap.PreparedMemberAck{
		MemberID: memberID, ManifestDigest: digest, Minimum: manifest.Minimum, PreparedStateDigest: verifiedStateDigest,
	}, manifest, signer)
	return ack, errors.WithStack(err)
}

func validateBootstrapPreparedSigner(config *BootstrapPeerConfig, manifest raftbootstrap.BaselineManifest, memberID uint64) error {
	if config == nil || config.ClientTLS == nil || len(config.ClientTLS.Certificates) == 0 {
		return errors.New("prepared acknowledgment requires configured local TLS identity")
	}
	if memberID == 0 || config.Local.Member != memberID || config.Local.Cluster != manifest.Identity.Cluster ||
		manifest.Minimum != (raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}) {
		return errors.New("prepared acknowledgment identity or format floor mismatch")
	}
	return validateBootstrapPreparedRoster(config, manifest, memberID)
}

func validateBootstrapPreparedRoster(config *BootstrapPeerConfig, manifest raftbootstrap.BaselineManifest, memberID uint64) error {
	nodes := bootstrapSessionNodes(config, manifest.Identity.Group)
	if len(nodes) != len(manifest.Configuration.Members) || nodes[memberID] != config.Local {
		return errors.New("prepared acknowledgment requires the pinned local roster")
	}
	for _, member := range manifest.Configuration.Members {
		node := nodes[member.ID]
		if node.Cluster != manifest.Identity.Cluster || node.Member != member.ID || node.Address != member.Address || node.Key != member.PublicKeyDigest {
			return errors.New("prepared acknowledgment manifest differs from pinned roster")
		}
	}
	return nil
}

// JSON carries public SPKI/signature bytes only. Member IDs are decimal strings
// to preserve uint64 precision in operator tooling; digests are lowercase hex.
type BootstrapPreparedAckJSON struct {
	Version       uint32                       `json:"version"`
	MemberID      string                       `json:"member_id"`
	Manifest      string                       `json:"manifest_sha256"`
	Minimum       BootstrapBaselineMinimumJSON `json:"minimum"`
	PreparedState string                       `json:"prepared_state_sha256"`
	PublicKeyDER  []byte                       `json:"public_key_der"`
	Signature     []byte                       `json:"signature"`
}

func MarshalBootstrapPreparedAck(ack raftbootstrap.PreparedMemberAck, manifest raftbootstrap.BaselineManifest) ([]byte, error) {
	if err := raftbootstrap.VerifyPreparedMemberAck(ack, manifest); err != nil {
		return nil, errors.WithStack(err)
	}
	raw, err := json.Marshal(BootstrapPreparedAckJSON{Version: 1, MemberID: strconv.FormatUint(ack.MemberID, 10),
		Manifest: hex.EncodeToString(ack.ManifestDigest[:]), PreparedState: hex.EncodeToString(ack.PreparedStateDigest[:]),
		Minimum:      BootstrapBaselineMinimumJSON{Entry: ack.Minimum.Entry, Snapshot: ack.Minimum.Snapshot, Session: ack.Minimum.Session},
		PublicKeyDER: ack.PublicKeyDER, Signature: ack.Signature})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(raw) > BootstrapPreparedAckLimit {
		return nil, errors.New("prepared acknowledgment exceeds size limit")
	}
	return raw, nil
}

func ParseBootstrapPreparedAck(data []byte, manifest raftbootstrap.BaselineManifest) (raftbootstrap.PreparedMemberAck, error) {
	var empty raftbootstrap.PreparedMemberAck
	if err := validateBootstrapPreparedAckJSON(data); err != nil {
		return empty, err
	}
	var raw BootstrapPreparedAckJSON
	if err := json.Unmarshal(data, &raw); err != nil || raw.Version != 1 {
		return empty, errors.New("invalid prepared acknowledgment schema/version")
	}
	ack := raftbootstrap.PreparedMemberAck{Minimum: raftbootstrap.Versions{
		Entry: raw.Minimum.Entry, Snapshot: raw.Minimum.Snapshot, Session: raw.Minimum.Session},
		PublicKeyDER: raw.PublicKeyDER, Signature: raw.Signature}
	var err error
	ack.MemberID, err = bootstrapBaselineUint(raw.MemberID)
	if err != nil {
		return empty, err
	}
	ack.ManifestDigest, err = bootstrapBaselineDigest(raw.Manifest)
	if err != nil {
		return empty, err
	}
	ack.PreparedStateDigest, err = bootstrapBaselineDigest(raw.PreparedState)
	if err != nil {
		return empty, err
	}
	if err = raftbootstrap.VerifyPreparedMemberAck(ack, manifest); err != nil {
		return empty, errors.WithStack(err)
	}
	return ack, nil
}

func validateBootstrapPreparedAckJSON(data []byte) error {
	if len(data) == 0 || len(data) > BootstrapPreparedAckLimit || !utf8.Valid(data) {
		return errors.New("invalid prepared acknowledgment size/encoding")
	}
	fields, err := bootstrapPreparedJSONObject(data, []string{
		"version", bootstrapMemberIDJSONField, "manifest_sha256", "minimum", "prepared_state_sha256", "public_key_der", "signature",
	})
	if err != nil {
		return err
	}
	_, err = bootstrapPreparedJSONObject(fields["minimum"], []string{"entry", "snapshot", "session"})
	return err
}

// Decode each field separately so duplicate/escaped aliases, case aliases,
// omissions and null cannot be silently accepted by encoding/json's struct API.
func bootstrapPreparedJSONObject(data []byte, keys []string) (map[string]json.RawMessage, error) {
	d := json.NewDecoder(bytes.NewReader(data))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errors.New("prepared acknowledgment requires a JSON object")
	}
	fields := make(map[string]json.RawMessage, len(keys))
	for d.More() {
		if err := bootstrapPreparedJSONField(d, fields, keys); err != nil {
			return nil, err
		}
	}
	if _, err := d.Token(); err != nil {
		return nil, errors.Wrap(err, "close prepared acknowledgment object")
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) || len(fields) != len(keys) {
		return nil, errors.New("trailing or missing prepared acknowledgment fields")
	}
	return fields, nil
}

func bootstrapPreparedJSONField(d *json.Decoder, fields map[string]json.RawMessage, keys []string) error {
	token, err := d.Token()
	if err != nil {
		return errors.Wrap(err, "read prepared acknowledgment key")
	}
	key, ok := token.(string)
	if !ok || !bootstrapPreparedJSONKnownKey(key, keys) || fields[key] != nil {
		return errors.New("unknown or duplicate prepared acknowledgment field")
	}
	var value json.RawMessage
	if err := d.Decode(&value); err != nil {
		return errors.Wrap(err, "read prepared acknowledgment value")
	}
	if bytes.Equal(value, []byte("null")) {
		return errors.New("null prepared acknowledgment field")
	}
	fields[key] = value
	return nil
}

func bootstrapPreparedJSONKnownKey(key string, keys []string) bool {
	for _, known := range keys {
		if key == known {
			return true
		}
	}
	return false
}

// expected must be independently supplied by the operator, never extracted
// from manifest. This assembles W0 only; it cannot publish a policy or readiness.
func AssembleBootstrapActivation(manifest raftbootstrap.BaselineManifest, expected raftbootstrap.BaselineContext, acknowledgments [][]byte) ([]byte, error) {
	if err := raftbootstrap.VerifyBaselineManifest(manifest, expected); err != nil {
		return nil, errors.WithStack(err)
	}
	if len(acknowledgments) != len(manifest.Configuration.Members) {
		return nil, errors.New("activation requires every pinned member acknowledgment")
	}
	digest, err := raftbootstrap.BaselineManifestDigest(manifest)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	ready := raftbootstrap.PreparedCertificate{ManifestDigest: digest}
	remaining := raftbootstrap.MaxFileSize
	for _, raw := range acknowledgments {
		if len(raw) > remaining {
			return nil, errors.New("activation acknowledgments exceed size limit")
		}
		remaining -= len(raw)
		ack, err := ParseBootstrapPreparedAck(raw, manifest)
		if err != nil {
			return nil, err
		}
		ready.Members = append(ready.Members, ack)
	}
	configuration, err := raftbootstrap.ConfigurationDigest(expected.Configuration)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	encoded, err := raftbootstrap.EncodeActivationCommand(raftbootstrap.ActivationCommand{
		Manifest: manifest, Readiness: ready, Witness: raftbootstrap.Witness{
			Cluster: expected.Identity.Cluster, Group: expected.Identity.Group,
			ConfigurationIndex: expected.Configuration.Index, ConfigurationDigest: configuration,
		},
	})
	return encoded, errors.WithStack(err)
}
