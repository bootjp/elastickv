package raftbootstrap

import (
	"bytes"
	"encoding/binary"

	"github.com/cockroachdb/errors"
)

// ActivationCommand carries W0 and the complete agreed cut. It carries no user
// timestamp and cannot choose the activation index, which is its committed slot.
type ActivationCommand struct {
	Witness   Witness
	Manifest  BaselineManifest
	Readiness PreparedCertificate
}

func EncodeActivationCommand(command ActivationCommand) ([]byte, error) {
	manifest, err := MarshalBaselineManifest(command.Manifest)
	if err != nil {
		return nil, err
	}
	if err := validateActivationWitness(command); err != nil {
		return nil, err
	}
	readiness, err := MarshalPreparedCertificate(command.Readiness, command.Manifest)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	const framing = controlHeaderSize + WitnessSize + 4
	if len(manifest) > MaxFileSize-framing-len(readiness) {
		return nil, errors.WithStack(ErrInvalid)
	}
	out := make([]byte, framing+len(manifest)+len(readiness))
	out[0], out[3], out[4] = controlPrefix, controlVersion, byte(ControlActivate)
	copy(out[1:3], controlMagic[:])
	binary.BigEndian.PutUint32(out[5:9], uint32(len(out)-controlHeaderSize)) // #nosec G115 -- bounded to MaxFileSize above.
	encodeWitness(out[controlHeaderSize:], command.Witness)
	offset := controlHeaderSize + WitnessSize
	binary.BigEndian.PutUint32(out[offset:offset+4], uint32(len(manifest))) // #nosec G115 -- bounded to MaxFileSize above.
	copy(out[offset+4:], manifest)
	copy(out[offset+4+len(manifest):], readiness)
	return out, nil
}

// ParseActivationCommand leaves other reserved kinds to their owning decoders.
// A malformed reserved record must still never fall through to legacy parsing.
func ParseActivationCommand(payload []byte) (ActivationCommand, bool, error) {
	if !isActivationFrame(payload) {
		return ActivationCommand{}, false, nil
	}
	if len(payload) < controlHeaderSize+WitnessSize+4 || len(payload) > MaxFileSize ||
		!bytes.Equal(payload[1:3], controlMagic[:]) ||
		int(binary.BigEndian.Uint32(payload[5:9])) != len(payload)-controlHeaderSize {
		return ActivationCommand{}, true, errors.WithStack(ErrInvalid)
	}
	if payload[3] != controlVersion {
		return ActivationCommand{}, true, errors.WithStack(ErrUnsupported)
	}
	command, err := parseActivationBody(payload[controlHeaderSize:])
	if err != nil {
		return ActivationCommand{}, true, err
	}
	if err := validateActivationWitness(command); err != nil {
		return ActivationCommand{}, true, err
	}
	return command, true, nil
}

func parseActivationBody(body []byte) (ActivationCommand, error) {
	manifestLength := binary.BigEndian.Uint32(body[WitnessSize : WitnessSize+4])
	remaining := body[WitnessSize+4:]
	if manifestLength == 0 || uint64(manifestLength) >= uint64(len(remaining)) {
		return ActivationCommand{}, errors.WithStack(ErrInvalid)
	}
	manifestSize := int(manifestLength)
	manifest, err := ParseBaselineManifest(remaining[:manifestSize])
	if err != nil {
		return ActivationCommand{}, err
	}
	ready, err := ParsePreparedCertificate(remaining[manifestSize:], manifest)
	if err != nil {
		return ActivationCommand{}, err
	}
	return ActivationCommand{Witness: decodeWitness(body), Manifest: manifest, Readiness: ready}, nil
}

func isActivationFrame(payload []byte) bool {
	return len(payload) >= 5 && payload[0] == controlPrefix && payload[4] == byte(ControlActivate)
}

func validateActivationWitness(command ActivationCommand) error {
	w := command.Witness
	c := command.Manifest.BaselineContext
	digest, err := ConfigurationDigest(c.Configuration)
	if err != nil {
		return err
	}
	if w.Cluster != c.Identity.Cluster || w.Group != c.Identity.Group ||
		w.ActivationIndex != 0 || w.Generation != 0 ||
		w.ConfigurationIndex != c.Configuration.Index || w.ConfigurationDigest != digest {
		return errors.WithStack(ErrInvalid)
	}
	return nil
}
