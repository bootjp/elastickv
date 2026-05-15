package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"strings"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

// runEncryptionRotateDEK invokes EncryptionAdmin.RotateDEK on the
// configured endpoint. The CLI accepts the wrapped DEK material as
// base64 because operator-side wrapping is done out-of-band via
// the KEK CLI (Stage 9) and base64 is the lingua franca for
// pasting opaque bytes through a terminal. The wire-format
// validation on local_epoch <= 0xFFFF is duplicated here so the
// CLI fails fast before a round-trip; the server enforces it as
// the source of truth.
func runEncryptionRotateDEK(args []string, out io.Writer) error {
	parsed, endpoint, err := parseRotateDEKArgs(args)
	if err != nil {
		return err
	}
	if parsed == nil {
		// Help requested; usage was already written by flag parser.
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), *endpoint.timeout)
	defer cancel()
	client, closeFn, err := dialEncryption(ctx, endpoint)
	if err != nil {
		return err
	}
	defer func() {
		if err := closeFn(); err != nil {
			fmt.Fprintf(os.Stderr, "encryption: close connection: %v\n", err)
		}
	}()
	resp, err := client.RotateDEK(ctx, parsed.request)
	if err != nil {
		return errors.Wrap(err, "RotateDEK")
	}
	if _, err := fmt.Fprintf(out, "rotated dek_id=%d purpose=%s applied_index=%d\n",
		parsed.request.NewDekId, parsed.purposeLabel, resp.AppliedIndex); err != nil {
		return errors.Wrap(err, "write result")
	}
	return nil
}

type parsedRotateDEK struct {
	request      *pb.RotateDEKRequest
	purposeLabel string
}

// parseRotateDEKArgs returns the validated proto request and the
// shared endpoint flags. A nil request with no error means the
// caller requested --help; runEncryptionRotateDEK then exits 0.
func parseRotateDEKArgs(args []string) (*parsedRotateDEK, *encryptionEndpointFlags, error) {
	fs := flag.NewFlagSet("encryption rotate-dek", flag.ContinueOnError)
	endpoint := newEncryptionEndpointFlags(fs)
	purpose := fs.String("purpose", "", "DEK purpose: storage | raft")
	newDEKID := fs.Uint("new-dek-id", 0, "Key id for the new DEK; must be non-zero")
	wrappedB64 := fs.String("wrapped-new-dek", "", "Base64 of the KEK-wrapped new DEK")
	proposerNodeID := fs.Uint64("proposer-node-id", 0, "The proposer's 64-bit full_node_id (registered in §4.1 writer registry)")
	proposerLocalEpoch := fs.Uint("proposer-local-epoch", 0, "The proposer's local_epoch (0..0xFFFF) at proposal time")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, endpoint, nil
		}
		return nil, nil, errors.Wrap(err, "parse flags")
	}
	purposeEnum, err := parseRotatePurposeFlag(*purpose)
	if err != nil {
		return nil, nil, err
	}
	if err := requireUint32(*newDEKID, "new-dek-id"); err != nil {
		return nil, nil, err
	}
	if *newDEKID == 0 {
		return nil, nil, errors.New("encryption: --new-dek-id is required and must be non-zero")
	}
	if err := requireUint16Plus1(*proposerLocalEpoch, "proposer-local-epoch"); err != nil {
		return nil, nil, err
	}
	wrapped, err := decodeWrappedDEKFlag(*wrappedB64)
	if err != nil {
		return nil, nil, err
	}
	return &parsedRotateDEK{
		request: &pb.RotateDEKRequest{
			Purpose:            purposeEnum,
			NewDekId:           narrowUint32(*newDEKID),
			WrappedNewDek:      wrapped,
			ProposerNodeId:     *proposerNodeID,
			ProposerLocalEpoch: narrowUint32(*proposerLocalEpoch),
		},
		purposeLabel: *purpose,
	}, endpoint, nil
}

// runEncryptionRegisterWriter invokes
// EncryptionAdmin.RegisterEncryptionWriter for a single
// (dek_id, full_node_id, local_epoch) triple. Multi-writer
// batches go through bootstrap (PR-C).
func runEncryptionRegisterWriter(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("encryption register-writer", flag.ContinueOnError)
	endpoint := newEncryptionEndpointFlags(fs)
	dekID := fs.Uint("dek-id", 0, "Key id to register the writer under; must be non-zero")
	fullNodeID := fs.Uint64("full-node-id", 0, "The 64-bit full_node_id being registered")
	localEpoch := fs.Uint("local-epoch", 0, "The writer's local_epoch (0..0xFFFF)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return errors.Wrap(err, "parse flags")
	}
	if err := requireUint32(*dekID, "dek-id"); err != nil {
		return err
	}
	if *dekID == 0 {
		return errors.New("encryption: --dek-id is required and must be non-zero")
	}
	if err := requireUint16Plus1(*localEpoch, "local-epoch"); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), *endpoint.timeout)
	defer cancel()
	client, closeFn, err := dialEncryption(ctx, endpoint)
	if err != nil {
		return err
	}
	defer func() {
		if err := closeFn(); err != nil {
			fmt.Fprintf(os.Stderr, "encryption: close connection: %v\n", err)
		}
	}()
	resp, err := client.RegisterEncryptionWriter(ctx, &pb.RegisterEncryptionWriterRequest{
		DekId: narrowUint32(*dekID),
		Writers: []*pb.WriterRegistryEntry{
			{FullNodeId: *fullNodeID, LocalEpoch: narrowUint32(*localEpoch)},
		},
	})
	if err != nil {
		return errors.Wrap(err, "RegisterEncryptionWriter")
	}
	if _, err := fmt.Fprintf(out, "registered dek_id=%d full_node_id=%d local_epoch=%d applied_index=%d\n",
		*dekID, *fullNodeID, *localEpoch, resp.AppliedIndex); err != nil {
		return errors.Wrap(err, "write result")
	}
	return nil
}

func parseRotatePurposeFlag(s string) (pb.RotateDEKRequest_Purpose, error) {
	switch strings.ToLower(s) {
	case "storage":
		return pb.RotateDEKRequest_PURPOSE_STORAGE, nil
	case "raft":
		return pb.RotateDEKRequest_PURPOSE_RAFT, nil
	case "":
		return 0, errors.New("encryption: --purpose is required (storage|raft)")
	default:
		return 0, errors.Errorf("encryption: --purpose=%q invalid (want storage|raft)", s)
	}
}

func decodeWrappedDEKFlag(s string) ([]byte, error) {
	if s == "" {
		return nil, errors.New("encryption: --wrapped-new-dek is required (base64 of the KEK-wrapped DEK)")
	}
	out, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, errors.Wrap(err, "encryption: --wrapped-new-dek is not valid base64")
	}
	if len(out) == 0 {
		return nil, errors.New("encryption: --wrapped-new-dek decoded to zero bytes")
	}
	return out, nil
}

func requireUint32(v uint, name string) error {
	if uint64(v) > math.MaxUint32 {
		return errors.Errorf("encryption: --%s=%d exceeds the uint32 bound (max 0x%08x)", name, v, math.MaxUint32)
	}
	return nil
}

func requireUint16Plus1(v uint, name string) error {
	if uint64(v) > math.MaxUint16 {
		return errors.Errorf("encryption: --%s=%d exceeds the §4.1 16-bit bound (max 0x%04x)", name, v, math.MaxUint16)
	}
	return nil
}

// narrowUint32 mirrors adapter.uint32ToLocalEpoch: callers MUST
// have validated `v` against math.MaxUint32 first (via
// requireUint32). The masked conversion is a defence-in-depth
// truncation that cannot drift even if a future caller skips the
// bound check.
// uint32Mask is the defence-in-depth narrowing mask for the
// uint→uint32 conversion below; named so the magic-number linter
// does not flag the mask itself.
const uint32Mask uint = 0xFFFFFFFF

func narrowUint32(v uint) uint32 {
	return uint32(v & uint32Mask) //nolint:gosec // bound-checked + masked; G115 cannot trace across the flag-parse boundary
}
