package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
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

// runEncryptionEnableStorageEnvelope invokes
// EncryptionAdmin.EnableStorageEnvelope on the configured
// endpoint. The Stage 6D-4 / 6D-6a server method composes the
// §3.2 sequence (leader gate → bootstrap gate → §6.4 idempotent
// short-circuit → §4 capability fan-out → propose
// RotateSubEnableStorageEnvelope → discriminate fresh-success
// vs. §2.1 #3 stale-DEKID race); this CLI surface only
// dispatches the RPC and renders the result.
//
// Output discriminates the §3.1 was_already_active flag because
// an automation script should be able to tell whether THIS
// invocation proposed the cutover or hit the §6.4
// idempotent-retry path against a previously-active cluster:
//
//	fresh:        "enabled storage envelope applied_index=N
//	               capability summary: ..."
//	already-on:   "storage envelope already active (idempotent
//	               retry) applied_index=N"
//	defensive:    "warning: cutover_index_unknown=true"
//	              emitted on top of the already-on shape when
//	              the §6.4 hand-edited / schema-rollback hedge
//	              fires.
func runEncryptionEnableStorageEnvelope(args []string, out io.Writer) error {
	req, endpoint, err := parseEnableStorageEnvelopeArgs(args)
	if err != nil {
		return err
	}
	if req == nil {
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
	resp, err := client.EnableStorageEnvelope(ctx, req)
	if err != nil {
		return errors.Wrap(err, "EnableStorageEnvelope")
	}
	return printEnableStorageEnvelopeResult(out, resp)
}

// parseEnableStorageEnvelopeArgs returns the validated proto
// request and the shared endpoint flags. A nil request with no
// error means the caller requested --help; the caller then
// exits 0.
func parseEnableStorageEnvelopeArgs(args []string) (*pb.EnableStorageEnvelopeRequest, *encryptionEndpointFlags, error) {
	fs := flag.NewFlagSet("encryption enable-storage-envelope", flag.ContinueOnError)
	endpoint := newEncryptionEndpointFlags(fs)
	proposerNodeID := fs.Uint64("proposer-node-id", 0, "The proposer's 64-bit full_node_id (registered in §4.1 writer registry); MUST be non-zero (0 is the §6.1 not-capable sentinel)")
	proposerLocalEpoch := fs.Uint("proposer-local-epoch", 0, "The proposer's local_epoch at proposal time (0..0xFFFF)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, endpoint, nil
		}
		return nil, nil, errors.Wrap(err, "parse flags")
	}
	if *proposerNodeID == 0 {
		// §6.1 sentinel check duplicated on the CLI side so the
		// operator fails fast before a round-trip; the server
		// re-validates as the source of truth.
		return nil, nil, errors.New("encryption: --proposer-node-id is required and must be non-zero (0 is the §6.1 not-capable sentinel)")
	}
	if err := requireUint16Plus1(*proposerLocalEpoch, "proposer-local-epoch"); err != nil {
		return nil, nil, err
	}
	return &pb.EnableStorageEnvelopeRequest{
		ProposerNodeId:     *proposerNodeID,
		ProposerLocalEpoch: narrowUint32(*proposerLocalEpoch),
	}, endpoint, nil
}

// printEnableStorageEnvelopeResult renders the §3.1 response in
// a shell-friendly shape. Lines start with a stable prefix
// (`enabled` / `already-active`) so scripts can `awk` on column
// 1 to discriminate the §6.4 idempotency outcomes without
// parsing the full message.
func printEnableStorageEnvelopeResult(out io.Writer, resp *pb.EnableStorageEnvelopeResponse) error {
	if resp.GetWasAlreadyActive() {
		if resp.GetCutoverIndexUnknown() {
			// §6.4 defensive fallback fires only when a sidecar
			// reports StorageEnvelopeActive=true with
			// StorageEnvelopeCutoverIndex=0 — operationally
			// impossible under normal apply but hedged against
			// schema rollback / hand-edited sidecars. Surface
			// the warning so operators can investigate.
			if _, err := fmt.Fprintln(out, "warning: cutover_index_unknown=true (sidecar may have been hand-edited or rolled back)"); err != nil {
				return errors.Wrap(err, "write warning")
			}
		}
		if _, err := fmt.Fprintf(out, "already-active applied_index=%d\n", resp.GetAppliedIndex()); err != nil {
			return errors.Wrap(err, "write result")
		}
		return nil
	}
	if _, err := fmt.Fprintf(out, "enabled applied_index=%d\n", resp.GetAppliedIndex()); err != nil {
		return errors.Wrap(err, "write result")
	}
	if len(resp.GetCapabilitySummary()) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(out, "capability summary:"); err != nil {
		return errors.Wrap(err, "write capability summary header")
	}
	for _, v := range resp.GetCapabilitySummary() {
		if _, err := fmt.Fprintf(out, "  full_node_id=%d encryption_capable=%t build_sha=%s sidecar_present=%t\n",
			v.GetFullNodeId(), v.GetEncryptionCapable(), v.GetBuildSha(), v.GetSidecarPresent()); err != nil {
			return errors.Wrap(err, "write capability row")
		}
	}
	return nil
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
	return decodeWrappedDEKFlagWithName("wrapped-new-dek", s)
}

// decodeWrappedDEKFlagWithName is the flag-name-aware variant
// used by bootstrap (which has two wrapped-DEK flags) so error
// messages reference the actual flag the operator typed rather
// than the rotate-dek default.
func decodeWrappedDEKFlagWithName(flagName, s string) ([]byte, error) {
	if s == "" {
		return nil, errors.Errorf("encryption: --%s is required (base64 of the KEK-wrapped DEK)", flagName)
	}
	out, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, errors.Wrapf(err, "encryption: --%s is not valid base64", flagName)
	}
	if len(out) == 0 {
		return nil, errors.Errorf("encryption: --%s decoded to zero bytes", flagName)
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

// runEncryptionBootstrap invokes EncryptionAdmin.BootstrapEncryption.
// The operator provides the wrapped DEK pair (base64), the two
// dek_ids, and the §5.6 step 1a writer batch as repeated
// --writer=<full_node_id>:<local_epoch> flag values. Cluster-wide
// capability fan-out (computing the batch automatically by
// dialing every member's GetCapability) is deferred to a later
// CLI iteration; for now the operator runs `encryption status`
// against each member to gather the batch and passes it
// explicitly.
func runEncryptionBootstrap(args []string, out io.Writer) error {
	parsed, endpoint, err := parseBootstrapArgs(args)
	if err != nil {
		return err
	}
	if parsed == nil {
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
	resp, err := client.BootstrapEncryption(ctx, parsed)
	if err != nil {
		return errors.Wrap(err, "BootstrapEncryption")
	}
	if _, err := fmt.Fprintf(out, "bootstrapped storage_dek_id=%d raft_dek_id=%d writers=%d applied_index=%d\n",
		parsed.StorageDekId, parsed.RaftDekId, len(parsed.WriterBatch), resp.AppliedIndex); err != nil {
		return errors.Wrap(err, "write result")
	}
	return nil
}

// parseBootstrapArgs returns the validated proto request and the
// shared endpoint flags. A nil request with no error means the
// caller requested --help; runEncryptionBootstrap then exits 0.
func parseBootstrapArgs(args []string) (*pb.BootstrapEncryptionRequest, *encryptionEndpointFlags, error) {
	fs := flag.NewFlagSet("encryption bootstrap", flag.ContinueOnError)
	endpoint := newEncryptionEndpointFlags(fs)
	storageDEKID := fs.Uint("storage-dek-id", 0, "Storage DEK key id; must be non-zero and differ from --raft-dek-id")
	raftDEKID := fs.Uint("raft-dek-id", 0, "Raft DEK key id; must be non-zero and differ from --storage-dek-id")
	wrappedStorageB64 := fs.String("wrapped-storage-dek", "", "Base64 of the KEK-wrapped storage DEK")
	wrappedRaftB64 := fs.String("wrapped-raft-dek", "", "Base64 of the KEK-wrapped raft DEK")
	var writerFlags stringSliceFlag
	fs.Var(&writerFlags, "writer", "Writer-registry entry as <full_node_id>:<local_epoch>; repeat per member")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, endpoint, nil
		}
		return nil, nil, errors.Wrap(err, "parse flags")
	}
	if err := validateBootstrapDEKIDs(*storageDEKID, *raftDEKID); err != nil {
		return nil, nil, err
	}
	wrappedStorage, wrappedRaft, err := decodeBootstrapWrappedDEKs(*wrappedStorageB64, *wrappedRaftB64)
	if err != nil {
		return nil, nil, err
	}
	if len(writerFlags) == 0 {
		return nil, nil, errors.New("encryption: at least one --writer=<full_node_id>:<local_epoch> is required")
	}
	batch, err := parseWriterBatch(writerFlags)
	if err != nil {
		return nil, nil, err
	}
	return &pb.BootstrapEncryptionRequest{
		StorageDekId:      narrowUint32(*storageDEKID),
		RaftDekId:         narrowUint32(*raftDEKID),
		WrappedStorageDek: wrappedStorage,
		WrappedRaftDek:    wrappedRaft,
		WriterBatch:       batch,
	}, endpoint, nil
}

func validateBootstrapDEKIDs(storage, raft uint) error {
	if err := requireUint32(storage, "storage-dek-id"); err != nil {
		return err
	}
	if err := requireUint32(raft, "raft-dek-id"); err != nil {
		return err
	}
	if storage == 0 {
		return errors.New("encryption: --storage-dek-id is required and must be non-zero")
	}
	if raft == 0 {
		return errors.New("encryption: --raft-dek-id is required and must be non-zero")
	}
	if storage == raft {
		return errors.New("encryption: --storage-dek-id and --raft-dek-id must differ")
	}
	return nil
}

func decodeBootstrapWrappedDEKs(storageB64, raftB64 string) ([]byte, []byte, error) {
	storage, err := decodeWrappedDEKFlagWithName("wrapped-storage-dek", storageB64)
	if err != nil {
		return nil, nil, err
	}
	raft, err := decodeWrappedDEKFlagWithName("wrapped-raft-dek", raftB64)
	if err != nil {
		return nil, nil, err
	}
	return storage, raft, nil
}

// stringSliceFlag accumulates repeated `--writer=` flags as a
// flat string slice. Each entry is parsed in parseWriterBatch
// after the flag.Parse pass so a malformed entry surfaces a
// single composite error rather than a half-built batch.
type stringSliceFlag []string

func (s *stringSliceFlag) String() string { return strings.Join(*s, ",") }
func (s *stringSliceFlag) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func parseWriterBatch(raw []string) ([]*pb.WriterRegistryEntry, error) {
	out := make([]*pb.WriterRegistryEntry, 0, len(raw))
	for i, entry := range raw {
		nodeIDStr, epochStr, ok := strings.Cut(entry, ":")
		if !ok {
			return nil, errors.Errorf("encryption: --writer[%d]=%q is not <full_node_id>:<local_epoch>", i, entry)
		}
		nodeID, err := strconv.ParseUint(nodeIDStr, 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "encryption: --writer[%d] full_node_id", i)
		}
		if nodeID == 0 {
			return nil, errors.Errorf("encryption: --writer[%d] full_node_id must be non-zero (0 is reserved)", i)
		}
		epoch, err := strconv.ParseUint(epochStr, 10, 32)
		if err != nil {
			return nil, errors.Wrapf(err, "encryption: --writer[%d] local_epoch", i)
		}
		if epoch > math.MaxUint16 {
			return nil, errors.Errorf("encryption: --writer[%d] local_epoch=%d exceeds 0xFFFF", i, epoch)
		}
		out = append(out, &pb.WriterRegistryEntry{
			FullNodeId: nodeID,
			LocalEpoch: narrowUint32(uint(epoch)),
		})
	}
	return out, nil
}

// uint32Mask is the defence-in-depth narrowing mask for the
// uint→uint32 conversion below; named so the magic-number linter
// does not flag the mask itself.
const uint32Mask uint = 0xFFFFFFFF

// narrowUint32 mirrors adapter.uint32ToLocalEpoch: callers MUST
// have validated `v` against math.MaxUint32 first (via
// requireUint32). The masked conversion is a defence-in-depth
// truncation that cannot drift even if a future caller skips the
// bound check.
func narrowUint32(v uint) uint32 {
	return uint32(v & uint32Mask) //nolint:gosec // bound-checked + masked; G115 cannot trace across the flag-parse boundary
}
