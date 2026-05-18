package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const encryptionDialTimeout = 5 * time.Second

// encryptionMain dispatches the `elastickv-admin encryption ...`
// subcommand tree. The HTTP-server path in run() is mutually exclusive
// with this one — main() picks based on argv[1] before flag.Parse() so
// the two surfaces do not share a global flag namespace.
//
// PR-A wired `status`. PR-B added `rotate-dek` and
// `register-writer`. PR-C adds `bootstrap`; Stage 6 adds
// `enable-storage-envelope` and `enable-raft-envelope`. ResyncSidecar
// is a server-side §5.5 fallback (no CLI surface).
func encryptionMain(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: elastickv-admin encryption <subcommand> [flags]")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "status":
		return runEncryptionStatus(rest, os.Stdout)
	case "rotate-dek":
		return runEncryptionRotateDEK(rest, os.Stdout)
	case "register-writer":
		return runEncryptionRegisterWriter(rest, os.Stdout)
	case "bootstrap":
		return runEncryptionBootstrap(rest, os.Stdout)
	case "probe-node-id":
		return runEncryptionProbeNodeID(rest, os.Stdout)
	case "-h", "--help", "help":
		// `-h` is the universal "show usage" affordance for CLI
		// subcommands; returning nil keeps the exit code at 0
		// so shell scripts using $? to detect success do not
		// trip on a help request.
		_, err := fmt.Fprintln(os.Stdout, "usage: elastickv-admin encryption <subcommand> [flags]\n\nsubcommands:\n  status\n  rotate-dek\n  register-writer\n  bootstrap\n  probe-node-id")
		if err != nil {
			return errors.Wrap(err, "write usage")
		}
		return nil
	default:
		return errors.Errorf("encryption: unknown subcommand %q (supported: status, rotate-dek, register-writer, bootstrap, probe-node-id)", sub)
	}
}

// runEncryptionProbeNodeID implements the §5.1 collision-
// mitigation helper from the 6D design doc: it derives the
// 16-bit `node_id` from a candidate `full_node_id` so the
// operator can verify the value is free in the cluster's
// allocated node_id space BEFORE joining the node and
// triggering an `ErrNodeIDCollision` refusal.
//
// The narrowing — `uint16(full_node_id & 0xFFFF)` — matches the
// shipped writer-registry keying and §4.1 GCM nonce prefix
// (see `internal/encryption/applier.go::nodeIDMask`). Anything
// other than this narrowing would compute a value that diverges
// from what the runtime uses, defeating the purpose of the
// probe.
//
// No RPC — this is a pure local computation. The operator runs
// it against a candidate `full_node_id` and a current cluster's
// allocated node_id list (out of scope for this subcommand; the
// operator can read `gh ... encryption status` on each member
// to gather them).
func runEncryptionProbeNodeID(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("encryption probe-node-id", flag.ContinueOnError)
	fullNodeIDStr := fs.String("full-node-id", "", "candidate 64-bit full_node_id to probe (decimal or 0x-prefixed hex)")
	if err := fs.Parse(args); err != nil {
		// flag.ContinueOnError reports `-h`/`--help` via the
		// sentinel flag.ErrHelp after writing usage to
		// fs.Output(). Match the convention used by every other
		// encryption subcommand (status, rotate-dek,
		// register-writer, bootstrap): treat ErrHelp as a usage
		// request, exit 0 so shell scripts that test $? on help
		// invocations don't trip.
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return errors.Wrap(err, "parse probe-node-id flags")
	}
	if *fullNodeIDStr == "" {
		return errors.New("--full-node-id is required (decimal or 0x-prefixed hex)")
	}
	full, err := parseUint64WithRadix(*fullNodeIDStr)
	if err != nil {
		return errors.Wrapf(err, "parse --full-node-id=%q", *fullNodeIDStr)
	}
	const nodeIDMask = 0xFFFF
	narrowed := uint16(full & nodeIDMask) //nolint:gosec // masked to 16 bits; matches applier.go convention
	if _, err := fmt.Fprintf(out, "full_node_id: %#016x (%d)\nnode_id:      %#04x (%d)\n",
		full, full, narrowed, narrowed); err != nil {
		return errors.Wrap(err, "write probe-node-id result")
	}
	return nil
}

// parseUint64WithRadix accepts either decimal ("12345") or
// 0x-prefixed hex ("0xDEADBEEF") so operators can paste values
// in whichever form their inventory uses. Uses strconv.ParseUint
// rather than fmt.Sscanf: ParseUint requires the ENTIRE input to
// be valid for the chosen radix, whereas Sscanf stops at the
// first non-matching character and silently returns the prefix.
// The silent-prefix behaviour would let "0x1234ZZZZ" parse as
// 0x1234 (or "1234abc" as 1234), which is unsafe for an
// operator-visible identifier where every digit matters.
func parseUint64WithRadix(s string) (uint64, error) {
	if len(s) >= 2 && (s[0:2] == "0x" || s[0:2] == "0X") {
		v, err := strconv.ParseUint(s[2:], 16, 64)
		if err != nil {
			return 0, errors.Wrap(err, "hex parse")
		}
		return v, nil
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, errors.Wrap(err, "decimal parse")
	}
	return v, nil
}

type encryptionEndpointFlags struct {
	endpoint *string
	timeout  *time.Duration
}

// newEncryptionEndpointFlags registers the shared --endpoint /
// --timeout flags every encryption subcommand needs. The CLI
// surface is plaintext-only through PR-B; TLS / token
// authentication shares the existing --nodeTLSCACertFile /
// --nodeTokenFile flags on the HTTP surface and is wired into
// the encryption CLI in Stage 6 alongside the --encryption-enabled
// cluster-flag gate.
func newEncryptionEndpointFlags(fs *flag.FlagSet) *encryptionEndpointFlags {
	return &encryptionEndpointFlags{
		endpoint: fs.String("endpoint", "127.0.0.1:50051", "gRPC address of an elastickv node"),
		timeout:  fs.Duration("timeout", encryptionDialTimeout, "RPC timeout"),
	}
}

// dialEncryption opens a gRPC client. The context argument is
// reserved for the Stage 6 auth path (TLS handshake + token
// attach); grpc.NewClient itself is non-blocking and ignores the
// context. Per-RPC timeouts come from the caller's request
// context, not from this dial.
func dialEncryption(_ context.Context, flags *encryptionEndpointFlags) (pb.EncryptionAdminClient, func() error, error) {
	conn, err := grpc.NewClient(*flags.endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "dial %s", *flags.endpoint)
	}
	return pb.NewEncryptionAdminClient(conn), conn.Close, nil
}

func runEncryptionStatus(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("encryption status", flag.ContinueOnError)
	endpoint := newEncryptionEndpointFlags(fs)
	if err := fs.Parse(args); err != nil {
		// flag.ContinueOnError reports `-h`/`--help` via the
		// sentinel flag.ErrHelp after writing usage to
		// fs.Output(). That is not an error condition for the
		// CLI, just a usage request — exit 0.
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return errors.Wrap(err, "parse flags")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *endpoint.timeout)
	defer cancel()
	client, closeFn, err := dialEncryption(ctx, endpoint)
	if err != nil {
		return err
	}
	defer func() {
		// Surface gRPC client-conn cleanup failures so a host
		// stuck with leaked FDs or a misbehaving transport is
		// visible; close errors are otherwise easy to miss in
		// a one-shot CLI.
		if err := closeFn(); err != nil {
			fmt.Fprintf(os.Stderr, "encryption: close connection: %v\n", err)
		}
	}()
	cap, err := client.GetCapability(ctx, &pb.Empty{})
	if err != nil {
		return errors.Wrap(err, "GetCapability")
	}
	state, stateErr := client.GetSidecarState(ctx, &pb.Empty{})
	// Only FailedPrecondition is the documented "node not
	// configured for encryption" sentinel that downgrades to the
	// soft "unavailable" line in writeEncryptionStatus. Anything
	// else (Unimplemented after a binary downgrade, Unavailable
	// from a half-open transport, etc.) is a real failure the
	// operator must see; let it bubble up so the CLI exits
	// non-zero.
	if stateErr != nil && status.Code(stateErr) != codes.FailedPrecondition {
		return errors.Wrap(stateErr, "GetSidecarState")
	}
	return writeEncryptionStatus(out, cap, state, stateErr)
}

func writeEncryptionStatus(out io.Writer, cap *pb.CapabilityReport, state *pb.SidecarStateReport, stateErr error) error {
	if _, err := fmt.Fprintln(out, "capability:"); err != nil {
		return errors.Wrap(err, "write capability header")
	}
	if _, err := fmt.Fprintf(out, "  encryption_capable: %t\n", cap.EncryptionCapable); err != nil {
		return errors.Wrap(err, "write encryption_capable")
	}
	if _, err := fmt.Fprintf(out, "  sidecar_present:    %t\n", cap.SidecarPresent); err != nil {
		return errors.Wrap(err, "write sidecar_present")
	}
	if _, err := fmt.Fprintf(out, "  full_node_id:       %d\n", cap.FullNodeId); err != nil {
		return errors.Wrap(err, "write full_node_id")
	}
	if _, err := fmt.Fprintf(out, "  local_epoch:        %d\n", cap.LocalEpoch); err != nil {
		return errors.Wrap(err, "write local_epoch")
	}
	if cap.BuildSha != "" {
		if _, err := fmt.Fprintf(out, "  build_sha:          %s\n", cap.BuildSha); err != nil {
			return errors.Wrap(err, "write build_sha")
		}
	}
	if stateErr != nil {
		if _, err := fmt.Fprintf(out, "sidecar_state: <unavailable: %v>\n", stateErr); err != nil {
			return errors.Wrap(err, "write sidecar_state unavailable")
		}
		return nil
	}
	return writeSidecarStateBlock(out, state)
}

func writeSidecarStateBlock(out io.Writer, state *pb.SidecarStateReport) error {
	if _, err := fmt.Fprintln(out, "sidecar_state:"); err != nil {
		return errors.Wrap(err, "write sidecar_state header")
	}
	if _, err := fmt.Fprintf(out, "  active_storage_id:           %d\n", state.ActiveStorageId); err != nil {
		return errors.Wrap(err, "write active_storage_id")
	}
	if _, err := fmt.Fprintf(out, "  active_raft_id:              %d\n", state.ActiveRaftId); err != nil {
		return errors.Wrap(err, "write active_raft_id")
	}
	if _, err := fmt.Fprintf(out, "  storage_envelope_active:     %t\n", state.StorageEnvelopeActive); err != nil {
		return errors.Wrap(err, "write storage_envelope_active")
	}
	if _, err := fmt.Fprintf(out, "  raft_envelope_cutover_index: %d\n", state.RaftEnvelopeCutoverIndex); err != nil {
		return errors.Wrap(err, "write raft_envelope_cutover_index")
	}
	if _, err := fmt.Fprintf(out, "  latest_applied_index:        %d\n", state.LatestAppliedIndex); err != nil {
		return errors.Wrap(err, "write latest_applied_index")
	}
	ids := make([]uint32, 0, len(state.WrappedDeksById))
	for id := range state.WrappedDeksById {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	if _, err := fmt.Fprintf(out, "  wrapped_dek_ids:             %v\n", ids); err != nil {
		return errors.Wrap(err, "write wrapped_dek_ids")
	}
	return nil
}
