package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
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
	case "-h", "--help", "help":
		// `-h` is the universal "show usage" affordance for CLI
		// subcommands; returning nil keeps the exit code at 0
		// so shell scripts using $? to detect success do not
		// trip on a help request.
		_, err := fmt.Fprintln(os.Stdout, "usage: elastickv-admin encryption <subcommand> [flags]\n\nsubcommands:\n  status\n  rotate-dek\n  register-writer")
		if err != nil {
			return errors.Wrap(err, "write usage")
		}
		return nil
	default:
		return errors.Errorf("encryption: unknown subcommand %q (PR-B supports: status, rotate-dek, register-writer)", sub)
	}
}

type encryptionEndpointFlags struct {
	endpoint *string
	timeout  *time.Duration
}

// newEncryptionEndpointFlags registers the shared --endpoint /
// --timeout flags every encryption subcommand needs. PR-A is
// plaintext-only; TLS / token authentication is shared with the
// existing --nodeTLSCACertFile / --nodeTokenFile pair on the HTTP
// surface but is deferred to PR-B for the CLI surface so the
// initial PR stays scoped to read-only status.
func newEncryptionEndpointFlags(fs *flag.FlagSet) *encryptionEndpointFlags {
	return &encryptionEndpointFlags{
		endpoint: fs.String("endpoint", "127.0.0.1:50051", "gRPC address of an elastickv node"),
		timeout:  fs.Duration("timeout", encryptionDialTimeout, "RPC timeout"),
	}
}

// dialEncryption opens a gRPC client. The context argument is
// reserved for the PR-B auth path (TLS handshake + token attach);
// grpc.NewClient itself is non-blocking and ignores the context.
// Per-RPC timeouts come from the caller's request context, not
// from this dial.
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
