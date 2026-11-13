package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"os"

	"github.com/bootjp/elastickv/internal/server"
	"github.com/cockroachdb/errors"
)

// adminBuildVersion is surfaced by both GET /admin/api/v1/cluster and the
// Admin gRPC GetNodeVersion RPC. Release builds set it with
//
//	-ldflags "-X main.adminBuildVersion=<version-or-git-sha>"
var adminBuildVersion = "dev"

// cliFlags is bound to the process command line. Only main() reads it.
var cliFlags = server.RegisterFlags(flag.CommandLine)

// exitCodeMemoryPressure is reported by main when memwatch triggered the
// shutdown. It is non-zero so supervisors see a non-success exit, but
// distinct from log.Fatalf's 1 and from os.Exit(1) in the other binaries
// so log scraping can tell them apart.
const exitCodeMemoryPressure = 2

func main() {
	if handled, err := server.DispatchBootstrapOperator(os.Args[1:], os.Stdout); handled {
		if err != nil {
			log.Fatalf("%v", err)
		}
		return
	}
	flag.Parse()
	cliFlags.RecordExplicitFlags(flag.CommandLine)

	err := run(cliFlags)
	if server.MemoryPressureExit() {
		// memwatch fired: surface exit code 2 regardless of whether run()
		// returned a nil or an error (cancel() can cause in-flight
		// listeners to return spurious errors during shutdown). Still
		// log any residual error so a secondary failure during the
		// graceful shutdown is visible in logs rather than swallowed.
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn("shutdown error after memory pressure", "error", err)
		}
		os.Exit(exitCodeMemoryPressure)
	}
	if err := server.IgnoreCanceledAfterSignal(err); err != nil {
		log.Fatalf("%v", err)
	}
}

// run hands the parsed flags and the link-time build version to the server.
func run(flags *server.Flags) error {
	server.SetBuildVersion(adminBuildVersion)
	return errors.WithStack(server.Run(flags))
}
