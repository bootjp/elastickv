package server

import (
	"flag"
	"io"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/cockroachdb/errors"
)

// SetBuildVersion records the build version the admin surfaces report. The
// root binary calls it before Run with its link-time main.adminBuildVersion.
func SetBuildVersion(v string) { adminBuildVersion = v }

// MemoryPressureExit reports whether memwatch triggered the shutdown, so the
// root binary can exit with its memory-pressure code.
func MemoryPressureExit() bool { return memoryPressureExit.Load() }

// RecordExplicitFlags notes which flags the operator set explicitly on fs
// after it is parsed; key visualizer defaults depend on it.
func (f *Flags) RecordExplicitFlags(fs *flag.FlagSet) { f.keyViz.RecordExplicit(fs) }

// DispatchBootstrapOperator runs an `elastickv bootstrap ...` operator
// command when args name one. handled is false for a normal server start.
func DispatchBootstrapOperator(args []string, w io.Writer) (bool, error) {
	handled, err := clusterbootstrap.DispatchBootstrapOperator(args, w)
	return handled, errors.WithStack(err)
}
