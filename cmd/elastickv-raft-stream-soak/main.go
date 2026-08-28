package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine/etcd/transportsoak"
	"github.com/cockroachdb/errors"
)

const defaultSoakTimeout = 2 * time.Minute

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	defaults := transportsoak.DefaultConfig()
	groups := flag.Int("groups", defaults.Groups, "number of concurrent transport groups")
	nodes := flag.Int("nodes", defaults.NodesPerGroup, "number of TCP endpoints per group")
	messages := flag.Int("messages-per-link", defaults.MessagesPerLink, "steady messages per directed peer link")
	regularBytes := flag.Int("regular-payload-bytes", defaults.RegularPayloadBytes, "steady MsgApp payload bytes")
	backpressureBytes := flag.Int("backpressure-payload-bytes", defaults.BackpressurePayloadBytes, "MsgApp payload bytes used while receiver is blocked")
	snapshotBytes := flag.Int("snapshot-payload-bytes", defaults.SnapshotPayloadBytes, "snapshot payload bytes per node")
	timeout := flag.Duration("timeout", defaultSoakTimeout, "overall soak deadline")
	output := flag.String("output", "", "write generated JSON evidence to this path")
	verify := flag.String("verify", "", "verify an existing JSON evidence file instead of running")
	flag.Parse()

	if *verify != "" {
		evidence, err := transportsoak.ReadEvidence(*verify)
		if err != nil {
			return errors.Wrap(err, "read streaming transport evidence")
		}
		if err := transportsoak.Verify(evidence); err != nil {
			return errors.Wrap(err, "verify streaming transport evidence")
		}
		fmt.Printf("verified streaming transport soak evidence: groups=%d nodes_per_group=%d result=%s\n", evidence.Config.Groups, evidence.Config.NodesPerGroup, evidence.Result)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	evidence, err := transportsoak.Run(ctx, transportsoak.Config{
		Groups:                   *groups,
		NodesPerGroup:            *nodes,
		MessagesPerLink:          *messages,
		RegularPayloadBytes:      *regularBytes,
		BackpressurePayloadBytes: *backpressureBytes,
		SnapshotPayloadBytes:     *snapshotBytes,
	})
	if err != nil {
		return errors.Wrap(err, "run streaming transport soak")
	}
	if *output != "" {
		if err := transportsoak.WriteEvidence(*output, evidence); err != nil {
			return errors.Wrap(err, "write streaming transport evidence")
		}
	}
	fmt.Printf("streaming transport soak passed: groups=%d nodes_per_group=%d evidence=%s\n", evidence.Config.Groups, evidence.Config.NodesPerGroup, *output)
	return nil
}
