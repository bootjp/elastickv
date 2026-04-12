package main

import (
	"flag"
	"fmt"
	"log"

	hashicorpraftengine "github.com/bootjp/elastickv/internal/raftengine/hashicorp"
)

var (
	sourceFSM = flag.String("fsm-store", "", "Path to the source FSM Pebble store (for example group dir/fsm.db)")
	destDir   = flag.String("dest", "", "Destination hashicorp raft data dir")
	peerList  = flag.String("peers", "", "Comma-separated raft peers (id=host:port,...)")
)

func main() {
	flag.Parse()

	peers, err := hashicorpraftengine.ParsePeers(*peerList)
	if err != nil {
		log.Fatal(err)
	}
	stats, err := hashicorpraftengine.MigrateFSMStore(*sourceFSM, *destDir, peers)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rolled back snapshot_bytes=%d peers=%d\n", stats.SnapshotBytes, stats.Peers)
}
