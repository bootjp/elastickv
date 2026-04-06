package main

import (
	"flag"
	"fmt"
	"log"

	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
)

var (
	sourceFSM = flag.String("fsm-store", "", "Path to the source FSM Pebble store (for example group dir/fsm.db)")
	destDir   = flag.String("dest", "", "Destination etcd raft data dir")
	peerList  = flag.String("peers", "", "Comma-separated raft peers (id=host:port,...)")
)

func main() {
	flag.Parse()

	peers, err := etcdraftengine.ParsePeers(*peerList)
	if err != nil {
		log.Fatal(err)
	}
	stats, err := etcdraftengine.MigrateFSMStore(*sourceFSM, *destDir, peers)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("migrated snapshot_bytes=%d peers=%d\n", stats.SnapshotBytes, stats.Peers)
}
