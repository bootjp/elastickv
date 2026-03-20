package main

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"

	"github.com/bootjp/elastickv/internal/raftstore"
)

func main() {
	var (
		dir = flag.String("dir", "", "Directory containing legacy logs.dat and stable.dat")
		out = flag.String("out", "", "Destination Pebble raft.db directory (default: <dir>/raft.db)")
	)
	flag.Parse()

	if *dir == "" {
		log.Fatal("--dir is required")
	}

	dest := *out
	if dest == "" {
		dest = filepath.Join(*dir, "raft.db")
	}

	stats, err := raftstore.MigrateLegacyBoltDB(
		filepath.Join(*dir, "logs.dat"),
		filepath.Join(*dir, "stable.dat"),
		dest,
	)
	if err != nil {
		log.Fatalf("migration failed: %v", err)
	}

	fmt.Printf("migrated legacy raft storage to %s (logs=%d stable_keys=%d)\n", dest, stats.Logs, stats.StableKeys)
	fmt.Println("next: archive or remove logs.dat and stable.dat before starting elastickv")
}
