// Command write-skew-wal prints transaction phases and DynamoDB list values
// from the stopped, unencrypted M5 Jepsen fixture. Compare the values and
// start timestamps with the successful operations in its history.edn.
package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	pb "github.com/bootjp/elastickv/proto"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const usageError = 2

func main() {
	if len(os.Args[1:]) != 1 {
		fmt.Fprintln(os.Stderr, "usage: write-skew-wal DIR-CONTAINING-GROUP-1-AND-GROUP-2")
		os.Exit(usageError)
	}
	for _, group := range []string{"group-1", "group-2"} {
		if err := printGroup(os.Args[1], group); err != nil {
			fmt.Fprintln(os.Stderr, group, err)
			os.Exit(1)
		}
	}
}

func printGroup(dir, group string) error {
	// The isolated fixture bootstraps both groups at index 1, term 1. A WAL
	// compacted past this point cannot establish a complete witness.
	bootstrap := &walpb.Snapshot{Index: proto.Uint64(1), Term: proto.Uint64(1)}
	reader, err := wal.OpenForRead(zap.NewNop(), filepath.Join(dir, group, "wal"), bootstrap)
	if err != nil {
		return fmt.Errorf("open WAL: %w", err)
	}
	defer reader.Close()
	_, state, entries, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("read WAL: %w", err)
	}
	for _, entry := range entries {
		if entry.GetIndex() > state.GetCommit() || entry.GetType() != raftpb.EntryNormal {
			continue
		}
		requests, err := txnRequests(entry.GetData())
		if err != nil {
			return err
		}
		for _, req := range requests {
			if req.GetIsTxn() {
				if err := printItems(group, entry.GetIndex(), req); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func txnRequests(entry []byte) ([]*pb.Request, error) {
	// etcd proposal envelope v1 has a one-byte version and eight-byte ID.
	if len(entry) == 0 {
		return nil, nil
	}
	if len(entry) < 10 || entry[0] != 1 {
		return nil, fmt.Errorf("unrecognized proposal envelope")
	}
	switch entry[9] {
	case 0:
		req := new(pb.Request)
		if err := proto.Unmarshal(entry[10:], req); err != nil {
			return nil, fmt.Errorf("decode Raft request: %w", err)
		}
		return []*pb.Request{req}, nil
	case 1:
		cmd := new(pb.RaftCommand)
		if err := proto.Unmarshal(entry[10:], cmd); err != nil {
			return nil, fmt.Errorf("decode Raft batch: %w", err)
		}
		return cmd.GetRequests(), nil
	default:
		// The fixture also issues non-transactional FSM commands.
		return nil, nil
	}
}

func printItems(group string, index uint64, req *pb.Request) error {
	for _, mut := range req.GetMutations() {
		if !bytes.HasPrefix(mut.GetKey(), []byte("!ddb|item|")) {
			continue
		}
		var values []string
		if data := mut.GetValue(); bytes.HasPrefix(data, []byte{0, 'D', 'I', 1}) {
			item := new(pb.DynamoItem)
			if err := proto.Unmarshal(data[4:], item); err != nil {
				return fmt.Errorf("decode DynamoDB item at index %d: %w", index, err)
			}
			for _, v := range item.GetAttributes()["val"].GetL().GetValues() {
				values = append(values, v.GetN())
			}
		}
		fmt.Printf("%s index=%d start_ts=%d phase=%s key=%q values=%v\n",
			group, index, req.GetTs(), req.GetPhase(), mut.GetKey(), values)
	}
	return nil
}
