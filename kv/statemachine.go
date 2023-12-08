// Copyright 2017,2018 Lei Ni (nilei81@gmail.com).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"google.golang.org/protobuf/proto"
)

// ExampleStateMachine is the IStateMachine implementation used in the example
// for handling all inputs not ends with "?".
// See https://github.com/lni/dragonboat/blob/master/statemachine/rsm.go for
// more details of the IStateMachine interface.
type ExampleStateMachine struct {
	ShardID   uint64
	ReplicaID uint64
	Count     uint64

	store Store
}

// NewExampleStateMachine creates and return a new ExampleStateMachine object.
func NewExampleStateMachine(shardID uint64, replicaID uint64) sm.IStateMachine {
	return &ExampleStateMachine{
		ShardID:   shardID,
		ReplicaID: replicaID,
		Count:     0,
		store:     NewMemoryStore(),
	}
}

// Lookup performs local lookup on the ExampleStateMachine instance. In this example,
// we always return the Count value as a little endian binary encoded byte
// slice.
func (s *ExampleStateMachine) Lookup(query any) (any, error) {
	result := make([]byte, 8)
	binary.LittleEndian.PutUint64(result, s.Count)
	return result, nil
}

// Update updates the object using the specified committed raft entry.
func (s *ExampleStateMachine) Update(e sm.Entry) (sm.Result, error) {
	// in this example, we print out the following message for each
	// incoming update request. we also increase the counter by one to remember
	// how many updates we have applied
	//f.mtx.Lock()
	//defer f.mtx.Unlock()
	ctx := context.TODO()

	req, err := s.unmarshal(e.Cmd)

	if err != nil {
		return sm.Result{}, errors.WithStack(err)
	}

	switch v := req.(type) {
	case *pb.PutRequest:
		return e.Result, errors.WithStack(s.store.Put(ctx, v.Key, v.Value))
	case *pb.DeleteRequest:
		return e.Result, errors.WithStack(s.store.Delete(ctx, v.Key))
	case *pb.GetRequest:
		return e.Result, errors.WithStack(ErrUnknownRequestType)
	default:
		return e.Result, errors.WithStack(ErrUnknownRequestType)
	}
}

func (s *ExampleStateMachine) unmarshal(b []byte) (proto.Message, error) {
	// todo パフォーマンス上の問題があるので、もっと良い方法を考える

	putReq := &pb.PutRequest{}
	if err := proto.Unmarshal(b, putReq); err == nil {
		fmt.Println("Unmarshaled as PutRequest:", putReq)
		return putReq, nil
	}

	delReq := &pb.DeleteRequest{}
	if err := proto.Unmarshal(b, delReq); err == nil {
		fmt.Println("Unmarshaled as DeleteRequest:", delReq)
		return delReq, nil
	}

	getReq := &pb.GetRequest{}
	if err := proto.Unmarshal(b, getReq); err == nil {
		fmt.Println("Unmarshaled as GetRequest:", getReq)
		return getReq, nil
	}

	return nil, errors.WithStack(ErrUnknownRequestType)
}

// SaveSnapshot saves the current IStateMachine state into a snapshot using the
// specified io.Writer object.
func (s *ExampleStateMachine) SaveSnapshot(w io.Writer,
	fc sm.ISnapshotFileCollection, done <-chan struct{}) error {
	// as shown above, the only state that can be saved is the Count variable
	// there is no external file in this IStateMachine example, we thus leave
	// the fc untouched
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, s.Count)
	_, err := w.Write(data)
	return err
}

// RecoverFromSnapshot recovers the state using the provided snapshot.
func (s *ExampleStateMachine) RecoverFromSnapshot(r io.Reader,
	files []sm.SnapshotFile, done <-chan struct{}) error {
	// restore the Count variable, that is the only state we maintain in this
	// example, the input files is expected to be empty
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	v := binary.LittleEndian.Uint64(data)
	s.Count = v
	return nil
}

// Close closes the IStateMachine instance. There is nothing for us to cleanup
// or release as this is a pure in memory data store. Note that the Close
// method is not guaranteed to be called as node can crash at any time.
func (s *ExampleStateMachine) Close() error { return nil }
