package kv

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestTSOClaimCompactEvidenceBindsActualPayload(t *testing.T) {
	_, allocator, _ := newProductionClaimFixture(t)
	request := &pb.Request{TimestampClaimGroupId: 1, ObservedRouteVersion: 9,
		ReadKeys: [][]byte{[]byte("read")}, WriteFenceBypassKeys: [][]byte{[]byte("key")},
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("key"), Value: []byte("value"), RequireAbsent: true}}}
	require.NoError(t, IssueRequestTimestampClaim(t.Context(), allocator, request))
	require.NoError(t, ValidateRequestTimestampClaim(t.Context(), allocator, request))
	digest := sha256.Sum256([]byte("value"))
	require.Equal(t, digest[:], request.TimestampIntent.Participants[0].Mutations[0].ValueSha256)
	for name, change := range map[string]func(*pb.Request){
		"value":  func(r *pb.Request) { r.Mutations[0].Value[0] ^= 1 },
		"length": func(r *pb.Request) { r.Mutations[0].Value = append(r.Mutations[0].Value, 'x') },
		"hash is not authority": func(r *pb.Request) {
			r.Mutations[0].Value = []byte("other")
			d := sha256.Sum256(r.Mutations[0].Value)
			r.TimestampIntent.Participants[0].Mutations[0].ValueSha256 = d[:]
		},
		"manifest length":     func(r *pb.Request) { r.TimestampIntent.Participants[0].Mutations[0].ValueLength++ },
		"require absent":      func(r *pb.Request) { r.Mutations[0].RequireAbsent = false },
		"op":                  func(r *pb.Request) { r.Mutations[0].Op = pb.Op_DEL },
		"key":                 func(r *pb.Request) { r.Mutations[0].Key = []byte("other") },
		"route":               func(r *pb.Request) { r.ObservedRouteVersion++ },
		"reads":               func(r *pb.Request) { r.ReadKeys = nil },
		"bypass":              func(r *pb.Request) { r.WriteFenceBypassKeys = nil },
		"undeclared patch":    func(r *pb.Request) { r.Mutations[0].CommitTsValueOffset = 1 },
		"foreign participant": func(r *pb.Request) { r.TimestampClaimGroupId = 2 },
		"subset":              func(r *pb.Request) { r.Mutations = nil },
		"duplicate participant": func(r *pb.Request) {
			r.TimestampIntent.Participants = append(r.TimestampIntent.Participants, proto.CloneOf(r.TimestampIntent.Participants[0]))
		},
		"duplicate canonical reads": func(r *pb.Request) {
			r.TimestampIntent.Participants[0].ReadKeys = append(r.TimestampIntent.Participants[0].ReadKeys, []byte("read"))
		},
		"unknown evidence": func(r *pb.Request) {
			r.TimestampIntent.Participants[0].Mutations[0].ProtoReflect().SetUnknown([]byte{0x40, 1})
		},
		"unknown mutation":     func(r *pb.Request) { r.Mutations[0].ProtoReflect().SetUnknown([]byte{0x30, 1}) },
		"old manifest":         func(r *pb.Request) { r.TimestampIntent.ManifestVersion = 1 },
		"old full-value field": func(r *pb.Request) { r.TimestampIntent.ProtoReflect().SetUnknown([]byte{0x22, 0}) },
	} {
		t.Run(name, func(t *testing.T) {
			forged := proto.CloneOf(request)
			change(forged)
			require.ErrorIs(t, ValidateRequestTimestampClaim(t.Context(), allocator, forged), ErrTSOClaimInvalid)
		})
	}
}

func TestTSOClaimCompactPatchEvidence(t *testing.T) {
	_, allocator, _ := newProductionClaimFixture(t)
	value := append([]byte("prefix"), make([]byte, 8)...)
	value = append(value, []byte("suffix")...)
	request := &pb.Request{TimestampClaimGroupId: 1, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("key"), Value: value, CommitTsValueOffset: 6}}}
	normalized := sha256.Sum256(bytes.Clone(value))
	require.NoError(t, IssueRequestTimestampClaim(t.Context(), allocator, request))
	evidence := request.TimestampIntent.Participants[0].Mutations[0]
	require.Equal(t, normalized[:], evidence.ValueSha256)
	require.EqualValues(t, 6, evidence.CommitTsValueOffset)
	require.EqualValues(t, 8, evidence.CommitTsValueLength)
	require.Equal(t, request.Ts, binary.BigEndian.Uint64(request.Mutations[0].Value[6:14]))
	require.NoError(t, ValidateRequestTimestampClaim(t.Context(), allocator, request))
	for name, change := range map[string]func(*pb.Request){
		"stamped bytes": func(r *pb.Request) { r.Mutations[0].Value[7] ^= 1 },
		"prefix":        func(r *pb.Request) { r.Mutations[0].Value[0] ^= 1 },
		"suffix":        func(r *pb.Request) { r.Mutations[0].Value[14] ^= 1 },
		"offset":        func(r *pb.Request) { r.TimestampIntent.Participants[0].Mutations[0].CommitTsValueOffset++ },
		"patch length":  func(r *pb.Request) { r.TimestampIntent.Participants[0].Mutations[0].CommitTsValueLength = 7 },
		"overflow":      func(r *pb.Request) { r.TimestampIntent.Participants[0].Mutations[0].CommitTsValueOffset = ^uint64(0) },
		"actual offset": func(r *pb.Request) { r.Mutations[0].CommitTsValueOffset = 7 },
	} {
		t.Run(name, func(t *testing.T) {
			forged := proto.CloneOf(request)
			change(forged)
			require.ErrorIs(t, ValidateRequestTimestampClaim(t.Context(), allocator, forged), ErrTSOClaimInvalid)
		})
	}
}

func TestTSOClaimCompactParticipantPlan(t *testing.T) {
	_, allocator, _ := newProductionClaimFixture(t)
	first := &pb.Request{TimestampClaimGroupId: 1, Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte("a"), Value: []byte("one")},
		{Op: pb.Op_PUT, Key: []byte("a"), Value: []byte("two")},
	}}
	second := &pb.Request{TimestampClaimGroupId: 2, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("b"), Value: []byte("three")}}}
	group := &OperationGroup[OP]{}
	_, err := bindClaimToParticipantRequests(t.Context(), allocator, []*pb.Request{first, second}, group)
	require.NoError(t, err)
	for _, request := range []*pb.Request{first, second} {
		require.NoError(t, ValidateRequestTimestampClaim(t.Context(), allocator, request))
	}
	for name, change := range map[string]func(*pb.Request){
		"reordered":                  func(r *pb.Request) { r.Mutations[0], r.Mutations[1] = r.Mutations[1], r.Mutations[0] },
		"subset":                     func(r *pb.Request) { r.Mutations = r.Mutations[:1] },
		"duplicate":                  func(r *pb.Request) { r.Mutations[1] = r.Mutations[0] },
		"prefix index on point plan": func(r *pb.Request) { r.TimestampClaimPrefixIndices = []uint32{0} },
		"participant remap":          func(r *pb.Request) { r.TimestampClaimGroupId = 2 },
	} {
		t.Run(name, func(t *testing.T) {
			forged := proto.CloneOf(first)
			change(forged)
			require.ErrorIs(t, ValidateRequestTimestampClaim(t.Context(), allocator, forged), ErrTSOClaimInvalid)
		})
	}
}

func TestTSOClaimCompactTxnMetadata(t *testing.T) {
	_, allocator, _ := newProductionClaimFixture(t)
	startTS, err := allocator.Next(t.Context())
	require.NoError(t, err)
	meta := TxnMeta{PrimaryKey: []byte("key"), LockTTLms: 1234, PrevCommitTS: startTS - 1}
	request := &pb.Request{IsTxn: true, Ts: startTS, TimestampClaimGroupId: 1,
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(meta)}, {Op: pb.Op_PUT, Key: []byte("key"), Value: []byte("value")}}}
	require.NoError(t, IssueRequestTimestampClaim(t.Context(), allocator, request))
	require.NoError(t, ValidateRequestTimestampClaim(t.Context(), allocator, request))
	for _, name := range []string{"primary", "ttl", "previous", "read start"} {
		t.Run(name, func(t *testing.T) {
			forged := proto.CloneOf(request)
			changed, _, err := extractTxnMeta(forged.Mutations)
			require.NoError(t, err)
			switch name {
			case "primary":
				changed.PrimaryKey = []byte("other")
			case "ttl":
				changed.LockTTLms++
			case "previous":
				changed.PrevCommitTS++
			case "read start":
				forged.Ts++
			}
			forged.Mutations[0].Value = EncodeTxnMeta(changed)
			require.ErrorIs(t, ValidateRequestTimestampClaim(t.Context(), allocator, forged), ErrTSOClaimInvalid)
		})
	}
}
