// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.31.0
// 	protoc        v4.23.4
// source: internal.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// internal.proto is node to node communication message in raft replication.
type Op int32

const (
	Op_PUT Op = 0
	Op_DEL Op = 1
)

// Enum value maps for Op.
var (
	Op_name = map[int32]string{
		0: "PUT",
		1: "DEL",
	}
	Op_value = map[string]int32{
		"PUT": 0,
		"DEL": 1,
	}
)

func (x Op) Enum() *Op {
	p := new(Op)
	*p = x
	return p
}

func (x Op) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Op) Descriptor() protoreflect.EnumDescriptor {
	return file_internal_proto_enumTypes[0].Descriptor()
}

func (Op) Type() protoreflect.EnumType {
	return &file_internal_proto_enumTypes[0]
}

func (x Op) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Op.Descriptor instead.
func (Op) EnumDescriptor() ([]byte, []int) {
	return file_internal_proto_rawDescGZIP(), []int{0}
}

type Phase int32

const (
	Phase_NONE    Phase = 0
	Phase_PREPARE Phase = 1
	Phase_COMMIT  Phase = 2
	Phase_ABORT   Phase = 3
)

// Enum value maps for Phase.
var (
	Phase_name = map[int32]string{
		0: "NONE",
		1: "PREPARE",
		2: "COMMIT",
		3: "ABORT",
	}
	Phase_value = map[string]int32{
		"NONE":    0,
		"PREPARE": 1,
		"COMMIT":  2,
		"ABORT":   3,
	}
)

func (x Phase) Enum() *Phase {
	p := new(Phase)
	*p = x
	return p
}

func (x Phase) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Phase) Descriptor() protoreflect.EnumDescriptor {
	return file_internal_proto_enumTypes[1].Descriptor()
}

func (Phase) Type() protoreflect.EnumType {
	return &file_internal_proto_enumTypes[1]
}

func (x Phase) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Phase.Descriptor instead.
func (Phase) EnumDescriptor() ([]byte, []int) {
	return file_internal_proto_rawDescGZIP(), []int{1}
}

type Mutation struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Op    Op     `protobuf:"varint,1,opt,name=op,proto3,enum=Op" json:"op,omitempty"`
	Key   []byte `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
	Value []byte `protobuf:"bytes,3,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *Mutation) Reset() {
	*x = Mutation{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Mutation) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Mutation) ProtoMessage() {}

func (x *Mutation) ProtoReflect() protoreflect.Message {
	mi := &file_internal_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Mutation.ProtoReflect.Descriptor instead.
func (*Mutation) Descriptor() ([]byte, []int) {
	return file_internal_proto_rawDescGZIP(), []int{0}
}

func (x *Mutation) GetOp() Op {
	if x != nil {
		return x.Op
	}
	return Op_PUT
}

func (x *Mutation) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *Mutation) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

type Request struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	IsTxn     bool        `protobuf:"varint,1,opt,name=is_txn,json=isTxn,proto3" json:"is_txn,omitempty"`
	Phase     Phase       `protobuf:"varint,2,opt,name=phase,proto3,enum=Phase" json:"phase,omitempty"`
	Ts        uint64      `protobuf:"varint,3,opt,name=ts,proto3" json:"ts,omitempty"`
	Mutations []*Mutation `protobuf:"bytes,4,rep,name=mutations,proto3" json:"mutations,omitempty"`
}

func (x *Request) Reset() {
	*x = Request{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Request) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Request) ProtoMessage() {}

func (x *Request) ProtoReflect() protoreflect.Message {
	mi := &file_internal_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Request.ProtoReflect.Descriptor instead.
func (*Request) Descriptor() ([]byte, []int) {
	return file_internal_proto_rawDescGZIP(), []int{1}
}

func (x *Request) GetIsTxn() bool {
	if x != nil {
		return x.IsTxn
	}
	return false
}

func (x *Request) GetPhase() Phase {
	if x != nil {
		return x.Phase
	}
	return Phase_NONE
}

func (x *Request) GetTs() uint64 {
	if x != nil {
		return x.Ts
	}
	return 0
}

func (x *Request) GetMutations() []*Mutation {
	if x != nil {
		return x.Mutations
	}
	return nil
}

var File_internal_proto protoreflect.FileDescriptor

var file_internal_proto_rawDesc = []byte{
	0x0a, 0x0e, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x22, 0x47, 0x0a, 0x08, 0x4d, 0x75, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x13, 0x0a, 0x02,
	0x6f, 0x70, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x03, 0x2e, 0x4f, 0x70, 0x52, 0x02, 0x6f,
	0x70, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03,
	0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x0c, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x77, 0x0a, 0x07, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x12, 0x15, 0x0a, 0x06, 0x69, 0x73, 0x5f, 0x74, 0x78, 0x6e, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x08, 0x52, 0x05, 0x69, 0x73, 0x54, 0x78, 0x6e, 0x12, 0x1c, 0x0a, 0x05, 0x70,
	0x68, 0x61, 0x73, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x06, 0x2e, 0x50, 0x68, 0x61,
	0x73, 0x65, 0x52, 0x05, 0x70, 0x68, 0x61, 0x73, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x74, 0x73, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x04, 0x52, 0x02, 0x74, 0x73, 0x12, 0x27, 0x0a, 0x09, 0x6d, 0x75, 0x74,
	0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x09, 0x2e, 0x4d,
	0x75, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x09, 0x6d, 0x75, 0x74, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x73, 0x2a, 0x16, 0x0a, 0x02, 0x4f, 0x70, 0x12, 0x07, 0x0a, 0x03, 0x50, 0x55, 0x54, 0x10,
	0x00, 0x12, 0x07, 0x0a, 0x03, 0x44, 0x45, 0x4c, 0x10, 0x01, 0x2a, 0x35, 0x0a, 0x05, 0x50, 0x68,
	0x61, 0x73, 0x65, 0x12, 0x08, 0x0a, 0x04, 0x4e, 0x4f, 0x4e, 0x45, 0x10, 0x00, 0x12, 0x0b, 0x0a,
	0x07, 0x50, 0x52, 0x45, 0x50, 0x41, 0x52, 0x45, 0x10, 0x01, 0x12, 0x0a, 0x0a, 0x06, 0x43, 0x4f,
	0x4d, 0x4d, 0x49, 0x54, 0x10, 0x02, 0x12, 0x09, 0x0a, 0x05, 0x41, 0x42, 0x4f, 0x52, 0x54, 0x10,
	0x03, 0x42, 0x23, 0x5a, 0x21, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f,
	0x62, 0x6f, 0x6f, 0x74, 0x6a, 0x70, 0x2f, 0x65, 0x6c, 0x61, 0x73, 0x74, 0x69, 0x63, 0x6b, 0x76,
	0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_internal_proto_rawDescOnce sync.Once
	file_internal_proto_rawDescData = file_internal_proto_rawDesc
)

func file_internal_proto_rawDescGZIP() []byte {
	file_internal_proto_rawDescOnce.Do(func() {
		file_internal_proto_rawDescData = protoimpl.X.CompressGZIP(file_internal_proto_rawDescData)
	})
	return file_internal_proto_rawDescData
}

var file_internal_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_internal_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_internal_proto_goTypes = []interface{}{
	(Op)(0),          // 0: Op
	(Phase)(0),       // 1: Phase
	(*Mutation)(nil), // 2: Mutation
	(*Request)(nil),  // 3: Request
}
var file_internal_proto_depIdxs = []int32{
	0, // 0: Mutation.op:type_name -> Op
	1, // 1: Request.phase:type_name -> Phase
	2, // 2: Request.mutations:type_name -> Mutation
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_internal_proto_init() }
func file_internal_proto_init() {
	if File_internal_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_internal_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Mutation); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_internal_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Request); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_internal_proto_rawDesc,
			NumEnums:      2,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_internal_proto_goTypes,
		DependencyIndexes: file_internal_proto_depIdxs,
		EnumInfos:         file_internal_proto_enumTypes,
		MessageInfos:      file_internal_proto_msgTypes,
	}.Build()
	File_internal_proto = out.File
	file_internal_proto_rawDesc = nil
	file_internal_proto_goTypes = nil
	file_internal_proto_depIdxs = nil
}