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

type Mutation_Operation int32

const (
	Mutation_UNKNOWN Mutation_Operation = 0
	Mutation_PUT     Mutation_Operation = 1
	Mutation_GET     Mutation_Operation = 2
	Mutation_DELETE  Mutation_Operation = 3
)

// Enum value maps for Mutation_Operation.
var (
	Mutation_Operation_name = map[int32]string{
		0: "UNKNOWN",
		1: "PUT",
		2: "GET",
		3: "DELETE",
	}
	Mutation_Operation_value = map[string]int32{
		"UNKNOWN": 0,
		"PUT":     1,
		"GET":     2,
		"DELETE":  3,
	}
)

func (x Mutation_Operation) Enum() *Mutation_Operation {
	p := new(Mutation_Operation)
	*p = x
	return p
}

func (x Mutation_Operation) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Mutation_Operation) Descriptor() protoreflect.EnumDescriptor {
	return file_internal_proto_enumTypes[0].Descriptor()
}

func (Mutation_Operation) Type() protoreflect.EnumType {
	return &file_internal_proto_enumTypes[0]
}

func (x Mutation_Operation) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Mutation_Operation.Descriptor instead.
func (Mutation_Operation) EnumDescriptor() ([]byte, []int) {
	return file_internal_proto_rawDescGZIP(), []int{0, 0}
}

type Mutation struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Op    Mutation_Operation `protobuf:"varint,1,opt,name=op,proto3,enum=Mutation_Operation" json:"op,omitempty"`
	Key   []byte             `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
	Value []byte             `protobuf:"bytes,3,opt,name=value,proto3" json:"value,omitempty"`
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

func (x *Mutation) GetOp() Mutation_Operation {
	if x != nil {
		return x.Op
	}
	return Mutation_UNKNOWN
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

	// Types that are assignable to Request:
	//
	//	*Request_Put
	//	*Request_Delete
	//	*Request_PreCommit
	//	*Request_Commit
	Request isRequest_Request `protobuf_oneof:"request"`
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

func (m *Request) GetRequest() isRequest_Request {
	if m != nil {
		return m.Request
	}
	return nil
}

func (x *Request) GetPut() *Request_PutRequest {
	if x, ok := x.GetRequest().(*Request_Put); ok {
		return x.Put
	}
	return nil
}

func (x *Request) GetDelete() *Request_DeleteRequest {
	if x, ok := x.GetRequest().(*Request_Delete); ok {
		return x.Delete
	}
	return nil
}

func (x *Request) GetPreCommit() *Request_PreCommitRequest {
	if x, ok := x.GetRequest().(*Request_PreCommit); ok {
		return x.PreCommit
	}
	return nil
}

func (x *Request) GetCommit() *Request_CommitRequest {
	if x, ok := x.GetRequest().(*Request_Commit); ok {
		return x.Commit
	}
	return nil
}

type isRequest_Request interface {
	isRequest_Request()
}

type Request_Put struct {
	Put *Request_PutRequest `protobuf:"bytes,1,opt,name=put,proto3,oneof"`
}

type Request_Delete struct {
	Delete *Request_DeleteRequest `protobuf:"bytes,2,opt,name=delete,proto3,oneof"`
}

type Request_PreCommit struct {
	PreCommit *Request_PreCommitRequest `protobuf:"bytes,10,opt,name=pre_commit,json=preCommit,proto3,oneof"`
}

type Request_Commit struct {
	Commit *Request_CommitRequest `protobuf:"bytes,11,opt,name=commit,proto3,oneof"`
}

func (*Request_Put) isRequest_Request() {}

func (*Request_Delete) isRequest_Request() {}

func (*Request_PreCommit) isRequest_Request() {}

func (*Request_Commit) isRequest_Request() {}

type Request_PreCommitRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Mutations []*Mutation `protobuf:"bytes,1,rep,name=mutations,proto3" json:"mutations,omitempty"`
	StartTs   uint64      `protobuf:"varint,2,opt,name=start_ts,json=startTs,proto3" json:"start_ts,omitempty"`
}

func (x *Request_PreCommitRequest) Reset() {
	*x = Request_PreCommitRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Request_PreCommitRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Request_PreCommitRequest) ProtoMessage() {}

func (x *Request_PreCommitRequest) ProtoReflect() protoreflect.Message {
	mi := &file_internal_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Request_PreCommitRequest.ProtoReflect.Descriptor instead.
func (*Request_PreCommitRequest) Descriptor() ([]byte, []int) {
	return file_internal_proto_rawDescGZIP(), []int{1, 0}
}

func (x *Request_PreCommitRequest) GetMutations() []*Mutation {
	if x != nil {
		return x.Mutations
	}
	return nil
}

func (x *Request_PreCommitRequest) GetStartTs() uint64 {
	if x != nil {
		return x.StartTs
	}
	return 0
}

type Request_CommitRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key     [][]byte `protobuf:"bytes,1,rep,name=key,proto3" json:"key,omitempty"`
	StartTs uint64   `protobuf:"varint,2,opt,name=start_ts,json=startTs,proto3" json:"start_ts,omitempty"`
}

func (x *Request_CommitRequest) Reset() {
	*x = Request_CommitRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Request_CommitRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Request_CommitRequest) ProtoMessage() {}

func (x *Request_CommitRequest) ProtoReflect() protoreflect.Message {
	mi := &file_internal_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Request_CommitRequest.ProtoReflect.Descriptor instead.
func (*Request_CommitRequest) Descriptor() ([]byte, []int) {
	return file_internal_proto_rawDescGZIP(), []int{1, 1}
}

func (x *Request_CommitRequest) GetKey() [][]byte {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *Request_CommitRequest) GetStartTs() uint64 {
	if x != nil {
		return x.StartTs
	}
	return 0
}

type Request_PutRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key   []byte `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *Request_PutRequest) Reset() {
	*x = Request_PutRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Request_PutRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Request_PutRequest) ProtoMessage() {}

func (x *Request_PutRequest) ProtoReflect() protoreflect.Message {
	mi := &file_internal_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Request_PutRequest.ProtoReflect.Descriptor instead.
func (*Request_PutRequest) Descriptor() ([]byte, []int) {
	return file_internal_proto_rawDescGZIP(), []int{1, 2}
}

func (x *Request_PutRequest) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *Request_PutRequest) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

type Request_DeleteRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key []byte `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
}

func (x *Request_DeleteRequest) Reset() {
	*x = Request_DeleteRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Request_DeleteRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Request_DeleteRequest) ProtoMessage() {}

func (x *Request_DeleteRequest) ProtoReflect() protoreflect.Message {
	mi := &file_internal_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Request_DeleteRequest.ProtoReflect.Descriptor instead.
func (*Request_DeleteRequest) Descriptor() ([]byte, []int) {
	return file_internal_proto_rawDescGZIP(), []int{1, 3}
}

func (x *Request_DeleteRequest) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

var File_internal_proto protoreflect.FileDescriptor

var file_internal_proto_rawDesc = []byte{
	0x0a, 0x0e, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x22, 0x8f, 0x01, 0x0a, 0x08, 0x4d, 0x75, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x23, 0x0a,
	0x02, 0x6f, 0x70, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x13, 0x2e, 0x4d, 0x75, 0x74, 0x61,
	0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x02,
	0x6f, 0x70, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52,
	0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x03, 0x20,
	0x01, 0x28, 0x0c, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x36, 0x0a, 0x09, 0x4f, 0x70,
	0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x0b, 0x0a, 0x07, 0x55, 0x4e, 0x4b, 0x4e, 0x4f,
	0x57, 0x4e, 0x10, 0x00, 0x12, 0x07, 0x0a, 0x03, 0x50, 0x55, 0x54, 0x10, 0x01, 0x12, 0x07, 0x0a,
	0x03, 0x47, 0x45, 0x54, 0x10, 0x02, 0x12, 0x0a, 0x0a, 0x06, 0x44, 0x45, 0x4c, 0x45, 0x54, 0x45,
	0x10, 0x03, 0x22, 0xcc, 0x03, 0x0a, 0x07, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x27,
	0x0a, 0x03, 0x70, 0x75, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x13, 0x2e, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x2e, 0x50, 0x75, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x48, 0x00, 0x52, 0x03, 0x70, 0x75, 0x74, 0x12, 0x30, 0x0a, 0x06, 0x64, 0x65, 0x6c, 0x65, 0x74,
	0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x2e, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x48,
	0x00, 0x52, 0x06, 0x64, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x12, 0x3a, 0x0a, 0x0a, 0x70, 0x72, 0x65,
	0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x2e, 0x50, 0x72, 0x65, 0x43, 0x6f, 0x6d, 0x6d, 0x69,
	0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x48, 0x00, 0x52, 0x09, 0x70, 0x72, 0x65, 0x43,
	0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x12, 0x30, 0x0a, 0x06, 0x63, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x18,
	0x0b, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x2e,
	0x43, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x48, 0x00, 0x52,
	0x06, 0x63, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x1a, 0x56, 0x0a, 0x10, 0x50, 0x72, 0x65, 0x43, 0x6f,
	0x6d, 0x6d, 0x69, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x27, 0x0a, 0x09, 0x6d,
	0x75, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x09,
	0x2e, 0x4d, 0x75, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x09, 0x6d, 0x75, 0x74, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x73, 0x12, 0x19, 0x0a, 0x08, 0x73, 0x74, 0x61, 0x72, 0x74, 0x5f, 0x74, 0x73,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x07, 0x73, 0x74, 0x61, 0x72, 0x74, 0x54, 0x73, 0x1a,
	0x3c, 0x0a, 0x0d, 0x43, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0c, 0x52, 0x03, 0x6b,
	0x65, 0x79, 0x12, 0x19, 0x0a, 0x08, 0x73, 0x74, 0x61, 0x72, 0x74, 0x5f, 0x74, 0x73, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x04, 0x52, 0x07, 0x73, 0x74, 0x61, 0x72, 0x74, 0x54, 0x73, 0x1a, 0x34, 0x0a,
	0x0a, 0x50, 0x75, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x6b,
	0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a,
	0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x76, 0x61,
	0x6c, 0x75, 0x65, 0x1a, 0x21, 0x0a, 0x0d, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x0c, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x42, 0x09, 0x0a, 0x07, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x42, 0x23, 0x5a, 0x21, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f,
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

var file_internal_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_internal_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_internal_proto_goTypes = []interface{}{
	(Mutation_Operation)(0),          // 0: Mutation.Operation
	(*Mutation)(nil),                 // 1: Mutation
	(*Request)(nil),                  // 2: Request
	(*Request_PreCommitRequest)(nil), // 3: Request.PreCommitRequest
	(*Request_CommitRequest)(nil),    // 4: Request.CommitRequest
	(*Request_PutRequest)(nil),       // 5: Request.PutRequest
	(*Request_DeleteRequest)(nil),    // 6: Request.DeleteRequest
}
var file_internal_proto_depIdxs = []int32{
	0, // 0: Mutation.op:type_name -> Mutation.Operation
	5, // 1: Request.put:type_name -> Request.PutRequest
	6, // 2: Request.delete:type_name -> Request.DeleteRequest
	3, // 3: Request.pre_commit:type_name -> Request.PreCommitRequest
	4, // 4: Request.commit:type_name -> Request.CommitRequest
	1, // 5: Request.PreCommitRequest.mutations:type_name -> Mutation
	6, // [6:6] is the sub-list for method output_type
	6, // [6:6] is the sub-list for method input_type
	6, // [6:6] is the sub-list for extension type_name
	6, // [6:6] is the sub-list for extension extendee
	0, // [0:6] is the sub-list for field type_name
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
		file_internal_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Request_PreCommitRequest); i {
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
		file_internal_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Request_CommitRequest); i {
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
		file_internal_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Request_PutRequest); i {
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
		file_internal_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Request_DeleteRequest); i {
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
	file_internal_proto_msgTypes[1].OneofWrappers = []interface{}{
		(*Request_Put)(nil),
		(*Request_Delete)(nil),
		(*Request_PreCommit)(nil),
		(*Request_Commit)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_internal_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   6,
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
