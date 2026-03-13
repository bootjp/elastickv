package adapter

import (
	"bytes"
	"encoding/json"
	"maps"
	"reflect"
	"strings"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	gproto "google.golang.org/protobuf/proto"
)

var (
	storedDynamoSchemaProtoPrefix = []byte{0x00, 'D', 'S', 0x01}
	storedDynamoItemProtoPrefix   = []byte{0x00, 'D', 'I', 0x01}
	storedDynamoMarshalOptions    = gproto.MarshalOptions{Deterministic: true}

	errStoredDynamoMessageTooLarge        = errors.New("stored dynamo message too large")
	errNilDynamoTableSchema               = errors.New("nil dynamo table schema")
	errInvalidDynamoKeyEncodingVersion    = errors.New("invalid key encoding version")
	errDynamoKeyEncodingVersionOverflow   = errors.New("dynamo key encoding version overflows int")
	errNilDynamoItem                      = errors.New("nil dynamo item")
	errDynamoAttributeValueNestingTooDeep = errors.New("attribute value nesting exceeds maximum depth")
	errInvalidDynamoAttributeValue        = errors.New("invalid attribute value")
	errDynamoNullAttributeMustBeSet       = errors.New("dynamodb NULL attribute must be set")
	errDynamoNullAttributeMustBeTrue      = errors.New("dynamodb NULL attribute must be true")

	dynamoAttributeValueProtoEncoders = map[attributeValueKind]func(attributeValue) *pb.DynamoAttributeValue{
		attributeValueKindString:    dynamoStringAttributeValueToProto,
		attributeValueKindNumber:    dynamoNumberAttributeValueToProto,
		attributeValueKindBinary:    dynamoBinaryAttributeValueToProto,
		attributeValueKindBool:      dynamoBoolAttributeValueToProto,
		attributeValueKindStringSet: dynamoStringSetAttributeValueToProto,
		attributeValueKindNumberSet: dynamoNumberSetAttributeValueToProto,
		attributeValueKindBinarySet: dynamoBinarySetAttributeValueToProto,
	}

	dynamoAttributeValueProtoDecoders = map[reflect.Type]func(any, int) (attributeValue, error){
		reflect.TypeFor[*pb.DynamoAttributeValue_S]():         dynamoStringAttributeValueFromProto,
		reflect.TypeFor[*pb.DynamoAttributeValue_N]():         dynamoNumberAttributeValueFromProto,
		reflect.TypeFor[*pb.DynamoAttributeValue_B]():         dynamoBinaryAttributeValueFromProto,
		reflect.TypeFor[*pb.DynamoAttributeValue_BoolValue](): dynamoBoolAttributeValueFromProto,
		reflect.TypeFor[*pb.DynamoAttributeValue_NullValue](): dynamoNullAttributeValueFromProto,
		reflect.TypeFor[*pb.DynamoAttributeValue_Ss]():        dynamoStringSetAttributeValueFromProto,
		reflect.TypeFor[*pb.DynamoAttributeValue_Ns]():        dynamoNumberSetAttributeValueFromProto,
		reflect.TypeFor[*pb.DynamoAttributeValue_Bs]():        dynamoBinarySetAttributeValueFromProto,
	}
)

func encodeStoredDynamoTableSchema(schema *dynamoTableSchema) ([]byte, error) {
	msg, err := dynamoTableSchemaToProto(schema)
	if err != nil {
		return nil, err
	}
	return marshalStoredDynamoMessage(storedDynamoSchemaProtoPrefix, msg)
}

func decodeStoredDynamoTableSchema(b []byte) (*dynamoTableSchema, error) {
	if hasStoredDynamoPrefix(b, storedDynamoSchemaProtoPrefix) {
		msg := &pb.DynamoTableSchema{}
		if err := gproto.Unmarshal(b[len(storedDynamoSchemaProtoPrefix):], msg); err != nil {
			return nil, errors.WithStack(err)
		}
schema, err := dynamoTableSchemaFromProto(msg)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return schema, nil
	}

	schema := &dynamoTableSchema{}
	if err := json.Unmarshal(b, schema); err != nil {
		return nil, errors.WithStack(err)
	}
	return schema, nil
}

func encodeStoredDynamoItem(item map[string]attributeValue) ([]byte, error) {
	msg, err := dynamoItemToProto(item)
	if err != nil {
		return nil, err
	}
	return marshalStoredDynamoMessage(storedDynamoItemProtoPrefix, msg)
}

func decodeStoredDynamoItem(b []byte) (map[string]attributeValue, error) {
	if hasStoredDynamoPrefix(b, storedDynamoItemProtoPrefix) {
		msg := &pb.DynamoItem{}
		if err := gproto.Unmarshal(b[len(storedDynamoItemProtoPrefix):], msg); err != nil {
			return nil, errors.WithStack(err)
		}
item, err := dynamoItemFromProto(msg)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return item, nil
	}

	item := map[string]attributeValue{}
	if err := json.Unmarshal(b, &item); err != nil {
		return nil, errors.WithStack(err)
	}
	return item, nil
}

func marshalStoredDynamoMessage(prefix []byte, msg gproto.Message) ([]byte, error) {
	body, err := storedDynamoMarshalOptions.Marshal(msg)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	prefixLen := uint64(len(prefix))
	bodyLen := uint64(len(body))
	maxInt := uint64(int(^uint(0) >> 1))
	if prefixLen > maxInt || bodyLen > maxInt-prefixLen {
		return nil, errStoredDynamoMessageTooLarge
	}

	return append(bytes.Clone(prefix), body...), nil
}

func hasStoredDynamoPrefix(b []byte, prefix []byte) bool {
	return len(b) >= len(prefix) && bytes.Equal(b[:len(prefix)], prefix)
}

func dynamoTableSchemaToProto(schema *dynamoTableSchema) (*pb.DynamoTableSchema, error) {
	if schema == nil {
		return nil, errNilDynamoTableSchema
	}
	if schema.KeyEncodingVersion < 0 {
		return nil, errInvalidDynamoKeyEncodingVersion
	}

	gsis := make(map[string]*pb.DynamoGlobalSecondaryIndex, len(schema.GlobalSecondaryIndexes))
	for name, gsi := range schema.GlobalSecondaryIndexes {
		gsis[name] = dynamoGlobalSecondaryIndexToProto(gsi)
	}

	return &pb.DynamoTableSchema{
		TableName:               schema.TableName,
		AttributeDefinitions:    cloneStringMap(schema.AttributeDefinitions),
		PrimaryKey:              dynamoKeySchemaToProto(schema.PrimaryKey),
		GlobalSecondaryIndexes:  gsis,
		KeyEncodingVersion:      uint64(schema.KeyEncodingVersion),
		MigratingFromGeneration: schema.MigratingFromGeneration,
		Generation:              schema.Generation,
	}, nil
}

func dynamoTableSchemaFromProto(msg *pb.DynamoTableSchema) (*dynamoTableSchema, error) {
	if msg == nil {
		return nil, errNilDynamoTableSchema
	}

	keyEncodingVersion, err := parseDynamoKeyEncodingVersion(msg.GetKeyEncodingVersion())
	if err != nil {
		return nil, err
	}

	gsis := make(map[string]dynamoGlobalSecondaryIndex, len(msg.GetGlobalSecondaryIndexes()))
	for name, gsi := range msg.GetGlobalSecondaryIndexes() {
		gsis[name] = dynamoGlobalSecondaryIndexFromProto(gsi)
	}

	return &dynamoTableSchema{
		TableName:               msg.GetTableName(),
		AttributeDefinitions:    cloneStringMap(msg.GetAttributeDefinitions()),
		PrimaryKey:              dynamoKeySchemaFromProto(msg.GetPrimaryKey()),
		GlobalSecondaryIndexes:  gsis,
		KeyEncodingVersion:      keyEncodingVersion,
		MigratingFromGeneration: msg.GetMigratingFromGeneration(),
		Generation:              msg.GetGeneration(),
	}, nil
}

func parseDynamoKeyEncodingVersion(v uint64) (int, error) {
	maxInt := uint64(int(^uint(0) >> 1))
	if v > maxInt {
		return 0, errDynamoKeyEncodingVersionOverflow
	}

	return int(v), nil
}

func dynamoKeySchemaToProto(schema dynamoKeySchema) *pb.DynamoKeySchema {
	return &pb.DynamoKeySchema{
		HashKey:  schema.HashKey,
		RangeKey: schema.RangeKey,
	}
}

func dynamoKeySchemaFromProto(msg *pb.DynamoKeySchema) dynamoKeySchema {
	if msg == nil {
		return dynamoKeySchema{}
	}
	return dynamoKeySchema{
		HashKey:  msg.GetHashKey(),
		RangeKey: msg.GetRangeKey(),
	}
}

func dynamoGlobalSecondaryIndexToProto(gsi dynamoGlobalSecondaryIndex) *pb.DynamoGlobalSecondaryIndex {
	return &pb.DynamoGlobalSecondaryIndex{
		KeySchema:  dynamoKeySchemaToProto(gsi.KeySchema),
		Projection: dynamoGSIProjectionToProto(gsi.Projection),
	}
}

func dynamoGlobalSecondaryIndexFromProto(msg *pb.DynamoGlobalSecondaryIndex) dynamoGlobalSecondaryIndex {
	gsi := dynamoGlobalSecondaryIndex{
		KeySchema:  dynamoKeySchemaFromProto(msg.GetKeySchema()),
		Projection: dynamoGSIProjectionFromProto(msg.GetProjection()),
	}
	if strings.TrimSpace(gsi.Projection.ProjectionType) == "" {
		gsi.Projection = dynamoGSIProjection{ProjectionType: "ALL"}
	}
	return gsi
}

func dynamoGSIProjectionToProto(projection dynamoGSIProjection) *pb.DynamoGSIProjection {
	return &pb.DynamoGSIProjection{
		ProjectionType:   projection.ProjectionType,
		NonKeyAttributes: cloneStringSlice(projection.NonKeyAttributes),
	}
}

func dynamoGSIProjectionFromProto(msg *pb.DynamoGSIProjection) dynamoGSIProjection {
	if msg == nil {
		return dynamoGSIProjection{}
	}
	return dynamoGSIProjection{
		ProjectionType:   msg.GetProjectionType(),
		NonKeyAttributes: cloneStringSlice(msg.GetNonKeyAttributes()),
	}
}

func dynamoItemToProto(item map[string]attributeValue) (*pb.DynamoItem, error) {
	attrs := make(map[string]*pb.DynamoAttributeValue, len(item))
	for name, value := range item {
		msg, err := dynamoAttributeValueToProto(value, 1)
		if err != nil {
			return nil, err
		}
		attrs[name] = msg
	}
	return &pb.DynamoItem{Attributes: attrs}, nil
}

func dynamoItemFromProto(msg *pb.DynamoItem) (map[string]attributeValue, error) {
	if msg == nil {
		return nil, errNilDynamoItem
	}
	item := make(map[string]attributeValue, len(msg.GetAttributes()))
	for name, value := range msg.GetAttributes() {
		attr, err := dynamoAttributeValueFromProto(value, 1)
		if err != nil {
			return nil, err
		}
		item[name] = attr
	}
	return item, nil
}

func dynamoAttributeValueToProto(value attributeValue, depth int) (*pb.DynamoAttributeValue, error) {
	if depth > maxAttributeValueNestingDepth {
		return nil, errDynamoAttributeValueNestingTooDeep
	}

	kind, count := detectAttributeValueKind(value)
	if count != 1 {
		return nil, errInvalidDynamoAttributeValue
	}

	encode := dynamoAttributeValueProtoEncoders[kind]
	if encode != nil {
		return encode(value), nil
	}
	if kind == attributeValueKindNull {
		return dynamoNullAttributeValueToProto(value)
	}
	if kind == attributeValueKindList {
		return dynamoListAttributeValueToProto(value, depth)
	}
	if kind == attributeValueKindMap {
		return dynamoMapAttributeValueToProto(value, depth)
	}
	return nil, errInvalidDynamoAttributeValue
}

func dynamoStringAttributeValueToProto(value attributeValue) *pb.DynamoAttributeValue {
	return &pb.DynamoAttributeValue{Value: &pb.DynamoAttributeValue_S{S: value.stringValue()}}
}

func dynamoNumberAttributeValueToProto(value attributeValue) *pb.DynamoAttributeValue {
	return &pb.DynamoAttributeValue{Value: &pb.DynamoAttributeValue_N{N: value.numberValue()}}
}

func dynamoBinaryAttributeValueToProto(value attributeValue) *pb.DynamoAttributeValue {
	return &pb.DynamoAttributeValue{Value: &pb.DynamoAttributeValue_B{B: value.binaryValue()}}
}

func dynamoBoolAttributeValueToProto(value attributeValue) *pb.DynamoAttributeValue {
	return &pb.DynamoAttributeValue{Value: &pb.DynamoAttributeValue_BoolValue{BoolValue: *value.BOOL}}
}

func dynamoNullAttributeValueToProto(value attributeValue) (*pb.DynamoAttributeValue, error) {
	if value.NULL == nil {
		return nil, errDynamoNullAttributeMustBeSet
	}
	return &pb.DynamoAttributeValue{Value: &pb.DynamoAttributeValue_NullValue{NullValue: true}}, nil
}

func dynamoStringSetAttributeValueToProto(value attributeValue) *pb.DynamoAttributeValue {
	return &pb.DynamoAttributeValue{
		Value: &pb.DynamoAttributeValue_Ss{Ss: &pb.DynamoStringSet{Values: cloneStringSlice(value.SS)}},
	}
}

func dynamoNumberSetAttributeValueToProto(value attributeValue) *pb.DynamoAttributeValue {
	return &pb.DynamoAttributeValue{
		Value: &pb.DynamoAttributeValue_Ns{Ns: &pb.DynamoNumberSet{Values: cloneStringSlice(value.NS)}},
	}
}

func dynamoBinarySetAttributeValueToProto(value attributeValue) *pb.DynamoAttributeValue {
	return &pb.DynamoAttributeValue{
		Value: &pb.DynamoAttributeValue_Bs{Bs: &pb.DynamoBinarySet{Values: cloneBinarySet(value.BS)}},
	}
}

func dynamoListAttributeValueToProto(value attributeValue, depth int) (*pb.DynamoAttributeValue, error) {
	list := make([]*pb.DynamoAttributeValue, len(value.L))
	for i := range value.L {
		elem, err := dynamoAttributeValueToProto(value.L[i], depth+1)
		if err != nil {
			return nil, err
		}
		list[i] = elem
	}
	return &pb.DynamoAttributeValue{
		Value: &pb.DynamoAttributeValue_L{L: &pb.DynamoAttributeValueList{Values: list}},
	}, nil
}

func dynamoMapAttributeValueToProto(value attributeValue, depth int) (*pb.DynamoAttributeValue, error) {
	m := make(map[string]*pb.DynamoAttributeValue, len(value.M))
	for key, elem := range value.M {
		msg, err := dynamoAttributeValueToProto(elem, depth+1)
		if err != nil {
			return nil, err
		}
		m[key] = msg
	}
	return &pb.DynamoAttributeValue{
		Value: &pb.DynamoAttributeValue_M{M: &pb.DynamoAttributeValueMap{Values: m}},
	}, nil
}

func dynamoAttributeValueFromProto(msg *pb.DynamoAttributeValue, depth int) (attributeValue, error) {
	if depth > maxAttributeValueNestingDepth {
		return attributeValue{}, errDynamoAttributeValueNestingTooDeep
	}
	if msg == nil {
		return attributeValue{}, errInvalidDynamoAttributeValue
	}

	decode := dynamoAttributeValueProtoDecoders[reflect.TypeOf(msg.Value)]
	if decode != nil {
		return decode(msg.Value, depth)
	}
	if typed, ok := msg.Value.(*pb.DynamoAttributeValue_L); ok {
		return dynamoListAttributeValueFromProto(typed.L, depth)
	}
	if typed, ok := msg.Value.(*pb.DynamoAttributeValue_M); ok {
		return dynamoMapAttributeValueFromProto(typed.M, depth)
	}
	return attributeValue{}, errInvalidDynamoAttributeValue
}

func dynamoStringAttributeValueFromProto(value any, _ int) (attributeValue, error) {
	typed, ok := value.(*pb.DynamoAttributeValue_S)
	if !ok {
		return attributeValue{}, errInvalidDynamoAttributeValue
	}
	return newStringAttributeValue(typed.S), nil
}

func dynamoNumberAttributeValueFromProto(value any, _ int) (attributeValue, error) {
	typed, ok := value.(*pb.DynamoAttributeValue_N)
	if !ok {
		return attributeValue{}, errInvalidDynamoAttributeValue
	}
	numberValue := typed.N
	return attributeValue{N: &numberValue}, nil
}

func dynamoBinaryAttributeValueFromProto(value any, _ int) (attributeValue, error) {
	typed, ok := value.(*pb.DynamoAttributeValue_B)
	if !ok {
		return attributeValue{}, errInvalidDynamoAttributeValue
	}
	return attributeValue{B: bytes.Clone(typed.B)}, nil
}

func dynamoBoolAttributeValueFromProto(value any, _ int) (attributeValue, error) {
	typed, ok := value.(*pb.DynamoAttributeValue_BoolValue)
	if !ok {
		return attributeValue{}, errInvalidDynamoAttributeValue
	}
	boolValue := typed.BoolValue
	return attributeValue{BOOL: &boolValue}, nil
}

func dynamoNullAttributeValueFromProto(value any, _ int) (attributeValue, error) {
	typed, ok := value.(*pb.DynamoAttributeValue_NullValue)
	if !ok {
		return attributeValue{}, errInvalidDynamoAttributeValue
	}
	if !typed.NullValue {
		return attributeValue{}, errDynamoNullAttributeMustBeTrue
	}
	nullValue := true
	return attributeValue{NULL: &nullValue}, nil
}

func dynamoStringSetAttributeValueFromProto(value any, _ int) (attributeValue, error) {
	typed, ok := value.(*pb.DynamoAttributeValue_Ss)
	if !ok {
		return attributeValue{}, errInvalidDynamoAttributeValue
	}
	return attributeValue{SS: cloneStringSlice(typed.Ss.GetValues())}, nil
}

func dynamoNumberSetAttributeValueFromProto(value any, _ int) (attributeValue, error) {
	typed, ok := value.(*pb.DynamoAttributeValue_Ns)
	if !ok {
		return attributeValue{}, errInvalidDynamoAttributeValue
	}
	return attributeValue{NS: cloneStringSlice(typed.Ns.GetValues())}, nil
}

func dynamoBinarySetAttributeValueFromProto(value any, _ int) (attributeValue, error) {
	typed, ok := value.(*pb.DynamoAttributeValue_Bs)
	if !ok {
		return attributeValue{}, errInvalidDynamoAttributeValue
	}
	return attributeValue{BS: cloneBinarySet(typed.Bs.GetValues())}, nil
}

func dynamoListAttributeValueFromProto(
	list *pb.DynamoAttributeValueList,
	depth int,
) (attributeValue, error) {
	out := make([]attributeValue, len(list.GetValues()))
	for i, elem := range list.GetValues() {
		attr, err := dynamoAttributeValueFromProto(elem, depth+1)
		if err != nil {
			return attributeValue{}, err
		}
		out[i] = attr
	}
	return attributeValue{L: out}, nil
}

func dynamoMapAttributeValueFromProto(
	m *pb.DynamoAttributeValueMap,
	depth int,
) (attributeValue, error) {
	out := make(map[string]attributeValue, len(m.GetValues()))
	for key, elem := range m.GetValues() {
		attr, err := dynamoAttributeValueFromProto(elem, depth+1)
		if err != nil {
			return attributeValue{}, err
		}
		out[key] = attr
	}
	return attributeValue{M: out}, nil
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	maps.Copy(out, in)
	return out
}
