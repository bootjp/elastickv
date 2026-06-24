package adapter

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"math/big"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

var attributeValueKeyExtractors = map[attributeValueKind]func(attributeValue) string{
	attributeValueKindString: func(attr attributeValue) string { return attr.stringValue() },
	attributeValueKindNumber: func(attr attributeValue) string { return attr.numberValue() },
	attributeValueKindBinary: func(attr attributeValue) string { return string(attr.B) },
}

var attributeValueKeyByteExtractors = map[attributeValueKind]func(attributeValue) []byte{
	attributeValueKindString: func(attr attributeValue) []byte {
		return []byte(attr.stringValue())
	},
	attributeValueKindBinary: func(attr attributeValue) []byte {
		return bytes.Clone(attr.B)
	},
}

var attributeValueScalarEqualityComparators = map[attributeValueKind]func(attributeValue, attributeValue) bool{
	attributeValueKindString: func(left attributeValue, right attributeValue) bool { return left.stringValue() == right.stringValue() },
	attributeValueKindNumber: numberAttributeValueEqual,
	attributeValueKindBinary: func(left attributeValue, right attributeValue) bool { return bytes.Equal(left.B, right.B) },
	attributeValueKindBool:   func(left attributeValue, right attributeValue) bool { return *left.BOOL == *right.BOOL },
	attributeValueKindNull:   func(left attributeValue, right attributeValue) bool { return *left.NULL == *right.NULL },
	attributeValueKindStringSet: func(left attributeValue, right attributeValue) bool {
		return unorderedStringSlicesEqual(left.SS, right.SS)
	},
	attributeValueKindNumberSet: func(left attributeValue, right attributeValue) bool {
		return unorderedNumberSlicesEqual(left.NS, right.NS)
	},
	attributeValueKindBinarySet: func(left attributeValue, right attributeValue) bool {
		return unorderedBinarySlicesEqual(left.BS, right.BS)
	},
}

var attributeValueSortFormatters = map[attributeValueKind]func(attributeValue) string{
	attributeValueKindString:    func(attr attributeValue) string { return attr.stringValue() },
	attributeValueKindNumber:    func(attr attributeValue) string { return attr.numberValue() },
	attributeValueKindBinary:    func(attr attributeValue) string { return base64.RawURLEncoding.EncodeToString(attr.B) },
	attributeValueKindBool:      formatBoolAttributeValue,
	attributeValueKindNull:      func(attributeValue) string { return "" },
	attributeValueKindStringSet: func(attr attributeValue) string { return strings.Join(sortedStringSlice(attr.SS), "\x00") },
	attributeValueKindNumberSet: func(attr attributeValue) string { return strings.Join(sortedNumberStrings(attr.NS), "\x00") },
	attributeValueKindBinarySet: func(attr attributeValue) string { return strings.Join(sortedBinaryStrings(attr.BS), "\x00") },
}

func attributeValueAsKey(attr attributeValue) (string, error) {
	kind, count := detectAttributeValueKind(attr)
	if count != 1 {
		return "", errors.New("unsupported key attribute type")
	}
	extract, ok := attributeValueKeyExtractors[kind]
	if !ok {
		return "", errors.New("unsupported key attribute type")
	}
	return extract(attr), nil
}

func attributeValueAsKeyBytes(attr attributeValue) ([]byte, error) {
	kind, count := detectAttributeValueKind(attr)
	if count != 1 {
		return nil, errors.New("unsupported key attribute type")
	}
	if kind == attributeValueKindNumber {
		return encodeNumericKeyBytes(attr.numberValue())
	}
	extract, ok := attributeValueKeyByteExtractors[kind]
	if !ok {
		return nil, errors.New("unsupported key attribute type")
	}
	return extract(attr), nil
}

func attributeValueAsKeySegment(attr attributeValue) ([]byte, error) {
	raw, err := attributeValueAsKeyBytes(attr)
	if err != nil {
		return nil, err
	}
	return encodeDynamoKeySegment(raw), nil
}

type numericKeyParts struct {
	negative bool
	exponent int64
	digits   []byte
}

func encodeNumericKeyBytes(v string) ([]byte, error) {
	parts, err := parseNumericKeyParts(v)
	if err != nil {
		return nil, err
	}
	if len(parts.digits) == 0 {
		return []byte{0x01}, nil
	}
	body := encodeOrderedSignedInt64(parts.exponent)
	body = append(body, dynamoKeyEscapeByte)
	body = append(body, parts.digits...)
	if !parts.negative {
		return append([]byte{0x02}, body...), nil
	}
	return append([]byte{0x00}, invertBytes(body)...), nil
}

func parseNumericKeyParts(v string) (numericKeyParts, error) {
	trimmed, negative, exp10, err := parseNumericKeyLiteral(v)
	if err != nil {
		return numericKeyParts{}, err
	}
	digits, exponent, zero, err := normalizeNumericKeyParts(trimmed, exp10)
	if err != nil {
		return numericKeyParts{}, err
	}
	if zero {
		return numericKeyParts{}, nil
	}
	return numericKeyParts{
		negative: negative,
		exponent: exponent,
		digits:   digits,
	}, nil
}

func parseNumericKeyLiteral(v string) (string, bool, int64, error) {
	trimmed := strings.TrimSpace(v)
	if trimmed == "" {
		return "", false, 0, errors.New("unsupported key attribute type")
	}

	negative := false
	switch trimmed[0] {
	case '+':
		trimmed = trimmed[1:]
	case '-':
		negative = true
		trimmed = trimmed[1:]
	}
	if trimmed == "" {
		return "", false, 0, errors.New("unsupported key attribute type")
	}

	exp10 := int64(0)
	if idx := strings.IndexAny(trimmed, "eE"); idx >= 0 {
		expPart := strings.TrimSpace(trimmed[idx+1:])
		trimmed = trimmed[:idx]
		parsedExp, err := parseNumericExponent(expPart)
		if err != nil {
			return "", false, 0, err
		}
		exp10 = parsedExp
	}
	return trimmed, negative, exp10, nil
}

func parseNumericExponent(expPart string) (int64, error) {
	if expPart == "" {
		return 0, errors.New("unsupported key attribute type")
	}
	parsedExp, err := strconv.ParseInt(expPart, 10, 64)
	if err != nil {
		return 0, errors.New("unsupported key attribute type")
	}
	return parsedExp, nil
}

func normalizeNumericKeyParts(trimmed string, exp10 int64) ([]byte, int64, bool, error) {
	intPart, fracPart, err := splitNumericMantissa(trimmed)
	if err != nil {
		return nil, 0, false, err
	}
	combined := intPart + fracPart
	leadingZeros := leadingZeroCount(combined)
	if leadingZeros == len(combined) {
		return nil, 0, true, nil
	}
	digits := []byte(strings.TrimRight(combined[leadingZeros:], "0"))
	if len(digits) == 0 {
		return nil, 0, true, nil
	}
	exponent := int64(len(intPart)) + exp10 - int64(leadingZeros)
	return digits, exponent, false, nil
}

func splitNumericMantissa(trimmed string) (string, string, error) {
	if strings.Count(trimmed, ".") > 1 {
		return "", "", errors.New("unsupported key attribute type")
	}
	intPart := trimmed
	fracPart := ""
	if before, after, ok := strings.Cut(trimmed, "."); ok {
		intPart = before
		fracPart = after
	}
	if intPart == "" && fracPart == "" {
		return "", "", errors.New("unsupported key attribute type")
	}
	if !decimalDigitsOnly(intPart) || !decimalDigitsOnly(fracPart) {
		return "", "", errors.New("unsupported key attribute type")
	}
	return intPart, fracPart, nil
}

func leadingZeroCount(v string) int {
	count := 0
	for count < len(v) && v[count] == '0' {
		count++
	}
	return count
}

func decimalDigitsOnly(v string) bool {
	for i := range v {
		if v[i] < '0' || v[i] > '9' {
			return false
		}
	}
	return true
}

func encodeOrderedSignedInt64(v int64) []byte {
	switch {
	case v < 0:
		return append([]byte{0x00}, invertBytes(encodeOrderedUint64(signedMagnitude(v)))...)
	case v == 0:
		return []byte{0x01}
	default:
		return append([]byte{0x02}, encodeOrderedUint64(uint64(v))...)
	}
}

func signedMagnitude(v int64) uint64 {
	if v >= 0 {
		return uint64(v)
	}
	abs := big.NewInt(v)
	abs.Abs(abs)
	return abs.Uint64()
}

var orderedUint64LengthPrefix = [...]byte{0, 1, 2, 3, 4, 5, 6, 7, 8}

func encodeOrderedUint64(v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	start := 0
	for start < len(buf)-1 && buf[start] == 0 {
		start++
	}
	width := len(buf) - start
	out := make([]byte, 0, width+1)
	out = append(out, orderedUint64LengthPrefix[width])
	out = append(out, buf[start:]...)
	return out
}

func invertBytes(in []byte) []byte {
	out := make([]byte, len(in))
	for i := range in {
		out[i] = ^in[i]
	}
	return out
}

func encodeDynamoKeySegment(raw []byte) []byte {
	out := encodeDynamoKeySegmentPrefix(raw)
	out = append(out, dynamoKeyEscapeByte, dynamoKeyTerminatorByte)
	return out
}

func encodeDynamoKeySegmentPrefix(raw []byte) []byte {
	return appendEscapedDynamoKeyBytes(make([]byte, 0, len(raw)+dynamoKeySegmentOverhead), raw)
}

func appendEscapedDynamoKeyBytes(dst []byte, raw []byte) []byte {
	for _, b := range raw {
		if b == dynamoKeyEscapeByte {
			dst = append(dst, dynamoKeyEscapeByte, dynamoKeyEscapedZeroByte)
			continue
		}
		dst = append(dst, b)
	}
	return dst
}

func attributeValueEqual(left attributeValue, right attributeValue) bool {
	leftKind, leftCount := detectAttributeValueKind(left)
	rightKind, rightCount := detectAttributeValueKind(right)
	if leftCount == 0 && rightCount == 0 {
		return true
	}
	if leftCount != 1 || rightCount != 1 || leftKind != rightKind {
		return false
	}
	if leftKind == attributeValueKindMap {
		return mapAttributeValueEqual(left, right)
	}
	if leftKind == attributeValueKindList {
		return listAttributeValueEqual(left, right)
	}
	compare, ok := attributeValueScalarEqualityComparators[leftKind]
	if !ok {
		return false
	}
	return compare(left, right)
}

func numberAttributeValueEqual(left attributeValue, right attributeValue) bool {
	cmp, ok := compareNumericAttributeString(left.numberValue(), right.numberValue())
	if !ok {
		return left.numberValue() == right.numberValue()
	}
	return cmp == 0
}

func mapAttributeValueEqual(left attributeValue, right attributeValue) bool {
	if len(left.M) != len(right.M) {
		return false
	}
	for key, leftValue := range left.M {
		rightValue, ok := right.M[key]
		if !ok || !attributeValueEqual(leftValue, rightValue) {
			return false
		}
	}
	return true
}

func listAttributeValueEqual(left attributeValue, right attributeValue) bool {
	if len(left.L) != len(right.L) {
		return false
	}
	for i := range left.L {
		if !attributeValueEqual(left.L[i], right.L[i]) {
			return false
		}
	}
	return true
}

func compareAttributeValueSortKey(left attributeValue, right attributeValue) int {
	if left.hasNumberType() && right.hasNumberType() {
		if cmp, ok := compareNumericAttributeString(left.numberValue(), right.numberValue()); ok {
			return cmp
		}
	}
	if left.hasBinaryType() && right.hasBinaryType() {
		return bytes.Compare(left.B, right.B)
	}
	return strings.Compare(attributeValueSortFallback(left), attributeValueSortFallback(right))
}

func compareNumericAttributeString(left string, right string) (int, bool) {
	leftRat := &big.Rat{}
	rightRat := &big.Rat{}
	if _, ok := leftRat.SetString(strings.TrimSpace(left)); !ok {
		return 0, false
	}
	if _, ok := rightRat.SetString(strings.TrimSpace(right)); !ok {
		return 0, false
	}
	return leftRat.Cmp(rightRat), true
}

func attributeValueSortFallback(attr attributeValue) string {
	kind, count := detectAttributeValueKind(attr)
	if count != 1 {
		return ""
	}
	format, ok := attributeValueSortFormatters[kind]
	if !ok {
		return ""
	}
	return format(attr)
}

func formatBoolAttributeValue(attr attributeValue) string {
	if *attr.BOOL {
		return "1"
	}
	return "0"
}

func unorderedStringSlicesEqual(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	lv := sortedStringSlice(left)
	rv := sortedStringSlice(right)
	for i := range lv {
		if lv[i] != rv[i] {
			return false
		}
	}
	return true
}

func unorderedNumberSlicesEqual(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	lv := sortedNumberStrings(left)
	rv := sortedNumberStrings(right)
	for i := range lv {
		if lv[i] != rv[i] {
			return false
		}
	}
	return true
}

func unorderedBinarySlicesEqual(left [][]byte, right [][]byte) bool {
	if len(left) != len(right) {
		return false
	}
	lv := sortedBinaryStrings(left)
	rv := sortedBinaryStrings(right)
	for i := range lv {
		if lv[i] != rv[i] {
			return false
		}
	}
	return true
}

func sortedStringSlice(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func sortedNumberStrings(in []string) []string {
	out := make([]string, len(in))
	for i := range in {
		out[i] = canonicalNumberString(in[i])
	}
	sort.Strings(out)
	return out
}

func sortedBinaryStrings(in [][]byte) []string {
	out := make([]string, len(in))
	for i := range in {
		out[i] = base64.RawURLEncoding.EncodeToString(in[i])
	}
	sort.Strings(out)
	return out
}

func canonicalNumberString(v string) string {
	rat := &big.Rat{}
	if _, ok := rat.SetString(strings.TrimSpace(v)); !ok {
		return strings.TrimSpace(v)
	}
	return rat.RatString()
}

func reverseItems(items []map[string]attributeValue) {
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}
}

func cloneAttributeValueMap(in map[string]attributeValue) map[string]attributeValue {
	if in == nil {
		return nil
	}
	out := make(map[string]attributeValue, len(in))
	for k, v := range in {
		out[k] = cloneAttributeValue(v)
	}
	return out
}

func cloneAttributeValueList(in []attributeValue) []attributeValue {
	if in == nil {
		return nil
	}
	out := make([]attributeValue, 0, len(in))
	for _, value := range in {
		out = append(out, cloneAttributeValue(value))
	}
	return out
}

func cloneAttributeValue(in attributeValue) attributeValue {
	out := attributeValue{}
	if in.S != nil {
		s := *in.S
		out.S = &s
	}
	if in.N != nil {
		n := *in.N
		out.N = &n
	}
	if in.B != nil {
		out.B = bytes.Clone(in.B)
	}
	if in.BOOL != nil {
		b := *in.BOOL
		out.BOOL = &b
	}
	if in.NULL != nil {
		n := *in.NULL
		out.NULL = &n
	}
	out.SS = cloneStringSlice(in.SS)
	out.NS = cloneStringSlice(in.NS)
	out.BS = cloneBinarySet(in.BS)
	if in.L != nil {
		out.L = make([]attributeValue, len(in.L))
		for i := range in.L {
			out.L[i] = cloneAttributeValue(in.L[i])
		}
	}
	if in.M != nil {
		out.M = cloneAttributeValueMap(in.M)
	}
	return out
}
