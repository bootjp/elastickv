package backup

import (
	"encoding/binary"
	"math/big"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

// encode_dynamodb_numeric.go reproduces the live adapter's numeric
// primary-key ordered encoding (adapter/dynamodb.go: encodeNumericKeyBytes
// and helpers) so the item encoder can build keys for N (number) hash and
// range keys that the live store sorts and looks up identically. (M3b-2)
//
// The scheme is order-preserving over the byte-lexicographic comparison
// Pebble uses: a sign marker (0x00 negative / 0x01 zero / 0x02 positive)
// followed, for non-zero values, by the order-encoded decimal exponent, a
// 0x00 separator, and the significant digits — with the whole body
// bit-inverted for negatives so larger magnitudes sort earlier. The result
// is then escaped+terminated by encodeDDBOrderedKeySegment, exactly as the
// live attributeValueAsKeySegment wraps attributeValueAsKeyBytes.

const (
	ddbNumNegativeMarker = byte(0x00)
	ddbNumZeroMarker     = byte(0x01)
	ddbNumPositiveMarker = byte(0x02)
)

// ddbNumericKeyBytes maps a DynamoDB number literal to its order-preserving
// key bytes (the raw bytes, before the segment escape/terminator).
func ddbNumericKeyBytes(v string) ([]byte, error) {
	negative, exponent, digits, err := parseDDBNumericKeyParts(v)
	if err != nil {
		return nil, err
	}
	if len(digits) == 0 {
		return []byte{ddbNumZeroMarker}, nil
	}
	body := encodeDDBOrderedSignedInt64(exponent)
	body = append(body, ddbKeyEscapeByte)
	body = append(body, digits...)
	if !negative {
		return append([]byte{ddbNumPositiveMarker}, body...), nil
	}
	return append([]byte{ddbNumNegativeMarker}, ddbInvertBytes(body)...), nil
}

// parseDDBNumericKeyParts splits a number literal into sign, decimal
// exponent, and significant digits. A zero value yields empty digits.
func parseDDBNumericKeyParts(v string) (bool, int64, []byte, error) {
	trimmed, negative, exp10, err := parseDDBNumericLiteral(v)
	if err != nil {
		return false, 0, nil, err
	}
	digits, exponent, zero, err := normalizeDDBNumericParts(trimmed, exp10)
	if err != nil {
		return false, 0, nil, err
	}
	if zero {
		return false, 0, nil, nil
	}
	return negative, exponent, digits, nil
}

// parseDDBNumericLiteral strips an optional sign and eE-exponent suffix.
func parseDDBNumericLiteral(v string) (string, bool, int64, error) {
	trimmed := strings.TrimSpace(v)
	if trimmed == "" {
		return "", false, 0, errors.Wrap(ErrDDBEncodeInvalidItem, "empty number literal")
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
		return "", false, 0, errors.Wrap(ErrDDBEncodeInvalidItem, "number literal is sign only")
	}
	exp10 := int64(0)
	if idx := strings.IndexAny(trimmed, "eE"); idx >= 0 {
		expPart := strings.TrimSpace(trimmed[idx+1:])
		trimmed = trimmed[:idx]
		parsed, err := parseDDBNumericExponent(expPart)
		if err != nil {
			return "", false, 0, err
		}
		exp10 = parsed
	}
	return trimmed, negative, exp10, nil
}

func parseDDBNumericExponent(expPart string) (int64, error) {
	if expPart == "" {
		return 0, errors.Wrap(ErrDDBEncodeInvalidItem, "empty exponent")
	}
	parsed, err := strconv.ParseInt(expPart, 10, 64)
	if err != nil {
		return 0, errors.Wrapf(ErrDDBEncodeInvalidItem, "bad exponent %q", expPart)
	}
	return parsed, nil
}

// normalizeDDBNumericParts trims leading/trailing zeros and computes the
// decimal exponent of the first significant digit. The third return is
// true when the value is zero.
func normalizeDDBNumericParts(trimmed string, exp10 int64) ([]byte, int64, bool, error) {
	intPart, fracPart, err := splitDDBNumericMantissa(trimmed)
	if err != nil {
		return nil, 0, false, err
	}
	combined := intPart + fracPart
	leadingZeros := ddbLeadingZeroCount(combined)
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

func splitDDBNumericMantissa(trimmed string) (string, string, error) {
	if strings.Count(trimmed, ".") > 1 {
		return "", "", errors.Wrap(ErrDDBEncodeInvalidItem, "number has multiple decimal points")
	}
	intPart := trimmed
	fracPart := ""
	if before, after, ok := strings.Cut(trimmed, "."); ok {
		intPart = before
		fracPart = after
	}
	if intPart == "" && fracPart == "" {
		return "", "", errors.Wrap(ErrDDBEncodeInvalidItem, "number has no digits")
	}
	if !ddbDecimalDigitsOnly(intPart) || !ddbDecimalDigitsOnly(fracPart) {
		return "", "", errors.Wrap(ErrDDBEncodeInvalidItem, "number has non-decimal characters")
	}
	return intPart, fracPart, nil
}

func ddbLeadingZeroCount(v string) int {
	count := 0
	for count < len(v) && v[count] == '0' {
		count++
	}
	return count
}

func ddbDecimalDigitsOnly(v string) bool {
	for i := range v {
		if v[i] < '0' || v[i] > '9' {
			return false
		}
	}
	return true
}

// encodeDDBOrderedSignedInt64 order-encodes a signed exponent: 0x00 +
// inverted magnitude (negative), 0x01 (zero), or 0x02 + magnitude
// (positive).
func encodeDDBOrderedSignedInt64(v int64) []byte {
	switch {
	case v < 0:
		return append([]byte{ddbNumNegativeMarker}, ddbInvertBytes(encodeDDBOrderedUint64(ddbSignedMagnitude(v)))...)
	case v == 0:
		return []byte{ddbNumZeroMarker}
	default:
		return append([]byte{ddbNumPositiveMarker}, encodeDDBOrderedUint64(uint64(v))...)
	}
}

func ddbSignedMagnitude(v int64) uint64 {
	if v >= 0 {
		return uint64(v)
	}
	abs := big.NewInt(v)
	abs.Abs(abs)
	return abs.Uint64()
}

// ddbOrderedUint64LengthPrefix[width] == width; the array lookup avoids an
// int->byte conversion (and keeps gosec quiet), matching the live code.
var ddbOrderedUint64LengthPrefix = [...]byte{0, 1, 2, 3, 4, 5, 6, 7, 8}

// encodeDDBOrderedUint64 emits a length-prefixed big-endian magnitude with
// leading zero bytes stripped, so shorter (smaller) magnitudes sort first.
func encodeDDBOrderedUint64(v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	start := 0
	for start < len(buf)-1 && buf[start] == 0 {
		start++
	}
	width := len(buf) - start
	out := make([]byte, 0, width+1)
	out = append(out, ddbOrderedUint64LengthPrefix[width])
	out = append(out, buf[start:]...)
	return out
}

func ddbInvertBytes(in []byte) []byte {
	out := make([]byte, len(in))
	for i := range in {
		out[i] = ^in[i]
	}
	return out
}
