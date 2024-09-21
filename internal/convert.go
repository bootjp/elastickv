package internal

import (
	"math"

	"github.com/cockroachdb/errors"
)

var ErrIntOverflow = errors.New("int64 に変換できません（オーバーフロー）")

func Uint64ToInt64(u uint64) (int64, error) {
	if u > math.MaxInt64 {
		return 0, ErrIntOverflow
	}
	return int64(u), nil
}

func Uint64ToInt(u uint64) (int, error) {
	if u > math.MaxInt64 {
		return 0, ErrIntOverflow
	}
	return int(u), nil
}
