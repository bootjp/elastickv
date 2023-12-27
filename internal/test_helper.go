package internal

import "github.com/cockroachdb/errors"

func WithStacks[T any](t T, err error) (T, error) {
	//nolint:wrapcheck
	return t, errors.WithStackDepth(err, 1)
}
