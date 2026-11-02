package peersession

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// grpc-go's per-call credentials option replaces the preceding option. Extract
// and compose them explicitly instead of silently dropping startup/admin auth.
// Collisions fail closed; the session never replaces another credential header.
func (h *Handle) options(input []grpc.CallOption, check func(context.Context) error) ([]grpc.CallOption, error) {
	composed := &callCredentials{session: h, check: check}
	result := []grpc.CallOption{grpc.MaxCallRecvMsgSize(TransportMaxMessageSize), grpc.MaxCallSendMsgSize(TransportMaxMessageSize)}
	for _, option := range input {
		cred, ok := callCredential(option)
		if ok {
			if cred == nil {
				return nil, ErrDenied
			}
			composed.extra = append(composed.extra, cred)
		} else {
			result = append(result, option)
		}
	}
	return append(result, grpc.PerRPCCredentials(composed)), nil
}

func callCredential(option grpc.CallOption) (credentials.PerRPCCredentials, bool) {
	switch opt := option.(type) {
	case grpc.PerRPCCredsCallOption:
		return opt.Creds, true
	case *grpc.PerRPCCredsCallOption:
		if opt == nil {
			return nil, true
		}
		return opt.Creds, true
	default:
		return nil, false
	}
}

type callCredentials struct {
	session *Handle
	extra   []credentials.PerRPCCredentials
	check   func(context.Context) error
}

func (*callCredentials) RequireTransportSecurity() bool { return true }
func (c *callCredentials) GetRequestMetadata(ctx context.Context, uris ...string) (map[string]string, error) {
	if err := c.check(ctx); err != nil {
		return nil, err
	}
	result, err := c.session.GetRequestMetadata(ctx, uris...)
	if err != nil {
		return nil, err
	}
	for _, extra := range c.extra {
		md, err := extra.GetRequestMetadata(ctx, uris...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if err := mergeMetadata(result, md); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func mergeMetadata(dst, src map[string]string) error {
	for key, value := range src {
		key = strings.ToLower(key)
		if _, exists := dst[key]; exists {
			return ErrDenied
		}
		dst[key] = value
	}
	return nil
}
