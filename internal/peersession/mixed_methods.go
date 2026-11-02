package peersession

import (
	"context"

	"google.golang.org/grpc/metadata"
)

// Presence, including an empty/partial/malformed header, selects the private
// lane. Parsing failure must never downgrade a peer attempt to a public call.
func hasSessionMetadata(ctx context.Context) bool {
	md, _ := metadata.FromIncomingContext(ctx)
	_, session := md[sessionHeader]
	_, proof := md[proofHeader]
	return session || proof
}

func (o *Owner) permitsPublic(ctx context.Context, name string) bool {
	return !hasSessionMetadata(ctx) && (o.public[name] || o.methods[name].AllowPublic)
}

func privateBinding(h *Handle, outbound bool) (RequestBinding, error) {
	if !h.owned() {
		return RequestBinding{}, ErrDenied
	}
	b := h.s.binding
	from, to := b.Remote, b.Local
	if outbound {
		from, to = to, from
	}
	return RequestBinding{Group: b.Group, From: from, To: to}, nil
}

func decodeRequest(h *Handle, m Method, payload any, outbound bool) (Message, error) {
	if m.BoundRequest != nil {
		binding, err := privateBinding(h, outbound)
		if err != nil {
			return Message{}, err
		}
		return m.BoundRequest(binding, payload)
	}
	if m.Request == nil {
		return Message{}, ErrDenied
	}
	return m.Request(payload)
}
