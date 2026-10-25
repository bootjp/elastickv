package adapter

import (
	"strings"

	"google.golang.org/grpc/status"
)

// forwardedStatusMessage removes only nested envelopes with the same gRPC
// code. A server-side wrapped status can acquire one such envelope per hop.
func forwardedStatusMessage(st *status.Status) string {
	message := st.Message()
	prefix := "rpc error: code = " + st.Code().String() + " desc = "
	for strings.HasPrefix(message, prefix) {
		message = strings.TrimPrefix(message, prefix)
	}
	return message
}
