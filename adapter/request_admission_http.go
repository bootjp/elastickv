package adapter

import (
	"context"
	"net"
	"net/http"
	"sync"

	"github.com/cockroachdb/errors"
)

type httpAdmissionConnKey struct{}

// Capture the accepted connection, not Body.Close: net/http body closure can
// wait on the same mutex held by a blocked read. Closing the transport releases
// both request reads and response writes without abandoning handler goroutines.
func installHTTPRequestAdmission(server *http.Server, admission RequestAdmission) {
	if admission == nil {
		return
	}
	prior := server.ConnContext
	server.ConnContext = func(ctx context.Context, conn net.Conn) context.Context {
		if prior != nil {
			ctx = prior(ctx, conn)
		}
		return context.WithValue(ctx, httpAdmissionConnKey{}, conn)
	}
	server.Handler = withHTTPRequestAdmission(server.Handler, admission)
}

func httpRequestTransportLifetime(request *http.Request, owned context.Context) func() {
	conn, ok := request.Context().Value(httpAdmissionConnKey{}).(net.Conn)
	if !ok || conn == nil {
		// Direct Handler/Recorder tests have no accepted transport. Production
		// servers install the ConnContext hook before serving any requests.
		return func() {}
	}
	done := make(chan struct{})
	stop := context.AfterFunc(owned, func() {
		defer close(done)
		_ = conn.Close()
	})
	var once sync.Once
	return func() {
		once.Do(func() {
			if !stop() {
				<-done
			}
		})
	}
}

func withHTTPRequestAdmission(next http.Handler, admission RequestAdmission) http.Handler {
	if admission == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		owner, err := admission(request.Context())
		if err != nil || owner == nil {
			if owner != nil {
				owner.CancelAndJoin()
			}
			http.Error(w, "runtime admission is closed", http.StatusServiceUnavailable)
			return
		}
		joinTransport := func() {}
		defer func() {
			joinTransport()
			owner.CancelAndJoin()
		}()
		if owner.Context() == nil {
			http.Error(w, "runtime admission is unavailable", http.StatusServiceUnavailable)
			return
		}
		joinTransport = httpRequestTransportLifetime(request, owner.Context())
		guarded := &admittedHTTPWriter{ResponseWriter: w, owner: owner}
		next.ServeHTTP(guarded, request.WithContext(owner.Context()))
		// Finish retires and cancels the owner too. Disarm first so normal
		// completion leaves a healthy keep-alive connection open.
		joinTransport()
		if err := owner.Finish(nil); err != nil {
			guarded.refuse()
		}
	})
}

type admittedHTTPWriter struct {
	http.ResponseWriter
	owner RequestOwnership
}

func (w *admittedHTTPWriter) refuse() {
	// The handler has already entered and may have committed effects, even if
	// it has not written headers. No generic status can prove non-acceptance.
	// Abort instead of turning an unknown outcome into a retryable refusal.
	panic(http.ErrAbortHandler)
}

func (w *admittedHTTPWriter) WriteHeader(code int) {
	if err := w.owner.Validate(); err != nil {
		w.refuse()
		return
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *admittedHTTPWriter) Write(payload []byte) (int, error) {
	if err := w.owner.Validate(); err != nil {
		w.refuse()
		return 0, errors.Wrap(err, "HTTP response admission revoked")
	}
	n, err := w.ResponseWriter.Write(payload)
	return n, errors.WithStack(err)
}

func (w *admittedHTTPWriter) Flush() {
	if err := w.owner.Validate(); err != nil {
		w.refuse()
		return
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func WithDynamoDBRequestAdmission(admission RequestAdmission) DynamoDBServerOption {
	return func(server *DynamoDBServer) { server.requestAdmission = admission }
}

func WithS3RequestAdmission(admission RequestAdmission) S3ServerOption {
	return func(server *S3Server) { server.requestAdmission = admission }
}

func WithSQSRequestAdmission(admission RequestAdmission) SQSServerOption {
	return func(server *SQSServer) { server.requestAdmission = admission }
}
