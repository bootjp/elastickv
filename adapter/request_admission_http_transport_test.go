package adapter

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

const httpAdmissionTransportTimeout = 5 * time.Second

type httpTransportTestOwner struct {
	ctx                      context.Context
	cancel                   context.CancelFunc
	conn                     net.Conn
	finished, joined         chan struct{}
	finishOnce, joinOnce     sync.Once
	finishErr                error
	finishResult             error
	joinStarted, releaseJoin chan struct{}
}

func newHTTPTransportTestOwner(ctx context.Context) *httpTransportTestOwner {
	owned, cancel := context.WithCancel(ctx)
	conn, _ := ctx.Value(httpAdmissionConnKey{}).(net.Conn)
	return &httpTransportTestOwner{ctx: owned, cancel: cancel, conn: conn,
		finished: make(chan struct{}), joined: make(chan struct{})}
}

func (o *httpTransportTestOwner) Context() context.Context { return o.ctx }
func (o *httpTransportTestOwner) Validate() error {
	if o.ctx.Err() != nil {
		return errRequestRevoked
	}
	return nil
}
func (o *httpTransportTestOwner) Finish(original error) error {
	o.finishOnce.Do(func() {
		o.finishErr = errors.Join(original, o.Validate(), o.finishResult)
		o.cancel()
		close(o.finished)
	})
	return o.finishErr
}
func (o *httpTransportTestOwner) CancelAndJoin() {
	o.joinOnce.Do(func() {
		o.cancel()
		if o.joinStarted != nil {
			close(o.joinStarted)
			<-o.releaseJoin
		}
		close(o.joined)
	})
}

type httpAdmissionReadProbe struct {
	io.ReadCloser
	reads   int
	entered chan struct{}
}

func (r *httpAdmissionReadProbe) Read(data []byte) (int, error) {
	r.reads++
	if r.reads == 2 {
		close(r.entered)
	}
	n, err := r.ReadCloser.Read(data)
	return n, errors.WithStack(err)
}

func waitHTTPTransport(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(httpAdmissionTransportTimeout):
		t.Fatal("HTTP transport ownership did not drain")
	}
}

func receiveHTTPTransportOwner(t *testing.T, owners <-chan *httpTransportTestOwner) *httpTransportTestOwner {
	t.Helper()
	select {
	case owner := <-owners:
		return owner
	case <-time.After(httpAdmissionTransportTimeout):
		t.Fatal("HTTP request was not admitted")
		return nil
	}
}

func serveHTTPTransport(t *testing.T, server *http.Server, listener func(net.Listener) net.Listener) (*httptest.Server, net.Conn) {
	t.Helper()
	testServer := httptest.NewUnstartedServer(nil)
	testServer.Config = server
	if listener != nil {
		testServer.Listener = listener(testServer.Listener)
	}
	testServer.Start()
	t.Cleanup(testServer.Close)
	conn, err := (&net.Dialer{Timeout: httpAdmissionTransportTimeout}).DialContext(t.Context(), "tcp", testServer.Listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return testServer, conn
}

func newHTTPTransportDynamo(t *testing.T, admission RequestAdmission, options ...DynamoDBServerOption) *DynamoDBServer {
	t.Helper()
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	options = append(options, WithDynamoDBRequestAdmission(admission))
	server := NewDynamoDBServer(nil, st, &stubAdapterCoordinator{clock: kv.NewHLC()}, options...)
	t.Cleanup(server.Stop)
	return server
}

func TestHTTPRequestAdmissionConstructorsAbortStalledUpload(t *testing.T) {
	constructors := map[string]struct {
		newServer func(*testing.T, RequestAdmission) *http.Server
		head      string
		prefix    string
	}{
		"dynamodb": {func(t *testing.T, admission RequestAdmission) *http.Server {
			return newHTTPTransportDynamo(t, admission).httpServer
		}, "POST / HTTP/1.1\r\nX-Amz-Target: DynamoDB_20120810.CreateTable\r\n", "{"},
		"sqs": {func(t *testing.T, admission RequestAdmission) *http.Server {
			st := store.NewMVCCStore()
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			server := NewSQSServer(nil, st, &stubAdapterCoordinator{clock: kv.NewHLC()}, WithSQSRequestAdmission(admission))
			t.Cleanup(server.Stop)
			return server.httpServer
		}, "POST / HTTP/1.1\r\nX-Amz-Target: AmazonSQS.CreateQueue\r\n", "{"},
		"s3": {func(t *testing.T, admission RequestAdmission) *http.Server {
			st := store.NewMVCCStore()
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			server := NewS3Server(nil, "", st, &stubAdapterCoordinator{clock: kv.NewHLC()}, nil, WithS3RequestAdmission(admission))
			t.Cleanup(server.Stop)
			return server.httpServer
		}, "POST /bucket/object?uploadId=stalled HTTP/1.1\r\n", "<"},
	}
	for name, fixture := range constructors {
		t.Run(name, func(t *testing.T) {
			owners := make(chan *httpTransportTestOwner, 1)
			server := fixture.newServer(t, func(ctx context.Context) (RequestOwnership, error) {
				owner := newHTTPTransportTestOwner(ctx)
				owners <- owner
				return owner, nil
			})
			entered := make(chan struct{})
			original := server.Handler
			server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				r.Body = &httpAdmissionReadProbe{ReadCloser: r.Body, entered: entered}
				original.ServeHTTP(w, r)
			})
			_, conn := serveHTTPTransport(t, server, nil)
			_, err := io.WriteString(conn, fixture.head+"Host: localhost\r\nContent-Length: 65536\r\nContent-Type: application/x-amz-json-1.0\r\n\r\n"+fixture.prefix)
			require.NoError(t, err)
			owner := receiveHTTPTransportOwner(t, owners)
			require.NotNil(t, owner.conn, "full adapter constructor must capture the real accepted connection")
			waitHTTPTransport(t, entered)
			select {
			case <-owner.joined:
				t.Fatal("incomplete upload did not remain owned")
			default:
			}
			owner.cancel()
			waitHTTPTransport(t, owner.joined)
			require.ErrorIs(t, owner.ctx.Err(), context.Canceled)
			require.NoError(t, conn.SetReadDeadline(time.Now().Add(httpAdmissionTransportTimeout)))
			_, err = conn.Read(make([]byte, 1))
			require.Error(t, err)
			var networkError net.Error
			require.False(t, errors.As(err, &networkError) && networkError.Timeout(), "the raw connection must close, not just expire a test deadline")
		})
	}
}

func TestHTTPRequestAdmissionPostCommitFinishFailureHasUnknownOutcome(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			owners := make(chan *httpTransportTestOwner, 1)
			effect := make(chan error, 1)
			server := newHTTPTransportDynamo(t, func(ctx context.Context) (RequestOwnership, error) {
				owner := newHTTPTransportTestOwner(ctx)
				owner.finishResult = errors.Join(kv.ErrTransactionOutcomeUnknown, errRequestRevoked)
				owners <- owner
				return owner, nil
			}, func(server *DynamoDBServer) {
				server.httpServer.Handler = http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
					effect <- server.store.PutAt(r.Context(), []byte("committed"), []byte("value"), 1, 0)
				})
			})
			_, conn := serveHTTPTransport(t, server.httpServer, nil)
			_, err := io.WriteString(conn, method+" / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n")
			require.NoError(t, err)
			owner := receiveHTTPTransportOwner(t, owners)
			waitHTTPTransport(t, owner.joined)
			require.NoError(t, <-effect)
			require.ErrorIs(t, owner.finishErr, errRequestRevoked)
			require.ErrorIs(t, owner.finishErr, kv.ErrTransactionOutcomeUnknown)
			value, err := server.store.GetAt(t.Context(), []byte("committed"), 1)
			require.NoError(t, err)
			require.Equal(t, []byte("value"), value)
			require.NoError(t, conn.SetReadDeadline(time.Now().Add(httpAdmissionTransportTimeout)))
			response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: method})
			require.Error(t, err, "post-work rejection must be ambiguous I/O, not a definite 503/409 refusal")
			require.Nil(t, response)
		})
	}
}

func TestHTTPRequestAdmissionPreservesConnContextAndHealthyKeepAlive(t *testing.T) {
	type priorContextKey struct{}
	owners := make(chan *httpTransportTestOwner, 2)
	priorValue := make(chan bool, 2)
	server := newHTTPTransportDynamo(t, func(ctx context.Context) (RequestOwnership, error) {
		priorValue <- ctx.Value(priorContextKey{}) == "preserved"
		owner := newHTTPTransportTestOwner(ctx)
		owners <- owner
		return owner, nil
	}, func(server *DynamoDBServer) {
		server.httpServer.ConnContext = func(ctx context.Context, _ net.Conn) context.Context {
			return context.WithValue(ctx, priorContextKey{}, "preserved")
		}
	})
	_, conn := serveHTTPTransport(t, server.httpServer, nil)
	reader := bufio.NewReader(conn)
	for range 2 {
		_, err := io.WriteString(conn, "GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n")
		require.NoError(t, err)
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(httpAdmissionTransportTimeout)))
		response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
		require.NoError(t, err, "Finish cancellation must not close a healthy keep-alive connection")
		_, err = io.Copy(io.Discard, response.Body)
		require.NoError(t, err)
		require.NoError(t, response.Body.Close())
		owner := receiveHTTPTransportOwner(t, owners)
		waitHTTPTransport(t, owner.joined)
		require.NoError(t, owner.finishErr)
		require.ErrorIs(t, owner.ctx.Err(), context.Canceled, "Finish really cancels ownership")
		require.True(t, <-priorValue)
		require.NotNil(t, owner.conn)
	}
}

type httpAdmissionCloseLatch struct {
	started, release, done chan struct{}
	releaseOnce            sync.Once
}

func newHTTPAdmissionCloseLatch(t *testing.T) *httpAdmissionCloseLatch {
	t.Helper()
	latch := &httpAdmissionCloseLatch{started: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{})}
	t.Cleanup(latch.unblock)
	return latch
}

func (l *httpAdmissionCloseLatch) unblock() { l.releaseOnce.Do(func() { close(l.release) }) }

type httpAdmissionListener struct {
	net.Listener
	latch *httpAdmissionCloseLatch
}

func (l *httpAdmissionListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &httpAdmissionLatchedConn{Conn: conn, latch: l.latch}, nil
}

type httpAdmissionLatchedConn struct {
	net.Conn
	latch *httpAdmissionCloseLatch
	once  sync.Once
	err   error
}

func (c *httpAdmissionLatchedConn) Close() error {
	c.once.Do(func() {
		c.err = c.Conn.Close()
		close(c.latch.started)
		<-c.latch.release
		close(c.latch.done)
	})
	return errors.WithStack(c.err)
}

func TestHTTPRequestAdmissionJoinsTransportCloseBeforeRetiringOwner(t *testing.T) {
	owners := make(chan *httpTransportTestOwner, 1)
	latch := newHTTPAdmissionCloseLatch(t)
	server := newHTTPTransportDynamo(t, func(ctx context.Context) (RequestOwnership, error) {
		owner := newHTTPTransportTestOwner(ctx)
		owners <- owner
		return owner, nil
	})
	_, conn := serveHTTPTransport(t, server.httpServer, func(listener net.Listener) net.Listener {
		return &httpAdmissionListener{Listener: listener, latch: latch}
	})
	// Register after the HTTP server: even a failing assertion must release the
	// test's deliberate Close fence before httptest attempts server shutdown.
	t.Cleanup(latch.unblock)
	_, err := io.WriteString(conn, "POST / HTTP/1.1\r\nHost: localhost\r\nX-Amz-Target: DynamoDB_20120810.CreateTable\r\nContent-Length: 65536\r\n\r\n{")
	require.NoError(t, err)
	owner := receiveHTTPTransportOwner(t, owners)
	owner.cancel()
	waitHTTPTransport(t, latch.started)
	select {
	case <-owner.finished:
		t.Fatal("Finish ran before the transport cancellation callback joined")
	case <-owner.joined:
		t.Fatal("CancelAndJoin ran before the transport cancellation callback joined")
	case <-time.After(25 * time.Millisecond):
	}
	latch.unblock()
	waitHTTPTransport(t, owner.joined)
	waitHTTPTransport(t, latch.done)
}

func TestHTTPRequestAdmissionPanicStopsTransportCallbackBeforeOwnerCancellation(t *testing.T) {
	owners := make(chan *httpTransportTestOwner, 1)
	allowJoin := make(chan struct{})
	var release sync.Once
	unblock := func() { release.Do(func() { close(allowJoin) }) }
	latch := newHTTPAdmissionCloseLatch(t)
	latch.unblock()
	server := newHTTPTransportDynamo(t, func(ctx context.Context) (RequestOwnership, error) {
		owner := newHTTPTransportTestOwner(ctx)
		owner.joinStarted, owner.releaseJoin = make(chan struct{}), allowJoin
		owners <- owner
		return owner, nil
	}, func(server *DynamoDBServer) {
		server.httpServer.Handler = http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic(http.ErrAbortHandler) })
	})
	_, conn := serveHTTPTransport(t, server.httpServer, func(listener net.Listener) net.Listener {
		return &httpAdmissionListener{Listener: listener, latch: latch}
	})
	t.Cleanup(unblock)
	_, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
	require.NoError(t, err)
	owner := receiveHTTPTransportOwner(t, owners)
	waitHTTPTransport(t, owner.joinStarted)
	select {
	case <-latch.started:
		t.Fatal("owner cancellation launched transport callback during panic retirement")
	case <-time.After(25 * time.Millisecond):
	}
	unblock()
	waitHTTPTransport(t, owner.joined)
	waitHTTPTransport(t, latch.done)
	select {
	case <-owner.finished:
		t.Fatal("panic must not publish a successful Finish")
	default:
	}
}

func TestHTTPRequestAdmissionNilHookPreservesServer(t *testing.T) {
	handler := http.NewServeMux()
	server := &http.Server{Handler: handler}
	installHTTPRequestAdmission(server, nil)
	require.Nil(t, server.ConnContext)
	require.Same(t, handler, server.Handler)
}

type httpAdmissionWriteListener struct {
	net.Listener
	entered chan struct{}
}

func (l *httpAdmissionWriteListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		if err := tcp.SetWriteBuffer(1024); err != nil {
			_ = conn.Close()
			return nil, errors.WithStack(err)
		}
	}
	return &httpAdmissionWriteConn{Conn: conn, entered: l.entered}, nil
}

type httpAdmissionWriteConn struct {
	net.Conn
	entered chan struct{}
	once    sync.Once
}

func (c *httpAdmissionWriteConn) Write(payload []byte) (int, error) {
	if len(payload) > 4096 {
		c.once.Do(func() { close(c.entered) })
	}
	n, err := c.Conn.Write(payload)
	return n, errors.WithStack(err)
}

func TestHTTPRequestAdmissionAbortsBlockedResponseWrite(t *testing.T) {
	owners := make(chan *httpTransportTestOwner, 1)
	entered, writeResult := make(chan struct{}), make(chan error, 1)
	server := newHTTPTransportDynamo(t, func(ctx context.Context) (RequestOwnership, error) {
		owner := newHTTPTransportTestOwner(ctx)
		owners <- owner
		return owner, nil
	}, func(server *DynamoDBServer) {
		server.httpServer.Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, err := w.Write(make([]byte, 8<<20))
			writeResult <- err
		})
	})
	_, conn := serveHTTPTransport(t, server.httpServer, func(listener net.Listener) net.Listener {
		return &httpAdmissionWriteListener{Listener: listener, entered: entered}
	})
	tcp, ok := conn.(*net.TCPConn)
	require.True(t, ok)
	require.NoError(t, tcp.SetReadBuffer(1024))
	_, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
	require.NoError(t, err)
	owner := receiveHTTPTransportOwner(t, owners)
	waitHTTPTransport(t, entered)
	select {
	case <-writeResult:
		t.Fatal("response did not block while the client withheld reads")
	case <-time.After(25 * time.Millisecond):
	}
	owner.cancel()
	waitHTTPTransport(t, owner.joined)
	select {
	case err := <-writeResult:
		require.Error(t, err)
	case <-time.After(httpAdmissionTransportTimeout):
		t.Fatal("revoked response write remained blocked")
	}
}
