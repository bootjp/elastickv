package proxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	rawRedisCopyDirections   = 2
	rawRedisHandshakeTimeout = 5 * time.Second
)

func (p *ProxyServer) listenAndServeRawRedis(ctx context.Context) error {
	p.shutdownCtx = ctx

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", p.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("raw redis proxy listen: %w", err)
	}
	defer ln.Close()

	p.logger.Info("raw redis proxy starting",
		"addr", p.cfg.ListenAddr,
		"mode", p.cfg.Mode.String(),
		"primary", p.cfg.PrimaryAddr,
		"primary_db", p.cfg.PrimaryDB,
	)

	var wg sync.WaitGroup
	defer wg.Wait()

	go func() {
		<-ctx.Done()
		p.logger.Info("shutting down raw redis proxy")
		_ = ln.Close()
	}()

	for {
		client, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("raw redis proxy accept: %w", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.handleRawRedisConn(ctx, client)
		}()
	}
}

func (p *ProxyServer) handleRawRedisConn(ctx context.Context, client net.Conn) {
	p.metrics.ActiveConnections.Inc()
	defer p.metrics.ActiveConnections.Dec()
	defer client.Close()

	var dialer net.Dialer
	upstream, err := dialer.DialContext(ctx, "tcp", p.cfg.PrimaryAddr)
	if err != nil {
		p.logger.Warn("raw redis upstream dial failed", "addr", p.cfg.PrimaryAddr, "err", err)
		return
	}
	defer upstream.Close()

	upstreamReader := bufio.NewReader(upstream)
	if err := p.prepareRawRedisUpstream(upstream, upstreamReader); err != nil {
		p.logger.Warn("raw redis upstream prepare failed", "addr", p.cfg.PrimaryAddr, "err", err)
		return
	}

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			_ = client.Close()
			_ = upstream.Close()
		case <-stop:
		}
	}()

	errCh := make(chan struct{}, rawRedisCopyDirections)
	go rawCopy(upstream, client, errCh)
	go rawCopy(client, upstreamReader, errCh)
	// Wait for both directions. Returning on the first one closes the client
	// and upstream through the deferred closes, which cuts off a response the
	// reverse copier is still delivering -- the case a one-shot client that
	// half-closes after writing its command lands in.
	for range rawRedisCopyDirections {
		<-errCh
	}
}

// rawCopy pumps one direction and then shuts down only that direction's write
// side. Closing the whole connection here would tear down the reverse copier
// too, so a client that sends a request and calls shutdown(SHUT_WR) -- valid
// TCP, and what one-shot clients do -- would never receive its reply. The
// full close is left to handleRawRedisConn's deferred closes, which run once
// both directions are finished.
func rawCopy(dst net.Conn, src io.Reader, done chan<- struct{}) {
	_, _ = io.Copy(dst, src)
	closeWriteOrConn(dst)
	done <- struct{}{}
}

// closeWriteOrConn half-closes dst when the transport supports it (TCP does),
// and falls back to a full close otherwise so a non-TCP transport still
// terminates rather than hanging.
func closeWriteOrConn(dst net.Conn) {
	if cw, ok := dst.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
		return
	}
	_ = dst.Close()
}

func (p *ProxyServer) prepareRawRedisUpstream(upstream net.Conn, upstreamReader *bufio.Reader) error {
	if err := upstream.SetDeadline(time.Now().Add(rawRedisHandshakeTimeout)); err != nil {
		return fmt.Errorf("set handshake deadline: %w", err)
	}
	if p.cfg.PrimaryPassword != "" {
		if err := rawRedisRoundTrip(upstream, upstreamReader, "AUTH", p.cfg.PrimaryPassword); err != nil {
			return fmt.Errorf("auth: %w", err)
		}
	}
	if p.cfg.PrimaryDB != 0 {
		if err := rawRedisRoundTrip(upstream, upstreamReader, "SELECT", strconv.Itoa(p.cfg.PrimaryDB)); err != nil {
			return fmt.Errorf("select db %d: %w", p.cfg.PrimaryDB, err)
		}
	}
	if err := upstream.SetDeadline(time.Time{}); err != nil {
		return fmt.Errorf("clear handshake deadline: %w", err)
	}
	return nil
}

func rawRedisRoundTrip(w io.Writer, r *bufio.Reader, args ...string) error {
	if _, err := w.Write(rawRedisCommand(args...)); err != nil {
		return fmt.Errorf("write command: %w", err)
	}
	prefix, err := r.ReadByte()
	if err != nil {
		return fmt.Errorf("read reply prefix: %w", err)
	}
	line, err := r.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read reply line: %w", err)
	}
	line = strings.TrimRight(line, "\r\n")
	if prefix == '-' {
		return fmt.Errorf("%s", line)
	}
	return nil
}

func rawRedisCommand(args ...string) []byte {
	var b strings.Builder
	b.WriteByte('*')
	b.WriteString(strconv.Itoa(len(args)))
	b.WriteString("\r\n")
	for _, arg := range args {
		b.WriteByte('$')
		b.WriteString(strconv.Itoa(len(arg)))
		b.WriteString("\r\n")
		b.WriteString(arg)
		b.WriteString("\r\n")
	}
	return []byte(b.String())
}
