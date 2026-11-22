package client

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/coder/websocket"

	"go.beppler.dev.br/wsproxy/internal/dest"
	"go.beppler.dev.br/wsproxy/internal/tunnel"
)

const defaultBufferedBody = 10 << 20 // 10 MiB

// HTTPProxy is a local HTTP proxy that forwards CONNECT and
// absolute-URI requests through the wsproxy server. Each proxied
// connection gets its own tunnel (one WebSocket per connection).
type HTTPProxy struct {
	// URL is the wsproxy server endpoint (wss://).
	URL string
	// Key is the shared secret.
	Key string
	// HandshakeTimeout bounds each tunnel dial, including the server's
	// destination check and upstream dial (default 30s, which must
	// exceed the server's dial_timeout).
	HandshakeTimeout time.Duration
	// MaxBufferedBody bounds plain-HTTP request bodies kept in memory
	// before forwarding (default 10 MiB). CONNECT is unaffected.
	MaxBufferedBody int64
}

func (p *HTTPProxy) handshake() time.Duration {
	if p.HandshakeTimeout > 0 {
		return p.HandshakeTimeout
	}
	return 30 * time.Second
}

func (p *HTTPProxy) maxBody() int64 {
	if p.MaxBufferedBody > 0 {
		return p.MaxBufferedBody
	}
	return defaultBufferedBody
}

// ServeHTTP implements the local proxy protocol.
func (p *HTTPProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodConnect:
		p.serveConnect(w, r)
	case r.URL.Host == "":
		http.Error(w, "wsproxy proxy: send CONNECT or an absolute-URI request", http.StatusBadRequest)
	case r.URL.Scheme == "https":
		http.Error(w, "wsproxy proxy: use CONNECT for https destinations", http.StatusBadRequest)
	default:
		p.serveForward(w, r)
	}
}

// serveConnect relays a CONNECT request: it dials a tunnel to the
// requested host:port, answers 200 and pipes raw bytes both ways.
func (p *HTTPProxy) serveConnect(w http.ResponseWriter, r *http.Request) {
	host, portStr, err := net.SplitHostPort(r.Host)
	if err != nil {
		host, portStr = r.Host, "443"
	}
	port, err := dest.ParsePort(portStr, 443)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := dest.ValidateHost(host); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	raw, rw, ok := hijack(w)
	if !ok {
		return
	}
	defer raw.Close()

	conn, err := p.dial(context.Background(), host, port)
	if err != nil {
		writeRawStatus(raw, http.StatusBadGateway, err)
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	if _, err := raw.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartKeepalive(ctx, conn, KeepaliveInterval, cancel)

	// Bytes net/http buffered past the CONNECT headers belong to the
	// tunneled stream and must be replayed first.
	target := &bufferedConn{ReadWriteCloser: raw, pre: rw.Reader}
	_ = tunnel.Pump(ctx, conn, target)
}

// serveForward relays a plain (absolute-URI) HTTP request: it dials a
// tunnel to the target host, forwards the request head and buffered
// body, then streams the response back verbatim.
func (p *HTTPProxy) serveForward(w http.ResponseWriter, r *http.Request) {
	host := r.URL.Hostname()
	port, err := dest.ParsePort(r.URL.Port(), 80)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := dest.ValidateHost(host); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	raw, _, ok := hijack(w)
	if !ok {
		return
	}
	defer raw.Close()

	conn, err := p.dial(context.Background(), host, port)
	if err != nil {
		writeRawStatus(raw, http.StatusBadGateway, err)
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	// Plain HTTP needs to know the body length up front (we do not
	// implement chunked re-encoding), so buffer it. The request asks
	// the origin to close the connection, which ends the tunnel
	// cleanly after the response.
	body, err := io.ReadAll(io.LimitReader(r.Body, p.maxBody()+1))
	if err != nil {
		writeRawStatus(raw, http.StatusBadGateway, err)
		return
	}
	if int64(len(body)) > p.maxBody() {
		writeRawStatus(raw, http.StatusRequestEntityTooLarge,
			errors.New("plain-HTTP request body exceeds the proxy limit; use CONNECT instead"))
		return
	}

	head := buildRequestHead(r, len(body))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := conn.Write(ctx, websocket.MessageBinary, head); err != nil {
		writeRawStatus(raw, http.StatusBadGateway, err)
		return
	}
	if len(body) > 0 {
		if err := conn.Write(ctx, websocket.MessageBinary, body); err != nil {
			writeRawStatus(raw, http.StatusBadGateway, err)
			return
		}
	}
	StartKeepalive(ctx, conn, KeepaliveInterval, cancel)

	cw := &countingWriter{w: raw}
	if _, err := tunnel.Drain(ctx, conn, cw); err != nil && cw.n == 0 {
		// The server never relayed a response (e.g. its upstream
		// dial failed); the client still deserves an HTTP error.
		writeRawStatus(raw, http.StatusBadGateway, tunnelErr(err))
	}
}

// tunnelErr renders a peer's WebSocket close frame as a readable
// message.
func tunnelErr(err error) error {
	var ce websocket.CloseError
	if errors.As(err, &ce) {
		if ce.Reason != "" {
			return fmt.Errorf("tunnel closed by server: %s", ce.Reason)
		}
		return fmt.Errorf("tunnel closed by server (status %d)", ce.Code)
	}
	return err
}

// countingWriter counts bytes passing through to the client.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

func (p *HTTPProxy) dial(ctx context.Context, host string, port int) (*websocket.Conn, error) {
	return Dial(ctx, Options{
		URL:              p.URL,
		Key:              p.Key,
		Host:             host,
		Port:             port,
		HandshakeTimeout: p.handshake(),
	})
}

// hijack takes over the raw client connection; ok is false if a
// plain HTTP error response has already been written.
func hijack(w http.ResponseWriter) (raw io.ReadWriteCloser, rw *bufio.ReadWriter, ok bool) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijacking not supported", http.StatusInternalServerError)
		return nil, nil, false
	}
	raw, rw, err := hj.Hijack()
	if err != nil {
		return nil, nil, false
	}
	return raw, rw, true
}

// writeRawStatus writes a plain HTTP error response on an already
// hijacked connection.
func writeRawStatus(w io.Writer, code int, err error) {
	detail := err.Error()
	fmt.Fprintf(w, "HTTP/1.1 %d %s\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\nContent-Length: %d\r\n\r\n%s\n",
		code, http.StatusText(code), len(detail)+1, detail)
}

// bufferedConn replays bytes the net/http server buffered before the
// hijack, then reads straight from the connection.
type bufferedConn struct {
	io.ReadWriteCloser
	pre io.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	if c.pre != nil {
		n, err := c.pre.Read(p)
		if n > 0 {
			if errors.Is(err, io.EOF) {
				c.pre = nil
			}
			return n, nil
		}
		if errors.Is(err, io.EOF) {
			c.pre = nil
		} else if err != nil {
			return 0, err
		}
	}
	return c.ReadWriteCloser.Read(p)
}

// hopByHop headers are not meaningful end-to-end and are stripped.
var hopByHop = map[string]bool{
	"Connection":          true,
	"Proxy-Connection":    true,
	"Keep-Alive":          true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
	"Te":                  true,
	"Trailer":             true,
	"Transfer-Encoding":   true,
	"Upgrade":             true,
}

// buildRequestHead renders the request to forward to the origin, with
// a fixed Host, an explicit Content-Length and Connection: close so
// the origin closes the socket when the response ends.
func buildRequestHead(r *http.Request, bodyLen int) []byte {
	uri := r.URL.RequestURI()
	if uri == "" {
		uri = "/"
	}
	var b bytes.Buffer
	fmt.Fprintf(&b, "%s %s HTTP/1.1\r\n", r.Method, uri)
	for k, vv := range r.Header {
		if hopByHop[http.CanonicalHeaderKey(k)] {
			continue
		}
		for _, v := range vv {
			fmt.Fprintf(&b, "%s: %s\r\n", k, v)
		}
	}
	fmt.Fprintf(&b, "Host: %s\r\n", r.URL.Host)
	fmt.Fprintf(&b, "Content-Length: %d\r\n", bodyLen)
	b.WriteString("Connection: close\r\n\r\n")
	return b.Bytes()
}
