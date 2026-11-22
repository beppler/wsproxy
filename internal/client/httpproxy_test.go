package client

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"go.beppler.dev.br/wsproxy/internal/tunnel"
)

// responderTunnelHandler implements a wsproxy server whose upstream
// is an HTTP origin: it acks the tunnel, captures the forwarded
// request (head plus optional body), replies with a canned response
// and closes the tunnel.
func responderTunnelHandler(response []byte, got func(head []byte, body []byte)) func(ctx context.Context, conn *websocket.Conn, host string, port int) {
	return func(_ context.Context, conn *websocket.Conn, _ string, _ int) {
		ctx := context.Background()
		if err := conn.Write(ctx, websocket.MessageBinary, []byte(tunnel.AckOK)); err != nil {
			return
		}
		_, head, err := conn.Read(ctx)
		if err != nil {
			return
		}
		var body []byte
		if cl := contentLength(head); cl > 0 {
			_, body, err = conn.Read(ctx)
			if err != nil {
				return
			}
			if len(body) > cl {
				body = body[:cl]
			}
		}
		got(head, body)

		wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
		defer wcancel()
		_ = conn.Write(wctx, websocket.MessageBinary, response)
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}
}

// closeAfterRequestHandler acks the tunnel, reads the forwarded
// request, then closes with a reason instead of relaying a response.
func closeAfterRequestHandler(status websocket.StatusCode, reason string) func(ctx context.Context, conn *websocket.Conn, host string, port int) {
	return func(_ context.Context, conn *websocket.Conn, _ string, _ int) {
		ctx := context.Background()
		if err := conn.Write(ctx, websocket.MessageBinary, []byte(tunnel.AckOK)); err != nil {
			return
		}
		// Consume the request, then give up on it with a reason.
		_, _, _ = conn.Read(ctx)
		_ = conn.Close(status, reason)
	}
}

// contentLength extracts the Content-Length value from a raw HTTP
// request head (0 if absent, -1 if unparseable).
func contentLength(head []byte) int {
	i := bytes.Index(head, []byte("\r\n\r\n"))
	if i < 0 {
		i = len(head)
	}
	for _, line := range strings.Split(string(head[:i]), "\r\n") {
		k, v, ok := strings.Cut(line, ":")
		if !ok || !strings.EqualFold(strings.TrimSpace(k), "Content-Length") {
			continue
		}
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return -1
		}
		return n
	}
	return 0
}

// rawClient speaks handcrafted HTTP/1.1 to the local proxy over a TCP
// connection, with deadlines so no test can hang.
type rawClient struct {
	t  *testing.T
	c  net.Conn
	br *bufio.Reader
}

func newRawClient(t *testing.T, addr string) *rawClient {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return &rawClient{t: t, c: c, br: bufio.NewReader(c)}
}

func (rc *rawClient) send(format string, args ...any) {
	rc.t.Helper()
	rc.c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprintf(rc.c, format, args...)
}

// readHead reads the response head (status line, headers and the
// empty line) and returns it verbatim.
func (rc *rawClient) readHead() string {
	rc.t.Helper()
	rc.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	var b strings.Builder
	for {
		line, err := rc.br.ReadString('\n')
		b.WriteString(line)
		if err != nil {
			rc.t.Fatalf("read response head: %v", err)
		}
		if line == "\r\n" {
			return b.String()
		}
	}
}

// readN reads exactly n bytes of body.
func (rc *rawClient) readN(n int) []byte {
	rc.t.Helper()
	rc.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, n)
	if _, err := io.ReadFull(rc.br, buf); err != nil {
		rc.t.Fatalf("read %d body bytes: %v", n, err)
	}
	return buf
}

// readRest reads until EOF (or the read deadline expires), returning
// whatever arrived.
func (rc *rawClient) readRest() []byte {
	rc.t.Helper()
	rc.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	data, _ := io.ReadAll(rc.br)
	return data
}

func newProxy(t *testing.T, url string, tune func(*HTTPProxy)) *httptest.Server {
	t.Helper()
	proxy := &HTTPProxy{URL: url, Key: testKey}
	if tune != nil {
		tune(proxy)
	}
	ts := httptest.NewServer(proxy)
	t.Cleanup(ts.Close)
	return ts
}

func TestConnectTunnel(t *testing.T) {
	host, port := echoAddr(t)
	url := newFakeTunnel(t, testKey, echoTunnelHandler())
	ts := newProxy(t, url, nil)

	rc := newRawClient(t, ts.Listener.Addr().String())
	dest := net.JoinHostPort(host, strconv.Itoa(port))
	rc.send("CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", dest, dest)

	if head := rc.readHead(); !strings.HasPrefix(head, "HTTP/1.1 200 Connection Established") {
		t.Fatalf("expected 200, got %q", head)
	}

	// Bidirectional raw byte stream through the tunnel.
	rc.send("ping1")
	if got := rc.readN(5); string(got) != "ping1" {
		t.Fatalf("echo1: got %q", got)
	}
	rc.send("ping2")
	if got := rc.readN(5); string(got) != "ping2" {
		t.Fatalf("echo2: got %q", got)
	}
}

func TestConnectReplaysPipelinedBytes(t *testing.T) {
	host, port := echoAddr(t)
	url := newFakeTunnel(t, testKey, echoTunnelHandler())
	ts := newProxy(t, url, nil)

	rc := newRawClient(t, ts.Listener.Addr().String())
	dest := net.JoinHostPort(host, strconv.Itoa(port))
	// Bytes the proxy's HTTP server buffered past the CONNECT line
	// belong to the tunneled stream and must be replayed first.
	rc.send("CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\npipelined", dest, dest)

	if head := rc.readHead(); !strings.HasPrefix(head, "HTTP/1.1 200 Connection Established") {
		t.Fatalf("expected 200, got %q", head)
	}
	if got := rc.readN(len("pipelined")); string(got) != "pipelined" {
		t.Fatalf("pipelined bytes not replayed, got %q", got)
	}
	rc.send("more")
	if got := rc.readN(4); string(got) != "more" {
		t.Fatalf("echo after replay: got %q", got)
	}
}

func TestConnectUpstreamDialFailure(t *testing.T) {
	port := closedPort(t)
	url := newFakeTunnel(t, testKey, echoTunnelHandler())
	ts := newProxy(t, url, nil)

	rc := newRawClient(t, ts.Listener.Addr().String())
	rc.send("CONNECT 127.0.0.1:%d HTTP/1.1\r\nHost: 127.0.0.1:%d\r\n\r\n", port, port)

	resp := rc.readRest()
	if !bytes.Contains(resp, []byte("HTTP/1.1 502")) {
		t.Fatalf("expected 502, got %q", resp)
	}
	if !bytes.Contains(resp, []byte("upstream dial failed")) {
		t.Fatalf("expected dial failure reason, got %q", resp)
	}
}

func TestConnectBadDestination(t *testing.T) {
	url := newFakeTunnel(t, testKey, echoTunnelHandler())
	ts := newProxy(t, url, nil)

	rc := newRawClient(t, ts.Listener.Addr().String())
	rc.send("CONNECT foo:bar HTTP/1.1\r\nHost: foo:bar\r\n\r\n")
	if resp := rc.readRest(); !bytes.Contains(resp, []byte("HTTP/1.1 400")) {
		t.Fatalf("expected 400, got %q", resp)
	}
}

func TestConnectKeyRequired(t *testing.T) {
	host, port := echoAddr(t)
	url := newFakeTunnel(t, testKey, echoTunnelHandler())
	ts := newProxy(t, url, func(p *HTTPProxy) { p.Key = "wrong" })

	rc := newRawClient(t, ts.Listener.Addr().String())
	dest := net.JoinHostPort(host, strconv.Itoa(port))
	rc.send("CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", dest, dest)
	if resp := rc.readRest(); !bytes.Contains(resp, []byte("unauthorized")) {
		t.Fatalf("expected the server's unauthorized rejection to surface as a 502, got %q", resp)
	}
}

func TestForwardAbsoluteURI(t *testing.T) {
	host, port := echoAddr(t) // echo target doubles as the fake origin host
	url := newFakeTunnel(t, testKey, responderTunnelHandler(
		[]byte("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 5\r\n\r\nhello"),
		func(head, body []byte) {},
	))
	ts := newProxy(t, url, nil)

	rc := newRawClient(t, ts.Listener.Addr().String())
	rc.send("GET http://%s:%d/path?x=1 HTTP/1.1\r\nHost: %s:%d\r\nX-Test: yes\r\nProxy-Connection: keep-alive\r\nKeep-Alive: timeout=5\r\n\r\n",
		host, port, host, port)

	resp := rc.readRest()
	if !bytes.Contains(resp, []byte("HTTP/1.1 200 OK")) {
		t.Fatalf("expected 200, got %q", resp)
	}
	if !bytes.HasSuffix(resp, []byte("hello")) {
		t.Fatalf("expected body %q, got %q", "hello", resp)
	}
}

func TestForwardedRequestHead(t *testing.T) {
	host, port := echoAddr(t)
	var muHead struct {
		head []byte
	}
	url := newFakeTunnel(t, testKey, responderTunnelHandler(
		[]byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"),
		func(head, body []byte) { muHead.head = append([]byte(nil), head...) },
	))
	ts := newProxy(t, url, nil)

	rc := newRawClient(t, ts.Listener.Addr().String())
	rc.send("GET http://%s:%d/a/b?c=1 HTTP/1.1\r\nHost: %s:%d\r\nProxy-Connection: keep-alive\r\nKeep-Alive: timeout=5\r\n\r\n",
		host, port, host, port)
	rc.readRest()

	got := string(muHead.head)
	if !strings.HasPrefix(got, "GET /a/b?c=1 HTTP/1.1\r\n") {
		t.Fatalf("expected absolute URI rewritten to origin form, got %q", got)
	}
	if !strings.Contains(got, fmt.Sprintf("Host: %s:%d", host, port)) {
		t.Fatalf("expected target Host header, got %q", got)
	}
	if !strings.Contains(got, "Connection: close") {
		t.Fatalf("expected Connection: close, got %q", got)
	}
	for _, hop := range []string{"Proxy-Connection", "Keep-Alive"} {
		if strings.Contains(got, hop+":") {
			t.Fatalf("expected hop-by-hop header %q to be stripped, got %q", hop, got)
		}
	}
}

func TestForwardRequestBody(t *testing.T) {
	host, port := echoAddr(t)
	var gotBody []byte
	var gotHead []byte
	url := newFakeTunnel(t, testKey, responderTunnelHandler(
		[]byte("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"),
		func(head, body []byte) {
			gotHead = append([]byte(nil), head...)
			gotBody = append([]byte(nil), body...)
		},
	))
	ts := newProxy(t, url, nil)

	rc := newRawClient(t, ts.Listener.Addr().String())
	rc.send("POST http://%s:%d/submit HTTP/1.1\r\nHost: %s:%d\r\nContent-Length: 5\r\n\r\n12345", host, port, host, port)
	if resp := rc.readRest(); !bytes.Contains(resp, []byte("HTTP/1.1 200")) {
		t.Fatalf("expected 200, got %q", resp)
	}

	if string(gotBody) != "12345" {
		t.Fatalf("expected body %q forwarded, got %q", "12345", gotBody)
	}
	if !strings.Contains(string(gotHead), "Content-Length: 5") {
		t.Fatalf("expected explicit Content-Length, got %q", gotHead)
	}
}

func TestForwardBodyTooLarge(t *testing.T) {
	host, port := echoAddr(t)
	url := newFakeTunnel(t, testKey, responderTunnelHandler(
		[]byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"),
		func(head, body []byte) { t.Errorf("origin must not be dialed for an oversized body") },
	))
	ts := newProxy(t, url, func(p *HTTPProxy) { p.MaxBufferedBody = 4 })

	rc := newRawClient(t, ts.Listener.Addr().String())
	rc.send("POST http://%s:%d/submit HTTP/1.1\r\nHost: %s:%d\r\nContent-Length: 8\r\n\r\n12345678", host, port, host, port)
	if resp := rc.readRest(); !bytes.Contains(resp, []byte("HTTP/1.1 413")) {
		t.Fatalf("expected 413, got %q", resp)
	}
}

func TestForwardRelativeURIBlocked(t *testing.T) {
	url := newFakeTunnel(t, testKey, echoTunnelHandler())
	ts := newProxy(t, url, nil)

	rc := newRawClient(t, ts.Listener.Addr().String())
	rc.send("GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n")
	if resp := rc.readRest(); !bytes.Contains(resp, []byte("HTTP/1.1 400")) {
		t.Fatalf("expected 400, got %q", resp)
	}
}

func TestForwardHTTPSBlocked(t *testing.T) {
	url := newFakeTunnel(t, testKey, echoTunnelHandler())
	ts := newProxy(t, url, nil)

	rc := newRawClient(t, ts.Listener.Addr().String())
	rc.send("GET https://example.com/ HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n")
	if resp := rc.readRest(); !bytes.Contains(resp, []byte("HTTP/1.1 400")) {
		t.Fatalf("expected 400, got %q", resp)
	}
}

func TestForwardServerClosedWithoutResponse(t *testing.T) {
	host, port := echoAddr(t)
	url := newFakeTunnel(t, testKey, closeAfterRequestHandler(
		websocket.StatusInternalError, "origin refused",
	))
	ts := newProxy(t, url, nil)

	rc := newRawClient(t, ts.Listener.Addr().String())
	rc.send("GET http://%s:%d/ HTTP/1.1\r\nHost: %s:%d\r\n\r\n", host, port, host, port)
	resp := rc.readRest()
	if !bytes.Contains(resp, []byte("HTTP/1.1 502")) {
		t.Fatalf("expected 502, got %q", resp)
	}
	// Either the drain surfaces the close reason ("tunnel closed by
	// server: ...") or the handshake does; both must carry it.
	if !bytes.Contains(resp, []byte("origin refused")) {
		t.Fatalf("expected server close reason, got %q", resp)
	}
}
