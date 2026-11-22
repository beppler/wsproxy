package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"go.beppler.dev.br/wsproxy/internal/dest"
	"go.beppler.dev.br/wsproxy/internal/handshake"
	"go.beppler.dev.br/wsproxy/internal/tunnel"
)

const testKey = "secret"

// echoAddr starts a TCP echo server used as the tunnel destination.
func echoAddr(t *testing.T) (host string, port int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return "127.0.0.1", ln.Addr().(*net.TCPAddr).Port
}

// closedPort reserves a TCP port and releases it again, so dialing it
// is refused quickly.
func closedPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

// trustTestCert makes http.DefaultTransport trust the self-signed
// certificate of an httptest TLS server for the duration of the test.
// client.Dial enforces wss:// (and websocket.Dial falls back to
// http.DefaultClient), so this is the only way tests can reach an
// httptest server over TLS without changing production code. Tests in
// this package do not run in parallel.
func trustTestCert(t *testing.T, ts *httptest.Server) {
	t.Helper()
	tp := http.DefaultTransport.(*http.Transport).Clone()
	if tp.TLSClientConfig == nil {
		tp.TLSClientConfig = &tls.Config{}
	}
	pool := x509.NewCertPool()
	pool.AddCert(ts.Certificate())
	tp.TLSClientConfig.RootCAs = pool
	old := http.DefaultTransport
	http.DefaultTransport = tp
	t.Cleanup(func() { http.DefaultTransport = old })
}

// newFakeTunnel starts an httptest TLS server speaking the wsproxy
// protocol: after the upgrade, it reads the single handshake message,
// verifies its HMAC signature under key (a missing, wrong or
// malformed message gets the same rejection as the real server's) and
// then hands the connection plus the authenticated destination to
// handle for as long as the tunnel lives. It returns the wss:// URL.
func newFakeTunnel(t *testing.T, key string, handle func(ctx context.Context, conn *websocket.Conn, host string, port int)) string {
	t.Helper()
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true, // clients are not browsers
		})
		if err != nil {
			return
		}
		ctx := context.Background()
		typ, data, err := conn.Read(ctx)
		host, port, verr := handshake.Verify(data, key)
		if err != nil || typ != websocket.MessageBinary || verr != nil {
			rejectTunnel(ctx, conn, errors.New("unauthorized"))
			return
		}
		// The handler runs for the tunnel's lifetime; the request
		// context would be canceled as soon as ServeHTTP returns.
		handle(ctx, conn, host, port)
	}))
	trustTestCert(t, ts)
	t.Cleanup(ts.Close)
	return "wss" + strings.TrimPrefix(ts.URL, "https")
}

// rejectTunnel sends the reason message followed by a close frame,
// mirroring the real server's rejection.
func rejectTunnel(ctx context.Context, conn *websocket.Conn, reason error) {
	wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
	defer wcancel()
	_ = conn.Write(wctx, websocket.MessageBinary, []byte(reason.Error()))
	_ = conn.Close(websocket.StatusPolicyViolation, reason.Error())
}

// echoTunnelHandler implements the server side of the protocol for an
// echo destination: dial it, ack, then pump bytes both ways.
func echoTunnelHandler() func(ctx context.Context, conn *websocket.Conn, host string, port int) {
	return func(_ context.Context, conn *websocket.Conn, host string, port int) {
		ctx := context.Background()
		target, err := net.DialTimeout("tcp", dest.Join(host, port), 5*time.Second)
		if err != nil {
			rejectTunnel(ctx, conn, fmt.Errorf("upstream dial failed: %w", err))
			return
		}
		defer target.Close()
		if err := conn.Write(ctx, websocket.MessageBinary, []byte(tunnel.AckOK)); err != nil {
			return
		}
		_ = tunnel.Pump(ctx, conn, target)
	}
}

// rejectTunnelHandler rejects the tunnel with a fixed reason without
// dialing anything.
func rejectTunnelHandler(reason string) func(ctx context.Context, conn *websocket.Conn, host string, port int) {
	return func(ctx context.Context, conn *websocket.Conn, _ string, _ int) {
		rejectTunnel(ctx, conn, errors.New(reason))
	}
}

// slowAckHandler acks the tunnel only after a fixed delay.
func slowAckHandler(delay time.Duration) func(ctx context.Context, conn *websocket.Conn, host string, port int) {
	return func(_ context.Context, conn *websocket.Conn, _ string, _ int) {
		ctx := context.Background()
		time.Sleep(delay)
		_ = conn.Write(ctx, websocket.MessageBinary, []byte(tunnel.AckOK))
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}
}

func TestDialRequiresWSS(t *testing.T) {
	cases := []struct {
		name string
		url  string
		want string
	}{
		{"ws scheme", "ws://srv.example/ws", "only wss:// server URLs are supported"},
		{"http scheme", "http://srv.example/ws", "only wss:// server URLs are supported"},
		{"no scheme", "srv.example/ws", "only wss:// server URLs are supported"},
		{"https scheme", "https://srv.example/ws", "only wss:// server URLs are supported"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Dial(context.Background(), Options{URL: tc.url, Key: "k", Host: "h", Port: 80})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestDialValidation(t *testing.T) {
	const url = "wss://srv.example/ws"
	cases := []struct {
		name string
		o    Options
		want string
	}{
		{"missing host in URL", Options{URL: "wss:///ws", Key: "k", Host: "h", Port: 80}, "missing a host"},
		{"missing key", Options{URL: url, Host: "h", Port: 80}, "missing security key"},
		{"missing destination host", Options{URL: url, Key: "k", Port: 80}, "missing destination host"},
		{"port zero", Options{URL: url, Key: "k", Host: "h", Port: 0}, "invalid destination port"},
		{"port too high", Options{URL: url, Key: "k", Host: "h", Port: 70000}, "invalid destination port"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Dial(context.Background(), tc.o)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestDialHandshakeSuccess(t *testing.T) {
	host, port := echoAddr(t)
	url := newFakeTunnel(t, testKey, echoTunnelHandler())

	conn, err := Dial(context.Background(), Options{
		URL:  url,
		Key:  testKey,
		Host: host,
		Port: port,
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageBinary, []byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}
	typ, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if typ != websocket.MessageBinary || string(data) != "hello" {
		t.Fatalf("got (%v, %q)", typ, data)
	}
}

func TestDialSurfacesRejectionReason(t *testing.T) {
	url := newFakeTunnel(t, testKey, rejectTunnelHandler("destination not allowed"))
	_, err := Dial(context.Background(), Options{URL: url, Key: testKey, Host: "db.internal.example", Port: 22})
	if err == nil || !strings.Contains(err.Error(), "destination not allowed") {
		t.Fatalf("expected rejection reason in error, got %v", err)
	}
}

func TestDialSurfacesUpstreamDialFailure(t *testing.T) {
	// Reserve a port, then release it: dialing it is refused quickly.
	port := closedPort(t)
	url := newFakeTunnel(t, testKey, echoTunnelHandler())

	_, err := Dial(context.Background(), Options{URL: url, Key: testKey, Host: "127.0.0.1", Port: port})
	if err == nil || !strings.Contains(err.Error(), "upstream dial failed") {
		t.Fatalf("expected upstream dial failure in error, got %v", err)
	}
}

func TestDialHandshakeTimeout(t *testing.T) {
	url := newFakeTunnel(t, testKey, slowAckHandler(2*time.Second))
	_, err := Dial(context.Background(), Options{
		URL:              url,
		Key:              testKey,
		Host:             "127.0.0.1",
		Port:             9, // never dialed; the server just acks slowly
		HandshakeTimeout: 300 * time.Millisecond,
	})
	if err == nil || !strings.Contains(err.Error(), "tunnel handshake") {
		t.Fatalf("expected handshake timeout error, got %v", err)
	}
}

func TestStartKeepalive(t *testing.T) {
	// The handler echoes (so Pump keeps the server reading and it
	// answers pings) and exposes the server-side connection so the
	// test can kill it later.
	serverConn := make(chan *websocket.Conn, 1)
	inner := echoTunnelHandler()
	url := newFakeTunnel(t, testKey, func(ctx context.Context, conn *websocket.Conn, host string, port int) {
		select {
		case serverConn <- conn:
		default:
		}
		inner(ctx, conn, host, port)
	})

	host, port := echoAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn, err := Dial(ctx, Options{URL: url, Key: testKey, Host: host, Port: port})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	// Keep the client side reading so coder/websocket dispatches the
	// server's pong replies to Ping (in production, tunnel.Pump or
	// tunnel.Drain is always the reader).
	go func() {
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
		}
	}()

	tctx, tcancel := context.WithCancel(context.Background())
	defer tcancel()
	StartKeepalive(tctx, conn, 20*time.Millisecond, tcancel)

	// Healthy tunnel: several keepalive cycles pass without the
	// tunnel being torn down.
	select {
	case <-tctx.Done():
		t.Fatalf("keepalive canceled a healthy tunnel: %v", tctx.Err())
	case <-time.After(150 * time.Millisecond):
	}

	// The server side dies: the next ping fails and the tunnel must
	// be torn down promptly.
	select {
	case sc := <-serverConn:
		_ = sc.CloseNow()
	case <-time.After(5 * time.Second):
		t.Fatal("server never handed out its connection")
	}
	select {
	case <-tctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("keepalive did not cancel after the server died")
	}
}

func TestStartKeepaliveStopsOnCtxDone(t *testing.T) {
	host, port := echoAddr(t)
	url := newFakeTunnel(t, testKey, echoTunnelHandler())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn, err := Dial(ctx, Options{URL: url, Key: testKey, Host: host, Port: port})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	tctx, tcancel := context.WithCancel(context.Background())
	StartKeepalive(tctx, conn, 20*time.Millisecond, tcancel)
	tcancel() // the caller cancels: the keepalive goroutine must exit

	// No way to observe the goroutine directly; a panic/crash would
	// fail the test run, so just give it time to wind down.
	time.Sleep(50 * time.Millisecond)
}
