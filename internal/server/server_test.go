package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"go.beppler.dev.br/wsproxy/internal/config"
	"go.beppler.dev.br/wsproxy/internal/dest"
	"go.beppler.dev.br/wsproxy/internal/handshake"
	"go.beppler.dev.br/wsproxy/internal/tunnel"
)

// echoTarget starts a TCP echo server used as the tunnel destination.
// It also returns a counter of accepted connections, so tests can
// assert the destination was never dialed.
func echoTarget(t *testing.T) (host string, port int, conns *atomic.Int64) {
	t.Helper()
	var accepted atomic.Int64
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
			accepted.Add(1)
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return "127.0.0.1", ln.Addr().(*net.TCPAddr).Port, &accepted
}

// newTestServer starts the server handler behind httptest and returns
// the server — so tests can adjust its knobs, e.g. destWait — along
// with its ws:// URL.
func newTestServer(t *testing.T, cfg config.ServerConfig) (*Server, string) {
	t.Helper()
	srv, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return srv, "ws" + strings.TrimPrefix(ts.URL, "http")
}

// dialTunnel performs the WebSocket handshake and the tunnel
// handshake: the single signed message carries the destination, signed
// with the given key (an empty key and address sends nothing). It
// returns the first message the server sends — the ack on success, or
// the rejection reason.
func dialTunnel(t *testing.T, url, key, address string) (*websocket.Conn, []byte, error) {
	t.Helper()
	// Wide enough for a missing-handshake-message test, where the
	// server only gives up on the message after destWait.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return nil, nil, err
	}
	if key != "" && address != "" {
		// Parse the address the same way the protocol does, so every
		// accepted form (host, host:port, [v6]:port) signs correctly.
		host, port, err := dest.ParseAddress(address, 80)
		if err != nil {
			_ = conn.CloseNow()
			t.Fatalf("dialTunnel: parse address %q: %v", address, err)
		}
		data, err := json.Marshal(handshake.Sign(host, port, key))
		if err != nil {
			_ = conn.CloseNow()
			t.Fatalf("dialTunnel: encode handshake message: %v", err)
		}
		if err := conn.Write(ctx, websocket.MessageBinary, data); err != nil {
			_ = conn.CloseNow()
			return conn, nil, err
		}
	}
	_, ack, err := conn.Read(ctx)
	if err != nil {
		return conn, nil, err
	}
	return conn, ack, nil
}

// dialRaw performs the WebSocket handshake and sends payload verbatim
// as the first (and only) message. It returns the first message the
// server sends.
func dialRaw(t *testing.T, url, payload string) (*websocket.Conn, []byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return nil, nil, err
	}
	if payload != "" {
		if err := conn.Write(ctx, websocket.MessageBinary, []byte(payload)); err != nil {
			_ = conn.CloseNow()
			return conn, nil, err
		}
	}
	_, ack, err := conn.Read(ctx)
	if err != nil {
		return conn, nil, err
	}
	return conn, ack, nil
}

// readClose reads until the server's close frame arrives and returns
// its code and reason.
func readClose(t *testing.T, conn *websocket.Conn) websocket.CloseError {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		_, _, err := conn.Read(ctx)
		if err == nil {
			continue // skip any data frames before the close
		}
		var ce websocket.CloseError
		if !errors.As(err, &ce) {
			t.Fatalf("expected a close frame, got %v", err)
		}
		return ce
	}
}

// expectClose reads from conn until the server's close frame arrives
// and returns its status and reason.
func expectClose(t *testing.T, conn *websocket.Conn) (websocket.StatusCode, string) {
	t.Helper()
	// The server may take up to destWait to give up on the
	// destination message before sending its close frame.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for {
		_, _, err := conn.Read(ctx)
		if err == nil {
			continue // skip any data frames before the close
		}
		var ce websocket.CloseError
		if !errors.As(err, &ce) {
			t.Fatalf("expected a close frame, got %v", err)
		}
		return ce.Code, ce.Reason
	}
}

func TestEndToEndEcho(t *testing.T) {
	host, port, _ := echoTarget(t)
	_, url := newTestServer(t, config.ServerConfig{Key: "secret"})

	conn, ack, err := dialTunnel(t, url, "secret", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	if string(ack) != tunnel.AckOK {
		t.Fatalf("expected ack %q, got %q", tunnel.AckOK, ack)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageBinary, []byte("hello tunnel")); err != nil {
		t.Fatalf("write: %v", err)
	}
	typ, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if typ != websocket.MessageBinary || string(data) != "hello tunnel" {
		t.Fatalf("got (%v, %q)", typ, data)
	}
}

func TestBadKeyRejected(t *testing.T) {
	_, url := newTestServer(t, config.ServerConfig{Key: "secret"})

	// The upgrade succeeds; a message signed with the wrong key
	// yields the same opaque rejection as a missing one, followed by
	// a close frame.
	conn, ack, err := dialTunnel(t, url, "wrong", "127.0.0.1:80")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()
	if string(ack) != "unauthorized" {
		t.Fatalf("expected %q, got %q", "unauthorized", ack)
	}
	if ce := readClose(t, conn); ce.Code != websocket.StatusPolicyViolation {
		t.Fatalf("expected close status %d, got %d", websocket.StatusPolicyViolation, ce.Code)
	}
}

func TestMissingMessageRejected(t *testing.T) {
	_, url := newTestServer(t, config.ServerConfig{Key: "secret"})

	// No handshake message at all: same opaque rejection as a
	// wrongly signed one.
	conn, ack, err := dialTunnel(t, url, "", "")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()
	if string(ack) != "unauthorized" {
		t.Fatalf("expected %q, got %q", "unauthorized", ack)
	}
	if ce := readClose(t, conn); ce.Code != websocket.StatusPolicyViolation {
		t.Fatalf("expected close status %d, got %d", websocket.StatusPolicyViolation, ce.Code)
	}
}

// TestLateMessageRejected exercises readHandshake's lose path
// end-to-end (issue #5): the destWait timer fires while a valid,
// correctly signed handshake message is in flight, the gate hands the
// connection to the timer, and the late message must be abandoned —
// no rejection other than the timer's, no dial, no ack.
func TestLateMessageRejected(t *testing.T) {
	srv, url := newTestServer(t, config.ServerConfig{Key: "secret"})
	host, port, conns := echoTarget(t)
	srv.destWait = 50 * time.Millisecond // shrink the window; the sleep below is 10x

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()

	// Stay silent past destWait, so the timer fires and claims the
	// connection; only then send a valid handshake message.
	time.Sleep(500 * time.Millisecond)
	data, err := json.Marshal(handshake.Sign(host, port, "secret"))
	if err != nil {
		t.Fatalf("encode handshake: %v", err)
	}
	if err := conn.Write(ctx, websocket.MessageBinary, data); err != nil {
		t.Fatalf("write handshake: %v", err)
	}

	// The rejection — every data message must be the unauthorized
	// reason (the follow-up reject from serveHTTP may duplicate it) —
	// followed by the timer-side close.
	for {
		typ, msg, err := conn.Read(ctx)
		if err != nil {
			var ce websocket.CloseError
			if !errors.As(err, &ce) {
				t.Fatalf("expected a close frame, got %v", err)
			}
			if ce.Code != websocket.StatusPolicyViolation {
				t.Fatalf("expected close status %d, got %d", websocket.StatusPolicyViolation, ce.Code)
			}
			if ce.Reason != errUnauthorized.Error() {
				t.Fatalf("expected close reason %q, got %q", errUnauthorized.Error(), ce.Reason)
			}
			break
		}
		if typ != websocket.MessageBinary || string(msg) != errUnauthorized.Error() {
			t.Fatalf("expected the unauthorized rejection, got (%v, %q)", typ, msg)
		}
	}

	// The gate is consulted before the allowlist and the dial, so a
	// handshake that lost it must never reach the destination: give
	// any wiring-ignored dial time to land, then assert none did.
	time.Sleep(250 * time.Millisecond)
	if n := conns.Load(); n != 0 {
		t.Fatalf("destination dialed %d time(s) despite the lost gate", n)
	}
}

func TestMalformedMessageRejected(t *testing.T) {
	for name, payload := range map[string]string{
		"garbage bytes": "\x00\x01",
		"not json":      "127.0.0.1:80",
		"bad sig":       `{"v":1,"host":"127.0.0.1","port":80,"sig":"` + strings.Repeat("0", 64) + `"}`,
		"not hex":       `{"v":1,"host":"127.0.0.1","port":80,"sig":"zz"}`,
		"wrong version": `{"v":99,"host":"127.0.0.1","port":80,"sig":"` + strings.Repeat("0", 64) + `"}`,
	} {
		_, url := newTestServer(t, config.ServerConfig{Key: "secret"})
		conn, ack, err := dialRaw(t, url, payload)
		if err != nil {
			t.Fatalf("%s: dial: %v", name, err)
		}
		if string(ack) != "unauthorized" {
			t.Errorf("%s: expected %q, got %q", name, "unauthorized", ack)
		}
		if ce := readClose(t, conn); ce.Code != websocket.StatusPolicyViolation {
			t.Errorf("%s: expected close status %d, got %d", name, websocket.StatusPolicyViolation, ce.Code)
		}
		conn.CloseNow()
	}
}

func TestDialFailureReported(t *testing.T) {
	// Reserve a port, then release it: dialing it is refused quickly.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	_, url := newTestServer(t, config.ServerConfig{Key: "secret"})
	conn, ack, err := dialTunnel(t, url, "secret", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()

	// The reason is a single message naming the dial failure,
	// followed by a close frame.
	if string(ack) == tunnel.AckOK {
		t.Fatal("expected a failure reason, got the success ack")
	}
	if !strings.HasPrefix(string(ack), "upstream dial failed: ") {
		t.Fatalf("expected dial failure reason, got %q", ack)
	}
	if ce := readClose(t, conn); ce.Code != websocket.StatusPolicyViolation {
		t.Fatalf("expected close status %d, got %d", websocket.StatusPolicyViolation, ce.Code)
	}
}

func TestAllowlist(t *testing.T) {
	host, port, _ := echoTarget(t)
	_, url := newTestServer(t, config.ServerConfig{
		Key:   "secret",
		Allow: []string{"*.example.com:*"},
	})

	// Not matching the allowlist -> rejection message + close frame.
	conn, ack, err := dialTunnel(t, url, "secret", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()
	if string(ack) != "destination not allowed" {
		t.Fatalf("expected rejection message, got %q", ack)
	}
	if ce := readClose(t, conn); ce.Code != websocket.StatusPolicyViolation {
		t.Fatalf("expected close status %d, got %d", websocket.StatusPolicyViolation, ce.Code)
	}

	// Matching the allowlist -> tunnel works.
	_, url2 := newTestServer(t, config.ServerConfig{
		Key:   "secret",
		Allow: []string{"127.0.0.1:*"},
	})
	conn2, ack2, err := dialTunnel(t, url2, "secret", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn2.Close(websocket.StatusNormalClosure, "")
	if string(ack2) != tunnel.AckOK {
		t.Fatalf("expected ack %q, got %q", tunnel.AckOK, ack2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn2.Write(ctx, websocket.MessageBinary, []byte("x")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, _, err := conn2.Read(ctx); err != nil {
		t.Fatalf("read: %v", err)
	}
}

func TestDestAllowedIPv6(t *testing.T) {
	// The match string must use the bracketed form dest.Join renders
	// for IPv6 hosts (issue #16).
	s := &Server{cfg: config.ServerConfig{Allow: []string{"[::1]:*"}}}
	if !s.destAllowed("::1", 22) {
		t.Fatal("[::1]:* should match IPv6 host ::1 port 22")
	}
	if s.destAllowed("::2", 22) {
		t.Fatal("[::1]:* should not match a different host")
	}
	s2 := &Server{cfg: config.ServerConfig{Allow: nil}}
	if !s2.destAllowed("::1", 22) {
		t.Fatal("nil allowlist permits everything")
	}
}

// The tests below drive the handshake gate and the post-read decision
// directly (issue #5): the real destWait window is far too narrow to
// hit in an integration test.

func TestHandshakeGateReadWins(t *testing.T) {
	var fired int
	g := &handshakeGate{}
	if !g.completeRead() {
		t.Fatal("read lost before the timer fired")
	}
	for i := 0; i < 2; i++ {
		g.fire(func() { fired++ })
	}
	if fired != 0 {
		t.Fatalf("timer rejected a completed handshake %d time(s)", fired)
	}
}

func TestHandshakeGateTimerWins(t *testing.T) {
	var fired int
	g := &handshakeGate{}
	g.fire(func() { fired++ })
	if fired != 1 {
		t.Fatalf("timer fired %d time(s), want 1", fired)
	}
	if g.completeRead() {
		t.Fatal("handshake proceeded after the timer rejected it")
	}
	g.fire(func() { fired++ })
	if fired != 1 {
		t.Fatalf("rejection ran %d time(s), want 1", fired)
	}
}

func TestHandshakeGateRace(t *testing.T) {
	// Fire and read concurrently: whichever side wins, the outcome
	// is exclusive — the rejection runs at most once, and the read
	// completes the handshake only when no rejection ran.
	for i := 0; i < 1000; i++ {
		var (
			fired  atomic.Int64
			readOK bool
			wg     sync.WaitGroup
		)
		g := &handshakeGate{}
		wg.Add(2)
		go func() {
			defer wg.Done()
			g.fire(func() { fired.Add(1) })
		}()
		go func() {
			defer wg.Done()
			readOK = g.completeRead()
		}()
		wg.Wait()

		switch {
		case readOK && fired.Load() != 0:
			t.Fatalf("iteration %d: handshake completed but reject ran %d time(s)", i, fired.Load())
		case !readOK && fired.Load() != 1:
			t.Fatalf("iteration %d: read lost but reject ran %d time(s)", i, fired.Load())
		}
	}
}

func TestHandshakeAbandonedWhenTimerWon(t *testing.T) {
	srv, err := New(config.ServerConfig{Key: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(handshake.Sign("127.0.0.1", 80, "secret"))
	if err != nil {
		t.Fatal(err)
	}

	// A read that lost the gate — the timer fired first and owns the
	// rejection and the close — must be abandoned even when it
	// returned a valid, correctly signed message: deadline semantics
	// linearize at read-return.
	g := &handshakeGate{}
	g.fire(func() {})
	if _, _, err := srv.verifyHandshake(g, websocket.MessageBinary, data, nil); !errors.Is(err, errUnauthorized) {
		t.Fatalf("timer won the gate but the message was processed: %v", err)
	}
}

func TestHandshakeVerifiedWhenReadWon(t *testing.T) {
	srv, err := New(config.ServerConfig{Key: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(handshake.Sign("127.0.0.1", 80, "secret"))
	if err != nil {
		t.Fatal(err)
	}

	gotHost, gotPort, err := srv.verifyHandshake(&handshakeGate{}, websocket.MessageBinary, data, nil)
	if err != nil {
		t.Fatalf("read won the gate but the handshake failed: %v", err)
	}
	if gotHost != "127.0.0.1" || gotPort != 80 {
		t.Fatalf("got %s:%d, want 127.0.0.1:80", gotHost, gotPort)
	}
}
