package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path"

	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"go.beppler.dev.br/wsproxy/internal/config"
	"go.beppler.dev.br/wsproxy/internal/dest"
	"go.beppler.dev.br/wsproxy/internal/handshake"
	"go.beppler.dev.br/wsproxy/internal/tunnel"
)

const (
	// defaultDestWait is the initial value of Server.destWait: how
	// long the server waits for the single handshake message
	// (destination + signature) after the upgrade; clients send it
	// immediately.
	defaultDestWait = 5 * time.Second
	// ackWait bounds the server's own control writes (ack/rejection)
	// before the pump starts.
	ackWait = 5 * time.Second
)

// errUnauthorized is the only rejection surfaced before the client
// proves knowledge of the shared key: a missing, malformed, wrong
// version or wrongly signed handshake message is indistinguishable
// from the outside.
var errUnauthorized = errors.New("unauthorized")

// Server is a wsproxy server: it authenticates WebSocket clients,
// enforces the destination allowlist and relays each tunnel (one
// WebSocket per connection) to its destination.
type Server struct {
	cfg   config.ServerConfig
	key   string
	conns atomic.Int64

	// destWait bounds how long readHandshake waits for the handshake
	// message; New sets it from defaultDestWait, and tests shrink it
	// to exercise the deadline deterministically.
	destWait time.Duration
}

// New creates a Server.
func New(cfg config.ServerConfig) (*Server, error) {
	if cfg.Key == "" {
		return nil, errors.New("server: key is required")
	}
	return &Server{cfg: cfg, key: cfg.Key, destWait: defaultDestWait}, nil
}

// Handler returns the http.Handler serving the WebSocket endpoint.
func (s *Server) Handler() http.Handler { return http.HandlerFunc(s.serveHTTP) }

func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if max := s.cfg.MaxConcurrent; max > 0 && s.conns.Load() >= int64(max) {
		http.Error(w, "too many concurrent tunnels", http.StatusServiceUnavailable)
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Clients are not browsers; the HMAC-signed handshake message,
		// sent as the first WebSocket message, is the authentication
		// mechanism, so skip the origin check.
		InsecureSkipVerify: true,
	})
	if err != nil {
		return
	}
	s.conns.Add(1)
	defer s.conns.Add(-1)

	host, port, err := s.readHandshake(conn)
	if err == nil && !s.destAllowed(host, port) {
		err = errors.New("destination not allowed")
	}
	if err != nil {
		// Fail fast: no dial attempt for a rejected destination.
		s.reject(conn, err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dialer := &net.Dialer{Timeout: time.Duration(s.cfg.DialTimeout)}
	target, err := dialer.DialContext(ctx, "tcp", dest.Join(host, port))
	if err != nil {
		s.reject(conn, fmt.Errorf("upstream dial failed: %w", err))
		return
	}
	defer target.Close()

	// The single ack goes out only once the target connection is up,
	// so client.Dial returns with a fully established tunnel.
	actx, acancel := context.WithTimeout(context.Background(), ackWait)
	defer acancel()
	if err := conn.Write(actx, websocket.MessageBinary, []byte(tunnel.AckOK)); err != nil {
		conn.Close(websocket.StatusInternalError, "ack write failed")
		return
	}

	ac := newActivityConn(target)
	if idle := time.Duration(s.cfg.IdleTimeout); idle > 0 {
		go s.watchIdle(ctx, ac, conn, idle)
	}
	tunnel.Pump(ctx, conn, ac)
}

// handshakeGate arbitrates the destWait timer against the handshake
// read, so a rejection can never be written onto a connection whose
// handshake just completed: t.Stop cannot un-fire a callback that is
// already running, so either the read completes first (and the timer
// stays silent) or the timer fires first (and the read abandons the
// handshake) — exactly one side owns the connection.
//
// Ownership is a single CompareAndSwap on state, so it holds without
// a lock: exactly one side flips undecided, and the winner acts on
// the connection while the loser returns without blocking — even
// while the timer's reject is still writing to the connection.
type handshakeGate struct {
	state atomic.Int32 // gateUndecided, gateReadWon or gateTimerWon
}

const (
	gateUndecided int32 = iota
	gateReadWon         // conn.Read returned and claimed the handshake
	gateTimerWon        // the timer fired first; owns the rejection and the close
)

// completeRead claims the handshake for the read. It must be called
// at most once. It reports whether the handshake may proceed (false:
// the timer won the gate and owns the rejection and the close).
func (g *handshakeGate) completeRead() bool {
	return g.state.CompareAndSwap(gateUndecided, gateReadWon)
}

// fire runs reject if — and only if — the timer wins the flip from
// undecided: the rejection and its close frame still originate from
// the timer side, which keeps the close handshake intact (see
// readHandshake). reject runs after the CAS, so nothing is held
// across it. Later calls are no-ops.
func (g *handshakeGate) fire(reject func()) {
	if !g.state.CompareAndSwap(gateUndecided, gateTimerWon) {
		return
	}
	reject()
}

// readHandshake reads the single post-upgrade message: a JSON object
// carrying the destination and an HMAC-SHA256 signature over it,
// keyed by the shared key — so the key itself never crosses the wire.
//
// A client that never sends the handshake message gets the same
// opaque rejection; closing from a timer (instead of canceling the
// read context) keeps the close handshake intact.
func (s *Server) readHandshake(conn *websocket.Conn) (string, int, error) {
	g := &handshakeGate{}
	t := time.AfterFunc(s.destWait, func() { g.fire(func() { s.reject(conn, errUnauthorized) }) })
	defer t.Stop()

	typ, data, err := conn.Read(context.Background())
	return s.verifyHandshake(g, typ, data, err)
}

// verifyHandshake arbitrates the completed read against the gate and
// verifies the message. A read that lost the gate — the destWait
// timer fired first and owns the rejection and the close frame — is
// abandoned even when it returned data. The message is first
// authenticated, then its destination parsed: every authentication
// failure — a missing, malformed, non-binary, wrong-version or
// wrongly signed message, or a client that never speaks — yields the
// same opaque errUnauthorized; destination parsing failures are only
// surfaced to an authenticated client.
func (s *Server) verifyHandshake(g *handshakeGate, typ websocket.MessageType, data []byte, readErr error) (host string, port int, err error) {
	if !g.completeRead() {
		// The destWait timer fired while we were reading: it won
		// the gate and owns the rejection and the close frame.
		// The follow-up reject from serveHTTP carries the same
		// bytes and fails silently on the closing connection.
		return "", 0, errUnauthorized
	}
	if readErr != nil || typ != websocket.MessageBinary {
		return "", 0, errUnauthorized
	}
	host, port, err = handshake.Verify(data, s.key)
	if err != nil {
		return "", 0, errUnauthorized
	}
	host, port, err = dest.ParseAddress(dest.Join(host, port), 80)
	if err != nil {
		return "", 0, fmt.Errorf("invalid destination: %w", err)
	}
	return host, port, nil
}

// reject informs the client that the tunnel was not established (a
// rejected destination or a failed upstream dial): one binary message
// carrying the reason, then a close frame with the same error.
func (s *Server) reject(conn *websocket.Conn, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), ackWait)
	defer cancel()
	_ = conn.Write(ctx, websocket.MessageBinary, []byte(err.Error()))
	_ = conn.Close(websocket.StatusPolicyViolation, closeReason(err))
}

// closeReason renders err as a WebSocket close reason (capped well
// below the 123-byte limit of the close frame).
func closeReason(err error) string {
	r := err.Error()
	if len(r) > 100 {
		r = r[:100]
	}
	return r
}

// destAllowed checks the allowlist. A nil Allow (not configured)
// permits everything; a configured empty list permits nothing.
func (s *Server) destAllowed(host string, port int) bool {
	if s.cfg.Allow == nil {
		return true
	}
	target := strings.ToLower(dest.Join(host, port))
	// Escape [ ] so path.Match does not read an IPv6 literal in the
	// pattern ("[::1]:*") as a character class.
	for _, pattern := range s.cfg.Allow {
		escaped := strings.ReplaceAll(strings.ReplaceAll(strings.ToLower(pattern), "[", `\[`), "]", `\]`)
		if ok, _ := path.Match(escaped, target); ok {
			return true
		}
	}
	return false
}

// watchIdle closes the tunnel when no traffic has crossed the
// destination connection for longer than idle.
func (s *Server) watchIdle(ctx context.Context, ac *activityConn, conn *websocket.Conn, idle time.Duration) {
	interval := idle / 2
	if interval <= 0 {
		interval = idle
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if time.Since(ac.lastActivity()) > idle {
				conn.Close(websocket.StatusGoingAway, "idle timeout")
				return
			}
		}
	}
}

// activityConn wraps a net.Conn, recording the time of the last byte
// transferred in either direction.
type activityConn struct {
	net.Conn
	last atomic.Int64
}

func newActivityConn(c net.Conn) *activityConn {
	ac := &activityConn{Conn: c}
	ac.touch()
	return ac
}

func (c *activityConn) touch() { c.last.Store(time.Now().UnixNano()) }

func (c *activityConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.touch()
	return n, err
}

func (c *activityConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.touch()
	return n, err
}

func (c *activityConn) lastActivity() time.Time { return time.Unix(0, c.last.Load()) }
