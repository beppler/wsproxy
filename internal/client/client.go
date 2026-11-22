package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/coder/websocket"

	"go.beppler.dev.br/wsproxy/internal/handshake"
	"go.beppler.dev.br/wsproxy/internal/tunnel"
)

const (
	// KeepaliveInterval is how often the client pings the server so
	// reverse proxies do not drop idle tunnels.
	KeepaliveInterval = 30 * time.Second
)

// Options describe a single tunneled connection.
type Options struct {
	// URL is the server endpoint. Only "wss://" is supported; the
	// server is always reached through a TLS-terminating reverse
	// proxy.
	URL string
	// Key is the shared secret. It never crosses the wire: it only
	// keys the HMAC-SHA256 signature of the destination, sent as the
	// single handshake message after the upgrade (never in the
	// handshake URL or headers).
	Key string
	// Host and Port are the destination behind the server, carried in
	// the signed handshake message sent right after the upgrade.
	Host string
	Port int
	// HandshakeTimeout bounds the WebSocket dial plus the server's
	// destination check and upstream dial, i.e. the whole tunnel
	// handshake. It must exceed the server's dial_timeout (default
	// 30s, well above the server's default 10s).
	HandshakeTimeout time.Duration
}

// Dial opens a tunneled WebSocket connection to the server.
func Dial(ctx context.Context, o Options) (*websocket.Conn, error) {
	u, err := url.Parse(o.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid server URL: %w", err)
	}
	if u.Scheme != "wss" {
		return nil, fmt.Errorf("only wss:// server URLs are supported, got %q", o.URL)
	}
	if u.Host == "" {
		return nil, errors.New("server URL is missing a host")
	}
	if o.Key == "" {
		return nil, errors.New("missing security key")
	}
	if o.Host == "" {
		return nil, errors.New("missing destination host")
	}
	if o.Port < 1 || o.Port > 65535 {
		return nil, fmt.Errorf("invalid destination port %d", o.Port)
	}
	if o.HandshakeTimeout <= 0 {
		// The handshake includes the server's upstream dial, so it
		// needs more headroom than the server's dial_timeout.
		o.HandshakeTimeout = 30 * time.Second
	}

	dctx, cancel := context.WithTimeout(ctx, o.HandshakeTimeout)
	defer cancel()
	conn, resp, err := websocket.Dial(dctx, o.URL, nil)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("connect to %s: %s", o.URL, resp.Status)
		}
		return nil, fmt.Errorf("connect to %s: %w", o.URL, err)
	}

	// The destination travels as a single signed handshake message on
	// the tunnel, so it and the key never appear in the request line,
	// headers or reverse-proxy access logs.
	data, err := json.Marshal(handshake.Sign(o.Host, o.Port, o.Key))
	if err != nil {
		_ = conn.Close(websocket.StatusInternalError, "")
		return nil, fmt.Errorf("encode handshake message: %w", err)
	}
	if err := conn.Write(dctx, websocket.MessageBinary, data); err != nil {
		_ = conn.Close(websocket.StatusInternalError, "")
		return nil, fmt.Errorf("send handshake message: %w", err)
	}

	// The server replies with a single message once the destination
	// was validated and the target dialed: AckOK on success, or the
	// reason the tunnel was not established (followed by a close
	// frame). The message text is self-describing; surface it as-is.
	_, adata, err := conn.Read(dctx)
	if err != nil {
		_ = conn.Close(websocket.StatusInternalError, "")
		return nil, fmt.Errorf("tunnel handshake with %s: %w", o.URL, err)
	}
	if string(adata) != tunnel.AckOK {
		_ = conn.Close(websocket.StatusPolicyViolation, "")
		return nil, fmt.Errorf("connect to %s: %s", o.URL, adata)
	}
	return conn, nil
}

// StartKeepalive pings the server every interval until ctx is done or
// a ping fails, in which case cancel is called to tear the tunnel
// down.
func StartKeepalive(ctx context.Context, conn *websocket.Conn, interval time.Duration, cancel context.CancelFunc) {
	if interval <= 0 {
		interval = KeepaliveInterval
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				pctx, pcancel := context.WithTimeout(ctx, interval/2)
				err := conn.Ping(pctx)
				pcancel()
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()
}
