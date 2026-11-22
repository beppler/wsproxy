// Package handshake carries the tunnel handshake between client and
// server: a single binary WebSocket message right after the upgrade
// holding the destination, a unix timestamp and an HMAC-SHA256
// signature over them, keyed by the shared key. The key itself never
// crosses the wire.
package handshake

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.beppler.dev.br/wsproxy/internal/dest"
)

// Version is the protocol version of the handshake message; the
// server rejects messages carrying any other value.
const Version = 1

// maxSkew bounds how far a handshake timestamp may drift from the
// server's clock, in seconds. A captured message stops working one
// minute after it was signed.
const maxSkew = 60 * time.Second

// macDomain separates the handshake MAC from any other HMAC use of
// the shared key and binds the protocol version into the signature.
const macDomain = "wsproxy-handshake-v1:"

// Message is the single post-upgrade handshake message. The signature
// covers the canonical "host:port" address and the unix timestamp;
// the version rides alongside unsigned (bound into the MAC by the
// domain prefix, so a future version cannot accept a v1 signature).
type Message struct {
	Version int    `json:"v"`
	Host    string `json:"host"`
	Port    int    `json:"port"`
	TS      int64  `json:"ts"`
	Sig     string `json:"sig"`
}

// Sign builds the handshake message for a destination, timestamped
// now: hex HMAC-SHA256 over the canonical host:port address and the
// timestamp, keyed by the shared key.
func Sign(host string, port int, key string) Message {
	return signAt(host, port, key, time.Now().Unix())
}

// signAt is Sign with an explicit timestamp, for tests.
func signAt(host string, port int, key string, ts int64) Message {
	return Message{
		Version: Version,
		Host:    host,
		Port:    port,
		TS:      ts,
		Sig:     hex.EncodeToString(mac(host, port, ts, key)),
	}
}

// Verify parses and authenticates a handshake message: the JSON must
// carry the current version, a signature matching the destination and
// timestamp under the shared key, and a timestamp within maxSkew of
// the server's clock (both directions, so a slightly fast client
// clock does not fail). It returns the authenticated destination.
func Verify(data []byte, key string) (host string, port int, err error) {
	var m Message
	if err := json.Unmarshal(data, &m); err != nil {
		return "", 0, fmt.Errorf("decode handshake message: %w", err)
	}
	if m.Version != Version {
		return "", 0, fmt.Errorf("unsupported handshake version %d", m.Version)
	}
	want, err := hex.DecodeString(m.Sig)
	if err != nil {
		return "", 0, errors.New("handshake signature is not hex")
	}
	if !hmac.Equal(want, mac(m.Host, m.Port, m.TS, key)) {
		return "", 0, errors.New("handshake signature mismatch")
	}
	if d := time.Since(time.Unix(m.TS, 0)); d > maxSkew || d < -maxSkew {
		return "", 0, errors.New("handshake timestamp outside the allowed clock skew")
	}
	return m.Host, m.Port, nil
}

// mac computes the HMAC-SHA256 over the domain-prefixed canonical
// host:port address and timestamp.
func mac(host string, port int, ts int64, key string) []byte {
	m := hmac.New(sha256.New, []byte(key))
	m.Write([]byte(macDomain))
	m.Write([]byte(dest.Join(host, port)))
	m.Write([]byte(":"))
	fmt.Fprintf(m, "%d", ts)
	return m.Sum(nil)
}
