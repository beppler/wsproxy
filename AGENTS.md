# AGENTS.md

Guidance for AI coding agents working in this repository.

## Project overview

`wsproxy` is a minimal wstunnel-like tool in Go: it tunnels TCP
connections over WebSockets and is designed to sit behind a reverse
proxy (Caddy/Nginx) that terminates TLS. One binary, two subcommands
(`server`, `client`). See `README.md` for usage and GitHub issue #1
for the design and decision history — plans live as GitHub issues,
not in the repository.

## Commands

Go **may not be installed** in the working environment. Prefer
[mise](https://mise.jdx.dev/) when available: `mise use go@latest`
(creates `.mise.toml`) or `mise x go -- go test ./...`. If neither Go
nor mise is present, install Go from https://go.dev/dl/ into a writable
prefix. In this sandbox Go is pre-installed at `/usr/local/go/bin/go`
(may not be on `PATH`).

    /usr/local/go/bin/go build ./...
    /usr/local/go/bin/go build -o wsproxy ./cmd/wsproxy
    /usr/local/go/bin/go test ./...
    /usr/local/go/bin/go vet ./...
    /usr/local/go/bin/gofmt -l .   # must print nothing before committing

No linters or code generators are configured; stdlib + two deps only
(`github.com/coder/websocket`, `gopkg.in/yaml.v3`). Run
`/usr/local/go/bin/go mod tidy` after changing dependencies.

## Layout

- `cmd/wsproxy/main.go` — subcommand dispatch and flags only. Keep
  business logic out of here.
- `internal/config/` — server YAML config loading, validation,
  `${ENV}` expansion.
- `internal/dest/` — shared host/port validation helpers (used by both
  sides).
- `internal/handshake/` — the single signed handshake message (JSON +
  HMAC-SHA256), shared by both sides.
- `internal/tunnel/` — the byte pipe between a WebSocket and a duplex
  stream (`Pump`, `Drain`). All framing lives here.
- `internal/server/` — WS server: limits → upgrade → verify signed
  handshake → allowlist → dial → pump.
- `internal/client/` — `client.go` (wss dialer + keepalive),
  `httpproxy.go` (CONNECT + absolute-URI), `stdio.go` (ssh
  ProxyCommand).

## Locked-in design decisions

Do not change these without updating the plan issue (#1) and the
README:

- **One WebSocket per tunneled connection.** No stream multiplexing; a
  tunnel is a transparent byte pipe (32 KiB binary messages).
- **`wss://` only on the client.** No insecure-transport fallback, no
  `--insecure` flag. TLS is the reverse proxy's job.
- **Server listens on a local TCP address** (default `localhost:2345`,
  configurable via the config's `listen` key). Non-loopback addresses
  expose the endpoint beyond localhost; the shared key remains the
  access control.
- **Auth + destination in one signed message.** The client sends a
  **single** JSON message right after the upgrade:
  `{"v":1,"host":...,"port":...,"ts":...,"sig":...}` where `sig` is
  `hex(HMAC-SHA256(key, "wsproxy-handshake-v1:host:port:ts"))`
  (`internal/handshake`); `ts` is unix seconds. The version is bound
  into the MAC via the `wsproxy-handshake-v1:` domain prefix. The
  server rejects timestamps more than one minute away from its clock
  (either direction), so a captured handshake is only replayable for
  ~1 minute. The key itself is **never** transmitted; the server
  verifies the HMAC in constant time (`hmac.Equal`). A missing,
  malformed, wrong-version, stale or wrongly signed message gets the
  same opaque `unauthorized` rejection.
- **Destination** travels inside that signed message (`host`,
  `host:port` or `[v6]:port`; default port 80) — never in the
  handshake URL or headers. Constrained by the optional `allow` glob
  list (checked after the upgrade, before dialing). The server
  replies with a single `ok` message only after the upstream dial
  succeeds; any other message carries the reason the tunnel was not
  established (auth failure, policy rejection or dial failure,
  including the cause) and is followed by a close frame
  (`tunnel.AckOK` is the shared framing constant). Client handshake
  timeouts must exceed the server's `dial_timeout`. If the client
  never sends the handshake message, the server closes the tunnel
  after ~5 s with the same failure treatment.
- **Teardown semantics**: in `tunnel.Pump`, the first direction to
  finish tears the tunnel down (WS has no half-close). Keepalive pings
  (30 s) protect against idle reverse-proxy drops.

## Conventions

- Small, focused packages under `internal/`; the binary is a thin shell.
- Errors are wrapped with `fmt.Errorf("...: %w", err)`; package-specific
  prefixes like `config:` / `server:` are fine.
- CLI output goes to stderr; stdout is reserved for payload data
  (stdio mode tunnels stdout — never log to it). The one exception is
  `wsproxy version`, which prints to stdout so scripts can capture it.
- Server config is a single YAML file per instance; durations are
  strings parsed via `config.Duration` ("30s", "1m").

## Testing notes

- Integration-style tests use `httptest` + `coder/websocket` against
  plain `ws://` (client.Dial enforces `wss://`, so tests dial raw).
- Echo-server helpers live in `internal/server/server_test.go`.
- `websocket.Conn.Close` performs a close **handshake** (waits up to 5 s
  for the peer's close frame). In tests, one side must keep reading or
  participate in the handshake, or the teardown stalls the full 5 s.
- `net.Pipe` reads return `io.ErrClosedPipe`, not `io.EOF`, on peer
  close — handled in `tunnel.normalize`.

## Environment quirks (this sandbox)

- Temp/scratch files belong in `/tmp/opencode` (pre-approved).
- No `pkill`/`ps`; enumerate `/proc/*/cmdline` (or `/proc/*/comm`) and
  `kill` PIDs. Background a process with
  `(nohup CMD > log 2>&1 &)` — `A && B &` backgrounds the whole chain
  and races the next command.
- This environment sets `no_proxy=127.0.0.1,localhost,::1`; curl will
  **bypass** an explicitly passed `-x` proxy for those hosts unless you
  add `--noproxy ''`.
- `go build -o X /dev/stdin` and out-of-module paths fail; build from
  inside the module directory.
