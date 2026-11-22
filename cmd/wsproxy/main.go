// Command wsproxy is a minimal WebSocket TCP tunnel, designed to run
// behind a reverse proxy that terminates TLS. The server listens on a
// local TCP address (loopback by default); one WebSocket connection
// carries one tunneled TCP connection.
//
// Usage:
//
//	wsproxy server --config server.yaml
//	wsproxy client --url wss://host/ws --key KEY --mode http --listen 127.0.0.1:3128
//	wsproxy client --url wss://host/ws --key KEY --mode ssh --host 10.0.0.5:22
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.beppler.dev.br/wsproxy/internal/client"
	"go.beppler.dev.br/wsproxy/internal/config"
	"go.beppler.dev.br/wsproxy/internal/dest"
	"go.beppler.dev.br/wsproxy/internal/server"
)

const defaultSSHPort = 22

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "server":
		os.Exit(runServer(os.Args[2:]))
	case "client":
		os.Exit(runClient(os.Args[2:]))
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "wsproxy: unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `wsproxy - minimal WebSocket TCP tunnel

Usage:
  wsproxy server --config <server.yaml>
  wsproxy client --url <wss://host/ws> --key <key> --mode http --listen <addr>
  wsproxy client --url <wss://host/ws> --key <key> --mode ssh --host <host:port>

The server listens on a local TCP address (loopback by default); the
client only speaks wss://. TLS termination is the job of the reverse
proxy in front of the server, which connects to the server over TCP.
`)
}

// runServer runs `wsproxy server`.
func runServer(args []string) int {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	configPath := fs.String("config", "", "path to YAML config file (required)")
	_ = fs.Parse(args)

	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "wsproxy server: --config is required")
		return 2
	}
	cfg, err := config.LoadServer(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "wsproxy server: %v\n", err)
		return 1
	}

	srv, err := server.New(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "wsproxy server: %v\n", err)
		return 1
	}

	httpSrv := &http.Server{
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		fmt.Fprintf(os.Stderr, "wsproxy server: %v\n", err)
		return 1
	}
	defer ln.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}()

	fmt.Fprintf(os.Stderr, "wsproxy server: listening on %s\n", cfg.Listen)
	if err := httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintf(os.Stderr, "wsproxy server: %v\n", err)
		return 1
	}
	return 0
}

// runClient runs `wsproxy client`.
func runClient(args []string) int {
	fs := flag.NewFlagSet("client", flag.ExitOnError)
	urlFlag := fs.String("url", "", "server endpoint, wss:// only (required)")
	keyFlag := fs.String("key", "", "security key (falls back to $WSPROXY_KEY)")
	mode := fs.String("mode", "http", "client mode: http or ssh")
	listen := fs.String("listen", "127.0.0.1:3128", "http mode: local proxy listen address")
	hostFlag := fs.String("host", "", "ssh mode: destination host:port behind the server")
	timeout := fs.Duration("connect-timeout", 30*time.Second, "tunnel handshake timeout (must exceed the server's dial_timeout)")
	_ = fs.Parse(args)

	if *urlFlag == "" {
		fmt.Fprintln(os.Stderr, "wsproxy client: --url is required")
		return 2
	}
	key := *keyFlag
	if key == "" {
		key = os.Getenv("WSPROXY_KEY")
	}
	if key == "" {
		fmt.Fprintln(os.Stderr, "wsproxy client: --key (or $WSPROXY_KEY) is required")
		return 2
	}

	opts := client.Options{
		URL:              *urlFlag,
		Key:              key,
		HandshakeTimeout: *timeout,
	}

	switch *mode {
	case "http":
		proxy := &client.HTTPProxy{
			URL:              opts.URL,
			Key:              key,
			HandshakeTimeout: *timeout,
		}
		srv := &http.Server{
			Addr:              *listen,
			Handler:           proxy,
			ReadHeaderTimeout: 30 * time.Second,
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdownCtx)
		}()
		fmt.Fprintf(os.Stderr, "wsproxy client: HTTP proxy listening on %s\n", *listen)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(os.Stderr, "wsproxy client: %v\n", err)
			return 1
		}
		return 0

	case "ssh":
		host, port, err := dest.ParseAddress(*hostFlag, defaultSSHPort)
		if err != nil {
			fmt.Fprintf(os.Stderr, "wsproxy client: --host: %v\n", err)
			return 2
		}
		opts.Host, opts.Port = host, port
		if err := client.RunStdio(context.Background(), opts); err != nil {
			fmt.Fprintf(os.Stderr, "wsproxy client: %v\n", err)
			return 1
		}
		return 0

	default:
		fmt.Fprintf(os.Stderr, "wsproxy client: unknown --mode %q (want http or ssh)\n", *mode)
		return 2
	}
}
