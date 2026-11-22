package client

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/coder/websocket"

	"go.beppler.dev.br/wsproxy/internal/tunnel"
)

// stdioTarget bridges a process's stdin/stdout to the tunnel for ssh
// ProxyCommand usage.
type stdioTarget struct {
	in  io.Reader
	out io.Writer
}

func (s *stdioTarget) Read(p []byte) (int, error)  { return s.in.Read(p) }
func (s *stdioTarget) Write(p []byte) (int, error) { return s.out.Write(p) }
func (s *stdioTarget) Close() error                { return nil }

// RunStdio connects to the server and bridges stdin/stdout, for use as
// an ssh ProxyCommand:
//
//	ssh -o ProxyCommand='wsproxy client --mode ssh ...' user@host
func RunStdio(ctx context.Context, o Options) error {
	conn, err := Dial(ctx, o)
	if err != nil {
		return err
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	sctx, scancel := context.WithCancel(ctx)
	defer scancel()

	StartKeepalive(sctx, conn, KeepaliveInterval, scancel)

	err = tunnel.Pump(sctx, conn, &stdioTarget{in: os.Stdin, out: os.Stdout})
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
