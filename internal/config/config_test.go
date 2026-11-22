package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "server.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadServerDefaults(t *testing.T) {
	path := writeTemp(t, "key: hunter2\n")
	cfg, err := LoadServer(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Key != "hunter2" {
		t.Fatalf("key = %q", cfg.Key)
	}
	if cfg.Listen != "localhost:2345" {
		t.Fatalf("listen = %q", cfg.Listen)
	}
	if time.Duration(cfg.DialTimeout) != 10*time.Second {
		t.Fatalf("dial_timeout = %v", cfg.DialTimeout)
	}
	if cfg.Allow != nil {
		t.Fatalf("allow = %v, want nil (unrestricted)", cfg.Allow)
	}
}

func TestLoadServerFull(t *testing.T) {
	path := writeTemp(t, `
listen: localhost:23450
key: ${WSPROXY_TEST_KEY}
allow:
  - "*.internal.example:22"
  - "db.local:*"
dial_timeout: 3s
idle_timeout: 1m
max_concurrent: 5
`)
	t.Setenv("WSPROXY_TEST_KEY", "s3cret")
	cfg, err := LoadServer(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Listen != "localhost:23450" || cfg.Key != "s3cret" || cfg.MaxConcurrent != 5 {
		t.Fatalf("cfg = %+v", cfg)
	}
	if len(cfg.Allow) != 2 || cfg.Allow[0] != "*.internal.example:22" {
		t.Fatalf("allow = %v", cfg.Allow)
	}
	if time.Duration(cfg.DialTimeout) != 3*time.Second || time.Duration(cfg.IdleTimeout) != time.Minute {
		t.Fatalf("timeouts = %v %v", cfg.DialTimeout, cfg.IdleTimeout)
	}
}

func TestLoadServerErrors(t *testing.T) {
	cases := []struct {
		name    string
		content string
		wantErr string
	}{
		{"missing key", "listen: localhost:2345\n", "key is required"},
		{"missing port", "key: k\nlisten: localhost\n", "not a host:port address"},
		{"port too big", "key: k\nlisten: localhost:99999\n", "invalid port"},
		{"port not a number", "key: k\nlisten: localhost:notaport\n", "invalid port"},
		{"bad duration", "key: k\ndial_timeout: banana\n", "invalid duration"},
		{"negative limit", "key: k\nmax_concurrent: -1\n", "non-negative"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadServer(writeTemp(t, tc.content))
			if err == nil || !contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func contains(s, sub string) bool {
	return strings.Contains(s, sub)
}
