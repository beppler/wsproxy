// Package config loads the wsproxy server's single YAML
// configuration.
package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that unmarshals from a YAML string such
// as "30s" or "1m".
type Duration time.Duration

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err != nil {
		return fmt.Errorf("duration must be a string like \"30s\": %w", err)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// ServerConfig is the single configuration of a wsproxy server
// instance.
type ServerConfig struct {
	// Listen is the TCP address (host:port) the server listens on.
	// It defaults to localhost:2345.
	Listen string `yaml:"listen"`
	// Key is the shared secret clients must present.
	Key string `yaml:"key"`
	// Allow restricts destinations to glob patterns such as
	// "*.internal.example:22" or "db.local:*" (matched against
	// "host:port"). When not set, every destination is allowed. An
	// explicitly empty list allows nothing.
	Allow []string `yaml:"allow"`
	// DialTimeout bounds connecting to the destination (default 10s).
	DialTimeout Duration `yaml:"dial_timeout"`
	// IdleTimeout closes tunnels with no traffic for this long
	// (default: disabled).
	IdleTimeout Duration `yaml:"idle_timeout"`
	// MaxConcurrent limits concurrent tunnels (default: unlimited).
	MaxConcurrent int `yaml:"max_concurrent"`
}

// LoadServer reads and validates a server config file. ${VAR}
// references in the file are expanded from the environment.
func LoadServer(path string) (ServerConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ServerConfig{}, fmt.Errorf("read config: %w", err)
	}
	var cfg ServerConfig
	if err := yaml.Unmarshal([]byte(os.ExpandEnv(string(raw))), &cfg); err != nil {
		return ServerConfig{}, fmt.Errorf("parse config: %w", err)
	}
	applyServerDefaults(&cfg)
	if err := validateServer(&cfg); err != nil {
		return ServerConfig{}, err
	}
	return cfg, nil
}

func applyServerDefaults(cfg *ServerConfig) {
	if cfg.Listen == "" {
		cfg.Listen = "localhost:2345"
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = Duration(10 * time.Second)
	}
}

func validateServer(cfg *ServerConfig) error {
	if cfg.Key == "" {
		return errors.New("config: key is required")
	}
	if _, err := validateListenAddr(cfg.Listen); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	if cfg.DialTimeout < 0 || cfg.IdleTimeout < 0 {
		return errors.New("config: timeouts must be non-negative")
	}
	if cfg.MaxConcurrent < 0 {
		return errors.New("config: max_concurrent must be non-negative")
	}
	return nil
}

// validateListenAddr checks that a listen address is a usable TCP
// host:port address: non-empty, free of NUL bytes, with a numeric port.
// The host is accepted as-is (an empty host means all interfaces);
// unresolvable hosts fail at startup when the listener is created.
func validateListenAddr(listen string) (string, error) {
	if listen == "" {
		return "", errors.New("listen address is empty")
	}
	if strings.IndexByte(listen, 0) >= 0 {
		return "", fmt.Errorf("listen address %q contains a NUL byte", listen)
	}
	_, portStr, err := net.SplitHostPort(listen)
	if err != nil {
		return "", fmt.Errorf("listen address %q is not a host:port address", listen)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return "", fmt.Errorf("listen address %q has an invalid port %q", listen, portStr)
	}
	return listen, nil
}
