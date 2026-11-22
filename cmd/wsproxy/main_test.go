package main

import (
	"strings"
	"testing"
)

func TestPrintVersion(t *testing.T) {
	var b strings.Builder
	printVersion(&b)
	if !strings.HasPrefix(b.String(), "wsproxy ") || !strings.HasSuffix(b.String(), "\n") {
		t.Fatalf("unexpected output %q", b.String())
	}
}
