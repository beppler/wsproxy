# Cross-compiles the wsproxy binary for every supported platform.
# Pure Go cross-compilation (CGO disabled) — no platform toolchains needed.
# Output lands in dist/, gitignored.
#
# Requires GNU make. Recipes avoid shell-specific syntax so this works the
# same from sh (Linux/macOS/Git Bash) and cmd.exe on Windows.
#
#   make                               # build every platform
#   make dist/wsproxy-linux-amd64  # build a single platform
#   make clean                         # remove dist/

BIN       := wsproxy
PKG       := ./cmd/wsproxy
DIST      := dist
PLATFORMS := linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64

export CGO_ENABLED := 0

.DEFAULT_GOAL := cross-build

# $(call platform-rule,os,arch): one binary target with its own GOOS/GOARCH.
define platform-rule
$(DIST)/$(BIN)-$(1)-$(2)$(if $(filter windows,$(1)),.exe): export GOOS := $(1)
$(DIST)/$(BIN)-$(1)-$(2)$(if $(filter windows,$(1)),.exe): export GOARCH := $(2)
TARGETS += $(DIST)/$(BIN)-$(1)-$(2)$(if $(filter windows,$(1)),.exe)
endef

$(foreach p,$(PLATFORMS),$(eval $(call platform-rule,$(word 1,$(subst /, ,$(p))),$(word 2,$(subst /, ,$(p))))))

# Always invoke go build; Go's build cache makes no-op rebuilds cheap and
# tracks source dependencies better than make could.
.PHONY: cross-build clean $(TARGETS)

cross-build: $(TARGETS)

# go build -o creates $(DIST) itself, so no mkdir is needed.
$(TARGETS):
	go build -o $@ $(PKG)

clean:
ifeq ($(OS),Windows_NT)
	if exist $(DIST) rmdir /s /q $(DIST)
else
	rm -rf $(DIST)
endif
