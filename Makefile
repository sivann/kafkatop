# Sivann

#https://www.gnu.org/software/make/manual/html_node/One-Shell.html

.ONESHELL:
SHELL:=/bin/bash

# Python settings
VENV_DIR = python/venv
PYTHON := $(shell /usr/bin/which python3)
PIP = $(VENV_DIR)/bin/pip

# Go settings
GO := $(shell which go)
GOBIN := kafkatop
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

# Go source files
GO_SOURCES := $(shell find go -name '*.go' -type f)
GO_MOD := go/go.mod go/go.sum

# Colors for echos (check if tput is available and TERM is set)
ccend = $(shell if [ -n "$$TERM" ] && command -v tput >/dev/null 2>&1; then tput sgr0; fi)
ccbold = $(shell if [ -n "$$TERM" ] && command -v tput >/dev/null 2>&1; then tput bold; fi)
ccgreen = $(shell if [ -n "$$TERM" ] && command -v tput >/dev/null 2>&1; then tput setaf 2; fi)
ccso = $(shell if [ -n "$$TERM" ] && command -v tput >/dev/null 2>&1; then tput smso; fi)

#Do not echo commands
.SILENT:

.PHONY: all init venv_update clean pex pex-mp build pypi-publish go go-deps go-clean go-build-all

all: go 

# Go targets
go: go-build ## Build Go version (default)

go-build: $(GOBIN) ## Build Go binary with static linking for current platform

$(GOBIN): $(GO_SOURCES) $(GO_MOD)
	@echo "$(ccso)--> Building Go version for $(GOOS)/$(GOARCH) $(ccend)"
	cd go && CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build -ldflags="-s -w" -o ../$(GOBIN) .
	@echo "$(ccgreen)Go binary built: ./$(GOBIN)$(ccend)"

go-build-all: go-build-linux go-build-darwin go-build-windows ## Build for all platforms (Linux, macOS, Windows)

go-build-linux: kafkatop-linux-amd64 kafkatop-linux-arm64 ## Build for Linux (amd64 and arm64)

kafkatop-linux-amd64: $(GO_SOURCES) $(GO_MOD)
	@echo "$(ccso)--> Building for Linux amd64 $(ccend)"
	cd go && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -ldflags="-s -w" -o ../kafkatop-linux-amd64 .
	@echo "$(ccgreen)Built: kafkatop-linux-amd64$(ccend)"

kafkatop-linux-arm64: $(GO_SOURCES) $(GO_MOD)
	@echo "$(ccso)--> Building for Linux arm64 $(ccend)"
	cd go && CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GO) build -ldflags="-s -w" -o ../kafkatop-linux-arm64 .
	@echo "$(ccgreen)Built: kafkatop-linux-arm64$(ccend)"

go-build-darwin: kafkatop-darwin-amd64 kafkatop-darwin-arm64 ## Build for macOS (amd64 and arm64)

kafkatop-darwin-amd64: $(GO_SOURCES) $(GO_MOD)
	@echo "$(ccso)--> Building for macOS amd64 (Intel) $(ccend)"
	cd go && CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(GO) build -ldflags="-s -w" -o ../kafkatop-darwin-amd64 .
	@echo "$(ccgreen)Built: kafkatop-darwin-amd64$(ccend)"

kafkatop-darwin-arm64: $(GO_SOURCES) $(GO_MOD)
	@echo "$(ccso)--> Building for macOS arm64 (Apple Silicon) $(ccend)"
	cd go && CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 $(GO) build -ldflags="-s -w" -o ../kafkatop-darwin-arm64 .
	@echo "$(ccgreen)Built: kafkatop-darwin-arm64$(ccend)"

go-build-windows: kafkatop-windows-amd64.exe ## Build for Windows (amd64)

kafkatop-windows-amd64.exe: $(GO_SOURCES) $(GO_MOD)
	@echo "$(ccso)--> Building for Windows amd64 $(ccend)"
	cd go && CGO_ENABLED=0 GOOS=windows GOARCH=amd64 $(GO) build -ldflags="-s -w" -o ../kafkatop-windows-amd64.exe .
	@echo "$(ccgreen)Built: kafkatop-windows-amd64.exe$(ccend)"

go-deps: ## Install Go dependencies
	@echo "$(ccso)--> Installing Go dependencies $(ccend)"
	cd go && $(GO) mod tidy
	cd go && $(GO) mod download

go-clean: ## Clean Go build artifacts
	@echo "$(ccso)--> Cleaning Go build artifacts $(ccend)"
	rm -f $(GOBIN) kafkatop-* *.exe
	cd go && $(GO) clean

go-test: ## Run Go tests
	cd go && $(GO) test -v ./...

# Python targets
init: $(VENV_DIR)
	echo "$(VENV_DIR) exists, type first 'make clean' to start again if needed"

$(VENV_DIR):
	echo "Creating $(VENV_DIR)"
	cd python && $(PYTHON) -m venv venv || echo "Failed creating venv"
	$(VENV_DIR)/bin/pip install --upgrade pip
	cd python && $(VENV_DIR)/bin/pip install .[dev]

venv_update: $(VENV_DIR)
	cd python && $(VENV_DIR)/bin/pip install .[dev]

clean: go-clean

python-clean: ## >> remove all environment and build files
	@echo ""
	@echo "$(ccso)--> Removing python virtual environment $(ccend)"
	rm -rf $(VENV_DIR) python/makepex.* python/wh/ python/venv-*/ python/platforms.json kafkatop python/kafkatop.egg-info/ python/build/ python/dist/ python/*.pex python/releasebody.md python/platforms.tmp python/__pycache__
	$(MAKE) go-clean

pex: $(VENV_DIR)
	echo "PYTHON is set to $(PYTHON)" #errors if not set above
	$(VENV_DIR)/bin/pip install pex
	cd python && $(VENV_DIR)/bin/pex . --disable-cache -o ../kafkatop -e kafkatop:main --python-shebang $(PYTHON)

#pex-multiplatform
pex-mp:
	cd python && ./make-pex-mp.sh

# Build for pypi repo (after tagging)
build: $(VENV_DIR)
	$(VENV_DIR)/bin/pip install build
	cd python && $(VENV_DIR)/bin/python -m build

# Publish for pypi repo (after tagging)
pypi-publish: $(VENV_DIR) build
	cd python && $(VENV_DIR)/bin/twine upload dist/*

