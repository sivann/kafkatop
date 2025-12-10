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

# Colors for echos
ccend = $(shell tput sgr0)
ccbold = $(shell tput bold)
ccgreen = $(shell tput setaf 2)
ccso = $(shell tput smso)

#Do not echo commands
.SILENT:

.PHONY: all init venv_update clean pex pex-mp build publish go go-build go-install go-clean go-build-all go-build-linux go-build-darwin go-build-windows

all: go ## Build both Python and Go versions

# Go targets
go: go-build ## Build Go version (default)

go-build: ## Build Go binary with static linking for current platform
	@echo "$(ccso)--> Building Go version for $(GOOS)/$(GOARCH) $(ccend)"
	cd go && CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build -ldflags="-s -w" -o ../$(GOBIN) .
	@echo "$(ccgreen)Go binary built: ./$(GOBIN)$(ccend)"

go-build-all: go-build-linux go-build-darwin go-build-windows ## Build for all platforms (Linux, macOS, Windows)

go-build-linux: ## Build for Linux (amd64 and arm64)
	@echo "$(ccso)--> Building for Linux amd64 $(ccend)"
	cd go && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -ldflags="-s -w" -o ../kafkatop-linux-amd64 .
	@echo "$(ccgreen)Built: kafkatop-linux-amd64$(ccend)"
	@echo "$(ccso)--> Building for Linux arm64 $(ccend)"
	cd go && CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GO) build -ldflags="-s -w" -o ../kafkatop-linux-arm64 .
	@echo "$(ccgreen)Built: kafkatop-linux-arm64$(ccend)"

go-build-darwin: ## Build for macOS (amd64 and arm64)
	@echo "$(ccso)--> Building for macOS amd64 (Intel) $(ccend)"
	cd go && CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(GO) build -ldflags="-s -w" -o ../kafkatop-darwin-amd64 .
	@echo "$(ccgreen)Built: kafkatop-darwin-amd64$(ccend)"
	@echo "$(ccso)--> Building for macOS arm64 (Apple Silicon) $(ccend)"
	cd go && CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 $(GO) build -ldflags="-s -w" -o ../kafkatop-darwin-arm64 .
	@echo "$(ccgreen)Built: kafkatop-darwin-arm64$(ccend)"

go-build-windows: ## Build for Windows (amd64)
	@echo "$(ccso)--> Building for Windows amd64 $(ccend)"
	cd go && CGO_ENABLED=0 GOOS=windows GOARCH=amd64 $(GO) build -ldflags="-s -w" -o ../kafkatop-windows-amd64.exe .
	@echo "$(ccgreen)Built: kafkatop-windows-amd64.exe$(ccend)"

go-install: ## Install Go dependencies
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

clean: ## >> remove all environment and build files
	@echo ""
	@echo "$(ccso)--> Removing virtual environment $(ccend)"
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
publish: $(VENV_DIR) build
	cd python && $(VENV_DIR)/bin/twine upload dist/*

