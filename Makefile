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

.PHONY: all init venv_update clean pex pex-mp build publish go go-build go-install go-clean

all: go ## Build both Python and Go versions

# Go targets
go: go-build ## Build Go version (default)

go-build: ## Build Go binary with static linking
	@echo "$(ccso)--> Building Go version $(ccend)"
	cd go && CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build -ldflags="-s -w" -o ../$(GOBIN) .
	@echo "$(ccgreen)Go binary built: ./$(GOBIN)$(ccend)"

go-install: ## Install Go dependencies
	@echo "$(ccso)--> Installing Go dependencies $(ccend)"
	cd go && $(GO) mod tidy
	cd go && $(GO) mod download

go-clean: ## Clean Go build artifacts
	@echo "$(ccso)--> Cleaning Go build artifacts $(ccend)"
	rm -f $(GOBIN)
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

