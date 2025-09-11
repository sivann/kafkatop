# Sivann

#https://www.gnu.org/software/make/manual/html_node/One-Shell.html

.ONESHELL:
SHELL:=/bin/bash
VENV_DIR = venv
#PYTHON = python3
#PYTHON := $(shell /bin/which python3.10)
PYTHON := $(shell /usr/bin/which python3)
PIP = $(VENV_DIR)/bin/pip



# Colors for echos 
ccend = $(shell tput sgr0)
ccbold = $(shell tput bold)
ccgreen = $(shell tput setaf 2)
ccso = $(shell tput smso)

#Do not echo commands
.SILENT:

.PHONY: init venv_update clean pex pex-mp build publish

init: $(VENV_DIR)
	echo "$(VENV_DIR) exists, type first 'make clean' to start again if needed"
	#sed -i 

$(VENV_DIR):
	echo "Creating $(VENV_DIR)"
	$(PYTHON) -m venv $(VENV_DIR) || echo "Failed creating venv" 
	$(VENV_DIR)/bin/pip install --upgrade pip
	$(VENV_DIR)/bin/pip install .[dev]

venv_update: $(VENV_DIR)
	$(VENV_DIR)/bin/pip install .[dev]

clean: ## >> remove all environment and build files
	@echo ""
	@echo "$(ccso)--> Removing virtual environment $(ccend)"
	rm -rf $(VENV_DIR) makepex.* wh/ venv-*/ platforms.json kafkatop kafkatop.egg-info/ build/ dist/ *.pex  releasebody.md platforms.tmp

pex: $(VENV_DIR)
	echo "PYTHON is set to $(PYTHON)" #errors if not set above
	$(VENV_DIR)/bin/pip install pex
	$(VENV_DIR)/bin/pex . --disable-cache -o kafkatop -e kafkatop:main --python-shebang $(PYTHON)

#pex-multiplatform
pex-mp:
	./make-pex-mp.sh

build: $(VENV_DIR)
	$(VENV_DIR)/bin/pip install build
	$(VENV_DIR)/bin/python -m build

publish: $(VENV_DIR) build
	$(VENV_DIR)/bin/twine upload dist/*
