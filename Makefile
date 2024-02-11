# Sivann

#https://www.gnu.org/software/make/manual/html_node/One-Shell.html

.ONESHELL:
SHELL:=/bin/bash
VENV_DIR = venv
#PYTHON = python3
#PYTHON := $(shell /bin/which python3.10)
PYTHON := $(shell /bin/which python3)
PIP = $(VENV_DIR)/bin/pip



# Colors for echos 
ccend = $(shell tput sgr0)
ccbold = $(shell tput bold)
ccgreen = $(shell tput setaf 2)
ccso = $(shell tput smso)

#Do not echo commands
.SILENT:

init: $(VENV_DIR)
	echo "$(VENV_DIR) exists, type first 'make clean' to start again if needed"
	#sed -i 

$(VENV_DIR):
	echo "Creating $(VENV_DIR)"
	$(PYTHON) -m venv $(VENV_DIR) || echo "Failed creating venv" 
	source $(VENV_DIR)/bin/activate || /bin/echo "Failed activating" 
	echo "venv: VIRTUAL_ENV is set to $${VIRTUAL_ENV:?}" #errors if not set above
	err=0
	echo "Upgrading pip"
	pip install --upgrade pip || err=1
	echo "donepip"
	pip install wheel || err=1
	pip install pytest || err=1
	pip install coverage || err=1
	pip install -r requirements.txt || err=1
	@[[ "$${err}" == "0" ]] || (echo "ERRORS above"; exit 1)

venv_update: $(VENV_DIR)
	source $(VENV_DIR)/bin/activate || /bin/echo "Failed activating" 
	echo "venv_update: VIRTUAL_ENV is set to $${VIRTUAL_ENV:?}" #errors if not set above
	pip install -r requirements.txt

clean: ## >> remove all environment and build files
	@echo ""
	@echo "$(ccso)--> Removing virtual environment $(ccend)"
	rm -rf $(VENV_DIR) makepex.* wh/ venv-*/

pex:
	source $(VENV_DIR)/bin/activate || /bin/echo "Failed activating" 
	echo "pex: VIRTUAL_ENV is set to $${VIRTUAL_ENV:?}" #errors if not set above
	echo "PYTHON is set to $(PYTHON)" #errors if not set above
	pip install pex
	sed -i "s/%version%/$(cat tag.txt)/" kafkatop.py
	pex . --disable-cache -o kafkatop -c kafkatop.py --python-shebang $(PYTHON)

#pex-multiplatform
pex-mp:
	./pex-mp.sh
