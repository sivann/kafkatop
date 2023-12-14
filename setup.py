#!/usr/bin/env python
#
# License
#
# Copyright
#
# $Id: __init__.py 956 2009-08-20 10:26:55Z manousos $
#

import os
import sys
import re
from setuptools import setup

PKG_NAME = 'ifms_diagnostics'
PKG_VER = 0.1

#v = open(os.path.join(os.path.dirname(__file__), 'ifms', '__init__.py'))
#PKG_VER = re.compile(r".*__version__ = '(.*?)'", re.S).match(v.read()).group(1)
#v.close()

with open('requirements.txt') as f:
    install_requirements = f.read().splitlines()

setup(
	name = PKG_NAME,
	version = PKG_VER,
	description = "KAFKA Lag statistics",
	author = "Spiros Ioannou",
	python_requires = ">=3.10",
	license = "",
	zip_safe = False,
	install_requires = install_requirements,
	scripts= ['kafka-lagstats.py'],
	packages = []
	)

