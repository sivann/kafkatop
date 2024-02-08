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

PKG_NAME = 'kafka-lagstats'
PKG_VER = 0.1

with open('requirements.txt') as f:
    install_requirements = f.read().splitlines()

setup(
	name = PKG_NAME,
	version = PKG_VER,
	description = "KAFKA Lag statistics",
	author = "Spiros Ioannou",
	python_requires = ">=3.9",
	license = "",
	zip_safe = False,
	install_requires = install_requirements,
	scripts= ['kafka-lagstats.py'],
	packages = []
	)

