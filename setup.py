from setuptools import setup

with open('tag.txt') as f:
    version = f.read().strip()

setup(
    version=version,
)