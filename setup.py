from setuptools import setup
import os

# Read version from tag.txt
def get_version():
    if os.path.exists('tag.txt'):
        with open('tag.txt') as f:
            return f.read().strip()
    return "0.0.0"

setup(
    version=get_version(),
)