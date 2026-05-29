#!/usr/bin/env python
"""OnedataRESTFS is a PyFilesystem implementation for Onedata."""

import sys

import setuptools
from setuptools import setup

_setuptools_ver = tuple(int(x) for x in setuptools.__version__.split(".")[:2])
if _setuptools_ver >= (81, 0):
    sys.exit(
        f"ERROR: setuptools {setuptools.__version__} is not supported. "
        "The pyfilesystem2 dependency requires setuptools<81.0.0. "
        "Please downgrade first: pip install 'setuptools<81.0.0'"
    )

__version__ = "25.1.0"

CLASSIFIERS = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Programming Language :: Python :: 3.14",
    "Topic :: System :: Filesystems",
]

with open("README.md", "rt", encoding="utf-8") as f:
    DESCRIPTION = f.read()

REQUIREMENTS = ["fs", "onedatafilerestclient>=25.0.0"]

setup(
    name="fs.onedatarestfs",
    author="Bartek Kryza",
    author_email="bkryza@gmail.com",
    classifiers=CLASSIFIERS,
    description="Onedata REST-based filesystem for PyFilesystem",
    python_requires=">=3.10",
    install_requires=REQUIREMENTS,
    license="MIT",
    long_description=DESCRIPTION,
    long_description_content_type="text/markdown",
    packages=["fs.onedatarestfs"],
    keywords=["pyfilesystem", "Onedata"],
    url="https://github.com/onedata/onedatarestfs",
    version=__version__,
    entry_points={
        "fs.opener": ["onedatarestfs = fs.onedatarestfs.opener:OnedataRESTFSOpener"]
    },
)
