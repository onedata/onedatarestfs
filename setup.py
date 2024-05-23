#!/usr/bin/env python
"""OnedataRESTFS is a PyFilesystem implementation for Onedata."""

from setuptools import setup

__version__ = '21.2.5.1'

CLASSIFIERS = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3.5",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Topic :: System :: Filesystems",
]

with open("README.md", "rt") as f:
    DESCRIPTION = f.read()

REQUIREMENTS = ["fs", "onedatafilerestclient"]

setup(
    name="fs.onedatarestfs",
    author="Bartek Kryza",
    author_email="bkryza@gmail.com",
    classifiers=CLASSIFIERS,
    description="Onedata REST-based filesystem for PyFilesystem",
    install_requires=REQUIREMENTS,
    license="MIT",
    long_description=DESCRIPTION,
    long_description_content_type='text/markdown',
    packages=["fs.onedatarestfs"],
    keywords=["pyfilesystem", "Onedata"],
    test_suite="nose.collector",
    url="https://github.com/onedata/onedatarestfs",
    version=__version__,
    entry_points={
        "fs.opener":
        ["onedatarestfs = fs.onedatarestfs.opener:OnedataRESTFSOpener"]
    },
)
