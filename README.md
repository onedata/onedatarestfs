# OnedataRESTFS

`OnedataRESTFS` is a pure Python library, which exposes [Onedata REST API](https://onedata.org/#/home/api) through [PyFilesystem2](https://www.pyfilesystem.org/) interface, requiring only a minimal set of dependencies.

As a `PyFilesystem2` implementation, `OnedataRESTFS`
allows you to work with [Onedata](https://onedata.org/#/home/api) virtual filesystem in
the same way as any other supported filesystem.

Supported Onezone versions: `>= 21.02.5`.

Supported Oneprovider versions: `>= 21.02.5`.

## High-performance data access alternative

Since `OnedataRESTFS` performs all filesystem operations using `Onedata REST API`, all
requests incur latency overhead inherent to HTTPS. For use cases which require minimum
latency (e.g. frequent random access read/write operations), an alternative Python client
based on Onedata native binary communication protocol is available -
[OnedataFS](https://github.com/onedata/fs-onedatafs). This Python library however is just a
wrapper over low-level functionality implemented in C++ and requires several dependencies
to be installed.
The main benefit, is that it allows to leverage direct storage access, a.k.a.
[DirectIO](https://onedata.org/#/home/documentation/21.02/user-guide/oneclient[direct-io-and-proxy-io-modes].html),
if the client machine has network access to the storage.

However if performance is not critical, it's better to use `OnedataRESTFS` which is much
easier to install, use and integrate in third-party applications.

## Installation

You can install `OnedataRESTFS` from `PyPI` as follows:

```bash
pip install fs.onedatarestfs
```

> Make sure to always install a version not newer than that of the Onedata Onezone service in your deployment.

## Usage

### Creating `OnedataRESTFS` client instance

In order to use `OnedataRESTFS` it is necessary to first create an instance of
`fs.onedatarestfs.OnedataRESTFS`, with at least 2 required parameters:

* `Onedata Onezone host` - hostname of Onezone instance to which `OnedataRESTFS` should connect
* `Onedata access token` - Onedata access token with Oneprovider REST API privileges (see [Access tokens](https://onedata.org/#/home/documentation/21.02/user-guide/tokens[gui-guide].html))

```python
from fs.onedatarestfs import OnedataRESTFS
onedata_onezone_host = "..."
onedata_access_token = "..."
odfs = OnedataRESTFS(onedata_onezone_host, onedata_access_token)
```

Other optional `OnedataRESTFS` constructor positional arguments include:

* `space` - when specified, the `OnedataRESTFS` client will be limited to a single space
* `preferred_oneproviders` - by default `OnedataRESTFS` will select a Oneprovider for each space automatically, however a list of preferred Oneprovider hostnames can be provided here, and will be used to prioritize the selection based on which Oneproviders support which data space
* `verify_ssl` - `True` by default, used only for development environments
* `timeout` - request timeout, 30 seconds by default

### Example operations

The following examples assume that `odfs` variable points to an instance of
`OnedataRESTFS` class.

#### List spaces

```python
>>> odfs.listdir('/')
['Space1', 'Space2']
```

#### Read and write files

```python
>>> odfs.writetext('/Space1/file.txt', 'TEST')
>>> odfs.readtext('/Space1/file.txt')
'TEST'
```

#### Rename file

```python
>>> odfs.move('/Space1/file.txt', '/Space1/file2.txt')
>>> odfs.listdir('/Space1')
['file2.txt']
```

For more information on how to use `OnedataRESTFS` instance see [PyFilesystem2 Docs](https://pyfilesystem2.readthedocs.io/en/latest/).

## Development

### Building and running tests

```bash
virtualenv -p /usr/bin/python3 venv
. venv/bin/activate

# Install tox
pip install coverage tox

# Run flake8 check
tox -c tox.ini -e flake8

# Run mypy typing check
tox -c tox.ini -e mypy

# Run PyFilesystem test suite
tox -c tox.ini -e fstest
```

## Documentation

- [PyFilesystem2](https://github.com/PyFilesystem/pyfilesystem2)
- [PyFilesystem2 Docs](https://pyfilesystem2.readthedocs.io/en/latest/)
- [Onedata Homepage](https://onedata.org)
- [Onedata REST API](https://onedata.org/#/home/api)
- [Access tokens](https://onedata.org/#/home/documentation/21.02/user-guide/tokens[gui-guide].html)
