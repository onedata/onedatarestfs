# OnedataRESTFS

`OnedataRESTFS` is a pure Python library that exposes the Onedata [File access and management REST API](https://onedata.org/#/home/api/stable/oneprovider?anchor=group/File-access-and-management) through the [PyFilesystem2](https://www.pyfilesystem.org/) interface, requiring only a minimal set of dependencies.

As a `PyFilesystem2` implementation, `OnedataRESTFS` allows you to work with the [Onedata](https://onedata.org) virtual filesystem in the same way as with any other supported filesystem.

Supported Onezone versions: `>= 21.02.5`.

Supported Oneprovider versions: `>= 21.02.5`.

## High-performance data access alternative

Since `OnedataRESTFS` performs all filesystem operations using the `Onedata REST API`, all requests incur latency overhead inherent to HTTPS. For use cases requiring minimal latency (e.g., frequent random access read/write operations), an alternative Python client based on Onedata's native binary communication protocol is available: [OnedataFS](https://github.com/onedata/fs-onedatafs). However, this library is just a wrapper over low-level functionality implemented in C++ and requires several dependencies to be installed.

The main benefit is that it allows leveraging direct storage access, a.k.a. [DirectIO](https://onedata.org/#/home/documentation/21.02/user-guide/oneclient[direct-io-and-proxy-io-modes].html), if the client machine has network access to the storage.

However, if performance is not critical, it's better to use `OnedataRESTFS`, which is much easier to install, use, and integrate into third-party applications.

## Installation

You can install `OnedataRESTFS` from PyPI as follows:

```bash
pip install fs.onedatarestfs
```

> Make sure to install a version **not newer** than the Onedata Onezone service in your deployment. New versions of this library are published only when some changes are made or a new major Onedata release is published, so there might not be an exact version matching current Onedata release.

## Usage

### Creating a `OnedataRESTFS` client instance

To use `OnedataRESTFS`, you must first create an instance of `fs.onedatarestfs.OnedataRESTFS`, providing at least two required parameters:

- `onedata_onezone_host` – Hostname of the Onezone instance to connect to
- `onedata_access_token` – Onedata access token with Oneprovider REST API privileges (see [Access tokens](https://onedata.org/#/home/documentation/21.02/user-guide/tokens[gui-guide].html))

```python
from fs.onedatarestfs import OnedataRESTFS
onedata_onezone_host = "..."
onedata_access_token = "..."
odfs = OnedataRESTFS(onedata_onezone_host, onedata_access_token)
```

Other optional `OnedataRESTFS` constructor arguments include:

- `space` – If specified, the client will be limited to a single space
- `preferred_oneproviders` – By default, `OnedataRESTFS` will select a Oneprovider for each space automatically. However, a list of preferred Oneprovider hostnames can be provided to prioritize selection based on which Oneproviders support the given space.
- `verify_ssl` – `True` by default; can be disabled in development environments
- `timeout` – Request timeout in seconds (default is 30)

### Example operations

The following examples assume that the `odfs` variable refers to an instance of the `OnedataRESTFS` class.

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

#### Rename a file

```python
>>> odfs.move('/Space1/file.txt', '/Space1/file2.txt')
>>> odfs.listdir('/Space1')
['file2.txt']
```

For more information on how to use an `OnedataRESTFS` instance, see the [PyFilesystem2 Docs](https://pyfilesystem2.readthedocs.io/en/latest/).

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

## References

- [PyFilesystem2](https://github.com/PyFilesystem/pyfilesystem2)
- [PyFilesystem2 Docs](https://pyfilesystem2.readthedocs.io/en/latest/)
- [Onedata Homepage](https://onedata.org)
- [File access and management REST API](https://onedata.org/#/home/api/stable/oneprovider?anchor=group/File-access-and-management)
- [Onedata access tokens](https://onedata.org/#/home/documentation/21.02/user-guide/tokens[gui-guide].html)