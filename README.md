# OnedataRESTFS

OnedataRESTFS is a [PyFilesystem](https://www.pyfilesystem.org/) interface to
[Onedata](https://onedata.org) virtual file system based on 
[Onedata REST API](https://onedata.org/#/home/api).

As a PyFilesystem concrete class, [OnedataRESTFS](https://github.com/onedata/onedatarestfs/)
allows you to work with Onedata in the same way as any other supported filesystem.

## Heavyweight alternative

To achieve the best performance, consider using a heavyweight cousin of
OnedataRESTFS - [OnedataFS](https://github.com/onedata/fs-onedatafs). It takes 
advantage of direct storage access, a.k.a.
[DirectIO](https://onedata.org/#/home/documentation/21.02/user-guide/oneclient[direct-io-and-proxy-io-modes].html).
However, it requires that the machine running OnedataFS has direct access to the
underlying storage system. If it's not the case, OnedataRESTFS is a good choice,
with identical functionality and comparable performance for ProxyIO data access mode.

## Installing

You can install OnedataRESTFS from pip as follows:

```
pip install fs.onedatarestfs
```

## Opening a OnedataRESTFS

Open an OnedataRESTFS by explicitly using the constructor:

```python
from fs.onedatarestfs import OnedataRESTFS
onedata_onezone_host = "..."
onedata_access_token = "..."
odfs = OnedataRESTFS(onedata_onezone_host, onedata_access_token)
```

Or with a FS URL:

```python
from fs import open_fs
odfs = open_fs('onedatarestfs://HOST?token=...')
```


## Building and running tests

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

- [PyFilesystem Wiki](https://www.pyfilesystem.org)
- [Onedata Homepage](https://onedata.org)
