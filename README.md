# OnedataRESTFS

OnedataRESTFS is a [PyFilesystem](https://www.pyfilesystem.org/) interface to
[Onedata](https://onedata.org) virtual file system based on [Onedata REST API].

As a PyFilesystem concrete class, [OnedataFS](https://github.com/onedata/onedatarestfs/)
allows you to work with Onedata in the same way as any other supported filesystem.

## Installing

You can install OnedataRESTFS from pip as follows:

```
pip install onedatarestfs
```

## Opening a OnedataRESTFS

Open an OnedataFS by explicitly using the constructor:

```python
from fs.onedatarestfs import OnedataRESTFS
onedata_onezone_host = "..."
onedata_access_token = "..."
odfs = OnedataFS(onedata_onezone_host, onedata_access_token)
```

Or with a FS URL:

```python
  from fs import open_fs
  odfs = open_fs('onedatarestfs://HOST?token=...')
```

## Extended attributes

OnedataRESTFS supports in addition to standard PyFilesystem API operations
on metadata via POSIX compatible extended attributes API.


## Building and running tests

```bash
virtualenv -p /usr/bin/python3 venv
. venv/bin/activate

# Install tox
pip install coverage tox

# Run flake8 check
tox -c tox.ini -e flake8

# Run mypy typing check
tox -c tox.ini -e fstest

# Run PyFilesystem test suite
tox -c tox.ini -e fstest
```

## Documentation

- [PyFilesystem Wiki](https://www.pyfilesystem.org)
- [OnedataRESTFS Reference](http://onedatarestfs.readthedocs.io/en/latest/)
- [Onedata Homepage](https://onedata.org)