# coding: utf-8
"""OnedataRESTFS PyFilesystem implementation."""

__author__ = "Bartek Kryza"
__copyright__ = "Copyright (C) 2023 ACK CYFRONET AGH"
__license__ = (
    "This software is released under the MIT license cited in LICENSE.txt"
)

__all__ = ["OnedataRESTFS"]

import io
import os
import stat
import threading
from typing import Any, BinaryIO, Iterable, Optional, SupportsInt, Text

from fs.base import FS
from fs.constants import DEFAULT_CHUNK_SIZE
from fs.enums import ResourceType, Seek
from fs.errors import DirectoryExists, DirectoryExpected, DirectoryNotEmpty
from fs.errors import FileExists, FileExpected
from fs.errors import RemoveRootError, ResourceInvalid, ResourceNotFound
from fs.info import Info
from fs.iotools import line_iterator
from fs.mode import Mode
from fs.path import basename, dirname
from fs.permissions import Permissions
from fs.subfs import SubFS

from ._util import stat_to_permissions
from .onedata_file_client import OnedataFileClient, FileType, OnedataRESTError

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

__all__ = ["OnedataRESTFS"]


class OnedataRESTFile(io.RawIOBase):
    _file_id = None
    _space_name = None
    _oneprovider_host = None
    _odfs = None
    pos = 0
    mode = None

    def __init__(
        self,
        odfs,
        oneprovider_host,  # type: Text
        space_name,
        file_id,  # type: Text
        mode
    ):
        super(OnedataRESTFile, self).__init__()
        self._odfs = odfs
        self._oneprovider_host = oneprovider_host
        self._file_id = file_id
        self._space_name = space_name
        self.mode = mode

    def close(self):
        pass

    def tell(self):
        return self.pos

    def readable(self):
        return self.mode.reading

    def read(self, size=-1):
        if not self.mode.reading:
            raise IOError("File not open for reading")

        if size == 0:
            return b''

        file_size = self._odfs._client.get_attributes(self._space_name, file_id=self._file_id)['size']

        effective_size = file_size

        if size > 0:
            effective_size = min(file_size-self.pos, size)

        return self._odfs._client.get_file_content(self._space_name, self.pos, effective_size, file_id=self._file_id)

    def readinto(self, buf):
        """
        Read from the file into the buffer.

        Read `len(buf)` bytes from the file starting from current position
        and place the data in the `buf` buffer.

        :param bytearray buf: Buffer where the read data will be stored.
        """

        data = self.read(len(buf))
        bytes_read = len(data)
        buf[:len(data)] = data

        return bytes_read

    def readline(self, size=-1):
        """
        Read a single line from file.

        Read `size` bytes from the file starting from current position
        in the file until the end of the line.

        If `size` is negative read until end of the line.

        :param int size: Number of bytes to read from the current line.
        """

        return next(line_iterator(self, size))  # type: ignore

    def readlines(self, hint=-1):
        """
        Read `hint` lines from the file starting from current position.

        If `hint` is negative read until end of the line.

        :param int hint: Number of lines to read.
        """

        lines = []
        size = 0
        for line in line_iterator(self):  # type: ignore
            lines.append(line)
            size += len(line)
            if hint != -1 and size > hint:
                break
        return lines

    def writable(self):
        """Return True if the file was opened for writing."""
        return self.mode.writing

    def write(self, data):
        """
        Write `data` to file starting from current position in the file.

        :param bytes data: Data to write to the file
        """

        if not self.mode.writing:
            raise IOError("File not open for writing")

        return self._odfs._client.put_file_content(self._space_name, self._file_id, self.pos, data)

    def writelines(self, lines):
        """
        Write `lines` to file starting at the current position in the file.

        The elements of `lines` list do not need to contain new line
        characters.

        :param list lines: Lines to wrie to the file
        """

        self.write(b"".join(lines))

    def truncate(self, size=None):
        """
        Change the size of the file to `size`.

        If `size` is smaller than the current size of the file,
        the remaining data will be deleted, if the `size` is larger than the
        current size of the file the file will be padded with zeros.

        :param int size: The new size of the file
        """
        pass

    def seekable(self):
        """Return `True` if the file is seekable."""
        return True

    def seek(self, pos, whence=Seek.set):
        """
        Change current position in an opened file.

        The position can point beyond the current size of the file.
        In such case the file will contain holes.

        :param int pos: New position in the file.
        """

        _whence = int(whence)
        _pos = int(pos)
        if _whence not in (Seek.set, Seek.current, Seek.end):
            raise ValueError("invalid value for whence")

        if _whence == Seek.current or _whence == Seek.set:
            if _pos < 0:
                raise ValueError("Negative seek position {}".format(_pos))
        elif _whence == Seek.end:
            if _pos > 0:
                raise ValueError("Positive seek position {}".format(_pos))

        with self._lock:
            if _whence == Seek.set:
                self.pos = _pos
            if _whence == Seek.current:
                self.pos = self.pos + _pos
            if _whence == Seek.end:
                size = self._odfs._client.get_attributes(self._space_name, file_id=self._file_id)['size']
                self.pos = size + _pos

        return self.tell()


class OnedataRESTFS(FS):
    """
    Implementation of Onedata virtual filesystem for PyFilesystem based on REST API.

    Implementation of `Onedata <https://onedata.org>` filesystem for
    `PyFilesystem <https://pyfilesystem.org>`.
    """

    _meta = {
        "case_insensitive": False,
        "invalid_path_chars": "\0",
        "network": True,
        "read_only": True,
        "thread_safe": True,
        "unicode_paths": True,
        "virtual": False,
    }

    def __init__(
        self,
        onezone_host,  # type: Text
        token,  # type: Text
        space=None,  # type: Text
        insecure=False,  # type: bool
        timeout=30
    ):
        """
        Onedata client OnedataRESTFS constructor.

        `OnedataRESTFS` instance maintains an active connection pool to the
        Oneprovider specified in the `host` parameter as long as it
        is referenced in the code. To close the connection call `close()`
        directly or use context manager.

        :param str host: The Onedata Oneprovider host name
        :param str token: The Onedata user access token
        :param int port: The Onedata Oneprovider port
        :param list space: The list of space names which should be opened.
                           By default, all spaces are opened.
        """

        self._onezone_host = onezone_host
        self._token = token
        self._space = space
        self._timeout = timeout
        self._insecure = insecure
        self._client = OnedataFileClient(self._onezone_host, self._token)

        super(OnedataRESTFS, self).__init__()

    def __repr__(self):
        """Return unique representation of the OnedataRESTFS instance."""

        return self.__str__()

    def __str__(self):
        """Return unique representation of the OnedataRESTFS instance."""

        return "<onedatarestfs '{}:{}/{}'>".format(
            self._onezone_host, self._space
        )

    def _is_space_relative(self):
        return self._space is not None

    def _split_space_path(self, path):
        if self._is_space_relative():
            return self._space, path
        else:
            path_tokens = list(filter(str.strip, path.split('/')))
            if len(path_tokens) == 0:
                raise ResourceInvalid
            elif len(path_tokens) == 1:
                return path_tokens[0], None

            return str(path_tokens[0]), '/'.join(path_tokens[1:])

    def getinfo(self, path, namespaces=None):
        """
        See https://docs.pyfilesystem.org/en/latest/_modules/fs/base.html#FS.getinfo

        :param path:
        :param namespaces:
        :return:
        """

        (space_name, file_path) = self._split_space_path(path)

        try:
            attr = self._client.get_attributes(space_name, file_path=file_path)
        except OnedataRESTError:
            raise ResourceNotFound(path)

        if not 'name' in attr:
            raise ResourceNotFound(path)

        # `info` must be JSON serializable dictionary, so all
        # values must be valid JSON types
        info = {
            "basic": {
                "name": basename(path),
                "is_dir": attr['mode'] == 'DIR',
            }
        }

        rt = ResourceType.unknown
        if attr['mode'] == 'REG':
            rt = ResourceType.file
        if attr['mode'] == 'DIR':
            rt = ResourceType.directory
        if attr['mode'] == 'LNK':
            rt = ResourceType.symlink

        info["details"] = {
            "accessed": attr['atime'],
            "modified": attr['mtime'],
            "size": attr['size'],
            "uid": attr['storage_user_id'],
            "gid": attr['storage_group_id'],
            "type": int(rt),
        }

        info["access"] = {
            "uid": attr['storage_user_id'],
            "gid": attr['storage_group_id'],
            "permissions": Permissions(mode=int(attr['mode'])).dump(),
        }

        return Info(info)

    def listdir(self, path):
        if not self._is_space_relative() and (path == '' or path == '/'):
            # list spaces
            return self._client.list_spaces()

        (space_name, dir_path) = self._split_space_path(path)

        result = []

        limit = 1000
        continuation_token = None

        while True:
            res = self._client.readdir(space_name, dir_path, limit, continuation_token)

            for child in res['children']:
                result.append(child['name'])

            if res['isLast']:
                break

            continuation_token = res['nextPageToken']

        return result

    def makedir(
            self,
            path,  # type: Text
            permissions=None,  # type: Optional[Permissions]
            recreate=False,  # type: bool
    ):
        (space_name, dir_path) = self._split_space_path(path)
        self._client.create_file_at_path(space_name, dir_path, FileType.DIR)

    def openbin(
            self,
            path,  # type: Text
            mode="r",  # type: Text
            buffering=-1,  # type: int
            **options  # type: Any
    ):
        (space_name, file_path) = self._split_space_path(path)

        file_id = None
        try:
            file_id = self._client.get_file_id(space_name, file_path)
        except OnedataRESTError as e:
            if e.http_code == 404 or (e.http_code == 400 and e.error['details']['errno'] == 'enoent'):
                file_id = self._client.create_file(space_name, file_path)
            else:
                raise e

        return OnedataRESTFile(self,  # type: OnedataRESTFS
                self._client.get_provider_for_space(space_name),  # type: Text
                space_name,
                file_id,  # type: Text
                Mode(mode))

    def remove(self, path):
        info = self.getinfo(path)
        if info.is_dir:
            raise FileExpected(path)

        (space_name, file_path) = self._split_space_path(path)

        file_id = self._client.remove(space_name, file_path)

    def removedir(self, path):
        info = self.getinfo(path)
        if not info.is_dir:
            raise FileExpected(path)

        (space_name, file_path) = self._split_space_path(path)

        file_id = self._client.remove(space_name, file_path)

    def setinfo(self, path, info):
        if not self.exists(path):
            raise ResourceNotFound(path)

        attributes = {'mode': f'0{str(info.permissions.mode)}'}

        (space_name, file_path) = self._split_space_path(path)

        self._client.set_attributes(space_name, file_path, attributes)
