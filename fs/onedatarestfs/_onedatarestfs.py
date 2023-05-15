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

import fs.errors
from fs.base import FS
from fs.constants import DEFAULT_CHUNK_SIZE
from fs.enums import ResourceType, Seek
from fs.errors import DirectoryExists, DirectoryExpected, DirectoryNotEmpty, DestinationExists
from fs.errors import FileExists, FileExpected
from fs.errors import RemoveRootError, ResourceInvalid, ResourceNotFound
from fs.info import Info
from fs.iotools import line_iterator
from fs.mode import Mode
from fs.path import basename, dirname
from fs.permissions import Permissions

from .onedata_file_client import OnedataFileClient, OnedataRESTError

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

__all__ = ["OnedataRESTFS"]


def to_fserror(e: OnedataRESTError, msg: str = None, request: str = None):
    if msg is None:
        msg = e.description

    if e.http_code == 404:
        return fs.errors.ResourceNotFound(msg)

    if e.http_code == 416:
        return fs.errors.FSError("Invalid range")

    if e.error_category and e.error_category == 'posix':
        if e.error_details['errno'] == 'enoent':
            return fs.errors.ResourceNotFound(msg)
        if e.error_details['errno'] == 'eexist':
            return fs.errors.FileExists(msg)
        if e.error_details['errno'] == 'eaccess':
            return fs.errors.PermissionDenied(msg)
        if e.error_details['errno'] == 'eperm':
            return fs.errors.PermissionDenied(msg)
        if e.error_details['errno'] == 'enotdir':
            if request == 'get_attributes':
                return fs.errors.ResourceNotFound(msg)
            return fs.errors.DirectoryExpected(msg)

    if e.error_category and e.error_category == 'badValueFilePath':
        return fs.errors.InvalidCharsInPath(msg)


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

        assert(self._space_name is not None)

        if mode.appending:
            self.pos = self._odfs._client.get_attributes(space_name, file_id=self._file_id)['size']


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

        if size < 0:
            size = file_size

        available_size = min(file_size - self.pos, size)

        if available_size <= 0:
            return b''

        try:
            data = self._odfs._client.get_file_content(self._space_name, self.pos,
                                                       available_size, file_id=self._file_id)
            self.pos += len(data)

            return data
        except OnedataRESTError as e:
            raise to_fserror(e, self._file_id)

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

        self._odfs._client.put_file_content(self._space_name, self._file_id, self.pos, data)

        self.pos += len(data)

    def writelines(self, lines):
        """
        Write `lines` to file starting at the current position in the file.

        The elements of `lines` list do not need to contain new line
        characters.

        :param list lines: Lines to wrie to the file
        """

        self.write(b''.join(lines))

    def truncate(self, size=None):
        """
        Change the size of the file to `size`.

        If `size` is smaller than the current size of the file,
        the remaining data will be deleted, if the `size` is larger than the
        current size of the file the file will be padded with zeros.

        :param int size: The new size of the file
        """

        if size is None:
            size = self.pos

        if size == 0:
            self._odfs._client.put_file_content(self._space_name, self._file_id, None, b'')
            self.pos = 0
            return

        file_size = self._odfs._client.get_attributes(self._space_name, file_id=self._file_id)['size']

        if size < file_size:
            self.pos = 0
            self._odfs._client.put_file_content(self._space_name, self._file_id, None, self.read(size))
            self.pos = size
        else:
            # Append file size with zeros up to size
            self._odfs._client.put_file_content(self._space_name, self._file_id, file_size, b'\0' * (size - file_size))



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
        "case_insensitive": True,
        "invalid_path_chars": "\0",
        "network": True,
        "read_only": False,
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
        timeout=5
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

        return "<onedatarestfs '{}:{}'>".format(
            self._onezone_host, self._space
        )

    def _is_space_relative(self):
        return self._space is not None

    def _split_space_path(self, path):
        rpath = fs.path.relpath(path)
        if self._is_space_relative():
            return self._space, rpath
        else:
            path_tokens = list(filter(str.strip, rpath.split('/')))
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
        self.check()

        (space_name, file_path) = self._split_space_path(path)

        try:
            attr = self._client.get_attributes(space_name, file_path=file_path)
        except OnedataRESTError as e:
            raise to_fserror(e, path, 'get_attributes')

        if 'name' not in attr:
            raise ResourceNotFound(path)

        # `info` must be JSON serializable dictionary, so all
        # values must be valid JSON types
        info = {
            "basic": {
                "name": basename(path),
                "is_dir": attr['type'] == 'DIR',
            }
        }

        rt = ResourceType.unknown
        if attr['type'] == 'REG' or attr['type'] == 'LNK':
            rt = ResourceType.file
        if attr['type'] == 'DIR':
            rt = ResourceType.directory
        if attr['type'] == 'SYMLNK':
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
        self.check()

        try:
            if not self._is_space_relative() and (path == '' or path == '/'):
                # list spaces
                return self._client.list_spaces()

            if not self.getinfo(path).is_dir:
                raise DirectoryExpected(path)

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
        except OnedataRESTError as e:
            raise to_fserror(e, path)

    def makedir(
            self,
            path,  # type: Text
            permissions=None,  # type: Optional[Permissions]
            recreate=False,  # type: bool
    ):
        self.check()

        (space_name, dir_path) = self._split_space_path(path)

        if dir_path == '/' or dir_path == '' or dir_path == '.':
            if recreate:
                return self.opendir(path)
            else:
                raise DirectoryExists(path)

        if self.exists(path):
            if not recreate:
                raise DirectoryExists(path)
        else:
            attr = self.getinfo(dirname(path))

            if not attr.is_dir:
                raise DirectoryExpected(dirname(path))

            self._client.create_file(space_name, dir_path, 'DIR')

        return self.opendir(path)

    def create(self, path, wipe=False):
        # type: (Text, bool) -> bool
        """Create an empty file.

        The default behavior is to create a new file if one doesn't
        already exist. If ``wipe`` is `True`, any existing file will
        be truncated.

        Arguments:
            path (str): Path to a new file in the filesystem.
            wipe (bool): If `True`, truncate any existing
                file to 0 bytes (defaults to `False`).

        Returns:
            bool: `True` if a new file had to be created.

        """
        self.check()

        exists = self.exists(path)
        if not wipe and exists:
            return False

        if wipe and exists:
            with self.openbin(path, 'wb') as f:
                f.truncate(0)
            return

        (space_name, dir_path) = self._split_space_path(path)

        self._client.create_file(space_name, dir_path, 'REG')

        return True

    def openbin(
            self,
            path,  # type: Text
            mode='r',  # type: Text
            buffering=-1,  # type: int
            **options  # type: Any
    ):
        self.check()

        if mode == 'x':
            mode = 'rwx'

        if self.exists(path) and self.getinfo(path).is_dir:
            raise FileExpected(path)

        (space_name, file_path) = self._split_space_path(path)

        assert(space_name is not None)

        file_id = None
        try:
            if ('w' in mode or 'a' in mode) and not self.exists(path):
                if not self.exists(dirname(path)):
                    raise ResourceNotFound(dirname(path))

                self._client.create_file(space_name, file_path)

            if file_id is None:
                file_id = self._client.get_file_id(space_name, file_path)
        except OnedataRESTError as e:
            raise to_fserror(e, path)

        return OnedataRESTFile(self,
                self._client.get_provider_for_space(space_name),
                space_name,
                file_id,
                Mode(mode))

    def remove(self, path):
        self.check()

        info = self.getinfo(path)
        if info.is_dir:
            raise FileExpected(path)

        (space_name, file_path) = self._split_space_path(path)

        file_id = self._client.remove(space_name, file_path)

    def removedir(self, path):
        self.check()

        info = self.getinfo(path)
        if not info.is_dir:
            raise FileExpected(path)

        (space_name, file_path) = self._split_space_path(path)

        file_id = self._client.remove(space_name, file_path)

    def setinfo(self, path, info):
        self.check()

        if not self.exists(path):
            raise ResourceNotFound(path)

        # Currently we only support mode setting
        if 'access' in info and 'permissions' in info['access']:
            attributes = {'mode': f'0{str(Permissions(info["access"]["permissions"]).mode)}'}
            (space_name, file_path) = self._split_space_path(path)
            self._client.set_attributes(space_name, file_path, attributes)

    def move(self, src_path, dst_path, overwrite=False, preserve_time=False):
        """
        Rename file from `src_path` to `dst_path`.

        :param str src_path: The old file path
        :param str dst_path: The new file path
        :param bool overwrite: When `True`, existing file at `dst_path` will be
                               replaced by contents of file at `src_path`
        :param bool preserve_time: If `True`, try to preserve mtime of the
                                   resources (defaults to `False`).
        """
        # type: (Text, Text, bool, bool) -> None

        self.check()

        if not self.exists(src_path) or not self.exists(dirname(dst_path)):
            raise ResourceNotFound(src_path)

        if self.isdir(src_path):
            raise FileExpected(src_path)

        if not overwrite and self.exists(dst_path):
            raise DestinationExists(dst_path)

        (src_space_name, src_file_path) = self._split_space_path(src_path)
        (dst_space_name, dst_file_path) = self._split_space_path(dst_path)

        if src_space_name != dst_space_name:
            FS.move(src_path, dst_path)

        self._client.move(src_space_name, src_file_path, dst_space_name, dst_file_path)
