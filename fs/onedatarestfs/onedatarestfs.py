# coding: utf-8
"""OnedataRESTFS PyFilesystem implementation."""

from __future__ import annotations

__author__ = "Bartek Kryza"
__copyright__ = "Copyright (C) 2023 Onedata"
__license__ = (
    "This software is released under the MIT license cited in LICENSE.txt")

__all__ = ["OnedataRESTFS"]

import io
from typing import (Any, Collection, List, Mapping, Optional, Sized, Text,
                    Tuple, cast)

import fs.errors
from fs.base import FS
from fs.enums import ResourceType, Seek
from fs.errors import (DestinationExists, DirectoryExists, DirectoryExpected,
                       DirectoryNotEmpty, FileExists, FileExpected,
                       RemoveRootError, ResourceInvalid, ResourceNotFound)
from fs.info import Info
from fs.iotools import line_iterator
from fs.mode import Mode
from fs.path import basename, dirname
from fs.permissions import Permissions
from fs.subfs import SubFS

from onedatafilerestclient import OnedataFileRESTClient  # type: ignore
from onedatafilerestclient import OnedataRESTError  # type: ignore

from .errors import to_fserror

__all__ = ["OnedataRESTFS"]


class OnedataRESTFile(io.RawIOBase):
    """OnedataRESTFS file handle implementation.

    These objects provide file handle interface to PyFilesystem.
    """
    _file_id: str
    _space_name: Optional[str] = None
    _oneprovider_host: str
    _odfs: Any
    pos: int = 0
    mode: fs.mode.Mode

    def __init__(self, odfs: Any, oneprovider_host: str, space_name: str,
                 file_id: str, mode: fs.mode.Mode):
        """Create instace of OnedataRESTFile handle."""
        super(OnedataRESTFile, self).__init__()
        self._odfs = odfs
        self._oneprovider_host = oneprovider_host
        self._file_id = file_id
        self._space_name = space_name
        self.mode = mode

        assert (self._space_name is not None)

        if self.mode.appending:
            self.pos = self.client().get_attributes(
                space_name, file_id=self._file_id)['size']

    def client(self) -> OnedataFileRESTClient:
        """Return a reference to the OnedataFileClient instance."""
        return self._odfs.client()

    def tell(self) -> int:
        """Get current file offset."""
        return self.pos

    def readable(self) -> bool:
        """Check whether the file is readable."""
        return self.mode.reading

    def read(self, size: int = -1) -> bytes:
        """Read 'size' bytes from file at current offset."""
        if not self.mode.reading:
            raise IOError("File not open for reading")

        if size == 0:
            return b''

        file_size = self.client().get_attributes(self._space_name,
                                                 file_id=self._file_id)['size']

        if size < 0:
            size = file_size

        available_size = min(file_size - self.pos, size)

        if available_size <= 0:
            return b''

        try:
            data = self.client().get_file_content(self._space_name,
                                                  self.pos,
                                                  available_size,
                                                  file_id=self._file_id)
            self.pos += len(data)

            return cast(bytes, data)
        except OnedataRESTError as e:
            raise to_fserror(e, self._file_id)

    def readinto(self, buf: bytearray) -> int:  # type: ignore
        """
        Read from the file into the buffer.

        Read `len(buf)` bytes from the file starting from current position
        and place the data in the `buf` buffer.

        :param bytearray buf: Buffer where the read data will be stored.
        """
        data = self.read(len(cast(Sized, buf)))
        bytes_read = len(data)
        buf[:len(data)] = data

        return bytes_read

    def readline(self, size: Optional[int] = -1) -> bytes:
        """
        Read a single line from file.

        Read `size` bytes from the file starting from current position
        in the file until the end of the line.

        If `size` is negative read until end of the line.

        :param int size: Number of bytes to read from the current line.
        """
        if not size:
            size = -1
        return cast(bytes, next(line_iterator(self, size)))  # type: ignore

    def readlines(self, hint: int = -1) -> list[bytes]:
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

    def writable(self) -> bool:
        """Return True if the file was opened for writing."""
        return self.mode.writing

    def write(self, data: bytearray) -> Optional[int]:  # type: ignore
        """
        Write `data` to file starting from current position in the file.

        :param bytes data: Data to write to the file
        """
        if not self.mode.writing:
            raise IOError("File not open for writing")

        self.client().put_file_content(self._space_name, self._file_id,
                                       self.pos, data)

        size = len(cast(Sized, data))

        self.pos += size

        return size

    def writelines(self, lines: list[bytes]) -> None:  # type: ignore
        """
        Write `lines` to file starting at the current position in the file.

        The elements of `lines` list do not need to contain new line
        characters.

        :param list lines: Lines to write to the file
        """
        self.write(b''.join(lines))  # type: ignore

    def truncate(self, size: Optional[int] = None) -> int:
        """
        Change the size of the file to `size`.

        If `size` is smaller than the current size of the file,
        the remaining data will be deleted, if the `size` is larger than the
        current size of the file will be padded with zeros.

        :param int size: The new size of the file
        """
        if size is None:
            size = self.pos

        if size == 0:
            self.client().put_file_content(self._space_name, self._file_id,
                                           None, b'')
            self.pos = 0
            return 0

        file_size = self.client().get_attributes(self._space_name,
                                                 file_id=self._file_id)['size']

        if size < file_size:
            self.pos = 0
            self.client().put_file_content(self._space_name, self._file_id,
                                           None, self.read(size))
            self.pos = size
            return size

        # Append file size with zeros up to size
        self.client().put_file_content(self._space_name, self._file_id,
                                       file_size, b'\0' * (size - file_size))
        return size

    def seekable(self) -> bool:
        """Return `True` if the file is seekable."""
        return True

    def seek(self, pos: int, whence: Seek = Seek.set) -> int:  # type: ignore
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
            size = self.client().get_attributes(self._space_name,
                                                file_id=self._file_id)['size']
            self.pos = size + _pos

        return self.tell()


class OnedataRESTFS(FS):
    """Implementation of Onedata virtual filesystem for PyFilesystem.

    Implementation of `Onedata <https://onedata.org>` filesystem for
    `PyFilesystem <https://pyfilesystem.org>`.
    """

    _meta = {
        "case_insensitive": False,
        "invalid_path_chars": "\0",
        "network": True,
        "read_only": False,
        "thread_safe": True,
        "unicode_paths": True,
        "virtual": False,
    }

    def __init__(self,
                 onezone_host: str,
                 token: str,
                 space: Optional[str] = None,
                 preferred_oneproviders: Optional[list[str]] = [],
                 insecure: bool = False,
                 timeout: int = 30):
        """
        Onedata client OnedataRESTFS constructor.

        `OnedataRESTFS` instance maintains an active connection pool to the
        Oneprovider specified in the `host` parameter as long as it
        is referenced in the code. To close the connection call `close()`
        directly or use context manager.

        :param str onezone_host: The Onedata Onezone host name
        :param str token: The Onedata user access token
        :param str space: The Onedata space to be access, if none all available
                          spaces for given token will be visible
        :param [str] preferred_oneproviders: Optional list of preferred
                                             Oneproviders to handle requests
        :param bool insecure: If True, disable SSL verification.
        :param int timeout: Timeout on REST requests.
        """
        self._onezone_host = onezone_host
        self._token = token
        self._space = space
        self._timeout = timeout
        self._preferred_oneproviders = preferred_oneproviders
        self._insecure = insecure
        self._client = OnedataFileRESTClient(self._onezone_host, self._token,
                                             self._preferred_oneproviders)

        super(OnedataRESTFS, self).__init__()

    def __repr__(self) -> str:
        """Return unique representation of the OnedataRESTFS instance."""
        return self.__str__()

    def __str__(self) -> str:
        """Return unique representation of the OnedataRESTFS instance."""
        return "<onedatarestfs '{}:{}...:{}:{}'>".format(
            self._onezone_host, self._token[:24], self._space,
            self._preferred_oneproviders)

    def client(self) -> OnedataFileRESTClient:
        """Return a reference to OnedataFileClient."""
        return self._client

    def _is_space_relative(self) -> bool:
        """Is this OnedataRESTFS instance relative to a specific space."""
        return self._space is not None

    def _split_space_path(self, path: str) -> Tuple[str, Optional[str]]:
        """Create a (space_name, file_path) pair from path.

        Returns a pair (space_name, file_path) where space_name is the name
        of the space and file_path is a relative path in the space.

        If OnedataRESTFS has been created for a specific space, that space is
        used regardless of the ``path`` contents, and the ``path`` is assumed
        to be already relative to the space.
        """
        rpath = fs.path.relpath(path)
        if rpath.endswith('/'):
            rpath = rpath.rstrip('/')

        if self._is_space_relative():
            assert self._space is not None
            return self._space, rpath
        else:
            path_tokens = list(filter(str.strip, rpath.split('/')))
            if len(path_tokens) == 0:
                raise ResourceInvalid(path)
            elif len(path_tokens) == 1:
                return path_tokens[0], None

            return str(path_tokens[0]), '/'.join(path_tokens[1:])

    def getinfo(self,
                path: str,
                namespaces: Optional[Collection[Text]] = None) -> Info:
        """Get information about a resource on a filesystem.

        Arguments:
            path (str): A path to a resource on the filesystem.
            namespaces (list, optional): Info namespaces to query. The
                `"basic"` namespace is alway included in the returned
                info, whatever the value of `namespaces` may be.

        Returns:
            ~fs.info.Info: resource information object.

        Raises:
            fs.errors.ResourceNotFound: If ``path`` does not exist.

        For more information regarding resource information, see :ref:`info`.
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

    def listdir(self, path: str) -> list[str]:
        """Get a list of the resource names in a directory.

        This method will return a list of the resources in a directory.
        A *resource* is a file, directory, or one of the other types
        defined in `~fs.enums.ResourceType`.

        Arguments:
            path (str): A path to a directory on the filesystem

        Returns:
            list: list of names, relative to ``path``.

        Raises:
            fs.errors.DirectoryExpected: If ``path`` is not a directory.
            fs.errors.ResourceNotFound: If ``path`` does not exist.

        """
        self.check()

        try:
            if not self._is_space_relative() and (path == '' or path == '/'):
                # list spaces
                return cast(List[str], self._client.list_spaces())

            if not self.getinfo(path).is_dir:
                raise DirectoryExpected(path)

            (space_name, dir_path) = self._split_space_path(path)

            result = []

            limit = 1000
            continuation_token = None

            while True:
                res = self._client.readdir(space_name, dir_path, limit,
                                           continuation_token)

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
        path: str,
        permissions: Optional[Permissions] = None,
        recreate: bool = False,
    ) -> SubFS[FS]:
        """Make a directory.

        Arguments:
            path (str): Path to directory from root.
            permissions (~fs.permissions.Permissions, optional): a
                `Permissions` instance, or `None` to use default.
            recreate (bool): Set to `True` to avoid raising an error if
                the directory already exists (defaults to `False`).

        Returns:
            ~fs.subfs.SubFS: a filesystem whose root is the new directory.

        Raises:
            fs.errors.DirectoryExists: If the path already exists.
            fs.errors.ResourceNotFound: If the path is not found.

        """
        self.check()

        (space_name, dir_path) = self._split_space_path(path)

        if dir_path is None:
            raise fs.errors.PermissionDenied

        if dir_path == '/' or dir_path == '' or dir_path == '.':
            if recreate:
                return self.opendir(path)
            else:
                raise DirectoryExists(path)

        if self.exists(path):
            if not recreate:
                raise DirectoryExists(path)
        else:
            mode = None
            if permissions:
                mode = permissions.mode

            try:
                self._client.create_file(space_name,
                                         dir_path,
                                         'DIR',
                                         create_parents=recreate,
                                         mode=mode)
            except OnedataRESTError as e:
                raise to_fserror(e, path)

        return self.opendir(path)

    def create(self, path: str, wipe: bool = False) -> bool:
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
            return True

        (space_name, dir_path) = self._split_space_path(path)

        if dir_path is None:
            raise fs.errors.PermissionDenied

        self._client.create_file(space_name, dir_path, 'REG')

        return True

    def openbin(  # type: ignore[override]
            self,
            path: str,
            mode: str = 'r',
            buffering: int = -1,
            **options: Any) -> OnedataRESTFile:
        """Open a binary file-like object.

        Arguments:
            path (str): A path on the filesystem.
            mode (str): Mode to open file (must be a valid non-text mode,
                defaults to *r*). Since this method only opens binary files,
                the ``b`` in the mode string is implied.
            buffering (int): Buffering policy (-1 to use default buffering,
                0 to disable buffering, or any positive integer to indicate
                a buffer size).
            **options: keyword arguments for any additional information
                required by the filesystem (if any).

        Returns:
            io.IOBase: a *file-like* object.

        Raises:
            fs.errors.FileExpected: If ``path`` exists and is not a file.
            fs.errors.FileExists: If the ``path`` exists, and
                *exclusive mode* is specified (``x`` in the mode).
            fs.errors.ResourceNotFound: If ``path`` does not exist and
                ``mode`` does not imply creating the file, or if any
                ancestor of ``path`` does not exist.

        """
        self.check()

        Mode(mode).validate_bin()

        if mode == 'x':
            mode = 'rwx'

        if 'b' not in mode:
            mode = f'{mode}b'

        if self.exists(path) and self.getinfo(path).is_dir:
            raise FileExpected(path)

        (space_name, file_path) = self._split_space_path(path)

        if file_path is None:
            raise FileExpected(path)

        file_id = None
        try:
            if 'x' in mode and self.exists(path):
                raise FileExists(path)

            if ('w' in mode or 'a' in mode) and not self.exists(path):
                if not self.exists(dirname(path)):
                    raise ResourceNotFound(dirname(path))

                self._client.create_file(space_name, file_path)

            if file_id is None:
                file_id = self._client.get_file_id(space_name, file_path)
        except OnedataRESTError as e:
            raise to_fserror(e, path, 'get_attributes')

        return OnedataRESTFile(self,
                               self._client.get_provider_for_space(space_name),
                               space_name, file_id, Mode(mode))

    def remove(self, path: str) -> None:
        """Remove a file from the filesystem.

        Arguments:
            path (str): Path of the file to remove.

        Raises:
            fs.errors.FileExpected: If the path is a directory.
            fs.errors.ResourceNotFound: If the path does not exist.

        """
        self.check()

        info = self.getinfo(path)
        if info.is_dir:
            raise FileExpected(path)

        (space_name, file_path) = self._split_space_path(path)

        if file_path is None:
            raise fs.errors.PermissionDenied

        self._client.remove(space_name, file_path)

    def removedir(self, path: str) -> None:
        """Remove a directory from the filesystem.

        Arguments:
            path (str): Path of the directory to remove.

        Raises:
            fs.errors.DirectoryNotEmpty: If the directory is not empty (
                see `~fs.base.FS.removetree` for a way to remove the
                directory contents).
            fs.errors.DirectoryExpected: If the path does not refer to
                a directory.
            fs.errors.ResourceNotFound: If no resource exists at the
                given path.
            fs.errors.RemoveRootError: If an attempt is made to remove
                the root directory (i.e. ``'/'``)

        """
        self.check()

        if path == '/' or path == '' or path == '.':
            raise RemoveRootError(path)

        info = self.getinfo(path)
        if not info.is_dir:
            raise DirectoryExpected(path)

        (space_name, dir_path) = self._split_space_path(path)

        if dir_path is None:
            raise fs.errors.PermissionDenied

        res = self._client.readdir(space_name, dir_path, 2, None)

        if 'children' in res and len(res['children']) > 0:
            raise DirectoryNotEmpty(path)

        self._client.remove(space_name, dir_path)

    def setinfo(self, path: str, info: Mapping[Text, Mapping[Text,
                                                             object]]) -> None:
        """Set info on a resource.

        This method is the complement to `~fs.base.FS.getinfo`
        and is used to set info values on a resource.

        Arguments:
            path (str): Path to a resource on the filesystem.
            info (dict): Dictionary of resource info.

        Raises:
            fs.errors.ResourceNotFound: If ``path`` does not exist
                on the filesystem

        The ``info`` dict should be in the same format as the raw
        info returned by ``getinfo(file).raw``.

        Example:
            >>> details_info = {"details": {
            ...     "modified": time.time()
            ... }}
            >>> my_fs.setinfo('file.txt', details_info)

        """
        self.check()

        if not self.exists(path):
            raise ResourceNotFound(path)

        # Currently we only support mode setting
        if 'access' in info and 'permissions' in info['access']:
            perms = cast(str, info["access"]["permissions"])
            attributes = {'mode': f'0{str(Permissions(perms).mode)}'}
            (space_name, file_path) = self._split_space_path(path)

            if file_path is None:
                raise fs.errors.PermissionDenied

            self._client.set_attributes(space_name, file_path, attributes)

    def move(self,
             src_path: str,
             dst_path: str,
             overwrite: bool = False,
             preserve_time: bool = False) -> None:
        """Move a file from ``src_path`` to ``dst_path``.

        Arguments:
            src_path (str): A path on the filesystem to move.
            dst_path (str): A path on the filesystem where the source
                file will be written to.
            overwrite (bool): If `True`, destination path will be
                overwritten if it exists.
            preserve_time (bool): If `True`, try to preserve mtime of the
                resources (defaults to `False`).

        Raises:
            fs.errors.FileExpected: If ``src_path`` maps to a
                directory instead of a file.
            fs.errors.DestinationExists: If ``dst_path`` exists,
                and ``overwrite`` is `False`.
            fs.errors.ResourceNotFound: If a parent directory of
                ``dst_path`` does not exist.

        """
        self.check()

        if not self.exists(src_path) or not self.exists(dirname(dst_path)):
            raise ResourceNotFound(src_path)

        if self.isdir(src_path):
            raise FileExpected(src_path)

        if not overwrite and self.exists(dst_path):
            raise DestinationExists(dst_path)

        (src_space_name, src_file_path) = self._split_space_path(src_path)
        (dst_space_name, dst_file_path) = self._split_space_path(dst_path)

        if src_file_path is None:
            raise fs.errors.PermissionDenied

        if dst_file_path is None:
            raise fs.errors.PermissionDenied

        if src_space_name != dst_space_name:
            super(OnedataRESTFS, self).move(src_path, dst_path)

        self._client.move(src_space_name, src_file_path, dst_space_name,
                          dst_file_path)
