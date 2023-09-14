# coding: utf-8
"""onedatarestfs error mapping."""

from __future__ import annotations

__author__ = "Bartek Kryza"
__copyright__ = "Copyright (C) 2023 Onedata"
__license__ = (
    "This software is released under the MIT license cited in LICENSE.txt")

from typing import Mapping, Optional, Type

import fs.errors as fs_errors

from onedatafilerestclient import OnedataRESTError  # type: ignore

POSIX_TO_PYFS_ERROR_MAP: Mapping[str, Type[fs_errors.FSError]] = {
    "e2big": fs_errors.FSError,
    "eacces": fs_errors.PermissionDenied,
    "eaddrinuse": fs_errors.FSError,
    "eaddrnotavail": fs_errors.NoURL,
    "eafnosupport": fs_errors.Unsupported,
    "eagain": fs_errors.FSError,
    "ealready": fs_errors.FSError,
    "ebadf": fs_errors.FSError,
    "ebadmsg": fs_errors.FSError,
    "ebusy": fs_errors.OperationTimeout,
    "ecanceled": fs_errors.OperationFailed,
    "echild": fs_errors.FSError,
    "econnaborted": fs_errors.FSError,
    "econnrefused": fs_errors.FSError,
    "econnreset": fs_errors.FSError,
    "edeadlk": fs_errors.FSError,
    "edestaddrreq": fs_errors.FSError,
    "edom": fs_errors.OperationFailed,
    "eexist": fs_errors.FileExists,
    "efault": fs_errors.OperationFailed,
    "efbig": fs_errors.FSError,
    "ehostunreach": fs_errors.FSError,
    "eidrm": fs_errors.FSError,
    "eilseq": fs_errors.FSError,
    "einprogress": fs_errors.FSError,
    "eintr": fs_errors.FSError,
    "einval": fs_errors.InvalidCharsInPath,
    "eio": fs_errors.OperationFailed,
    "eisconn": fs_errors.FSError,
    "eisdir": fs_errors.FileExpected,
    "ekeyexpired": fs_errors.FSError,
    "eloop": fs_errors.FSError,
    "emfile": fs_errors.FSError,
    "emlink": fs_errors.FSError,
    "emsgsize": fs_errors.FSError,
    "enametoolong": fs_errors.FSError,
    "enetdown": fs_errors.FSError,
    "enetreset": fs_errors.FSError,
    "enetunreach": fs_errors.FSError,
    "enfile": fs_errors.FSError,
    "enoattr": fs_errors.FSError,
    "enobufs": fs_errors.FSError,
    "enodata": fs_errors.FSError,
    "enodev": fs_errors.FSError,
    "enoent": fs_errors.ResourceNotFound,
    "enoexec": fs_errors.FSError,
    "enolck": fs_errors.FSError,
    "enolink": fs_errors.FSError,
    "enomem": fs_errors.FSError,
    "enomsg": fs_errors.FSError,
    "enoprotoopt": fs_errors.FSError,
    "enospc": fs_errors.InsufficientStorage,
    "enosr": fs_errors.FSError,
    "enostr": fs_errors.FSError,
    "enosys": fs_errors.FSError,
    "enotconn": fs_errors.RemoteConnectionError,
    "enotdir": fs_errors.DirectoryExpected,
    "enotempty": fs_errors.DirectoryNotEmpty,
    "enotrecoverable": fs_errors.FSError,
    "enotsock": fs_errors.FSError,
    "enotsup": fs_errors.FSError,
    "enotty": fs_errors.FSError,
    "enxio": fs_errors.FSError,
    "eopnotsupp": fs_errors.FSError,
    "eoverflow": fs_errors.FSError,
    "eownerdead": fs_errors.FSError,
    "eperm": fs_errors.PermissionDenied,
    "epipe": fs_errors.FSError,
    "eproto": fs_errors.FSError,
    "eprotonosupport": fs_errors.FSError,
    "eprototype": fs_errors.FSError,
    "erange": fs_errors.FSError,
    "erofs": fs_errors.FSError,
    "espipe": fs_errors.FSError,
    "esrch": fs_errors.FSError,
    "etime": fs_errors.FSError,
    "etimedout": fs_errors.OperationTimeout,
    "etxtbsy": fs_errors.FSError,
    "ewouldblock": fs_errors.OperationFailed,
    "exdev": fs_errors.FSError
}


def to_fserror(e: OnedataRESTError,
               msg: Optional[str] = None,
               request: Optional[str] = None) -> fs_errors.FSError:
    """Return PyFilesystem exception based on a OnedataRESTError instance."""
    if msg is None:
        msg = e.description

        if e.error_category:
            msg = f'[{e.error_category}] {msg}'

        if e.error_details:
            msg = f'{msg} ({e.error_details})'

    if e.http_code == 404:
        return fs_errors.ResourceNotFound(msg)

    if e.http_code == 416:
        return fs_errors.FSError("Invalid range")

    if e.http_code == 400:
        if e.error_category and e.error_category == 'posix':
            error_class = POSIX_TO_PYFS_ERROR_MAP.get(e.error_details['errno'],
                                                      fs_errors.FSError)

            if e.error_details['errno'] == 'enotdir' \
                    and request == 'get_attributes':
                error_class = fs_errors.ResourceNotFound

            return error_class(msg)

        if e.error_category and e.error_category == 'badValueFilePath':
            return fs_errors.InvalidCharsInPath(msg)

    if e.http_code == 500:
        return fs_errors.FSError(msg)

    return fs_errors.FSError(msg)
