# coding: utf-8
"""OnedataRESTFS PyFilesystem utility functions."""

__author__ = "Bartek Kryza"
__copyright__ = "Copyright (C) 2023 ACK CYFRONET AGH"
__license__ = (
    "This software is released under the MIT license cited in LICENSE.txt"
)

__all__ = ["stat_to_permissions"]

import stat

from fs.permissions import Permissions


FUSE_SET_ATTR_MODE = (1 << 0)
FUSE_SET_ATTR_SIZE = (1 << 3)
FUSE_SET_ATTR_ATIME = (1 << 4)
FUSE_SET_ATTR_MTIME = (1 << 5)


def stat_to_permissions(attr):
    """
    Convert PyFilesystem Info instance `attr` to permissions string.

    :param Info attr: The PyFilesystem Info instance.
    """
    # 'other' permissions
    other = ""
    other += "r" if stat.S_IROTH & attr.mode else "-"
    other += "w" if stat.S_IWOTH & attr.mode else "-"
    other += "x" if stat.S_IXOTH & attr.mode else "-"

    # 'group' permission
    group = ""
    group += "r" if stat.S_IRGRP & attr.mode else "-"
    group += "w" if stat.S_IWGRP & attr.mode else "-"
    group += "x" if stat.S_IXGRP & attr.mode else "-"

    # 'user' permission
    user = ""
    user += "r" if stat.S_IRUSR & attr.mode else "-"
    user += "w" if stat.S_IWUSR & attr.mode else "-"
    user += "x" if stat.S_IXUSR & attr.mode else "-"

    sticky = stat.S_ISVTX & attr.mode
    setuid = stat.S_ISUID & attr.mode
    setguid = stat.S_ISGID & attr.mode

    return Permissions(
        user=user,
        group=group,
        other=other,
        sticky=sticky,
        setuid=setuid,
        setguid=setguid,
    )


def info_to_stat(info):
    """
    Convert PyFilesystem Info instance to Stat structure.

    Only the following attributes from the Info structure can
    be updated in Onedata:
       - mode (i.e. permissions)
       - size
       - atime
       - mtime
    Only these parameters are added to the returned Stat instance.

    :param Info info: The PyFilesystem Info instance.
    """
    from onedatafs import Stat # noqa

    attr = Stat()
    to_set = 0

    if 'details' in info:
        if 'size' in info['details']:
            attr.size = info['details']['size']
            to_set = to_set | FUSE_SET_ATTR_SIZE
        if 'accessed' in info['details']:
            attr.atime = int(info['details']['accessed'])
            to_set = to_set | FUSE_SET_ATTR_ATIME
        if 'modified' in info['details']:
            attr.mtime = int(info['details']['modified'])
            to_set = to_set | FUSE_SET_ATTR_MTIME
    if 'access' in info:
        if 'permissions' in info['access']:
            attr.mode = Permissions(info['access']['permissions']).mode
            to_set = to_set | FUSE_SET_ATTR_MODE

    return (attr, to_set)