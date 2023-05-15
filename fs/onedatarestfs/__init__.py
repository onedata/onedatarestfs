# coding: utf-8
"""OnedataRESTFS PyFilesystem implementation."""

__author__ = "Bartek Kryza"
__copyright__ = "Copyright (C) 2023 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in " \
              "LICENSE.txt"

import sys

#if "pytest" not in sys.modules:
from ._onedatarestfs import OnedataRESTFS # noqa