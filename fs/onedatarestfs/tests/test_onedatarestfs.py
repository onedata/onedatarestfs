# coding: utf-8
"""OnedataRESTFS PyFilesystem test case suite."""

__author__ = "Bartek Kryza"
__copyright__ = "Copyright (C) 2023 Onedata"
__license__ = (
    "This software is released under the MIT license cited in LICENSE.txt"
)

import unittest
import sys
from fs.test import FSTestCases

try:
    from fs.onedatarestfs import OnedataRESTFS
    from fs.onedatarestfs.onedata_file_client import OnedataFileClient
except ModuleNotFoundError:
    # This is necessary for running unit tests directly without installing
    sys.path.extend(['../..'])
    from onedatarestfs import OnedataRESTFS
    from onedatarestfs.onedata_file_client import OnedataFileClient
except ImportError:
    # This is necessary for running unit tests directly without installing
    sys.path.extend(['../..'])
    from onedatarestfs import OnedataRESTFS
    from onedatarestfs.onedata_file_client import OnedataFileClient

if "pytest" in sys.modules:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class TestOnedataRESTFS(FSTestCases, unittest.TestCase):
    space_name = ''
    token = ''
    onezone_host = ''

    client = OnedataFileClient(onezone_host, token)

    def make_fs(self):
        self._delete_contents()
        # Return an instance of your FS object here
        return OnedataRESTFS(self.onezone_host,
                             self.token,
                             self.space_name)

    def _delete_contents(self):
        res = self.client.readdir(self.space_name, '')
        for child in res['children']:
            self.client.remove(self.space_name, child['name'])