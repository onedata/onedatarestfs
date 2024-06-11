# coding: utf-8
"""OnedataRESTFS PyFilesystem test case suite."""

__author__ = "Bartek Kryza"
__copyright__ = "Copyright (C) 2023 Onedata"
__license__ = (
    "This software is released under the MIT license cited in LICENSE.txt")

import sys
import unittest
import os
import pytest

from fs.test import FSTestCases

try:
    from fs.onedatarestfs import OnedataRESTFS
except ModuleNotFoundError:
    # This is necessary for running unit tests directly without installing
    sys.path.extend(['../..'])
    from onedatarestfs import OnedataRESTFS
except ImportError:
    # This is necessary for running unit tests directly without installing
    sys.path.extend(['../../fs'])
    from onedatarestfs import OnedataRESTFS

if "pytest" in sys.modules:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@pytest.mark.usefixtures("onezone_ip", "onezone_admin_token")
class TestOnedataRESTFS(FSTestCases, unittest.TestCase):
    space_name = 'test_onedatarestfs'

    def make_fs(self):
        # Return an instance of your FS object here
        preferred_providers = [
            'dev-oneprovider-krakow.default.svc.cluster.local'
        ]
        restfs = OnedataRESTFS(os.getenv('ONEZONE_IP'),
                               os.getenv('ONEZONE_ADMIN_TOKEN'),
                               self.space_name,
                               preferred_providers,
                               verify_ssl=False)
        self._client = restfs.client()
        self._delete_contents()
        return restfs

    def _delete_contents(self):
        res = self._client.list_children(self.space_name, file_path='')
        for child in res['children']:
            self._client.remove(self.space_name, file_path=child['name'])
