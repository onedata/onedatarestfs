import unittest
import sys
from fs.test import FSTestCases

sys.path.extend(['../..'])

from onedatarestfs import OnedataRESTFS
from onedatarestfs.onedata_file_client import OnedataFileClient

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