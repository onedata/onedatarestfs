# coding: utf-8
"""OnedataRESTFS PyFilesystem test case suite."""

__author__ = "Bartek Kryza"
__copyright__ = "Copyright (C) 2023 Onedata"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import os
import sys
import unittest

import pytest

# Import PyFilesystem2's test module by temporarily removing local fs from path
original_path = sys.path[:]
for path in sys.path[:]:
    if path.endswith("onedatarestfs") or "onedatarestfs" in path:
        sys.path.remove(path)

from fs.test import FSTestCases

# Restore original path
sys.path[:] = original_path

# Now import the local onedatarestfs module
try:
    # Try the installed package first
    from fs.onedatarestfs import OnedataRESTFS
except ModuleNotFoundError:
    # Fall back to local module import
    try:
        # Add project root to path for local fs package
        import os

        project_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../..")
        )
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
        from fs.onedatarestfs import OnedataRESTFS
    except (ImportError, ModuleNotFoundError):
        # Last resort - try old-style import
        sys.path.insert(
            0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../fs"))
        )
        from onedatarestfs import OnedataRESTFS

if "pytest" in sys.modules:
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@pytest.mark.usefixtures("onezone_ip", "onezone_admin_token")
class TestOnedataRESTFS(FSTestCases, unittest.TestCase):
    space_name = "test_onedatarestfs"

    def assertRaisesRegexp(self, *args, **kwargs):
        """Compatibility method for deprecated assertRaisesRegexp."""
        return self.assertRaisesRegex(*args, **kwargs)

    def make_fs(self):
        # Return an instance of your FS object here
        preferred_providers = ["dev-oneprovider-krakow.default.svc.cluster.local"]
        restfs = OnedataRESTFS(
            os.getenv("ONEZONE_IP"),
            os.getenv("ONEZONE_ADMIN_TOKEN"),
            self.space_name,
            preferred_providers,
            verify_ssl=False,
        )
        self._client = restfs.client()
        self._delete_contents()
        return restfs

    def _delete_contents(self):
        res = self._client.list_children(self.space_name, file_path="")
        for child in res["children"]:
            self._client.remove(self.space_name, file_path=child["name"])
