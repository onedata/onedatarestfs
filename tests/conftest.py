# coding: utf-8
"""Pytest test setup."""

import logging
import os
import time
import uuid

import pytest
import requests
from urllib3.util import connection

from onedatafilerestclient import OnedataFileRESTClient


def trace_requests_messages() -> None:
    """Enable logging HTTP requests."""
    import http.client as http_client
    http_client.HTTPConnection.debuglevel = 1

    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True


# Uncomment to enable HTTP request trace log
# trace_requests_messages()

FIXTURE_SCOPE = "session"

_original_create_connection = connection.create_connection


def patched_create_connection(address, *args, **kwargs):
    """Resolve Kubernetes domain names to IP's from environment variables."""
    host, port = address
    hostname = host

    if host == 'dev-onezone-0.default.svc.cluster.local':
        hostname = os.getenv('DEV_ONEZONE_0')
    elif host == 'dev-oneprovider-krakow.default.svc.cluster.local':
        hostname = os.getenv('DEV_ONEPROVIDER_KRAKOW_0')
    elif host == 'dev-oneprovider-paris.default.svc.cluster.local':
        hostname = os.getenv('DEV_ONEPROVIDER_PARIS_0')

    return _original_create_connection((hostname, port), *args, **kwargs)


connection.create_connection = patched_create_connection


@pytest.fixture(scope=FIXTURE_SCOPE)
def git_version():
    """Get version based on Git commit."""
    gv = os.getenv('GIT_VERSION')
    yield gv


@pytest.fixture
def uuid_str():
    """Generate UUID v4."""
    return str(uuid.uuid4())


@pytest.fixture(scope=FIXTURE_SCOPE)
def onezone_ip():
    """Get Onezone IP from environment variable."""
    ozip = os.getenv('DEV_ONEZONE_0')
    os.environ['ONEZONE_IP'] = ozip
    yield ozip


@pytest.fixture(scope=FIXTURE_SCOPE)
def oneprovider_krakow_ip():
    """Get Oneprovider 'krakow' IP from environment variable."""
    opip = os.getenv('DEV_ONEPROVIDER_KRAKOW_0')
    yield opip


@pytest.fixture(scope=FIXTURE_SCOPE)
def oneprovider_paris_ip():
    """Get Oneprovider 'paris' IP from environment variable."""
    opip = os.getenv('DEV_ONEPROVIDER_PARIS_0')
    yield opip


@pytest.fixture(scope=FIXTURE_SCOPE)
def onezone_admin_token(onezone_ip):
    """Generate a new client token."""
    tokens_endpoint = f'https://{onezone_ip}/api/v3/onezone/user/client_tokens'
    res = requests.post(tokens_endpoint, {},
                        auth=requests.auth.HTTPBasicAuth('admin', 'password'),
                        verify=False)
    os.environ['ONEZONE_ADMIN_TOKEN'] = res.json()["token"]
    return res.json()["token"]


@pytest.fixture(scope="module", autouse=True)
def wait_for_support_sync(onezone_ip, oneprovider_krakow_ip,
                          oneprovider_paris_ip, onezone_admin_token):
    """Wait until providers are fully synchronized after setup."""
    print("INFO: Waiting for space support synchronization...")

    retry_count = 60
    krakow_client = OnedataFileRESTClient(onezone_ip,
                                          onezone_admin_token,
                                          [oneprovider_krakow_ip],
                                          verify_ssl=False)
    paris_client = OnedataFileRESTClient(onezone_ip,
                                         onezone_admin_token,
                                         [oneprovider_paris_ip],
                                         verify_ssl=False)

    while True:
        try:
            krakow_client.get_file_id('test_onedatarestfs')
            paris_client.get_file_id('test_onedatarestfs')
            time.sleep(1)
        except:
            if retry_count == 0:
                raise RuntimeError('ERROR: Space support information for '
                                   '"test_onedatarestfs" did not synchronize '
                                   'properly')
            retry_count -= 1
            continue

        break

    print("INFO: Space 'test_onedatarestfs' support information synced")


@pytest.fixture(scope=FIXTURE_SCOPE)
def onezone_readonly_token(onezone_ip):
    """Generate new readonly only client token."""
    temporary_token_path = 'api/v3/onezone/user/tokens/temporary'
    tokens_endpoint = f'https://{onezone_ip}/{temporary_token_path}'
    headers = {'content-type': 'application/json'}
    res = requests.post(tokens_endpoint,
                        json={
                            "type": {
                                "accessToken": {}
                            },
                            "caveats": [{
                                "type": "data.readonly"
                            }, {
                                "type":
                                "time",
                                "validUntil":
                                int(time.time()) + 2592000
                            }]
                        },
                        headers=headers,
                        auth=requests.auth.HTTPBasicAuth('admin', 'password'),
                        verify=False)
    return res.json()["token"]
