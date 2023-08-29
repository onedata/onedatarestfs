# coding: utf-8
"""Onedata REST file API client."""

from __future__ import annotations

__author__ = "Bartek Kryza"
__copyright__ = "Copyright (C) 2023 Onedata"
__license__ = (
    "This software is released under the MIT license cited in LICENSE.txt"
)

import random
import requests
import logging
import json
import urllib3
from functools import lru_cache
from typing import Any, Optional

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def trace_requests_messages() -> None:
    import http.client as http_client
    http_client.HTTPConnection.debuglevel = 1

    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True


# Uncomment to enable HTTP request trace log
#trace_requests_messages()


class OnedataRESTError(Exception):
    """Custom Onedata REST exception class."""

    def __init__(self, response: requests.Response):
        self.http_code = response.status_code
        self.error_category = None
        self.error_details = None
        self.description = None

        try:
            self.error_category = response.json()['error']['id']
            self.error_details = response.json()['error']['details']
            self.description = response.json()['error']['description']
        except:
            pass

    def __repr__(self) -> str:
        """Return unique representation of the OnedataRESTFS instance."""

        return self.__str__()

    def __str__(self) -> str:
        """Return unique representation of the OnedataRESTFS instance."""

        return "<onedataresterror '{} {}:{}'>".format(
            self.http_code, self.error_category, self.description
        )


class OnedataFileClient:
    """Custom REST client for Onedata REST basic file operations API."""
    _timeout: int = 5

    def __init__(self, onezone_host: str, token: str):
        self._onezone_host = onezone_host
        self._token = token
        self._session = requests.Session()
        self._session.headers.update({'X-Auth-Token': self._token})

    def oz_url(self, path: str) -> str:
        """Generate Onezone URL for specific path."""
        return f'https://{self._onezone_host}/api/v3/onezone{path}'

    def op_url(self, space_name: str, path: str) -> str:
        """Generate Oneprovider URL for specific path."""
        return f'https://{self.get_provider_for_space(space_name)}/api/v3/oneprovider{path}'

    def send_request(self, method: str, url: str, data: Any = None, headers: dict[str, str] = {}) -> requests.Response:
        logging.debug(f">> {method} {url} {headers}")

        if not 'Content-type' in headers:
            headers['Content-type'] = 'application/json'

        req = requests.Request(method, url, data=data, headers=headers)
        prepared = self._session.prepare_request(req)

        response = self._session.send(prepared, timeout=self._timeout, verify=False)

        if not response.ok:
            logging.debug(f"ERROR: {method} {url} '{response.text}'")
            raise OnedataRESTError(response)

        logging.debug(f'<< {response.content}')

        return response

    def get_token_capabilities(self) -> dict[str, Any]:
        caps = """
        {
            "spaces": {
                "84af6570d3e133c7164e52594b368f22ch7d58": {
                    "name": "test01",
                    "supports": {
                        "d8190b4632eccaeaf1f2128f267c4176che3cf": {
                            "readonly": true
                        }
                    }
                },
                "03b1fdcdef49c3d013342c3a3ef9cbb8chfadd": {
                    "name": "test02",
                    "supports": {
                        "d8190b4632eccaeaf1f2128f267c4176che3cf": {
                            "readonly": true
                        }
                    }
                }
            },
            "providers": {
                "d8190b4632eccaeaf1f2128f267c4176che3cf": {
                    "domain": "dev-oneprovider-krakow.default.svc.cluster.local",
                    "version": "23.02.1",
                    "online": true
                }
            }
        }
        """
        return json.loads(caps)

    @lru_cache
    def get_space_id(self, space_name: str) -> Optional[str]:
        caps = self.get_token_capabilities()

        spaces = caps['spaces']

        for space_id in spaces:
            if spaces[space_id]['name'] == space_name:
                return space_id

        return None

    @lru_cache
    def get_space_id(self, space_name: str) -> Optional[str]:
        caps = self.get_token_capabilities()

        spaces = caps['spaces']

        for space_id in spaces:
            if spaces[space_id]['name'] == space_name:
                return space_id

        return None

    def get_file_id(self, space_name: str, file_path: str, retries: int = 3) -> str:
        try:
            return self.send_request('POST',
                                     self.op_url(space_name,
                                                 f'/lookup-file-id/{space_name}/{file_path}')).json()["fileId"]
        except requests.exceptions.ReadTimeout as e:
            if retries > 0:
                return self.get_file_id(space_name, file_path, retries - 1)
            raise e

    @lru_cache
    def get_provider_for_space(self, space_name: str) -> str:
        space_id = self.get_space_id(space_name)
        caps = self.get_token_capabilities()
        provider_ids = caps['spaces'][space_id]['supports']
        provider_id = random.choice(list(provider_ids.keys()))
        return caps['providers'][provider_id]['domain']

    def get_attributes(self, space_name: str, file_path: Optional[str] = None, file_id: Optional[str] = None):
        if file_id is None:
            if file_path is None:
                file_id = self.get_space_id(space_name)
            else:
                file_id = self.get_file_id(space_name, file_path)
        return self.send_request('GET', self.op_url(space_name, f'/data/{file_id}')).json()

    def set_attributes(self, space_name: str, file_path: str, attributes: dict):
        file_id = self.get_file_id(space_name, file_path)
        self._client.send_request('PUT', self.op_url(space_name, f'/data/{file_id}'), data=attributes)

    def readdir(self, space_name: str, file_path: str,
                limit: int = 1000, continuation_token: Optional[str] = None) -> Any:
        if file_path is None:
            # We're listing space contents
            dir_id = self.get_space_id(space_name)
        else:
            dir_id = self.get_file_id(space_name, file_path)

        return self.send_request('GET',
            self.op_url(space_name, f'/data/{dir_id}/children?attribute=size&attribute=name&attribute=type')).json()

    def list_spaces(self) -> list[str]:
        caps = self.get_token_capabilities()

        def is_space_supported(s):
            return ('supports' in s) and s['supports']

        supported_spaces = []
        for space_id in caps['spaces']:
            space = caps['spaces'][space_id]
            if is_space_supported(space):
                supported_spaces.append(space['name'])

        return supported_spaces

    def get_file_content(self, space_name: str, offset: int, size: int,
                         file_path: Optional[str] = None, file_id: Optional[str] = None):
        if file_id is None:
            file_id = self.get_file_id(space_name, file_path)
        headers = {'Range': f'bytes={offset}-{offset+size-1}'}
        return self.send_request('GET', self.op_url(space_name, f'/data/{file_id}/content'), headers=headers).content

    def put_file_content(self, space_name: str, file_id: str, offset: Optional[int], data: bytes):
        headers = {'Content-type': 'application/octet-stream'}
        path_url = f'/data/{file_id}/content'
        if offset is not None:
            path_url += f'?offset={offset}'
        self.send_request('PUT', self.op_url(space_name, path_url), data=data, headers=headers)

    def create_file(self, space_name: str, file_path: str, file_type: str = 'REG',
                    create_parents: bool = False, mode: Optional[int] = None) -> str:
        space_id = self.get_space_id(space_name)
        url_path = f'/data/{space_id}/path/{file_path}?type={file_type}&create_parents={str(create_parents).lower()}'
        if mode:
            url_path += f'&mode={int(mode, 8)}'
        return self.send_request('PUT',
                                 self.op_url(space_name, url_path), b'').json()['fileId']

    def remove(self, space_name: str, file_path: str):
        space_id = self.get_space_id(space_name)
        attr = self.get_attributes(space_name, file_path)

        self.send_request('DELETE', self.op_url(space_name, f'/data/{space_id}/path/{file_path}'))

    def move(self, src_space_name: str, src_file_path: str, dst_space_name: str, dst_file_path: str):
        # First create the target directory (this assumes that the src_file_path already exists)
        headers = {"X-CDMI-Specification-Version": "1.1.1",
                   "Content-type": "application/cdmi-object"}

        url = f'https://{self.get_provider_for_space(dst_space_name)}/cdmi/{dst_space_name}/{dst_file_path}'

        data = {'move': f'{src_space_name}/{src_file_path}'}

        self.send_request('PUT', url, data=json.dumps(data), headers=headers)
