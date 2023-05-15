import random
from functools import lru_cache

import requests
import os
from enum import Enum


class FileType(Enum):
    REG = 0
    DIR = 1
    LNK = 2


class OnedataRESTError(Exception):

    def __init__(self, response):
        self.http_code = response.status_code
        self.error = None
        self.description = None

        #print(f"ERROR: {response.status_code} - {response.json()}")
        try:
            self.error = response.json()['error']
            self.description = response.json()['description']
        except:
            pass


class OnedataFileClient:
    """
    ...
    """
    _session = None
    _onezone_host = None
    _token = None
    _timeout = 30

    def __init__(self, onezone_host, token):
        self._onezone_host = onezone_host
        self._token = token
        self._session = requests.Session()

    def oz_url(self, path):
        return f'https://{self._onezone_host}/api/v3/onezone{path}'

    def op_url(self, space_name, path):
        return f'https://{self.get_provider_for_space(space_name)}/api/v3/oneprovider{path}'

    def send_request(self, method, url, data=None, headers={}):
        #print(f"{method} {url}")

        headers['X-Auth-Token'] = self._token
        if not 'Content-type' in headers:
            headers['Content-type'] = 'application/json'

        req = requests.Request(method, url, data=data, headers=headers)
        prepared = self._session.prepare_request(req)

        response = self._session.send(prepared, timeout=self._timeout, verify=False)

        if not response.ok:
            print(response.text)
            raise OnedataRESTError(response)

        return response

    def get_space_details(self, space_id):
        return self.send_request('GET', self.oz_url(f'/user/effective_spaces/{space_id}')).json()

    def get_provider_details(self, provider_id):
        return self.send_request('GET', self.oz_url(f'/providers/{provider_id}')).json()

    @lru_cache
    def get_space_id(self, space_name):
        spaces = self.list_spaces_ids()['spaces']

        for space_id in spaces:
            space_details = self.get_space_details(space_id)

            if space_details['name'] == space_name:
                return space_id

        return None

    def get_file_id(self, space_name, file_path):
        return self.send_request('POST',
                                 self.op_url(space_name, f'/lookup-file-id/{space_name}/{file_path}')).json()["fileId"]

    @lru_cache
    def get_provider_for_space(self, space_name):
        provider_ids = self.get_space_details(self.get_space_id(space_name))['providers']
        provider_id = random.choice(list(provider_ids.keys()))
        return self.get_provider_details(provider_id)['domain']

    def get_attributes(self, space_name, file_path=None, file_id=None):
        if file_id is None:
            file_id = self.get_file_id(space_name, file_path)
        return self.send_request('GET', self.op_url(space_name, f'/data/{file_id}')).json()

    def set_attributes(self, space_name, file_path, attributes):
        file_id = self.get_file_id(space_name, file_path)
        self._client.send_request('PUT', self.op_url(space_name, f'/data/{file_id}'), data=attributes)

    def readdir(self, space_name, file_path, limit=1000, continuation_token=None):
        if file_path is None:
            # We're listing space contents
            dir_id = self.get_space_id(space_name)
        else:
            dir_id = self.get_file_id(space_name, file_path)

        return self.send_request('GET',
            self.op_url(space_name, f'/data/{dir_id}/children?attribute=size&attribute=name&attribute=type')).json()

    def list_spaces_ids(self):
        return self.send_request('GET', self.oz_url('/user/effective_spaces')).json()

    def list_spaces(self):
        spaces = self.list_spaces_ids()
        return list(map(lambda s: self.get_space_details(s)['name'], spaces['spaces']))

    def get_file_content(self, space_name, offset, size, file_path=None, file_id=None):
        if file_id is None:
            file_id = self.get_file_id(space_name, file_path)
        headers = {'Range': f'bytes={offset}-{offset+size-1}'}
        return self.send_request('GET', self.op_url(space_name, f'/data/{file_id}/content'), headers=headers).content

    def put_file_content(self, space_name, file_id, offset, data):
        headers = {'Content-type': 'application/octet-stream'}
        self.send_request('PUT',
                          self.op_url(space_name, f'/data/{file_id}/content?offset={offset}'),
                          data=data, headers=headers)

    def create_file(self, space_name, file_path):
        space_id = self.get_space_id(space_name)
        return self.send_request('PUT',
                                 self.op_url(space_name, f'/data/{space_id}/path/{file_path}'), b'').json()['fileId']

    def remove(self, space_name, file_path):
        space_id = self.get_space_id(space_name)
        attr = self.get_file_attributes(space_name, file_path)

        self.send_request('DELETE', self.op_url(space_name, f'/data/{space_id}/path/{file_path}'))




