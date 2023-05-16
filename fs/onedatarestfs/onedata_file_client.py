import random
from functools import lru_cache

import requests
import os
from enum import Enum
import logging
import json

def trace_requests_messages():
    import http.client as http_client
    http_client.HTTPConnection.debuglevel = 1

    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True


#trace_requests_messages()

class OnedataRESTError(Exception):

    def __init__(self, response):
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

    def __repr__(self):
        """Return unique representation of the OnedataRESTFS instance."""

        return self.__str__()

    def __str__(self):
        """Return unique representation of the OnedataRESTFS instance."""

        return "<onedataresterror '{} {}:{}'>".format(
            self.http_code, self.error_category, self.description
        )

class OnedataFileClient:
    """
    ...
    """
    _session = None
    _onezone_host = None
    _token = None
    _timeout = 5

    def __init__(self, onezone_host, token):
        self._onezone_host = onezone_host
        self._token = token
        self._session = requests.Session()

    def oz_url(self, path):
        return f'https://{self._onezone_host}/api/v3/onezone{path}'

    def op_url(self, space_name, path):
        return f'https://{self.get_provider_for_space(space_name)}/api/v3/oneprovider{path}'

    def send_request(self, method, url, data=None, headers={}):
        headers['X-Auth-Token'] = self._token
        if not 'Content-type' in headers:
            headers['Content-type'] = 'application/json'

        req = requests.Request(method, url, data=data, headers=headers)
        prepared = self._session.prepare_request(req)

        response = self._session.send(prepared, timeout=self._timeout, verify=False)

        if not response.ok:
            # print(f"ERROR: {method} {url} {data}")
            # print(response.text)
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
            if file_path is None:
                file_id = self.get_space_id(space_name)
            else:
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
        path_url = f'/data/{file_id}/content'
        if offset is not None:
            path_url += f'?offset={offset}'
        self.send_request('PUT', self.op_url(space_name, path_url), data=data, headers=headers)

    def create_file(self, space_name, file_path, file_type='REG', create_parents=False, mode=None):
        space_id = self.get_space_id(space_name)
        url_path = f'/data/{space_id}/path/{file_path}?type={file_type}&create_parents={str(create_parents).lower()}'
        if mode:
            url_path += f'&mode={int(mode, 8)}'
        return self.send_request('PUT',
                                 self.op_url(space_name, url_path), b'').json()['fileId']

    def remove(self, space_name, file_path):
        space_id = self.get_space_id(space_name)
        attr = self.get_attributes(space_name, file_path)

        self.send_request('DELETE', self.op_url(space_name, f'/data/{space_id}/path/{file_path}'))

    def move(self, src_space_name, src_file_path, dst_space_name, dst_file_path):
        # First create the target directory (this assumes that the src_file_path already exists)

        #dst_dirname = os.path.dirname(dst_file_path)
        #if dst_dirname != '' and dst_dirname != '/' and dst_dirname != '.':
        #    dst_file_id = self.create_file(dst_space_name, dst_dirname, 'DIR')

        headers = {"X-CDMI-Specification-Version": "1.1.1",
                   "Content-type": "application/cdmi-object"}

        url = f'https://{self.get_provider_for_space(dst_space_name)}/cdmi/{dst_space_name}/{dst_file_path}'

        data = {'move': f'{src_space_name}/{src_file_path}'}

        self.send_request('PUT', url, data=json.dumps(data), headers=headers)






