#
# Copyright 2024 Tabs Data Inc.
#

import json
import logging
import os
import time
from http import HTTPStatus
from typing import List
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from tabsdata._utils.tableframe._constants import PYTEST_CONTEXT_ACTIVE

DEFAULT_APISERVER_PORT = "2457"
HTTP_PROTOCOL = "http://"
HTTPS_PROTOCOL = "https://"
PORT_SEPARATOR = ":"

BASE_API_URL_V1 = "/api/v1"
BASE_API_URL = BASE_API_URL_V1

DEFAULT_TABSDATA_DIRECTORY = os.path.join(os.path.expanduser("~"), ".tabsdata")
DEFAULT_TABSDATA_CERTIFICATE_FOLDER = os.path.join(
    DEFAULT_TABSDATA_DIRECTORY, "client", "https"
)


CONNECTION_TIMEOUT = 60 * 5
READ_TIMEOUT = 60 * 5
MIN_CONNECTIONS = 2
MAX_CONNECTIONS = 8
REFRESH_BUFFER_IN_SECONDS = 300  # 5 minutes

DEFAULT_LIST_ENDPOINT_PARAMETERS = [
    "len",
    "filter",
    "order_by",
    "pagination_id",
    "next",
]

logger = logging.getLogger(__name__)


def configure_request_connection_pool():
    http_retries_delay = 5
    http_attempts = 12

    class ConstantWaitRetry(Retry):
        def sleep(self, _response=None):
            time.sleep(http_retries_delay)

    default_retry_strategy = ConstantWaitRetry(
        total=http_attempts,
        connect=http_attempts,
        read=0,
        redirect=http_attempts,
        status=0,
        other=0,
        allowed_methods=None,
        status_forcelist=None,
        raise_on_status=False,
        raise_on_redirect=False,
        respect_retry_after_header=False,
    )

    get_retry_strategy = ConstantWaitRetry(
        total=http_attempts,
        connect=http_attempts,
        read=http_attempts,
        redirect=http_attempts,
        status=http_attempts,
        other=http_attempts,
        allowed_methods={"GET"},
        status_forcelist=range(500, 600),
        raise_on_status=False,
        raise_on_redirect=False,
    )

    default_retry_adapter = HTTPAdapter(
        max_retries=default_retry_strategy,
        pool_connections=MIN_CONNECTIONS,
        pool_maxsize=MAX_CONNECTIONS,
        pool_block=True,
    )

    get_retry_adapter = HTTPAdapter(
        max_retries=get_retry_strategy,
        pool_connections=MIN_CONNECTIONS,
        pool_maxsize=MAX_CONNECTIONS,
        pool_block=True,
    )

    default_session = requests.Session()
    default_session.mount("http://", default_retry_adapter)
    default_session.mount("https://", default_retry_adapter)

    get_session = requests.Session()
    get_session.mount("http://", get_retry_adapter)
    get_session.mount("https://", get_retry_adapter)

    return default_session, get_session


DEFAULT_HTTP_SESSION, GET_HTTP_SESSION = configure_request_connection_pool()


def _obtain_certificate_file_path(url: str) -> str:
    try:
        parsed_url = urlparse(url)
        if parsed_url.port is None:
            port = DEFAULT_APISERVER_PORT
        else:
            port = str(parsed_url.port)
        file_name = (
            f"{parsed_url.hostname.replace('.', '_')}_{port.replace('.', '_')}_cert.pem"
        )
        full_path = os.path.join(DEFAULT_TABSDATA_CERTIFICATE_FOLDER, file_name)
        return full_path
    except Exception as e:
        raise ValueError(
            f"Invalid URL '{url}' for obtaining certificate file path. Please ensure "
            f"that it is a valid URL, with a hostname and a port: {e}"
        )


class APIServerError(Exception):

    def __init__(self, dictionary: dict):
        self.code = dictionary.get("code")
        self.error = dictionary.get("error")
        self.error_description = dictionary.get("error_description")
        super().__init__(
            self.error_description if self.error_description else "Unknown error"
        )


def process_url(url: str) -> str:
    """
    A helper function to process the url string. It adds the protocol and the
        default port if missing
    """
    if not url.startswith(HTTP_PROTOCOL) and not url.startswith(HTTPS_PROTOCOL):
        url = HTTP_PROTOCOL + url
    parsed_url = urlparse(url)
    if not parsed_url.port:
        url = url + PORT_SEPARATOR + DEFAULT_APISERVER_PORT
    if not parsed_url.path.endswith(BASE_API_URL):
        url = url + BASE_API_URL
    return url


class APIServer:

    def __init__(self, url: str, credentials_file: str = None):
        url = process_url(url)
        self.url = url
        self.bearer_token = None
        self.refresh_token = None
        self.token_type = None
        self.expires_in = None
        self.expiration_time = None
        self.credentials_file = credentials_file

    @property
    def certificate_file(self) -> str | None:
        if not self.url.startswith(HTTPS_PROTOCOL):
            return None
        full_path = _obtain_certificate_file_path(self.url)
        if os.path.isfile(full_path):
            return full_path
        else:
            return None

    def _refresh_token_if_needed(self):
        if not self.refresh_token or not self.expiration_time:
            # No refresh token or expiration time, no need to refresh
            return
        current_time = time.time()
        already_expired = self.expiration_time < current_time
        near_expiration = (
            current_time > self.expiration_time - REFRESH_BUFFER_IN_SECONDS
        )
        if near_expiration and not already_expired:
            logger.debug("Refreshing authentication token")
            self.authentication_refresh()

    @property
    def authentication_header(self):
        return (
            {"Authorization": f"Bearer {self.bearer_token}"}
            if self.bearer_token
            else {}
        )

    def get(self, path, params=None, refresh_if_needed=True):
        headers = {}

        if refresh_if_needed:
            self._refresh_token_if_needed()

        headers.update(self.authentication_header)

        if os.environ.get(PYTEST_CONTEXT_ACTIVE) is None:
            return requests.get(
                self.url + path,
                headers=headers,
                timeout=(CONNECTION_TIMEOUT, READ_TIMEOUT),
                params=params,
                verify=self.certificate_file,
            )
        else:
            with GET_HTTP_SESSION as session:
                return session.get(
                    self.url + path,
                    headers=headers,
                    timeout=(CONNECTION_TIMEOUT, READ_TIMEOUT),
                    params=params,
                    verify=self.certificate_file,
                )

    def post(
        self,
        path,
        data=None,
        json=None,
        params=None,
        refresh_if_needed=True,
        content_type=None,
    ):
        headers = {}

        if refresh_if_needed:
            self._refresh_token_if_needed()

        headers.update(self.authentication_header)
        if content_type:
            headers.update({"Content-Type": content_type})

        if os.environ.get(PYTEST_CONTEXT_ACTIVE) is None:
            return requests.post(
                self.url + path,
                headers=headers,
                timeout=(CONNECTION_TIMEOUT, READ_TIMEOUT),
                json=json,
                data=data,
                params=params,
                verify=self.certificate_file,
            )
        else:
            with DEFAULT_HTTP_SESSION as session:
                return session.post(
                    self.url + path,
                    headers=headers,
                    timeout=(CONNECTION_TIMEOUT, READ_TIMEOUT),
                    json=json,
                    data=data,
                    params=params,
                    verify=self.certificate_file,
                )

    def post_binary(self, path, data, refresh_if_needed=True):
        headers = {}

        if refresh_if_needed:
            self._refresh_token_if_needed()

        headers.update(self.authentication_header)
        headers.update({"Content-Type": "application/octet-stream"})

        if os.environ.get(PYTEST_CONTEXT_ACTIVE) is None:
            return requests.post(
                self.url + path,
                headers=headers,
                timeout=(CONNECTION_TIMEOUT, READ_TIMEOUT),
                data=data,
                verify=self.certificate_file,
            )
        else:
            with DEFAULT_HTTP_SESSION as session:
                return session.post(
                    self.url + path,
                    headers=headers,
                    timeout=(CONNECTION_TIMEOUT, READ_TIMEOUT),
                    data=data,
                    verify=self.certificate_file,
                )

    def delete(self, path, refresh_if_needed=True):
        headers = {}

        if refresh_if_needed:
            self._refresh_token_if_needed()

        headers.update(self.authentication_header)

        if os.environ.get(PYTEST_CONTEXT_ACTIVE) is None:
            return requests.delete(
                self.url + path,
                headers=headers,
                timeout=(CONNECTION_TIMEOUT, READ_TIMEOUT),
                verify=self.certificate_file,
            )
        else:
            with DEFAULT_HTTP_SESSION as session:
                return session.delete(
                    self.url + path,
                    headers=headers,
                    timeout=(CONNECTION_TIMEOUT, READ_TIMEOUT),
                    verify=self.certificate_file,
                )

    def _store_in_file(self, file_path: str):
        with open(file_path, "w") as file:
            json.dump(
                {
                    "url": self.url,
                    "bearer_token": self.bearer_token,
                    "refresh_token": self.refresh_token,
                    "token_type": self.token_type,
                    "expires_in": self.expires_in,
                    "expiration_time": self.expiration_time,
                },
                file,
            )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.url!r})"

    def __str__(self):
        return self.url

    def __eq__(self, other):
        if not isinstance(other, APIServer):
            return False
        return self.url == other.url

    def raise_for_status_or_return(
        self, raise_for_status: bool, response: requests.Response
    ) -> requests.Response:
        if raise_for_status:
            return self.raise_for_status(response)
        else:
            return response

    def authentication_info(self, raise_for_status: bool = True):
        endpoint = "/auth/info"
        response = self.get(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def authentication_login(self, name: str, password: str, role: str = None):
        endpoint = "/auth/login"
        data = self.get_params_dict(
            ["name", "password", "role"], [name, password, role]
        )
        time_of_request = time.time()
        response = self.post(endpoint, json=data, refresh_if_needed=False)
        if HTTPStatus(response.status_code).is_success:
            self.bearer_token = response.json()["access_token"]
            self.refresh_token = response.json()["refresh_token"]
            self.token_type = response.json()["token_type"]
            self.expires_in = response.json()["expires_in"]
            self.expiration_time = time_of_request + self.expires_in
            if self.credentials_file:
                self._store_in_file(self.credentials_file)
            return response
        else:
            raise APIServerError(response.json())

    def authentication_logout(self, raise_for_status: bool = True):
        endpoint = "/auth/logout"
        response = self.post(endpoint, json={})
        if HTTPStatus(response.status_code).is_success:
            self.bearer_token = None
            self.refresh_token = None
            self.token_type = None
            self.expires_in = None
            self.expiration_time = None
        if self.credentials_file:
            try:
                os.remove(self.credentials_file)
            except FileNotFoundError:
                pass
        return self.raise_for_status_or_return(raise_for_status, response)

    def authentication_password_change(
        self,
        name: str,
        old_password: str,
        new_password: str,
        raise_for_status: bool = True,
    ):
        endpoint = "/auth/password_change"
        json = {
            "name": name,
            "old_password": old_password,
            "new_password": new_password,
        }
        response = self.post(endpoint, json=json, refresh_if_needed=False)
        return self.raise_for_status_or_return(raise_for_status, response)

    def authentication_refresh(self):
        endpoint = "/auth/refresh"
        data = {"refresh_token": self.refresh_token, "grant_type": "refresh_token"}
        time_of_request = time.time()
        response = self.post(
            endpoint,
            data=data,
            refresh_if_needed=False,
            content_type="application/x-www-form-urlencoded",
        )
        if HTTPStatus(response.status_code).is_success:
            self.bearer_token = response.json()["access_token"]
            self.refresh_token = response.json()["refresh_token"]
            self.token_type = response.json()["token_type"]
            self.expires_in = response.json()["expires_in"]
            self.expiration_time = time_of_request + self.expires_in
            if self.credentials_file:
                self._store_in_file(self.credentials_file)
            return response
        else:
            raise APIServerError(response.json())

    def authentication_role_change(self, role: str):
        endpoint = "/auth/role_change"
        data = {"role": role}
        time_of_request = time.time()
        response = self.post(endpoint, json=data)
        if HTTPStatus(response.status_code).is_success:
            self.bearer_token = response.json()["access_token"]
            self.refresh_token = response.json()["refresh_token"]
            self.token_type = response.json()["token_type"]
            self.expires_in = response.json()["expires_in"]
            self.expiration_time = time_of_request + self.expires_in
            if self.credentials_file:
                self._store_in_file(self.credentials_file)
            return response
        else:
            raise APIServerError(response.json())

    def authz_inter_coll_perm_create(
        self, collection: str, to_collection: str, raise_for_status: bool = True
    ):
        endpoint = f"/collections/{collection}/inter-collection-permissions"

        data = {"to_collection": to_collection}
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def authz_inter_coll_perm_delete(
        self, collection: str, permission: str, raise_for_status: bool = True
    ):
        endpoint = (
            f"/collections/{collection}/inter-collection-permissions/{permission}"
        )

        response = self.delete(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def authz_inter_coll_perm_list(
        self,
        collection: str,
        request_len: int = None,
        request_filter: List[str] | str = None,
        order_by: str = None,
        pagination_id: str = None,
        next_step: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/collections/{collection}/inter-collection-permissions"

        params = self.get_params_dict(
            DEFAULT_LIST_ENDPOINT_PARAMETERS,
            [request_len, request_filter, order_by, pagination_id, next_step],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def collection_create(
        self, name: str, description: str, raise_for_status: bool = True
    ):
        endpoint = "/collections"
        data = {"name": name, "description": description}
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def collection_delete(self, collection_name: str, raise_for_status: bool = True):
        endpoint = f"/collections/{collection_name}"
        response = self.delete(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def collection_get_by_name(
        self, collection_name: str, raise_for_status: bool = True
    ):
        endpoint = f"/collections/{collection_name}"
        response = self.get(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def collection_list(
        self,
        request_len: int = None,
        request_filter: List[str] | str = None,
        order_by: str = None,
        pagination_id: str = None,
        next_step: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = "/collections"

        params = self.get_params_dict(
            DEFAULT_LIST_ENDPOINT_PARAMETERS,
            [request_len, request_filter, order_by, pagination_id, next_step],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def collection_update(
        self,
        collection_name: str,
        new_collection_name: str = None,
        description: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/collections/{collection_name}"
        data = self.get_params_dict(
            ["name", "description"], [new_collection_name, description]
        )
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def dataversion_list(
        self,
        collection_name: str,
        table_name: str,
        request_len: int = None,
        request_filter: List[str] | str = None,
        order_by: str = None,
        pagination_id: str = None,
        next_step: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/collections/{collection_name}/tables/{table_name}/data-versions"

        params = self.get_params_dict(
            DEFAULT_LIST_ENDPOINT_PARAMETERS,
            [request_len, request_filter, order_by, pagination_id, next_step],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def execution_cancel(self, execution_id: str, raise_for_status: bool = True):
        endpoint = f"/executions/{execution_id}/cancel"
        response = self.post(endpoint, json={})
        return self.raise_for_status_or_return(raise_for_status, response)

    def execution_list(
        self,
        request_len: int = None,
        request_filter: List[str] | str = None,
        order_by: str = None,
        pagination_id: str = None,
        next_step: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = "/executions"

        params = self.get_params_dict(
            DEFAULT_LIST_ENDPOINT_PARAMETERS,
            [request_len, request_filter, order_by, pagination_id, next_step],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def execution_read_function_run(
        self,
        collection_name: str,
        function_name_or_version_id: str,
        execution_id: str,
        raise_for_status: bool = True,
    ):
        endpoint = (
            f"/collections/{collection_name}/functions/"
            f"{function_name_or_version_id}/executions/{execution_id}"
        )
        response = self.get(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def execution_recover(self, execution_id: str, raise_for_status: bool = True):
        endpoint = f"/executions/{execution_id}/recover"
        response = self.post(endpoint, json={})
        return self.raise_for_status_or_return(raise_for_status, response)

    def function_register(
        self,
        collection_name: str,
        function_name: str,
        description: str,
        tables: list[str],
        dependencies: list[str],
        trigger_by: list[str],
        function_snippet: str,
        bundle_id: str,
        decorator: str,
        runtime_values: str,
        reuse_frozen_tables: bool,
        plugin_name: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/collections/{collection_name}/functions"

        names = [
            "name",
            "description",
            "bundle_id",
            "snippet",
            "decorator",
            "dependencies",
            "triggers",
            "tables",
            "reuse_frozen_tables",
            "runtime_values",
            "connector",
        ]
        values = [
            function_name,
            description,
            bundle_id,
            function_snippet,
            decorator,
            dependencies,
            trigger_by,
            tables,
            reuse_frozen_tables,
            runtime_values,
            plugin_name,
        ]
        data = self.get_params_dict(names, values)
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def function_delete(
        self, collection_name: str, function_name: str, raise_for_status: bool = True
    ):
        endpoint = f"/collections/{collection_name}/functions/{function_name}"
        response = self.delete(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def function_execute(
        self,
        collection_name: str,
        function_name: str,
        execution_name=None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/collections/{collection_name}/functions/{function_name}/execute"
        data = self.get_params_dict(["name"], [execution_name])
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def function_get(
        self, collection_name: str, function_name: str, raise_for_status: bool = True
    ):
        endpoint = f"/collections/{collection_name}/functions/{function_name}"
        response = self.get(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def function_in_collection_list(
        self,
        collection_name: str,
        request_len: int = None,
        request_filter: List[str] | str = None,
        order_by: str = None,
        pagination_id: str = None,
        next_step: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/collections/{collection_name}/functions"

        params = self.get_params_dict(
            DEFAULT_LIST_ENDPOINT_PARAMETERS,
            [request_len, request_filter, order_by, pagination_id, next_step],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def function_list_history(
        self,
        collection_name: str,
        function_name: str,
        request_len: int = None,
        request_filter: List[str] | str = None,
        order_by: str = None,
        pagination_id: str = None,
        next_step: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/collections/{collection_name}/functions/{function_name}/history"

        params = self.get_params_dict(
            DEFAULT_LIST_ENDPOINT_PARAMETERS,
            [request_len, request_filter, order_by, pagination_id, next_step],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def function_list_runs(
        self,
        request_len: int = None,
        request_filter: List[str] | str = None,
        order_by: str = None,
        pagination_id: str = None,
        next_step: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = "/function_runs"

        params = self.get_params_dict(
            DEFAULT_LIST_ENDPOINT_PARAMETERS,
            [request_len, request_filter, order_by, pagination_id, next_step],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def function_update(
        self,
        collection_name: str,
        function_name: str,
        new_function_name: str = None,
        description: str = None,
        bundle_id: str = None,
        tables: list[str] = None,
        dependencies: list[str] = None,
        trigger_by: list[str] = None,
        function_snippet: str = None,
        decorator: str = None,
        runtime_values: str = None,
        reuse_frozen_tables: bool = None,
        plugin_name: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/collections/{collection_name}/functions/{function_name}"

        data = self.get_params_dict(
            [
                "name",
                "description",
                "bundle_id",
                "tables",
                "dependencies",
                "triggers",
                "snippet",
                "decorator",
                "runtime_values",
                "reuse_frozen_tables",
                "connector",
            ],
            [
                new_function_name,
                description,
                bundle_id,
                tables,
                dependencies,
                trigger_by,
                function_snippet,
                decorator,  # Either P, S or T
                runtime_values,
                reuse_frozen_tables,
                plugin_name,
            ],
        )
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def function_upload_bundle(
        self,
        collection_name: str,
        bundle: bytes,
        raise_for_status: bool = True,
    ):
        endpoint = f"/collections/{collection_name}/function-bundle-upload"
        response = self.post_binary(endpoint, data=bundle)
        return self.raise_for_status_or_return(raise_for_status, response)

    def role_create(
        self, name: str, description: str = None, raise_for_status: bool = True
    ):
        endpoint = "/roles"
        data = self.get_params_dict(["name", "description"], [name, description])
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def role_delete(self, name: str, raise_for_status: bool = True):
        endpoint = f"/roles/{name}"
        response = self.delete(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def role_get_by_name(self, name: str, raise_for_status: bool = True):
        endpoint = f"/roles/{name}"
        response = self.get(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def role_list(
        self,
        request_len: int = None,
        request_filter: List[str] | str = None,
        order_by: str = None,
        pagination_id: str = None,
        next_step: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = "/roles"
        params = self.get_params_dict(
            DEFAULT_LIST_ENDPOINT_PARAMETERS,
            [request_len, request_filter, order_by, pagination_id, next_step],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def role_update(
        self,
        name: str,
        new_name: str = None,
        new_description: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/roles/{name}"
        data = self.get_params_dict(
            ["name", "description"], [new_name, new_description]
        )
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def role_permission_create(
        self,
        role: str,
        permission_type: str,
        entity_name: str | None = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/roles/{role}/permissions"

        data = self.get_params_dict(
            ["permission_type", "entity_name"], [permission_type, entity_name]
        )
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def role_permission_delete(
        self,
        role: str,
        permission: str,
        raise_for_status: bool = True,
    ):
        endpoint = f"/roles/{role}/permissions/{permission}"

        response = self.delete(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def role_permission_list(
        self,
        role: str,
        request_len: int = None,
        request_filter: List[str] | str = None,
        order_by: str = None,
        pagination_id: str = None,
        next_step: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/roles/{role}/permissions"
        params = self.get_params_dict(
            DEFAULT_LIST_ENDPOINT_PARAMETERS,
            [request_len, request_filter, order_by, pagination_id, next_step],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def role_user_add(self, role: str, user: str, raise_for_status: bool = True):

        endpoint = f"/roles/{role}/users"

        data = {"user": user}
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def role_user_delete(self, role: str, user: str, raise_for_status: bool = True):
        """
        Delete a user from a specific role.
        """
        endpoint = f"/roles/{role}/users/{user}"

        response = self.delete(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def role_user_list(
        self,
        role: str,
        request_len: int = None,
        request_filter: List[str] | str = None,
        order_by: str = None,
        pagination_id: str = None,
        next_step: str = None,
        raise_for_status: bool = True,
    ):
        """
        List users associated with a specific role.
        """
        endpoint = f"/roles/{role}/users"
        params = self.get_params_dict(
            DEFAULT_LIST_ENDPOINT_PARAMETERS,
            [request_len, request_filter, order_by, pagination_id, next_step],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def role_user_read(self, role: str, user: str, raise_for_status: bool = True):
        endpoint = f"/roles/{role}/users/{user}"

        response = self.get(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def runtime_info_get(self, raise_for_status: bool = True):
        endpoint = "/runtime-info"
        response = self.get(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def status_get(self, raise_for_status: bool = True):
        endpoint = "/status"
        response = self.get(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def table_delete(
        self,
        collection_name: str,
        table_name: str,
        raise_for_status: bool = True,
    ):
        endpoint = f"/collections/{collection_name}/tables/{table_name}"
        response = self.delete(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def table_download(
        self,
        collection_name: str,
        table_name: str,
        at: int = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/collections/{collection_name}/tables/{table_name}/download"
        params = self.get_params_dict(
            ["at"],
            [at],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def table_get_sample(
        self,
        collection_name: str,
        table_name: str,
        at: int = None,
        offset: int = None,
        len: int = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/collections/{collection_name}/tables/{table_name}/sample"
        params = self.get_params_dict(
            [
                "at",
                "offset",
                "len",
            ],
            [at, offset, len],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def table_get_schema(
        self,
        collection_name: str,
        table_name: str,
        at: int = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/collections/{collection_name}/tables/{table_name}/schema"
        params = self.get_params_dict(
            ["at"],
            [at],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def table_list(
        self,
        collection_name: str,
        at: int = None,
        request_len: int = None,
        request_filter: List[str] | str = None,
        order_by: str = None,
        pagination_id: str = None,
        next_step: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/collections/{collection_name}/tables"
        names = ["at"] + DEFAULT_LIST_ENDPOINT_PARAMETERS
        values = [at, request_len, request_filter, order_by, pagination_id, next_step]
        params = self.get_params_dict(names, values)
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def transaction_cancel(self, transaction_id: str, raise_for_status: bool = True):
        endpoint = f"/transactions/{transaction_id}/cancel"
        response = self.post(endpoint, json={})
        return self.raise_for_status_or_return(raise_for_status, response)

    def transaction_list(
        self,
        request_len: int = None,
        request_filter: List[str] | str = None,
        order_by: str = None,
        pagination_id: str = None,
        next_step: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = "/transactions"

        params = self.get_params_dict(
            DEFAULT_LIST_ENDPOINT_PARAMETERS,
            [request_len, request_filter, order_by, pagination_id, next_step],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def transaction_recover(self, transaction_id: str, raise_for_status: bool = True):
        endpoint = f"/transactions/{transaction_id}/recover"
        response = self.post(endpoint, json={})
        return self.raise_for_status_or_return(raise_for_status, response)

    def users_create(
        self,
        name: str,
        full_name: str,
        email: str,
        password: str,
        enabled: bool,
        raise_for_status: bool = True,
    ):
        endpoint = "/users"

        data = self.get_params_dict(
            ["name", "full_name", "email", "password", "enabled"],
            [name, full_name, email, password, enabled],
        )
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def users_delete(self, name: str, raise_for_status: bool = True):
        endpoint = f"/users/{name}"
        response = self.delete(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def users_get_by_name(self, name: str, raise_for_status: bool = True):
        endpoint = f"/users/{name}"
        response = self.get(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def users_list(
        self,
        request_len: int = None,
        request_filter: List[str] | str = None,
        order_by: str = None,
        pagination_id: str = None,
        next_step: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = "/users"
        params = self.get_params_dict(
            DEFAULT_LIST_ENDPOINT_PARAMETERS,
            [request_len, request_filter, order_by, pagination_id, next_step],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def users_update(
        self,
        name: str,
        full_name: str = None,
        email: str = None,
        password: str = None,
        enabled: bool = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/users/{name}"

        data = self.get_params_dict(
            ["full_name", "email", "enabled", "password"],
            [full_name, email, enabled, password],
        )
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def worker_log(self, message_id: str, raise_for_status: bool = True):
        endpoint = f"/workers/{message_id}/logs"

        response = self.get(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def workers_list(
        self,
        request_len: int = None,
        request_filter: List[str] | str = None,
        order_by: str = None,
        pagination_id: str = None,
        next_step: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = "/workers"

        params = self.get_params_dict(
            DEFAULT_LIST_ENDPOINT_PARAMETERS,
            [request_len, request_filter, order_by, pagination_id, next_step],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    @staticmethod
    def raise_for_status(response: requests.Response):
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise APIServerError(response.json())
        else:
            return response

    @staticmethod
    def get_params_dict(names: list, values: list) -> dict:
        return {name: value for name, value in zip(names, values) if value is not None}


def obtain_connection(
    url: str,
    name: str = None,
    password: str = None,
    role: str = None,
    credentials_file: str = None,
) -> APIServer:
    connection = APIServer(url, credentials_file=credentials_file)
    if name and password:
        connection.authentication_login(name, password, role=role)
    return connection
