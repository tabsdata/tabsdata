#
# Copyright 2024 Tabs Data Inc.
#

import json
import logging
import os
import time
from typing import List
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from tabsdata.utils.tableframe._constants import PYTEST_CONTEXT_ACTIVE

DEFAULT_APISERVER_PORT = "2457"
HTTP_PROTOCOL = "http://"
PORT_SEPARATOR = ":"

BASE_API_URL_V1 = "/api/v1"
BASE_API_URL = BASE_API_URL_V1

CONNECTION_TIMEOUT = 60 * 5
READ_TIMEOUT = 60 * 5
MIN_CONNECTIONS = 2
MAX_CONNECTIONS = 8
REFRESH_BUFFER_IN_SECONDS = 300  # 5 minutes

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
    if not url.startswith(HTTP_PROTOCOL):
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
            )
        else:
            return GET_HTTP_SESSION.get(
                self.url + path,
                headers=headers,
                timeout=(CONNECTION_TIMEOUT, READ_TIMEOUT),
                params=params,
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
            )
        else:
            return DEFAULT_HTTP_SESSION.post(
                self.url + path,
                headers=headers,
                timeout=(CONNECTION_TIMEOUT, READ_TIMEOUT),
                json=json,
                data=data,
                params=params,
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
            )
        else:
            return DEFAULT_HTTP_SESSION.post(
                self.url + path,
                headers=headers,
                timeout=(CONNECTION_TIMEOUT, READ_TIMEOUT),
                data=data,
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
            )
        else:
            return DEFAULT_HTTP_SESSION.delete(
                self.url + path,
                headers=headers,
                timeout=(CONNECTION_TIMEOUT, READ_TIMEOUT),
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
        if response.status_code == 200:
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
        if response.status_code == 200:
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
        if response.status_code == 200:
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
        if response.status_code == 200:
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

    def collection_create(
        self, name: str, description: str, raise_for_status: bool = True
    ):
        # Aleix: Updated
        endpoint = "/collections"
        data = {"name": name, "description": description}
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def collection_delete(self, collection_name: str, raise_for_status: bool = True):
        # Aleix: Updated
        endpoint = f"/collections/{collection_name}"
        response = self.delete(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def collection_get_by_name(
        self, collection_name: str, raise_for_status: bool = True
    ):
        # Aleix: Updated
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
            ["len", "filter", "order_by", "pagination_id", "next"],
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
        # Aleix: Updated
        endpoint = f"/collections/{collection_name}"
        data = self.get_params_dict(
            ["name", "description"], [new_collection_name, description]
        )
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def commit_list(
        self,
        offset: int = None,
        len: int = None,
        filter: str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = "/commits"

        params = self.get_params_dict(
            ["offset", "len", "filter", "order_by"], [offset, len, filter, order_by]
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def dataversion_list(
        self,
        collection_name: str,
        function_name: str,
        offset: int = None,
        len: int = None,
        filter: str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/collections/{collection_name}/functions/{function_name}/versions"

        params = self.get_params_dict(
            ["offset", "len", "filter", "order_by"], [offset, len, filter, order_by]
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def execution_plan_list(
        self,
        offset: int = None,
        len: int = None,
        filter: str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = "/execution_plans"

        params = self.get_params_dict(
            ["offset", "len", "filter", "order_by"], [offset, len, filter, order_by]
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def execution_plan_read(
        self,
        execution_plan_id: str,
        raise_for_status: bool = True,
    ):
        endpoint = f"/execution_plans/{execution_plan_id}"
        response = self.get(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def execution_read_function_run(
        self,
        collection_name: str,
        function_name_or_version_id: str,
        execution_id: str,
        raise_for_status: bool = True,
    ):
        endpoint = (
            f"/collections/{collection_name}/function_versions/"
            f"{function_name_or_version_id}/executions/{execution_id}"
        )
        response = self.get(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def function_create(
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
        raise_for_status: bool = True,
    ):
        endpoint = f"/collections/{collection_name}/functions"

        data = {
            "name": function_name,
            "description": description,
            "bundle_id": bundle_id,
            "snippet": function_snippet,
            "decorator": decorator,  # Either P, S or T
            "dependencies": dependencies,
            "triggers": trigger_by,
            "tables": tables,
            "reuse_frozen_tables": reuse_frozen_tables,
        }
        if runtime_values:
            data["runtime_values"] = runtime_values
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def function_delete(
        self, collection_name: str, function_name: str, raise_for_status: bool = True
    ):
        return
        # TODO: Implement this method once the API is ready
        endpoint = f"/collections/{collection_name}/functions/{function_name}"
        response = self.delete(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def function_execute(
        self,
        collection_name: str,
        function_name: str,
        execution_plan_name=None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/collections/{collection_name}/functions/{function_name}/execute"
        data = self.get_params_dict(["name"], [execution_plan_name])
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
        offset: int = None,
        len: int = None,
        filter: str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/collections/{collection_name}/functions"
        params = self.get_params_dict(
            ["offset", "len", "filter", "order_by"], [offset, len, filter, order_by]
        )
        response = self.get(endpoint, params=params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def function_list_history(
        self,
        collection_name: str,
        function_name: str,
        offset: int = None,
        len: int = None,
        filter: str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/collections/{collection_name}/functions/{function_name}/history"
        params = self.get_params_dict(
            ["offset", "len", "filter", "order_by"], [offset, len, filter, order_by]
        )
        response = self.get(endpoint, params=params)
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
        # Aleix: Updated
        endpoint = "/roles"
        data = self.get_params_dict(["name", "description"], [name, description])
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def role_delete(self, name: str, raise_for_status: bool = True):
        # Aleix: Updated
        endpoint = f"/roles/{name}"
        response = self.delete(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def role_get_by_name(self, name: str, raise_for_status: bool = True):
        # Aleix: Updated
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
            ["len", "filter", "order_by", "pagination_id", "next"],
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
        # Aleix: Updated
        endpoint = f"/roles/{name}"
        data = self.get_params_dict(
            ["name", "description"], [new_name, new_description]
        )
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def status_get(self, raise_for_status: bool = True):
        endpoint = "/status"
        response = self.get(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def table_get_data(
        self,
        collection_name: str,
        table_name: str,
        commit: str = None,
        time: str = None,
        version: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/collections/{collection_name}/tables/{table_name}/data"
        params = self.get_params_dict(
            ["at_commit", "at_time", "at_version"],
            [commit, time, version],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def table_get_sample(
        self,
        collection_name: str,
        table_name: str,
        commit: str = None,
        time: str = None,
        version: str = None,
        offset: int = None,
        len: int = None,
        filter: str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/collections/{collection_name}/tables/{table_name}/sample"
        params = self.get_params_dict(
            [
                "at_commit",
                "at_time",
                "at_version",
                "offset",
                "len",
                "filter",
                "order_by",
            ],
            [commit, time, version, offset, len, filter, order_by],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def table_get_schema(
        self,
        collection_name: str,
        table_name: str,
        commit: str = None,
        time: str = None,
        version: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/collections/{collection_name}/tables/{table_name}/schema"
        params = self.get_params_dict(
            ["at_commit", "at_time", "at_version"],
            [commit, time, version],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def table_list(
        self,
        collection_name: str,
        offset: int = None,
        len: int = None,
        filter: str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/collections/{collection_name}/tables"
        params = self.get_params_dict(
            ["offset", "len", "filter", "order_by"], [offset, len, filter, order_by]
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def transaction_cancel(self, transaction_id: str, raise_for_status: bool = True):
        endpoint = f"/transactions/{transaction_id}/cancel"
        response = self.post(endpoint, json={})
        return self.raise_for_status_or_return(raise_for_status, response)

    def transaction_list(
        self,
        offset: int = None,
        len: int = None,
        filter: str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = "/transactions"

        params = self.get_params_dict(
            ["offset", "len", "filter", "order_by"], [offset, len, filter, order_by]
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
        # Aleix: Updated
        endpoint = "/users"

        data = self.get_params_dict(
            ["name", "full_name", "email", "password", "enabled"],
            [name, full_name, email, password, enabled],
        )
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def users_delete(self, name: str, raise_for_status: bool = True):
        # Aleix: Updated
        endpoint = f"/users/{name}"
        response = self.delete(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def users_get_by_name(self, name: str, raise_for_status: bool = True):
        # Aleix: Updated
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
        # Aleix: Updated
        endpoint = "/users"
        params = self.get_params_dict(
            ["len", "filter", "order_by", "pagination_id", "next"],
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
        # Aleix: Updated
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
        by_function_id: str = None,
        by_transaction_id: str = None,
        by_execution_plan_id: str = None,
        by_data_version_id: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = "/workers"
        params = self.get_params_dict(
            [
                "by_function_id",
                "by_transaction_id",
                "by_execution_plan_id",
                "by_data_version_id",
            ],
            [
                by_function_id,
                by_transaction_id,
                by_execution_plan_id,
                by_data_version_id,
            ],
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
