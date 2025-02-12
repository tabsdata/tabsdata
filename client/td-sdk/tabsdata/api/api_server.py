#
# Copyright 2024 Tabs Data Inc.
#

import json
import logging
import os
import time
from urllib.parse import urlparse

import requests

from tabsdata.utils.tableframe._constants import PYTEST_CONTEXT_ACTIVE

DEFAULT_APISERVER_PORT = "2457"
HTTP_PROTOCOL = "http://"
PORT_SEPARATOR = ":"

HTTP_TIMEOUT = 60 * 5
MIN_HTTP_ATTEMPTS = 1
MAX_HTTP_ATTEMPTS = 12
HTTP_RETIRES_DELAY = 5

logger = logging.getLogger(__name__)


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
    return url


class APIServer:

    def __init__(self, url: str):
        url = process_url(url)
        self.url = url
        self.bearer_token = None
        self.refresh_token = None

    @property
    def authentication_header(self):
        return {"Authorization": f"Bearer {self.bearer_token}", "Connection": "close"}

    @property
    def connection_header(self):
        return {"Connection": "close"}

    def get(self, path, params=None):
        headers = {}
        headers.update(self.authentication_header)
        headers.update(self.connection_header)

        max_tries = (
            MIN_HTTP_ATTEMPTS
            if os.environ.get(PYTEST_CONTEXT_ACTIVE) is None
            else MAX_HTTP_ATTEMPTS
        )

        for attempt in range(max_tries):
            try:
                response = requests.get(
                    self.url + path,
                    headers=headers,
                    timeout=HTTP_TIMEOUT,
                    params=params,
                )
                return response
            except Exception as e:
                logger.debug(f"Attempt {attempt + 1} for 'get' failed: {e}")
                if attempt < MAX_HTTP_ATTEMPTS - 1:
                    time.sleep(HTTP_RETIRES_DELAY)
                else:
                    raise

    def post(self, path, data):
        headers = {}
        headers.update(self.authentication_header)
        headers.update(self.connection_header)

        max_tries = (
            MIN_HTTP_ATTEMPTS
            if os.environ.get(PYTEST_CONTEXT_ACTIVE) is None
            else MAX_HTTP_ATTEMPTS
        )

        for attempt in range(max_tries):
            try:
                response = requests.post(
                    self.url + path,
                    headers=headers,
                    timeout=HTTP_TIMEOUT,
                    json=data,
                )
                return response
            except Exception as e:
                logger.debug(f"Attempt {attempt + 1} for 'post' failed: {e}")
                if attempt < MAX_HTTP_ATTEMPTS - 1:
                    time.sleep(HTTP_RETIRES_DELAY)
                else:
                    raise

    def post_binary(self, path, data):
        headers = {}
        headers.update(self.authentication_header)
        headers.update(self.connection_header)
        headers.update({"Content-Type": "application/octet-stream"})

        max_tries = (
            MIN_HTTP_ATTEMPTS
            if os.environ.get(PYTEST_CONTEXT_ACTIVE) is None
            else MAX_HTTP_ATTEMPTS
        )

        for attempt in range(max_tries):
            try:
                response = requests.post(
                    self.url + path,
                    headers=headers,
                    timeout=HTTP_TIMEOUT,
                    data=data,
                )
                return response
            except Exception as e:
                logger.debug(f"Attempt {attempt + 1} for 'binary' failed: {e}")
                if attempt < MAX_HTTP_ATTEMPTS - 1:
                    time.sleep(HTTP_RETIRES_DELAY)
                else:
                    raise

    def delete(self, path):
        headers = {}
        headers.update(self.authentication_header)
        headers.update(self.connection_header)

        max_tries = (
            MIN_HTTP_ATTEMPTS
            if os.environ.get(PYTEST_CONTEXT_ACTIVE) is None
            else MAX_HTTP_ATTEMPTS
        )

        for attempt in range(max_tries):
            try:
                response = requests.delete(
                    self.url + path,
                    headers=headers,
                    timeout=HTTP_TIMEOUT,
                )
                return response
            except Exception as e:
                print(f"Attempt {attempt + 1} for 'delete' failed: {e}")
                if attempt < MAX_HTTP_ATTEMPTS - 1:
                    time.sleep(HTTP_RETIRES_DELAY)
                else:
                    raise

    def _store_in_file(self, file_path: str):
        with open(file_path, "w") as file:
            json.dump(
                {
                    "url": self.url,
                    "bearer_token": self.bearer_token,
                    "refresh_token": self.refresh_token,
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

    def authentication_access(self, name: str, password: str):
        endpoint = "/auth/access"
        data = {"name": name, "password": password}
        response = self.post(endpoint, data)
        if response.status_code == 200:
            self.bearer_token = response.json()["access_token"]
            self.refresh_token = response.json()["refresh_token"]
            return response
        else:
            raise APIServerError(response.json())

    def authentication_refresh(self):
        endpoint = "/auth/refresh"
        data = {"refresh_token": self.refresh_token}
        response = self.post(endpoint, data)
        if response.status_code == 200:
            self.bearer_token = response.json()["access_token"]
            self.refresh_token = response.json()["refresh_token"]
            return response
        else:
            raise APIServerError(response.json())

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

    def collection_create(
        self, name: str, description: str, raise_for_status: bool = True
    ):
        endpoint = "/collections"
        data = {"name": name, "description": description}
        response = self.post(endpoint, data)
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
        offset: int = None,
        len: int = None,
        filter: str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = "/collections"

        params = self.get_params_dict(
            ["offset", "len", "filter", "order_by"], [offset, len, filter, order_by]
        )
        response = self.get(endpoint, params=params)
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
        response = self.post(endpoint, data)
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

    def function_create(
        self,
        collection_name: str,
        function_name: str,
        description: str,
        bundle_hash: str,
        tables: list[str],
        dependencies: list[str],
        trigger_by: list[str],
        function_snippet: str,
        raise_for_status: bool = True,
    ):
        endpoint = f"/collections/{collection_name}/functions"

        data = {
            "name": function_name,
            "description": description,
            "bundle_hash": bundle_hash,
            "tables": tables,
            "dependencies": dependencies,
            "trigger_by": trigger_by,
            "function_snippet": function_snippet,
        }
        response = self.post(endpoint, data)
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
        response = self.post(endpoint, data)
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
        bundle_hash: str = None,
        tables: list[str] = None,
        dependencies: list[str] = None,
        trigger_by: list[str] = None,
        function_snippet: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/collections/{collection_name}/functions/{function_name}"

        data = self.get_params_dict(
            [
                "name",
                "description",
                "bundle_hash",
                "tables",
                "dependencies",
                "trigger_by",
                "function_snippet",
            ],
            [
                new_function_name,
                description,
                bundle_hash,
                tables,
                dependencies,
                trigger_by,
                function_snippet,
            ],
        )
        response = self.post(endpoint, data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def function_upload_bundle(
        self,
        collection_name: str,
        function_name: str,
        function_id: str,
        bundle: bytes,
        raise_for_status: bool = True,
    ):
        endpoint = (
            f"/collections/{collection_name}/functions/"
            f"{function_name}/upload/{function_id}"
        )
        response = self.post_binary(endpoint, data=bundle)
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
        response = self.post(endpoint, {})
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
        response = self.post(endpoint, {})
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
        data = {
            "name": name,
            "full_name": full_name,
            "email": email,
            "password": password,
            "enabled": enabled,
        }
        response = self.post(endpoint, data)
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
        offset: int = None,
        len: int = None,
        filter: str = None,
        order_by: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = "/users"

        params = self.get_params_dict(
            ["offset", "len", "filter", "order_by"], [offset, len, filter, order_by]
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def users_update(
        self,
        name: str,
        full_name: str = None,
        email: str = None,
        old_password: str = None,
        new_password: str = None,
        force_password_change: bool = False,
        enabled: bool = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/users/{name}"
        data = self.get_params_dict(
            ["full_name", "email", "enabled"], [full_name, email, enabled]
        )
        if old_password and new_password:
            data["password"] = {
                "Change": {"old_password": old_password, "new_password": new_password}
            }
        elif force_password_change:
            data["password"] = {"ForceChange": {"temporary_password": new_password}}
        response = self.post(endpoint, data)
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


def obtain_connection(url: str, name: str, password: str) -> APIServer:
    connection = APIServer(url)
    connection.authentication_access(name, password)
    return connection
