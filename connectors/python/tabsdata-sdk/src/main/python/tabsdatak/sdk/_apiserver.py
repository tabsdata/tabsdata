#
# Copyright 2026 Tabsdata Inc.
#

"""Low-level HTTP client + auth/refresh wrapper around the tabsdata
apiserver. The high-level user-facing SDK in
:mod:`tabsdatak.sdk.tabsdata_server` wraps this with friendlier method
names; consumers normally don't talk to ``APIServer`` directly."""

import logging
import os
import time
from contextlib import contextmanager, nullcontext
from http import HTTPStatus
from typing import List
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from tabsdatak.sdk import _credentials

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
AUTH_RETRY_SECONDS = 30.0
AUTH_RETRY_FIRST_PAUSE = 0.1
AUTH_RETRY_MAX_PAUSE = 2.0
REFRESH_LOCK_SECONDS = 30.0


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
    # A busy server stays busy while its writes keep conflicting, which under
    # many concurrent clients lasts minutes: five, at the same pace.
    http_busy_attempts = 60

    class ConstantWaitRetry(Retry):
        def sleep(self, _response=None):
            time.sleep(http_retries_delay)

    # A 503 means the server applied nothing: it was too busy to run the
    # request, so any request may be repeated after one.
    default_retry_strategy = ConstantWaitRetry(
        total=http_busy_attempts,
        connect=http_attempts,
        read=0,
        redirect=http_attempts,
        status=http_busy_attempts,
        other=0,
        allowed_methods=None,
        status_forcelist={503},
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
    except (AttributeError, TypeError, ValueError) as e:
        raise ValueError(
            f"Invalid URL '{url}' for obtaining certificate file path. Please ensure "
            f"that it is a valid URL, with a hostname and a port: {e}"
        )


class APIServerError(Exception):
    def __init__(self, dictionary):
        if not isinstance(dictionary, dict):
            dictionary = {"error_description": str(dictionary)}
        self.code = dictionary.get("code")
        self.error = dictionary.get("error")
        self.error_description = dictionary.get("error_description")
        super().__init__(
            self.error_description if self.error_description else "Unknown error"
        )


class NoProjectSelectedError(Exception):
    """Raised when a project-scoped call has no active project.

    Name-keyed resources (collections, functions, tables, groups, ...) live
    under ``/projects/{project}/...``. The active project rides on the
    connection, set from the pinned project or a command's ``--proj``
    override; when neither is present there is no project to target.
    """

    def __init__(self):
        super().__init__(
            "No project selected. Run 'tdk set --proj <name>' or pass "
            "'--proj <name>' to the command."
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
        # Active project for project-scoped requests. Set once per CLI run
        # from the pinned project or a command's `--proj` override; `_scoped`
        # builds scoped paths from it and `_project_narrow` narrows
        # cross-project lists to it.
        self.project: str | None = None
        self._active_project_name_for: str | None = None
        self._resolved_active_project_name = None

    def _active_project_name(self) -> str:
        # `--proj`/pin may be an id; the list filter matches the project
        # name, so resolve id-or-name to the name (cached per active value).
        if self._active_project_name_for != self.project:
            data = self.project_get_by_name(self.project).json()["data"]
            self._resolved_active_project_name = data["name"]
            self._active_project_name_for = self.project
        return self._resolved_active_project_name

    def _scoped(self, path: str) -> str:
        """Build a project-scoped path under ``/projects/{project}``.

        Name-keyed resources (collections, functions, tables, groups,
        metrics) are addressed within the active project, which rides on the
        connection. Each endpoint method decides whether its path is scoped;
        paths not built through this helper go out verbatim. With no project
        selected a scoped call fails fast rather than guessing a default.
        """
        if not self.project:
            raise NoProjectSelectedError()
        return f"/projects/{self.project}{path}"

    @contextmanager
    def spanning(self):
        """Suspend the active project for one call: by-id lookups resolve a
        globally unique id and must not be narrowed to the active project."""
        project = self.project
        self.project = None
        try:
            yield
        finally:
            self.project = project

    def _project_narrow(
        self, request_filter: List[str] | str = None
    ) -> List[str] | str | None:
        """Narrow a cross-project list to the active project, when one is set."""
        if not self.project:
            return request_filter
        narrowed = f"project:eq:{self._active_project_name()}"
        if request_filter is None:
            return [narrowed]
        if isinstance(request_filter, str):
            return [request_filter, narrowed]
        return [*request_filter, narrowed]

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
        if not self.refresh_near() and not self.expired():
            return
        lock = (
            _credentials.locked(self.credentials_file, REFRESH_LOCK_SECONDS)
            if self.credentials_file
            else nullcontext()
        )
        with lock:
            stored = self.stored_credentials()
            if stored and stored.get("refresh_token") != self.refresh_token:
                self.adopt(stored)
            if not self.refresh_near():
                return
            logger.debug("Refreshing authentication token")
            try:
                self.authentication_refresh()
            except APIServerError as e:
                logger.debug(f"Authentication token refresh failed: {e}")

    def expired(self) -> bool:
        return bool(
            self.refresh_token
            and self.expiration_time
            and self.expiration_time < time.time()
        )

    def refresh_near(self) -> bool:
        if not self.refresh_token or not self.expiration_time:
            return False
        current_time = time.time()
        already_expired = self.expiration_time < current_time
        near_expiration = (
            current_time > self.expiration_time - REFRESH_BUFFER_IN_SECONDS
        )
        return near_expiration and not already_expired

    def stored_credentials(self) -> dict | None:
        if not self.credentials_file:
            return None
        try:
            stored = _credentials.read(self.credentials_file)
        except (OSError, ValueError) as e:
            logger.debug(f"Could not read the credentials file: {e}")
            return None
        if not stored or stored.get("url") != self.url:
            return None
        return stored

    def credentials(self) -> dict:
        return {
            "url": self.url,
            "bearer_token": self.bearer_token,
            "refresh_token": self.refresh_token,
            "token_type": self.token_type,
            "expires_in": self.expires_in,
            "expiration_time": self.expiration_time,
        }

    def adopt(self, stored: dict):
        self.bearer_token = stored.get("bearer_token")
        self.refresh_token = stored.get("refresh_token")
        self.token_type = stored.get("token_type")
        self.expires_in = stored.get("expires_in")
        self.expiration_time = stored.get("expiration_time")

    def recoverable(self) -> bool:
        return bool(self.credentials_file and self.refresh_token)

    def await_new_token(self, rejected: str | None, deadline: float) -> bool:
        pause = AUTH_RETRY_FIRST_PAUSE
        while True:
            remaining = deadline - time.monotonic()
            with _credentials.locked(self.credentials_file, max(remaining, 0)):
                stored = self.stored_credentials()
            if stored and stored.get("bearer_token") not in (None, rejected):
                self.adopt(stored)
                return True
            if self.expired():
                return False
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            time.sleep(min(pause, remaining))
            pause = min(pause * 2, AUTH_RETRY_MAX_PAUSE)

    @property
    def authentication_header(self):
        return (
            {"Authorization": f"Bearer {self.bearer_token}"}
            if self.bearer_token
            else {}
        )

    def send_request(
        self,
        method: str,
        path: str,
        refresh_if_needed: bool,
        content_type=None,
        **kwargs,
    ):
        if refresh_if_needed:
            self._refresh_token_if_needed()
        response = self.request_once(method, path, content_type, **kwargs)
        deadline = None
        while (
            refresh_if_needed
            and response.status_code == HTTPStatus.UNAUTHORIZED
            and self.recoverable()
        ):
            if deadline is None:
                deadline = time.monotonic() + AUTH_RETRY_SECONDS
            if not self.await_new_token(self.bearer_token, deadline):
                break
            logger.debug("Retrying with the session another process refreshed")
            response = self.request_once(method, path, content_type, **kwargs)
        return response

    def request_once(self, method: str, path: str, content_type=None, **kwargs):
        headers = dict(self.authentication_header)
        if content_type:
            headers["Content-Type"] = content_type
        session = GET_HTTP_SESSION if method == "get" else DEFAULT_HTTP_SESSION
        return getattr(session, method)(
            self.url + path,
            headers=headers,
            timeout=(CONNECTION_TIMEOUT, READ_TIMEOUT),
            verify=self.certificate_file,
            **kwargs,
        )

    def get(self, path, params=None, refresh_if_needed=True):
        return self.send_request("get", path, refresh_if_needed, params=params)

    def post(
        self,
        path,
        data=None,
        json=None,
        params=None,
        refresh_if_needed=True,
        content_type=None,
    ):
        return self.send_request(
            "post",
            path,
            refresh_if_needed,
            content_type,
            json=json,
            data=data,
            params=params,
        )

    def post_binary(self, path, data, refresh_if_needed=True):
        return self.send_request(
            "post", path, refresh_if_needed, "application/octet-stream", data=data
        )

    def delete(self, path, json=None, refresh_if_needed=True):
        return self.send_request("delete", path, refresh_if_needed, json=json)

    def _store_in_file(self, file_path: str):
        _credentials.write(file_path, self.credentials())

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

    def authentication_service_token(self, raise_for_status: bool = True):
        """The service token making this request.

        The one endpoint a token may call about itself; a session is refused
        with a 403.
        """
        endpoint = "/auth/service-token"
        response = self.get(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def authentication_service_token_login(self, token: str):
        """Authenticate with a service token instead of a password.

        A token neither expires into a refresh nor can be renewed, so there
        is nothing to store but the token itself; it is validated against
        `/auth/service-token` so a bad one is reported here rather than on the first
        real call.
        """
        self.bearer_token = token
        self.refresh_token = None
        self.token_type = "Bearer"
        self.expires_in = None
        self.expiration_time = None
        response = self.authentication_service_token(raise_for_status=True)
        if self.credentials_file:
            self._store_in_file(self.credentials_file)
        return response

    def service_token_create(
        self,
        slot: int,
        expires_in_days: int,
        comment: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/auth/service-tokens/{slot}"
        json = {"expires_in_days": expires_in_days, "comment": comment}
        response = self.post(endpoint, json=json)
        return self.raise_for_status_or_return(raise_for_status, response)

    def service_token_list(
        self,
        request_len: int = None,
        request_filter: List[str] | str = None,
        order_by: str = None,
        pagination_id: str = None,
        next_step: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = "/auth/service-tokens"
        params = self.get_params_dict(
            DEFAULT_LIST_ENDPOINT_PARAMETERS,
            [request_len, request_filter, order_by, pagination_id, next_step],
        )
        response = self.get(endpoint, params=params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def service_token_revoke(self, slot: int, raise_for_status: bool = True):
        endpoint = f"/auth/service-tokens/{slot}"
        response = self.delete(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def authentication_login(self, name: str, password: str):
        endpoint = "/auth/login"
        data = self.get_params_dict(["name", "password"], [name, password])
        time_of_request = time.time()
        response = self.post(endpoint, json=data, refresh_if_needed=False)
        response_data = response.json()
        if HTTPStatus(response.status_code).is_success:
            self.bearer_token = response_data["access_token"]
            self.refresh_token = response_data["refresh_token"]
            self.token_type = response_data["token_type"]
            self.expires_in = response_data["expires_in"]
            self.expiration_time = time_of_request + self.expires_in
            if self.credentials_file:
                self._store_in_file(self.credentials_file)
            return response
        else:
            raise APIServerError(response_data)

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
            _credentials.remove(self.credentials_file)
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
        response_data = response.json()
        if HTTPStatus(response.status_code).is_success:
            self.bearer_token = response_data["access_token"]
            self.refresh_token = response_data["refresh_token"]
            self.token_type = response_data["token_type"]
            self.expires_in = response_data["expires_in"]
            self.expiration_time = time_of_request + self.expires_in
            if self.credentials_file:
                self._store_in_file(self.credentials_file)
            return response
        else:
            raise APIServerError(response_data)

    def collection_create(
        self,
        name: str,
        description: str,
        group: str | None = None,
        base_path: str | None = None,
        connection: str | None = None,
        secrets: list | None = None,
        raise_for_status: bool = True,
    ):
        endpoint = self._scoped("/collections")
        data = {"name": name, "description": description}
        if group is not None:
            data["group"] = group
        if base_path is not None:
            data["base_path"] = base_path
        if connection is not None:
            # Source/destination collections are created WITH their
            # connection (1:1) — the pair rides on the create payload.
            data["connection"] = connection
            data["secrets"] = secrets or []
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def collection_delete(self, collection_name: str, raise_for_status: bool = True):
        endpoint = self._scoped(f"/collections/{collection_name}")
        response = self.delete(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def collection_connection_upsert(
        self,
        collection_name: str,
        connection: str,
        secrets: list | None = None,
        raise_for_status: bool = True,
    ):
        endpoint = self._scoped(f"/collections/{collection_name}/connection")
        data = {
            "connection": connection,
            "secrets": secrets or [],
        }
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def collection_connection_get(
        self, collection_name: str, raise_for_status: bool = True
    ):
        endpoint = self._scoped(f"/collections/{collection_name}/connection")
        response = self.get(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def collection_get_by_name(
        self, collection_name: str, raise_for_status: bool = True
    ):
        endpoint = self._scoped(f"/collections/{collection_name}")
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
            [
                request_len,
                self._project_narrow(request_filter),
                order_by,
                pagination_id,
                next_step,
            ],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def collection_update(
        self,
        collection_name: str,
        new_collection_name: str = None,
        description: str = None,
        base_path: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = self._scoped(f"/collections/{collection_name}")
        data = self.get_params_dict(
            ["name", "description", "base_path"],
            [new_collection_name, description, base_path],
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
        at: int | None = None,
    ):
        endpoint = self._scoped(
            f"/collections/{collection_name}/tables/{table_name}/data-versions"
        )

        params = self.get_params_dict(
            [*DEFAULT_LIST_ENDPOINT_PARAMETERS, "at"],
            [request_len, request_filter, order_by, pagination_id, next_step, at],
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
            [
                request_len,
                self._project_narrow(request_filter),
                order_by,
                pagination_id,
                next_step,
            ],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def execution_read(
        self,
        execution_id: str,
        with_disabled: bool = False,
        raise_for_status: bool = True,
    ):
        endpoint = f"/executions/{execution_id}"
        params = self.get_params_dict(
            ["with_disabled"], [with_disabled.__str__().lower()]
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
        endpoint = self._scoped(
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
        table_types: dict[str, str],
        dependencies: list[str],
        trigger_by: list[str],
        function_snippet: str,
        bundle_id: str,
        decorator: str,
        runtime_values: str,
        reuse_frozen_tables: bool,
        enabled: bool = None,
        connector: str = None,
        connection: str = None,
        workers: dict = None,
        base_path: str = None,
        child_functions: list[dict] = None,
        parent_tables: dict[str, str] = None,
        raise_for_status: bool = True,
    ):
        endpoint = self._scoped(f"/collections/{collection_name}/functions")

        names = [
            "name",
            "description",
            "bundle_id",
            "snippet",
            "decorator",
            "dependencies",
            "triggers",
            "tables",
            "table_types",
            "parent_tables",
            "reuse_frozen_tables",
            "runtime_values",
            "enabled",
            "connector",
            "connection",
            "workers",
            "base_path",
            "child_functions",
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
            table_types,
            parent_tables,
            reuse_frozen_tables,
            runtime_values,
            enabled,
            connector,
            connection,
            workers,
            base_path,
            child_functions,
        ]
        data = self.get_params_dict(names, values)
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def function_delete(
        self, collection_name: str, function_name: str, raise_for_status: bool = True
    ):
        endpoint = self._scoped(
            f"/collections/{collection_name}/functions/{function_name}"
        )
        response = self.delete(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def function_execute(
        self,
        collection_name: str,
        function_name: str,
        execution_name=None,
        raise_for_status: bool = True,
    ):
        endpoint = self._scoped(
            f"/collections/{collection_name}/functions/{function_name}/execute"
        )
        data = self.get_params_dict(["name"], [execution_name])
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def function_get(
        self, collection_name: str, function_name: str, raise_for_status: bool = True
    ):
        endpoint = self._scoped(
            f"/collections/{collection_name}/functions/{function_name}"
        )
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
        endpoint = self._scoped(f"/collections/{collection_name}/functions")

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
        endpoint = self._scoped(
            f"/collections/{collection_name}/functions/{function_name}/history"
        )

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
        endpoint = "/function-runs"

        params = self.get_params_dict(
            DEFAULT_LIST_ENDPOINT_PARAMETERS,
            [
                request_len,
                self._project_narrow(request_filter),
                order_by,
                pagination_id,
                next_step,
            ],
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
        table_types: dict[str, str] = None,
        dependencies: list[str] = None,
        trigger_by: list[str] = None,
        function_snippet: str = None,
        decorator: str = None,
        runtime_values: str = None,
        reuse_frozen_tables: bool = None,
        enabled: bool = None,
        connector: str = None,
        connection: str = None,
        workers: dict = None,
        base_path: str = None,
        reset_state: bool = None,
        reset_stage: bool = None,
        child_functions: list[dict] = None,
        parent_tables: dict[str, str] = None,
        raise_for_status: bool = True,
    ):
        endpoint = self._scoped(
            f"/collections/{collection_name}/functions/{function_name}"
        )

        data = self.get_params_dict(
            [
                "name",
                "description",
                "bundle_id",
                "tables",
                "table_types",
                "parent_tables",
                "dependencies",
                "triggers",
                "snippet",
                "decorator",
                "runtime_values",
                "reuse_frozen_tables",
                "enabled",
                "connector",
                "connection",
                "workers",
                "base_path",
                "reset_state",
                "reset_stage",
                "child_functions",
            ],
            [
                new_function_name,
                description,
                bundle_id,
                tables,
                table_types,
                parent_tables,
                dependencies,
                trigger_by,
                function_snippet,
                decorator,  # Either P, S or T
                runtime_values,
                reuse_frozen_tables,
                enabled,
                connector,
                connection,
                workers,
                base_path,
                reset_state,
                reset_stage,
                child_functions,
            ],
        )
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def function_enable(
        self,
        collection_name: str,
        function_name: str,
        enabled: bool,
        raise_for_status: bool = True,
    ):
        endpoint = self._scoped(
            f"/collections/{collection_name}/functions/{function_name}/enable"
        )

        data = self.get_params_dict(["enabled"], [enabled])
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def function_upload_bundle(
        self,
        collection_name: str,
        bundle: bytes,
        raise_for_status: bool = True,
    ):
        endpoint = self._scoped(
            f"/collections/{collection_name}/function-bundle-upload"
        )
        response = self.post_binary(endpoint, data=bundle)
        return self.raise_for_status_or_return(raise_for_status, response)

    def function_patch(
        self,
        collection_name: str,
        function_name: str,
        patch_of: str,
        bundle_id: str,
        snippet: str,
        runtime_values: str = None,
        description: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = self._scoped(
            f"/collections/{collection_name}/functions/{function_name}/patch"
        )
        data = self.get_params_dict(
            ["patch_of", "bundle_id", "snippet", "runtime_values", "description"],
            [patch_of, bundle_id, snippet, runtime_values, description],
        )
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def function_list_patches(
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
        endpoint = self._scoped(
            f"/collections/{collection_name}/functions/{function_name}/patches"
        )
        params = self.get_params_dict(
            DEFAULT_LIST_ENDPOINT_PARAMETERS,
            [request_len, request_filter, order_by, pagination_id, next_step],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def function_bundle_download(
        self, function_version_id: str, raise_for_status: bool = True
    ):
        endpoint = f"/function-versions/{function_version_id}/bundle"
        response = self.get(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def function_run_raw_data(
        self,
        collection_name: str,
        function_run_id: str,
        input_idx: int,
        raise_for_status: bool = True,
    ):
        # A publisher run stamps one raw-data file per input it ingested;
        # `input_idx` selects among them. Returns the file as parquet bytes.
        endpoint = self._scoped(
            f"/collections/{collection_name}/function-runs/{function_run_id}"
            f"/inputs/{input_idx}/raw-data"
        )
        response = self.get(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def logical_function_run_replay(
        self,
        logical_function_run_id: str,
        raise_for_status: bool = True,
    ):
        endpoint = f"/logical-function-runs/{logical_function_run_id}/replay"
        response = self.post(endpoint, json={})
        return self.raise_for_status_or_return(raise_for_status, response)

    def group_create(
        self, name: str, description: str = None, raise_for_status: bool = True
    ):
        endpoint = self._scoped("/groups")
        data = self.get_params_dict(["name", "description"], [name, description])
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def group_delete(self, name: str, raise_for_status: bool = True):
        endpoint = self._scoped(f"/groups/{name}")
        response = self.delete(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def group_get_by_name(self, name: str, raise_for_status: bool = True):
        endpoint = self._scoped(f"/groups/{name}")
        response = self.get(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def group_list(
        self,
        request_len: int = None,
        request_filter: List[str] | str = None,
        order_by: str = None,
        pagination_id: str = None,
        next_step: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = self._scoped("/groups")
        params = self.get_params_dict(
            DEFAULT_LIST_ENDPOINT_PARAMETERS,
            [request_len, request_filter, order_by, pagination_id, next_step],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def group_update(
        self,
        name: str,
        new_name: str = None,
        new_description: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = self._scoped(f"/groups/{name}")
        data = self.get_params_dict(
            ["name", "description"], [new_name, new_description]
        )
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def project_create(
        self, name: str, description: str, raise_for_status: bool = True
    ):
        endpoint = "/projects"
        data = self.get_params_dict(["name", "description"], [name, description])
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def project_delete(self, name: str, raise_for_status: bool = True):
        endpoint = f"/projects/{name}"
        response = self.delete(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def project_get_by_name(self, name: str, raise_for_status: bool = True):
        endpoint = f"/projects/{name}"
        response = self.get(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def project_list(
        self,
        request_len: int = None,
        request_filter: List[str] | str = None,
        order_by: str = None,
        pagination_id: str = None,
        next_step: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = "/projects"
        params = self.get_params_dict(
            DEFAULT_LIST_ENDPOINT_PARAMETERS,
            [request_len, request_filter, order_by, pagination_id, next_step],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def project_update(
        self,
        name: str,
        new_name: str = None,
        new_description: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = f"/projects/{name}"
        data = self.get_params_dict(
            ["name", "description"], [new_name, new_description]
        )
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def metrics_get(
        self,
        params: dict | None = None,
        raise_for_status: bool = True,
    ):
        """Consolidated `/metrics` endpoint — returns `(nodes, edges)`
        over a window, parameterised by `dim`.

        `params` is a flat dict matching the URL query string (e.g.
        ``{"dim": "collection", "unit": "day", "status": ["C", "F"]}``).
        Repeated params (collection, group, status) accept lists.
        """
        response = self.get(self._scoped("/metrics"), params=params or {})
        return self.raise_for_status_or_return(raise_for_status, response)

    def inflight_get(
        self,
        params: dict | None = None,
        raise_for_status: bool = True,
    ):
        """Point-in-time `/inflight` — per-(collection, group, status)
        count of non-terminal runs.
        """
        response = self.get(self._scoped("/inflight"), params=params or {})
        return self.raise_for_status_or_return(raise_for_status, response)

    def permission_grant(
        self,
        user: str,
        permission: str,
        project: str = None,
        raise_for_status: bool = True,
    ):
        """Grant a flat permission to a user (Simple Security Mode facade)."""
        endpoint = "/permission-grants"
        data = self.get_params_dict(
            ["user", "permission", "project"], [user, permission, project]
        )
        response = self.post(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def permission_revoke(
        self,
        user: str,
        permission: str,
        project: str = None,
        raise_for_status: bool = True,
    ):
        """Revoke a flat permission from a user (Simple Security Mode facade)."""
        endpoint = "/permission-grants"
        data = self.get_params_dict(
            ["user", "permission", "project"], [user, permission, project]
        )
        response = self.delete(endpoint, json=data)
        return self.raise_for_status_or_return(raise_for_status, response)

    def permission_list_by_user(self, user: str, raise_for_status: bool = True):
        """List a user's effective flat permissions (Simple Security Mode
        facade)."""
        endpoint = "/permission-grants/by-user"
        response = self.get(endpoint, params={"user": user})
        return self.raise_for_status_or_return(raise_for_status, response)

    def permission_list_by_project(self, project: str, raise_for_status: bool = True):
        """List everyone with authority on a project (Simple Security Mode
        facade)."""
        endpoint = "/permission-grants/by-project"
        response = self.get(endpoint, params={"project": project})
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
        endpoint = self._scoped(f"/collections/{collection_name}/tables/{table_name}")
        response = self.delete(endpoint)
        return self.raise_for_status_or_return(raise_for_status, response)

    def table_download(
        self,
        collection_name: str,
        table_name: str,
        at: int = None,
        raise_for_status: bool = True,
    ):
        endpoint = self._scoped(
            f"/collections/{collection_name}/tables/{table_name}/download"
        )
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
        sql: str = None,
        extra_cols: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = self._scoped(
            f"/collections/{collection_name}/tables/{table_name}/sample"
        )
        params = self.get_params_dict(
            [
                "at",
                "offset",
                "len",
                "sql",
                "extra-cols",
            ],
            [at, offset, len, sql, extra_cols],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    def table_get_schema(
        self,
        collection_name: str,
        table_name: str,
        at: int = None,
        extra_cols: str = None,
        raise_for_status: bool = True,
    ):
        endpoint = self._scoped(
            f"/collections/{collection_name}/tables/{table_name}/schema"
        )
        params = self.get_params_dict(
            ["at", "extra-cols"],
            [at, extra_cols],
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
        endpoint = self._scoped(f"/collections/{collection_name}/tables")
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
            [
                request_len,
                self._project_narrow(request_filter),
                order_by,
                pagination_id,
                next_step,
            ],
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
            [
                request_len,
                self._project_narrow(request_filter),
                order_by,
                pagination_id,
                next_step,
            ],
        )
        response = self.get(endpoint, params)
        return self.raise_for_status_or_return(raise_for_status, response)

    @staticmethod
    def raise_for_status(response: requests.Response):
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            try:
                detail = response.json()
            except ValueError:
                detail = response.text
            raise APIServerError(detail)
        else:
            return response

    @staticmethod
    def get_params_dict(names: list, values: list) -> dict:
        return {name: value for name, value in zip(names, values) if value is not None}


def obtain_connection(
    url: str,
    name: str = None,
    password: str = None,
    credentials_file: str = None,
    token: str = None,
) -> APIServer:
    connection = APIServer(url, credentials_file=credentials_file)
    if token:
        connection.authentication_service_token_login(token)
    elif name and password:
        connection.authentication_login(name, password)
    return connection
