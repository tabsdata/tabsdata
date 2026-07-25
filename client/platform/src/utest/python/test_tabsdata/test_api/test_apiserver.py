#
# Copyright 2024 Tabsdata Inc.
#

import ast
import hashlib
import pathlib
import tempfile
from http import HTTPStatus
from unittest.mock import MagicMock, patch

import pytest
import requests

from tabsdata.api.apiserver import (
    BASE_API_URL,
    DEFAULT_HTTP_SESSION,
    GET_HTTP_SESSION,
    APIServer,
    APIServerError,
    NoProjectSelectedError,
    _obtain_certificate_file_path,
    obtain_connection,
)
from tabsdata.api.tabsdata_server import Collection, Function
from test_tabsdata.conftest import APISERVER_URL

# noinspection PyUnresolvedReferences
from . import pytestmark as _package_pytestmark  # noqa: F401

# Module-level pytestmark must combine the package marker with a
# file-specific xdist_group so all tests in this file land on the
# same worker (they share APIServer state — duplicate-record errors
# show up when ``--dist=loadgroup`` spreads them across workers).
pytestmark = [_package_pytestmark, pytest.mark.xdist_group("platform-apiserver_seq")]

# The role / role-permission / role-user endpoints (`/roles`...) are mounted
# only in `rbac` security mode. The pytest apiserver runs in the default
# `simple` mode (RBAC left dormant), so those endpoints are unmounted and the
# tests below hit "API endpoint not found". Skip them until the suite exercises
# an `rbac`-mode server.
skip_rbac_dormant = pytest.mark.skip(
    reason=(
        "RBAC endpoints are dormant: the pytest apiserver runs in `simple` "
        "security mode, so the `/roles` endpoints are not mounted."
    )
)

FUNCTION_TESTING_COLLECTION_NAME = "function_testing_collection"
FUNCTION_TESTING_COLLECTION_DESCRIPTION = "function_testing_collection_description"


def calculate_sha256(binary_data: bytes) -> str:
    sha256_hash = hashlib.sha256()
    sha256_hash.update(binary_data)
    return sha256_hash.hexdigest()


@pytest.mark.integration
@pytest.mark.requires_internet
def test_obtain_connection():
    connection = obtain_connection(APISERVER_URL, "admin", "tabsdata")
    real_url = f"http://{APISERVER_URL}{BASE_API_URL}"
    assert connection.url == real_url
    assert connection.bearer_token is not None
    assert connection.refresh_token is not None
    assert connection.__repr__() == f"APIServer('{real_url}')"
    assert str(connection) == real_url
    assert connection == APIServer(APISERVER_URL)
    assert connection != APISERVER_URL


@pytest.mark.integration
@pytest.mark.requires_internet
def test_authentication_access_success():
    apiserver_connection = obtain_connection(APISERVER_URL, "admin", "tabsdata")
    current_bearer = apiserver_connection.bearer_token
    response = apiserver_connection.authentication_login("admin", "tabsdata")
    assert HTTPStatus(response.status_code).is_success
    assert apiserver_connection.bearer_token is not None
    assert apiserver_connection.refresh_token is not None
    assert apiserver_connection.bearer_token != current_bearer


@pytest.mark.integration
@pytest.mark.requires_internet
def test_authentication_info(apiserver_connection):
    response = apiserver_connection.authentication_info()
    assert HTTPStatus(response.status_code).is_success


@pytest.mark.integration
@pytest.mark.requires_internet
def test_authentication_access_raises_error():
    apiserver_connection = obtain_connection(APISERVER_URL, "admin", "tabsdata")
    with pytest.raises(APIServerError):
        apiserver_connection.authentication_login("wrong_user", "wrong_password")


@pytest.mark.integration
@pytest.mark.requires_internet
def test_authentication_refresh_success():
    apiserver_connection = obtain_connection(APISERVER_URL, "admin", "tabsdata")
    current_bearer = apiserver_connection.bearer_token
    response = apiserver_connection.authentication_refresh()
    assert HTTPStatus(response.status_code).is_success
    assert apiserver_connection.bearer_token is not None
    assert apiserver_connection.refresh_token is not None
    assert apiserver_connection.bearer_token != current_bearer


@pytest.mark.integration
@pytest.mark.requires_internet
def test_authentication_refresh_fail():
    apiserver_connection = obtain_connection(APISERVER_URL, "admin", "tabsdata")
    apiserver_connection.refresh_token = "incorrect_token"
    with pytest.raises(APIServerError):
        apiserver_connection.authentication_refresh()


@pytest.mark.integration
@pytest.mark.requires_internet
def test_status_get(apiserver_connection):
    response = apiserver_connection.status_get()
    assert HTTPStatus(response.status_code).is_success


@pytest.mark.integration
@pytest.mark.requires_internet
def test_runtime_info_get(apiserver_connection):
    response = apiserver_connection.runtime_info_get()
    assert HTTPStatus(response.status_code).is_success


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_list(apiserver_connection):
    response = apiserver_connection.collection_list()
    assert HTTPStatus(response.status_code).is_success


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_list_with_params(apiserver_connection):
    response = apiserver_connection.collection_list(
        order_by="name", request_len=42, request_filter="name:eq:invent"
    )
    assert HTTPStatus(response.status_code).is_success
    list_params = response.json().get("data").get("list_params")
    assert list_params is not None
    assert list_params.get("len") == 42
    assert list_params.get("filter") == ["name:eq:invent"]
    assert list_params.get("order_by") == "name"


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_create_api(apiserver_connection):
    apiserver_connection.collection_delete(
        "test_collection_create_api", raise_for_status=False
    )
    try:
        response = apiserver_connection.collection_create(
            "test_collection_create_api", "test_collection_create_api_description"
        )
        assert HTTPStatus(response.status_code).is_success
        response_json = response.json().get("data")
        assert response_json.get("name") == "test_collection_create_api"
        assert (
            response_json.get("description") == "test_collection_create_api_description"
        )
    finally:
        apiserver_connection.collection_delete(
            "test_collection_create_api", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_delete_api(apiserver_connection):
    apiserver_connection.collection_create(
        "test_collection_delete_api",
        "test_collection_delete_api_description",
        raise_for_status=False,
    )
    response = apiserver_connection.collection_delete("test_collection_delete_api")
    assert HTTPStatus(response.status_code).is_success


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_delete_no_exists_raises_error(apiserver_connection):
    with pytest.raises(APIServerError):
        apiserver_connection.collection_delete("test_collection_delete_no_exists")


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_get(apiserver_connection):
    apiserver_connection.collection_create(
        "test_collection_get", "test_collection_get_description", raise_for_status=False
    )
    try:
        response = apiserver_connection.collection_get_by_name("test_collection_get")
        assert HTTPStatus(response.status_code).is_success
        response_json = response.json().get("data")
        assert response_json.get("name") == "test_collection_get"
        assert response_json.get("description") == "test_collection_get_description"
    finally:
        apiserver_connection.collection_delete(
            "test_collection_get", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_get_no_exists_raises_error(apiserver_connection):
    with pytest.raises(APIServerError):
        apiserver_connection.collection_get_by_name(
            "test_collection_get_no_exists_raises_error"
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_update_new_description(apiserver_connection):
    apiserver_connection.collection_create(
        "test_collection_update_new_description",
        "test_collection_update_new_description_description",
        raise_for_status=False,
    )
    try:
        response = apiserver_connection.collection_update(
            "test_collection_update_new_description",
            description="test_collection_update_new_description_new_description",
        )
        assert HTTPStatus(response.status_code).is_success
        response_json = response.json().get("data")
        assert (
            response_json.get("description")
            == "test_collection_update_new_description_new_description"
        )
    finally:
        apiserver_connection.collection_delete(
            "test_collection_update_new_description", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_update_new_name(apiserver_connection):
    apiserver_connection.collection_create(
        "test_collection_update_new_name",
        "test_collection_update_new_name_description",
        raise_for_status=False,
    )
    try:
        response = apiserver_connection.collection_update(
            "test_collection_update_new_name",
            new_collection_name="test_collection_update_new_name_new_name",
        )
        assert HTTPStatus(response.status_code).is_success
        response_json = response.json().get("data")
        assert response_json.get("name") == "test_collection_update_new_name_new_name"
    finally:
        apiserver_connection.collection_delete(
            "test_collection_update_new_name", raise_for_status=False
        )
        apiserver_connection.collection_delete(
            "test_collection_update_new_name_new_name", raise_for_status=False
        )


@skip_rbac_dormant
@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_list(apiserver_connection):
    response = apiserver_connection.role_list()
    assert HTTPStatus(response.status_code).is_success


@skip_rbac_dormant
@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_list_with_params(apiserver_connection):
    response = apiserver_connection.role_list(
        order_by="name", request_len=42, request_filter="name:eq:invent"
    )
    assert HTTPStatus(response.status_code).is_success
    list_params = response.json().get("data").get("list_params")
    assert list_params is not None
    assert list_params.get("len") == 42
    assert list_params.get("filter") == ["name:eq:invent"]
    assert list_params.get("order_by") == "name"


@skip_rbac_dormant
@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_create_api(apiserver_connection):
    apiserver_connection.role_delete("test_role_create_api", raise_for_status=False)
    try:
        response = apiserver_connection.role_create(
            "test_role_create_api", "test_role_create_api_description"
        )
        assert HTTPStatus(response.status_code).is_success
        response_json = response.json().get("data")
        assert response_json.get("name") == "test_role_create_api"
        assert response_json.get("description") == "test_role_create_api_description"
    finally:
        apiserver_connection.role_delete("test_role_create_api", raise_for_status=False)


@skip_rbac_dormant
@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_delete_api(apiserver_connection):
    apiserver_connection.role_create(
        "test_role_delete_api",
        "test_role_delete_api_description",
        raise_for_status=False,
    )
    response = apiserver_connection.role_delete("test_role_delete_api")
    assert HTTPStatus(response.status_code).is_success


@skip_rbac_dormant
@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_delete_no_exists_raises_error(apiserver_connection):
    with pytest.raises(APIServerError):
        apiserver_connection.role_delete("test_role_delete_no_exists")


@skip_rbac_dormant
@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_get(apiserver_connection):
    apiserver_connection.role_create(
        "test_role_get", "test_role_get_description", raise_for_status=False
    )
    try:
        response = apiserver_connection.role_get_by_name("test_role_get")
        assert HTTPStatus(response.status_code).is_success
        response_json = response.json().get("data")
        assert response_json.get("name") == "test_role_get"
        assert response_json.get("description") == "test_role_get_description"
    finally:
        apiserver_connection.role_delete("test_role_get", raise_for_status=False)


@skip_rbac_dormant
@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_get_no_exists_raises_error(apiserver_connection):
    with pytest.raises(APIServerError):
        apiserver_connection.role_get_by_name("test_role_get_no_exists_raises_error")


@skip_rbac_dormant
@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_update_new_description(apiserver_connection):
    apiserver_connection.role_create(
        "test_role_update_new_description",
        "test_role_update_new_description_description",
        raise_for_status=False,
    )
    try:
        response = apiserver_connection.role_update(
            "test_role_update_new_description",
            new_description="test_role_update_new_description_new_description",
        )
        assert HTTPStatus(response.status_code).is_success
        response_json = response.json().get("data")
        assert (
            response_json.get("description")
            == "test_role_update_new_description_new_description"
        )
    finally:
        apiserver_connection.role_delete(
            "test_role_update_new_description", raise_for_status=False
        )


@skip_rbac_dormant
@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_update_new_name(apiserver_connection):
    apiserver_connection.role_create(
        "test_role_update_new_name",
        "test_role_update_new_name_description",
        raise_for_status=False,
    )
    try:
        response = apiserver_connection.role_update(
            "test_role_update_new_name",
            new_name="test_role_update_new_name_new_name",
        )
        assert HTTPStatus(response.status_code).is_success
        response_json = response.json().get("data")
        assert response_json.get("name") == "test_role_update_new_name_new_name"
    finally:
        apiserver_connection.role_delete(
            "test_role_update_new_name", raise_for_status=False
        )
        apiserver_connection.role_delete(
            "test_role_update_new_name_new_name", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_users_list(apiserver_connection):
    response = apiserver_connection.users_list()
    assert HTTPStatus(response.status_code).is_success


@pytest.mark.integration
@pytest.mark.requires_internet
def test_users_create(apiserver_connection):
    apiserver_connection.users_delete("test_users_create", raise_for_status=False)
    name = "test_users_create"
    full_name = "test_users_create_full_name"
    email = "test_users_create_email@tabsdata.com"
    password = "test_users_create_password"
    enabled = True
    try:
        response = apiserver_connection.users_create(
            name, full_name, email, password, enabled
        )
        assert HTTPStatus(response.status_code).is_success
        response_json = response.json().get("data")
        assert response_json.get("name") == name
        assert response_json.get("full_name") == full_name
        assert response_json.get("email") == email
        assert response_json.get("enabled") == enabled
    finally:
        apiserver_connection.users_delete(name, raise_for_status=False)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_users_list_after_create(apiserver_connection):
    apiserver_connection.users_delete("test_users_list", raise_for_status=False)
    name = "test_users_list"
    full_name = "test_users_list_full_name"
    email = "test_users_list_email@tabsdata.com"
    password = "test_users_list_password"
    enabled = True
    try:
        response = apiserver_connection.users_create(
            name, full_name, email, password, enabled
        )
        assert HTTPStatus(response.status_code).is_success
        response = apiserver_connection.users_list()
        assert HTTPStatus(response.status_code).is_success
    finally:
        apiserver_connection.users_delete(name, raise_for_status=False)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_users_delete(apiserver_connection):
    name = "test_users_delete"
    full_name = "test_users_delete_full_name"
    email = "test_users_delete_email@tabsdata.com"
    password = "test_users_delete_password"
    enabled = True
    apiserver_connection.users_create(
        name,
        full_name,
        email,
        password,
        enabled,
        raise_for_status=False,
    )
    response = apiserver_connection.users_delete(name)
    assert HTTPStatus(response.status_code).is_success


@pytest.mark.integration
@pytest.mark.requires_internet
def test_users_delete_no_exists_raises_error(apiserver_connection):
    with pytest.raises(APIServerError):
        apiserver_connection.users_delete("test_users_delete_no_exists_raises_error")


@pytest.mark.integration
@pytest.mark.requires_internet
def test_users_get_by_name(apiserver_connection):
    name = "test_users_get_by_name"
    full_name = "test_users_get_by_name_full_name"
    email = "test_users_get_by_name_email@tabsdata.com"
    password = "test_users_get_by_name_password"
    enabled = True
    apiserver_connection.users_create(
        name,
        full_name,
        email,
        password,
        enabled,
        raise_for_status=False,
    )
    try:
        response = apiserver_connection.users_get_by_name(name)
        assert HTTPStatus(response.status_code).is_success
        response_json = response.json().get("data")
        assert response_json.get("name") == name
        assert response_json.get("full_name") == full_name
        assert response_json.get("email") == email
        assert response_json.get("enabled") == enabled
    finally:
        apiserver_connection.users_delete(name, raise_for_status=False)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_users_get_by_name_no_exists_raises_error(apiserver_connection):
    with pytest.raises(APIServerError):
        apiserver_connection.users_get_by_name(
            "test_users_get_by_name_no_exists_raises_error"
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_users_update_no_new_password(apiserver_connection):
    name = "test_users_update_no_new_password"
    full_name = "test_users_update_no_new_password_full_name"
    email = "test_users_update_no_new_password_email@tabsdata.com"
    password = "test_users_update_no_new_password_password"
    enabled = True
    apiserver_connection.users_create(
        name,
        full_name,
        email,
        password,
        enabled,
        raise_for_status=False,
    )
    try:
        response = apiserver_connection.users_update(
            name,
            full_name="test_users_update_no_new_password_new_full_name",
            email="test_users_update_no_new_password_new_email@tabsdata.com",
            enabled=False,
        )
        assert HTTPStatus(response.status_code).is_success
        response_json = response.json().get("data")
        assert response_json.get("name") == name
        assert (
            response_json.get("full_name")
            == "test_users_update_no_new_password_new_full_name"
        )
        assert (
            response_json.get("email")
            == "test_users_update_no_new_password_new_email@tabsdata.com"
        )
        assert not response_json.get("enabled")
    finally:
        apiserver_connection.users_delete(name, raise_for_status=False)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_users_update_change_fullname(apiserver_connection):
    name = "test_users_update_change_fullname"
    full_name = "test_users_update_change_fullname"
    email = "test_users_update_change_fullname_email@tabsdata.com"
    password = "test_users_update_change_fullname_password"
    enabled = True
    apiserver_connection.users_create(
        name,
        full_name,
        email,
        password,
        enabled,
        raise_for_status=False,
    )
    try:
        response = apiserver_connection.users_update(
            name,
            full_name="test_users_update_change_fullname_new_full_name",
        )
        assert HTTPStatus(response.status_code).is_success
        response_json = response.json().get("data")
        assert response_json.get("name") == name
        assert (
            response_json.get("full_name")
            == "test_users_update_change_fullname_new_full_name"
        )
        assert (
            response_json.get("email")
            == "test_users_update_change_fullname_email@tabsdata.com"
        )
        assert response_json.get("enabled")
    finally:
        apiserver_connection.users_delete(name, raise_for_status=False)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_in_collection_list(apiserver_connection):
    apiserver_connection.collection_create(
        FUNCTION_TESTING_COLLECTION_NAME,
        FUNCTION_TESTING_COLLECTION_DESCRIPTION,
        raise_for_status=False,
    )
    response = apiserver_connection.function_in_collection_list(
        FUNCTION_TESTING_COLLECTION_NAME
    )
    assert HTTPStatus(response.status_code).is_success


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_in_collection_list_with_params(apiserver_connection):
    apiserver_connection.collection_create(
        FUNCTION_TESTING_COLLECTION_NAME,
        FUNCTION_TESTING_COLLECTION_DESCRIPTION,
        raise_for_status=False,
    )
    response = apiserver_connection.function_in_collection_list(
        FUNCTION_TESTING_COLLECTION_NAME,
        order_by="name",
        request_len=42,
        request_filter="name:eq:invent",
    )
    assert HTTPStatus(response.status_code).is_success
    list_params = response.json().get("data").get("list_params")
    assert list_params is not None
    assert list_params.get("len") == 42
    assert list_params.get("filter") == ["name:eq:invent"]
    assert list_params.get("order_by") == "name"


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_in_collection_get_no_exists_raises_error(apiserver_connection):
    apiserver_connection.collection_create(
        FUNCTION_TESTING_COLLECTION_NAME,
        FUNCTION_TESTING_COLLECTION_DESCRIPTION,
        raise_for_status=False,
    )
    with pytest.raises(APIServerError):
        apiserver_connection.function_get(
            FUNCTION_TESTING_COLLECTION_NAME,
            "test_function_in_collection_get_no_exists_raises_error",
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_users_update_change_password(apiserver_connection):
    apiserver_connection.users_delete(
        "test_users_update_change_own_password", raise_for_status=False
    )
    name = "test_users_update_change_own_password"
    full_name = "test_users_update_change_own_password_full_name"
    email = "test_users_update_change_own_password_email@tabsdata.com"
    password = "test_users_update_change_own_password_password"
    enabled = True
    try:
        response = apiserver_connection.users_create(
            name, full_name, email, password, enabled
        )
        assert HTTPStatus(response.status_code).is_success
        response = apiserver_connection.users_update(
            name,
            password="test_users_update_change_own_password_new_password",
        )
        assert HTTPStatus(response.status_code).is_success
    finally:
        apiserver_connection.users_delete(name, raise_for_status=False)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_execution_list_api(apiserver_connection):
    response = apiserver_connection.execution_list()
    assert HTTPStatus(response.status_code).is_success


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_list_api(apiserver_connection):
    apiserver_connection.collection_create(
        FUNCTION_TESTING_COLLECTION_NAME,
        FUNCTION_TESTING_COLLECTION_DESCRIPTION,
        raise_for_status=False,
    )
    response = apiserver_connection.table_list(FUNCTION_TESTING_COLLECTION_NAME)
    assert HTTPStatus(response.status_code).is_success


@pytest.mark.integration
@pytest.mark.requires_internet
def test_transaction_list_api(apiserver_connection):
    response = apiserver_connection.transaction_list()
    assert HTTPStatus(response.status_code).is_success


@skip_rbac_dormant
@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_permission_list(apiserver_connection):
    role_name = "test_role_permission_list_api"
    role_description = "test_role_permission_list_api_description"
    permission_type = "sa"
    apiserver_connection.role_delete(role_name, raise_for_status=False)
    try:
        apiserver_connection.role_create(role_name, role_description)
        response = apiserver_connection.role_permission_create(
            role_name, permission_type
        )
        permission_id = response.json().get("data", {}).get("id")
        response = apiserver_connection.role_permission_list(role_name)
        listed_permission_ids = [
            permission.get("id")
            for permission in response.json().get("data", {}).get("data", [])
        ]
        assert permission_id in listed_permission_ids
    finally:
        apiserver_connection.role_delete(role_name, raise_for_status=False)


@skip_rbac_dormant
@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_permission_list_with_params(apiserver_connection):
    role_name = "test_role_permission_list_with_params_api"
    role_description = "test_role_permission_list_with_params_api_description"
    permission_type = "sa"
    apiserver_connection.role_delete(role_name, raise_for_status=False)
    try:
        apiserver_connection.role_create(role_name, role_description)
        apiserver_connection.role_permission_create(role_name, permission_type)
        response = apiserver_connection.role_permission_list(
            role_name, order_by="role", request_len=42, request_filter="role:eq:invent"
        )
        assert HTTPStatus(response.status_code).is_success
        list_params = response.json().get("data").get("list_params")
        assert list_params is not None
        assert list_params.get("len") == 42
        assert list_params.get("filter") == ["role:eq:invent"]
        assert list_params.get("order_by") == "role"
    finally:
        apiserver_connection.role_delete(role_name, raise_for_status=False)


@skip_rbac_dormant
@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_permission_create_api(apiserver_connection):
    role_name = "test_role_permission_create_api"
    role_description = "test_role_permission_create_api_description"
    permission_type = "sa"
    apiserver_connection.role_delete(role_name, raise_for_status=False)
    try:
        apiserver_connection.role_create(role_name, role_description)
        response = apiserver_connection.role_permission_create(
            role_name, permission_type
        )
        assert HTTPStatus(response.status_code).is_success
        response_json = response.json().get("data")
        assert response_json.get("permission_type") == permission_type
    finally:
        apiserver_connection.role_delete(role_name, raise_for_status=False)


@skip_rbac_dormant
@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_permission_delete_api(apiserver_connection):
    role_name = "test_role_permission_delete_api"
    role_description = "test_role_permission_delete_api_description"
    permission_type = "sa"
    apiserver_connection.role_delete(role_name, raise_for_status=False)
    try:
        apiserver_connection.role_create(role_name, role_description)
        response = apiserver_connection.role_permission_create(
            role_name, permission_type
        )
        permission_id = response.json().get("data", {}).get("id")
        response = apiserver_connection.role_permission_list(role_name)
        listed_permission_ids = [
            permission.get("id")
            for permission in response.json().get("data", {}).get("data", [])
        ]
        assert permission_id in listed_permission_ids
        response = apiserver_connection.role_permission_delete(role_name, permission_id)
        assert HTTPStatus(response.status_code).is_success
        response = apiserver_connection.role_permission_list(role_name)
        listed_permission_ids = [
            permission.get("id")
            for permission in response.json().get("data", {}).get("data", [])
        ]
        assert permission_id not in listed_permission_ids
    finally:
        apiserver_connection.role_delete(role_name, raise_for_status=False)


@skip_rbac_dormant
@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_permission_delete_no_exists_raises_error(apiserver_connection):
    role_name = "test_role_permission_delete_no_exists_api"
    role_description = "test_role_permission_delete_no_exists_api"
    apiserver_connection.role_delete(role_name, raise_for_status=False)
    try:
        apiserver_connection.role_create(role_name, role_description)
        with pytest.raises(Exception):
            apiserver_connection.role_permission_delete(
                role_name, "test_role_delete_no_exists"
            )
    finally:
        apiserver_connection.role_delete(role_name, raise_for_status=False)


@skip_rbac_dormant
@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_user_list(apiserver_connection):
    role_name = "test_role_user_list_api"
    role_description = "test_role_user_list_api_description"
    user_name = "test_role_user_list_api_user"
    apiserver_connection.users_delete(user_name, raise_for_status=False)
    apiserver_connection.role_delete(role_name, raise_for_status=False)
    try:
        apiserver_connection.role_create(role_name, role_description)
        apiserver_connection.users_create(
            user_name,
            "test_role_user_list_api_user_full_name",
            "fake@email.com",
            "fakepassword",
            True,
        )
        apiserver_connection.role_user_add(role_name, user_name)
        response = apiserver_connection.role_user_list(role_name)
        listed_users = [
            user.get("user") for user in response.json().get("data", {}).get("data", [])
        ]
        assert user_name in listed_users
    finally:
        apiserver_connection.users_delete(user_name, raise_for_status=False)
        apiserver_connection.role_delete(role_name, raise_for_status=False)


@skip_rbac_dormant
@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_user_list_with_params(apiserver_connection):
    role_name = "test_role_user_list_with_params_api"
    role_description = "test_role_user_list_with_params_api_description"
    user_name = "test_role_user_list_with_params_api_user"
    apiserver_connection.users_delete(user_name, raise_for_status=False)
    apiserver_connection.role_delete(role_name, raise_for_status=False)
    try:
        apiserver_connection.role_create(role_name, role_description)
        apiserver_connection.users_create(
            user_name,
            "test_role_user_list_with_params_api_user_full_name",
            "fake@email.com",
            "fakepassword",
            True,
        )
        apiserver_connection.role_user_add(role_name, user_name)
        response = apiserver_connection.role_user_list(
            role_name, order_by="role", request_len=42, request_filter="role:eq:invent"
        )
        assert HTTPStatus(response.status_code).is_success
        list_params = response.json().get("data").get("list_params")
        assert list_params is not None
        assert list_params.get("len") == 42
        assert list_params.get("filter") == ["role:eq:invent"]
        assert list_params.get("order_by") == "role"
    finally:
        apiserver_connection.users_delete(user_name, raise_for_status=False)
        apiserver_connection.role_delete(role_name, raise_for_status=False)


@skip_rbac_dormant
@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_user_add_api(apiserver_connection):
    role_name = "test_role_user_create_api"
    role_description = "test_role_user_create_api_description"
    user_name = "test_role_user_add_api_user"
    apiserver_connection.users_delete(user_name, raise_for_status=False)
    apiserver_connection.role_delete(role_name, raise_for_status=False)
    try:
        apiserver_connection.role_create(role_name, role_description)
        apiserver_connection.users_create(
            user_name,
            "test_role_user_list_with_params_api_user_full_name",
            "fake@email.com",
            "fakepassword",
            True,
        )
        response = apiserver_connection.role_user_list(role_name)
        listed_users = [
            user.get("user") for user in response.json().get("data", {}).get("data", [])
        ]
        assert user_name not in listed_users
        response = apiserver_connection.role_user_add(role_name, user_name)
        assert HTTPStatus(response.status_code).is_success
        response = apiserver_connection.role_user_list(role_name)
        listed_users = [
            user.get("user") for user in response.json().get("data", {}).get("data", [])
        ]
        assert user_name in listed_users
    finally:
        apiserver_connection.users_delete(user_name, raise_for_status=False)
        apiserver_connection.role_delete(role_name, raise_for_status=False)


@skip_rbac_dormant
@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_user_delete_api(apiserver_connection):
    role_name = "test_role_user_delete_api"
    role_description = "test_role_user_delete_api_description"
    user_name = "test_role_user_delete_api_user"
    apiserver_connection.users_delete(user_name, raise_for_status=False)
    apiserver_connection.role_delete(role_name, raise_for_status=False)
    try:
        apiserver_connection.role_create(role_name, role_description)
        apiserver_connection.users_create(
            user_name,
            "test_role_user_list_with_params_api_user_full_name",
            "fake@email.com",
            "fakepassword",
            True,
        )
        apiserver_connection.role_user_add(role_name, user_name)
        response = apiserver_connection.role_user_list(role_name)
        listed_users = [
            user.get("user") for user in response.json().get("data", {}).get("data", [])
        ]
        assert user_name in listed_users
        response = apiserver_connection.role_user_delete(role_name, user_name)
        assert HTTPStatus(response.status_code).is_success
        response = apiserver_connection.role_user_list(role_name)
        listed_users = [
            user.get("user") for user in response.json().get("data", {}).get("data", [])
        ]
        assert user_name not in listed_users
    finally:
        apiserver_connection.users_delete(user_name, raise_for_status=False)
        apiserver_connection.role_delete(role_name, raise_for_status=False)


@skip_rbac_dormant
@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_user_delete_no_exists_raises_error(apiserver_connection):
    role_name = "test_role_user_delete_no_exists_api"
    role_description = "test_role_user_delete_no_exists_api"
    user_name = "test_role_user_delete_no_exists_api_user"
    apiserver_connection.users_delete(user_name, raise_for_status=False)
    apiserver_connection.role_delete(role_name, raise_for_status=False)
    try:
        apiserver_connection.role_create(role_name, role_description)
        with pytest.raises(Exception):
            apiserver_connection.role_user_delete(role_name, user_name)
    finally:
        apiserver_connection.role_delete(role_name, raise_for_status=False)


# ---------------------------------------------------------------------------
# Unit tests for HTTP session lifecycle
# ---------------------------------------------------------------------------


def _count_adapters(session: requests.Session) -> int:
    """Count the mounted adapters on a session (non-closed session has >= 2)."""
    return len(session.adapters)


class TestHttpSessionNotClosedAfterRequest:
    """Regression: HTTP sessions must not be closed after each request.

    The global GET_HTTP_SESSION and DEFAULT_HTTP_SESSION carry retry strategies
    and connection pool settings. Using them inside a ``with`` block closes
    them after the first request, losing those settings for all subsequent
    requests.
    """

    @patch.dict("os.environ", {"PYTEST_CURRENT_TEST": "fake_test"})
    def test_get_session_not_closed_after_get(self):
        adapters_before = _count_adapters(GET_HTTP_SESSION)
        assert adapters_before >= 2

        server = APIServer("http://localhost:0")
        mock_response = MagicMock()
        mock_response.status_code = 200

        # Global path: this test is about session/adapter lifecycle, not tenant
        # routing, so it deliberately avoids a project-scoped path.
        with patch.object(GET_HTTP_SESSION, "get", return_value=mock_response):
            server.get("/status")

        adapters_after = _count_adapters(GET_HTTP_SESSION)
        assert adapters_after == adapters_before, (
            f"GET session lost adapters after request: {adapters_before} -> "
            f"{adapters_after}. Session was likely closed via 'with' block."
        )

    @patch.dict("os.environ", {"PYTEST_CURRENT_TEST": "fake_test"})
    def test_default_session_not_closed_after_post(self):
        adapters_before = _count_adapters(DEFAULT_HTTP_SESSION)
        assert adapters_before >= 2

        server = APIServer("http://localhost:0")
        mock_response = MagicMock()
        mock_response.status_code = 200

        # Global path: lifecycle test, not tenant routing.
        with patch.object(DEFAULT_HTTP_SESSION, "post", return_value=mock_response):
            server.post("/status", json={"key": "value"})

        adapters_after = _count_adapters(DEFAULT_HTTP_SESSION)
        assert adapters_after == adapters_before, (
            f"DEFAULT session lost adapters after request: {adapters_before} -> "
            f"{adapters_after}. Session was likely closed via 'with' block."
        )

    @patch.dict("os.environ", {"PYTEST_CURRENT_TEST": "fake_test"})
    def test_default_session_not_closed_after_delete(self):
        adapters_before = _count_adapters(DEFAULT_HTTP_SESSION)
        assert adapters_before >= 2

        server = APIServer("http://localhost:0")
        mock_response = MagicMock()
        mock_response.status_code = 200

        # Global path: lifecycle test, not tenant routing.
        with patch.object(DEFAULT_HTTP_SESSION, "delete", return_value=mock_response):
            server.delete("/status")

        adapters_after = _count_adapters(DEFAULT_HTTP_SESSION)
        assert adapters_after == adapters_before, (
            f"DEFAULT session lost adapters after request: {adapters_before} -> "
            f"{adapters_after}. Session was likely closed via 'with' block."
        )

    @patch.dict("os.environ", {"PYTEST_CURRENT_TEST": "fake_test"})
    def test_sessions_survive_multiple_requests(self):
        server = APIServer("http://localhost:0")
        mock_response = MagicMock()
        mock_response.status_code = 200

        # Global path: lifecycle test, not tenant routing.
        with (
            patch.object(GET_HTTP_SESSION, "get", return_value=mock_response),
            patch.object(DEFAULT_HTTP_SESSION, "post", return_value=mock_response),
        ):
            for _ in range(5):
                server.get("/status")
                server.post("/status", json={})

        assert _count_adapters(GET_HTTP_SESSION) >= 2, (
            "GET session closed after multiple requests"
        )
        assert _count_adapters(DEFAULT_HTTP_SESSION) >= 2, (
            "DEFAULT session closed after multiple requests"
        )


# ---------------------------------------------------------------------------
# Unit tests for APIServerError input validation
# ---------------------------------------------------------------------------


class TestAPIServerError:
    def test_from_dict(self):
        err = APIServerError({
            "code": "AUTH_001",
            "error": "unauthorized",
            "error_description": "Invalid credentials",
        })
        assert err.code == "AUTH_001"
        assert err.error == "unauthorized"
        assert err.error_description == "Invalid credentials"
        assert str(err) == "Invalid credentials"

    def test_from_dict_missing_fields(self):
        err = APIServerError({})
        assert err.code is None
        assert err.error is None
        assert err.error_description is None
        assert str(err) == "Unknown error"

    def test_from_string(self):
        err = APIServerError("something went wrong")
        assert err.error_description == "something went wrong"
        assert str(err) == "something went wrong"

    def test_from_none(self):
        err = APIServerError(None)
        assert err.error_description == "None"
        assert str(err) == "None"

    def test_from_integer(self):
        err = APIServerError(500)
        assert err.error_description == "500"
        assert str(err) == "500"

    def test_from_list(self):
        err = APIServerError(["error1", "error2"])
        assert err.error_description == "['error1', 'error2']"


# ---------------------------------------------------------------------------
# Unit tests for certificate path resolution
# ---------------------------------------------------------------------------


class TestObtainCertificateFilePath:
    def test_valid_url_with_port(self):
        result = _obtain_certificate_file_path("https://example.com:8443")
        assert "example_com_8443_cert.pem" in result

    def test_valid_url_without_port_uses_default(self):
        result = _obtain_certificate_file_path("https://example.com")
        assert "example_com_" in result
        assert "_cert.pem" in result

    def test_invalid_url_raises_valueerror(self):
        with pytest.raises(ValueError, match="Invalid URL"):
            _obtain_certificate_file_path("://not-a-url")

    def test_empty_string_raises_valueerror(self):
        with pytest.raises(ValueError, match="Invalid URL"):
            _obtain_certificate_file_path("")


# ---------------------------------------------------------------------------
# Unit tests for authentication_login response parsing
# ---------------------------------------------------------------------------


class TestAuthenticationLoginResponseParsing:
    """Verify that authentication_login parses response.json() only once."""

    @patch.dict("os.environ", {"PYTEST_CURRENT_TEST": "fake_test"})
    def test_login_parses_json_once(self):
        server = APIServer("http://localhost:0")
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "tok_access",
            "refresh_token": "tok_refresh",
            "token_type": "bearer",
            "expires_in": 3600,
        }

        with patch.object(DEFAULT_HTTP_SESSION, "post", return_value=mock_response):
            server.authentication_login("user", "pass")

        assert server.bearer_token == "tok_access"
        assert server.refresh_token == "tok_refresh"
        assert server.token_type == "bearer"
        assert server.expires_in == 3600
        # json() should be called exactly once (not 4+ times)
        assert mock_response.json.call_count == 1

    @patch.dict("os.environ", {"PYTEST_CURRENT_TEST": "fake_test"})
    def test_login_failure_raises_api_server_error(self):
        server = APIServer("http://localhost:0")
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.json.return_value = {
            "code": "AUTH_001",
            "error": "unauthorized",
            "error_description": "Bad credentials",
        }

        with patch.object(DEFAULT_HTTP_SESSION, "post", return_value=mock_response):
            with pytest.raises(APIServerError, match="Bad credentials"):
                server.authentication_login("user", "wrong")

        assert mock_response.json.call_count == 1


# ---------------------------------------------------------------------------
# Unit tests for Collection.list_functions_generator collection injection
# ---------------------------------------------------------------------------


class TestListFunctionsCollectionInjection:
    """Verify that list_functions_generator injects collection when missing."""

    def _make_collection(self):
        connection = MagicMock()
        return Collection(connection=connection, name="test_coll")

    def test_injects_collection_when_missing(self):
        coll = self._make_collection()
        coll.connection.function_in_collection_list.return_value.json.return_value = {
            "data": {
                "next_pagination_id": None,
                "next": None,
                "data": [
                    {"name": "fn1"},
                    {"name": "fn2"},
                ],
            }
        }
        functions = list(coll.list_functions_generator())
        assert len(functions) == 2
        for fn in functions:
            assert isinstance(fn, Function)
            assert fn.collection is coll

    def test_preserves_existing_collection(self):
        coll = self._make_collection()
        coll.connection.function_in_collection_list.return_value.json.return_value = {
            "data": {
                "next_pagination_id": None,
                "next": None,
                "data": [
                    {"name": "fn1", "collection": "already_set"},
                ],
            }
        }
        functions = list(coll.list_functions_generator())
        assert len(functions) == 1
        # collection is NOT overwritten — it keeps the API-provided value
        assert functions[0].collection is not coll


# ---------------------------------------------------------------------------
# Unit tests for pagination with empty/null data
# ---------------------------------------------------------------------------


class TestPaginationEmptyData:
    """Verify that pagination handles None or empty data gracefully."""

    def _make_collection(self):
        connection = MagicMock()
        return Collection(connection=connection, name="test_coll")

    def test_list_functions_with_null_data_field(self):
        coll = self._make_collection()
        coll.connection.function_in_collection_list.return_value.json.return_value = {
            "data": {
                "next_pagination_id": None,
                "next": None,
                "data": None,
            }
        }
        functions = list(coll.list_functions_generator())
        assert functions == []

    def test_list_functions_with_empty_data_field(self):
        coll = self._make_collection()
        coll.connection.function_in_collection_list.return_value.json.return_value = {
            "data": {
                "next_pagination_id": None,
                "next": None,
                "data": [],
            }
        }
        functions = list(coll.list_functions_generator())
        assert functions == []

    def test_list_functions_with_missing_data_field(self):
        coll = self._make_collection()
        coll.connection.function_in_collection_list.return_value.json.return_value = {
            "data": {
                "next_pagination_id": None,
                "next": None,
            }
        }
        functions = list(coll.list_functions_generator())
        assert functions == []


# ---------------------------------------------------------------------------
# Unit tests for TemporaryDirectory cleanup in register_function
# ---------------------------------------------------------------------------


class TestRegisterFunctionTempDirCleanup:
    """Verify that register_function cleans up temp directories on failure."""

    def test_temp_dir_cleaned_up_on_exception(self):
        import os

        connection = MagicMock()
        connection.runtime_info_get.return_value.json.return_value = {
            "data": {"python_versions": ["3.12"]}
        }
        coll = Collection(connection=connection, name="test_coll")

        temp_dir_path = None
        original_init = tempfile.TemporaryDirectory.__init__

        def capture_init(self_td, *args, **kwargs):
            original_init(self_td, *args, **kwargs)
            nonlocal temp_dir_path
            temp_dir_path = self_td.name

        with (
            patch.object(tempfile.TemporaryDirectory, "__init__", capture_init),
            patch(
                "tabsdata.api.tabsdata_server._create_archive",
                side_effect=RuntimeError("archive failed"),
            ),
        ):
            with pytest.raises(RuntimeError, match="archive failed"):
                coll.register_function(
                    function_path="dummy/path.py::func",
                    raise_for_status=True,
                )

        assert temp_dir_path is not None
        assert not os.path.exists(temp_dir_path), (
            f"Temp directory {temp_dir_path} was not cleaned up after exception"
        )


# ---------------------------------------------------------------------------
# Unit tests for project scoping
# ---------------------------------------------------------------------------


class TestScopedPathBuilding:
    """`_scoped` builds ``/projects/{project}/...`` paths from the connection.

    Scoping is a per-endpoint decision: name-keyed resources build their path
    through ``_scoped``; everything else (globals and id-keyed resources) goes
    out verbatim. With no project selected a scoped call must fail fast rather
    than guessing a default.
    """

    def test_scoped_requires_a_project(self):
        server = APIServer("http://localhost:0")
        assert server.project is None
        with pytest.raises(NoProjectSelectedError):
            server._scoped("/collections")

    def test_scoped_prefixes_the_active_project(self):
        server = APIServer("http://localhost:0")
        server.project = "acme"
        assert server._scoped("/collections") == "/projects/acme/collections"
        assert (
            server._scoped("/collections/c1/functions")
            == "/projects/acme/collections/c1/functions"
        )
        assert server._scoped("/metrics") == "/projects/acme/metrics"

    def test_project_narrow_appends_filter_when_project_set(self):
        server = APIServer("http://localhost:0")
        assert server._project_narrow(None) is None
        assert server._project_narrow("status:eq:D") == "status:eq:D"
        server.project = "acme"
        with patch.object(server, "_active_project_name", return_value="acme"):
            assert server._project_narrow(None) == ["project:eq:acme"]
            assert server._project_narrow("status:eq:D") == [
                "status:eq:D",
                "project:eq:acme",
            ]
            assert server._project_narrow(["status:eq:D"]) == [
                "status:eq:D",
                "project:eq:acme",
            ]


class TestProjectScopingOnTheWire:
    """The active project rides on the connection into scoped request URLs."""

    @patch.dict("os.environ", {"PYTEST_CURRENT_TEST": "fake_test"})
    def test_scoped_request_url_carries_project(self):
        server = APIServer("http://localhost:0")
        server.project = "acme"
        mock_response = MagicMock(status_code=200)
        with patch.object(GET_HTTP_SESSION, "get", return_value=mock_response) as m:
            server.group_list(raise_for_status=False)
        url = m.call_args.args[0]
        assert url == f"{server.url}/projects/acme/groups"

    @patch.dict("os.environ", {"PYTEST_CURRENT_TEST": "fake_test"})
    def test_global_request_url_has_no_project(self):
        server = APIServer("http://localhost:0")
        server.project = "acme"
        mock_response = MagicMock(status_code=200)
        with patch.object(GET_HTTP_SESSION, "get", return_value=mock_response) as m:
            server.role_list(raise_for_status=False)
        url = m.call_args.args[0]
        assert url == f"{server.url}/roles"

    @patch.dict("os.environ", {"PYTEST_CURRENT_TEST": "fake_test"})
    def test_id_keyed_request_url_has_no_project(self):
        server = APIServer("http://localhost:0")
        server.project = "acme"
        mock_response = MagicMock(status_code=200)
        with patch.object(GET_HTTP_SESSION, "get", return_value=mock_response) as m:
            server.execution_read("e1", raise_for_status=False)
        url = m.call_args.args[0]
        assert url == f"{server.url}/executions/e1"

    @patch.dict("os.environ", {"PYTEST_CURRENT_TEST": "fake_test"})
    def test_list_narrows_to_active_project(self):
        server = APIServer("http://localhost:0")
        server.project = "acme"
        mock_response = MagicMock(status_code=200)
        with patch.object(GET_HTTP_SESSION, "get", return_value=mock_response) as m:
            server.workers_list(raise_for_status=False)
        assert m.call_args.args[1] == {"filter": ["project:eq:acme"]}

    @patch.dict("os.environ", {"PYTEST_CURRENT_TEST": "fake_test"})
    def test_scoped_request_without_project_fails_fast(self):
        server = APIServer("http://localhost:0")
        # No project set and no network call should happen.
        with patch.object(GET_HTTP_SESSION, "get") as m:
            with pytest.raises(NoProjectSelectedError):
                server.group_list(raise_for_status=False)
        m.assert_not_called()


class TestEndpointScopeClassification:
    """Every endpoint the client builds must be consciously classified.

    Scoping is a per-endpoint decision: a project-scoped path is built through
    ``_scoped`` and anything else goes out verbatim. This pins the reviewed
    split: a new top-level prefix fails here until it is added to the matching
    set below, confirmed against the server's ``SCOPED`` routes in td-objects
    ``rest_urls``.
    """

    # `/collections` appears in both sets: the cross-project list is flat,
    # while the name-keyed CRUD stays under `/projects/{project}`.
    SCOPED_PREFIXES = {
        "/collections",
        "/groups",
        "/metrics",
        "/inflight",
    }
    GLOBAL_PREFIXES = {
        "/auth",
        "/collections",
        "/executions",
        "/function-runs",
        "/logical-function-runs",
        "/permission-grants",
        "/projects",
        "/roles",
        "/runtime-info",
        "/status",
        "/transactions",
        "/users",
        "/workers",
    }

    @staticmethod
    def _leading_literal(node):
        """The leading string literal of a str constant or an f-string."""
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.JoinedStr) and node.values:
            head = node.values[0]
            if isinstance(head, ast.Constant) and isinstance(head.value, str):
                return head.value
        return None

    @classmethod
    def _classified_literal(cls, node):
        """`(leading_literal, scoped)` for a path expression, else `None`.

        A path built as ``self._scoped(...)`` is scoped; a bare literal or
        f-string goes out verbatim.
        """
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "_scoped"
            and node.args
        ):
            lit = cls._leading_literal(node.args[0])
            return (lit, True) if lit else None
        lit = cls._leading_literal(node)
        return (lit, False) if lit else None

    @classmethod
    def _endpoint_prefixes(cls):
        """`(prefix, scoped)` pairs for every endpoint built in `APIServer`.

        Scrapes the source rather than calling each method: collects
        `endpoint = ...` assignments and the first argument of
        `self.{get,post,delete,post_binary}(...)` calls, then reduces each
        path to its first segment.
        """
        import tabsdata.api.apiserver as apiserver_module

        source = pathlib.Path(apiserver_module.__file__).read_text()
        tree = ast.parse(source)
        api_cls = next(
            n
            for n in tree.body
            if isinstance(n, ast.ClassDef) and n.name == "APIServer"
        )
        classified = []
        for fn in api_cls.body:
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if fn.name in {"get", "post", "post_binary", "delete", "_scoped"}:
                continue
            for sub in ast.walk(fn):
                if isinstance(sub, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id == "endpoint" for t in sub.targets
                ):
                    entry = cls._classified_literal(sub.value)
                    if entry:
                        classified.append(entry)
                elif (
                    isinstance(sub, ast.Call)
                    and isinstance(sub.func, ast.Attribute)
                    and sub.func.attr in {"get", "post", "delete", "post_binary"}
                    and sub.args
                ):
                    entry = cls._classified_literal(sub.args[0])
                    if entry:
                        classified.append(entry)
        prefixes = set()
        for lit, scoped in classified:
            if lit.startswith("/"):
                seg = lit.lstrip("/").split("/", 1)[0]
                if seg:
                    prefixes.add(("/" + seg, scoped))
        return prefixes

    def test_every_endpoint_prefix_is_classified(self):
        known = {(p, True) for p in self.SCOPED_PREFIXES} | {
            (p, False) for p in self.GLOBAL_PREFIXES
        }
        unclassified = self._endpoint_prefixes() - known
        assert not unclassified, (
            f"Unclassified or misclassified endpoint prefixes: "
            f"{sorted(unclassified)}. Scoped paths must be built via "
            f"`self._scoped(...)` and listed in SCOPED_PREFIXES; verbatim paths "
            f"must be listed in GLOBAL_PREFIXES, each confirmed against the "
            f"server's rest_urls SCOPED routes."
        )

    def test_reviewed_prefixes_are_actually_used(self):
        # Keep the reviewed lists from rotting: every listed prefix must still
        # be built somewhere in the client.
        used = {p for p, _ in self._endpoint_prefixes()}
        stale = (self.SCOPED_PREFIXES | self.GLOBAL_PREFIXES) - used
        assert not stale, f"prefixes no longer used by the client: {sorted(stale)}"
