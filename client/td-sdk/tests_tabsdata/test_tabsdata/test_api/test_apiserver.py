#
# Copyright 2024 Tabs Data Inc.
#

import hashlib
from http import HTTPStatus

import pytest

from tabsdata.api.apiserver import (
    BASE_API_URL,
    APIServer,
    APIServerError,
    obtain_connection,
)
from tests_tabsdata.conftest import APISERVER_URL

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

FUNCTION_TESTING_COLLECTION_NAME = "function_testing_collection"
FUNCTION_TESTING_COLLECTION_DESCRIPTION = "function_testing_collection_description"


def calculate_sha256(binary_data: bytes) -> str:
    sha256_hash = hashlib.sha256()
    sha256_hash.update(binary_data)
    return sha256_hash.hexdigest()


@pytest.mark.integration
@pytest.mark.requires_internet
def test_obtain_connection():
    connection = obtain_connection(APISERVER_URL, "admin", "tabsdata", "sys_admin")
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
    apiserver_connection = obtain_connection(
        APISERVER_URL, "admin", "tabsdata", "sys_admin"
    )
    current_bearer = apiserver_connection.bearer_token
    response = apiserver_connection.authentication_login(
        "admin", "tabsdata", role="sys_admin"
    )
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
    apiserver_connection = obtain_connection(
        APISERVER_URL, "admin", "tabsdata", "sys_admin"
    )
    with pytest.raises(APIServerError):
        apiserver_connection.authentication_login("wrong_user", "wrong_password")


@pytest.mark.integration
@pytest.mark.requires_internet
def test_authentication_refresh_success():
    apiserver_connection = obtain_connection(
        APISERVER_URL, "admin", "tabsdata", "sys_admin"
    )
    current_bearer = apiserver_connection.bearer_token
    response = apiserver_connection.authentication_refresh()
    assert HTTPStatus(response.status_code).is_success
    assert apiserver_connection.bearer_token is not None
    assert apiserver_connection.refresh_token is not None
    assert apiserver_connection.bearer_token != current_bearer


@pytest.mark.integration
@pytest.mark.requires_internet
def test_authentication_refresh_fail():
    apiserver_connection = obtain_connection(
        APISERVER_URL, "admin", "tabsdata", "sys_admin"
    )
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


@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_list(apiserver_connection):
    response = apiserver_connection.role_list()
    assert HTTPStatus(response.status_code).is_success


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


@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_delete_no_exists_raises_error(apiserver_connection):
    with pytest.raises(APIServerError):
        apiserver_connection.role_delete("test_role_delete_no_exists")


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


@pytest.mark.integration
@pytest.mark.requires_internet
def test_role_get_no_exists_raises_error(apiserver_connection):
    with pytest.raises(APIServerError):
        apiserver_connection.role_get_by_name("test_role_get_no_exists_raises_error")


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
def test_users_list(apiserver_connection):
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


@pytest.mark.integration
@pytest.mark.requires_internet
def test_inter_coll_perm_list(apiserver_connection):
    coll_a = "test_inter_coll_perm_list_api_coll_a"
    coll_b = "test_inter_coll_perm_list_api_coll_b"
    apiserver_connection.collection_delete(coll_a, raise_for_status=False)
    apiserver_connection.collection_delete(coll_b, raise_for_status=False)
    try:
        apiserver_connection.collection_create(coll_a, "Collection A")
        apiserver_connection.collection_create(coll_b, "Collection B")
        response = apiserver_connection.authz_inter_coll_perm_create(coll_a, coll_b)
        assert HTTPStatus(response.status_code).is_success
        permission_id = response.json().get("data", {}).get("id")
        response = apiserver_connection.authz_inter_coll_perm_list(coll_a)
        listed_permissions = [
            permission.get("id")
            for permission in response.json().get("data", {}).get("data", [])
        ]
        assert permission_id in listed_permissions
    finally:
        apiserver_connection.collection_delete(coll_a, raise_for_status=False)
        apiserver_connection.collection_delete(coll_b, raise_for_status=False)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_inter_coll_perm_list_with_params(apiserver_connection):
    coll_a = "test_inter_coll_perm_list_with_params_api_coll_a"
    coll_b = "test_inter_coll_perm_list_with_params_api_coll_b"
    apiserver_connection.collection_delete(coll_a, raise_for_status=False)
    apiserver_connection.collection_delete(coll_b, raise_for_status=False)
    try:
        apiserver_connection.collection_create(coll_a, "Collection A")
        apiserver_connection.collection_create(coll_b, "Collection B")
        response = apiserver_connection.authz_inter_coll_perm_create(coll_a, coll_b)
        assert HTTPStatus(response.status_code).is_success
        response = apiserver_connection.authz_inter_coll_perm_list(
            coll_a,
            order_by="to_collection",
            request_len=42,
            request_filter="to_collection:eq:invent",
        )
        assert HTTPStatus(response.status_code).is_success
        list_params = response.json().get("data").get("list_params")
        assert list_params is not None
        assert list_params.get("len") == 42
        assert list_params.get("filter") == ["to_collection:eq:invent"]
        assert list_params.get("order_by") == "to_collection"
    finally:
        apiserver_connection.collection_delete(coll_a, raise_for_status=False)
        apiserver_connection.collection_delete(coll_b, raise_for_status=False)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_inter_coll_perm_create_api(apiserver_connection):
    coll_a = "test_inter_coll_perm_create_api_coll_a"
    coll_b = "test_inter_coll_perm_create_api_coll_b"
    apiserver_connection.collection_delete(coll_a, raise_for_status=False)
    apiserver_connection.collection_delete(coll_b, raise_for_status=False)
    try:
        apiserver_connection.collection_create(coll_a, "Collection A")
        apiserver_connection.collection_create(coll_b, "Collection B")
        response = apiserver_connection.authz_inter_coll_perm_create(coll_a, coll_b)
        assert HTTPStatus(response.status_code).is_success
    finally:
        apiserver_connection.collection_delete(coll_a, raise_for_status=False)
        apiserver_connection.collection_delete(coll_b, raise_for_status=False)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_inter_coll_perm_delete_api(apiserver_connection):
    coll_a = "test_inter_coll_perm_delete_api_coll_a"
    coll_b = "test_inter_coll_perm_delete_api_coll_b"
    apiserver_connection.collection_delete(coll_a, raise_for_status=False)
    apiserver_connection.collection_delete(coll_b, raise_for_status=False)
    try:
        apiserver_connection.collection_create(coll_a, "Collection A")
        apiserver_connection.collection_create(coll_b, "Collection B")
        response = apiserver_connection.authz_inter_coll_perm_create(coll_a, coll_b)
        assert HTTPStatus(response.status_code).is_success
        permission_id = response.json().get("data", {}).get("id")
        response = apiserver_connection.authz_inter_coll_perm_list(coll_a)
        listed_permissions = [
            permission.get("id")
            for permission in response.json().get("data", {}).get("data", [])
        ]
        assert permission_id in listed_permissions
        response = apiserver_connection.authz_inter_coll_perm_delete(
            coll_a, permission_id
        )
        assert HTTPStatus(response.status_code).is_success
        response = apiserver_connection.authz_inter_coll_perm_list(coll_a)
        listed_permissions = [
            permission.get("id")
            for permission in response.json().get("data", {}).get("data", [])
        ]
        assert permission_id not in listed_permissions
    finally:
        apiserver_connection.collection_delete(coll_a, raise_for_status=False)
        apiserver_connection.collection_delete(coll_b, raise_for_status=False)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_inter_coll_perm_delete_no_exists_raises_error(apiserver_connection):
    coll_a = "test_inter_coll_perm_delete_no_exists_api_coll_a"
    apiserver_connection.collection_delete(coll_a, raise_for_status=False)
    try:
        apiserver_connection.collection_create(coll_a, "Collection A")
        with pytest.raises(Exception):
            apiserver_connection.authz_inter_coll_perm_delete(coll_a, "does_not_exist")
    finally:
        apiserver_connection.collection_delete(coll_a, raise_for_status=False)
