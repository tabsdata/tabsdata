#
# Copyright 2024 Tabs Data Inc.
#

import hashlib

import pytest

from tabsdata.api.api_server import APIServer, APIServerError, obtain_connection
from tests.conftest import API_SERVER_URL

FUNCTION_TESTING_Collection_NAME = "function_testing_collection"
FUNCTION_TESTING_Collection_DESCRIPTION = "function_testing_collection_description"


def calculate_sha256(binary_data: bytes) -> str:
    sha256_hash = hashlib.sha256()
    sha256_hash.update(binary_data)
    return sha256_hash.hexdigest()


@pytest.mark.integration
@pytest.mark.requires_internet
def test_obtain_connection():
    connection = obtain_connection(API_SERVER_URL, "admin", "tabsdata")
    real_url = f"http://{API_SERVER_URL}"
    assert connection.url == real_url
    assert connection.bearer_token is not None
    assert connection.refresh_token is not None
    assert connection.__repr__() == f"APIServer('{real_url}')"
    assert str(connection) == real_url
    assert connection == APIServer(API_SERVER_URL)
    assert connection != API_SERVER_URL


@pytest.mark.integration
@pytest.mark.requires_internet
def test_authentication_access_success(api_server_connection):
    current_bearer = api_server_connection.bearer_token
    response = api_server_connection.authentication_access("admin", "tabsdata")
    assert response.status_code == 200
    assert api_server_connection.bearer_token is not None
    assert api_server_connection.refresh_token is not None
    assert api_server_connection.bearer_token != current_bearer


@pytest.mark.integration
@pytest.mark.requires_internet
def test_authentication_access_raises_error(api_server_connection):
    with pytest.raises(APIServerError):
        api_server_connection.authentication_access("wrong_user", "wrong_password")


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.skip(
    reason=(
        "This test is not working due to a backend bug. The backend "
        "is returning a 'Failed to fetch' error."
    )
)
def test_authentication_refresh_success(api_server_connection):
    current_bearer = api_server_connection.bearer_token
    response = api_server_connection.authentication_refresh()
    assert response.status_code == 200
    assert api_server_connection.bearer_token is not None
    assert api_server_connection.refresh_token is not None
    assert api_server_connection.bearer_token != current_bearer


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.skip(
    reason=(
        "This test is not working due to a backend bug: the backend "
        "is always providing a new token, even if the refresh token "
        "is incorrect."
    )
)
def test_authentication_refresh_fail(api_server_connection):
    api_server_connection.refresh_token = "incorrect_token"
    with pytest.raises(APIServerError):
        api_server_connection.authentication_refresh()


@pytest.mark.integration
@pytest.mark.requires_internet
def test_status_get(api_server_connection):
    response = api_server_connection.status_get()
    assert response.status_code == 200


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_list(api_server_connection):
    response = api_server_connection.collection_list()
    assert response.status_code == 200


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_list_with_params(api_server_connection):
    response = api_server_connection.collection_list(
        offset=10, len=42, filter="hi", order_by="hello"
    )
    assert response.status_code == 200
    list_params = response.json().get("list_params")
    assert list_params is not None
    assert list_params.get("offset") == 10
    assert list_params.get("len") == 42
    assert list_params.get("filter") == "hi"
    assert list_params.get("order_by") == "hello"


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_create_api(api_server_connection):
    api_server_connection.collection_delete(
        "test_collection_create_api", raise_for_status=False
    )
    try:
        response = api_server_connection.collection_create(
            "test_collection_create_api", "test_collection_create_api_description"
        )
        assert response.status_code == 201
        response_json = response.json()
        assert response_json.get("name") == "test_collection_create_api"
        assert (
            response_json.get("description") == "test_collection_create_api_description"
        )
    finally:
        api_server_connection.collection_delete(
            "test_collection_create_api", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_delete_api(api_server_connection):
    api_server_connection.collection_create(
        "test_collection_delete_api",
        "test_collection_delete_api_description",
        raise_for_status=False,
    )
    response = api_server_connection.collection_delete("test_collection_delete_api")
    assert response.status_code == 204


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_delete_no_exists_raises_error(api_server_connection):
    with pytest.raises(APIServerError):
        api_server_connection.collection_delete("test_collection_delete_no_exists")


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_get(api_server_connection):
    api_server_connection.collection_create(
        "test_collection_get", "test_collection_get_description", raise_for_status=False
    )
    try:
        response = api_server_connection.collection_get_by_name("test_collection_get")
        assert response.status_code == 200
        response_json = response.json()
        assert response_json.get("name") == "test_collection_get"
        assert response_json.get("description") == "test_collection_get_description"
    finally:
        api_server_connection.collection_delete(
            "test_collection_get", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_get_no_exists_raises_error(api_server_connection):
    with pytest.raises(APIServerError):
        api_server_connection.collection_get_by_name(
            "test_collection_get_no_exists_raises_error"
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_update_new_description(api_server_connection):
    api_server_connection.collection_create(
        "test_collection_update_new_description",
        "test_collection_update_new_description_description",
        raise_for_status=False,
    )
    try:
        response = api_server_connection.collection_update(
            "test_collection_update_new_description",
            description="test_collection_update_new_description_new_description",
        )
        assert response.status_code == 200
        response_json = response.json()
        assert (
            response_json.get("description")
            == "test_collection_update_new_description_new_description"
        )
    finally:
        api_server_connection.collection_delete(
            "test_collection_update_new_description", raise_for_status=False
        )


@pytest.mark.skip(
    reason=(
        "This test is not working due to a backend bug: when "
        "provided a new name, the backend errors out."
    )
)
@pytest.mark.integration
@pytest.mark.requires_internet
def test_collection_update_new_name(api_server_connection):
    api_server_connection.collection_create(
        "test_collection_update_new_name",
        "test_collection_update_new_name_description",
        raise_for_status=False,
    )
    try:
        response = api_server_connection.collection_update(
            "test_collection_update_new_name",
            new_collection_name="test_collection_update_new_name_new_name",
        )
        assert response.status_code == 200
        response_json = response.json()
        assert response_json.get("name") == "test_collection_update_new_name_new_name"
    finally:
        api_server_connection.collection_delete(
            "test_collection_update", raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_create(api_server_connection):
    api_server_connection.collection_create(
        FUNCTION_TESTING_Collection_NAME,
        FUNCTION_TESTING_Collection_DESCRIPTION,
        raise_for_status=False,
    )
    function_name = "test_function_create"
    function_description = "test_function_create_description"
    bundle_hash = "test_bundle_hash"
    tables = []
    dependencies = []
    trigger_by = []
    function_snippet = "function_snippet"
    try:
        response = api_server_connection.function_create(
            FUNCTION_TESTING_Collection_NAME,
            function_name,
            function_description,
            bundle_hash,
            tables,
            dependencies,
            trigger_by,
            function_snippet,
        )
        assert response.status_code == 201
        response_json = response.json()
        assert response_json.get("name") == function_name
        assert response_json.get("description") == function_description
        assert response_json.get("tables") is None
        assert response_json.get("dependencies") is None
        assert response_json.get("trigger_by") is None
        assert response_json.get("function_snippet") == function_snippet
    finally:
        api_server_connection.function_delete(
            FUNCTION_TESTING_Collection_NAME, function_name, raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_users_list(api_server_connection):
    response = api_server_connection.users_list()
    assert response.status_code == 200


@pytest.mark.integration
@pytest.mark.requires_internet
def test_users_list_with_params(api_server_connection):
    response = api_server_connection.users_list(
        offset=10, len=42, filter="hi", order_by="hello"
    )
    assert response.status_code == 200
    list_params = response.json().get("list_params")
    assert list_params is not None
    assert list_params.get("offset") == 10
    assert list_params.get("len") == 42
    assert list_params.get("filter") == "hi"
    assert list_params.get("order_by") == "hello"


@pytest.mark.integration
@pytest.mark.requires_internet
def test_users_create(api_server_connection):
    api_server_connection.users_delete("test_users_create", raise_for_status=False)
    name = "test_users_create"
    full_name = "test_users_create_full_name"
    email = "test_users_create_email"
    password = "test_users_create_password"
    enabled = True
    try:
        response = api_server_connection.users_create(
            name, full_name, email, password, enabled
        )
        assert response.status_code == 201
        response_json = response.json()
        assert response_json.get("name") == name
        assert response_json.get("full_name") == full_name
        assert response_json.get("email") == email
        assert response_json.get("enabled") == enabled
    finally:
        api_server_connection.users_delete(name, raise_for_status=False)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_users_delete(api_server_connection):
    name = "test_users_delete"
    full_name = "test_users_delete_full_name"
    email = "test_users_delete_email"
    password = "test_users_delete_password"
    enabled = True
    api_server_connection.users_create(
        name,
        full_name,
        email,
        password,
        enabled,
        raise_for_status=False,
    )
    response = api_server_connection.users_delete(name)
    assert response.status_code == 204


@pytest.mark.integration
@pytest.mark.requires_internet
def test_users_delete_no_exists_raises_error(api_server_connection):
    with pytest.raises(APIServerError):
        api_server_connection.users_delete("test_users_delete_no_exists_raises_error")


@pytest.mark.integration
@pytest.mark.requires_internet
def test_users_get_by_name(api_server_connection):
    name = "test_users_get_by_name"
    full_name = "test_users_get_by_name_full_name"
    email = "test_users_get_by_name_email"
    password = "test_users_get_by_name_password"
    enabled = True
    api_server_connection.users_create(
        name,
        full_name,
        email,
        password,
        enabled,
        raise_for_status=False,
    )
    try:
        response = api_server_connection.users_get_by_name(name)
        assert response.status_code == 200
        response_json = response.json()
        assert response_json.get("name") == name
        assert response_json.get("full_name") == full_name
        assert response_json.get("email") == email
        assert response_json.get("enabled") == enabled
    finally:
        api_server_connection.users_delete(name, raise_for_status=False)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_users_get_by_name_no_exists_raises_error(api_server_connection):
    with pytest.raises(APIServerError):
        api_server_connection.users_get_by_name(
            "test_users_get_by_name_no_exists_raises_error"
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_users_update_new_password(api_server_connection):
    name = "test_users_update_new_password"
    full_name = "test_users_update_new_password_full_name"
    email = "test_users_update_new_password_email"
    password = "test_users_update_new_password_password"
    enabled = True
    api_server_connection.users_create(
        name,
        full_name,
        email,
        password,
        enabled,
        raise_for_status=False,
    )
    try:
        response = api_server_connection.users_update(
            name,
            full_name="test_users_update_new_password_new_full_name",
            email="test_users_update_new_password_new_email",
            new_password="test_users_update_new_password_new_password",
            force_password_change=True,
            enabled=False,
        )
        assert response.status_code == 200
        response_json = response.json()
        assert response_json.get("name") == name
        assert (
            response_json.get("full_name")
            == "test_users_update_new_password_new_full_name"
        )
        assert response_json.get("email") == "test_users_update_new_password_new_email"
        assert not response_json.get("enabled")
    finally:
        api_server_connection.users_delete(name, raise_for_status=False)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_users_update_no_new_password(api_server_connection):
    name = "test_users_update_no_new_password"
    full_name = "test_users_update_no_new_password_full_name"
    email = "test_users_update_no_new_password_email"
    password = "test_users_update_no_new_password_password"
    enabled = True
    api_server_connection.users_create(
        name,
        full_name,
        email,
        password,
        enabled,
        raise_for_status=False,
    )
    try:
        response = api_server_connection.users_update(
            name,
            full_name="test_users_update_no_new_password_new_full_name",
            email="test_users_update_no_new_password_new_email",
            new_password=None,
            force_password_change=True,
            enabled=False,
        )
        assert response.status_code == 200
        response_json = response.json()
        assert response_json.get("name") == name
        assert (
            response_json.get("full_name")
            == "test_users_update_no_new_password_new_full_name"
        )
        assert (
            response_json.get("email") == "test_users_update_no_new_password_new_email"
        )
        assert not response_json.get("enabled")
    finally:
        api_server_connection.users_delete(name, raise_for_status=False)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_users_update_change_fullname(api_server_connection):
    name = "test_users_update_change_fullname"
    full_name = "test_users_update_change_fullname"
    email = "test_users_update_change_fullname_email"
    password = "test_users_update_change_fullname_password"
    enabled = True
    api_server_connection.users_create(
        name,
        full_name,
        email,
        password,
        enabled,
        raise_for_status=False,
    )
    try:
        response = api_server_connection.users_update(
            name,
            full_name="test_users_update_change_fullname_new_full_name",
        )
        assert response.status_code == 200
        response_json = response.json()
        assert response_json.get("name") == name
        assert (
            response_json.get("full_name")
            == "test_users_update_change_fullname_new_full_name"
        )
        assert response_json.get("email") == "test_users_update_change_fullname_email"
        assert response_json.get("enabled")
    finally:
        api_server_connection.users_delete(name, raise_for_status=False)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_in_collection_list(api_server_connection):
    api_server_connection.collection_create(
        FUNCTION_TESTING_Collection_NAME,
        FUNCTION_TESTING_Collection_DESCRIPTION,
        raise_for_status=False,
    )
    response = api_server_connection.function_in_collection_list(
        FUNCTION_TESTING_Collection_NAME
    )
    assert response.status_code == 200


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_in_collection_list_with_params(api_server_connection):
    api_server_connection.collection_create(
        FUNCTION_TESTING_Collection_NAME,
        FUNCTION_TESTING_Collection_DESCRIPTION,
        raise_for_status=False,
    )
    response = api_server_connection.function_in_collection_list(
        FUNCTION_TESTING_Collection_NAME,
        offset=10,
        len=42,
        filter="hi",
        order_by="hello",
    )
    assert response.status_code == 200
    list_params = response.json().get("list_params")
    assert list_params is not None
    assert list_params.get("offset") == 10
    assert list_params.get("len") == 42
    assert list_params.get("filter") == "hi"
    assert list_params.get("order_by") == "hello"


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_in_collection_get(api_server_connection):
    api_server_connection.collection_create(
        FUNCTION_TESTING_Collection_NAME,
        FUNCTION_TESTING_Collection_DESCRIPTION,
        raise_for_status=False,
    )
    function_name = "test_function_in_collection_get"
    function_description = "test_function_in_collection_get_description"
    bundle_hash = "test_function_in_collection_get_bundle_hash"
    tables = []
    dependencies = []
    trigger_by = []
    function_snippet = "test_function_in_collection_get_function_snippet"
    try:
        response = api_server_connection.function_create(
            FUNCTION_TESTING_Collection_NAME,
            function_name,
            function_description,
            bundle_hash,
            tables,
            dependencies,
            trigger_by,
            function_snippet,
        )
        assert response.status_code == 201
        response = api_server_connection.function_get(
            FUNCTION_TESTING_Collection_NAME, function_name
        )
        assert response.status_code == 200
    finally:
        api_server_connection.function_delete(
            FUNCTION_TESTING_Collection_NAME, function_name, raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_in_collection_get_no_exists_raises_error(api_server_connection):
    api_server_connection.collection_create(
        FUNCTION_TESTING_Collection_NAME,
        FUNCTION_TESTING_Collection_DESCRIPTION,
        raise_for_status=False,
    )
    with pytest.raises(APIServerError):
        api_server_connection.function_get(
            FUNCTION_TESTING_Collection_NAME,
            "test_function_in_collection_get_no_exists_raises_error",
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_update(api_server_connection):
    api_server_connection.collection_create(
        FUNCTION_TESTING_Collection_NAME,
        FUNCTION_TESTING_Collection_DESCRIPTION,
        raise_for_status=False,
    )
    function_name = "test_function_update_api"
    function_description = "test_function_update_api_description"
    bundle_hash = "test_function_update_api_bundle_hash"
    tables = []
    dependencies = []
    trigger_by = []
    function_snippet = "test_function_update_api_function_snippet"
    try:
        response = api_server_connection.function_create(
            FUNCTION_TESTING_Collection_NAME,
            function_name,
            function_description,
            bundle_hash,
            tables,
            dependencies,
            trigger_by,
            function_snippet,
        )
        assert response.status_code == 201
        new_description = "test_function_update_api_new_description"
        new_bundle_hash = "test_function_update_api_new_bundle_hash"
        new_function_snippet = "test_function_update_api_new_function_snippet"
        response = api_server_connection.function_update(
            FUNCTION_TESTING_Collection_NAME,
            function_name,
            new_function_name=function_name,
            description=new_description,
            bundle_hash=new_bundle_hash,
            tables=tables,
            dependencies=dependencies,
            trigger_by=trigger_by,
            function_snippet=new_function_snippet,
        )
        assert response.status_code == 200
        response_json = response.json()
        assert response_json.get("name") == function_name
        assert response_json.get("description") == new_description
        assert response_json.get("function_snippet") == new_function_snippet

    finally:
        api_server_connection.function_delete(
            FUNCTION_TESTING_Collection_NAME, function_name, raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_upload_function_bundle(api_server_connection):
    binary_data = b"\x00\x01\x02\x03\x04\x05"
    api_server_connection.collection_create(
        FUNCTION_TESTING_Collection_NAME,
        FUNCTION_TESTING_Collection_DESCRIPTION,
        raise_for_status=False,
    )
    function_name = "test_function_upload_function_bundle"
    function_description = "test_function_upload_function_bundle_description"
    bundle_hash = calculate_sha256(binary_data)
    tables = []
    dependencies = []
    trigger_by = []
    function_snippet = "test_function_upload_function_bundle_function_snippet"
    try:
        response = api_server_connection.function_create(
            FUNCTION_TESTING_Collection_NAME,
            function_name,
            function_description,
            bundle_hash,
            tables,
            dependencies,
            trigger_by,
            function_snippet,
        )
        assert response.status_code == 201
        response_json = response.json()
        assert response_json
        function_id = response_json.get("current_function_id")
        response = api_server_connection.function_upload_bundle(
            FUNCTION_TESTING_Collection_NAME, function_name, function_id, binary_data
        )
        assert response.status_code == 204
    finally:
        api_server_connection.function_delete(
            FUNCTION_TESTING_Collection_NAME, function_name, raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_list_functions(api_server_connection):
    binary_data = b"\x00\x01\x02\x03\x04\x05"
    api_server_connection.collection_create(
        FUNCTION_TESTING_Collection_NAME,
        FUNCTION_TESTING_Collection_DESCRIPTION,
        raise_for_status=False,
    )
    function_name = "test_function_list_functions_api"
    function_description = "test_function_list_functions_api_description"
    bundle_hash = calculate_sha256(binary_data)
    tables = []
    dependencies = []
    trigger_by = []
    function_snippet = "test_function_list_functions_api_function_snippet"
    try:
        response = api_server_connection.function_create(
            FUNCTION_TESTING_Collection_NAME,
            function_name,
            function_description,
            bundle_hash,
            tables,
            dependencies,
            trigger_by,
            function_snippet,
        )
        assert response.status_code == 201
        response_json = response.json()
        assert response_json
        function_id = response_json.get("current_function_id")
        response = api_server_connection.function_upload_bundle(
            FUNCTION_TESTING_Collection_NAME, function_name, function_id, binary_data
        )
        assert response.status_code == 204
        response = api_server_connection.function_list_history(
            FUNCTION_TESTING_Collection_NAME, function_name
        )
        assert response.status_code == 200
    finally:
        api_server_connection.function_delete(
            FUNCTION_TESTING_Collection_NAME, function_name, raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_execute(api_server_connection):
    binary_data = b"\x00\x01\x02\x03\x04\x05"
    api_server_connection.collection_create(
        FUNCTION_TESTING_Collection_NAME,
        FUNCTION_TESTING_Collection_DESCRIPTION,
        raise_for_status=False,
    )
    function_name = "test_function_execute_api"
    function_description = "test_function_execute_api_description"
    bundle_hash = calculate_sha256(binary_data)
    tables = []
    dependencies = []
    trigger_by = []
    function_snippet = "test_function_execute_api_function_snippet"
    try:
        response = api_server_connection.function_create(
            FUNCTION_TESTING_Collection_NAME,
            function_name,
            function_description,
            bundle_hash,
            tables,
            dependencies,
            trigger_by,
            function_snippet,
        )
        assert response.status_code == 201
        response_json = response.json()
        assert response_json
        function_id = response_json.get("current_function_id")
        response = api_server_connection.function_upload_bundle(
            FUNCTION_TESTING_Collection_NAME, function_name, function_id, binary_data
        )
        assert response.status_code == 204
        response = api_server_connection.function_execute(
            FUNCTION_TESTING_Collection_NAME, function_name
        )
        assert response.status_code == 201
    finally:
        api_server_connection.function_delete(
            FUNCTION_TESTING_Collection_NAME, function_name, raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_function_execute_execution_plan_name(api_server_connection):
    binary_data = b"\x00\x01\x02\x03\x04\x05"
    api_server_connection.collection_create(
        FUNCTION_TESTING_Collection_NAME,
        FUNCTION_TESTING_Collection_DESCRIPTION,
        raise_for_status=False,
    )
    function_name = "test_function_execute_execution_plan_name_api"
    function_description = "test_function_execute_execution_plan_name_api_description"
    bundle_hash = calculate_sha256(binary_data)
    tables = []
    dependencies = []
    trigger_by = []
    function_snippet = "test_function_execute_execution_plan_name_api_function_snippet"
    try:
        response = api_server_connection.function_create(
            FUNCTION_TESTING_Collection_NAME,
            function_name,
            function_description,
            bundle_hash,
            tables,
            dependencies,
            trigger_by,
            function_snippet,
        )
        assert response.status_code == 201
        response_json = response.json()
        assert response_json
        function_id = response_json.get("current_function_id")
        response = api_server_connection.function_upload_bundle(
            FUNCTION_TESTING_Collection_NAME, function_name, function_id, binary_data
        )
        assert response.status_code == 204
        response = api_server_connection.function_execute(
            FUNCTION_TESTING_Collection_NAME,
            function_name,
            execution_plan_name="test_function_execute_execution_plan_name_api",
        )
        assert response.status_code == 201
        assert (
            response.json().get("name")
            == "test_function_execute_execution_plan_name_api"
        )
    finally:
        api_server_connection.function_delete(
            FUNCTION_TESTING_Collection_NAME, function_name, raise_for_status=False
        )


@pytest.mark.integration
@pytest.mark.requires_internet
def test_users_update_change_own_password(api_server_connection):
    api_server_connection.users_delete(
        "test_users_update_change_own_password", raise_for_status=False
    )
    name = "test_users_update_change_own_password"
    full_name = "test_users_update_change_own_password_full_name"
    email = "test_users_update_change_own_password_email"
    password = "test_users_update_change_own_password_password"
    enabled = True
    try:
        response = api_server_connection.users_create(
            name, full_name, email, password, enabled
        )
        assert response.status_code == 201
        new_connection = obtain_connection(API_SERVER_URL, name, password)
        response = new_connection.users_update(
            name,
            old_password=password,
            new_password="test_users_update_change_own_password_new_password",
        )
        assert response.status_code == 200
    finally:
        api_server_connection.users_delete(name, raise_for_status=False)


@pytest.mark.integration
@pytest.mark.requires_internet
def test_execution_plan_list_api(api_server_connection):
    response = api_server_connection.execution_plan_list()
    assert response.status_code == 200


@pytest.mark.integration
@pytest.mark.requires_internet
def test_execution_plan_read_api(api_server_connection, tabsserver_connection):
    execution_plans = tabsserver_connection.execution_plans
    if execution_plans:
        response = api_server_connection.execution_plan_read(execution_plans[0].id)
        assert response.status_code == 200


@pytest.mark.integration
@pytest.mark.requires_internet
def test_table_list_api(api_server_connection):
    api_server_connection.collection_create(
        FUNCTION_TESTING_Collection_NAME,
        FUNCTION_TESTING_Collection_DESCRIPTION,
        raise_for_status=False,
    )
    response = api_server_connection.table_list(FUNCTION_TESTING_Collection_NAME)
    assert response.status_code == 200


@pytest.mark.integration
@pytest.mark.requires_internet
def test_transaction_list_api(api_server_connection):
    response = api_server_connection.transaction_list()
    assert response.status_code == 200
