#
# Copyright 2024 Tabs Data Inc.
#

import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest
import yaml

import tabsdata as td
from tabsdata.exceptions import ErrorCode, RegistrationError
from tabsdata.utils.bundle_utils import (
    CODE_FOLDER,
    CONFIG_ENTRY_POINT_FUNCTION_FILE_KEY,
    CONFIG_ENTRY_POINT_KEY,
    CONFIG_FILE_NAME,
    CONFIG_INPUTS_KEY,
    CONFIG_OUTPUT_KEY,
    LOCAL_PACKAGES_FOLDER,
    PYTHON_IGNORE_UNAVAILABLE_PUBLIC_PACKAGES_KEY,
    PYTHON_INSTALL_DEPENDENCIES_KEY,
    PYTHON_LOCAL_PACKAGES_KEY,
    PYTHON_PUBLIC_PACKAGES_KEY,
    PYTHON_VERSION_KEY,
    REQUIREMENTS_FILE_NAME,
    SaveTarget,
    copy_and_verify_requirements_file,
    create_bundle_archive,
    create_configuration,
    create_requirements,
    generate_entry_point_field,
    obtain_ordered_dists,
    store_file_contents,
    store_folder_contents,
    store_function_codebase,
)
from tests.conftest import (
    ABSOLUTE_TEST_FOLDER_LOCATION,
    CORRECT_DESTINATION,
    CORRECT_SOURCE,
)
from tests.testing_resources.test_custom_requirements.example import custom_requirements


class BaseDummyFunction:
    def __init__(self, name, module):
        self.__name__ = name
        self.__module__ = module


class DummyFunction:
    def __init__(
        self,
        name,
        module,
        input=None,
        interpreter=False,
        original_folder=None,
        original_file=None,
        output=None,
    ):
        self.__name__ = name
        self.__module__ = module
        self.input = input
        self.interpreter = interpreter
        self.original_folder = original_folder
        self.original_file = original_file
        self.output = output
        self.original_function = BaseDummyFunction(name, module)


def test_create_configuration_json(tmp_path):
    dummy_function = DummyFunction(
        name="dummy_function",
        module="dummy_module",
        input=CORRECT_SOURCE,
        original_file="dummy_file.py",
        output=CORRECT_DESTINATION,
    )
    save_location = tmp_path / "save_location"
    save_location.mkdir()
    result = create_configuration(dummy_function, str(save_location))
    expected = {
        CONFIG_INPUTS_KEY: CORRECT_SOURCE.to_dict(),
        CONFIG_ENTRY_POINT_KEY: {
            CONFIG_ENTRY_POINT_FUNCTION_FILE_KEY: "dummy_function.pkl",
        },
        CONFIG_OUTPUT_KEY: CORRECT_DESTINATION.to_dict(),
    }
    expected_load = {
        CONFIG_INPUTS_KEY: CORRECT_SOURCE.to_dict(),
        CONFIG_ENTRY_POINT_KEY: {
            CONFIG_ENTRY_POINT_FUNCTION_FILE_KEY: "dummy_function.pkl",
        },
        CONFIG_OUTPUT_KEY: CORRECT_DESTINATION.to_dict(),
    }
    print(f"result: {result}")
    assert result == expected
    with open(save_location / CONFIG_FILE_NAME) as f:
        assert json.load(f) == expected_load


def test_create_configuration_json_output_dict(tmp_path):
    dummy_function = DummyFunction(
        name="dummy_function",
        module="dummy_module",
        input=CORRECT_SOURCE,
        original_file="dummy_file.py",
        output=CORRECT_DESTINATION,
    )
    save_location = tmp_path / "save_location"
    save_location.mkdir()
    result = create_configuration(dummy_function, str(save_location))
    expected = {
        CONFIG_INPUTS_KEY: CORRECT_SOURCE.to_dict(),
        CONFIG_ENTRY_POINT_KEY: {
            CONFIG_ENTRY_POINT_FUNCTION_FILE_KEY: "dummy_function.pkl",
        },
        CONFIG_OUTPUT_KEY: CORRECT_DESTINATION.to_dict(),
    }
    expected_load = {
        CONFIG_INPUTS_KEY: CORRECT_SOURCE.to_dict(),
        CONFIG_ENTRY_POINT_KEY: {
            CONFIG_ENTRY_POINT_FUNCTION_FILE_KEY: "dummy_function.pkl",
        },
        CONFIG_OUTPUT_KEY: CORRECT_DESTINATION.to_dict(),
    }
    assert result == expected
    with open(save_location / CONFIG_FILE_NAME) as f:
        assert json.load(f) == expected_load


def test_generate_entry_point_field_json():
    dummy_function = DummyFunction(
        name="dummy_function", module="dummy_module", original_file="dummy_file.py"
    )
    result = generate_entry_point_field(dummy_function)
    expected = {
        CONFIG_ENTRY_POINT_FUNCTION_FILE_KEY: "dummy_function.pkl",
    }
    assert result == expected


@patch(
    "tabsdata.utils.bundle_utils.obtain_ordered_dists",
    return_value=["package1==1.0.0", "package2==2.0.0"],
)
def test_create_requirements_yaml(mock_obtain_ordered_dists, tmp_path):
    save_location = tmp_path / "save_location"
    save_location.mkdir()
    result = create_requirements(str(save_location))
    expected = ["package1==1.0.0", "package2==2.0.0"]
    assert result == expected
    with open(save_location / REQUIREMENTS_FILE_NAME) as f:
        data = yaml.safe_load(f)
        assert data == {
            PYTHON_INSTALL_DEPENDENCIES_KEY: False,
            PYTHON_VERSION_KEY: (
                f"{sys.version_info.major}.{sys.version_info.minor}"
                f".{sys.version_info.micro}"
            ),
            PYTHON_PUBLIC_PACKAGES_KEY: expected,
            PYTHON_IGNORE_UNAVAILABLE_PUBLIC_PACKAGES_KEY: True,
        }


@patch("tabsdata.utils.bundle_utils.importlib_metadata.packages_distributions")
@patch("tabsdata.utils.bundle_utils.pkgutil.iter_modules")
def test_obtain_ordered_dists(mock_iter_modules, mock_packages_distributions):
    # Mocking importlib.metadata.packages_distributions
    mock_packages_distributions.return_value = {
        "module1": ["package1"],
        "module2": ["package2"],
    }

    # Mocking pkgutil.iter_modules
    module1 = MagicMock()
    module1.name = "module1"
    module2 = MagicMock()
    module2.name = "module2"
    mock_iter_modules.return_value = [module1, module2]

    # Mocking importlib.metadata.version
    with patch(
        "tabsdata.utils.bundle_utils.importlib_metadata.version"
    ) as mock_version:
        mock_version.side_effect = lambda name: (
            "1.0.0" if name == "package1" else "2.0.0"
        )

        result = obtain_ordered_dists()
        expected = ["package1==1.0.0", "package2==2.0.0"]
        assert result == expected


def test_store_file_contents(tmp_path):
    src_file = tmp_path / "src_file.txt"
    src_file.write_text("dummy content")
    save_location = tmp_path / "save_location"
    save_location.mkdir()
    store_file_contents(str(src_file), str(save_location))
    assert (save_location / "src_file.txt").read_text() == "dummy content"


def test_store_folder_contents(tmp_path):
    src_folder = tmp_path / "src_folder"
    src_folder.mkdir()
    (src_folder / "src_file.txt").write_text("dummy content")
    save_location = tmp_path / "save_location"
    store_folder_contents(str(src_folder), str(save_location))
    assert (save_location / "src_file.txt").read_text() == "dummy content"


@patch("tabsdata.utils.bundle_utils.shutil.copy")
def test_store_non_interpreter_function_codebase(mock_copy, tmp_path):
    src_file = tmp_path / "src_file.txt"
    src_file.write_text("dummy content")
    save_location = tmp_path / "save_location"
    save_location.mkdir()
    store_function_codebase(str(src_file), str(save_location))
    mock_copy.assert_called_once_with(
        str(src_file), os.path.join(str(save_location), CODE_FOLDER, "src_file.txt")
    )


@patch(
    "tabsdata.utils.bundle_utils.obtain_ordered_dists",
    return_value=["package1==1.0.0", "package2==2.0.0"],
)
def test_create_requirements_yaml_no_local_packages(
    mock_obtain_ordered_dists, tmp_path
):
    save_location = tmp_path / "save_location"
    save_location.mkdir()
    result = create_requirements(str(save_location))
    expected = ["package1==1.0.0", "package2==2.0.0"]
    assert result == expected
    with open(save_location / REQUIREMENTS_FILE_NAME) as f:
        data = yaml.safe_load(f)
        assert data == {
            PYTHON_INSTALL_DEPENDENCIES_KEY: False,
            PYTHON_VERSION_KEY: (
                f"{sys.version_info.major}.{sys.version_info.minor}."
                f"{sys.version_info.micro}"
            ),
            PYTHON_PUBLIC_PACKAGES_KEY: expected,
            PYTHON_IGNORE_UNAVAILABLE_PUBLIC_PACKAGES_KEY: True,
        }


@patch(
    "tabsdata.utils.bundle_utils.obtain_ordered_dists",
    return_value=["package1==1.0.0", "package2==2.0.0"],
)
@patch("tabsdata.utils.bundle_utils.store_folder_contents")
def test_create_requirements_yaml_with_local_packages(
    mock_store_folder_contents, mock_obtain_ordered_dists, tmp_path
):
    save_location = tmp_path / "save_location"
    save_location.mkdir()
    local_package_path = tmp_path / "local_package"
    local_package_path.mkdir()
    result = create_requirements(str(save_location), [str(local_package_path)])
    expected = ["package1==1.0.0", "package2==2.0.0"]
    assert result == expected
    with open(save_location / REQUIREMENTS_FILE_NAME) as f:
        data = yaml.safe_load(f)
        assert data == {
            PYTHON_INSTALL_DEPENDENCIES_KEY: False,
            PYTHON_VERSION_KEY: (
                f"{sys.version_info.major}.{sys.version_info.minor}"
                f".{sys.version_info.micro}"
            ),
            PYTHON_PUBLIC_PACKAGES_KEY: expected,
            PYTHON_LOCAL_PACKAGES_KEY: [str(local_package_path)],
            PYTHON_IGNORE_UNAVAILABLE_PUBLIC_PACKAGES_KEY: True,
        }
    mock_store_folder_contents.assert_called_once_with(
        str(local_package_path),
        os.path.join(str(save_location), LOCAL_PACKAGES_FOLDER, "0"),
    )


@patch(
    "tabsdata.utils.bundle_utils.obtain_ordered_dists",
    return_value=["package1==1.0.0", "package2==2.0.0"],
)
@patch("tabsdata.utils.bundle_utils.store_folder_contents")
def test_create_requirements_yaml_with_multiple_local_packages(
    mock_store_folder_contents, mock_obtain_ordered_dists, tmp_path
):
    save_location = tmp_path / "save_location"
    save_location.mkdir()
    local_package_path1 = tmp_path / "local_package1"
    local_package_path1.mkdir()
    local_package_path2 = tmp_path / "local_package2"
    local_package_path2.mkdir()
    result = create_requirements(
        str(save_location), [str(local_package_path1), str(local_package_path2)]
    )
    expected = ["package1==1.0.0", "package2==2.0.0"]
    assert result == expected
    with open(save_location / REQUIREMENTS_FILE_NAME) as f:
        data = yaml.safe_load(f)
        assert data == {
            PYTHON_INSTALL_DEPENDENCIES_KEY: False,
            PYTHON_VERSION_KEY: (
                f"{sys.version_info.major}.{sys.version_info.minor}"
                f".{sys.version_info.micro}"
            ),
            PYTHON_PUBLIC_PACKAGES_KEY: expected,
            PYTHON_LOCAL_PACKAGES_KEY: [
                str(local_package_path1),
                str(local_package_path2),
            ],
            PYTHON_IGNORE_UNAVAILABLE_PUBLIC_PACKAGES_KEY: True,
        }
    mock_store_folder_contents.assert_any_call(
        str(local_package_path1),
        os.path.join(str(save_location), LOCAL_PACKAGES_FOLDER, "0"),
    )
    mock_store_folder_contents.assert_any_call(
        str(local_package_path2),
        os.path.join(str(save_location), LOCAL_PACKAGES_FOLDER, "1"),
    )


@patch(
    "tabsdata.utils.bundle_utils.obtain_ordered_dists",
    return_value=["package1==1.0.0", "package2==2.0.0"],
)
@patch("tabsdata.utils.bundle_utils.store_folder_contents")
def test_create_requirements_yaml_with_tuple_local_packages_raises_exception(
    mock_store_folder_contents, mock_obtain_ordered_dists, tmp_path
):
    save_location = tmp_path / "save_location"
    save_location.mkdir()
    local_package_path1 = tmp_path / "local_package1"
    local_package_path1.mkdir()
    local_package_path2 = tmp_path / "local_package2"
    local_package_path2.mkdir()
    with pytest.raises(RegistrationError) as e:
        create_requirements(
            str(save_location), (str(local_package_path1), str(local_package_path2))
        )
    assert e.value.code == "RE-011"


@patch(
    "tabsdata.utils.bundle_utils.obtain_ordered_dists",
    return_value=["package1==1.0.0", "package2==2.0.0"],
)
@patch("tabsdata.utils.bundle_utils.store_folder_contents")
def test_create_requirements_yaml_with_wrong_type_local_packages_raises_exception(
    mock_store_folder_contents, mock_obtain_ordered_dists, tmp_path
):
    save_location = tmp_path / "save_location"
    save_location.mkdir()
    with pytest.raises(RegistrationError) as e:
        create_requirements(str(save_location), 42)
    assert e.value.code == "RE-011"


def test_create_requirements_yaml_wrong_local_path_raises_file_error(tmp_path):
    with pytest.raises(RegistrationError) as e:
        create_requirements(save_location=tmp_path, local_packages="wrong_path")
    assert e.value.code == "RE-006"


@pytest.mark.slow
def test_copy_and_verify_requirements_file_all_correct(tmp_path):
    correct_requirements = {
        PYTHON_LOCAL_PACKAGES_KEY: [os.getcwd()],
        PYTHON_VERSION_KEY: "3.10",
        PYTHON_PUBLIC_PACKAGES_KEY: [
            "connectorx==0.3.3",
            "mysql-connector-python==9.0.0",
            "polars==1.4.1",
            "pyarrow==17.0.0",
            "base32hex==1.0.2",
            "uuid-v7==1.0.0",
        ],
    }
    correct_custom_requirements_path = os.path.join(
        tmp_path, "correct_custom_requirements.yaml"
    )
    with open(correct_custom_requirements_path, "w") as f:
        yaml.dump(correct_requirements, f)
    copy_and_verify_requirements_file(
        save_location=tmp_path,
        requirements_file=correct_custom_requirements_path,
    )
    with open(os.path.join(tmp_path, REQUIREMENTS_FILE_NAME), "r") as f:
        data = yaml.safe_load(f)
    with open(correct_custom_requirements_path, "r") as f:
        expected_data = yaml.safe_load(f)
    assert data == expected_data
    assert os.path.exists(os.path.join(tmp_path, LOCAL_PACKAGES_FOLDER, "0"))


def test_copy_and_verify_requirements_file_local_packages_not_found(tmp_path):
    incorrect_requirements = {
        PYTHON_LOCAL_PACKAGES_KEY: ["path/does/not/exist"],
        PYTHON_VERSION_KEY: "3.10",
        PYTHON_PUBLIC_PACKAGES_KEY: [
            "connectorx==0.3.3",
            "mysql-connector-python==9.0.0",
            "polars==1.4.1",
            "pyarrow==17.0.0",
            "base32hex==1.0.2",
            "uuid-v7==1.0.0",
        ],
    }
    incorrect_custom_requirements_path = os.path.join(
        tmp_path, "incorrect_requirements_wrong_local_packages.yaml"
    )
    with open(incorrect_custom_requirements_path, "w") as f:
        yaml.dump(incorrect_requirements, f)

    with pytest.raises(RegistrationError) as e:
        copy_and_verify_requirements_file(
            save_location=tmp_path, requirements_file=incorrect_custom_requirements_path
        )
    assert e.value.code == "RE-006"


def test_copy_and_verify_requirements_file_file_not_found(tmp_path):
    with pytest.raises(RegistrationError) as e:
        copy_and_verify_requirements_file(
            save_location=tmp_path, requirements_file="wrong_path"
        )
    assert e.value.code == "RE-007"


def test_copy_and_verify_requirements_file_no_python_version_key(tmp_path):
    incorrect_requirements = {
        PYTHON_PUBLIC_PACKAGES_KEY: [
            "connectorx==0.3.3",
            "mysql-connector-python==9.0.0",
            "polars==1.4.1",
            "pyarrow==17.0.0",
            "base32hex==1.0.2",
            "uuid-v7==1.0.0",
        ],
    }
    incorrect_custom_requirements_path = os.path.join(
        tmp_path, "incorrect_requirements_missing_version.yaml"
    )
    with open(incorrect_custom_requirements_path, "w") as f:
        yaml.dump(incorrect_requirements, f)

    with pytest.raises(RegistrationError) as e:
        copy_and_verify_requirements_file(
            save_location=tmp_path, requirements_file=incorrect_custom_requirements_path
        )
    assert e.value.code == "RE-008"


def test_copy_and_verify_requirements_file_no_python_requirements_key(tmp_path):
    incorrect_requirements = {
        PYTHON_VERSION_KEY: "3.10",
    }
    incorrect_custom_requirements_path = os.path.join(
        tmp_path, "incorrect_requirements_missing_requirements.yaml"
    )
    with open(incorrect_custom_requirements_path, "w") as f:
        yaml.dump(incorrect_requirements, f)

    with pytest.raises(RegistrationError) as e:
        copy_and_verify_requirements_file(
            save_location=tmp_path, requirements_file=incorrect_custom_requirements_path
        )
    assert e.value.code == "RE-009"


def test_copy_and_verify_requirements_file_python_requirements_key_wrong_type(tmp_path):
    incorrect_requirements = {
        PYTHON_VERSION_KEY: "3.10",
        PYTHON_PUBLIC_PACKAGES_KEY: 42,
    }
    incorrect_custom_requirements_path = os.path.join(
        tmp_path, "incorrect_requirements_wrong_requirement_type.yaml"
    )
    with open(incorrect_custom_requirements_path, "w") as f:
        yaml.dump(incorrect_requirements, f)

    with pytest.raises(RegistrationError) as e:
        copy_and_verify_requirements_file(
            save_location=tmp_path, requirements_file=incorrect_custom_requirements_path
        )
    assert e.value.code == "RE-010"


def test_tabsets_register_return_points_to_tar_file(tmp_path):
    @td.transformer(
        name="dataset_name",
        input_tables=CORRECT_SOURCE,
        output_tables=CORRECT_DESTINATION,
    )
    def foo_context_tar(a1, a2):
        return a1, a2

    context_tar = create_bundle_archive(
        foo_context_tar,
        save_location=tmp_path,
    )

    assert os.path.exists(context_tar)


def test_tabsets_connection_register_with_folder_and_target_to_persist(tmp_path):
    @td.transformer(
        name="dataset_name",
        input_tables=CORRECT_SOURCE,
        output_tables=CORRECT_DESTINATION,
    )
    def foo_folder_and_target_to_persist(a1, a2):
        return a1, a2

    with pytest.raises(RegistrationError) as e:
        create_bundle_archive(
            foo_folder_and_target_to_persist,
            path_to_code=os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION),
            save_target=SaveTarget.FOLDER.value,
            save_location=tmp_path,
        )
    assert e.value.error_code == ErrorCode.RE2


def test_tabsets_connection_register_with_file_and_target_to_persist(tmp_path):
    @td.transformer(
        name="dataset_name",
        input_tables=CORRECT_SOURCE,
        output_tables=CORRECT_DESTINATION,
    )
    def foo_file_and_target_to_persist(a1, a2):
        return a1, a2

    with pytest.raises(RegistrationError) as e:
        create_bundle_archive(
            foo_file_and_target_to_persist,
            path_to_code=os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION, "conftest.py"),
            save_target=SaveTarget.FILE.value,
            save_location=tmp_path,
        )
    assert e.value.error_code == ErrorCode.RE2


def test_tabsets_connection_register_with_target_file_to_persist(tmp_path):
    @td.transformer(
        name="dataset_name",
        input_tables=CORRECT_SOURCE,
        output_tables=CORRECT_DESTINATION,
    )
    def foo_target_file_to_persist(a1, a2):
        return a1, a2

    _ = create_bundle_archive(
        foo_target_file_to_persist,
        save_target=SaveTarget.FILE.value,
        save_location=tmp_path,
    )

    assert os.path.exists(
        os.path.join(
            tmp_path,
            "foo_target_file_to_persist_context",
            CODE_FOLDER,
            __file__,
        )
    )


def test_tabsets_connection_register_with_target_folder_to_persist(tmp_path):
    @td.transformer(
        name="dataset_name",
        input_tables=CORRECT_SOURCE,
        output_tables=CORRECT_DESTINATION,
    )
    def foo_target_folder_to_persist(a1, a2):
        return a1, a2

    _ = create_bundle_archive(
        foo_target_folder_to_persist,
        save_target=SaveTarget.FOLDER.value,
        save_location=tmp_path,
    )
    assert os.path.exists(
        os.path.join(
            tmp_path,
            "foo_target_folder_to_persist_context",
            CODE_FOLDER,
            __file__,
        )
    )


def test_tabsets_connection_register_with_save_location(tmp_path):
    @td.transformer(
        name="dataset_name",
        input_tables=CORRECT_SOURCE,
        output_tables=CORRECT_DESTINATION,
    )
    def foo_save_location(a1, a2):
        return a1, a2

    save_location = os.path.join(tmp_path, "tests", "dummy_storage_location")
    os.makedirs(save_location, exist_ok=True)
    _ = create_bundle_archive(
        foo_save_location,
        path_to_code=os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION),
        save_location=save_location,
    )
    assert os.path.exists(
        os.path.join(
            save_location,
            "foo_save_location_context",
            CODE_FOLDER,
            __file__,
        )
    )
    assert os.path.exists(
        os.path.join(
            save_location,
            "foo_save_location_compressed_context",
        )
    )


def test_tabsets_connection_register_with_wrong_save_location():
    @td.transformer(
        name="dataset_name",
        input_tables=CORRECT_SOURCE,
        output_tables=CORRECT_DESTINATION,
    )
    def foo_save_location(a1, a2):
        return a1, a2

    with pytest.raises(RegistrationError) as e:
        _ = create_bundle_archive(
            foo_save_location,
            path_to_code=os.path.join(ABSOLUTE_TEST_FOLDER_LOCATION),
            save_location=os.path.join(os.getcwd(), "this doesn't exist"),
        )
    assert e.value.error_code == ErrorCode.RE4


def test_tabsets_connection_register_with_wrong_folder_to_persist(tmp_path):
    @td.transformer(
        name="dataset_name",
        input_tables=CORRECT_SOURCE,
        output_tables=CORRECT_DESTINATION,
    )
    def foo_wrong_folder_to_persist(a1, a2):
        return a1, a2

    with pytest.raises(RegistrationError) as e:
        _ = create_bundle_archive(
            foo_wrong_folder_to_persist,
            path_to_code="this also doesn't exist",
            save_location=tmp_path,
        )
    assert e.value.error_code == ErrorCode.RE5


def test_register_plain_function_raises_error(tmp_path):
    with pytest.raises(RegistrationError) as e:
        create_bundle_archive(lambda x: x, save_location=tmp_path)
    assert e.value.error_code == ErrorCode.RE1


@pytest.mark.slow
def test_register_with_custom_requirements(tmp_path):
    correct_requirements = {
        PYTHON_LOCAL_PACKAGES_KEY: [os.getcwd()],
        PYTHON_VERSION_KEY: "3.10",
        PYTHON_PUBLIC_PACKAGES_KEY: [
            "connectorx==0.3.3",
            "mysql-connector-python==9.0.0",
            "polars==1.4.1",
            "pyarrow==17.0.0",
            "base32hex==1.0.2",
            "uuid-v7==1.0.0",
        ],
    }
    correct_custom_requirements_path = os.path.join(
        tmp_path, "correct_custom_requirements.yaml"
    )
    with open(correct_custom_requirements_path, "w") as f:
        yaml.dump(correct_requirements, f)
    create_bundle_archive(
        custom_requirements,
        save_location=tmp_path,
        requirements=correct_custom_requirements_path,
    )
    with open(
        os.path.join(tmp_path, "custom_requirements_context", REQUIREMENTS_FILE_NAME),
        "r",
    ) as f:
        data = yaml.safe_load(f)
    with open(correct_custom_requirements_path, "r") as f:
        expected_data = yaml.safe_load(f)
    assert data == expected_data
    assert os.path.exists(
        os.path.join(
            tmp_path, "custom_requirements_context", LOCAL_PACKAGES_FOLDER, "0"
        )
    )


def test_register_function_wrong_save_target_raises_error(tmp_path):
    @td.transformer(
        name="dataset_name",
        input_tables=CORRECT_SOURCE,
        output_tables=CORRECT_DESTINATION,
    )
    def foo(a1, a2):
        return a1, a2

    with pytest.raises(RegistrationError) as e:
        create_bundle_archive(foo, save_target="this is wrong", save_location=tmp_path)
    assert e.value.error_code == ErrorCode.RE3


def test_tabsets_connection_register_multiple_times(tmp_path):
    @td.transformer(
        name="dataset_name",
        input_tables=CORRECT_SOURCE,
        output_tables=CORRECT_DESTINATION,
    )
    def foo_multiple_times(a1, a2):
        return a1, a2

    for _ in range(10):
        create_bundle_archive(
            foo_multiple_times,
            save_location=tmp_path,
        )
