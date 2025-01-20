#
# Copyright 2024 Tabs Data Inc.
#

import os
import random
import string
import subprocess
from pathlib import Path

import pytest
import yaml

from tabsdata.utils.bundle_utils import (
    PYTHON_CHECK_MODULE_AVAILABILITY_KEY,
    PYTHON_LOCAL_PACKAGES_KEY,
    PYTHON_PUBLIC_PACKAGES_KEY,
    PYTHON_VERSION_KEY,
)
from tabsserver.pyenv_creation import (
    DEFAULT_ENVIRONMENT_FOLDER,
    UV_EXECUTABLE,
    create_virtual_environment,
    found_requirements,
    get_dir_hash,
    remove_path,
)
from tabsserver.utils import DEFAULT_DEVELOPMENT_LOCKS_LOCATION
from tests.conftest import PYTEST_DEFAULT_ENVIRONMENT_PREFIX


@pytest.mark.requires_internet
@pytest.mark.slow
def test_create_virtual_environment_check_availability_false_fails(tmp_path):
    incorrect_requirements = {
        PYTHON_LOCAL_PACKAGES_KEY: [os.getcwd()],
        PYTHON_VERSION_KEY: "3.12",
        PYTHON_PUBLIC_PACKAGES_KEY: ["pandas==2.2.3", "doesntexist"],
        PYTHON_CHECK_MODULE_AVAILABILITY_KEY: False,
    }
    incorrect_custom_requirements_path = os.path.join(
        tmp_path, "correct_custom_requirements.yaml"
    )
    with open(incorrect_custom_requirements_path, "w") as f:
        yaml.dump(incorrect_requirements, f)
    assert (
        create_virtual_environment(
            incorrect_custom_requirements_path,
            current_instance=None,
            locks_folder=DEFAULT_DEVELOPMENT_LOCKS_LOCATION,
            environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
        )
        is None
    )


@pytest.mark.requires_internet
@pytest.mark.slow
def test_create_virtual_environment_check_availability_true_works(tmp_path):
    incorrect_requirements = {
        PYTHON_LOCAL_PACKAGES_KEY: [os.getcwd()],
        PYTHON_VERSION_KEY: "3.12",
        PYTHON_PUBLIC_PACKAGES_KEY: ["pandas==2.2.3", "doesntexist"],
        PYTHON_CHECK_MODULE_AVAILABILITY_KEY: True,
    }
    incorrect_custom_requirements_path = os.path.join(
        tmp_path, "correct_custom_requirements.yaml"
    )
    with open(incorrect_custom_requirements_path, "w") as f:
        yaml.dump(incorrect_requirements, f)
    assert (
        create_virtual_environment(
            incorrect_custom_requirements_path,
            current_instance=None,
            locks_folder=DEFAULT_DEVELOPMENT_LOCKS_LOCATION,
            environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
        )
        is not None
    )


@pytest.mark.requires_internet
@pytest.mark.slow
def test_found_requirements_single_package():
    real_environment_name = "pytest_environment_test_found_requirements_single_package"
    environment_folder = os.path.join(DEFAULT_ENVIRONMENT_FOLDER, real_environment_name)
    try:
        command = [
            UV_EXECUTABLE,
            "venv",
            "--python",
            "3.12",
            environment_folder,
        ]
        result = subprocess.run(
            " ".join(command),
            shell=True,
        )
        assert result.returncode == 0
        assert found_requirements(["pandas"], real_environment_name) == ["pandas"]
    finally:
        remove_path(environment_folder)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_found_requirements_single_package_wrong_version():
    real_environment_name = (
        "pytest_environment_test_found_requirements_single_package_wrong_version"
    )
    environment_folder = os.path.join(DEFAULT_ENVIRONMENT_FOLDER, real_environment_name)
    try:
        command = [
            UV_EXECUTABLE,
            "venv",
            "--python",
            "3.12",
            environment_folder,
        ]
        result = subprocess.run(
            " ".join(command),
            shell=True,
        )
        assert result.returncode == 0
        assert found_requirements(["pandas==999.999.999"], real_environment_name) == []
    finally:
        remove_path(environment_folder)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_found_requirements_multiple_packages():
    real_environment_name = (
        "pytest_environment_test_found_requirements_multiple_packages"
    )
    environment_folder = os.path.join(DEFAULT_ENVIRONMENT_FOLDER, real_environment_name)
    try:
        command = [
            UV_EXECUTABLE,
            "venv",
            "--python",
            "3.12",
            environment_folder,
        ]
        result = subprocess.run(
            " ".join(command),
            shell=True,
        )
        assert result.returncode == 0
        assert found_requirements(
            ["pandas", "not_a_real_package"], real_environment_name
        ) == ["pandas"]
    finally:
        remove_path(environment_folder)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_found_requirements_all_not_found():
    real_environment_name = "pytest_environment_test_found_requirements_all_not_found"
    environment_folder = os.path.join(DEFAULT_ENVIRONMENT_FOLDER, real_environment_name)
    try:
        command = [
            UV_EXECUTABLE,
            "venv",
            "--python",
            "3.12",
            environment_folder,
        ]
        result = subprocess.run(
            " ".join(command),
            shell=True,
        )
        assert result.returncode == 0
        assert found_requirements(["not_a_real_package"], real_environment_name) == []
    finally:
        remove_path(environment_folder)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_found_requirements_empty_list():
    real_environment_name = "pytest_environment_test_found_requirements_empty_list"
    environment_folder = os.path.join(DEFAULT_ENVIRONMENT_FOLDER, real_environment_name)
    try:
        command = [
            UV_EXECUTABLE,
            "venv",
            "--python",
            "3.12",
            environment_folder,
        ]
        result = subprocess.run(
            " ".join(command),
            shell=True,
        )
        assert result.returncode == 0
        assert found_requirements([], real_environment_name) == []
    finally:
        remove_path(environment_folder)


@pytest.mark.requires_internet
@pytest.mark.slow
def test_found_requirements_mixed():
    real_environment_name = "pytest_environment_test_found_requirements_mixed"
    environment_folder = os.path.join(DEFAULT_ENVIRONMENT_FOLDER, real_environment_name)
    try:
        command = [
            UV_EXECUTABLE,
            "venv",
            "--python",
            "3.12",
            environment_folder,
        ]
        result = subprocess.run(
            " ".join(command),
            shell=True,
        )
        assert result.returncode == 0
        assert found_requirements(
            [
                "pandas==2.0.1",
                "not_a_real_package",
                "numpy",
                "also_not_a_real_package==1.0.0",
            ],
            real_environment_name,
        ) == ["pandas==2.0.1", "numpy"]
    finally:
        remove_path(environment_folder)


def create_test_directory_with_random_files(
    directory: Path, num_files: int, file_size: int
):
    for i in range(num_files):
        file_name = f"file_{i}.txt"
        file_path = os.path.join(directory, file_name)

        with open(file_path, "w") as file:
            content = "".join(
                random.choices(string.ascii_letters + string.digits, k=file_size)
            )
            file.write(content)


def create_file_with_name_and_content(directory: Path, name: str, content: str):
    file_path = os.path.join(directory, name)
    with open(file_path, "w") as file:
        file.write(content)


def test_get_dir_hash_same_directory(tmp_path):
    create_test_directory_with_random_files(tmp_path, 10, 100)
    create_file_with_name_and_content(tmp_path, "test.py", "print('Hello, World!')")
    create_file_with_name_and_content(tmp_path, "requirements.txt", "pandas")
    assert get_dir_hash(tmp_path) == get_dir_hash(tmp_path)


def test_get_dir_hash_different_directory_same_files(tmp_path):
    dir1 = tmp_path / "dir1"
    os.makedirs(dir1)
    create_test_directory_with_random_files(dir1, 10, 100)
    create_file_with_name_and_content(dir1, "test.py", "print('Hello, World!')")
    create_file_with_name_and_content(dir1, "requirements.txt", "pandas")
    dir2 = tmp_path / "dir2"
    os.makedirs(dir2)
    create_test_directory_with_random_files(dir2, 10, 100)
    create_file_with_name_and_content(dir2, "test.py", "print('Hello, World!')")
    create_file_with_name_and_content(dir2, "requirements.txt", "pandas")
    assert get_dir_hash(dir1) == get_dir_hash(dir2)


def test_get_dir_hash_different_directory_different_python_files(tmp_path):
    dir1 = tmp_path / "dir1"
    os.makedirs(dir1)
    create_test_directory_with_random_files(dir1, 10, 100)
    create_file_with_name_and_content(dir1, "test.py", "print('Hello, World!')")
    create_file_with_name_and_content(dir1, "requirements.txt", "pandas")
    dir2 = tmp_path / "dir2"
    os.makedirs(dir2)
    create_test_directory_with_random_files(dir2, 10, 100)
    create_file_with_name_and_content(dir2, "test.py", "print('Hello, other worlds!')")
    create_file_with_name_and_content(dir2, "requirements.txt", "pandas")
    assert get_dir_hash(dir1) != get_dir_hash(dir2)


def test_get_dir_hash_different_directory_different_requirements_files(tmp_path):
    dir1 = tmp_path / "dir1"
    os.makedirs(dir1)
    create_test_directory_with_random_files(dir1, 10, 100)
    create_file_with_name_and_content(dir1, "test.py", "print('Hello, World!')")
    create_file_with_name_and_content(dir1, "requirements.txt", "pandas")
    dir2 = tmp_path / "dir2"
    os.makedirs(dir2)
    create_test_directory_with_random_files(dir2, 10, 100)
    create_file_with_name_and_content(dir2, "test.py", "print('Hello, World!')")
    create_file_with_name_and_content(dir2, "requirements.txt", "polars")
    assert get_dir_hash(dir1) != get_dir_hash(dir2)


def test_get_dir_hash_different_directory_same_multiple_files(tmp_path):
    dir1 = tmp_path / "dir1"
    os.makedirs(dir1)
    create_test_directory_with_random_files(dir1, 10, 100)
    create_file_with_name_and_content(dir1, "test.py", "print('Hello, World!')")
    create_file_with_name_and_content(dir1, "main.py", "print('Hello, again!')")
    create_file_with_name_and_content(dir1, "context.py", "print('Hello, thir time!')")
    create_file_with_name_and_content(dir1, "requirements.txt", "pandas")
    dir2 = tmp_path / "dir2"
    os.makedirs(dir2)
    create_test_directory_with_random_files(dir2, 10, 100)
    create_file_with_name_and_content(dir2, "test.py", "print('Hello, World!')")
    create_file_with_name_and_content(dir2, "main.py", "print('Hello, again!')")
    create_file_with_name_and_content(dir2, "context.py", "print('Hello, thir time!')")
    create_file_with_name_and_content(dir2, "requirements.txt", "pandas")
    assert get_dir_hash(dir1) == get_dir_hash(dir2)
