#
# Copyright 2026 Tabs Data Inc.
#

"""Platform-specific library initialization."""


def setup_ibm_db():
    """Register clidriver/bin for DLL resolution on Windows.

    On Windows with Python 3.8+, extension modules no longer search PATH
    for DLL dependencies. The ibm_db wheel installs clidriver/ into
    site-packages but its bin/ subdirectory must be explicitly registered
    via os.add_dll_directory() before ibm_db can be imported.
    """
    import os
    import sys

    if sys.platform != "win32" or not hasattr(os, "add_dll_directory"):
        return

    import logging

    logger = logging.getLogger(__name__)

    import importlib.util

    spec = importlib.util.find_spec("clidriver")
    if spec and spec.submodule_search_locations:
        dll_path = os.path.join(spec.submodule_search_locations[0], "bin")
        if os.path.isdir(dll_path):
            os.add_dll_directory(dll_path)
            logger.debug(f"Registered ibm_db DLL folder: {dll_path}")
            try:
                import ibm_db_dbi as db2

                logger.debug(f"Successfully loaded ibm_db package: {db2.__version__}")
            except ImportError:
                logger.debug("The ibm_db package is not available")


def setup():
    setup_ibm_db()


setup()
