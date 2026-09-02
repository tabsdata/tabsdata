#
#  Copyright 2026 Tabsdata Inc.
#


def setup_ibm_db():
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


def setup_ibm_db_authentication():
    import os
    import sys

    if sys.platform != "win32":
        return

    if os.environ.get("DB2CLIINIPATH"):
        return

    import tempfile

    base = os.environ.get("LOCALAPPDATA") or tempfile.gettempdir()
    cfg_dir = os.path.join(base, "tabsdata", "db2cli")
    os.makedirs(cfg_dir, exist_ok=True)
    ini = os.path.join(cfg_dir, "db2cli.ini")
    if not os.path.exists(ini):
        with open(ini, "w", encoding="ascii") as handle:
            handle.write("[COMMON]\nAuthentication=SERVER\n")
    os.environ["DB2CLIINIPATH"] = cfg_dir

    import logging

    logging.getLogger(__name__).debug(f"Pointed DB2CLIINIPATH at: {cfg_dir}")


def load():
    # noinspection PyBroadException
    try:
        setup_ibm_db()
    except Exception:
        pass
    # noinspection PyBroadException
    try:
        setup_ibm_db_authentication()
    except Exception:
        pass


load()
