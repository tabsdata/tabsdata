#
# Copyright 2026 Tabsdata Inc.
#

from typing import Any

from tabsdatak.conn.common.error import ConnErrorCode
from tabsdatak.conn.common.types import CfgSpec

# Reserved key-prefix for tabsdata-internal knobs inside a
# ``conn_cfg`` / ``src_cfg`` / ``dest_cfg`` dict. Connector plugins
# strip these out before forwarding the cfg to the 3rd-party client
# library (SQLAlchemy, mysql-connector, ...).
TABSDATA_CFG_PREFIX = "tabsdata."


def remove_tabsdata_cfgs(cfgs: CfgSpec | None) -> CfgSpec | None:
    if cfgs is None:
        return None
    return {k: v for k, v in cfgs.items() if not k.startswith(TABSDATA_CFG_PREFIX)}


def get_cfg(
    cfgs: CfgSpec | None, key: str, default: Any = None, valid: list[Any] = None
) -> Any:
    if cfgs is None:
        return default
    cfg = cfgs.get(key, default)
    if valid is not None and cfg not in valid:
        raise ConnErrorCode.CONN_8.exception(name=key, value=cfg, valid=valid)
    return cfg
