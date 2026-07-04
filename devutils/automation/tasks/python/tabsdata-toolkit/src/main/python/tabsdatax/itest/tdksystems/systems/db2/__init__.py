"""IBM Db2 container system (ported to the unified `System` contract)."""
from .system import Db2System, db2_schema_name

__all__ = ["Db2System", "db2_schema_name"]
