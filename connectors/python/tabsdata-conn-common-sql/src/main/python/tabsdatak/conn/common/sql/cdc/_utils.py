from __future__ import annotations


def sanitize_file_name(name: str) -> str:
    """Map a qualified-table name (``schema.table``) to a parquet
    file-name fragment, keeping alphanumerics / underscore / dot
    and replacing everything else with ``-``."""
    return "".join(c if c.isalnum() or c in "_." else "-" for c in name)
