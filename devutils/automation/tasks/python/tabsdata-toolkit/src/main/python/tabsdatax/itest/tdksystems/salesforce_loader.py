"""Automated Salesforce load — no manual cci/OAuth steps.

`tdx-domain create --model salesforce --data` runs this: it generates the dataset
from the model's snowfakery recipe, then wipes the target objects and
bulk-uploads the fresh records to the org. Authentication is a non-interactive
SOAP login (username + password + security token) via `simple-salesforce`,
using the `URL` / `USERNAME` / `PASSWORD` / `TOKEN` the salesforce domain
already carries — no browser OAuth, no `cci` keychain.

The CumulusCI load mapping (`*.mapping.yml`) drives which objects/fields are
loaded and how lookups resolve, so the result mirrors the documented
`cci task run generate_and_load_from_yaml` workflow.

This module imports `snowfakery`, `simple_salesforce` and `yaml`, which live
only in the salesforce venv — so it is imported lazily, after the CLI has
re-exec'd there (see `_venv`).
"""
from __future__ import annotations

import datetime
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlparse

from .errors import DatagenError


def _domain_from_url(url: str) -> str:
    """Map a Salesforce login URL to simple-salesforce's `domain=` value.
    `login`/`test` for the standard endpoints, otherwise the My Domain host
    (e.g. `acme.my` for `https://acme.my.salesforce.com`)."""
    host = (urlparse(url).hostname or url).strip().lower()
    if host in ("login.salesforce.com", ""):
        return "login"
    if host == "test.salesforce.com":
        return "test"
    suffix = ".salesforce.com"
    return host[: -len(suffix)] if host.endswith(suffix) else host


def _client(meta: dict):
    from simple_salesforce import Salesforce  # salesforce-venv only
    url = (meta.get("URL") or "").rstrip("/")
    user, pwd, token = meta.get("USERNAME"), meta.get("PASSWORD"), meta.get("TOKEN")
    if not (url and user and pwd and token):
        raise DatagenError(
            "salesforce load needs URL, USERNAME, PASSWORD and TOKEN in the "
            "domain (source the domain script / set the SALESFORCE__* vars)."
        )
    return Salesforce(username=user, password=pwd, security_token=token,
                      domain=_domain_from_url(url))


def _find_one(model_dir: Path, suffix: str) -> Path:
    hits = sorted(model_dir.glob(f"*{suffix}"))
    if not hits:
        raise DatagenError(f"salesforce model has no '*{suffix}' in {model_dir}")
    return hits[0]


def _generate(recipe: Path, db: Path) -> None:
    """Run the snowfakery recipe into a SQLite db (one table per object)."""
    snowfakery = Path(sys.executable).with_name("snowfakery")
    cmd = [str(snowfakery), str(recipe), "--dburl", f"sqlite:///{db}"]
    if subprocess.run(cmd).returncode != 0:
        raise DatagenError(f"snowfakery generation failed: {' '.join(cmd)}")


def _read_table(db: Path, table: str) -> list[dict]:
    conn = sqlite3.connect(str(db))
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(f'SELECT * FROM "{table}"').fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def _load_mapping(path: Path) -> list[dict]:
    """Parse the cci load mapping into ordered steps: each carries the SF
    object, the snowfakery source table, the fields to load and any lookups
    (lookup-field -> source table)."""
    import yaml  # pyyaml ships with snowfakery
    raw = yaml.safe_load(path.read_text()) or {}
    steps = []
    for name, spec in raw.items():
        steps.append({
            "name": name,
            "object": spec.get("sf_object") or name.split()[-1],
            "table": spec.get("table") or spec.get("sf_object"),
            "fields": list(spec.get("fields") or []),
            "lookups": {
                fld: (info.get("table") if isinstance(info, dict) else info)
                for fld, info in (spec.get("lookups") or {}).items()
            },
        })
    return steps


def _coerce(value):
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return value


def _wipe(sf, objects: list[str], log) -> None:
    """Delete every record of each object, in the given (reverse-dependency)
    order, via the Bulk API. Reports actual successes; records that can't be
    deleted (e.g. an org's seed accounts with child Cases/Contracts/Assets
    outside this dataset) are left in place and surfaced, not silently
    assumed gone."""
    for obj in objects:
        ids = [{"Id": r["Id"]}
               for r in sf.query_all(f"SELECT Id FROM {obj}")["records"]]
        if not ids:
            log(f"    {obj}: nothing to delete")
            continue
        results = getattr(sf.bulk, obj).delete(ids)
        ok = sum(1 for r in results if r.get("success"))
        msg = f"    {obj}: deleted {ok}/{len(ids)} record(s)"
        if ok < len(ids):
            reason = next((e.get("statusCode") for r in results if not r.get("success")
                           for e in (r.get("errors") or []) if e.get("statusCode")), "")
            msg += f" ({len(ids) - ok} left{f': {reason}' if reason else ''})"
        log(msg)


def _check(results: list[dict], obj: str) -> None:
    bad = [r for r in results if not r.get("success")]
    if bad:
        raise DatagenError(
            f"{obj}: {len(bad)}/{len(results)} record(s) failed to load; "
            f"first error: {bad[0].get('errors')}"
        )


def load(model_dir: Path, meta: dict, *, log=print) -> int:
    """Generate the recipe data and replace the org's data with it: wipe the
    mapped objects, then bulk-insert the fresh records (resolving lookups).
    Returns the total number of records uploaded."""
    recipe = _find_one(model_dir, ".recipe.yml")
    mapping = _find_one(model_dir, ".mapping.yml")
    steps = _load_mapping(mapping)
    sf = _client(meta)

    with tempfile.TemporaryDirectory(prefix="sf-gen-") as tmp:
        db = Path(tmp) / "data.db"
        log(f">>> generating from {recipe.name} ...")
        _generate(recipe, db)

        log(">>> wiping existing data ...")
        _wipe(sf, [s["object"] for s in reversed(steps)], log)

        log(">>> uploading ...")
        total = 0
        id_maps: dict[str, dict] = {}   # source table -> {snowfakery id: SF Id}
        for step in steps:
            obj, table = step["object"], step["table"]
            src_rows = _read_table(db, table)
            records = []
            for row in src_rows:
                rec = {f: _coerce(row[f]) for f in step["fields"]
                       if row.get(f) is not None}
                for fld, ref_table in step["lookups"].items():
                    ref = row.get(fld)
                    # snowfakery stores the PK as int but references it as a
                    # string, so key the id-map by str on both sides.
                    rec[fld] = (id_maps.get(ref_table, {}).get(str(ref))
                                if ref is not None else None)
                records.append(rec)
            if not records:
                log(f"    {obj}: nothing to upload")
                continue
            results = getattr(sf.bulk, obj).insert(records)
            _check(results, obj)
            id_maps[table] = {
                str(row["id"]): res["id"] for row, res in zip(src_rows, results)
            }
            total += len(records)
            log(f"    {obj}: uploaded {len(records)} record(s)")

    log(f">>> salesforce load complete ({total} record(s))")
    return total
