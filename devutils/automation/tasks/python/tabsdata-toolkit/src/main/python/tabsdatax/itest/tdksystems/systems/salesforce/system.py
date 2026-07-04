"""Salesforce SaaS system — the SPECIAL saas case: no schema domain, no SQL.

`domain_is_named = False` makes the domain a credentials clone (no schema op),
`supports_sql = False` makes the data plane a manual workflow. So only `_ping`
is implemented here: a basic HTTPS reachability check on the configured
instance URL (we can't do a real auth probe without an OAuth flow + connected
app, which the credentials don't carry).
"""
from __future__ import annotations

from .._saas import SaasBase
from ..base import DomainContext, LoadRequest, Option
from ...errors import DatagenError


class SalesforceSystem(SaasBase):
    slug = "salesforce"
    label = "Salesforce"
    kind = "salesforce"
    domain_is_named = False
    supports_sql = False
    options = (
        Option("URL", "url"),
        Option("USERNAME", "username"),
        Option("PASSWORD", "password", secret=True),
        Option("TOKEN", "token", secret=True),
    )

    def load_data(self, ctx: DomainContext, req: LoadRequest) -> int:
        """Wipe + upload the model's data to the org via the cci/snowfakery
        loader (which lives in the salesforce venv the CLI re-execs into)."""
        from ... import data_gen, salesforce_loader
        print()
        salesforce_loader.load(data_gen.model_dir(req.model), ctx.meta)
        print()
        return 0

    def _ping(self, values: dict[str, str]) -> None:
        import urllib.error
        import urllib.request

        url = (values.get("URL") or "").rstrip("/")
        if not url:
            raise DatagenError("URL is empty")
        if not (url.startswith("https://") or url.startswith("http://")):
            raise DatagenError(f"URL must include the scheme (got: {url!r})")
        # Just verify the host responds to TCP+TLS — any HTTP status (even
        # 404 for a bare path) means the instance is reachable. We can't
        # do a real auth probe without an OAuth flow + connected app, and
        # the 1Password item doesn't carry those.
        req = urllib.request.Request(url, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=15) as _resp:
                pass
        except urllib.error.HTTPError:
            # Got a response, just non-2xx — fine for reachability.
            pass
        except urllib.error.URLError as exc:
            raise DatagenError(f"salesforce {url} unreachable: {exc}")
