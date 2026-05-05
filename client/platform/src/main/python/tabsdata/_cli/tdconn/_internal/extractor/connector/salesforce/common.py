#
# Copyright (c) 2026 Tabsdata Inc.
#

"""Common Salesforce connection helper.

The ``info`` dict consumed by this module mirrors the JSON accepted by
``tabsdata.connector.salesforce.SalesforceConn`` — a nested
``credentials`` object, optional ``instance_url``, and an
``objects`` / ``category`` pair describing which sobjects the
extractor should inspect. Using that shape means any config written
for a ``SalesforceConn`` can feed the extractor as-is.
"""

from simple_salesforce import Salesforce

from tabsdata.connector.salesforce._connection import SalesforceConn

# Whitelist so :class:`SalesforceConn._load_from_dict` doesn't barf on
# CLI-layer envelope keys (``kind``, ``name``, …) that happen to ride
# along inside ``info``. Keep in lockstep with ``SalesforceConn.__init__``.
_SF_CONN_KEYS = frozenset({"credentials", "instance_url", "category", "objects"})


def load_conn(info: dict) -> SalesforceConn:
    """Rehydrate a :class:`SalesforceConn` from a conn_configs dict.

    Exposed so extractors that need more than credentials (the
    ``objects`` list, for instance) can read typed attributes off the
    loaded conn rather than re-parsing the raw dict.
    """
    conn_info = {k: v for k, v in info.items() if k in _SF_CONN_KEYS}
    return SalesforceConn._load_from_dict(conn_info)


def connect(info: dict) -> Salesforce:
    """Create a Salesforce client from a SalesforceConn-shaped info dict."""
    conn = load_conn(info)
    return Salesforce(
        username=conn.credentials.username.secret_value,
        password=conn.credentials.password.secret_value,
        security_token=conn.credentials.security_token.secret_value,
        instance_url=conn.instance_url,
    )


def check_connect(info: dict) -> None:
    """Verify Salesforce connectivity by fetching instance limits.

    ``simple_salesforce``'s ``Salesforce(...)`` constructor already
    performs the OAuth login, so the constructor itself raises on bad
    credentials; the follow-up ``limits()`` call confirms the session
    token works against the REST API.
    """
    connect(info).limits()
