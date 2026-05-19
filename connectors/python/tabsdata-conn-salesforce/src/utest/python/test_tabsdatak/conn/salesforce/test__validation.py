
import pytest

from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.salesforce import _validation
from tabsdatak.conn.salesforce.error import SalesforceErrorCode


pytestmark = pytest.mark.salesforce


@pytest.mark.parametrize(
    "value", ["Account", "MyObject__c", "Junction__r", "X1Y2", "A_B"]
)
def test_validate_api_name_accepts_valid(value):
    assert _validation._validate_api_name(value) == value


@pytest.mark.parametrize("value", ["", "a" * 81])
def test_validate_api_name_rejects_bad_length(value):
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_api_name(value)
    assert ei.value.api_error is SalesforceErrorCode.SALESFORCE_5


@pytest.mark.parametrize(
    "value",
    [
        "1Account",
        "Account__d",
        "Account__",
        "_Account",
        "A__B",
    ],
)
def test_validate_api_name_rejects_bad_shape(value):
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_api_name(value)
    assert ei.value.api_error is SalesforceErrorCode.SALESFORCE_8


@pytest.mark.parametrize(
    "value",
    [
        "https://example.my.salesforce.com",
        "https://acme.force.com",
        "https://acme.cloudforce.com/",
        "https://acme.salesforce-setup.com",
    ],
)
def test_validate_instance_url_accepts_valid(value):
    assert _validation._validate_instance_url(value) == value


@pytest.mark.parametrize(
    "value",
    [
        "",
        "http://example.my.salesforce.com",
        "https://example.com",
        "https://example.salesforce.org",
    ],
)
def test_validate_instance_url_rejects_invalid(value):
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_instance_url(value)
    assert ei.value.api_error is SalesforceErrorCode.SALESFORCE_6


@pytest.mark.parametrize("value", ["60.0", "1.0", "999.99"])
def test_validate_api_version_accepts_valid(value):
    assert _validation._validate_api_version(value) == value


@pytest.mark.parametrize("value", ["", "60", "60.", "1.000", "v60.0", "1234.0"])
def test_validate_api_version_rejects_invalid(value):
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_api_version(value)
    assert ei.value.api_error is SalesforceErrorCode.SALESFORCE_7
