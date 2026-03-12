#
# Copyright 2024 Tabs Data Inc.
#

import inspect
import re
import types
from enum import Enum
from typing import (
    Annotated,
    Any,
    Collection,
    Literal,
    NamedTuple,
    Optional,
    Union,
    get_args,
    get_origin,
)

from pydantic import ValidationError


class ErrorCode(Enum):
    CCE1 = {
        "code": "CCE-001",
        "message": (
            "The dictionary to build a Credentials object must contain exactly one "
            "key, which must be one of the following: {}. Instead, got the following "
            "key(s) in the dictionary: {}."
        ),
    }
    CCE2 = {
        "code": "CCE-002",
        "message": (
            "The '{}' key in the dictionary to build a "
            "Credentials must have an object of type 'dict' as its value. "
            "Instead, got an object of type '{}'."
        ),
    }
    CCE3 = {
        "code": "CCE-003",
        "message": "The 'credentials' parameter must be one of {}, got '{}' instead.",
    }
    CXCE1 = {
        "code": "CXCE-001",
        "message": "The 'uri' parameter in a {} must be a string, got '{}' instead.",
    }
    CXCE2 = {
        "code": "CXCE-002",
        "message": (
            "Scheme '{}' not supported. The supported schemes are {}."
            " The scheme is inferred from the URI, which should be of the form"
            " 'scheme://path'. The URI provided was '{}'."
        ),
    }
    CXCE3 = {
        "code": "CXCE-003",
        "message": (
            "The 'credentials' parameter in a {} must be a "
            "'UserPasswordCredentials' object or None, got '{}' instead."
        ),
    }
    CXCE4 = {
        "code": "CXCE-004",
        "message": (
            "The 'enforce_connection_params' parameter in a {} must be a "
            "'bool', got '{}' instead."
        ),
    }
    CXCE5 = {
        "code": "CXCE-005",
        "message": (
            "The 'cx_src_configs_sql' parameter in a {} must be a "
            "'dict' or None, got '{}' instead."
        ),
    }
    CXCE6 = {
        "code": "CXCE-006",
        "message": (
            "The 'cx_dst_configs_sql' parameter in a {} must be a "
            "'dict' or None, got '{}' instead."
        ),
    }
    CXCE7 = {
        "code": "CXCE-007",
        "message": (
            "The 'connection_string_or_uri' parameter in a {} must be a 'str', got '{}'"
            " instead"
        ),
    }
    CXCE8 = {
        "code": "CXCE-008",
        "message": (
            "The 'connection_string_or_uri' parameter in a {} must be either an"
            " ODBC connection string or a URI, got '{}' instead."
        ),
    }
    CXCE9 = {
        "code": "CXCE-009",
        "message": (
            "Scheme '{}' not supported. The supported scheme is {}."
            " The scheme is inferred from the URI, which should be of the form"
            " 'scheme://path'. The URI provided was '{}'."
        ),
    }
    DCE1 = {
        "code": "DCE-001",
        "message": (
            "The 'input_tables' parameter of a 'transformer' decorator must be a "
            "'TableInput' object, a string or a list of strings; got '{}' instead."
        ),
    }
    DCE2 = {
        "code": "DCE-002",
        "message": (
            "The 'output_tables' parameter of a 'transformer' decorator must be a "
            "'TableOutput' object, a string or a list of strings; got '{}' instead."
        ),
    }
    DCE3 = {
        "code": "DCE-003",
        "message": (
            "The 'source' parameter of a 'publisher' decorator must be a "
            "'SourcePlugin' object (except 'TableInput'), got '{}' instead."
        ),
    }
    DCE4 = {
        "code": "DCE-004",
        "message": (
            "The 'tables' parameter of a 'publisher' decorator must be a "
            "'TableOutput' object, a string or a list of strings; got '{}' instead."
        ),
    }
    DCE5 = {
        "code": "DCE-005",
        "message": (
            "The 'tables' parameter of a 'subscriber' decorator must be a 'TableInput' "
            "object, a string or a list of strings; got '{}' instead."
        ),
    }
    DCE6 = {
        "code": "DCE-006",
        "message": (
            "The 'destination' parameter of a 'subscriber' decorator must be a "
            "'DestinationPlugin' object (except 'TableOutput'), got '{}' instead."
        ),
    }
    DCE7 = {
        "code": "DCE-007",
        "message": (
            "The 'on_input_tables' parameter of a 'transformer' decorator must be a "
            "'OnTablesAction' object, a list of 'OnTablesAction' objects or None; "
            "got object '{}' of type '{}' instead."
        ),
    }
    DCE8 = {
        "code": "DCE-008",
        "message": (
            "The 'on_output_tables' parameter of a 'transformer' decorator must be a "
            "'OnTablesAction' object, a list of 'OnTablesAction' objects or None; "
            "got object '{}' of type '{}' instead."
        ),
    }
    DCE9 = {
        "code": "DCE-009",
        "message": (
            "The 'on_tables' parameter of a 'publisher' decorator must be a "
            "'OnTablesAction' object, a list of 'OnTablesAction' objects or None; "
            "got object '{}' of type '{}' instead."
        ),
    }
    DCE10 = {
        "code": "DCE-010",
        "message": (
            "The 'on_tables' parameter of a 'subscriber' decorator must be a "
            "'OnTablesAction' object, a list of 'OnTablesAction' objects or None; "
            "got object '{}' of type '{}' instead."
        ),
    }
    DCE11 = {
        "code": "DCE-011",
        "message": (
            "The 'start' parameter of a CronTrigger object must be a "
            "string in ISO 8601 format, a datetime object or None; "
            "got an object of type '{}' instead."
        ),
    }
    DCE12 = {
        "code": "DCE-012",
        "message": (
            "The 'end' parameter of a CronTrigger object must be a "
            "string in ISO 8601 format, a datetime object or None; "
            "got an object of type '{}' instead."
        ),
    }
    DCE13 = {
        "code": "DCE-013",
        "message": (
            "The 'start' parameter of a CronTrigger object must be earlier than the"
            " 'end' parameter. Got start='{}' and end='{}'."
        ),
    }
    DCE14 = {
        "code": "DCE-014",
        "message": (
            "The 'mask' parameter of a CronTrigger object must have 5 elements, "
            "for minute precision; mask '{}' has '{}'."
        ),
    }
    DCE15 = {
        "code": "DCE-015",
        "message": (
            "The 'mask' parameter of a CronTrigger object must be a valid cron"
            " specification; got '{}' instead."
        ),
    }
    DCE16 = {
        "code": "DCE-016",
        "message": (
            "The 'mask' parameter cannot be 'None' and must be a string; "
            "got '{}' instead."
        ),
    }
    DCE17 = {
        "code": "DCE-17",
        "message": (
            "The 'start' parameter of a CronTrigger object must be a "
            "string in ISO 8601 format or a datetime object with timezone "
            "information. Got '{}', which does not have timezone information. "
            "Ensure that it does by checking that 'start.tzinfo' is not None."
        ),
    }
    DCE18 = {
        "code": "DCE-18",
        "message": (
            "The 'end' parameter of a CronTrigger object must be a "
            "string in ISO 8601 format or a datetime object with timezone "
            "information. Got '{}', which does not have timezone information. "
            "Ensure that it does by checking that 'end.tzinfo' is not None."
        ),
    }
    DCE19 = {
        "code": "DCE-019",
        "message": (
            "The 'mask' parameter '{}' for CronTrigger object does not support "
            "'@' mask aliasing."
        ),
    }

    DECE2 = {
        "code": "DECE-002",
        "message": (
            "Scheme '{}' not currently supported. The supported scheme(s) must start "
            "with {}. The scheme is inferred from the URI, which should be of the form"
            " 'scheme<+driver>://path' (the driver is optional). The URI provided was "
            "'{}'."
        ),
    }
    DECE8 = {
        "code": "DECE-008",
        "message": (
            "The 'destination_table' parameter in a MySQLDestination must be a "
            "'list' or a 'str', got '{}' instead."
        ),
    }
    DECE9 = {
        "code": "DECE-009",
        "message": (
            "The 'credentials' parameter in a MySQLDestination must be a "
            "'UserPasswordCredentials' object or None; got '{}' instead."
        ),
    }
    DECE10 = {
        "code": "DECE-010",
        "message": (
            "The 'table' parameter in a TableOutput must be a 'str' or a list of 'str';"
            " got '{}' of type '{}' instead."
        ),
    }
    DECE11 = {
        "code": "DECE-011",
        "message": (
            "The 'path' parameter in a LocalFileDestination must be a 'str' or "
            "a 'list[str]', got '{}' instead"
        ),
    }
    DECE12 = {
        "code": "DECE-012",
        "message": (
            "Scheme '{}' not supported. The supported scheme is '{}'."
            " The scheme is inferred from the path, which should be of the form"
            " 'scheme://path' or '/path'. The provided path was '{}'."
        ),
    }
    DECE13 = {
        "code": "DECE-013",
        "message": (
            "File format '{}' not supported. The supported formats are"
            " {}. If the format was not provided, it was"
            " inferred from the file(s) extension."
        ),
    }
    DECE14 = {
        "code": "DECE-014",
        "message": (
            "The 'uri' parameter in a AzureDestination must be a 'str' or "
            "a 'list[str]', got '{}' instead"
        ),
    }
    DECE15 = {
        "code": "DECE-015",
        "message": (
            "Scheme '{}' not supported for AzureDestination. The supported scheme is"
            " '{}'. The scheme is inferred from the URI, which should be of the form"
            " 'scheme://path'. The URI provided was '{}'."
        ),
    }
    DECE16 = {
        "code": "DECE-016",
        "message": (
            "The 'credentials' parameter in an AzureDestination must be an "
            "'AzureCredentials' object, got '{}' instead"
        ),
    }
    DECE17 = {
        "code": "DECE-017",
        "message": (
            "The 'uri' parameter in a S3Destination must be a 'str' or "
            "a 'list[str]', got '{}' instead"
        ),
    }
    DECE18 = {
        "code": "DECE-018",
        "message": (
            "The 'region' parameter in a S3Destination must be a 'str', got '{}' "
            "instead"
        ),
    }
    DECE19 = {
        "code": "DECE-019",
        "message": (
            "The 'credentials' parameter in a S3Destination must be a "
            "'S3Credentials' object, got '{}' instead"
        ),
    }
    DECE20 = {
        "code": "DECE-020",
        "message": (
            "The 'destination_table' parameter in a PostgresDestination must be a "
            "'list' or a 'str', got '{}' instead."
        ),
    }
    DECE21 = {
        "code": "DECE-021",
        "message": (
            "The 'credentials' parameter in a PostgresDestination must be a "
            "'UserPasswordCredentials' object or None; got '{}' instead."
        ),
    }
    DECE22 = {
        "code": "DECE-022",
        "message": (
            "The 'destination_table' parameter in a MariaDBDestination must be a "
            "'list' or a 'str', got '{}' instead."
        ),
    }
    DECE23 = {
        "code": "DECE-023",
        "message": (
            "The 'credentials' parameter in a MariaDBDestination must be a "
            "'UserPasswordCredentials' object or None; got '{}' instead."
        ),
    }
    DECE24 = {
        "code": "DECE-024",
        "message": (
            "The 'destination_table' parameter in a OracleDestination must be a "
            "'list' or a 'str', got '{}' instead."
        ),
    }
    DECE25 = {
        "code": "DECE-025",
        "message": (
            "The 'credentials' parameter in a OracleDestination must be a "
            "'UserPasswordCredentials' object or None; got '{}' instead."
        ),
    }
    DECE26 = {
        "code": "DECE-026",
        "message": (
            "The 'if_table_exists' parameter in a MariaDBDestination must be one of the"
            " following values {}, got '{}' instead."
        ),
    }
    DECE27 = {
        "code": "DECE-027",
        "message": (
            "The 'if_table_exists' parameter in a MySQLDestination must be one of the "
            "following values {}, got '{}' instead."
        ),
    }
    DECE28 = {
        "code": "DECE-028",
        "message": (
            "The 'if_table_exists' parameter in a OracleDestination must be one of the "
            "following values {}, got '{}' instead."
        ),
    }
    DECE29 = {
        "code": "DECE-029",
        "message": (
            "The 'if_table_exists' parameter in a PostgresDestination must be one of"
            " the following values {}, got '{}' instead."
        ),
    }
    DECE30 = {
        "code": "DECE-030",
        "message": (
            "The 'definition' parameter in a Catalog must be a dictionary, got "
            "an object of type '{}' instead."
        ),
    }
    DECE31 = {
        "code": "DECE-031",
        "message": (
            "The 'tables' parameter in a Catalog must be a string or a list of "
            "strings, got a list with elements that are not strings instead."
        ),
    }
    DECE32 = {
        "code": "DECE-032",
        "message": (
            "The 'tables' parameter in a Catalog must be a string or a list of "
            "strings, got an object of type '{}' instead."
        ),
    }
    DECE33 = {
        "code": "DECE-033",
        "message": (
            "The 'if_table_exists' parameter in a Catalog must be one of the"
            " following values {}, got '{}' instead."
        ),
    }
    DECE34 = {
        "code": "DECE-034",
        "message": (
            "The 'catalog' parameter must be a Catalog object, got '{}' instead."
        ),
    }
    DECE35 = {
        "code": "DECE-035",
        "message": (
            "The 'catalog' dictionary to build a Catalog must contain exactly one "
            "key, which must be one of the following: {}. Instead, got the following "
            "key(s) in the dictionary: {}."
        ),
    }
    DECE36 = {
        "code": "DECE-036",
        "message": (
            "The '{}' key in the dictionary to build a "
            "Catalog must have an object of type 'dict' as its value. "
            "Instead, got an object of type '{}'."
        ),
    }
    DECE37 = {
        "code": "DECE-037",
        "message": (
            "The 'catalog' option is only compatible with file format(s) '{}', "
            "got '{}' instead."
        ),
    }
    DECE38 = {
        "code": "DECE-038",
        "message": (
            "The fragment index placeholder '{}' has been used in '{}', but this "
            "class does not support fragments. You can see if a class allows fragments "
            "by checking the 'allow_fragments' attribute of the class."
        ),
    }
    DECE39 = {
        "code": "DECE-039",
        "message": (
            "Partitioned tables with replace data are not currently supported. Please "
            "set either 'if_table_exists' to 'append' or 'partitioned_table' "
            "to 'False'."
        ),
    }
    DECE40 = {
        "code": "DECE-040",
        "message": (
            "The catalog parameter 'partitioned_table' expects a bool, got value of "
            "type '{}' instead."
        ),
    }
    DECE41 = {
        "code": "DECE-041",
        "message": (
            "The 'schema_strategy' parameter in a Catalog must be one of the"
            " following values {}, got '{}' instead."
        ),
    }
    DECE42 = {
        "code": "DECE-042",
        "message": (
            "The 'tables' parameter and the 'auto_create_at' parameter in a Catalog "
            "must have the same length, got '{}' and '{}' instead."
        ),
    }
    DECE43 = {
        "code": "DECE-043",
        "message": (
            "The 'auto_create_at' parameter in a Catalog must be a string, None, "
            "or a list of strings or Nones, got a list with elements that are neither "
            "instead."
        ),
    }
    DECE44 = {
        "code": "DECE-044",
        "message": (
            "The 'auto_create_at' parameter in a Catalog must be a string, None, or a "
            "list of strings or Nones, got an object of type '{}' instead."
        ),
    }
    DECE45 = {
        "code": "DECE-045",
        "message": (
            "The AWSGlue catalog has received two credential declarations, "
            "one in the definition and one in the s3_credentials parameter. Please "
            "provide only one."
        ),
    }
    DECE46 = {
        "code": "DECE-046",
        "message": (
            "The AWSGlue catalog has received two region declarations, "
            "one in the definition and one in the s3_region parameter. Please "
            "provide only one."
        ),
    }
    DECE47 = {
        "code": "DECE-047",
        "message": (
            "The 's3_credentials' parameter in an AWSGlue must be None or a "
            "'S3Credentials' object, got '{}' instead"
        ),
    }
    DECE48 = {
        "code": "DECE-048",
        "message": (
            "The 'region' parameter in an AWSGlue must be a 'str', got '{}' instead"
        ),
    }
    DECE49 = {
        "code": "DECE-049",
        "message": (
            "The 'credentials' parameter in a MSSQLDestination must be a "
            "'UserPasswordCredentials' object or None, got '{}' instead"
        ),
    }
    DECE50 = {
        "code": "DECE-050",
        "message": (
            "The 'uri' parameter in a GCSDestination must be a 'str' or "
            "a 'list[str]', got '{}' instead"
        ),
    }
    DECE51 = {
        "code": "DECE-051",
        "message": (
            "Scheme '{}' not supported for GCSDestination. The supported scheme is"
            " '{}'. The scheme is inferred from the URI, which should be of the form"
            " 'scheme://path'. The URI provided was '{}'."
        ),
    }
    DECE52 = {
        "code": "DECE-052",
        "message": (
            "The 'credentials' parameter in a GCSDestination must be a "
            "'GCPCredentials' object, got '{}' instead"
        ),
    }
    DECE53 = {
        "code": "DECE-053",
        "message": (
            "The number of destination tables provided ({}) does not match the number"
            " of results ({}) to write to {}."
        ),
    }
    DECE54 = {
        "code": "DECE-054",
        "message": (
            "The 'destination_table' parameter in a {} cannot be an empty string."
            " Got {}."
        ),
    }
    DQCE1 = {
        "code": "DQCE-001",
        "message": (
            "The 'tags' parameter of a Classifier object must be a "
            "'str', a 'list' of 'str', or None; got a list containing the object '{}' "
            "of type '{}' instead. Please ensure that all objects in the list are of "
            "type 'str'."
        ),
    }
    DQCE2 = {
        "code": "DQCE-002",
        "message": (
            "The 'tags' parameter of a Classifier object must be a "
            "'str', a 'list' of 'str', or None; got object '{}' "
            "of type '{}' instead."
        ),
    }
    DQCE3 = {
        "code": "DQCE-003",
        "message": (
            "The 'cols' parameter of a Classifier action must be a "
            "'str', a 'list' of 'str', a 'tuple' with two 'str', a 'list' of 'tuple' "
            "of two 'str' or None. Got the tuple '{}' that has {} elements instead of "
            "2. Please ensure that the tuple has exactly 2 "
            "elements of type 'str', the first representing the column to apply the "
            "classifier to, and the second the column to store the result in."
        ),
    }
    DQCE4 = {
        "code": "DQCE-004",
        "message": (
            "The 'cols' parameter of a Classifier action must be a "
            "'str', a 'list' of 'str', a 'tuple' with two 'str', a 'list' of 'tuple' "
            "of two 'str' or None. Got the tuple '{}' that has an element that is not "
            "a 'str'. Please ensure that the tuple has exactly 2 "
            "elements of type 'str', the first representing the column to apply the "
            "classifier to, and the second the column to store the result in."
        ),
    }
    DQCE5 = {
        "code": "DQCE-005",
        "message": (
            "The 'cols' parameter of a Classifier action must be a "
            "'str', a 'list' of 'str', a 'tuple' with two 'str', a 'list' of 'tuple' "
            "of two 'str' or None. Got a list with the tuple '{}' that has {} "
            "elements instead of 2. Please ensure that the tuple has exactly 2 "
            "elements of type 'str', the first representing the column to apply the "
            "classifier to, and the second the column to store the result in."
        ),
    }
    DQCE6 = {
        "code": "DQCE-006",
        "message": (
            "The 'cols' parameter of a Classifier action must be a "
            "'str', a 'list' of 'str', a 'tuple' with two 'str', a 'list' of 'tuple' "
            "of two 'str' or None. Got a list with the tuple '{}' that has an element "
            "that is not a 'str'. Please ensure that the tuple has exactly 2 "
            "elements of type 'str', the first representing the column to apply the "
            "classifier to, and the second the column to store the result in."
        ),
    }
    DQCE7 = {
        "code": "DQCE-006",
        "message": (
            "The 'cols' parameter of a Classifier action must be a "
            "'str', a 'list' of 'str', a 'tuple' with two 'str', a 'list' of 'tuple' "
            "of two 'str' or None. Got a list with the element '{}' of type '{}'. "
            "Please ensure that all elements in the list are of type 'str' or all "
            "elements in the list are of type 'tuple' of two 'str'."
        ),
    }
    DQCE8 = {
        "code": "DQCE-008",
        "message": (
            "The 'cols' parameter of a Classifier action must be a "
            "'str', a 'list' of 'str', a 'tuple' with two 'str', a 'list' of 'tuple' "
            "of two 'str' or None. Got a list with an element of type '{}' and another "
            "of type '{}'. Please make sure that all elements in the list have "
            "the same type."
        ),
    }
    DQCE9 = {
        "code": "DQCE-009",
        "message": (
            "The 'cols' parameter of a Classifier action must be a "
            "'str', a 'list' of 'str', a 'tuple' with two 'str', a 'list' of 'tuple' "
            "of two 'str' or None. Got value '{}' "
            "of type '{}' instead."
        ),
    }
    DQCE10 = {
        "code": "DQCE-010",
        "message": (
            "The 'ignore_nulls' parameter of an IsNan classifier must be a "
            "boolean. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE11 = {
        "code": "DQCE-011",
        "message": (
            "The 'ignore_nulls' parameter of an IsNotNan classifier must be a "
            "boolean. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE12 = {
        "code": "DQCE-012",
        "message": (
            "The 'ignore_nulls' parameter of an IsZero classifier must be a "
            "boolean. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE13 = {
        "code": "DQCE-013",
        "message": (
            "The 'ignore_nulls' parameter of an IsNotZero classifier must be a "
            "boolean. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE14 = {
        "code": "DQCE-014",
        "message": (
            "The 'ignore_nulls' parameter of an IsPositive classifier must be a "
            "boolean. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE15 = {
        "code": "DQCE-015",
        "message": (
            "The 'ignore_nulls' parameter of an IsPositiveOrZero classifier must be a "
            "boolean. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE16 = {
        "code": "DQCE-016",
        "message": (
            "The 'ignore_nulls' parameter of an IsNegative classifier must be a "
            "boolean. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE17 = {
        "code": "DQCE-017",
        "message": (
            "The 'ignore_nulls' parameter of an IsNegativeOrZero classifier must be a "
            "boolean. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE18 = {
        "code": "DQCE-018",
        "message": (
            "The 'ignore_nulls' parameter of a Matches classifier must be a "
            "boolean. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE19 = {
        "code": "DQCE-019",
        "message": (
            "The 'regex' parameter of a Matches classifier must be a "
            "string. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE20 = {
        "code": "DQCE-020",
        "message": (
            "The 'ignore_nulls' parameter of a DoesNotMatch classifier must be a "
            "boolean. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE21 = {
        "code": "DQCE-021",
        "message": (
            "The 'regex' parameter of a DoesNotMatch classifier must be a "
            "string. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE22 = {
        "code": "DQCE-022",
        "message": (
            "The 'min' parameter of a HasLen classifier must be an "
            "'int' or a 'float'. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE23 = {
        "code": "DQCE-023",
        "message": (
            "The 'max' parameter of a HasLen classifier must be an "
            "'int' or a 'float'. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE24 = {
        "code": "DQCE-024",
        "message": (
            "The 'ignore_nulls' parameter of an IsInRange classifier must be a "
            "boolean. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE25 = {
        "code": "DQCE-025",
        "message": (
            "The 'min' parameter of an IsInRange classifier must be an "
            "'int' or a 'float'. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE26 = {
        "code": "DQCE-026",
        "message": (
            "The 'max' parameter of an IsInRange classifier must be an "
            "'int' or a 'float'. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE27 = {
        "code": "DQCE-027",
        "message": (
            "The 'closed' parameter of an IsInRange classifier must be one these "
            "values: {}. Got value '{}' instead."
        ),
    }
    DQCE28 = {
        "code": "DQCE-028",
        "message": (
            "The 'ignore_nulls' parameter of a HasLen classifier must be a "
            "boolean. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE29 = {
        "code": "DQCE-029",
        "message": (
            "The 'ignore_nulls' parameter of an IsIn classifier must be a "
            "boolean. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE30 = {
        "code": "DQCE-030",
        "message": (
            "The 'set' parameter of an IsIn classifier must be a "
            "collection, meaning an object of a class that implements the 'in' "
            "operator. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE31 = {
        "code": "DQCE-031",
        "message": (
            "The 'ignore_nulls' parameter of an IsNotIn classifier must be a "
            "boolean. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE32 = {
        "code": "DQCE-032",
        "message": (
            "The 'set' parameter of an IsNotIn classifier must be a "
            "collection, meaning an object of a class that implements the 'in' "
            "operator. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE33 = {
        "code": "DQCE-033",
        "message": (
            "The 'cols' parameter of a HasColumns classifier must be a "
            "'str' or a list of 'str'. Got a list containing value '{}' of type '{}' "
            "instead."
        ),
    }
    DQCE34 = {
        "code": "DQCE-034",
        "message": (
            "The 'cols' parameter of a HasColumns classifier must be a "
            "'str' or a list of 'str'. Got object '{}' of type '{}' "
            "instead."
        ),
    }
    DQCE35 = {
        "code": "DQCE-035",
        "message": (
            "The 'mode' parameter of a HasColumns classifier must one of the "
            "following 'str': {}. Got object '{}' instead."
        ),
    }
    DQCE36 = {
        "code": "DQCE-036",
        "message": (
            "The 'cols' parameter of a HasColumnTypes classifier must be a "
            "tuple of 'str' and 'DataType' or a list of tuples of 'str' and "
            "'DataType'. Got a single tuple, but the first element is '{}' of type "
            "'{}'. Please ensure that the first element of the tuple is of type 'str' "
            "and it represents the name of the column to check the type against."
        ),
    }
    DQCE37 = {
        "code": "DQCE-037",
        "message": (
            "The 'cols' parameter of a HasColumnTypes classifier must be a "
            "tuple of 'str' and 'DataType' or a list of tuples of 'str' and "
            "'DataType'. Got a single tuple, but the second element is '{}' of type "
            "'{}'. Please ensure that the second element of the tuple is of type "
            "'DataType' and it represents the type to check the column against."
        ),
    }
    DQCE38 = {
        "code": "DQCE-038",
        "message": (
            "The 'cols' parameter of a HasColumnTypes classifier must be a "
            "tuple of 'str' and 'DataType' or a list of tuples of 'str' and "
            "'DataType'. Got a list containing the object '{}' of type "
            "'{}' instead."
        ),
    }
    DQCE39 = {
        "code": "DQCE-039",
        "message": (
            "The 'cols' parameter of a HasColumnTypes classifier must be a "
            "tuple of 'str' and 'DataType' or a list of tuples of 'str' and "
            "'DataType'. Got a list containing a tuple '{}' of length {} instead. "
            "Please ensure that each tuple has exactly two elements, the first one "
            "representing the name of the column to check and the second one being "
            "the type to check against."
        ),
    }
    DQCE40 = {
        "code": "DQCE-040",
        "message": (
            "The 'cols' parameter of a HasColumnTypes classifier must be a "
            "tuple of 'str' and 'DataType' or a list of tuples of 'str' and "
            "'DataType'. Got a list of tuples containing tuple '{}', where the "
            "first element is '{}' of type "
            "'{}'. Please ensure that the first element of the tuple is of type 'str' "
            "and it represents the name of the column to check the type against."
        ),
    }
    DQCE41 = {
        "code": "DQCE-041",
        "message": (
            "The 'cols' parameter of a HasColumnTypes classifier must be a "
            "tuple of 'str' and 'DataType' or a list of tuples of 'str' and "
            "'DataType'. Got a list of tuples containing tuple '{}', where the second "
            "element is '{}' of type "
            "'{}'. Please ensure that the second element of the tuple is of type "
            "'DataType' and it represents the type to check the column against."
        ),
    }
    DQCE42 = {
        "code": "DQCE-042",
        "message": (
            "The 'cols' parameter of a HasColumnTypes classifier must be a "
            "tuple of 'str' and 'DataType' or a list of tuples of 'str' and "
            "'DataType'. Got object '{}' of type '{}' instead."
        ),
    }
    DQCE43 = {
        "code": "DQCE-043",
        "message": (
            "The 'cols' parameter of a HasColumnTypes classifier must be a "
            "tuple of 'str' and 'DataType' or a list of tuples of 'str' and "
            "'DataType'. Got tuple '{}' of length {} instead. "
            "Please ensure that each tuple has exactly two elements, the first one "
            "representing the name of the column to check and the second one being "
            "the type to check against."
        ),
    }
    DQCE44 = {
        "code": "DQCE-044",
        "message": (
            "The 'tags' parameter of an Operation object must be a "
            "'str', a 'list' of 'str', or None; got a list containing the object '{}' "
            "of type '{}' instead. Please ensure that all objects in the list are of "
            "type 'str'."
        ),
    }
    DQCE45 = {
        "code": "DQCE-045",
        "message": (
            "The 'tags' parameter of an Operation object must be a "
            "'str', a 'list' of 'str', or None; got object '{}' "
            "of type '{}' instead."
        ),
    }
    DQCE46 = {
        "code": "DQCE-046",
        "message": (
            "The 'table' parameter of a Summary object must be a "
            "'str', or None; got object '{}' "
            "of type '{}' instead."
        ),
    }
    DQCE47 = {
        "code": "DQCE-047",
        "message": (
            "The 'scale_type' of a Scale object must be one of the following: {}. Got "
            "object '{}' instead."
        ),
    }
    DQCE48 = {
        "code": "DQCE-048",
        "message": (
            "The 'scale_range' parameter of a Scale object must be a tuple with "
            "exactly two elements of type 'int' or 'float'; got object '{}' "
            "of type '{}' instead."
        ),
    }
    DQCE49 = {
        "code": "DQCE-049",
        "message": (
            "The 'scale_range' parameter of a Scale object must be a tuple with "
            "exactly two elements of type 'int' or 'float'; got the tuple '{}' "
            "with a length of '{}' instead."
        ),
    }
    DQCE50 = {
        "code": "DQCE-050",
        "message": (
            "The 'scale_range' parameter of a Scale object must be a tuple with "
            "exactly two elements of type 'int' or 'float'; got the tuple '{}' "
            "which has exactly two elements, but at least one of them has the wrong "
            "type."
        ),
    }
    DQCE51 = {
        "code": "DQCE-051",
        "message": (
            "The 'scale_tick' parameter of a Scale object must be a an 'int', a "
            "'float', or None; got '{}' of type '{}' instead."
        ),
    }
    DQCE52 = {
        "code": "DQCE-052",
        "message": (
            "The 'buckets' parameter of a Scale object must be a an 'int' between 2 "
            "and 100; got '{}' of type '{}' instead."
        ),
    }
    DQCE53 = {
        "code": "DQCE-053",
        "message": (
            "When a Scale object has been provided both a 'buckets' parameter and a "
            "'bucket_names' parameter, the length of the 'bucket_names' parameter "
            "must equal the 'buckets' parameter. Got value {} for 'buckets', "
            "and value '{}' of length {} for 'bucket_names' instead."
        ),
    }
    DQCE54 = {
        "code": "DQCE-054",
        "message": (
            "The 'bucket_names' parameter of a Scale object must be a list of 'str' "
            "or None. Got the list {} containing the object '{}' of type '{}' instead."
        ),
    }
    DQCE55 = {
        "code": "DQCE-055",
        "message": (
            "The 'bucket_names' parameter of a Scale object must be a list of 'str' "
            "or None; got object '{}' of type '{}' instead."
        ),
    }
    DQCE56 = {
        "code": "DQCE-056",
        "message": (
            "The 'scale' parameter of a Categorizer object must be a Scale object "
            "; got object '{}' of type '{}' instead."
        ),
    }
    DQCE57 = {
        "code": "DQCE-057",
        "message": (
            "Direct instantiation of the class '{}' is not allowed. Use "
            "ScaleType.linear(), ScaleType.quadratic(), ScaleType.exponential() "
            "or ScaleType.log() to obtain an instance of this class"
        ),
    }
    DQCE58 = {
        "code": "DQCE-058",
        "message": (
            "Direct instantiation of the class '{}' is not allowed. Use "
            "Criteria.all_ok(), Criteria.any_failed(), or Criteria.by_bucket() to "
            "obtain an instance of this class"
        ),
    }
    DQCE59 = {
        "code": "DQCE-059",
        "message": (
            "The 'criteria_type' of a Criteria object must be one of the following: {}."
            " Got object '{}' instead."
        ),
    }
    DQCE60 = {
        "code": "DQCE-060",
        "message": (
            "The 'buckets' parameter of a Criteria object must be a "
            "collection, meaning an object of a class that implements the 'in' "
            "operator; or None. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE61 = {
        "code": "DQCE-061",
        "message": (
            "The 'buckets' parameter of a Criteria.by_buckets() object must be a "
            "collection, meaning an object of a class that implements the 'in' "
            "operator. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE62 = {
        "code": "DQCE-062",
        "message": (
            "The 'to_table' parameter of a FilterOut object must be a "
            "'str', or None. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE63 = {
        "code": "DQCE-063",
        "message": (
            "The 'criteria' parameter of a FilterOut object must be a "
            "Criteria object. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE64 = {
        "code": "DQCE-064",
        "message": (
            "The 'to_table' parameter of a Select object must be a "
            "'str'; got value '{}' of type '{}' instead."
        ),
    }
    DQCE65 = {
        "code": "DQCE-065",
        "message": (
            "The 'criteria' parameter of a Select object must be a "
            "Criteria object. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE66 = {
        "code": "DQCE-066",
        "message": (
            "The 'criteria' parameter of a Fail object must be a "
            "Criteria object. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE67 = {
        "code": "DQCE-067",
        "message": (
            "Direct instantiation of the class '{}' is not allowed. Use "
            "Threshold.count(), Threshold.percent(), or Threshold.one() to "
            "obtain an instance of this class"
        ),
    }
    DQCE68 = {
        "code": "DQCE-068",
        "message": (
            "The 'count' parameter of a Threshold object must be an 'int' greater "
            "than zero or None; "
            "got value '{}' of type '{}' instead."
        ),
    }
    DQCE69 = {
        "code": "DQCE-069",
        "message": (
            "The 'count' parameter of a Threshold object must be an 'int' greater "
            "than zero or None; "
            "got the 'int' '{}' that is not greater than zero."
        ),
    }
    DQCE70 = {
        "code": "DQCE-070",
        "message": (
            "The 'count' parameter of a Threshold.count() object must be an 'int' "
            "greater than zero; "
            "got a None instead."
        ),
    }
    DQCE71 = {
        "code": "DQCE-071",
        "message": (
            "The 'percent' parameter of a Threshold.percent() object must be a "
            "'float' or 'int' "
            "greater than zero and less than 100; "
            "got a None instead."
        ),
    }
    DQCE72 = {
        "code": "DQCE-072",
        "message": (
            "The 'percent' parameter of a Threshold object must be a "
            "'float' or 'int' "
            "greater than zero and less than 100, or None. Got value {} instead, that "
            "is not in the (0, 100) range."
        ),
    }
    DQCE73 = {
        "code": "DQCE-073",
        "message": (
            "The 'percent' parameter of a Threshold object must be a "
            "'float' or 'int' "
            "greater than zero and less than 100, or None. Got value '{}' of "
            "type '{}' instead."
        ),
    }
    DQCE74 = {
        "code": "DQCE-074",
        "message": (
            "The 'threshold' parameter of a Fail object must be a "
            "Threshold object. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE75 = {
        "code": "DQCE-075",
        "message": (
            "The 'table' parameter of a DataQuality object must be a "
            "'str'. Got value '{}' of type '{}' instead."
        ),
    }
    DQCE76 = {
        "code": "DQCE-076",
        "message": (
            "The 'classify' parameter of a DataQuality object must be a "
            "Classifier object or a list of Classifier objects. Got value '{}' of "
            "type '{}' instead."
        ),
    }
    DQCE77 = {
        "code": "DQCE-077",
        "message": (
            "The 'classify' parameter of a DataQuality object must be a "
            "Classifier object or a list of Classifier objects. Got a list containing "
            "value '{}' of type '{}' instead."
        ),
    }
    DQCE78 = {
        "code": "DQCE-078",
        "message": (
            "The 'operations' parameter of a DataQuality object must be an "
            "Operation object or a list of Operation objects. Got value '{}' of "
            "type '{}' instead."
        ),
    }
    DQCE79 = {
        "code": "DQCE-079",
        "message": (
            "The 'operations' parameter of a DataQuality object must be an "
            "Operation object or a list of Operation objects. Got a list containing "
            "value '{}' of type '{}' instead."
        ),
    }
    DQPE1 = {
        "code": "DQPE-001",
        "message": (
            "Unable to resolve table name for action table '{}'."
            "No matching table found in input_tables or input_tables_map."
        ),
    }
    DQPE2 = {
        "code": "DQPE-002",
        "message": (
            "Unable to resolve table name for action table '{}'. "
            "No matching table found in output_tables."
        ),
    }
    DQPE3 = {
        "code": "DQPE-003",
        "message": "Unsupported type of data quality operation: {}",
    }
    DQPE4 = {
        "code": "DQPE-004",
        "message": (
            "Duplicate table names found in data quality operations:\n{}"
            "\n\nEach data quality operation must produce a unique table name."
        ),
    }
    DQPE5 = {
        "code": "DQPE-005",
        "message": (
            "Input table position {} is out of range for action table '{}'. "
            "Available input table frames: {}."
        ),
    }
    DQPE6 = {
        "code": "DQPE-006",
        "message": (
            "Output table position {} is out of range for action table '{}'. "
            "Available output table frames: {}."
        ),
    }
    DQPE7 = {
        "code": "DQPE-007",
        "message": (
            "Mismatch between table_infos and operations for input action table '{}'. "
            "Expected equal lengths, but got {} table_infos and {} operations."
        ),
    }
    DQPE8 = {
        "code": "DQPE-008",
        "message": (
            "Mismatch between table_infos and operations for output action table '{}'. "
            "Expected equal lengths, but got {} table_infos and {} operators. "
            "Total infoes is: {}; Total operators is: {}"
        ),
    }
    DQPE9 = {
        "code": "DQPE-009",
        "message": (
            "Unsupported classifier type '{}' for action table '{}'. "
            "This classifier type is not yet mapped to a DataQualityEngine method."
        ),
    }
    DQPE10 = {
        "code": "DQPE-010",
        "message": (
            "Quality column '{}' not found in classifier registry. "
            "All quality columns must be registered in the DataQualityEngine."
        ),
    }
    DQPE11 = {
        "code": "DQPE-011",
        "message": (
            "Column '{}' not found in table '{}'. "
            "The specified column does not exist as a non-system column."
        ),
    }
    DQPE12 = {
        "code": "DQPE-012",
        "message": (
            "Column '{}' has incompatible dtype '{}' for classifier '{}'. "
            "The column's data type is not supported by this classifier."
        ),
    }
    DQPE13 = {
        "code": "DQPE-013",
        "message": (
            "Type conversion error: {}. "
            "Some value provided to evaluate a data quality operation has an "
            "unsupported data type. Only basic data types and comon date, time, "
            "or duration types are supported."
        ),
    }
    DQPE14 = {
        "code": "DQPE-014",
        "message": (
            "Data quality check failed for table '{}': criteria '{}' exceeded "
            "threshold '{}'"
        ),
    }
    DQPE15 = {
        "code": "DQPE-015",
        "message": "The following tables are overridden multiple times:\n{}",
    }
    DQPE16 = {
        "code": "DQPE-016",
        "message": (
            "Column '{}' has incompatible value for classifier '{}'. "
            "The classifier's parameter '{}' with value '{}' cannot be converted to "
            "the column's data type '{}'."
        ),
    }
    DQCE80 = {
        "code": "DQCE-080",
        "message": (
            "The 'buckets' parameter of a Scale object must be a an 'int' between 2 "
            "and 100; got the 'int' {} that is outside of that range instead."
        ),
    }
    DQCE81 = {
        "code": "DQCE-081",
        "message": (
            "The 'column_names' parameter of the 'Criteria.in_buckets' must be a "
            "string or a list of strings; got '{}' instead."
        ),
    }
    FCE1 = {
        "code": "FCE-001",
        "message": (
            "The 'original_function' parameter of a TabsDataFunction must be a callable"
            " object, got an object of type '{}' instead."
        ),
    }
    FCE2 = {
        "code": "FCE-002",
        "message": (
            "The 'trigger_by' parameter of a TabsDataFunction must be a str, "
            "a list of strings, a CronTrigger, or a StageTrigger in the case of a "
            "publisher. Got an "
            "object of type '{}' instead."
        ),
    }
    FCE3 = {
        "code": "FCE-003",
        "message": (
            "The 'trigger_by' parameter of a TabsDataFunction must point to "
            "a table in the system. Instead got '{}', which does not contain a "
            "table."
        ),
    }
    FCE5 = {
        "code": "FCE-005",
        "message": (
            "A TabsdataFunction must have either a TableInput as the input or a "
            "TableOutput as the output. Instead got {} as the input and {} "
            "as the output."
        ),
    }
    FCE6 = {
        "code": "FCE-006",
        "message": (
            "The 'name' parameter of a TabsdataFunction be of type 'str'; got '{}'"
            " instead."
        ),
    }
    FCE7 = {
        "code": "FCE-007",
        "message": (
            "The source of a TabsdataFunction must be of type 'SourcePlugin' or None; "
            "got '{}' instead."
        ),
    }
    FCE8 = {
        "code": "FCE-008",
        "message": (
            "The destination of a TabsdataFunction must be of type 'DestinationPlugin' "
            "or None; got '{}' instead."
        ),
    }
    FCE9 = {
        "code": "FCE-009",
        "message": (
            "The 'input_actions' parameter of a TabsdataFunction must be a "
            "'OnTablesAction' object, a list of 'OnTablesAction' objects or None; "
            "got object '{}' of type '{}' instead."
        ),
    }
    FCE10 = {
        "code": "FCE-010",
        "message": (
            "The 'output_actions' parameter of a TabsdataFunction must be a "
            "'OnTablesAction' object, a list of 'OnTablesAction' objects or None; "
            "got object '{}' of type '{}' instead."
        ),
    }
    FCE11 = {
        "code": "FCE-011",
        "message": (
            "The 'input_actions' parameter of a TabsdataFunction can only be used "
            "when the function has a TableInput as input. Got the actions {} for a "
            "function with input type {} instead."
        ),
    }
    FCE12 = {
        "code": "FCE-012",
        "message": (
            "The 'output_actions' parameter of a TabsdataFunction can only be used "
            "when the function has a TableOutput as output. Got the actions {} for a "
            "function with output type {} instead."
        ),
    }
    FCE13 = {
        "code": "FCE-013",
        "message": (
            "The 'trigger_by' parameter of a TabsdataFunction can only be a"
            " StageTrigger when the function is a publisher."
        ),
    }
    FCE14 = {
        "code": "FCE-014",
        "message": (
            "The 'trigger_by' parameter of a TabsdataFunction can only be a"
            " StageTrigger when the function is a publisher and the source is a Stage."
            " The current function was provided with a StageTrigger as the"
            " trigger, but the source {} is not a Stage."
        ),
    }
    FOCE1 = {
        "code": "FOCE-001",
        "message": (
            "The dictionary to build a FileFormat object must contain exactly one "
            "key, which must be one of the following: {}. Instead, got the following "
            "key(s) in the dictionary: {}."
        ),
    }
    FOCE2 = {
        "code": "FOCE-002",
        "message": (
            "The '{}' key in the dictionary to build a "
            "FileFormat must have an object of type 'dict' as its value. "
            "Instead, got an object of type '{}'."
        ),
    }
    FOCE3 = {
        "code": "FOCE-003",
        "message": (
            "The '{}' parameter for a {} must be one of the following types: "
            "'{}'. Instead, got an object of type '{}'."
        ),
    }
    FOCE4 = {
        "code": "FOCE-004",
        "message": (
            "The format string received was '{}', which is not one of the supported "
            "formats. The supported formats are: '{}'. If the format was not provided, "
            "it was inferred from the file extension. Please provide an explicit "
            "format that is supported, or use a FileFormat object."
        ),
    }
    FOCE5 = {
        "code": "FOCE-005",
        "message": "The 'format' parameter must be one of {}, got '{}' instead.",
    }
    FOCE6 = {
        "code": "FOCE-006",
        "message": (
            "The 'format' parameter must be one of {}, got 'None' instead. The most"
            " likely reason is that the format was not provided, and it could not be"
            " inferred from the file extension. Please provide it explicitly."
        ),
    }
    RE1 = {
        "code": "RE-001",
        "message": (
            "The 'function' parameter of the register function  must be an "
            "instance of TabsDataFunction. Either use the @tabset decorator around "
            "your function or create a TabsDataFunction object with your function as "
            "a parameter."
        ),
    }
    RE2 = {
        "code": "RE-002",
        "message": (
            "The 'code_location' and 'save_target' parameters of the register "
            "function cannot cannot be used simultaneously. Either provide "
            "'code_location' and a path to the code you want to be stored, "
            "or 'save_target' with value 'file' (to save only the file where "
            "the function is declared) or 'folder' (to save the entire folder where "
            "the original file of the function is)."
        ),
    }
    RE3 = {
        "code": "RE-003",
        "message": (
            "The 'save_target' parameter of the register function has value {}, "
            "which is not one of the allowed values: {}."
        ),
    }
    RE4 = {
        "code": "RE-004",
        "message": (
            "The 'save_location' parameter of the register function has value "
            "{}, which is not a valid folder path. Please ensure the path to the "
            "folder exists."
        ),
    }
    RE5 = {
        "code": "RE-005",
        "message": (
            "The 'path_to_code' parameter of the register function has value {}, "
            "which is not a valid system path. Please ensure it is a valid path to "
            "either a folder or a file."
        ),
    }
    RE6 = {
        "code": "RE-006",
        "message": (
            "The 'local_packages' parameter provided to the register function must be a"
            " string or a list of strings representing valid paths to folders in your"
            " local system. The provided path '{}' does not exist or is not a"
            " folder."
        ),
    }
    RE7 = {
        "code": "RE-007",
        "message": (
            "The 'requirements' parameter provided to the register function must be a"
            " string representing a valid path to a yaml file in your"
            " local system. The provided path '{}' does not exist or is not a"
            " file."
        ),
    }
    RE8 = {
        "code": "RE-008",
        "message": (
            "The 'requirements' file provided to the register function must contain"
            " the key '{}', which indicates the Python version to use. The provided "
            "file '{}' does not contain this key. The data it contains is: '{}'"
        ),
    }
    RE9 = {
        "code": "RE-009",
        "message": (
            "The 'requirements' file provided to the register function must contain"
            " the key '{}', which indicates the Python packages to install. The "
            "provided file '{}' does not contain this key. The data it contains is: "
            "'{}'"
        ),
    }
    RE10 = {
        "code": "RE-010",
        "message": (
            "The 'requirements' file provided to the register function must contain"
            " the key '{}' with a list of packages to install. The "
            "provided file '{}' contains this key, but it has a content of type '{}'."
        ),
    }
    RE11 = {
        "code": "RE-011",
        "message": (
            "The 'local_packages' parameter must be None or of type 'str' or 'list'."
            " Got '{}' instead."
        ),
    }
    RE12 = {
        "code": "RE-012",
        "message": (
            "The Python version in the requirements file is '{}', which is not "
            "supported. The supported Python versions are: {}. If you are using a "
            "custom requirements file, please ensure that the Python version key "
            "is set to one of the supported versions. If the requirements file "
            "is being automatically generated, please ensure that the Python version "
            "of the environment in which you are currently is one of the supported "
            "versions."
        ),
    }
    SDKE1 = {
        "code": "SDKE-001",
        "message": (
            "The SDK tried to raise an exception of type '{}', but the error code "
            "provided was '{}', which does not start with the expected prefix '{}'."
            " The message provided by the original exception was: '{}'."
        ),
    }
    SCE1 = {
        "code": "SCE-001",
        "message": (
            "The dictionary to build a Secret object must contain exactly one "
            "key, which must be one of the following: {}. Instead, got the following "
            "key(s) in the dictionary: {}."
        ),
    }
    SCE2 = {
        "code": "SCE-002",
        "message": (
            "The '{}' key in the dictionary to build a "
            "Secret must have an object of type 'dict' as its value. "
            "Instead, got an object of type '{}'."
        ),
    }
    SCE3 = {
        "code": "SCE-003",
        "message": (
            "The parameter to build a Secret object must be one of {}, "
            "got '{}' instead."
        ),
    }
    SCE4 = {
        "code": "SCE-004",
        "message": (
            "The vault parameter to build a HashiCorpSecret object must be of type "
            "'str'; got '{}' instead."
        ),
    }
    SCE5 = {
        "code": "SCE-005",
        "message": (
            "The vault parameter to build a HashiCorpSecret object must be a string "
            "containing only uppercase letters, numbers and underscores and cannot "
            "start with a number; got {} instead."
        ),
    }
    SOCE2 = {
        "code": "SOCE-002",
        "message": (
            "Scheme '{}' not supported. The supported schemes are {}."
            " The scheme is inferred from the URI, which should be of the form"
            " 'scheme://path'. The URI provided was '{}'."
        ),
    }
    SOCE4 = {
        "code": "SOCE-004",
        "message": (
            "File format '{}' not supported. The supported formats are"
            " {}. If the format was not provided, it was"
            " inferred from the file(s) extension."
        ),
    }
    SOCE5 = {
        "code": "SOCE-005",
        "message": (
            "The 'initial_last_modified' parameter in a file source must be a"
            " string in ISO 8601 format or a datetime object with timezone "
            "information. Got the string '{}', "
            "but it was not in ISO 8601 format. Ensure that it can be parsed by using"
            " datetime.datetime.fromisoformat()."
        ),
    }
    SOCE6 = {
        "code": "SOCE-006",
        "message": (
            "The 'initial_last_modified' parameter in a file source must be a"
            " string in ISO 8601 format or a datetime object. Instead, got an object of"
            " type '{}'."
        ),
    }
    SOCE12 = {
        "code": "SOCE-012",
        "message": (
            "The 'initial_values' parameter in a MySQLSource must be a 'dict' or "
            "'None', got '{}' instead"
        ),
    }
    SOCE13 = {
        "code": "SOCE-013",
        "message": (
            "The 'path' parameter in a LocalFileSource must be a 'str' or "
            "a 'list[str]', got '{}' instead"
        ),
    }
    SOCE14 = {
        "code": "SOCE-014",
        "message": (
            "Scheme '{}' not supported. The supported scheme is '{}'."
            " The scheme is inferred from the path, which should be of the form"
            " 'scheme://path' or '/path'. The provided path was '{}'."
        ),
    }
    SOCE16 = {
        "code": "SOCE-016",
        "message": (
            "The 'uri' parameter in a S3Source must be a 'str' or "
            "a 'list[str]', got '{}' instead"
        ),
    }
    SOCE17 = {
        "code": "SOCE-017",
        "message": (
            "Scheme '{}' not supported. The supported scheme is '{}'."
            " The scheme is inferred from the URI, which should be of the form"
            " 'scheme://path'. The URI provided was '{}'."
        ),
    }
    SOCE19 = {
        "code": "SOCE-019",
        "message": (
            "The 'query' parameter in a MySQLSource must be a 'str' or a 'list[str]'"
            ", got '{}' instead"
        ),
    }
    SOCE20 = {
        "code": "SOCE-020",
        "message": (
            "The 'credentials' parameter in a S3Source must be a "
            "'S3Credentials' object, got '{}' instead"
        ),
    }
    SOCE22 = {
        "code": "SOCE-022",
        "message": (
            "The 'credentials' parameter in a MySQLSource must be a "
            "'UserPasswordCredentials' object or None, got '{}' instead"
        ),
    }
    SOCE25 = {
        "code": "SOCE-025",
        "message": (
            "The table parameter for a TableInput must represent a "
            "table in the system, got '{}' instead."
        ),
    }
    SOCE26 = {
        "code": "SOCE-026",
        "message": (
            "The 'region' parameter in a S3Source must be a 'str', got '{}' instead"
        ),
    }
    SOCE28 = {
        "code": "SOCE-028",
        "message": (
            "The 'uri' parameter in a AzureSource must be a 'str' or "
            "a 'list[str]', got '{}' instead"
        ),
    }
    SOCE29 = {
        "code": "SOCE-029",
        "message": (
            "Scheme '{}' not supported for AzureSource. The supported scheme is '{}'."
            " The scheme is inferred from the URI, which should be of the form"
            " 'scheme://path'. The URI provided was '{}'."
        ),
    }
    SOCE30 = {
        "code": "SOCE-030",
        "message": (
            "The 'credentials' parameter in a AzureSource must be an "
            "'AzureCredentials' object, got '{}' instead"
        ),
    }
    SOCE31 = {
        "code": "SOCE-031",
        "message": (
            "The 'initial_values' parameter in a PostgresSource must be a 'dict' or "
            "'None', got '{}' instead"
        ),
    }
    SOCE32 = {
        "code": "SOCE-032",
        "message": (
            "The 'query' parameter in a PostgresSource must be a 'str' or a 'list[str]'"
            ", got '{}' instead"
        ),
    }
    SOCE33 = {
        "code": "SOCE-033",
        "message": (
            "The 'credentials' parameter in a PostgresSource must be a "
            "'UserPasswordCredentials' object or None, got '{}' instead"
        ),
    }
    SOCE34 = {
        "code": "SOCE-034",
        "message": (
            "The 'initial_values' parameter in a MariaDBSource must be a 'dict' or "
            "'None', got '{}' instead"
        ),
    }
    SOCE35 = {
        "code": "SOCE-035",
        "message": (
            "The 'query' parameter in a MariaDBSource must be a 'str' or a 'list[str]'"
            ", got '{}' instead"
        ),
    }
    SOCE36 = {
        "code": "SOCE-036",
        "message": (
            "The 'credentials' parameter in a MariaDBSource must be a "
            "'UserPasswordCredentials' object or None, got '{}' instead"
        ),
    }
    SOCE37 = {
        "code": "SOCE-037",
        "message": (
            "The 'initial_values' parameter in a OracleSource must be a 'dict' or "
            "'None', got '{}' instead"
        ),
    }
    SOCE38 = {
        "code": "SOCE-038",
        "message": (
            "The 'query' parameter in a OracleSource must be a 'str' or a 'list[str]'"
            ", got '{}' instead"
        ),
    }
    SOCE39 = {
        "code": "SOCE-039",
        "message": (
            "The 'credentials' parameter in a MariaDBSource must be a "
            "'UserPasswordCredentials' object or None, got '{}' instead"
        ),
    }
    SOCE40 = {
        "code": "SOCE-040",
        "message": (
            "The 'initial_values' parameter must be a dictionary where all keys are "
            "of type 'str', got a key of type '{}' instead."
        ),
    }
    SOCE41 = {
        "code": "SOCE-041",
        "message": (
            "The 'initial_last_modified' parameter in a file source must be a"
            " string in ISO 8601 format or a datetime object with timezone "
            "information. Got '{}', which does not have timezone information. "
            "Ensure that it does by checking that 'last_modified.tzinfo' is not None."
        ),
    }
    SOCE42 = {
        "code": "SOCE-042",
        "message": (
            "The 'credentials' parameter in a MSSQLSource must be a "
            "'UserPasswordCredentials' object or None, got '{}' instead"
        ),
    }
    SOCE43 = {
        "code": "SOCE-043",
        "message": (
            "The 'uri' parameter in a GCSSource must be a 'str' or "
            "a 'list[str]', got '{}' instead"
        ),
    }
    SOCE44 = {
        "code": "SOCE-044",
        "message": (
            "Scheme '{}' not supported for GCSSource. The supported scheme is '{}'."
            " The scheme is inferred from the URI, which should be of the form"
            " 'scheme://path'. The URI provided was '{}'."
        ),
    }
    SOCE45 = {
        "code": "SOCE-045",
        "message": (
            "The 'credentials' parameter in a GCSSource must be a "
            "'GCPCredentials' object, got '{}' instead"
        ),
    }
    SOCE46 = {
        "code": "SOCE-046",
        "message": (
            "The 'conn' parameter in a {} must be a {} instance got '{}' instead."
        ),
    }
    SOCE47 = {
        "code": "SOCE-047",
        "message": (
            "The 'queries' parameter in a {} must be a 'str' or a 'list[str]', got '{}'"
            " instead."
        ),
    }
    SOCE48 = {
        "code": "SOCE-048",
        "message": (
            "The 'initial_values' parameter in a {} must be a 'dict' or 'None', got"
            " '{}' instead."
        ),
    }
    SOCE49 = {
        "code": "SOCE-049",
        "message": (
            "The 'transactional' parameter in a {} must be a 'bool', got '{}' instead."
        ),
    }
    SOCE50 = {
        "code": "SOCE-050",
        "message": (
            "The 'chunk_size' parameter in a {} must be a 'int', got '{}' instead."
        ),
    }
    SOCE51 = {
        "code": "SOCE-051",
        "message": (
            "The 'chunk_size' parameter in a {} must be a positive 'int', got {}"
            " instead."
        ),
    }
    SOCE52 = {
        "code": "SOCE-052",
        "message": (
            "The 'loader' parameter in a {} must be one of {}, got '{}' instead."
        ),
    }
    SOCE53 = {
        "code": "SOCE-053",
        "message": (
            "The 'schema_overrides' parameter in a {} must be either a 'dict', 'list',"
            " or None, got '{}' instead."
        ),
    }
    SOCE54 = {
        "code": "SOCE-054",
        "message": (
            "All items in the 'schema_overrides' list must be a 'dict', got {} of types"
            " '{}' instead."
        ),
    }
    SOCE55 = {
        "code": "SOCE-055",
        "message": (
            "All keys in the 'schema_overrides' dict must be of type 'str', got keys {}"
            " of types '{}' instead."
        ),
    }
    SOCE56 = {
        "code": "SOCE-056",
        "message": (
            "All values in the 'schema_overrides' dict must be of type 'DataType', got"
            " values {} of types '{}' instead."
        ),
    }
    SOCE57 = {
        "code": "SOCE-057",
        "message": (
            "The 'schema_overrides' parameter must be of the same size ({}) as the"
            " number of queries ({})"
        ),
    }
    SOCE58 = {
        "code": "SOCE-058",
        "message": (
            "When the 'queries' parameter is a list, the 'schema_overrides' parameter"
            " must also be a list of the same size ({}), got a '{}' instead."
        ),
    }
    SOCE59 = {
        "code": "SOCE-059",
        "message": (
            "The 'loader' parameter in a {} must be one of {}, got '{}' instead."
        ),
    }
    SOCE60 = {
        "code": "SOCE-060",
        "message": (
            "The 'schema_overrides' parameter in a {} must be either a 'dict', 'list',"
            " or None, got '{}' instead."
        ),
    }
    TF1 = {
        "code": "TF-001",
        "message": "The input DataFrame is missing the required column(s) '{}'.",
    }
    TF2 = {
        "code": "TF-002",
        "message": (
            "TableFrame must be instantiated empty, or with a dictionary, or another"
            " TableFrame. '{}' was provided instead."
        ),
    }
    TF3 = {
        "code": "TF-003",
        "message": (
            "Column names specification as a regular expression is not supported: '{}'."
        ),
    }
    TF4 = {
        "code": "TF-004",
        "message": (
            "Using reserved system column names in expressions is not allowed: '{}'."
        ),
    }
    TF5 = {
        "code": "TF-005",
        "message": (
            "Expr must be instantiated with a polars Expr or a Tabsdata Expr object:"
            " '{}' was provided instead."
        ),
    }
    TF6 = {
        "code": "TF-006",
        "message": (
            "GroupBy must be instantiated with a polars LazyGroupBY or a Tabsdata"
            " LazyGroupBy object: '{}' was provided instead."
        ),
    }
    TF7 = {
        "code": "TF-007",
        "message": (
            "Only polars DataFrame's or LazyFrame's can be wrapped: '{}' was provided "
            "instead."
        ),
    }
    TF8 = {
        "code": "TF-008",
        "message": (
            "Lazy frame not eligible for 'item' operation as it has more than one non "
            "system columns."
        ),
    }
    TF9 = {
        "code": "TF-009",
        "message": (
            "Lazy frame not eligible for 'item' operation as it doesn't look to come "
            "from an aggregation operation."
        ),
    }
    TF10 = {
        "code": "TF-010",
        "message": (
            "Reserved system column names cannot be used to transform a TableFrame: {}."
        ),
    }
    TF11 = {
        "code": "TF-011",
        "message": (
            "Builder fom_polars requires a polars DataFrame or LazyFrameTableFrame, or "
            " None. '{}' was provided instead."
        ),
    }
    TF12 = {
        "code": "TF-012",
        "message": (
            "Builder fom_pandas requires a pandas DataFrame, or None. '{}' was provided"
            " instead."
        ),
    }
    TF13 = {
        "code": "TF-013",
        "message": (
            "Builder fom_dict requires a dictionary, or None. '{}' was provided"
            " instead."
        ),
    }
    TF14 = {
        "code": "TF-014",
        "message": (
            "Internal error: "
            "The input TableFrame is missing the non_optional column(s) '{}'. "
            "Contact Tabsdata for help and to report it."
        ),
    }
    TUCE1 = {
        "code": "TUCE-001",
        "message": (
            "The 'version' parameter of a Version object must be a 'str', "
            "got '{}' instead."
        ),
    }
    TUCE2 = {
        "code": "TUCE-002",
        "message": (
            "The 'initial_version' parameter of a VersionRange object must resolve "
            "to a '{}' object, got '{}' instead that is of type '{}'."
        ),
    }
    TUCE3 = {
        "code": "TUCE-003",
        "message": (
            "The 'final_version' parameter of a VersionRange object must resolve "
            "to a '{}' object, got '{}' instead that is of type '{}'."
        ),
    }
    TUCE4 = {
        "code": "TUCE-004",
        "message": (
            "The 'version' parameter to build any Version object must be one of '{}', "
            "got type '{}' instead."
        ),
    }
    TUCE5 = {
        "code": "TUCE-005",
        "message": (
            "A string to create a VersionRange object must contain exactly two "
            "valid versions, separated by a '..', got {} instead."
        ),
    }
    TUCE6 = {
        "code": "TUCE-006",
        "message": (
            "The 'version_list' parameter to build a VersionList object must be of type"
            "'{}', got '{}' instead."
        ),
    }
    TUCE7 = {
        "code": "TUCE-007",
        "message": (
            "The 'version_list' parameter must be a list of objects that resolve to "
            "type '{}', got '{}' that is of type '{}' instead."
        ),
    }
    TUCE8 = {
        "code": "TUCE-008",
        "message": (
            "The 'version_list' parameter must be a list of 2 or more elements, "
            "got '{}' that has length '{}' instead."
        ),
    }
    TUCE9 = {
        "code": "TUCE-009",
        "message": (
            "A valid version string must be of the form 'HEAD', 'HEAD^', 'HEAD~1', "
            "'INITIAL', 'INITIAL^', 'INITIAL~1', 'NEW', "
            "or a Hash, i.e., match the regex '{}'. Got '{}' instead."
        ),
    }
    TUCE10 = {
        "code": "TUCE-010",
        "message": (
            "The 'collection' parameter to build a URI object must be of type 'str' or "
            "'None', got '{}' instead."
        ),
    }
    TUCE12 = {
        "code": "TUCE-012",
        "message": (
            "The 'table' parameter to build a TableURI object must be of type 'str' or "
            "'None', got '{}' instead."
        ),
    }
    TUCE13 = {
        "code": "TUCE-013",
        "message": (
            "The 'uri' parameter to build a TableURI must be of the form "
            "'<collection/>table<@versions>', where everything inside <> is optional, "
            "got '{}' instead."
        ),
    }
    TUCE14 = {
        "code": "TUCE-014",
        "message": (
            "The 'uri' parameter to build a TableURI object must be of type 'str' or "
            "'TableURI', got '{}' instead."
        ),
    }
    TUCE15 = {
        "code": "TUCE-015",
        "message": "A TableURI object must have a valid 'table' parameter.",
    }
    STE1 = {
        "code": "STE-001",
        "message": (
            "The 'start' parameter of a StageTrigger object must be a datetime object "
            "or None; got '{}' of type {} instead."
        ),
    }
    STE2 = {
        "code": "STE-002",
        "message": (
            "The 'end' parameter of a StageTrigger object must be a datetime object "
            "or None; got '{}' of type {} instead."
        ),
    }
    STE3 = {
        "code": "STE-003",
        "message": (
            "The 'start' parameter of a StageTrigger object must be earlier than the"
            " 'end' parameter. Got start='{}' and end='{}'."
        ),
    }
    STE4 = {
        "code": "STE-004",
        "message": (
            "The 'start' parameter of a StageTrigger object must be either None or a "
            "datetime object with the timezone parameter informed. Got start='{}', "
            "which is a datetime object but does not have the timezone parameter "
            "informed."
        ),
    }
    STE5 = {
        "code": "STE-005",
        "message": (
            "The 'end' parameter of a StageTrigger object must be either None or a "
            "datetime object with the timezone parameter informed. Got end='{}', "
            "which is a datetime object but does not have the timezone parameter "
            "informed."
        ),
    }
    STE6 = {
        "code": "STE-006",
        "message": (
            "The 'group_id' parameter must be defined either in {} or in {}, "
            "but it was not defined in either of them."
        ),
    }
    STR1 = {
        "code": "STR-001",
        "message": "KafkaStageTrigger execution failed with status: {}",
    }

    # Testing
    DQC0 = object()

    # _operator.py
    DQC1 = object()
    DQC2 = object()
    DQC3 = object()
    DQC4 = object()
    DQC5 = object()
    DQC6 = object()
    DQC7 = object()
    DQC8 = object()
    DQC9 = object()
    DQC10 = object()
    DQC11 = object()
    DQC12 = object()
    DQC13 = object()
    DQC14 = object()
    DQC15 = object()
    DQC16 = object()
    DQC17 = object()
    DQC18 = object()
    DQC19 = object()
    DQC20 = object()
    DQC21 = object()
    DQC22 = object()
    DQC23 = object()
    DQC24 = object()
    DQC25 = object()
    DQC26 = object()
    DQC27 = object()
    DQC28 = object()
    DQC29 = object()
    DQC30 = object()
    DQC31 = object()

    # _classifier.py
    DQC101 = object()
    DQC102 = object()
    DQC103 = object()
    DQC104 = object()
    DQC105 = object()
    DQC106 = object()
    DQC107 = object()
    DQC108 = object()
    DQC109 = object()
    DQC110 = object()
    DQC111 = object()
    DQC112 = object()
    DQC113 = object()
    DQC114 = object()
    DQC115 = object()
    DQC116 = object()
    DQC117 = object()
    DQC118 = object()
    DQC119 = object()
    DQC120 = object()
    DQC121 = object()
    DQC122 = object()
    DQC123 = object()
    DQC124 = object()
    DQC125 = object()
    DQC126 = object()
    DQC127 = object()
    DQC128 = object()
    DQC129 = object()
    DQC130 = object()
    DQC131 = object()
    DQC132 = object()
    DQC133 = object()
    DQC134 = object()
    DQC135 = object()
    DQC136 = object()
    DQC137 = object()
    DQC138 = object()
    DQC139 = object()
    DQC140 = object()
    DQC141 = object()
    DQC142 = object()
    DQC143 = object()
    DQC144 = object()
    DQC145 = object()
    DQC146 = object()
    DQC147 = object()
    DQC148 = object()
    DQC149 = object()
    DQC150 = object()
    DQC151 = object()
    DQC152 = object()
    DQC153 = object()
    DQC154 = object()
    DQC155 = object()

    # _dataquality.py
    DQC201 = object()
    DQC202 = object()
    DQC203 = object()
    DQC204 = object()

    # destination configuration errors
    DECE100 = object()
    DECE101 = object()
    DECE102 = object()
    DECE103 = object()
    DECE104 = object()
    DECE105 = object()
    DECE106 = object()
    DECE107 = object()
    DECE108 = object()
    DECE109 = object()
    DECE110 = object()
    DECE111 = object()
    DECE112 = object()

    # source configuration errors
    SOCE100 = object()
    SOCE101 = object()
    SOCE102 = object()
    SOCE103 = object()
    SOCE104 = object()
    SOCE105 = object()
    SOCE106 = object()
    SOCE107 = object()
    SOCE108 = object()
    SOCE109 = object()
    SOCE110 = object()
    SOCE111 = object()
    SOCE112 = object()

    # stage trigger configuration errors
    STCE100 = object()
    STCE101 = object()
    STCE102 = object()
    STCE103 = object()
    # CDC common errors
    CDC1 = {
        "code": "CDC-001",
        "message": "Invalid table specification '{}': {}",
    }
    CDC2 = {
        "code": "CDC-002",
        "message": "Table {}.{} not found in database",
    }
    CDC3 = {
        "code": "CDC-003",
        "message": "Table {}.{} has no columns",
    }
    CDC4 = {
        "code": "CDC-004",
        "message": "CD table {}.{} not found in ASN registration table "
        "{}.ibmsnap_register. Refer to the documentation for instructions on "
        "configuring Db2 table replication using ASN, or contact your system "
        "administrator to configure it.",
    }
    CDC5 = {
        "code": "CDC-005",
        "message": "Unknown CDC operation type '{}' from {} in table {}",
    }
    CDC6 = {
        "code": "CDC-006",
        "message": "Unsupported start_from value '{}'",
    }
    CDC7 = {
        "code": "CDC-007",
        "message": "Column count mismatch for table {}.{}: "
        "catalog returned {} columns but inspector returned {}",
    }
    CDC8 = {
        "code": "CDC-008",
        "message": "Column name mismatch for table {}.{}: "
        "catalog column '{}' does not match inspector column '{}'",
    }

    # CDC MySQL errors
    CDCMYSQL1 = {
        "code": "CDCMYSQL-001",
        "message": "Invalid MySQL CDC connection configuration: {}",
    }
    CDCMYSQL2 = {
        "code": "CDCMYSQL-002",
        "message": "Invalid MySQL CDC trigger configuration: {}",
    }

    # CDC PostgreSQL errors
    CDCPOSTGRES1 = {
        "code": "CDCPOSTGRES-001",
        "message": "Invalid PostgreSQL CDC connection configuration: {}",
    }
    CDCPOSTGRES2 = {
        "code": "CDCPOSTGRES-002",
        "message": "Invalid PostgreSQL CDC trigger configuration: {}",
    }
    CDCPOSTGRES3 = {
        "code": "CDCPOSTGRES-003",
        "message": "Unsupported wal2json format version '{}'",
    }

    # CDC DB2 errors
    CDCDB21 = {
        "code": "CDCDB2-001",
        "message": "Invalid DB2 CDC connection configuration: {}",
    }
    CDCDB22 = {
        "code": "CDCDB2-002",
        "message": "Invalid DB2 CDC trigger configuration: {}",
    }

    def code(self) -> str:
        if type(self.value) is dict:
            code = self.value.get("code")
        else:
            code = self.name
            name, number = re.split(r"(?=\d)", code, maxsplit=1)
            code = f"{name}-{number.zfill(3)}"
        return code

    def message(self) -> str:
        message = None
        if type(self.value) is dict:
            message = self.value.get("message")
        if not message:
            message = "{}"
        return message


class TabsDataException(Exception):
    """
    Base exception for all exceptions in the Tabs Data SDK.
    """

    CODE_PREFIX: str

    def __init__(self, error_code: ErrorCode, *args):
        message = error_code.message().format(*args)
        super().__init__(message if message else "Unknown error")
        self._error_code = error_code
        self.code = error_code.code()
        self.message = message
        if not self.code.startswith(self.CODE_PREFIX):
            raise SDKError(
                ErrorCode.SDKE1,
                self.__class__.__name__,
                self.code,
                self.CODE_PREFIX,
                message,
            )

    @property
    def error_code(self) -> ErrorCode:
        return self._error_code


class ConnectionConfigurationError(TabsDataException):
    """
    Exception raised when the creation or modification of a Connection object fails.
    """

    CODE_PREFIX = "CXCE"


class CredentialsConfigurationError(TabsDataException):
    """
    Exception raised when the creation or modification of a Credentials object fails.
    """

    CODE_PREFIX = "CCE"


class DataQualityConfigurationError(TabsDataException):
    """
    Exception raised when the creation or modification of a DataQuality object or one of
    its auxiliary objects fails.
    """

    CODE_PREFIX = "DQCE"


class DataQualityProcessingError(TabsDataException):
    """
    Exception raised when processing the DataQuality specifications during a
    function registration or execution.
    """

    CODE_PREFIX = "DQPE"


class DecoratorConfigurationError(TabsDataException):
    """
    Exception raised when the creation or modification of a decorator object fails.
    """

    CODE_PREFIX = "DCE"


class DestinationConfigurationError(TabsDataException):
    """
    Exception raised when the creation or modification of a destination object fails.
    """

    CODE_PREFIX = "DECE"


class FormatConfigurationError(TabsDataException):
    """
    Exception raised when the creation or modification of a Format object fails.
    """

    CODE_PREFIX = "FOCE"


class FunctionConfigurationError(TabsDataException):
    """
    Exception raised when a function is not properly configured to be registered.
    """

    CODE_PREFIX = "FCE"


class RegistrationError(TabsDataException):
    """
    Exception raised when registration of a function in the server fails.
    """

    CODE_PREFIX = "RE"


class SDKError(TabsDataException):
    CODE_PREFIX = "SDKE"
    """
    Exception raised when an internal error occurs. This is likely a bug in the SDK.
    """


class SecretConfigurationError(TabsDataException):
    """
    Exception raised when the creation or modification of a Secret object fails.
    """

    CODE_PREFIX = "SCE"


class SourceConfigurationError(TabsDataException):
    """
    Exception raised when the creation or modification of a source object fails.
    """

    CODE_PREFIX = "SOCE"


class TableFrameError(TabsDataException):
    """
    Exception raised when handling a TableFrame.
    """

    CODE_PREFIX = "TF"


class TabsdataServerError(TabsDataException):
    """
    Exception raised when the server returns an error.
    """

    CODE_PREFIX = "TSE"


class TableURIConfigurationError(TabsDataException):
    """
    Exception raised when the creation or modification of a URI object or one of
    its auxiliary objects fails.
    """

    CODE_PREFIX = "TUCE"


class StageTriggerConfigurationError(TabsDataException):
    """
    Exception raised when the creation or modification of a StageTrigger object or one
    of its auxiliary objects fails.
    """

    CODE_PREFIX = "STE"


class StageTriggerExecutionError(TabsDataException):
    """
    Exception raised when a StageTrigger execution fails during runtime.
    """

    CODE_PREFIX = "STR"


class CdcError(TabsDataException):
    """
    Exception raised for generic CDC errors.
    """

    CODE_PREFIX = "CDC"


class Db2CdcError(CdcError):
    """
    Exception raised for DB2 CDC errors.
    """

    CODE_PREFIX = "CDCDB2"


class MySQLCdcError(CdcError):
    """
    Exception raised for MySQL CDC errors.
    """

    CODE_PREFIX = "CDCMYSQL"


class PostgresCdcError(CdcError):
    """
    Exception raised for PostgreSQL CDC errors.
    """

    CODE_PREFIX = "CDCPOSTGRES"


class ConstructorError(TabsDataException):
    """
    Exception raised when public API classes don't support using their
    constructor.
    """

    CODE_PREFIX = "PV"


class ParameterValueError(TabsDataException):
    CODE_PREFIX = ""

    def __init__(
        self,
        error_code: ErrorCode,
        fn_name: str,
        param_names: Union[str | list[str]],
        msg: str,
    ):
        if isinstance(param_names, str):
            param_names = [param_names]
        param_names = ", ".join(str(n) for n in param_names)

        msg = (
            "Parameter value validation error, "
            f"function: '{fn_name}', "
            f"parameter(s): {param_names}. error: {msg}"
        )
        super().__init__(error_code, msg)


class ParameterError(NamedTuple):
    name: str
    type: str
    value: str
    msg: str


class ParameterValidationError(TabsDataException):
    """
    Exception raised when public API parameter validation fails.
    """

    CODE_PREFIX = ""

    def __init__(
        self,
        function: types.FunctionType | types.MethodType,
        error_code: ErrorCode,
        pyd_validation_error: ValidationError,
    ):
        if not function:
            raise Exception("Function param cannot be None")
        if not callable(function):
            raise Exception("Function param is not a Callable object")
        if not error_code:
            raise Exception("Error_code param cannot be None")
        if not isinstance(pyd_validation_error, ValidationError):
            raise Exception(
                "Parameter 'pyd_validation_error' is not a ValidationError object"
            )

        mod, cls, fn = _get_function_details(function)
        if cls:
            f_name = f"{cls}.{fn}"
        else:
            f_name = f"{fn}"

        # Get the parameters type hints
        param_type_dict = _get_param_and_type_str_map(function)

        # Putting errors in a dict with the param as key to keep the last
        # error for the param
        errors = {}
        for p_err in pyd_validation_error.errors():
            param_name = _format_loc(function, p_err["loc"])
            # None means the parameter does not exist as there is
            # no type constraint. If it were an unhinted parameter,
            # it would not produce an error.
            param_type = param_type_dict.get(param_name, None)
            param_val = _format_value(p_err["input"])
            param_msg = p_err["msg"]
            errors[param_name] = ParameterError(
                param_name, param_type, param_val, param_msg
            )

        error_msg = (
            f"{error_code.code()} Parameter(s) validation error\n"
            f"    Function: '{f_name}'. Parameters with issues:\n"
        )
        errors = list(errors.values())
        for param_name, param_type, param_val, param_msg in errors:
            if param_type:
                error_msg += (
                    f"        {param_name}={param_val}. Expected type: {param_type}.\n"
                )
            else:
                error_msg += (
                    f"        {param_name}={param_val}. Unexpected parameter.\n"
                )

        super().__init__(error_code, error_msg)
        self._mod = mod
        self._cls = cls
        self._fn = fn
        self._errors = errors

    @property
    def mod(self) -> str:
        return self._mod

    @property
    def cls(self) -> Optional[str]:
        return self._cls

    @property
    def fn(self) -> Optional[str]:
        return self._fn

    @property
    def errors(self) -> Collection[ParameterError]:
        return self._errors


def _get_function_details(
    func: types.FunctionType | types.MethodType,
) -> tuple[str, str | None, str]:
    # 1. Module Name
    module = func.__module__

    # 2. Function Name
    name = func.__name__

    # 3. Class Name (Parsing __qualname__)
    # __qualname__ returns "ClassName.func_name" for methods
    qualname = func.__qualname__

    if "." in qualname:
        class_name = qualname.split(".")[-2]
    else:
        class_name = None  # It's a standalone function

    return module, class_name, name


def _format_loc(func: types.FunctionType | types.MethodType, loc: tuple) -> str | None:
    """
    Resolves the parameter name from a validation error location.

    Args:
        func: The function being validated.
        loc: The 'loc' tuple from the Pydantic error (e.g., (0,) or ('arg_name',)).

    Returns:
        The name of the parameter as a string, or None if not found.
    """
    if not loc:
        return None

    first_item = loc[0]

    # Case 1: Pydantic gave us the name directly (Standard behavior)
    if isinstance(first_item, str):
        return first_item

    # Case 2: Pydantic gave us an index (e.g., validating a list of args)
    if isinstance(first_item, int):
        try:
            sig = inspect.signature(func)
            param_names = list(sig.parameters.keys())

            # Map index to name
            if 0 <= first_item < len(param_names):
                return param_names[first_item]

            # Handle variable arguments (*args) if the index exceeds named params
            # (Optional logic: return the name of the *args variable)
            for param in sig.parameters.values():
                if param.kind == param.VAR_POSITIONAL:
                    return param.name

        except (ValueError, IndexError):
            return None

    return None


def _format_value(value):
    # What we consider "basic"
    if isinstance(value, str):
        value = value[:10] + "..." if len(value) > 10 else value
        return f"'{value}'"
    basic_types = (int, float, bool, type(None))
    if isinstance(value, basic_types):
        value = str(value)
        return value
    else:
        return _get_value_type_str(value)


def _clean_metadata(obj: Any) -> str:
    """
    Cleans up the metadata objects inside Annotated.
    - If it's a class (like Strict), returns just the name ("Strict").
    - Otherwise returns the standard representation (e.g. "Ge(ge=0)").
    """
    if isinstance(obj, type):
        return obj.__name__
    return repr(obj)


def _clean_type_str(annotation: Any) -> str:
    """
    Recursively formats type hints.
    """
    # 1. Handle Annotated specifically
    if get_origin(annotation) is Annotated:
        args = get_args(annotation)
        base_type = _clean_type_str(args[0])

        # Clean the metadata arguments (args[1:]) using our helper
        metadata = [_clean_metadata(m) for m in args[1:]]

        return f"Annotated[{base_type}, {', '.join(metadata)}]"

    # 2. Handle Literal (Args are values, not types, so we use repr)
    if get_origin(annotation) is Literal:
        args = get_args(annotation)
        clean_args = [repr(a) for a in args]
        return f"Literal[{', '.join(clean_args)}]"

    # 3. Handle Standard Generics (List, Union, Dict, etc.)
    origin = get_origin(annotation)
    if origin:
        args = get_args(annotation)
        # Recursively clean the inner types
        cleaned_args = [_clean_type_str(a) for a in args]

        # Get the origin name (e.g. 'list', 'Union')
        origin_name = getattr(origin, "__name__", str(origin).replace("typing.", ""))

        return f"{origin_name}[{', '.join(cleaned_args)}]"

    # 4. Handle Standard Classes
    if hasattr(annotation, "__name__"):
        return annotation.__name__

    # 5. Fallback
    return str(annotation).replace("typing.", "")


def _get_param_and_type_str_map(
    func: types.FunctionType | types.MethodType,
) -> dict[str, str]:
    try:
        sig = inspect.signature(func)
    except ValueError:
        return {}

    param_details = {}

    for param in sig.parameters.values():
        name = param.name
        annotation = param.annotation

        if annotation is inspect.Parameter.empty:
            type_str = "Any"
        else:
            type_str = _clean_type_str(annotation)

        param_details[name] = type_str

    return param_details


def _get_value_type_str(value: Any) -> str:
    """
    Returns the type of the value as a string.
    - Basic types (int, str, bool) -> "int"
    - Collections -> "list[int]", "dict[str, int]" (based on 1st element)
    - Objects -> "ClassName"
    """
    if value is None:
        return "NoneType"

    # Get the raw type object
    t = type(value)

    # 1. Handle Lists, Sets, Tuples (Iterables)
    if isinstance(value, (list, set, tuple)):
        collection_name = t.__name__  # e.g., 'list', 'set'

        if not value:
            return f"{collection_name}[Any]"  # Empty collection fallback

        # Grab the first item safely
        first_item = value[0] if isinstance(value, (list, tuple)) else next(iter(value))

        # Recursively determine the type of the content
        inner_type = _get_value_type_str(first_item)
        return f"{collection_name}[{inner_type}]"

    # 2. Handle Dictionaries
    if isinstance(value, dict):
        if not value:
            return "dict[Any, Any]"

        # Grab the first key-value pair
        k, v = next(iter(value.items()))

        key_type = _get_value_type_str(k)
        val_type = _get_value_type_str(v)
        return f"dict[{key_type}, {val_type}]"

    # 3. Handle Basic Types & Custom Objects (Base case)
    return t.__name__
