#
# Copyright 2024 Tabs Data Inc.
#

from enum import Enum


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
            "The 'data' parameter of a 'publisher' decorator must be an 'Input' object "
            "(except 'TableInput'), got '{}' instead."
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
            "The 'destination' parameter of a 'subscriber' decorator must be an "
            "'Output' object (except 'TableOutput'), got '{}' instead."
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
            "The 'trigger_by' parameter of a TabsDataFunction must be a str"
            " or a list of strings, got an object of type '{}' instead."
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
            "The 'name' parameter in TabsdataFunction be of type 'str'; got '{}'"
            " instead."
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
    ICE1 = {
        "code": "ICE-001",
        "message": (
            "Scheme '{}' not currently supported. The supported schemes are "
            "{}. Try using class {} instead."
        ),
    }
    ICE2 = {
        "code": "ICE-002",
        "message": (
            "Scheme '{}' not supported. The supported schemes are {}."
            " The scheme is inferred from the URI, which should be of the form"
            " 'scheme://path'. The URI provided was '{}'."
        ),
    }
    ICE3 = {
        "code": "ICE-003",
        "message": (
            "The 'format' parameter in a FileInput must be a 'str' or "
            "a 'dict', got '{}' instead"
        ),
    }
    ICE4 = {
        "code": "ICE-004",
        "message": (
            "File format '{}' not supported. The supported formats are"
            " {}. If the format was not provided, it was"
            " inferred from the file(s) extension."
        ),
    }
    ICE5 = {
        "code": "ICE-005",
        "message": (
            "The 'initial_last_modified' parameter in a FileInput must be a"
            " string in ISO 8601 format or a datetime object. Got the string '{}', "
            "but it was not in ISO 8601 format. Ensure that it can be parsed by using"
            " datetime.datetime.fromisoformat()."
        ),
    }
    ICE6 = {
        "code": "ICE-006",
        "message": (
            "The 'initial_last_modified' parameter in a FileInput must be a"
            " string in ISO 8601 format or a datetime object. Instead, got an object of"
            " type '{}'."
        ),
    }
    ICE7 = {
        "code": "ICE-007",
        "message": (
            "The 'input' dictionary to build a Input must contain exactly one "
            "key, which must be one of the following: {}. Instead, got the following "
            "key(s) in the dictionary: {}."
        ),
    }
    ICE8 = {
        "code": "ICE-008",
        "message": (
            "The '{}' key in the dictionary to build a "
            "Input must have an object of type 'dict' as its value. "
            "Instead, got an object of type '{}'."
        ),
    }
    ICE9 = {
        "code": "ICE-009",
        "message": (
            "The '{}' dictionary to build a Input must contain the key "
            "'{}', but it is not present."
        ),
    }
    ICE10 = {
        "code": "ICE-010",
        "message": (
            "The '{}' dictionary to build a Input must contain the key "
            "'{}', but it is not present."
        ),
    }
    ICE11 = {
        "code": "ICE-011",
        "message": (
            "The 'input' parameter to build a Output must be a 'dict', "
            "a Input object or 'None', got '{}' instead."
        ),
    }
    ICE12 = {
        "code": "ICE-012",
        "message": (
            "The 'initial_values' parameter in a MySQLSource must be a 'dict' or "
            "'None', got '{}' instead"
        ),
    }
    ICE13 = {
        "code": "ICE-013",
        "message": (
            "The 'path' parameter in a LocalFileSource must be a 'str' or "
            "a 'list[str]', got '{}' instead"
        ),
    }
    ICE14 = {
        "code": "ICE-014",
        "message": (
            "Scheme '{}' not supported. The supported scheme is '{}'."
            " The scheme is inferred from the path, which should be of the form"
            " 'scheme://path' or '/path'. The provided path was '{}'."
        ),
    }
    ICE15 = {
        "code": "ICE-015",
        "message": (
            "The 'format' parameter for the LocalFileSource was not provided, "
            "and we were unable to infer it from the extension of the files in the "
            "path parameter. The supported formats are '{}' and the obtained path was "
            "'{}'."
        ),
    }
    ICE16 = {
        "code": "ICE-016",
        "message": (
            "The 'uri' parameter in a S3Source must be a 'str' or "
            "a 'list[str]', got '{}' instead"
        ),
    }
    ICE17 = {
        "code": "ICE-017",
        "message": (
            "Scheme '{}' not supported. The supported scheme is '{}'."
            " The scheme is inferred from the URI, which should be of the form"
            " 'scheme://path'. The URI provided was '{}'."
        ),
    }
    ICE18 = {
        "code": "ICE-018",
        "message": (
            "The 'format' parameter for the S3Source was not provided, and we were "
            "unable to infer it from the extension of the files in the URI parameter. "
            "The supported formats are '{}' and the obtained URI was '{}'."
        ),
    }
    ICE19 = {
        "code": "ICE-019",
        "message": (
            "The 'query' parameter in a MySQLSource must be a 'str' or a 'list[str]'"
            ", got '{}' instead"
        ),
    }
    ICE20 = {
        "code": "ICE-020",
        "message": (
            "The 'credentials' parameter in a S3Source must be a 'dict' or a "
            "'S3Credentials' object, got '{}' instead"
        ),
    }
    ICE21 = {
        "code": "ICE-021",
        "message": (
            "The 'configs' parameter in a MySQLSource must be a 'dict' or None, "
            "got '{}' instead"
        ),
    }
    ICE22 = {
        "code": "ICE-022",
        "message": (
            "The 'credentials' parameter in a MySQLSource must be a 'dict', a "
            "'UserPasswordCredentials' object or None, got '{}' instead"
        ),
    }
    ICE25 = {
        "code": "ICE-025",
        "message": (
            "The table parameter for a TableInput must represent a "
            "table in the system, got '{}' instead."
        ),
    }
    ICE26 = {
        "code": "ICE-026",
        "message": (
            "The 'region' parameter in a S3FileInput must be a 'str', got '{}' instead"
        ),
    }
    ICE28 = {
        "code": "ICE-028",
        "message": (
            "The 'uri' parameter in a AzureSource must be a 'str' or "
            "a 'list[str]', got '{}' instead"
        ),
    }
    ICE29 = {
        "code": "ICE-029",
        "message": (
            "Scheme '{}' not supported for AzureSource. The supported scheme is '{}'."
            " The scheme is inferred from the URI, which should be of the form"
            " 'scheme://path'. The URI provided was '{}'."
        ),
    }
    ICE30 = {
        "code": "ICE-030",
        "message": (
            "The 'credentials' parameter in a AzureSource must be a 'dict' or a "
            "'AzureCredentials' object, got '{}' instead"
        ),
    }
    ICE31 = {
        "code": "ICE-031",
        "message": (
            "The 'initial_values' parameter in a PostgresSource must be a 'dict' or "
            "'None', got '{}' instead"
        ),
    }
    ICE32 = {
        "code": "ICE-032",
        "message": (
            "The 'query' parameter in a PostgresSource must be a 'str' or a 'list[str]'"
            ", got '{}' instead"
        ),
    }
    ICE33 = {
        "code": "ICE-033",
        "message": (
            "The 'credentials' parameter in a PostgresSource must be a 'dict', a "
            "'UserPasswordCredentials' object or None, got '{}' instead"
        ),
    }
    ICE34 = {
        "code": "ICE-034",
        "message": (
            "The 'initial_values' parameter in a MariaDBSource must be a 'dict' or "
            "'None', got '{}' instead"
        ),
    }
    ICE35 = {
        "code": "ICE-035",
        "message": (
            "The 'query' parameter in a MariaDBSource must be a 'str' or a 'list[str]'"
            ", got '{}' instead"
        ),
    }
    ICE36 = {
        "code": "ICE-036",
        "message": (
            "The 'credentials' parameter in a MariaDBSource must be a 'dict', a "
            "'UserPasswordCredentials' object or None, got '{}' instead"
        ),
    }
    ICE37 = {
        "code": "ICE-037",
        "message": (
            "The 'initial_values' parameter in a OracleSource must be a 'dict' or "
            "'None', got '{}' instead"
        ),
    }
    ICE38 = {
        "code": "ICE-038",
        "message": (
            "The 'query' parameter in a OracleSource must be a 'str' or a 'list[str]'"
            ", got '{}' instead"
        ),
    }
    ICE39 = {
        "code": "ICE-039",
        "message": (
            "The 'credentials' parameter in a MariaDBSource must be a 'dict', a "
            "'UserPasswordCredentials' object or None, got '{}' instead"
        ),
    }
    OCE1 = {
        "code": "OCE-001",
        "message": (
            "Scheme '{}' not currently supported. The supported schemes are "
            "{}. Try using class {} instead."
        ),
    }
    OCE2 = {
        "code": "OCE-002",
        "message": (
            "Scheme '{}' not currently supported. The supported scheme(s) must start "
            "with {}. The scheme is inferred from the URI, which should be of the form"
            " 'scheme<+driver>://path' (the driver is optional). The URI provided was "
            "'{}'."
        ),
    }
    OCE3 = {
        "code": "OCE-003",
        "message": (
            "The 'output' dictionary to build a Output must contain exactly one"
            " key, which must be one of the following: {}. Instead, got the following"
            " key(s) in the dictionary: {}."
        ),
    }
    OCE4 = {
        "code": "OCE-004",
        "message": (
            "The '{}' key in the dictionary to build a "
            "Output must have an object of type 'dict' as its value. "
            "Instead, got an object of type '{}'."
        ),
    }
    OCE5 = {
        "code": "OCE-005",
        "message": (
            "The '{}' dictionary to build a Output must contain the key "
            "'{}', but it is not present."
        ),
    }
    OCE6 = {
        "code": "OCE-006",
        "message": (
            "The '{}' dictionary to build a Output must contain the key "
            "'{}', but it is not present."
        ),
    }
    OCE7 = {
        "code": "OCE-007",
        "message": (
            "The 'output' parameter to build a Output must be a 'dict', "
            "a Output object or 'None', got '{}' instead."
        ),
    }
    OCE8 = {
        "code": "OCE-008",
        "message": (
            "The 'destination_table' parameter in a MySQLDestination must be a "
            "'list' or a 'str', got '{}' instead."
        ),
    }
    OCE9 = {
        "code": "OCE-009",
        "message": (
            "The 'credentials' parameter in a MySQLDestination must be a 'dict', a "
            "'UserPasswordCredentials' object or None; got '{}' instead."
        ),
    }
    OCE10 = {
        "code": "OCE-010",
        "message": (
            "The 'table' parameter in a TableOutput must be a 'str' or a list of 'str';"
            " got '{}' of type '{}' instead."
        ),
    }
    OCE11 = {
        "code": "OCE-011",
        "message": (
            "The 'path' parameter in a LocalFileDestination must be a 'str' or "
            "a 'list[str]', got '{}' instead"
        ),
    }
    OCE12 = {
        "code": "OCE-012",
        "message": (
            "Scheme '{}' not supported. The supported scheme is '{}'."
            " The scheme is inferred from the path, which should be of the form"
            " 'scheme://path' or '/path'. The provided path was '{}'."
        ),
    }
    OCE13 = {
        "code": "OCE-013",
        "message": (
            "File format '{}' not supported. The supported formats are"
            " {}. If the format was not provided, it was"
            " inferred from the file(s) extension."
        ),
    }
    OCE14 = {
        "code": "OCE-014",
        "message": (
            "The 'uri' parameter in a AzureDestination must be a 'str' or "
            "a 'list[str]', got '{}' instead"
        ),
    }
    OCE15 = {
        "code": "OCE-015",
        "message": (
            "Scheme '{}' not supported for AzureDestination. The supported scheme is"
            " '{}'. The scheme is inferred from the URI, which should be of the form"
            " 'scheme://path'. The URI provided was '{}'."
        ),
    }
    OCE16 = {
        "code": "OCE-016",
        "message": (
            "The 'credentials' parameter in a AzureDestination must be a 'dict' or a "
            "'AzureCredentials' object, got '{}' instead"
        ),
    }
    OCE17 = {
        "code": "OCE-017",
        "message": (
            "The 'uri' parameter in a S3Destination must be a 'str' or "
            "a 'list[str]', got '{}' instead"
        ),
    }
    OCE18 = {
        "code": "OCE-018",
        "message": (
            "The 'region' parameter in a S3FileOutput must be a 'str', got '{}' instead"
        ),
    }
    OCE19 = {
        "code": "OCE-019",
        "message": (
            "The 'credentials' parameter in a S3Destination must be a 'dict' or a "
            "'S3Credentials' object, got '{}' instead"
        ),
    }
    OCE20 = {
        "code": "OCE-020",
        "message": (
            "The 'destination_table' parameter in a PostgresDestination must be a "
            "'list' or a 'str', got '{}' instead."
        ),
    }
    OCE21 = {
        "code": "OCE-021",
        "message": (
            "The 'credentials' parameter in a PostgresDestination must be a 'dict', a "
            "'UserPasswordCredentials' object or None; got '{}' instead."
        ),
    }
    OCE22 = {
        "code": "OCE-022",
        "message": (
            "The 'destination_table' parameter in a MariaDBDestination must be a "
            "'list' or a 'str', got '{}' instead."
        ),
    }
    OCE23 = {
        "code": "OCE-023",
        "message": (
            "The 'credentials' parameter in a MariaDBDestination must be a 'dict', a "
            "'UserPasswordCredentials' object or None; got '{}' instead."
        ),
    }
    OCE24 = {
        "code": "OCE-024",
        "message": (
            "The 'destination_table' parameter in a OracleDestination must be a "
            "'list' or a 'str', got '{}' instead."
        ),
    }
    OCE25 = {
        "code": "OCE-025",
        "message": (
            "The 'credentials' parameter in a OracleDestination must be a 'dict', a "
            "'UserPasswordCredentials' object or None; got '{}' instead."
        ),
    }
    OCE26 = {
        "code": "OCE-026",
        "message": (
            "The 'if_table_exists' parameter in a MariaDBDestination must be one of the"
            " following values {}, got '{}' instead"
        ),
    }
    OCE27 = {
        "code": "OCE-027",
        "message": (
            "The 'if_table_exists' parameter in a MySQLDestination must be one of the "
            "following values {}, got '{}' instead"
        ),
    }
    OCE28 = {
        "code": "OCE-028",
        "message": (
            "The 'if_table_exists' parameter in a OracleDestination must be one of the "
            "following values {}, got '{}' instead"
        ),
    }
    OCE29 = {
        "code": "OCE-029",
        "message": (
            "The 'if_table_exists' parameter in a PostgresDestination must be one of"
            " the following values {}, got '{}' instead"
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
            "A valid version string must be of the form 'HEAD', 'HEAD^', 'HEAD~1' or "
            "a Hash, i.e., match the regex '{}'. Got '{}' instead."
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


class TabsDataException(Exception):
    """
    Base exception for all exceptions in the Tabs Data SDK.
    """

    def __init__(self, error_code: ErrorCode, *args):
        self.error_code = error_code
        self.code = self.error_code.value.get("code")
        self.message = self.error_code.value.get("message").format(*args)
        if not self.code.startswith(self.CODE_PREFIX):
            raise SDKError(
                ErrorCode.SDKE1,
                self.__class__.__name__,
                self.code,
                self.CODE_PREFIX,
                self.message,
            )
        super().__init__(self.message if self.message else "Unknown error")


class CredentialsConfigurationError(TabsDataException):
    """
    Exception raised when the creation or modification of a Credentials object fails.
    """

    CODE_PREFIX = "CCE"


class DecoratorConfigurationError(TabsDataException):
    """
    Exception raised when the creation or modification of a decorator object fails.
    """

    CODE_PREFIX = "DCE"


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


class InputConfigurationError(TabsDataException):
    """
    Exception raised when the creation or modification of an Input object fails.
    """

    CODE_PREFIX = "ICE"


class OutputConfigurationError(TabsDataException):
    """
    Exception raised when the creation or modification of an Input object fails.
    """

    CODE_PREFIX = "OCE"


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
