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
    FCE7 = {
        "code": "FCE-007",
        "message": (
            "The source in TabsdataFunction must be of type 'SourcePlugin' or None; "
            "got '{}' instead."
        ),
    }
    FCE8 = {
        "code": "FCE-008",
        "message": (
            "The destination in TabsdataFunction must be of type 'DestinationPlugin' "
            "or None; got '{}' instead."
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
            "'INITIAL', 'INITIAL^', 'INITIAL~1',"
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
