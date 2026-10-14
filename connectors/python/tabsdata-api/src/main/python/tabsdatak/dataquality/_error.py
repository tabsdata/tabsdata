#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
    TabsdataException,
)


class DataQualityException(TabsdataException): ...


class DataQualityProcessingError(DataQualityException): ...


class ParameterValidationError(DataQualityException): ...


class TableURIConfigurationError(DataQualityException): ...


class DataQualityErrorCode(ErrorCode):
    # Testing
    DQC_0 = ErrorDef(ParameterValidationError, "{message}")

    # --- Construction validation (per class). The pydantic ValidationError
    # --- is chained as the cause and carries the field-level detail.
    #
    # Operators, criteria and thresholds (_operator.py)
    DQC_1 = ErrorDef(
        ParameterValidationError,
        "Invalid parameters for a tag-scoped data quality operator.",
    )
    DQC_3 = ErrorDef(
        ParameterValidationError,
        "Invalid parameters for a boolean data quality criteria.",
    )
    DQC_7 = ErrorDef(
        ParameterValidationError,
        "Invalid parameters for a category data quality criteria.",
    )
    DQC_10 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'AllOk' criteria."
    )
    DQC_11 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'AnyFailed' criteria."
    )
    DQC_12 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'InBins' criteria."
    )
    DQC_13 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'NotInBins' criteria."
    )
    DQC_14 = ErrorDef(
        ParameterValidationError,
        "Invalid parameters for the 'RowCountThreshold' threshold.",
    )
    DQC_16 = ErrorDef(
        ParameterValidationError,
        "Invalid parameters for the 'PercentThreshold' threshold.",
    )
    DQC_18 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'Enrich' operator."
    )
    DQC_20 = ErrorDef(
        ParameterValidationError,
        "Invalid parameters for a criteria-based data quality operator.",
    )
    DQC_23 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'Fail' operator."
    )
    DQC_25 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'Filter' operator."
    )
    DQC_26 = ErrorDef(
        ParameterValidationError,
        "'Filter' carries verdict columns only into 'to_table', but "
        "'include_quality_columns' is '{policy}' and no 'to_table' is set. Set "
        "'to_table' or leave 'include_quality_columns' as 'none'.",
    )
    DQC_28 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'Select' operator."
    )
    DQC_31 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'Summary' operator."
    )
    DQC_32 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'Trends' operator."
    )
    DQC_33 = ErrorDef(
        ParameterValidationError,
        "'Trends' operator cannot have 'days', 'weeks' and 'months' all "
        "zero. Either set at least one tier to a positive value or leave "
        "'trends' unset to skip the trend table.",
    )

    # Scales and classifiers (_classifier.py)
    DQC_101 = ErrorDef(ParameterValidationError, "{message}")
    DQC_106 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'IdentityScale'."
    )
    DQC_107 = ErrorDef(ParameterValidationError, "{message}")
    DQC_110 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'LinearScale'."
    )
    DQC_111 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'MonomialScale'."
    )
    DQC_113 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'LogarithmicScale'."
    )
    DQC_115 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'ExponentialScale'."
    )
    DQC_121 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'ScaleCategorizer'."
    )
    DQC_123 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'IsNull' classifier."
    )
    DQC_124 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'IsNotNull' classifier."
    )
    DQC_125 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'IsNan' classifier."
    )
    DQC_126 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'IsNotNan' classifier."
    )
    DQC_127 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'IsNullOrNan' classifier."
    )
    DQC_128 = ErrorDef(
        ParameterValidationError,
        "Invalid parameters for the 'IsNotNullNorNan' classifier.",
    )
    DQC_129 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'IsZero' classifier."
    )
    DQC_130 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'IsNotZero' classifier."
    )
    DQC_131 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'IsPositive' classifier."
    )
    DQC_132 = ErrorDef(
        ParameterValidationError,
        "Invalid parameters for the 'IsPositiveOrZero' classifier.",
    )
    DQC_133 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'IsNegative' classifier."
    )
    DQC_134 = ErrorDef(
        ParameterValidationError,
        "Invalid parameters for the 'IsNegativeOrZero' classifier.",
    )
    DQC_135 = ErrorDef(
        ParameterValidationError,
        "Invalid parameters for a between data quality classifier.",
    )
    DQC_136 = ErrorDef(ParameterValidationError, "{message}")
    DQC_142 = ErrorDef(
        ParameterValidationError,
        "Invalid parameters for an in-set data quality classifier.",
    )
    DQC_144 = ErrorDef(
        ParameterValidationError,
        "Invalid parameters for a regex-match data quality classifier.",
    )
    DQC_145 = ErrorDef(ParameterValidationError, "{message}")
    DQC_146 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'HasLength' classifier."
    )
    DQC_147 = ErrorDef(ParameterValidationError, "{message}")
    DQC_154 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'IsTrue' classifier."
    )
    DQC_155 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'IsFalse' classifier."
    )

    # DataQuality action (_dataquality.py)
    DQC_201 = ErrorDef(
        ParameterValidationError, "Invalid parameters for the 'DataQuality' action."
    )
    DQC_202 = ErrorDef(
        ParameterValidationError,
        "{count} classifiers of this data quality action write to the same verdict "
        "column {column} ({detail}), so all but the last would be lost. Each "
        "classifier must write to a distinct verdict column: name them explicitly "
        "with a '(column, verdict_column)' tuple, for example "
        '\'{classifier}([("{source}", "{source}_variant_1")], ...)\'.',
    )
    DQC_203 = ErrorDef(
        ParameterValidationError,
        "Verdict column name '{column}' declared by classifier '{classifier}' uses "
        "the reserved prefix '{prefix}'. Column names starting with '$td.' are "
        "managed by Tabsdata; choose a name without that prefix.",
    )
    DQC_204 = ErrorDef(
        ParameterValidationError,
        "Classifier '{classifier}' names verdict column '{column}' for source column "
        "'{column}'. Writing the verdict into its own source column would destroy "
        "the classified data; choose a distinct verdict column name.",
    )
    DQC_205 = ErrorDef(
        ParameterValidationError,
        "Classifier '{classifier}' classifies column '{column}', which is the verdict "
        "column produced by classifier '{producer}' on source column '{source}'. "
        "Classifiers of one data quality action all read the table as it was before "
        "the action ran, so a verdict column cannot itself be classified; classify "
        "'{source}' instead, or move the second check to a separate data quality "
        "action on a table that already carries '{column}'.",
    )

    # DQPE codes (DataQualityProcessingError)
    DQPE_1 = ErrorDef(
        DataQualityProcessingError,
        "Unable to resolve table name for action table '{table}'.No matching table "
        "found in input_tables or input_tables_map.",
    )
    DQPE_2 = ErrorDef(
        DataQualityProcessingError,
        "Unable to resolve table name for action table '{table}'. No matching table "
        "found in output_tables.",
    )
    DQPE_3 = ErrorDef(
        DataQualityProcessingError,
        "Unsupported type of data quality operation: {operation}",
    )
    DQPE_4 = ErrorDef(
        DataQualityProcessingError,
        "Duplicate table names found in data quality operations:\n{report}\n\nEach "
        "data quality operation must produce a unique table name.",
    )
    DQPE_5 = ErrorDef(
        DataQualityProcessingError,
        "Input table position {position} is out of range for action table '{table}'. "
        "Available input table frames: {count}.",
    )
    DQPE_6 = ErrorDef(
        DataQualityProcessingError,
        "Output table position {position} is out of range for action table '{table}'. "
        "Available output table frames: {count}.",
    )
    DQPE_7 = ErrorDef(
        DataQualityProcessingError,
        "Mismatch between table_infos and operations for input action table '{}'. "
        "Expected equal lengths, but got {} table_infos and {} operations.",
    )
    DQPE_8 = ErrorDef(
        DataQualityProcessingError,
        "Data quality on output table '{table}' derived {index} of the "
        "{producing} frames its producing operators should yield. The action "
        "declares {operators} operator(s) and registers {infos} derived "
        "table(s).",
    )
    DQPE_9 = ErrorDef(
        DataQualityProcessingError,
        "Unsupported classifier type '{classifier}' for action table '{table}'. This "
        "classifier type is not yet mapped to a DataQualityEngine method.",
    )
    DQPE_10 = ErrorDef(
        DataQualityProcessingError,
        "Quality column '{column}' not found in classifier registry. All quality "
        "columns must be registered in the DataQualityEngine.",
    )
    DQPE_11 = ErrorDef(
        DataQualityProcessingError,
        "Column '{column}' not found in table '{table}'. The specified column does not "
        "exist as a non-system column.",
    )
    DQPE_12 = ErrorDef(
        DataQualityProcessingError,
        "Column '{column}' has incompatible dtype '{dtype}' for classifier "
        "'{classifier}'. The column's data type is not supported by this classifier.",
    )
    DQPE_13 = ErrorDef(
        DataQualityProcessingError,
        "Type conversion error: {detail}. Some value provided to evaluate a data "
        "quality operation has an unsupported data type. Only basic data types and "
        "comon date, time, or duration types are supported.",
    )
    DQPE_14 = ErrorDef(
        DataQualityProcessingError,
        "Data quality check failed for table '{table}': criteria '{criteria}' exceeded "
        "threshold '{threshold}'",
    )
    DQPE_15 = ErrorDef(
        DataQualityProcessingError,
        "The following tables are overridden multiple times:\n{report}",
    )
    DQPE_16 = ErrorDef(
        DataQualityProcessingError,
        "Column '{column}' has incompatible value for classifier '{classifier}'. The "
        "classifier's parameter '{parameter}' with value '{value}' cannot be converted "
        "to the column's data type '{dtype}'.",
    )
    DQPE_17 = ErrorDef(
        DataQualityProcessingError,
        "Verdict column '{column}' is already produced by classifier "
        "'{registered_classifier}' on source column '{registered_source}'; classifier "
        "'{classifier}' on source column '{source}' would overwrite it and its "
        "verdicts would be lost. Name the verdict columns explicitly with a "
        "'(column, verdict_column)' tuple, for example "
        '\'{classifier}([("{source}", "{source}_variant_1")], ...)\'.',
    )
    DQPE_18 = ErrorDef(
        DataQualityProcessingError,
        "Verdict column '{column}' produced by classifier '{classifier}' on source "
        "column '{source}' already exists in table '{table}' as a {kind} column; "
        "writing the verdict would destroy that column's data. Name the verdict "
        "column explicitly with a '(column, verdict_column)' tuple, for example "
        '\'{classifier}([("{source}", "{source}_verdict")], ...)\'.',
    )
    DQPE_19 = ErrorDef(
        DataQualityProcessingError,
        "Verdict column name '{column}' produced by classifier '{classifier}' uses "
        "the reserved prefix '{prefix}'. Column names starting with '$td.' are "
        "managed by Tabsdata; choose a name without that prefix.",
    )

    # TUCE codes (TableURIConfigurationError)
    TUCE_1 = ErrorDef(
        TableURIConfigurationError,
        "The 'version' parameter of a Version object must be a 'str', got '{type}' "
        "instead.",
    )
    TUCE_2 = ErrorDef(
        TableURIConfigurationError,
        "The 'initial_version' parameter of a VersionRange object must resolve to a "
        "'{expected}' object, got '{value}' instead that is of type '{type}'.",
    )
    TUCE_3 = ErrorDef(
        TableURIConfigurationError,
        "The 'final_version' parameter of a VersionRange object must resolve to a "
        "'{expected}' object, got '{value}' instead that is of type '{type}'.",
    )
    TUCE_4 = ErrorDef(
        TableURIConfigurationError,
        "The 'version' parameter to build any Version object must be one of "
        "'{expected}', got type '{type}' instead.",
    )
    TUCE_5 = ErrorDef(
        TableURIConfigurationError,
        "A string to create a VersionRange object must contain exactly two valid "
        "versions, separated by a '..', got {value} instead.",
    )
    TUCE_6 = ErrorDef(
        TableURIConfigurationError,
        "The 'version_list' parameter to build a VersionList object must be of type"
        "'{expected}', got '{type}' instead.",
    )
    TUCE_7 = ErrorDef(
        TableURIConfigurationError,
        "The 'version_list' parameter must be a list of objects that resolve to type "
        "'{expected}', got '{value}' that is of type '{type}' instead.",
    )
    TUCE_8 = ErrorDef(
        TableURIConfigurationError,
        "The 'version_list' parameter must be a list of 2 or more elements, got "
        "'{value}' that has length '{length}' instead.",
    )
    TUCE_9 = ErrorDef(
        TableURIConfigurationError,
        "A valid version string must be of the form 'HEAD', 'HEAD^', 'HEAD~1', "
        "'INITIAL', 'INITIAL^', 'INITIAL~1', 'NEW', or a Hash, i.e., match the regex "
        "'{pattern}'. Got '{value}' instead.",
    )
    TUCE_10 = ErrorDef(
        TableURIConfigurationError,
        "The 'collection' parameter to build a URI object must be of type 'str' or "
        "'None', got '{type}' instead.",
    )
    TUCE_12 = ErrorDef(
        TableURIConfigurationError,
        "The 'table' parameter to build a TableURI object must be of type 'str' or "
        "'None', got '{type}' instead.",
    )
    TUCE_13 = ErrorDef(
        TableURIConfigurationError,
        "The 'uri' parameter to build a TableURI must be of the form "
        "'<collection/>table<@versions>', where everything inside <> is optional, got "
        "'{value}' instead.",
    )
    TUCE_14 = ErrorDef(
        TableURIConfigurationError,
        "The 'uri' parameter to build a TableURI object must be of type 'str' or "
        "'TableURI', got '{type}' instead.",
    )
    TUCE_15 = ErrorDef(
        TableURIConfigurationError,
        "A TableURI object must have a valid 'table' parameter.",
    )
