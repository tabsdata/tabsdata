#
# Copyright 2024 Tabs Data Inc.
#

from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path

from tabsdata.exceptions import ErrorCode, FormatConfigurationError

AVRO_EXTENSION = "avro"
CSV_EXTENSION = "csv"
JSON_LINES_EXTENSION = "jsonl"
LOG_EXTENSION = "log"
NDJSON_EXTENSION = "ndjson"
PARQUET_EXTENSION = "parquet"
TABSDATA_EXTENSION = "t"


class FileFormat(ABC):
    """The class of the different possible formats for files."""

    IDENTIFIER = None

    @abstractmethod
    def _to_dict(self) -> dict:
        """
        Returns the dictionary representation of the object.

        Returns:
            dict: A dictionary with the object's attributes.
        """

    def __eq__(self, other: object) -> bool:
        """
        Compares a FileFormat with another object.

        Args:
            other: The other object to compare.

        Returns:
            bool: True if the objects are equal, False otherwise.
        """
        if not isinstance(other, FileFormat):
            return False
        return self._to_dict() == other._to_dict()

    def __repr__(self) -> str:
        """
        Returns a string representation of the FileInput.

        Returns:
            str: A string representation of the FileInput.
        """
        return f"{self.__class__.__name__}({self._to_dict()[self.IDENTIFIER]})"


class FileFormatIdentifier(Enum):
    """
    Enum for the identifiers of the different types of data outputs.
    """

    AVRO = "avro-format"
    CSV = "csv-format"
    LOG = "log-format"
    NDJSON = "ndjson-format"
    PARQUET = "parquet-format"


class AvroFormat(FileFormat):
    """The class of the Parquet file format."""

    IDENTIFIER = FileFormatIdentifier.AVRO.value

    def __init__(self, chunk_size: int = 50000):
        """
        Initializes the AVRO format object.

        Args:
            chunk_size (int, optional): The chunk size for reading/writing AVRO files.
                Defaults to 1000.
        """
        self.chunk_size = _verify_type_or_raise_exception(
            chunk_size, (int,), "chunk_size", self.__class__.__name__
        )

    @property
    def chunk_size(self) -> int:
        """
        Returns the chunk size for reading/writing AVRO files.

        Returns:
            int: The chunk size.
        """
        return self._chunk_size

    @chunk_size.setter
    def chunk_size(self, value: int):
        """
        Sets the chunk size for reading/writing AVRO files.

        Args:
            value (int): The chunk size to set.
        """
        self._chunk_size = _verify_type_or_raise_exception(
            value, (int,), "chunk_size", self.__class__.__name__
        )

    def _to_dict(self) -> dict:
        """
        Returns the dictionary representation of the object.

        Returns:
            dict: A dictionary with the object's attributes.
        """
        return {self.IDENTIFIER: {"chunk_size": self.chunk_size}}


class CSVFormat(FileFormat):
    """The class of the CSV file format.

    Attributes:
        separator (str | int): The separator of the CSV file.
        quote_char (str | int): The quote character of the CSV file.
        eol_char (str | int): The end of line character of the CSV file.
        input_encoding (str): The encoding of the CSV file. Only used when importing
            data.
        input_null_values (list | None): The null values of the CSV file. Only used
            when importing data.
        input_missing_is_null (bool): Whether missing values should be marked as null.
            Only used when importing data.
        input_truncate_ragged_lines (bool): Whether to truncate ragged lines of the
            CSV file. Only used when importing data.
        input_comment_prefix (str | int | None): The comment prefix of the CSV file.
            Only used when importing data.
        input_try_parse_dates (bool): Whether to try parse dates of the CSV file. Only
            used when importing data.
        input_decimal_comma (bool): Whether the CSV file uses decimal comma. Only used
            when importing data.
        input_has_header (bool): If the CSV file has header. Only used when importing
            data.
        input_skip_rows (int): How many rows should be skipped in the CSV file. Only
            used when importing data.
        input_skip_rows_after_header (int): How many rows should be skipped after the
            header in the CSV file. Only used when importing data.
        input_raise_if_empty (bool): If an error should be raised for an empty CSV. Only
            used when importing data.
        input_ignore_errors (bool): If the errors loading the CSV must be ignored. Only
            used when importing data.
        output_include_header (bool): Whether to include header in the CSV
            output. Only used when exporting data.
        output_datetime_format (str | None): A format string, with the
            specifiers defined by the chrono Rust crate. If no format specified,
            the default fractional-second precision is inferred from the maximum
            timeunit found in the frame’s Datetime cols (if any). Only used when
            exporting data.
        output_date_format (str | None): A format string, with the specifiers
            defined by the chrono Rust crate. Only used when exporting data.
        output_time_format (str | None): A format string, with the specifiers
            defined by the chrono Rust crate. Only used when exporting data.
        output_float_scientific (bool | None): Whether to use scientific form
            always (true), never (false), or automatically (None). Only used when
            exporting data.
        output_float_precision (int | None): Number of decimal places to write.
            Only used when exporting data.
        output_null_value (str | None): A string representing null values
            (defaulting to the empty string). Only used when exporting data.
        output_quote_style (str | None): Determines the quoting strategy used.
            Only used when exporting data.
            * necessary (default): This puts quotes around fields only when
            necessary. They are necessary when fields contain a quote, separator
            or record terminator. Quotes are also necessary when writing an empty
            record (which is indistinguishable from a record with one empty field).
            This is the default.
            * always: This puts quotes around every field. Always.
            * never: This never puts quotes around fields, even if that results
            in invalid CSV data (e.g.: by not quoting strings containing the
            separator).
            * non_numeric: This puts quotes around all fields that are
            non-numeric. Namely, when writing a field that does not parse as a
            valid float or integer, then quotes will be used even if they aren`t
            strictly necessary.
        output_maintain_order (bool): Maintain the order in which data is
            processed. Setting this to False will be slightly faster. Only used
            when exporting data.
    """

    IDENTIFIER = FileFormatIdentifier.CSV.value

    DEFAULT_SEPARATOR = ","
    DEFAULT_QUOTE_CHAR = '"'
    DEFAULT_EOL_CHAR = "\n"
    DEFAULT_INPUT_ENCODING = "Utf8"
    DEFAULT_INPUT_NULL_VALUES = None
    DEFAULT_INPUT_MISSING_IS_NULL = True
    DEFAULT_INPUT_TRUNCATE_RAGGED_LINES = False
    DEFAULT_INPUT_COMMENT_PREFIX = None
    DEFAULT_INPUT_TRY_PARSE_DATES = False
    DEFAULT_INPUT_DECIMAL_COMMA = False
    DEFAULT_INPUT_HAS_HEADER = True
    DEFAULT_INPUT_SKIP_ROWS = 0
    DEFAULT_INPUT_SKIP_ROWS_AFTER_HEADER = 0
    DEFAULT_INPUT_RAISE_IF_EMPTY = True
    DEFAULT_INPUT_IGNORE_ERRORS = False
    DEFAULT_OUTPUT_INCLUDE_HEADER = True
    DEFAULT_OUTPUT_DATETIME_FORMAT = None
    DEFAULT_OUTPUT_DATE_FORMAT = None
    DEFAULT_OUTPUT_TIME_FORMAT = None
    DEFAULT_OUTPUT_FLOAT_SCIENTIFIC = None
    DEFAULT_OUTPUT_FLOAT_PRECISION = None
    DEFAULT_OUTPUT_NULL_VALUE = None
    DEFAULT_OUTPUT_QUOTE_STYLE = None
    DEFAULT_OUTPUT_MAINTAIN_ORDER = True

    def __init__(
        self,
        separator: str | int = DEFAULT_SEPARATOR,
        quote_char: str | int = DEFAULT_QUOTE_CHAR,
        eol_char: str | int = DEFAULT_EOL_CHAR,
        input_encoding: str = DEFAULT_INPUT_ENCODING,
        input_null_values: list | None = DEFAULT_INPUT_NULL_VALUES,
        input_missing_is_null: bool = DEFAULT_INPUT_MISSING_IS_NULL,
        input_truncate_ragged_lines: bool = DEFAULT_INPUT_TRUNCATE_RAGGED_LINES,
        input_comment_prefix: str | int | None = DEFAULT_INPUT_COMMENT_PREFIX,
        input_try_parse_dates: bool = DEFAULT_INPUT_TRY_PARSE_DATES,
        input_decimal_comma: bool = DEFAULT_INPUT_DECIMAL_COMMA,
        input_has_header: bool = DEFAULT_INPUT_HAS_HEADER,
        input_skip_rows: int = DEFAULT_INPUT_SKIP_ROWS,
        input_skip_rows_after_header: int = DEFAULT_INPUT_SKIP_ROWS_AFTER_HEADER,
        input_raise_if_empty: bool = DEFAULT_INPUT_RAISE_IF_EMPTY,
        input_ignore_errors: bool = DEFAULT_INPUT_IGNORE_ERRORS,
        output_include_header: bool = DEFAULT_OUTPUT_INCLUDE_HEADER,
        output_datetime_format: str | None = DEFAULT_OUTPUT_DATETIME_FORMAT,
        output_date_format: str | None = DEFAULT_OUTPUT_DATE_FORMAT,
        output_time_format: str | None = DEFAULT_OUTPUT_TIME_FORMAT,
        output_float_scientific: bool | None = DEFAULT_OUTPUT_FLOAT_SCIENTIFIC,
        output_float_precision: int | None = DEFAULT_OUTPUT_FLOAT_PRECISION,
        output_null_value: str | None = DEFAULT_OUTPUT_NULL_VALUE,
        output_quote_style: str | None = DEFAULT_OUTPUT_QUOTE_STYLE,
        output_maintain_order: bool = DEFAULT_OUTPUT_MAINTAIN_ORDER,
    ):
        """
        Initializes the CSV format object.

        Args:
            separator (str | int, optional): The separator of the CSV file.
            quote_char (str | int, optional): The quote character of the CSV file.
            eol_char (str | int, optional): The end of line character of the CSV file.
            input_encoding (str, optional): The encoding of the CSV file. Only used when
                importing data.
            input_null_values (list | None, optional): The null values of the CSV file.
                Only used when importing data.
            input_missing_is_null (bool, optional): Whether missing values should be
                marked as null. Only used when importing data.
            input_truncate_ragged_lines (bool, optional): Whether to truncate ragged
                lines of the CSV file. Only used when importing data.
            input_comment_prefix (str | int | None, optional): The comment prefix of
                the CSV file. Only used when importing data.
            input_try_parse_dates (bool, optional): Whether to try parse dates of the
                CSV file. Only used when importing data.
            input_decimal_comma (bool, optional): Whether the CSV file uses decimal
                comma. Only used when importing data.
            input_has_header (bool, optional): If the CSV file has header. Only used
                when importing data.
            input_skip_rows (int, optional): How many rows should be skipped in the
                CSV file. Only used when importing data.
            input_skip_rows_after_header (int, optional): How many rows should be
                skipped after the header in the CSV file. Only used when importing data.
            input_raise_if_empty (bool, optional): If an error should be raised for an
                empty CSV. Only used when importing data.
            input_ignore_errors (bool, optional): If the errors loading the CSV must be
                ignored. Only used when importing data.
            output_include_header (bool, optional): Whether to include header in the CSV
                output. Only used when exporting data.
            output_datetime_format (str | None, optional): A format string, with the
                specifiers defined by the chrono Rust crate. If no format specified,
                the default fractional-second precision is inferred from the maximum
                timeunit found in the frame’s Datetime cols (if any). Only used when
                exporting data.
            output_date_format (str | None, optional): A format string, with the
                specifiers defined by the chrono Rust crate. Only used when exporting
                data.
            output_time_format (str | None, optional): A format string, with the
                specifiers defined by the chrono Rust crate. Only used when exporting
                data.
            output_float_scientific (bool | None, optional): Whether to use scientific
                form always (true), never (false), or automatically (None). Only used
                when exporting data.
            output_float_precision (int | None, optional): Number of decimal places
                to write. Only used when exporting data.
            output_null_value (str | None, optional): A string representing null values
                (defaulting to the empty string). Only used when exporting data.
            output_quote_style (str | None, optional): Determines the quoting strategy
                used. Only used when exporting data.
                * necessary (default): This puts quotes around fields only when
                necessary. They are necessary when fields contain a quote, separator
                or record terminator. Quotes are also necessary when writing an empty
                record (which is indistinguishable from a record with one empty field).
                This is the default.
                * always: This puts quotes around every field. Always.
                * never: This never puts quotes around fields, even if that results
                in invalid CSV data (e.g.: by not quoting strings containing the
                separator).
                * non_numeric: This puts quotes around all fields that are
                non-numeric. Namely, when writing a field that does not parse as a
                valid float or integer, then quotes will be used even if they aren`t
                strictly necessary.
            output_maintain_order (bool, optional): Maintain the order in which data is
                processed. Setting this to False will be slightly faster. Only used
                when exporting data.
        """
        self.separator = _verify_type_or_raise_exception(
            separator, (str, int), "separator", self.__class__.__name__
        )
        self.quote_char = _verify_type_or_raise_exception(
            quote_char, (str, int), "quote_char", self.__class__.__name__
        )
        self.eol_char = _verify_type_or_raise_exception(
            eol_char, (str, int), "eol_char", self.__class__.__name__
        )
        self.input_encoding = _verify_type_or_raise_exception(
            input_encoding, (str,), "input_encoding", self.__class__.__name__
        )
        self.input_null_values = _verify_type_or_raise_exception(
            input_null_values,
            (list, None),
            "input_null_values",
            self.__class__.__name__,
        )
        self.input_missing_is_null = _verify_type_or_raise_exception(
            input_missing_is_null,
            (bool,),
            "input_missing_is_null",
            self.__class__.__name__,
        )
        self.input_truncate_ragged_lines = _verify_type_or_raise_exception(
            input_truncate_ragged_lines,
            (bool,),
            "input_truncate_ragged_lines",
            self.__class__.__name__,
        )
        self.input_comment_prefix = _verify_type_or_raise_exception(
            input_comment_prefix,
            (str, int, None),
            "input_comment_prefix",
            self.__class__.__name__,
        )
        self.input_try_parse_dates = _verify_type_or_raise_exception(
            input_try_parse_dates,
            (bool,),
            "input_try_parse_dates",
            self.__class__.__name__,
        )
        self.input_decimal_comma = _verify_type_or_raise_exception(
            input_decimal_comma, (bool,), "input_decimal_comma", self.__class__.__name__
        )
        self.input_has_header = _verify_type_or_raise_exception(
            input_has_header, (bool,), "input_has_header", self.__class__.__name__
        )
        self.input_skip_rows = _verify_type_or_raise_exception(
            input_skip_rows, (int,), "input_skip_rows", self.__class__.__name__
        )
        self.input_skip_rows_after_header = _verify_type_or_raise_exception(
            input_skip_rows_after_header,
            (int,),
            "input_skip_rows_after_header",
            self.__class__.__name__,
        )
        self.input_raise_if_empty = _verify_type_or_raise_exception(
            input_raise_if_empty,
            (bool,),
            "input_raise_if_empty",
            self.__class__.__name__,
        )
        self.input_ignore_errors = _verify_type_or_raise_exception(
            input_ignore_errors, (bool,), "input_ignore_errors", self.__class__.__name__
        )
        self.output_include_header = _verify_type_or_raise_exception(
            output_include_header,
            (bool,),
            "output_include_header",
            self.__class__.__name__,
        )
        self.output_datetime_format = _verify_type_or_raise_exception(
            output_datetime_format,
            (str, None),
            "output_datetime_format",
            self.__class__.__name__,
        )
        self.output_date_format = _verify_type_or_raise_exception(
            output_date_format,
            (str, None),
            "output_date_format",
            self.__class__.__name__,
        )
        self.output_time_format = _verify_type_or_raise_exception(
            output_time_format,
            (str, None),
            "output_time_format",
            self.__class__.__name__,
        )
        self.output_float_scientific = _verify_type_or_raise_exception(
            output_float_scientific,
            (bool, None),
            "output_float_scientific",
            self.__class__.__name__,
        )
        self.output_float_precision = _verify_type_or_raise_exception(
            output_float_precision,
            (int, None),
            "output_float_precision",
            self.__class__.__name__,
        )
        self.output_null_value = _verify_type_or_raise_exception(
            output_null_value, (str, None), "output_null_value", self.__class__.__name__
        )
        self.output_quote_style = _verify_type_or_raise_exception(
            output_quote_style,
            (str, None),
            "output_quote_style",
            self.__class__.__name__,
        )
        self.output_maintain_order = _verify_type_or_raise_exception(
            output_maintain_order,
            (bool,),
            "output_maintain_order",
            self.__class__.__name__,
        )

    def _to_dict(self) -> dict:
        """
        Returns the dictionary representation of the object.

        Returns:
            dict: A dictionary with the object's attributes.
        """
        return {
            self.IDENTIFIER: {
                "separator": self.separator,
                "quote_char": self.quote_char,
                "eol_char": self.eol_char,
                "input_encoding": self.input_encoding,
                "input_null_values": self.input_null_values,
                "input_missing_is_null": self.input_missing_is_null,
                "input_truncate_ragged_lines": self.input_truncate_ragged_lines,
                "input_comment_prefix": self.input_comment_prefix,
                "input_try_parse_dates": self.input_try_parse_dates,
                "input_decimal_comma": self.input_decimal_comma,
                "input_has_header": self.input_has_header,
                "input_skip_rows": self.input_skip_rows,
                "input_skip_rows_after_header": self.input_skip_rows_after_header,
                "input_raise_if_empty": self.input_raise_if_empty,
                "input_ignore_errors": self.input_ignore_errors,
                "output_include_header": self.output_include_header,
                "output_datetime_format": self.output_datetime_format,
                "output_date_format": self.output_date_format,
                "output_time_format": self.output_time_format,
                "output_float_scientific": self.output_float_scientific,
                "output_float_precision": self.output_float_precision,
                "output_null_value": self.output_null_value,
                "output_quote_style": self.output_quote_style,
                "output_maintain_order": self.output_maintain_order,
            },
        }


class NDJSONFormat(FileFormat):
    """The class of the log file format."""

    IDENTIFIER = FileFormatIdentifier.NDJSON.value

    def _to_dict(self) -> dict:
        """
        Returns the dictionary representation of the object.

        Returns:
            dict: A dictionary with the object's attributes.
        """
        return {self.IDENTIFIER: {}}


class LogFormat(FileFormat):
    """The class of the log file format."""

    IDENTIFIER = FileFormatIdentifier.LOG.value

    def _to_dict(self) -> dict:
        """
        Returns the dictionary representation of the object.

        Returns:
            dict: A dictionary with the object's attributes.
        """
        return {self.IDENTIFIER: {}}


class ParquetFormat(FileFormat):
    """The class of the Parquet file format."""

    IDENTIFIER = FileFormatIdentifier.PARQUET.value

    def _to_dict(self) -> dict:
        """
        Returns the dictionary representation of the object.

        Returns:
            dict: A dictionary with the object's attributes.
        """
        return {self.IDENTIFIER: {}}


def _verify_type_or_raise_exception(value, tuple_of_types, variable_name, class_name):
    if None in tuple_of_types and value is None:
        return None
    else:
        tuple_of_types = tuple(x for x in tuple_of_types if x is not None)

    if not isinstance(value, tuple_of_types):
        raise FormatConfigurationError(
            ErrorCode.FOCE3, variable_name, class_name, tuple_of_types, type(value)
        )
    if isinstance(value, bool) and bool not in tuple_of_types:
        raise FormatConfigurationError(
            ErrorCode.FOCE3, variable_name, class_name, tuple_of_types, type(value)
        )
    return value


STR_TO_FILE_FORMAT = {
    AVRO_EXTENSION: AvroFormat,
    CSV_EXTENSION: CSVFormat,
    JSON_LINES_EXTENSION: NDJSONFormat,
    LOG_EXTENSION: LogFormat,
    NDJSON_EXTENSION: NDJSONFormat,
    PARQUET_EXTENSION: ParquetFormat,
}


def build_file_format(configuration: dict | str | FileFormat) -> FileFormat:
    """
    Builds a file format object from a dictionary, a string or a Format Object.
    :return: A file format object.
    """
    if isinstance(configuration, FileFormat):
        return configuration
    elif isinstance(configuration, str):
        configuration = configuration.lower()  # Make it case-insensitive
        if configuration not in STR_TO_FILE_FORMAT:
            raise FormatConfigurationError(
                ErrorCode.FOCE4,
                configuration,
                [element for element in STR_TO_FILE_FORMAT],
            )
        return STR_TO_FILE_FORMAT[configuration]()
    elif isinstance(configuration, dict):
        return build_file_format_from_dict(configuration)
    elif configuration is None:
        raise FormatConfigurationError(ErrorCode.FOCE6, [str, FileFormat])
    else:
        raise FormatConfigurationError(
            ErrorCode.FOCE5, [str, FileFormat], type(configuration)
        )


def build_file_format_from_dict(configuration: dict) -> FileFormat:
    valid_identifiers = [element.value for element in FileFormatIdentifier]
    # The input dictionary must have exactly one key, which must be one of the
    # valid identifiers
    if len(configuration) != 1 or next(iter(configuration)) not in valid_identifiers:
        raise FormatConfigurationError(
            ErrorCode.FOCE1, valid_identifiers, list(configuration.keys())
        )
    # Since we have only one key, we select the identifier and the configuration
    identifier, format_configuration = next(iter(configuration.items()))
    # The configuration must be a dictionary
    if not isinstance(format_configuration, dict):
        raise FormatConfigurationError(
            ErrorCode.FOCE2, identifier, type(format_configuration)
        )
    if identifier == FileFormatIdentifier.AVRO.value:
        return AvroFormat(**format_configuration)
    if identifier == FileFormatIdentifier.CSV.value:
        return CSVFormat(**format_configuration)
    elif identifier == FileFormatIdentifier.LOG.value:
        return LogFormat()
    elif identifier == FileFormatIdentifier.PARQUET.value:
        return ParquetFormat()
    elif identifier == FileFormatIdentifier.NDJSON.value:
        return NDJSONFormat()


def get_implicit_format_from_list(path_list: list) -> str:
    # Our current logic is to infer the format from the file extension of the data
    # if it is not provided. We will use the first file extension in the list of
    # paths. To find it, we take the first value after a '.' in the path. If there
    # is no '.' in the path, the format will remain as None
    implicit_format = None
    for path_str in path_list:
        path = Path(path_str)
        if path.suffix:
            implicit_format = path.suffix[1:]  # Remove the leading dot
            break
    return implicit_format
