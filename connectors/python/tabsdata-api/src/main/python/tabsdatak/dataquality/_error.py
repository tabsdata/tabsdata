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


class ParameterValueError(DataQualityException): ...


class TableURIConfigurationError(DataQualityException): ...


class DataQualityErrorCode(ErrorCode):
    # Testing
    DQC_0 = ErrorDef(ParameterValidationError, "{message}")

    # _operator.py
    DQC_1 = ErrorDef(ParameterValidationError, "{message}")
    DQC_2 = ErrorDef(ParameterValidationError, "{message}")
    DQC_3 = ErrorDef(ParameterValidationError, "{message}")
    DQC_4 = ErrorDef(ParameterValidationError, "{message}")
    DQC_5 = ErrorDef(ParameterValidationError, "{message}")
    DQC_6 = ErrorDef(ParameterValidationError, "{message}")
    DQC_7 = ErrorDef(ParameterValidationError, "{message}")
    DQC_8 = ErrorDef(ParameterValidationError, "{message}")
    DQC_9 = ErrorDef(ParameterValidationError, "{message}")
    DQC_10 = ErrorDef(ParameterValidationError, "{message}")
    DQC_11 = ErrorDef(ParameterValidationError, "{message}")
    DQC_12 = ErrorDef(ParameterValidationError, "{message}")
    DQC_13 = ErrorDef(ParameterValidationError, "{message}")
    DQC_14 = ErrorDef(ParameterValidationError, "{message}")
    DQC_15 = ErrorDef(ParameterValidationError, "{message}")
    DQC_16 = ErrorDef(ParameterValidationError, "{message}")
    DQC_17 = ErrorDef(ParameterValidationError, "{message}")
    DQC_18 = ErrorDef(ParameterValidationError, "{message}")
    DQC_19 = ErrorDef(ParameterValidationError, "{message}")
    DQC_20 = ErrorDef(ParameterValidationError, "{message}")
    DQC_21 = ErrorDef(ParameterValidationError, "{message}")
    DQC_22 = ErrorDef(ParameterValidationError, "{message}")
    DQC_23 = ErrorDef(ParameterValidationError, "{message}")
    DQC_24 = ErrorDef(ParameterValidationError, "{message}")
    DQC_25 = ErrorDef(ParameterValidationError, "{message}")
    DQC_26 = ErrorDef(ParameterValidationError, "{message}")
    DQC_27 = ErrorDef(ParameterValidationError, "{message}")
    DQC_28 = ErrorDef(ParameterValidationError, "{message}")
    DQC_29 = ErrorDef(ParameterValidationError, "{message}")
    DQC_30 = ErrorDef(ParameterValidationError, "{message}")
    DQC_31 = ErrorDef(ParameterValidationError, "{message}")

    # _classifier.py
    DQC_101 = ErrorDef(ParameterValidationError, "{message}")
    DQC_102 = ErrorDef(ParameterValidationError, "{message}")
    DQC_103 = ErrorDef(ParameterValidationError, "{message}")
    DQC_104 = ErrorDef(ParameterValidationError, "{message}")
    DQC_105 = ErrorDef(ParameterValidationError, "{message}")
    DQC_106 = ErrorDef(ParameterValidationError, "{message}")
    DQC_107 = ErrorDef(ParameterValidationError, "{message}")
    DQC_108 = ErrorDef(ParameterValidationError, "{message}")
    DQC_109 = ErrorDef(ParameterValidationError, "{message}")
    DQC_110 = ErrorDef(ParameterValidationError, "{message}")
    DQC_111 = ErrorDef(ParameterValidationError, "{message}")
    DQC_112 = ErrorDef(ParameterValidationError, "{message}")
    DQC_113 = ErrorDef(ParameterValidationError, "{message}")
    DQC_114 = ErrorDef(ParameterValidationError, "{message}")
    DQC_115 = ErrorDef(ParameterValidationError, "{message}")
    DQC_116 = ErrorDef(ParameterValidationError, "{message}")
    DQC_117 = ErrorDef(ParameterValidationError, "{message}")
    DQC_118 = ErrorDef(ParameterValidationError, "{message}")
    DQC_119 = ErrorDef(ParameterValidationError, "{message}")
    DQC_120 = ErrorDef(ParameterValidationError, "{message}")
    DQC_121 = ErrorDef(ParameterValidationError, "{message}")
    DQC_122 = ErrorDef(ParameterValidationError, "{message}")
    DQC_123 = ErrorDef(ParameterValidationError, "{message}")
    DQC_124 = ErrorDef(ParameterValidationError, "{message}")
    DQC_125 = ErrorDef(ParameterValidationError, "{message}")
    DQC_126 = ErrorDef(ParameterValidationError, "{message}")
    DQC_127 = ErrorDef(ParameterValidationError, "{message}")
    DQC_128 = ErrorDef(ParameterValidationError, "{message}")
    DQC_129 = ErrorDef(ParameterValidationError, "{message}")
    DQC_130 = ErrorDef(ParameterValidationError, "{message}")
    DQC_131 = ErrorDef(ParameterValidationError, "{message}")
    DQC_132 = ErrorDef(ParameterValidationError, "{message}")
    DQC_133 = ErrorDef(ParameterValidationError, "{message}")
    DQC_134 = ErrorDef(ParameterValidationError, "{message}")
    DQC_135 = ErrorDef(ParameterValidationError, "{message}")
    DQC_136 = ErrorDef(ParameterValidationError, "{message}")
    DQC_137 = ErrorDef(ParameterValidationError, "{message}")
    DQC_138 = ErrorDef(ParameterValidationError, "{message}")
    DQC_139 = ErrorDef(ParameterValidationError, "{message}")
    DQC_140 = ErrorDef(ParameterValidationError, "{message}")
    DQC_141 = ErrorDef(ParameterValidationError, "{message}")
    DQC_142 = ErrorDef(ParameterValidationError, "{message}")
    DQC_143 = ErrorDef(ParameterValidationError, "{message}")
    DQC_144 = ErrorDef(ParameterValidationError, "{message}")
    DQC_145 = ErrorDef(ParameterValidationError, "{message}")
    DQC_146 = ErrorDef(ParameterValidationError, "{message}")
    DQC_147 = ErrorDef(ParameterValidationError, "{message}")
    DQC_148 = ErrorDef(ParameterValidationError, "{message}")
    DQC_149 = ErrorDef(ParameterValidationError, "{message}")
    DQC_150 = ErrorDef(ParameterValidationError, "{message}")
    DQC_151 = ErrorDef(ParameterValidationError, "{message}")
    DQC_152 = ErrorDef(ParameterValidationError, "{message}")
    DQC_153 = ErrorDef(ParameterValidationError, "{message}")
    DQC_154 = ErrorDef(ParameterValidationError, "{message}")
    DQC_155 = ErrorDef(ParameterValidationError, "{message}")

    # _dataquality.py
    DQC_201 = ErrorDef(ParameterValidationError, "{message}")
    DQC_202 = ErrorDef(ParameterValidationError, "{message}")
    DQC_203 = ErrorDef(ParameterValidationError, "{message}")
    DQC_204 = ErrorDef(ParameterValidationError, "{message}")

    # DQCE codes (ParameterValueError)
    DQCE_1 = ErrorDef(
        ParameterValueError,
        "The 'tags' parameter of a Classifier object must be a 'str', a 'list' of "
        "'str', or None; got a list containing the object '{}' of type '{}' instead. "
        "Please ensure that all objects in the list are of type 'str'.",
    )
    DQCE_2 = ErrorDef(
        ParameterValueError,
        "The 'tags' parameter of a Classifier object must be a 'str', a 'list' of "
        "'str', or None; got object '{}' of type '{}' instead.",
    )
    DQCE_3 = ErrorDef(
        ParameterValueError,
        "The 'cols' parameter of a Classifier action must be a 'str', a 'list' of "
        "'str', a 'tuple' with two 'str', a 'list' of 'tuple' of two 'str' or None. "
        "Got the tuple '{}' that has {} elements instead of 2. Please ensure that the "
        "tuple has exactly 2 elements of type 'str', the first representing the column "
        "to apply the classifier to, and the second the column to store the result in.",
    )
    DQCE_4 = ErrorDef(
        ParameterValueError,
        "The 'cols' parameter of a Classifier action must be a 'str', a 'list' of "
        "'str', a 'tuple' with two 'str', a 'list' of 'tuple' of two 'str' or None. "
        "Got the tuple '{}' that has an element that is not a 'str'. Please ensure "
        "that the tuple has exactly 2 elements of type 'str', the first representing "
        "the column to apply the classifier to, and the second the column to store the "
        "result in.",
    )
    DQCE_5 = ErrorDef(
        ParameterValueError,
        "The 'cols' parameter of a Classifier action must be a 'str', a 'list' of "
        "'str', a 'tuple' with two 'str', a 'list' of 'tuple' of two 'str' or None. "
        "Got a list with the tuple '{}' that has {} elements instead of 2. Please "
        "ensure that the tuple has exactly 2 elements of type 'str', the first "
        "representing the column to apply the classifier to, and the second the column "
        "to store the result in.",
    )
    DQCE_6 = ErrorDef(
        ParameterValueError,
        "The 'cols' parameter of a Classifier action must be a 'str', a 'list' of "
        "'str', a 'tuple' with two 'str', a 'list' of 'tuple' of two 'str' or None. "
        "Got a list with the tuple '{}' that has an element that is not a 'str'. "
        "Please ensure that the tuple has exactly 2 elements of type 'str', the first "
        "representing the column to apply the classifier to, and the second the column "
        "to store the result in.",
    )
    DQCE_7 = ErrorDef(
        ParameterValueError,
        "The 'cols' parameter of a Classifier action must be a 'str', a 'list' of "
        "'str', a 'tuple' with two 'str', a 'list' of 'tuple' of two 'str' or None. "
        "Got a list with the element '{}' of type '{}'. Please ensure that all "
        "elements in the list are of type 'str' or all elements in the list are of "
        "type 'tuple' of two 'str'.",
    )
    DQCE_8 = ErrorDef(
        ParameterValueError,
        "The 'cols' parameter of a Classifier action must be a 'str', a 'list' of "
        "'str', a 'tuple' with two 'str', a 'list' of 'tuple' of two 'str' or None. "
        "Got a list with an element of type '{}' and another of type '{}'. Please make "
        "sure that all elements in the list have the same type.",
    )
    DQCE_9 = ErrorDef(
        ParameterValueError,
        "The 'cols' parameter of a Classifier action must be a 'str', a 'list' of "
        "'str', a 'tuple' with two 'str', a 'list' of 'tuple' of two 'str' or None. "
        "Got value '{}' of type '{}' instead.",
    )
    DQCE_10 = ErrorDef(
        ParameterValueError,
        "The 'ignore_nulls' parameter of an IsNan classifier must be a boolean. Got "
        "value '{}' of type '{}' instead.",
    )
    DQCE_11 = ErrorDef(
        ParameterValueError,
        "The 'ignore_nulls' parameter of an IsNotNan classifier must be a boolean. Got "
        "value '{}' of type '{}' instead.",
    )
    DQCE_12 = ErrorDef(
        ParameterValueError,
        "The 'ignore_nulls' parameter of an IsZero classifier must be a boolean. Got "
        "value '{}' of type '{}' instead.",
    )
    DQCE_13 = ErrorDef(
        ParameterValueError,
        "The 'ignore_nulls' parameter of an IsNotZero classifier must be a boolean. "
        "Got value '{}' of type '{}' instead.",
    )
    DQCE_14 = ErrorDef(
        ParameterValueError,
        "The 'ignore_nulls' parameter of an IsPositive classifier must be a boolean. "
        "Got value '{}' of type '{}' instead.",
    )
    DQCE_15 = ErrorDef(
        ParameterValueError,
        "The 'ignore_nulls' parameter of an IsPositiveOrZero classifier must be a "
        "boolean. Got value '{}' of type '{}' instead.",
    )
    DQCE_16 = ErrorDef(
        ParameterValueError,
        "The 'ignore_nulls' parameter of an IsNegative classifier must be a boolean. "
        "Got value '{}' of type '{}' instead.",
    )
    DQCE_17 = ErrorDef(
        ParameterValueError,
        "The 'ignore_nulls' parameter of an IsNegativeOrZero classifier must be a "
        "boolean. Got value '{}' of type '{}' instead.",
    )
    DQCE_18 = ErrorDef(
        ParameterValueError,
        "The 'ignore_nulls' parameter of a Matches classifier must be a boolean. Got "
        "value '{}' of type '{}' instead.",
    )
    DQCE_19 = ErrorDef(
        ParameterValueError,
        "The 'regex' parameter of a Matches classifier must be a string. Got value "
        "'{}' of type '{}' instead.",
    )
    DQCE_20 = ErrorDef(
        ParameterValueError,
        "The 'ignore_nulls' parameter of a DoesNotMatch classifier must be a boolean. "
        "Got value '{}' of type '{}' instead.",
    )
    DQCE_21 = ErrorDef(
        ParameterValueError,
        "The 'regex' parameter of a DoesNotMatch classifier must be a string. Got "
        "value '{}' of type '{}' instead.",
    )
    DQCE_22 = ErrorDef(
        ParameterValueError,
        "The 'min' parameter of a HasLen classifier must be an 'int' or a 'float'. Got "
        "value '{}' of type '{}' instead.",
    )
    DQCE_23 = ErrorDef(
        ParameterValueError,
        "The 'max' parameter of a HasLen classifier must be an 'int' or a 'float'. Got "
        "value '{}' of type '{}' instead.",
    )
    DQCE_24 = ErrorDef(
        ParameterValueError,
        "The 'ignore_nulls' parameter of an IsInRange classifier must be a boolean. "
        "Got value '{}' of type '{}' instead.",
    )
    DQCE_25 = ErrorDef(
        ParameterValueError,
        "The 'min' parameter of an IsInRange classifier must be an 'int' or a 'float'. "
        "Got value '{}' of type '{}' instead.",
    )
    DQCE_26 = ErrorDef(
        ParameterValueError,
        "The 'max' parameter of an IsInRange classifier must be an 'int' or a 'float'. "
        "Got value '{}' of type '{}' instead.",
    )
    DQCE_27 = ErrorDef(
        ParameterValueError,
        "The 'closed' parameter of an IsInRange classifier must be one these values: "
        "{}. Got value '{}' instead.",
    )
    DQCE_28 = ErrorDef(
        ParameterValueError,
        "The 'ignore_nulls' parameter of a HasLen classifier must be a boolean. Got "
        "value '{}' of type '{}' instead.",
    )
    DQCE_29 = ErrorDef(
        ParameterValueError,
        "The 'ignore_nulls' parameter of an IsIn classifier must be a boolean. Got "
        "value '{}' of type '{}' instead.",
    )
    DQCE_30 = ErrorDef(
        ParameterValueError,
        "The 'set' parameter of an IsIn classifier must be a collection, meaning an "
        "object of a class that implements the 'in' operator. Got value '{}' of type "
        "'{}' instead.",
    )
    DQCE_31 = ErrorDef(
        ParameterValueError,
        "The 'ignore_nulls' parameter of an IsNotIn classifier must be a boolean. Got "
        "value '{}' of type '{}' instead.",
    )
    DQCE_32 = ErrorDef(
        ParameterValueError,
        "The 'set' parameter of an IsNotIn classifier must be a collection, meaning an "
        "object of a class that implements the 'in' operator. Got value '{}' of type "
        "'{}' instead.",
    )
    DQCE_33 = ErrorDef(
        ParameterValueError,
        "The 'cols' parameter of a HasColumns classifier must be a 'str' or a list of "
        "'str'. Got a list containing value '{}' of type '{}' instead.",
    )
    DQCE_34 = ErrorDef(
        ParameterValueError,
        "The 'cols' parameter of a HasColumns classifier must be a 'str' or a list of "
        "'str'. Got object '{}' of type '{}' instead.",
    )
    DQCE_35 = ErrorDef(
        ParameterValueError,
        "The 'mode' parameter of a HasColumns classifier must one of the following "
        "'str': {}. Got object '{}' instead.",
    )
    DQCE_36 = ErrorDef(
        ParameterValueError,
        "The 'cols' parameter of a HasColumnTypes classifier must be a tuple of 'str' "
        "and 'DataType' or a list of tuples of 'str' and 'DataType'. Got a single "
        "tuple, but the first element is '{}' of type '{}'. Please ensure that the "
        "first element of the tuple is of type 'str' and it represents the name of the "
        "column to check the type against.",
    )
    DQCE_37 = ErrorDef(
        ParameterValueError,
        "The 'cols' parameter of a HasColumnTypes classifier must be a tuple of 'str' "
        "and 'DataType' or a list of tuples of 'str' and 'DataType'. Got a single "
        "tuple, but the second element is '{}' of type '{}'. Please ensure that the "
        "second element of the tuple is of type 'DataType' and it represents the type "
        "to check the column against.",
    )
    DQCE_38 = ErrorDef(
        ParameterValueError,
        "The 'cols' parameter of a HasColumnTypes classifier must be a tuple of 'str' "
        "and 'DataType' or a list of tuples of 'str' and 'DataType'. Got a list "
        "containing the object '{}' of type '{}' instead.",
    )
    DQCE_39 = ErrorDef(
        ParameterValueError,
        "The 'cols' parameter of a HasColumnTypes classifier must be a tuple of 'str' "
        "and 'DataType' or a list of tuples of 'str' and 'DataType'. Got a list "
        "containing a tuple '{}' of length {} instead. Please ensure that each tuple "
        "has exactly two elements, the first one representing the name of the column "
        "to check and the second one being the type to check against.",
    )
    DQCE_40 = ErrorDef(
        ParameterValueError,
        "The 'cols' parameter of a HasColumnTypes classifier must be a tuple of 'str' "
        "and 'DataType' or a list of tuples of 'str' and 'DataType'. Got a list of "
        "tuples containing tuple '{}', where the first element is '{}' of type '{}'. "
        "Please ensure that the first element of the tuple is of type 'str' and it "
        "represents the name of the column to check the type against.",
    )
    DQCE_41 = ErrorDef(
        ParameterValueError,
        "The 'cols' parameter of a HasColumnTypes classifier must be a tuple of 'str' "
        "and 'DataType' or a list of tuples of 'str' and 'DataType'. Got a list of "
        "tuples containing tuple '{}', where the second element is '{}' of type '{}'. "
        "Please ensure that the second element of the tuple is of type 'DataType' and "
        "it represents the type to check the column against.",
    )
    DQCE_42 = ErrorDef(
        ParameterValueError,
        "The 'cols' parameter of a HasColumnTypes classifier must be a tuple of 'str' "
        "and 'DataType' or a list of tuples of 'str' and 'DataType'. Got object '{}' "
        "of type '{}' instead.",
    )
    DQCE_43 = ErrorDef(
        ParameterValueError,
        "The 'cols' parameter of a HasColumnTypes classifier must be a tuple of 'str' "
        "and 'DataType' or a list of tuples of 'str' and 'DataType'. Got tuple '{}' of "
        "length {} instead. Please ensure that each tuple has exactly two elements, "
        "the first one representing the name of the column to check and the second one "
        "being the type to check against.",
    )
    DQCE_44 = ErrorDef(
        ParameterValueError,
        "The 'tags' parameter of an Operation object must be a 'str', a 'list' of "
        "'str', or None; got a list containing the object '{}' of type '{}' instead. "
        "Please ensure that all objects in the list are of type 'str'.",
    )
    DQCE_45 = ErrorDef(
        ParameterValueError,
        "The 'tags' parameter of an Operation object must be a 'str', a 'list' of "
        "'str', or None; got object '{}' of type '{}' instead.",
    )
    DQCE_46 = ErrorDef(
        ParameterValueError,
        "The 'table' parameter of a Summary object must be a 'str', or None; got "
        "object '{}' of type '{}' instead.",
    )
    DQCE_47 = ErrorDef(
        ParameterValueError,
        "The 'scale_type' of a Scale object must be one of the following: {}. Got "
        "object '{}' instead.",
    )
    DQCE_48 = ErrorDef(
        ParameterValueError,
        "The 'scale_range' parameter of a Scale object must be a tuple with exactly "
        "two elements of type 'int' or 'float'; got object '{}' of type '{}' instead.",
    )
    DQCE_49 = ErrorDef(
        ParameterValueError,
        "The 'scale_range' parameter of a Scale object must be a tuple with exactly "
        "two elements of type 'int' or 'float'; got the tuple '{}' with a length of "
        "'{}' instead.",
    )
    DQCE_50 = ErrorDef(
        ParameterValueError,
        "The 'scale_range' parameter of a Scale object must be a tuple with exactly "
        "two elements of type 'int' or 'float'; got the tuple '{}' which has exactly "
        "two elements, but at least one of them has the wrong type.",
    )
    DQCE_51 = ErrorDef(
        ParameterValueError,
        "The 'scale_tick' parameter of a Scale object must be a an 'int', a 'float', "
        "or None; got '{}' of type '{}' instead.",
    )
    DQCE_52 = ErrorDef(
        ParameterValueError,
        "The 'buckets' parameter of a Scale object must be a an 'int' between 2 and "
        "100; got '{}' of type '{}' instead.",
    )
    DQCE_53 = ErrorDef(
        ParameterValueError,
        "When a Scale object has been provided both a 'buckets' parameter and a "
        "'bucket_names' parameter, the length of the 'bucket_names' parameter must "
        "equal the 'buckets' parameter. Got value {} for 'buckets', and value '{}' of "
        "length {} for 'bucket_names' instead.",
    )
    DQCE_54 = ErrorDef(
        ParameterValueError,
        "The 'bucket_names' parameter of a Scale object must be a list of 'str' or "
        "None. Got the list {} containing the object '{}' of type '{}' instead.",
    )
    DQCE_55 = ErrorDef(
        ParameterValueError,
        "The 'bucket_names' parameter of a Scale object must be a list of 'str' or "
        "None; got object '{}' of type '{}' instead.",
    )
    DQCE_56 = ErrorDef(
        ParameterValueError,
        "The 'scale' parameter of a Categorizer object must be a Scale object ; got "
        "object '{}' of type '{}' instead.",
    )
    DQCE_57 = ErrorDef(
        ParameterValueError,
        "Direct instantiation of the class '{}' is not allowed. Use "
        "ScaleType.linear(), ScaleType.quadratic(), ScaleType.exponential() or "
        "ScaleType.log() to obtain an instance of this class",
    )
    DQCE_58 = ErrorDef(
        ParameterValueError,
        "Direct instantiation of the class '{}' is not allowed. Use Criteria.all_ok(), "
        "Criteria.any_failed(), or Criteria.by_bucket() to obtain an instance of this "
        "class",
    )
    DQCE_59 = ErrorDef(
        ParameterValueError,
        "The 'criteria_type' of a Criteria object must be one of the following: {}. "
        "Got object '{}' instead.",
    )
    DQCE_60 = ErrorDef(
        ParameterValueError,
        "The 'buckets' parameter of a Criteria object must be a collection, meaning an "
        "object of a class that implements the 'in' operator; or None. Got value '{}' "
        "of type '{}' instead.",
    )
    DQCE_61 = ErrorDef(
        ParameterValueError,
        "The 'buckets' parameter of a Criteria.by_buckets() object must be a "
        "collection, meaning an object of a class that implements the 'in' operator. "
        "Got value '{}' of type '{}' instead.",
    )
    DQCE_62 = ErrorDef(
        ParameterValueError,
        "The 'to_table' parameter of a FilterOut object must be a 'str', or None. Got "
        "value '{}' of type '{}' instead.",
    )
    DQCE_63 = ErrorDef(
        ParameterValueError,
        "The 'criteria' parameter of a FilterOut object must be a Criteria object. Got "
        "value '{}' of type '{}' instead.",
    )
    DQCE_64 = ErrorDef(
        ParameterValueError,
        "The 'to_table' parameter of a Select object must be a 'str'; got value '{}' "
        "of type '{}' instead.",
    )
    DQCE_65 = ErrorDef(
        ParameterValueError,
        "The 'criteria' parameter of a Select object must be a Criteria object. Got "
        "value '{}' of type '{}' instead.",
    )
    DQCE_66 = ErrorDef(
        ParameterValueError,
        "The 'criteria' parameter of a Fail object must be a Criteria object. Got "
        "value '{}' of type '{}' instead.",
    )
    DQCE_67 = ErrorDef(
        ParameterValueError,
        "Direct instantiation of the class '{}' is not allowed. Use Threshold.count(), "
        "Threshold.percent(), or Threshold.one() to obtain an instance of this class",
    )
    DQCE_68 = ErrorDef(
        ParameterValueError,
        "The 'count' parameter of a Threshold object must be an 'int' greater than "
        "zero or None; got value '{}' of type '{}' instead.",
    )
    DQCE_69 = ErrorDef(
        ParameterValueError,
        "The 'count' parameter of a Threshold object must be an 'int' greater than "
        "zero or None; got the 'int' '{}' that is not greater than zero.",
    )
    DQCE_70 = ErrorDef(
        ParameterValueError,
        "The 'count' parameter of a Threshold.count() object must be an 'int' greater "
        "than zero; got a None instead.",
    )
    DQCE_71 = ErrorDef(
        ParameterValueError,
        "The 'percent' parameter of a Threshold.percent() object must be a 'float' or "
        "'int' greater than zero and less than 100; got a None instead.",
    )
    DQCE_72 = ErrorDef(
        ParameterValueError,
        "The 'percent' parameter of a Threshold object must be a 'float' or 'int' "
        "greater than zero and less than 100, or None. Got value {} instead, that is "
        "not in the (0, 100) range.",
    )
    DQCE_73 = ErrorDef(
        ParameterValueError,
        "The 'percent' parameter of a Threshold object must be a 'float' or 'int' "
        "greater than zero and less than 100, or None. Got value '{}' of type '{}' "
        "instead.",
    )
    DQCE_74 = ErrorDef(
        ParameterValueError,
        "The 'threshold' parameter of a Fail object must be a Threshold object. Got "
        "value '{}' of type '{}' instead.",
    )
    DQCE_75 = ErrorDef(
        ParameterValueError,
        "The 'table' parameter of a DataQuality object must be a 'str'. Got value '{}' "
        "of type '{}' instead.",
    )
    DQCE_76 = ErrorDef(
        ParameterValueError,
        "The 'classify' parameter of a DataQuality object must be a Classifier object "
        "or a list of Classifier objects. Got value '{}' of type '{}' instead.",
    )
    DQCE_77 = ErrorDef(
        ParameterValueError,
        "The 'classify' parameter of a DataQuality object must be a Classifier object "
        "or a list of Classifier objects. Got a list containing value '{}' of type "
        "'{}' instead.",
    )
    DQCE_78 = ErrorDef(
        ParameterValueError,
        "The 'operations' parameter of a DataQuality object must be an Operation "
        "object or a list of Operation objects. Got value '{}' of type '{}' instead.",
    )
    DQCE_79 = ErrorDef(
        ParameterValueError,
        "The 'operations' parameter of a DataQuality object must be an Operation "
        "object or a list of Operation objects. Got a list containing value '{}' of "
        "type '{}' instead.",
    )
    DQCE_80 = ErrorDef(
        ParameterValueError,
        "The 'buckets' parameter of a Scale object must be a an 'int' between 2 and "
        "100; got the 'int' {} that is outside of that range instead.",
    )
    DQCE_81 = ErrorDef(
        ParameterValueError,
        "The 'column_names' parameter of the 'Criteria.in_buckets' must be a string or "
        "a list of strings; got '{}' instead.",
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
        "Mismatch between table_infos and operations for output action table "
        "'{table}'. Expected equal lengths, but got {index} table_infos and "
        "{producing} operators. Total infoes is: {infos}; Total operators is: "
        "{operators}",
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
