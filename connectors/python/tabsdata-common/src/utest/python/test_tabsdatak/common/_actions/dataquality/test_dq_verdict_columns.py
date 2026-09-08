#
# Copyright 2026 Tabsdata Inc.
#

import polars as pl
import pytest

import tabsdatak.dataquality as dq
from tabsdatak.common._actions.dataquality.engine import (
    ByteSentinel,
    DataQualityEngine,
)
from tabsdatak.common._actions.dataquality.processor import DataQualityProcessor
from tabsdatak.dataquality._error import (
    DataQualityErrorCode,
    DataQualityProcessingError,
    ParameterValidationError,
)
from tabsdatak.tableframe._core.hooks import tableframe_of

pytestmark = pytest.mark.dq


def orders():
    return tableframe_of(
        pl.LazyFrame({
            "order_id": [1, 2, 3, 4],
            "discount_pct": [10.0, 25.0, 60.0, 0.0],
            "amount": [100.0, -5.0, 50.0, 20.0],
            "region": ["EU", "US", "EU", "APAC"],
        })
    )


def typed():
    return tableframe_of(pl.LazyFrame({"n": [1.0, -2.0], "s": ["x", "y"]}))


def between(columns, max_val: float, **kwargs) -> dq.IsBetween:
    return dq.IsBetween(columns, min_val=0.0, max_val=max_val, **kwargs)


def action(classifiers, operators=None) -> dq.DataQuality:
    return dq.DataQuality(
        table="orders",
        classifiers=classifiers,
        operators=operators if operators is not None else [dq.Summary()],
    )


def engine_with(calls, frame=None) -> DataQualityEngine:
    engine = DataQualityEngine(frame if frame is not None else orders())
    for method, classifier in calls:
        engine = getattr(engine, method)(classifier)
    return engine


# --- Registration time: collisions decidable from the decorator literals ----


def test_one_classifier_repeating_a_source_column_is_rejected():
    with pytest.raises(ParameterValidationError) as exc:
        action([between(["discount_pct", "discount_pct"], 15.0)])
    assert exc.value.api_error is DataQualityErrorCode.DQC_202


def test_one_classifier_repeating_an_alias_is_rejected():
    with pytest.raises(ParameterValidationError) as exc:
        action([between([("amount", "v"), ("discount_pct", "v")], 15.0)])
    assert exc.value.api_error is DataQualityErrorCode.DQC_202


def test_classifiers_of_different_types_sharing_an_alias_are_rejected():
    with pytest.raises(ParameterValidationError) as exc:
        action([between([("amount", "v")], 15.0), dq.IsPositive([("region", "v")])])
    assert exc.value.api_error is DataQualityErrorCode.DQC_202


def test_an_explicit_none_alias_still_infers_and_collides():
    with pytest.raises(ParameterValidationError) as exc:
        action([
            between([("discount_pct", None)], 15.0),
            between([("discount_pct", None)], 40.0),
        ])
    assert exc.value.api_error is DataQualityErrorCode.DQC_202


def test_a_collision_is_reported_across_three_classifiers():
    with pytest.raises(ParameterValidationError) as exc:
        action([
            between([("amount", "v")], 15.0),
            between([("amount", "v")], 40.0),
            between([("amount", "v")], 90.0),
        ])
    assert "3 classifiers" in str(exc.value)


def test_mixing_bare_and_aliased_entries_in_one_classifier_is_allowed():
    declared = action([between(["discount_pct", ("amount", "amount_ok")], 15.0)])
    assert len(declared.classifiers) == 1


@pytest.mark.parametrize(
    "name",
    ["$td.id", "@td.origin.file.name", "$td.ver.version", "@td.dq.discount"],
)
def test_reserved_prefixes_are_rejected_at_registration(name: str):
    with pytest.raises(ParameterValidationError) as exc:
        action([between([("discount_pct", name)], 15.0)])
    assert exc.value.api_error is DataQualityErrorCode.DQC_203


@pytest.mark.parametrize("name", ["$td", "@td", "td.x", "@TD.x", "not_reserved"])
def test_names_that_only_resemble_a_reserved_prefix_are_allowed(name: str):
    declared = action([between([("discount_pct", name)], 15.0)])
    assert len(declared.classifiers) == 1


def test_a_verdict_column_named_after_its_own_source_is_rejected():
    with pytest.raises(ParameterValidationError) as exc:
        action([between([("discount_pct", "discount_pct")], 15.0)])
    assert exc.value.api_error is DataQualityErrorCode.DQC_204


def test_a_wildcard_and_a_literal_of_one_type_are_allowed_at_registration():
    declared = action([between(None, 15.0), between(["discount_pct"], 40.0)])
    assert len(declared.classifiers) == 2


# --- Execution time: collisions that need the schema ------------------------


def test_two_wildcards_of_one_type_collide_on_every_column():
    with pytest.raises(DataQualityProcessingError) as exc:
        engine_with([
            ("is_between", between(None, 15.0)),
            ("is_between", between(None, 40.0)),
        ])
    assert exc.value.api_error is DataQualityErrorCode.DQPE_17


def test_a_wildcard_colliding_with_a_literal_of_one_type_is_rejected():
    with pytest.raises(DataQualityProcessingError) as exc:
        engine_with([
            ("is_between", between(None, 15.0)),
            ("is_between", between(["discount_pct"], 40.0)),
        ])
    assert exc.value.api_error is DataQualityErrorCode.DQPE_17


def test_an_alias_colliding_with_an_inferred_name_is_rejected():
    with pytest.raises(DataQualityProcessingError) as exc:
        engine_with([
            ("is_between", between(["discount_pct"], 15.0)),
            (
                "is_not_between",
                dq.IsNotBetween(
                    [("amount", "discount_pct_is_between")], min_val=0.0, max_val=1.0
                ),
            ),
        ])
    assert exc.value.api_error is DataQualityErrorCode.DQPE_17


def test_the_error_names_both_classifiers_and_both_source_columns():
    with pytest.raises(DataQualityProcessingError) as exc:
        engine_with([
            ("is_between", between(["discount_pct"], 15.0)),
            (
                "is_not_between",
                dq.IsNotBetween(
                    [("amount", "discount_pct_is_between")], min_val=0.0, max_val=1.0
                ),
            ),
        ])
    message = str(exc.value)
    assert "IsBetween" in message and "IsNotBetween" in message
    assert "'discount_pct'" in message and "'amount'" in message


@pytest.mark.parametrize("victim", ["order_id", "region", "amount"])
def test_a_verdict_column_overwriting_a_user_column_is_rejected(victim: str):
    with pytest.raises(DataQualityProcessingError) as exc:
        engine_with([("is_between", between([("discount_pct", victim)], 15.0))])
    assert exc.value.api_error is DataQualityErrorCode.DQPE_18
    assert "user" in str(exc.value)


def test_a_verdict_column_overwriting_a_system_column_is_rejected():
    with pytest.raises(DataQualityProcessingError) as exc:
        engine_with([("is_between", between([("discount_pct", "$td.id")], 15.0))])
    assert exc.value.api_error is DataQualityErrorCode.DQPE_19


def test_a_categorizer_colliding_with_itself_is_rejected():
    scale = dq.LinearScale(0.0, 100.0, bins=4)
    with pytest.raises(DataQualityProcessingError) as exc:
        engine_with([
            ("categorizer", dq.ScaleCategorizer(["amount"], scale=scale)),
            ("categorizer", dq.ScaleCategorizer(["amount"], scale=scale)),
        ])
    assert exc.value.api_error is DataQualityErrorCode.DQPE_17


def test_a_categorizer_and_a_boolean_classifier_coexist_on_one_column():
    engine = engine_with([
        (
            "categorizer",
            dq.ScaleCategorizer(["amount"], scale=dq.LinearScale(0.0, 100.0, bins=4)),
        ),
        ("is_positive", dq.IsPositive(["amount"])),
    ])
    assert set(engine._cols) == {"amount_category", "amount_is_positive"}


def test_tags_do_not_mask_a_collision():
    with pytest.raises(DataQualityProcessingError) as exc:
        engine_with([
            ("is_between", between(["discount_pct"], 15.0, tags=["strict"])),
            ("is_between", between(["discount_pct"], 40.0, tags=["lenient"])),
        ])
    assert exc.value.api_error is DataQualityErrorCode.DQPE_17


# --- Pruning must not turn into a false positive ----------------------------


def test_a_pruned_classifier_frees_its_alias_for_another_classifier():
    engine = engine_with(
        [
            ("is_positive", dq.IsPositive([("s", "v")])),
            ("is_positive", dq.IsPositive([("n", "v")])),
        ],
        frame=typed(),
    )
    assert set(engine._cols) == {"v"}
    assert engine._cols["v"].column == "n"


def test_a_pruned_classifier_frees_its_alias_whatever_the_order():
    engine = engine_with(
        [
            ("is_positive", dq.IsPositive([("n", "v")])),
            ("is_positive", dq.IsPositive([("s", "v")])),
        ],
        frame=typed(),
    )
    assert set(engine._cols) == {"v"}


def test_a_missing_column_frees_its_alias_for_another_classifier():
    engine = engine_with(
        [
            ("is_positive", dq.IsPositive([("absent", "v")])),
            ("is_positive", dq.IsPositive([("n", "v")])),
        ],
        frame=typed(),
    )
    assert set(engine._cols) == {"v"}


def test_an_alias_naming_a_column_absent_from_the_table_is_allowed():
    engine = engine_with(
        [("is_positive", dq.IsPositive([("n", "absent_elsewhere")]))],
        frame=typed(),
    )
    assert set(engine._cols) == {"absent_elsewhere"}


# --- Chained engine state ---------------------------------------------------


def test_a_collision_is_detected_across_separately_chained_calls():
    engine = engine_with([("is_between", between(["discount_pct"], 15.0))])
    with pytest.raises(DataQualityProcessingError) as exc:
        engine.is_between(between(["discount_pct"], 40.0))
    assert exc.value.api_error is DataQualityErrorCode.DQPE_17


def test_a_rejected_classifier_leaves_the_engine_state_untouched():
    engine = engine_with([("is_between", between([("discount_pct", "d15")], 15.0))])
    with pytest.raises(DataQualityProcessingError):
        engine.is_between(between([("amount", "d15")], 40.0))
    assert set(engine._cols) == {"d15"}
    assert engine._cols["d15"].column == "discount_pct"


# --- Full actions: many classifiers and chained operators -------------------


def compute(declared):
    return DataQualityProcessor.compute([orders()], declared)


def harvest(declared, operator):
    df, schema, cols, tags = compute(declared)
    return DataQualityProcessor.harvest(df, schema, cols, tags, None, operator)


def test_a_full_action_keeps_every_verdict_column_across_four_operators():
    declared = action(
        [
            between([("discount_pct", "disc_within_15")], 15.0, tags=["strict"]),
            between([("discount_pct", "disc_within_40")], 40.0, tags=["lenient"]),
            dq.IsPositive(["amount"], tags=["strict"]),
            dq.IsIn(["region"], values={"EU", "US"}, tags=["lenient"]),
            dq.ScaleCategorizer(
                [("amount", "amount_band")], scale=dq.LinearScale(0.0, 100.0, bins=4)
            ),
        ],
        [
            dq.Enrich(),
            dq.Summary(to_table="orders_summary"),
            dq.Filter(
                dq.AllOk(tags=["strict"]),
                to_table="orders_bad",
                include_quality_columns="all",
            ),
            dq.Select(
                dq.AnyFailed(tags=["lenient"]),
                to_table="orders_lenient_fail",
                include_quality_columns="criteria",
            ),
        ],
    )
    df, _schema, cols, tags = compute(declared)
    assert set(cols) == {
        "disc_within_15",
        "disc_within_40",
        "amount_is_positive",
        "region_is_in",
        "amount_band",
    }
    assert set(tags["strict"]) == {"disc_within_15", "amount_is_positive"}
    assert set(tags["lenient"]) == {"disc_within_40", "region_is_in"}
    columns = df.derived_tf._lf.collect().columns
    for column in cols:
        assert column in columns


def test_both_acceptance_bands_survive_and_disagree_where_they_should():
    declared = action(
        [
            between([("discount_pct", "disc_within_15")], 15.0),
            between([("discount_pct", "disc_within_40")], 40.0),
        ],
        [dq.Enrich()],
    )
    df, _schema, _cols, _tags = compute(declared)
    result = df.derived_tf._lf.collect()
    assert result["disc_within_15"].to_list() == [
        ByteSentinel.TRUE,
        ByteSentinel.FALSE,
        ByteSentinel.FALSE,
        ByteSentinel.TRUE,
    ]
    assert result["disc_within_40"].to_list() == [
        ByteSentinel.TRUE,
        ByteSentinel.TRUE,
        ByteSentinel.FALSE,
        ByteSentinel.TRUE,
    ]


def test_the_summary_reports_one_row_per_declared_classifier():
    summary_operator = dq.Summary(to_table="orders_summary")
    declared = action(
        [
            between([("discount_pct", "disc_within_15")], 15.0),
            between([("discount_pct", "disc_within_40")], 40.0),
            dq.IsPositive(["amount"]),
        ],
        [summary_operator],
    )
    _df, _schema, cols, _tags = compute(declared)
    result = harvest(declared, summary_operator)
    summary = (result.derived or result.output).derived_tf._lf.collect()
    assert len(summary) == len(cols) == 3
    assert set(summary["dq.classifier"].to_list()) == set(cols)


def test_a_collision_inside_a_full_action_fails_the_whole_action():
    declared = dq.DataQuality(
        table="orders",
        classifiers=[
            between([("discount_pct", "verdict")], 15.0),
            dq.IsPositive([("amount", "amount_ok")]),
        ],
        operators=[dq.Enrich(), dq.Summary(to_table="orders_summary")],
    )
    object.__setattr__(
        declared.classifiers[1],
        "column_names",
        [("amount", "verdict")],
    )
    with pytest.raises(DataQualityProcessingError) as exc:
        compute(declared)
    assert exc.value.api_error is DataQualityErrorCode.DQPE_17


def test_a_fail_operator_sees_the_strict_band_that_used_to_be_lost():
    strict_only = dq.Fail(dq.AnyFailed(tags=["strict"]), dq.RowCountThreshold(0))
    declared = action(
        [
            between([("discount_pct", "disc_within_15")], 15.0, tags=["strict"]),
            between([("discount_pct", "disc_within_40")], 40.0, tags=["lenient"]),
        ],
        [strict_only],
    )
    with pytest.raises(DataQualityProcessingError) as exc:
        harvest(declared, strict_only)
    assert exc.value.api_error is DataQualityErrorCode.DQPE_14


def test_a_fail_operator_passes_when_only_the_lenient_band_is_checked():
    lenient_only = dq.Fail(dq.AnyFailed(tags=["lenient"]), dq.RowCountThreshold(1))
    declared = action(
        [
            between([("discount_pct", "disc_within_15")], 15.0, tags=["strict"]),
            between([("discount_pct", "disc_within_40")], 40.0, tags=["lenient"]),
        ],
        [lenient_only],
    )
    assert harvest(declared, lenient_only) is not None


def test_deserializing_an_action_revalidates_the_verdict_columns():
    declared = action([
        between([("discount_pct", "disc_within_15")], 15.0),
        between([("discount_pct", "disc_within_40")], 40.0),
    ])
    payload = declared.to_json().replace("disc_within_40", "disc_within_15")
    with pytest.raises(ParameterValidationError) as exc:
        dq.DataQuality.from_json(payload)
    assert exc.value.api_error is DataQualityErrorCode.DQC_202


# --- Classifying a verdict column -------------------------------------------


def test_classifying_an_aliased_verdict_column_is_rejected_at_registration():
    with pytest.raises(ParameterValidationError) as exc:
        action([
            between([("discount_pct", "disc_flag")], 15.0),
            dq.IsTrue(["disc_flag"]),
        ])
    assert exc.value.api_error is DataQualityErrorCode.DQC_205
    assert "discount_pct" in str(exc.value)


def test_classifying_a_verdict_column_is_rejected_whatever_the_order():
    with pytest.raises(ParameterValidationError) as exc:
        action([
            dq.IsTrue(["disc_flag"]),
            between([("discount_pct", "disc_flag")], 15.0),
        ])
    assert exc.value.api_error is DataQualityErrorCode.DQC_205


def test_consuming_a_verdict_column_as_an_aliased_source_is_rejected():
    with pytest.raises(ParameterValidationError) as exc:
        action([
            between([("discount_pct", "disc_flag")], 15.0),
            dq.IsTrue([("disc_flag", "flag_ok")]),
        ])
    assert exc.value.api_error is DataQualityErrorCode.DQC_205


def test_a_verdict_column_nobody_classifies_is_allowed():
    declared = action([
        between([("discount_pct", "disc_flag")], 15.0),
        between([("amount", "amount_flag")], 40.0),
    ])
    assert len(declared.classifiers) == 2


def test_two_classifiers_on_one_source_with_distinct_aliases_are_allowed():
    declared = action([
        between([("discount_pct", "disc_within_15")], 15.0),
        between([("discount_pct", "disc_within_40")], 40.0),
    ])
    assert len(declared.classifiers) == 2


def test_classifying_an_inferred_verdict_column_fails_at_run_time():
    declared = action([
        between(["discount_pct"], 15.0),
        dq.IsTrue(["discount_pct_is_between"], on_missing_column="fail"),
    ])
    with pytest.raises(DataQualityProcessingError) as exc:
        compute(declared)
    assert exc.value.api_error is DataQualityErrorCode.DQPE_11


def test_a_wildcard_classifier_never_sweeps_in_a_verdict_column():
    engine = engine_with([
        ("is_between", between(["discount_pct"], 15.0)),
        ("is_positive", dq.IsPositive(None)),
    ])
    assert set(engine._cols) == {
        "discount_pct_is_between",
        "order_id_is_positive",
        "discount_pct_is_positive",
        "amount_is_positive",
    }


def test_classifiers_all_read_the_table_as_it_was_before_the_action():
    engine = engine_with([("is_between", between(["discount_pct"], 15.0))])
    assert "discount_pct_is_between" in engine.tf().schema.names()
    assert "discount_pct_is_between" not in engine._schema.names()
