#
# Copyright 2025 Tabs Data Inc.
#

import pytest

from tabsdata import (
    MySQLSource,
    TabsdataFunction,
    UserPasswordCredentials,
    publisher,
    transformer,
)
from tabsdata._table_actions.actions import OnTablesAction
from tabsdata.dataquality import LinearScale
from tabsdata.dataquality._operator import AnyFailed

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


def test_example_publisher():
    # -- Example 1 - Publisher
    #
    # Publishers only allow interception on the tables the publisher returns.
    #
    # It will produce an additional `dq.summary_users` table
    # with the following data:
    #
    #     ==========================================
    #     | metric                  | True | False |
    #     ------------------------------------------
    #     | rows                    | 2500 |     0 |
    #     | all_columns_is_not_null | 1200 |  1300 |
    #     | age_is_positive         | 2400 |   100 |
    #     | gender_in               | 2490 |    10 |
    #     ==========================================
    #
    # Note: DQ table name and DQ table column names are customizable
    #       via parameters os the check/action constructors
    #

    import tabsdata.dataquality as dq

    classify_list = [
        dq.IsNotNull(),
        dq.IsPositiveOrZero("age"),
        dq.IsIn("gender", values={"M", "F", "O"}),
    ]
    op_list = [dq.Summary()]

    @publisher(
        source=MySQLSource(
            uri=f"mysql://corp_server:3307/hr",
            query=[
                (
                    "SELECT * FROM users WHERE last_updated > :last_updated ORDER BY"
                    " last_updated"
                ),
                "SELECT * FROM offices",
            ],
            credentials=UserPasswordCredentials("root", "mysql"),
            initial_values={"last_updated": "2025/01/01T00:00:00.000Z"},
        ),
        tables=["users", "offices"],
        on_tables=[
            dq.DataQuality(
                table="users",
                classifiers=classify_list,
                operators=op_list,
            ),
        ],
    )
    def pub1(users, offices):
        return users, offices

    assert isinstance(pub1, TabsdataFunction)
    pub1: TabsdataFunction = pub1
    output_actions = pub1.output_actions
    assert isinstance(output_actions, list)
    assert all(isinstance(action, OnTablesAction) for action in output_actions)
    assert len(output_actions) == 1
    action: dq.DataQuality = output_actions[0]
    assert isinstance(action, dq.DataQuality)
    assert action.table == "users"
    assert action.classifiers == classify_list
    assert action.operators == op_list


def test_example_publisher_with_filter():
    # -- Example 2 - Publisher (Adding a Filter to Example 1)
    #
    # It will produce the same `dq.summary_users` table as in Example 1
    # and it will drop all rows that didn't pass any of the checks
    #

    import tabsdata.dataquality as dq

    classify_list = [
        dq.IsNotNull(),
        dq.IsPositive("age"),
        dq.IsIn("gender", values={"M", "F", "O"}),
    ]
    op_list = [dq.Summary(), dq.Filter(AnyFailed())]

    @publisher(
        source=MySQLSource(
            uri=f"mysql://corp_server:3307/hr",
            query=[
                (
                    "SELECT * FROM users WHERE last_updated > :last_updated ORDER BY"
                    " last_updated"
                ),
                "SELECT * FROM offices",
            ],
            credentials=UserPasswordCredentials("root", "mysql"),
            initial_values={"last_updated": "2025/01/01T00:00:00.000Z"},
        ),
        tables=["users", "offices"],
        on_tables=dq.DataQuality(
            table="users",
            classifiers=classify_list,
            operators=op_list,
        ),
    )
    def pub2(users, offices):
        return users, offices

    assert isinstance(pub2, TabsdataFunction)
    pub2: TabsdataFunction = pub2
    output_actions = pub2.output_actions
    assert isinstance(output_actions, list)
    assert all(isinstance(action, OnTablesAction) for action in output_actions)
    assert len(output_actions) == 1
    action: dq.DataQuality = output_actions[0]
    assert isinstance(action, dq.DataQuality)
    assert action.table == "users"
    assert action.classifiers == classify_list
    assert action.operators == op_list


def test_example_publisher_with_filter_and_tags():
    # -- Example 3 - Publisher (Adding Tags to Example 2)
    #
    # It will produce the same `dq.summary_users` table as Example 1
    # It will drop all rows that fail the checks tagged with 'invalid_data'
    # It will filter out and sent to `users_with_incomplete_data` table all
    # rows failing any of the checks tagged with `missing_data`
    #

    import tabsdata.dataquality as dq

    classify_list = [
        dq.IsNull(column_names=["id"], tags=["missing_data"]),
        dq.IsPositive("age", tags="invalid_data"),
        dq.IsIn("gender", values={"M", "F", "O"}, tags="invalid_data"),
    ]
    op_list = [
        dq.Summary(),
        dq.Filter(AnyFailed(tags="invalid_data")),
        dq.Filter(
            AnyFailed(tags="missing_data"), to_table="users_with_incomplete_data"
        ),
    ]

    @publisher(
        source=MySQLSource(
            uri=f"mysql://corp_server:3307/hr",
            query=[
                (
                    "SELECT * FROM users WHERE last_updated > :last_updated ORDER BY"
                    " last_updated"
                ),
                "SELECT * FROM offices",
            ],
            credentials=UserPasswordCredentials("root", "mysql"),
            initial_values={"last_updated": "2025/01/01T00:00:00.000Z"},
        ),
        tables=["users", "offices"],
        on_tables=[
            dq.DataQuality(
                table="users",
                classifiers=classify_list,
                operators=op_list,
            )
        ],
    )
    def pub3(users, offices):
        return users, offices

    assert isinstance(pub3, TabsdataFunction)
    pub3: TabsdataFunction = pub3
    output_actions = pub3.output_actions
    assert isinstance(output_actions, list)
    assert all(isinstance(action, OnTablesAction) for action in output_actions)
    assert len(output_actions) == 1
    action: dq.DataQuality = output_actions[0]
    assert isinstance(action, dq.DataQuality)
    assert action.table == "users"
    assert action.classifiers == classify_list
    assert action.operators == op_list


@pytest.mark.skip("Un-skip this test when reverting TD-1089")
def test_example_transformer():
    import tabsdata.dataquality as dq

    continents = LinearScale(
        min=1,
        max=6,
        bins=6,
    )

    # Example 4 - Transformer
    #
    # Transformers allow interception on the input_tables (the tables the function
    # receives as parameters) and on the output_tables (the tables the function returns)
    #
    input_actions = [
        dq.DataQuality(
            table="employees",
            classifiers=[
                dq.IsNotNull(("zip", "zip_nn")),  # dq.zip_nn
                dq.IsNotNull("country"),
            ],
            operators=[
                dq.Summary(),
                dq.Filter(to_table="employees_invalid"),
            ],
        ),
        dq.DataQuality(
            table="employees",
            classifiers=[dq.ScaleCategorizer("country", scale=continents)],
            operators=[
                dq.Summary(),
                dq.Filter(to_table="employees_invalid"),
            ],
        ),
    ]
    output_actions = [
        dq.DataQuality(
            table="users_x",
            classifiers=[
                dq.IsNull("country"),
                dq.IsNull("id", tags="fail"),
                dq.ScaleCategorizer("country", scale=continents),
            ],
            operators=[
                dq.Summary(),
                dq.Filter(to_table="users_recheck", criteria=dq.Criteria.any_failed()),
                dq.Filter(
                    to_table="users_asia", criteria=dq.Criteria.in_bins(["asia"])
                ),
                dq.Fail(tags="fail", threshold=dq.RowCountThreshold(1)),
            ],
        )
    ]

    @transformer(
        input_tables=["employees", "campus_entries"],
        output_tables=["campus_attendance_per_zip"],
        on_input_tables=input_actions,
        on_output_tables=output_actions,
    )
    def user_tfr(users, offices):
        return users, offices

    assert user_tfr.input_actions == input_actions
    assert user_tfr.output_actions == output_actions
