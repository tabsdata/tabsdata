#
# Copyright 2025 Tabs Data Inc.
#

import tabsdata as td


def testing_tabsdata_function(uri: str, credentials=None, destination_table=None):
    # In this example, we are obtaining the data from the file data.csv and then dropping
    # the null values. The output is saved in output.json, and expected_result.json
    # contains the expected output of applying the function to the input data.
    uri = uri.replace("mysql://", "mysql+wrongdriver://")

    @td.subscriber(
        name="output_sql_wrong_driver_fails",
        tables="collection/table",
        destination=td.MySQLDestination(
            uri,
            destination_table,
            credentials=credentials,
        ),
    )
    def output_sql_wrong_driver_fails(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df, new_df

    return output_sql_wrong_driver_fails


if __name__ == "__main__":
    os.makedirs(DEFAULT_SAVE_LOCATION, exist_ok=True)
    create_bundle_archive(
        output_sql_wrong_driver_fails,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=DEFAULT_SAVE_LOCATION,
    )
