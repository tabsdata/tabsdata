#
# Copyright 2026 Tabsdata Inc.
#

import tabsdata as td

catalog_definition = {
    "name": "default",
    "type": "glue",
    "client.access-key-id": td.EnvironmentSecret("TRANSPORTER_AWS_ACCESS_KEY_ID"),
    "client.secret-access-key": td.EnvironmentSecret(
        "TRANSPORTER_AWS_SECRET_ACCESS_KEY"
    ),
    "client.region": "us-east-1",
}

# catalog = td.AWSGlue(
#     definition=catalog_definition,
#     tables=[
#         "testing_namespace.test_output_s3_catalog_first",
#         "testing_namespace.test_output_s3_catalog_second",
#     ],
# )


s3_credentials = td.S3AccessKeyCredentials(
    td.EnvironmentSecret("TRANSPORTER_AWS_ACCESS_KEY_ID"),
    td.EnvironmentSecret("TRANSPORTER_AWS_SECRET_ACCESS_KEY"),
)


def testing_tabsdata_function(
    bucket_uri: str,
    path: list[str],
    credentials=None,
    region: str = None,
    format=None,
    catalog=None,
):
    @td.subscriber(
        name="output_new_s3_catalog",
        tables="collection/table",
        destination=td.S3Dest(
            conn=td.S3Conn(
                bucket_uri=bucket_uri,
                credentials=credentials,
                region=region,
            ),
            path=path,
            catalog=catalog,
            format=format,
        ),
    )
    def output_new_s3_catalog(df: td.TableFrame):
        new_df = df.drop_nulls()
        return new_df, new_df

    return output_new_s3_catalog
