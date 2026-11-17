<!--
    Copyright 2026 Tabsdata Inc.
-->

# AWS user setup for the AWS Glue connector

The connector authenticates with an access key pair, so it needs an IAM user
with one policy and no roles. The policy below covers the Glue Data Catalog
calls and the S3 calls the connector makes.

## Connector user

Set the account, the region, the bucket and the prefix under the bucket where
the tables are stored (the `base_path` of `AwsGlueDestConn`, without leading or
trailing slashes).

```sh
ACCOUNT=123456789012
REGION=us-east-1
BUCKET=my-bucket
BASE=tabsdata
IAM_USER=tabsdata-awsglue

cat > awsglue-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "GlueCatalog",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase", "glue:GetDatabases",
        "glue:GetTable", "glue:GetTables",
        "glue:CreateTable", "glue:UpdateTable"
      ],
      "Resource": [
        "arn:aws:glue:${REGION}:${ACCOUNT}:catalog",
        "arn:aws:glue:${REGION}:${ACCOUNT}:database/*",
        "arn:aws:glue:${REGION}:${ACCOUNT}:table/*/*"
      ]
    },
    {
      "Sid": "BucketList",
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"],
      "Resource": "arn:aws:s3:::${BUCKET}"
    },
    {
      "Sid": "Objects",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
        "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"
      ],
      "Resource": "arn:aws:s3:::${BUCKET}/${BASE}/*"
    }
  ]
}
EOF

aws iam create-user --user-name "$IAM_USER"
aws iam create-policy --policy-name tabsdata-awsglue --policy-document file://awsglue-policy.json
aws iam attach-user-policy --user-name "$IAM_USER" \
  --policy-arn "arn:aws:iam::${ACCOUNT}:policy/tabsdata-awsglue"
aws iam create-access-key --user-name "$IAM_USER"
```

`create-access-key` prints the secret access key once. The pair is the
`AwsAccessSecretKey` of the connection.

The connector never creates a database: it fails when the database of a table
does not exist. Create the databases beforehand, with a principal that has
`glue:CreateDatabase`.

To restrict the user to one database, replace `database/*` with
`database/<database>` and `table/*/*` with `table/<database>/*`.

## Integration tests

The test system creates one database per test domain and drops it, with its
tables, afterwards. Its user needs this statement in addition to the ones
above:

```json
{
  "Sid": "GlueTestAdmin",
  "Effect": "Allow",
  "Action": [
    "glue:CreateDatabase", "glue:DeleteDatabase",
    "glue:DeleteTable", "glue:BatchDeleteTable"
  ],
  "Resource": [
    "arn:aws:glue:${REGION}:${ACCOUNT}:catalog",
    "arn:aws:glue:${REGION}:${ACCOUNT}:database/*",
    "arn:aws:glue:${REGION}:${ACCOUNT}:table/*/*"
  ]
}
```

The database names are generated, so the Glue resources stay wide here.

## Lake Formation

When Lake Formation enforces permissions in the account, the IAM policy alone
is not enough and the Glue calls fail with `AccessDeniedException`. Check the
settings:

```sh
aws lakeformation get-data-lake-settings --region "$REGION"
```

If `CreateDatabaseDefaultPermissions` does not list `IAM_ALLOWED_PRINCIPALS`,
the user also needs Lake Formation grants on the databases and on the S3
location.

## Bucket encryption

If the bucket is encrypted with a customer-managed KMS key, the user also needs
`kms:GenerateDataKey` and `kms:Decrypt` on that key.

## Querying from Amazon Athena

The tables the connector registers can be queried from Amazon Athena. Whoever
runs the queries needs a separate policy; `RESULTS_BUCKET` is where the Athena
workgroup writes its results.

```sh
RESULTS_BUCKET=my-athena-results

cat > athena-query-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution", "athena:StopQueryExecution",
        "athena:GetQueryExecution", "athena:GetQueryResults", "athena:GetWorkGroup"
      ],
      "Resource": "arn:aws:athena:${REGION}:${ACCOUNT}:workgroup/primary"
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase", "glue:GetDatabases",
        "glue:GetTable", "glue:GetTables", "glue:GetPartitions"
      ],
      "Resource": [
        "arn:aws:glue:${REGION}:${ACCOUNT}:catalog",
        "arn:aws:glue:${REGION}:${ACCOUNT}:database/*",
        "arn:aws:glue:${REGION}:${ACCOUNT}:table/*/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": ["arn:aws:s3:::${BUCKET}", "arn:aws:s3:::${BUCKET}/${BASE}/*"]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject", "s3:PutObject", "s3:ListBucket",
        "s3:GetBucketLocation", "s3:AbortMultipartUpload"
      ],
      "Resource": ["arn:aws:s3:::${RESULTS_BUCKET}", "arn:aws:s3:::${RESULTS_BUCKET}/*"]
    }
  ]
}
EOF

aws iam create-policy --policy-name tabsdata-awsglue-athena --policy-document file://athena-query-policy.json
```
