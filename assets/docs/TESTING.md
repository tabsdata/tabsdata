<!--
Copyright 2025 Tabs Data Inc.
-->

# Tests Using External Resources

Tests using external resources are activated by setting environment variables 
providing the details on how to access the corresponding external resource.

These tests will run only when the requirements for the test are met. 

Otherwise the tests will fail.

## Forcing a Skip for Tests with Requirements Not Met

If the environment variable `TD_TEST_SKIP_IF_NO_REQS` is set to `true`, then tests with missing requirements 
will be skipped.

CI builds should not set this variable to ensure tests are not skipped.

Developers can set this variable to `true` in their local environment to skip the tests if requirements are not met.

## Rust

The `#[td_test::test]` macro works only with tests using the `#[tokio::test]` macro (async tests).

Use the `#[td_test::test(when(reqs = ..., env_prefix = ...))]` macro to define a test that uses 
external resources.

*IMPORTANT:* The `#[td_test::test]` macro must be specified before the `#[tokio::test]` macro.

*NOTE:* The Rust test-framework does not allow runtime test skipping, the tests
will be invoked but will print a message that the test was skipped).

# External Resource Types

Test using external resources use a fixture in Python and a special test macro in Rust.

Each external resource type defines a set of environment variables that must be set for the test
to run. All the variables for an external resource have a common prefix that is specified in the 
Python fixture or Rust macro.

## AWS S3 with Access Key and Secret Key Credentials

* `<PREFIX>__S3_URI`: The URI of the S3 bucket.
* `<PREFIX>__S3_REGION`: The region of the S3 bucket.
* `<PREFIX>__S3_ACCESS_KEY`: The access key for the S3 bucket.
* `<PREFIX>__S3_SECRET_KEY`: The secret key for the S3 bucket.

Rust:
```rust
    #[td_test::test(when(reqs = S3WithAccessKeySecretKeyReqs, env_prefix= "MY_S3"))]
    #[tokio::test]
    async fn my_test(s3: S3WithAccessKeySecretKeyReqs) {
        ...
    }
```

Python:
```python
TBD
```

## Azure storage with Account Name and Account Key

* `<PREFIX>__AZ_URI`: The URI of the Azure container.
* `<PREFIX>__AZ_ACCOUNT_NAME`: The account name for the Azure storage account.
* `<PREFIX>__AZ_ACCOUNT_KEY`: The account key for the Azure storage account.

Rust:
```rust
    #[td_test::test(when(reqs = AzureStorageWithAccountKeyReqs, env_prefix= "MY_AZ"))]
    #[tokio::test]
    async fn my_test(az: AzureStorageWithAccountKeyReqs) {
        ...
    }
```

Python:
```python
TBD
```

## Google Cloud Storage with Service Account Credentials

* `<PREFIX>__GCP_URI`: The URI of the GCP Storage bucket.
* `<PREFIX>__GCP_SERVICE_ACCOUNT_KEY`: The service account key, JSON blob.

Rust:
```rust
    #[td_test::test(when(reqs = GcpStorageWithServiceAccountKeyReqs, env_prefix= "MY_GCP"))]
    #[tokio::test]
    async fn my_test(gcp: GcpStorageWithServiceAccountKeyReqs) {
        ...
    }
```

Python:
```python
TBD
```

## MySQL

* `<PREFIX>_MYSQL_URI`: The URI of the MySQL database.
* `<PREFIX>_MYSQL_USER`: The user for the MySQL database.
* `<PREFIX>_MYSQL_PASSWORD`: The password for the MySQL database.

Rust:
```rust
    #[crate::test(when(reqs = MySqlReqs, env_prefix= "MY_MYSQL"))]
    #[tokio::test]
    async fn my_test(mysql: MySqlReqs) {
        ...
    }
```

Python:
```python
TBD
```

## Oracle

* `<PREFIX>_ORACLE_URI`: The URI of the Oracle database.
* `<PREFIX>_ORACLE_USER`: The user for the Oracle database.
* `<PREFIX>_ORACLE_PASSWORD`: The password for the Oracle database.

Rust:
```rust
    #[crate::test(when(reqs = OracleReqs, env_prefix= "MY_ORACLE"))]
    #[tokio::test]
    async fn my_test(oracle: OracleReqs) {
        ...
    }
```

Python:
```python
TBD
```

## PostgreSQL

* `<PREFIX>_POSTGRES_URI`: The URI of the PostgreSQL database.
* `<PREFIX>_POSTGRES_USER`: The user for the PostgreSQL database.
* `<PREFIX>_POSTGRES_PASSWORD`: The password for the PostgreSQL database.

Rust:
```rust
    #[td_test::test(when(reqs = PostgresReqs, env_prefix= "MY_POSTGRES"))]
    #[tokio::test]
    async fn my_test(postgres: PostgresReqs) {
        ...
    }
```

Python:
```python
TBD
```

## MongoDB

* `<PREFIX>_MONGO_URI`: The URI of the MongoDB database.
* `<PREFIX>_MONGO_USER`: The user for the MongoDB database.
* `<PREFIX>_MONGO_PASSWORD`: The password for the MongoDB database.

Python:
```python
TBD
```

## Salesforce

* `<PREFIX>_SALESFORCE_URI`: The URI of the Salesforce instance.
* `<PREFIX>_SALESFORCE_USER`: The user for the Salesforce instance.
* `<PREFIX>_SALESFORCE_PASSWORD`: The password for the Salesforce instance.
* `<PREFIX>_SALESFORCE_TOKEN`: The token for the Salesforce instance.

Python:
```python
TBD
```

## Cassandra

* `<PREFIX>_CASSANDRA_HOST_POST_LIST`: A list of <HOST>:<PORTS> comma separated.
* `<PREFIX>_CASSANDRA_USER`: The user for the Cassandra database.
* `<PREFIX>_CASSANDRA_PASSWORD`: The password for the Cassandra database.

Python:
```python
TBD
```
