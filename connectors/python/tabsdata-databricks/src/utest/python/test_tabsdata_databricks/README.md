<!--
Copyright 2025 Tabsdata Inc.
-->

# Test Setup for Databricks

## Account Level `Developer` Group Creation

From Databricks root account user management UI create an account level group `dev-group`.

Add all dev accounts to that group, make sure you add the CI user as well.

*NOTE:* Groups created from a SQL console re not account level groups, they are workspace level groups.
Yo cannot grant permissions to workspace level groups (I've been for a more than 2hrs in that rabbit hole).

## Catalog 'databricks-ci-catalog' Creation

Drop and recreate the catalog with a managed volume and grant the necessary 
permissions to the `dev-group` group:

```sql
DROP CATALOG IF EXISTS `databricks-ci-catalog` CASCADE;
CREATE CATALOG `databricks-ci-catalog`;
CREATE VOLUME `databricks-ci-catalog`.`default`.`test-volume`;

GRANT USE CATALOG ON CATALOG `databricks-ci-catalog` TO `dev-group`;
GRANT USE SCHEMA ON SCHEMA `databricks-ci-catalog`.`default` TO `dev-group`;
GRANT CREATE TABLE ON SCHEMA `databricks-ci-catalog`.`default` TO `dev-group`;
GRANT READ VOLUME ON VOLUME `databricks-ci-catalog`.`default`.`test-volume` TO `dev-group`;
GRANT WRITE VOLUME ON VOLUME `databricks-ci-catalog`.`default`.`test-volume` TO `dev-group`;
```

## Developer Programmatic Access Token (PAT) Creation

Each user (including the CI user) needs a PAT should login to the Databricks console, 
go to Settings (via top right menu from (<USER>), then Developer, then Access Tokens.
From there create a PAT.

## Environment Variables for Running Tests

Make sure to set the right values:

```bash
export DBX0__CATALOG=databricks-ci-catalog
export DBX0__HOST=https://<ACCOUNT_ID>.cloud.databricks.com
export DBX0__SCHEMA=default
export DBX0__TOKEN=<USER_PAT>
export DBX0__VOLUME=test-volume
export DBX0__WAREHOUSE_NAME=Serverless Starter Warehouse
```