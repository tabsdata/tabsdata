<!--
Copyright 2025 Tabs Data Inc.
-->

# Creating a Databricks User

Ask your Databricks account administrator to create a user with your email.

## Databricks Administrator, User Creation

Login at https://login.databricks.com/.

The Databricks administrator user must login to the Databricks account, 
from the *(D)* user icon on the top right go to: 

  *Settings* > *Workspace admin* > *Identity and access* > *Users* 
  
Then go to *\[Manage]*, *\[Add user]*, *\[Add new]*  and enter the new user 
*<NAME>@<DOMAIN>* email address.,

## Creating a Databricks Catalog for the User

Create a `<NAME>-catalog` catalog and configure it so the *<NAME>@<DOMAIN>* 
user is the owner.

# User Login into the Databricks Console

*NOTE:* The following instructions are for the created user.

Login at https://login.databricks.com/.

You'll receive an email with a one-time password to login.

## Creating an Access Token a (PAT)

After login, from the *(D)* user icon on the top right got to:

  *Settings* > *Developer* > *\[Generate new token]*, give it a name and
  an expiration (maximum 730 days). Copy and save the token, as it will 
  not be shown again.

## Create a Managed Volume

Go to the `SQL Editor` and run the following SQL commands to grant the necessary permissions
in the `<NAME>-catalog` catalog:

```sql
GRANT USE SCHEMA ON SCHEMA default TO `<NAME>@<DOMAIN>`;
GRANT CREATE VOLUME ON SCHEMA default TO `<NAME>@<DOMAIN>`;
CREATE VOLUME `test-volume`;
```
## Grant Table Permissions

```sql
GRANT CREATE TABLE ON SCHEMA default TO `<NAME>@<DOMAIN>`;
```

