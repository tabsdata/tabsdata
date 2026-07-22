<!--
Copyright 2025 Tabsdata Inc.
-->

# Test Setup for Salesforce

Use a Salesforce developer account.

Every time you reset the Salesforce account password you need to reset
the Security Token as well.

## Resetting the Security Token

Login to the Salesforce account, got to <ACCOUNT NAME> on the top right,
go to `My Settings`, then `Reset My Security Token`.

You'll get an email (to the email associated to the account) with the new
security token.

## Environment Variables for Running Tests

Make sure to set the right values:

```bash
SF0__INSTANCE_URL=<ACCOUNT URL>
SF0__USERNAME=<USERNAME>
SF0__PASSWORD=<PASSWORD>
SF0__SECURITY_TOKEN=<SECURITY_TOKEN>
```