<!--
Copyright 2025 Tabs Data Inc.
-->

# Seeding Data for Salesforce Tests

Set the following environment variables:
    * SF0__USERNAME
    * SF0__PASSWORD
    * SF0__SECURITY_TOKEN
    * SF0__INSTANCE_URL

Run the `data-setup.py` script.The script will first delete previously seeded Contact data and then
it will upload the Contact data from the `data.csv` file (5000 contacts).

*Note:* the Salesforce Developer account has some predefined contacts that cannot be deleted.

### How the Salesforce Developer Edition Account Was Created

In case there is a need to recreate the Salesforce Developer Edition account, follow these steps:

* Go to https://developer.salesforce.com/
* Loging to the developer account
* To create the API token: Switch to the Lightning Experience console then go to user’s (avatar top right) 
Settings, then on the left vertical navigation go to `My Personal Information` => `Reset My Security Token`. 
The new token will be sent to the email associated with the developer account.

### Giving Bulk Hard Delete permissions to the Salesforce User

* Create a permission set with the "Bulk API Hard Delete" permission:
`Navigate to Setup` => `Permission Sets`
* Create a new Permission Set:
  * Click "New"
  * Enter a Label (e.g., "Hard Delete Access")
  * Enter an API Name
  * Save
* Add the Hard Delete Permission:
  * Open your new Permission Set
  * Click "System Permissions"
  * Find and check "Bulk API Hard Delete" Permission Allow Sys.
  * Save
* Assign the Permission Set:
  * Go to the Permission Set detail page
  * Click "Manage Assignments"
  * Click "Add Assignments"
  * Select the user(s) you want to grant this permission to
  * Click "Assign"