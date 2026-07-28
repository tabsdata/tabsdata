<!--
Copyright 2026 Tabsdata Inc.
-->

# The `salesforce` model

`tdx-domain create-data` loads this model into a Salesforce org **automatically**
— it generates the dataset, wipes the target objects, and bulk-uploads the fresh
records. No manual `cci` / browser-OAuth steps.

The companion **`finance`** model joins back to the loaded Accounts via
`customers.salesforce_account_id`.

## How it works

1. **Generate** — the bundled [Snowfakery](https://snowfakery.readthedocs.io/)
   recipe `accounts_opps.recipe.yml` (75 Accounts × 1–4 Opportunities each,
   realistic distributions) is rendered to a local SQLite database.
2. **Wipe** — every record of each mapped object is deleted from the org
   (Opportunity, then Account).
3. **Upload** — the records are bulk-inserted (Account first, then Opportunity
   with its `AccountId` lookup resolved to the new Account Ids), driven by the
   CumulusCI load mapping `accounts_opps.mapping.yml`.

Auth is a non-interactive SOAP login (`simple-salesforce`) using the
`URL` / `USERNAME` / `PASSWORD` / `TOKEN` the salesforce **domain** carries —
no OAuth, no `cci` keychain. The load runs in the `_tdx-sys-b` uv venv
(`cumulusci-plus` + `snowfakery`), which the tooling creates and re-execs into
automatically; the driver venv is untouched.

## Usage

```bash
# 1. Credentials + salesforce system.
eval "$(tdx-system secrets --set 1p:dev_xp --out export)"
tdx-system create --type salesforce --sys sf

# 2. Create the domain (a credentials clone — no schema), record the model, and
#    wipe + upload the data (destructive — replaces all Account/Opportunity data):
tdx-domain create       --sys sf --domain sf_data
tdx-domain create-model --domain sf_data --model salesforce
tdx-domain create-data  --domain sf_data
```

`create-model` installs nothing — Account and Opportunity are standard objects
that already exist in the org — it only records the model on the descriptor. The
wipe happens as the first step of `create-data`, which takes no `--size`: the
record counts are fixed by the recipe.

## Assets in this directory

- `accounts_opps.recipe.yml` — Snowfakery generation recipe
- `accounts_opps.mapping.yml` — object/field/lookup load mapping (also drives
  the wipe order)
- `cumulusci.yml` — CumulusCI project metadata (for the optional manual `cci`
  workflow)

## Refreshing the `finance` snapshot

`finance` ships a frozen `accounts.csv` snapshot, so it needs no live org. To
refresh it from a freshly loaded org, query the Accounts back (e.g. with
`simple-salesforce` or `cci task run query`) and overwrite
`models/finance/accounts.csv` with `Id, Name, Industry, Type, AnnualRevenue,
NumberOfEmployees, BillingCity, BillingState, BillingPostalCode, BillingCountry,
Phone, Website, CreatedDate`.
