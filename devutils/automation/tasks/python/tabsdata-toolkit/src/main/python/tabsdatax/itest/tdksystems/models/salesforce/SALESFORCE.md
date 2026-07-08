# The `salesforce` model

`tdx-domain create --model salesforce --data <SIZE>` loads this model into a
Salesforce org **automatically** — it generates the dataset, wipes the target
objects, and bulk-uploads the fresh records. No manual `cci` / browser-OAuth
steps.

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
no OAuth, no `cci` keychain. The load runs in the `.tdz` uv venv
(`cumulusci-plus` + `snowfakery`), which the tooling creates and re-execs into
automatically; the `main` venv is untouched.

## Usage

```bash
# 1. Credentials + salesforce system (--env-prefix defaults to SALESFORCE).
eval "$(tdx-system secrets --get 1p:dev_xp --out export)"
tdx-system create --type salesforce --sys ./sys.salesforce.json --name sf

# 2. Create the domain (creds clone — no schema) and wipe + upload the data
#    (destructive — replaces all Account/Opportunity data):
tdx-domain create --sys ./sys.salesforce.json --domain ./dom.salesforce.json \
    --name sf_data --model salesforce --data xs
```

`--model salesforce` installs no schema (the objects already exist in the org);
the wipe happens as the first step of the data load (`--data`).

## Assets in this directory

- `accounts_opps.recipe.yml` — Snowfakery generation recipe
- `accounts_opps.mapping.yml` — object/field/lookup load mapping (also drives
  the wipe order)
- `cumulusci.yml` — CumulusCI project metadata (for the optional manual `cci`
  workflow)

## Refreshing the `finance` snapshot

`finance` ships a frozen `Account.sf.csv` snapshot, so it needs no live org.
To refresh it from a freshly loaded org, query the Accounts back (e.g. with
`simple-salesforce` or `cci task run query`) and overwrite
`models/finance/accounts.csv` with `Id, Name, Industry, Type, AnnualRevenue,
NumberOfEmployees, BillingCity, BillingState, BillingPostalCode,
BillingCountry, Phone, Website, CreatedDate`.

## Troubleshooting

- **`INVALID_LOGIN` / auth fails** — check `URL` (must include scheme; use the
  My Domain or `https://login.salesforce.com`), `USERNAME`, `PASSWORD`, and a
  current `TOKEN` (Settings → Reset My Security Token). For a sandbox the URL
  host should be `test.salesforce.com`.
- **`REQUIRED_FIELD_MISSING` during upload** — your org has a required custom
  field; add it to both `accounts_opps.recipe.yml` and
  `accounts_opps.mapping.yml`, or make it optional in org setup.
- **`Probability` / `ForecastCategory` look off** — Salesforce auto-derives
  these from `StageName` per org config; expected.
