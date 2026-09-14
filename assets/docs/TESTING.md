<!--
Copyright 2025 Tabsdata Inc.
-->

# Tests Using External Resources

A test that needs an external resource runs only when that resource is available to it.
Otherwise it fails, unless it has been explicitly told to skip.

## Rust

A test reaches an external resource with `#[td_test::domain("<name>")]` or
`#[td_test::system("<name>")]` — a **tdx-provisioned backend**, read from the
descriptor store. Both compose with any test attribute, sync or async. See
[tdx Backends](#tdx-backends-domain--system).

*NOTE:* The Rust test-framework does not allow runtime test skipping, so a skipped test
is still invoked and prints `[td-test] SKIP: <test>`.

# tdx Backends (`domain` / `system`)

`tdx-system` / `tdx-domain` provision the external resources and record each one as an
encrypted descriptor in the store. The Rust fixtures read the very same descriptors the
Python `tabsdatax.itest.fixture` fixtures do, so a backend is provisioned once and both
languages consume it — no per-resource credentials in the environment.

Only two environment variables are involved:

* `TD_BACKENDS_CRYPTO_KEY`: the secret the descriptors are encrypted with (min 12 chars).
* `TD_BACKENDS_DIR`: the store root. Defaults to `~/.tabsdatax`; systems live under
  `systems2/<name>` and domains under `domains2/<name>`.

Descriptors are written in the `TDX3` format, which carries a per-descriptor salt. A
store still holding the older `TDX2` descriptors is **rejected** — every tdx command and
both languages' fixtures fail and tell you to convert it:

```sh
tdx-system upgrade    # converts every legacy descriptor in systems2/
tdx-domain upgrade    # ... and in domains2/
```

Both are idempotent and report what they changed. Nothing upgrades a descriptor as a side
effect of reading it — migrating a store is an explicit act, so it happens once, visibly.

A **system** is the resource itself (a bucket plus its staging root); a **domain** is one
container within it (a prefix), optionally seeded with a model and data.

**Prefer the domain**, whether the test reads seeded entities or writes its own scratch
data. Re-provisioning a domain clears its prefix, so what a run leaves behind is reclaimed;
objects written straight into a system's staging root accumulate there until someone runs
`tdx-system purge`. Reach for the system only when the test genuinely needs the resource
itself rather than a container in it.

The attribute's argument is the backend's **name in the store**; the parameter it fills is
that name prefixed with `domain_` / `system_`. So `#[td_test::domain("s3_world_xs")]` reads
the domain `s3_world_xs` and binds it to `domain_s3_world_xs` — the same naming as Python's
`@s3_domain("s3_world_xs")`. The fixture type comes from the parameter's own declared type
and is never repeated in the attribute.

```rust
    #[td_test::system("s3")]
    #[tokio::test]
    async fn writes_scratch(system_s3: S3System) {
        let store = system_s3.object_store().unwrap();
        let prefix = system_s3.run_prefix("my-area");  // <sys_path>/my-area/<uuid4>
    }

    #[td_test::domain("s3_world_xs")]
    #[test]
    fn reads_seeded_data(domain_s3_world_xs: S3Domain) {
        let url = domain_s3_world_xs.url().unwrap();   // s3://<bucket>/<base_path>
        let csvs = domain_s3_world_xs.staged(".csv");  // [(path, rows), ...]
    }
```

Put the attribute **above** the test attribute. Attributes expand top-down, and this one
has to remove the parameter before `#[test]` / `#[tokio::test]` sees a function it would
reject for taking arguments. Sync and async are treated alike — resolution is a file read
and a key derivation, so no runtime is involved.

Stack one attribute per backend, several of the same type included; binding is by
parameter name, so nothing is ambiguous:

```rust
    #[td_test::domain("s3")]
    #[td_test::domain("s3_world_xs")]
    #[td_test::system("s3")]
    #[tokio::test]
    async fn several(domain_s3: S3Domain, domain_s3_world_xs: S3Domain, system_s3: S3System) {
        ...
    }
```

The fixture struct's fields are read from the descriptor key of the same upper-cased name,
so `S3Domain::bucket` comes from `BUCKET`. Nothing is created or torn down by the fixture:
`tdx-domain destroy` / `tdx-system purge` own the lifecycle, and a test that writes should
carve out its own corner with `run_prefix`.

## Available Fixture Structs

These are ported so far; the rest stay Python-only until a Rust test needs them. All of
them live in `td_test::domain`:

| tdx type | System | Domain |
|---|---|---|
| `s3` | `S3System` | `S3Domain` |
| `gcs` | `GcsSystem` | `GcsDomain` |
| `azureblob` | `AzureblobSystem` | `AzureblobDomain` |
| `fs` | `FsSystem` | `FsDomain` |
| `postgres` | `PostgresSystem` | `PostgresDomain` |

The three object-store ones share a shape: `options()` is the `object_store` config,
`url()` / `child_url()` locate the prefix, and `object_store()` opens a store **rooted at
the bucket or container**. So the paths that address it are `base_path_prefix()`,
`run_prefix()`, and `child_prefix()` — the last is how a relative path from `staged()`
becomes a key:

```rust
    let store = domain_s3_world_xs.object_store().unwrap();
    let (path, rows) = domain_s3_world_xs.staged(".csv")[0];
    let bytes = store.get(&domain_s3_world_xs.child_prefix(path).into()).await;
```

`GcsDomain` / `GcsSystem` also carry the S3-interop HMAC credentials, as `s3_options()`,
for connectors that reach GCS over the S3 protocol.

`fs` is a local directory with nothing above it, so it deals in `PathBuf`s instead —
`path()`, `child_path()`, `run_path()` — and its `object_store()` is rooted at the
fixture's own directory, where a `staged()` path addresses the store as-is.

`postgres` is a SQL backend, so it has no store at all: a system is the container, a domain
is one database in it owned by a role of the same name. `url()` is the connection —
`postgres://<user>:<password>@<host>:<port>/<db>`, the libpq form sqlx and tokio-postgres
take. Use it rather than the descriptor's `sqlalchemy_uri` field, which carries the
`+psycopg2` dialect prefix only SQLAlchemy understands, or `uri`, which has no credentials.
`PostgresSystem::admin_url()` connects as the superuser to the maintenance database, the
only place `CREATE` / `DROP DATABASE` can be issued from. The seeded tables and their row
counts are `tables()` and `rows(table)`, the SQL counterpart of `staged()`.

Python names the same things the same way, so a test reads alike in either language:

```python
    @s3_domain("s3_world_xs")
    def test_reads_seeded_data(domain_s3_world_xs: S3Domain):
        ...
```

## Resolution Order

A backend is looked for in three places, in this order, identically in Rust and Python:

1. **The cache** — per `(descriptor path, fixture type)`. Python caches for the pytest
   session; Rust caches for the process, and the Rust harness runs a process per test, so
   it buys much less there.
2. **`<UPCASE_NAME>__<KEY>` environment variables.** A *complete* set is the whole
   backend: neither the descriptor nor `TD_BACKENDS_CRYPTO_KEY` is touched. It is the
   shape `tdx-domain info` emits, so exporting one is one command:

   ```sh
   eval "$(tdx-domain info --domain apiserver_postgres \
             --format export --prefix APISERVER_POSTGRES)"
   ```

   A whole manifest exports in one go, each domain keyed on its own upper-cased name,
   which is what `cargo make test_rs` does for the Rust domains:

   ```sh
   eval "$(tdx-domain info --manifest variant/config/pytest/domains_rs.yaml \
             --format export)"
   ```

   `__ENTITIES` is read as a JSON object (`{"city": 4079}`), so a seeded domain's row
   counts survive the round trip and `staged()` / `tables()` keep working. `NAME` is
   filled from the requested name.
3. **The descriptor**, which stays the source of truth.

The environment tier exists for cost: opening a descriptor runs a 200k-round PBKDF2 key
derivation (~350ms on an M-series Mac). Python pays that once per session, but the Rust
harness runs a **process per test**, so it would pay it once per test — around 40s across
the Rust suite.

An **incomplete** prefix is not an error. It prints
`[td-test] WARNING: $<NAME>__* is set but does not describe a whole ...` on stderr, keeps
**none** of the environment's values, and reads the descriptor instead — the two sources
are never mixed. Falling through rather than failing is what makes a collision harmless:
`<NAME>__<KEY>` is also the shape tdx's own create-time options use, so a system named
`s3` shares a prefix with the `S3__BUCKET` / `S3__REGION` variables `tdx-system create`
reads.

*WARNING:* **a stale export outranks a fresh descriptor.** `tdx-domain create` mints a new
password every time, so an environment exported before a re-provision still builds a
complete — and wrong — backend, and every test then fails against a database that no longer
exists. Re-export after provisioning, or unset the prefix.

## Skipping when a Backend is Absent

Independently per family, so systems and domains are governed separately:

* `TD_TEST_ON_MISSING_SYSTEM` / `TD_TEST_ON_MISSING_DOMAIN` = `skip` | `fail` (default
  `fail`) when the backend cannot be reached — either the descriptor is not in the store,
  or `TD_BACKENDS_CRYPTO_KEY` is unset so it cannot be opened.
* `TD_TEST_SKIP_SYSTEMS` / `TD_TEST_SKIP_DOMAINS` = comma-separated backend names to skip
  even when they are present.

A backend that *is* reachable but unusable — wrong type, a descriptor missing a key the
fixture needs, or a legacy `TDX2` descriptor — always fails, whatever the policy says.
Those are defects to fix, not absences to skip.

CI provisions the backends and leaves these unset, so a missing one is a failure. Developers
without a tdx setup can set `TD_TEST_ON_MISSING_SYSTEM=skip` and
`TD_TEST_ON_MISSING_DOMAIN=skip` locally to run the rest of the suite.

# Db-backed Tests (`#[td_test::test(db)]`)

Tests of apiserver logic take a database rather than an external resource, and they get it
from a tdx domain too — `apiserver_postgres`. **Nothing in the test code starts a database
server.** Each test creates its own uniquely-named schema in that domain's database,
brings it to the current version, and drops it on teardown, so tests still provision in
parallel and isolate completely:

```rust
    #[td_test::test(db)]
    async fn reads_its_own_schema(db: TdPool) {
        ...
    }
```

`cargo make test_rs` provisions the domains itself, from a manifest of its own rather than
`domains_py.yaml` — a Rust test run has no business provisioning the domains of the other two
dozen systems, and nothing in it declares an env-sourced option, so the run needs no
1Password load, only `TD_BACKENDS_CRYPTO_KEY`. That manifest,
`variant/config/pytest/domains_rs.yaml`, holds `apiserver_postgres` for the db tests and
`apiserver_s3` / `apiserver_az` / `apiserver_gcs` for the object-store ones — named apart
from the `s3` / `azureblob` / `gcs` the Python suite reads, because the manifest recreates
what it names and a Rust run must not clear a domain another suite is reading.

The domains are recreated with `--force` on every run, so the db tests start against an
empty database, no per-test schema outlives the run that made it, and the objects the
object-store tests write are reclaimed rather than accumulating under a system's staging
root. All four are **required**: a store missing one fails the provisioning task instead of
failing every test that needed it. **Systems** are not this task's business: they are
long-running and expected to be up, so `tdx-system create` / `restart` stay a separate,
deliberate act.

`cargo make test_rs_backends` runs just that provisioning, for when you want the domains
without the suite. `--no-domains` (or `--light`) skips it, for a run against a domain you
provisioned yourself. Point the tests at a different one with `TD_TEST_DB_DOMAIN=<name>`.

Every domain task takes a mandatory `--python` or `--rust` to say which suite's domains it
means: `cargo make domains_create --rust`, `cargo make domains_destroy --python`. The
descriptors are named to match, `domains_py.yaml` and `domains_rs.yaml`, with the GA Python
variants as `domains_py_ga_<os>.yaml`.

The domain descriptor carries the owning system's superuser as `SYS_ADMIN_USER` /
`SYS_ADMIN_PASSWORD`, which is what the per-test path connects as: creating and dropping
schemas and ensuring the cluster-global RLS role are beyond the domain's own owner role.

The same `TD_TEST_ON_MISSING_DOMAIN` policy applies, so a developer without a tdx setup can
set it to `skip`. td-database's own internals suite (`components/tests/*`,
`pool::connect`) tests the database layer itself rather than apiserver logic, so it needs
a superuser: it creates and drops cluster-global roles and sets `BYPASSRLS`. It takes that
from the `apiserver_postgres_internals` domain, whose descriptor carries the owning system's
superuser. Nothing starts a server for it, so it fails rather than skips when that domain's
environment tier is absent.
