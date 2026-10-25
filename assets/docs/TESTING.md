<!--
Copyright 2025 Tabsdata Inc.
-->

# Tests Using External Resources

A test that needs an external resource runs only when that resource is available to it.
Otherwise it fails, unless it has been explicitly told to skip.

## Rust

A test reaches an external resource with `#[td_test::domain("<name>")]` or
`#[td_test::system("<name>")]` — a **tdd-provisioned backend**, read from the
descriptor store through tdd. Both compose with any test attribute, sync or async. See
[tdd Backends](#tdd-backends-domain--system).

*NOTE:* The Rust test-framework does not allow runtime test skipping, so a skipped test
is still invoked and prints `[td-test] SKIP: <test>`.

# tdd Backends (`domain` / `system`)

`tdd system` / `tdd domain` (tabsdata-dt) provision the external resources and record each
one as an encrypted descriptor in tdd's store. Only tdd reads or writes that store: the
Rust fixtures and the Python `tabsdatax.itest.fixture` fixtures both ask tdd for a backend
(`tdd ... info --format json`), so a backend is provisioned once and both languages consume
it — no per-resource credentials in the environment, and no copy of the descriptor format
outside tdd.

The environment variables involved:

* `_TDD_CRYPTO_KEY`: the secret the descriptors are encrypted with (min 12 chars); load it
  with `tdd env set --crypto`.
* `TDD_DIR`: where tdd is installed, `~/.tdd` by default. The fixtures run its `tdd-exe`.
* `TDD_SYSTEM_DIR` / `TDD_DOMAIN_DIR`: another store to use instead of tdd's own
  (`<tdd_dir>/data/system` and `<tdd_dir>/data/domain`) — how the fixtures' own tests use
  a scratch store.

A store still holding legacy `TDX2` descriptors is **rejected** — tdd fails, and both
languages' fixtures fail with its message. Convert it once, explicitly:

```sh
tdd system upgrade
tdd domain upgrade
```

A **system** is the resource itself (a bucket plus its staging root); a **domain** is one
container within it (a prefix), optionally seeded with a model and data.

**Prefer the domain**, whether the test reads seeded entities or writes its own scratch
data. Re-provisioning a domain clears its prefix, so what a run leaves behind is reclaimed;
objects written straight into a system's staging root accumulate there until someone runs
`tdd system purge`. Reach for the system only when the test genuinely needs the resource
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
reject for taking arguments. Sync and async are treated alike — resolution is one blocking
tdd process, so no runtime is involved.

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
`tdd domain destroy` / `tdd system purge` own the lifecycle, and a test that writes should
carve out its own corner with `run_prefix`.

## Available Fixture Structs

These are ported so far; the rest stay Python-only until a Rust test needs them. All of
them live in `td_test::domain`:

| tdd type | System | Domain |
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

`postgres` is a SQL backend, so it has no store at all: a system is the container
(`docker_container_name`), a domain is one database in it owned by a role of the same name.
The postgres family names the network it is reached on, and tests reach it from the host, so
the fields are the external ones: `external_host`, `external_uri`,
`external_sqlalchemy_uri`, with the database as `database` (== `schema`). `url()` is the
connection — `postgres://<user>:<password>@<external_host>:<port>/<database>`, the libpq form
sqlx and tokio-postgres take. Use it rather than `external_sqlalchemy_uri`, which carries the
`+psycopg2` dialect prefix only SQLAlchemy understands, or `external_uri`, which has no
credentials.
`PostgresSystem::admin_url()` connects as the superuser to the maintenance database, the
only place `CREATE` / `DROP DATABASE` can be issued from. The seeded tables and their row
counts are `tables()` and `rows(table)`, the SQL counterpart of `staged()`.

Python names the same things the same way, so a test reads alike in either language:

```python
@s3_domain("s3_world_xs")
def test_reads_seeded_data(domain_s3_world_xs: S3Domain): ...
```

### Local twins of the object stores

`s3local`, `gcslocal` and `azurebloblocal` are the cloud object stores run as containers on
this machine (served by RustFS, cloudburrow and Azurite; tdd names them after the store they
stand in for, not the program behind it). They need no credentials, so they also run on CI.
Their descriptors, like every object store's, carry `STORAGE_OPTIONS`: the polars / obstore
`storage_options` that reach the store, built by the same tdd code for a cloud store and its
twin. So far they are Python-only (`S3LocalDomain`, `GcsLocalDomain`, `AzureBlobLocalDomain`
and their systems); every object-store fixture has `uri` and `storage_options`, so one line
reads any of the six, and a test moves between a store and its twin by its fixture alone:

```python
@s3local_domain("s3local_world_xs")
def test_scans(domain_s3local_world_xs: S3LocalDomain):
    domain = domain_s3local_world_xs
    lf = pl.scan_parquet(f"{domain.uri}/parquet/*.parquet",
                         storage_options=domain.storage_options)
```

## Resolution Order

A backend is looked for in three places, in this order, identically in Rust and Python:

1. **The cache** — per `(family, store, name, fixture type)`. Python caches for the pytest
   session; Rust caches for the process, and the Rust harness runs a process per test, so
   it buys much less there.
2. **`<UPCASE_NAME>__<KEY>` environment variables.** A *complete* set is the whole
   backend: neither tdd nor `_TDD_CRYPTO_KEY` is touched. It is the shape
   `tdd domain info` emits, so exporting one is one command:

   ```sh
   eval "$(tdd domain info --domain apiserver_postgres \
             --format export --prefix APISERVER_POSTGRES)"
   ```

   A whole manifest exports in one go, each domain keyed on its own upper-cased name,
   which is what `cargo make test_rs` does for the Rust domains — handing the values to
   the tests in memory, never through a file, since they hold the backends' passwords:

   ```sh
   eval "$(tdd domain info --manifest variant/config/pytest/domains_rs.yaml \
             --format export)"
   ```

   `__ENTITIES` is read as a JSON object (`{"city": 4079}`), so a seeded domain's row
   counts survive the round trip and `staged()` / `tables()` keep working. `NAME` is
   filled from the requested name.
3. **The descriptor**, through tdd, which stays the source of truth.

The environment tier exists for cost: asking tdd starts a process (a few hundred
milliseconds, decryption included). Python pays that once per backend per session, but the
Rust harness runs a **process per test**, so it would pay it once per test.

An **incomplete** prefix is not an error. It prints
`[td-test] WARNING: $<NAME>__* is set but does not describe a whole ...` on stderr, keeps
**none** of the environment's values, and reads the descriptor instead — the two sources
are never mixed. Falling through rather than failing is what makes a collision harmless:
`<NAME>__<KEY>` is also the shape tdd's own create-time options use, so a system named
`s3` shares a prefix with the `S3__BUCKET` / `S3__REGION` variables `tdd system create`
reads.

*WARNING:* **a stale export outranks a fresh descriptor.** `tdd domain create` mints a new
password every time, so an environment exported before a re-provision still builds a
complete — and wrong — backend, and every test then fails against a database that no longer
exists. Re-export after provisioning, or unset the prefix.

## Skipping when a Backend is Absent

Independently per family, so systems and domains are governed separately:

* `TD_TEST_ON_MISSING_SYSTEM` / `TD_TEST_ON_MISSING_DOMAIN` = `skip` | `fail` (default
  `fail`) when the backend cannot be reached — it is not in the tdd store, or, in Rust, tdd
  is not installed or `_TDD_CRYPTO_KEY` is unset. Python fails outright on those two:
  its fixtures are generated from tdd's store listing, so the tests that declare a backend
  fail with tdd's message, and the rest of the run is untouched.
* `TD_TEST_SKIP_SYSTEMS` / `TD_TEST_SKIP_DOMAINS` = comma-separated backend names to skip
  even when they are present.

A backend that *is* reachable but unusable — wrong type, a descriptor missing a key the
fixture needs, or one tdd cannot read (a legacy `TDX2` store) — always fails, whatever the
policy says. Those are defects to fix, not absences to skip.

CI provisions the backends and leaves these unset, so a missing one is a failure. Developers
without a tdd setup can set `TD_TEST_ON_MISSING_SYSTEM=skip` and
`TD_TEST_ON_MISSING_DOMAIN=skip` locally to run the rest of the suite.

# Db-backed Tests (`#[td_test::test(db)]`)

Tests of apiserver logic take a database rather than an external resource, and they get it
from a tdd domain too — `apiserver_postgres`. **Nothing in the test code starts a database
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
1Password load beyond the crypto key. That manifest,
`variant/config/pytest/domains_rs.yaml`, holds `apiserver_postgres` for the db tests and
`apiserver_s3` / `apiserver_az` / `apiserver_gcs` for the object-store ones — named apart
from the `s3` / `azureblob` / `gcs` the Python suite reads, because the manifest recreates
what it names and a Rust run must not clear a domain another suite is reading.

The domains are recreated with `--force` on every run, so the db tests start against an
empty database, no per-test schema outlives the run that made it, and the objects the
object-store tests write are reclaimed rather than accumulating under a system's staging
root. All four are **required**: a store missing one fails the provisioning task instead of
failing every test that needed it. **Systems** are not this task's business: they are
long-running and expected to be up, so `tdd system create` / `restart` stay a separate,
deliberate act.

`cargo make test_rs_backends` runs just that provisioning, for when you want the domains
without the suite. `--no-domains` (or `--light`) skips it, for a run against a domain you
provisioned yourself. Point the tests at a different one with `TD_TEST_DB_DOMAIN=<name>`.

Every domain task takes a mandatory `--python` or `--rust` to say which suite's domains it
means: `cargo make domains_create --rust`, `cargo make domains_destroy --python`. The
manifests are named to match, `domains_py.yaml` and `domains_rs.yaml`, with the GA Python
variants as `domains_py_ga_<os>.yaml`.

The domain descriptor carries the owning system's superuser as `SYS_ADMIN_USER` /
`SYS_ADMIN_PASSWORD`, which is what the per-test path connects as: creating and dropping
schemas and ensuring the cluster-global RLS role are beyond the domain's own owner role.

The same `TD_TEST_ON_MISSING_DOMAIN` policy applies, so a developer without a tdd setup can
set it to `skip`. td-database's own internals suite (`components/tests/*`,
`pool::connect`) tests the database layer itself rather than apiserver logic, so it needs
a superuser: it creates and drops cluster-global roles and sets `BYPASSRLS`. It takes that
from the `apiserver_postgres_internals` domain, whose descriptor carries the owning system's
superuser. Nothing starts a server for it, so it fails rather than skips when that domain
cannot be resolved.
