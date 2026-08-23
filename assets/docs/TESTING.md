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

The storage backends are ported so far; the rest stay Python-only until a Rust test needs
them. All of these live in `td_test::domain`:

| tdx type | System | Domain |
|---|---|---|
| `s3` | `S3System` | `S3Domain` |
| `gcs` | `GcsSystem` | `GcsDomain` |
| `azureblob` | `AzureblobSystem` | `AzureblobDomain` |
| `fs` | `FsSystem` | `FsDomain` |

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

Python names the same things the same way, so a test reads alike in either language:

```python
    @s3_domain("s3_world_xs")
    def test_reads_seeded_data(domain_s3_world_xs: S3Domain):
        ...
```

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

*NOTE:* CI runs `systems_create` before the Rust tests but does not yet run
`domains_create`, so only `#[td_test::system(...)]` tests can run there today.
