//
// Copyright 2026 Tabsdata Inc.
//

//! Inject tdd-provisioned systems / domains into tests as typed structs.
//!
//! Rust port of the Python `tabsdatax.itest.fixture` backend fixtures. A test
//! names the backend it needs and receives it built from the very same
//! descriptor the Python fixtures read — one provisioning tool, both languages:
//!
//! ```ignore
//! #[td_test::domain("s3_world_xs")]
//! #[tokio::test]
//! async fn reads_a_staged_csv(domain_s3_world_xs: S3Domain) {
//!     let store = domain_s3_world_xs.object_store().unwrap();
//! }
//! ```
//!
//! The attribute argument is the backend's name in the tdd store, and the
//! parameter it fills is that name prefixed with `domain_` / `system_` — so
//! `#[td_test::domain("s3_world_xs")]` binds `domain_s3_world_xs`. That is the
//! Python fixtures' naming, where `@s3_domain("orders")` injects
//! `domain_orders`, so a test reads the same in either language and several
//! backends of one type can be asked for at once. The fixture type comes from
//! the parameter's own type, never repeated in the attribute.
//!
//! Stack one attribute per backend, above whatever test attribute the function
//! uses — `#[test]` and `#[tokio::test]` both work, since resolution is
//! synchronous.
//!
//! Resolution asks tdd for the descriptor (`tdd ... info`, see [`tdd`]; only
//! tdd knows the store and decrypts it with `$_TDD_CRYPTO_KEY`), checks its role
//! and `system_type` match the requested struct, and projects its `meta` onto
//! the struct's fields — each field pulled from the descriptor key of the same
//! (upper-cased) name. Loads are cached per `(store, name, struct)` for the
//! lifetime of the test binary.
//!
//! # Resolution order
//!
//! A backend is looked for in three places, in this order:
//!
//! 1. **The cache**, per `(store, name, struct)` — but only within one process,
//!    and the test harness runs a process per test.
//! 2. **`<UPCASE_NAME>__<KEY>` environment variables**, the shape
//!    `tdd domain info --format export --prefix <NAME>` emits. A *complete* set
//!    is the whole backend: neither tdd nor `$_TDD_CRYPTO_KEY` is touched. This
//!    exists because asking tdd costs a process start, which a process-per-test
//!    harness would otherwise pay once per test. `__ENTITIES` is read as a JSON
//!    object (`{"city": 4079}`) so a seeded domain's row counts survive; `NAME`
//!    is filled from the requested name.
//! 3. **The descriptor**, through tdd, which stays the source of truth.
//!
//! An *incomplete* prefix is not an error — it warns on stderr and falls
//! through to the descriptor, keeping **none** of the environment's values, so
//! the two sources are never mixed. Falling through rather than failing is what
//! makes a collision harmless: `<NAME>__<KEY>` is also the shape tdd's own
//! create-time options use, so a system named `s3` shares a prefix with the
//! `S3__BUCKET` / `S3__REGION` variables `tdd system create` reads.
//!
//! **A stale export outranks a fresh descriptor.** `tdd domain create` mints a
//! new password each time, so an environment exported before a re-provision
//! still builds a complete — and wrong — backend. Re-export after provisioning,
//! which is what the `test_rs` backends task does.
//!
//! Unlike the `reqs` fixtures, nothing here is created or torn down: a domain is
//! provisioned by `tdd domain create` before the run and dropped by
//! `tdd domain destroy` after it. Per-test isolation is the test's job — carve
//! out a corner of the shared domain with [`run_prefix`].
//!
//! Env, split per family so systems and domains are governed independently:
//! `TD_TEST_ON_MISSING_SYSTEM` / `TD_TEST_ON_MISSING_DOMAIN` = `skip` | `fail`
//! (default) when the backend cannot be reached — not in the tdd store, tdd not
//! installed, or no `_TDD_CRYPTO_KEY` to open the store with;
//! `TD_TEST_SKIP_SYSTEMS` / `TD_TEST_SKIP_DOMAINS` = comma-separated backend
//! `name`s to skip even when present. A backend that is reachable but unusable
//! (wrong type, a descriptor missing a key the struct needs, or one tdd cannot
//! read) always fails, whatever the policy says.

use std::any::{Any, TypeId};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex};

use serde::Deserialize;

pub mod backends;
mod define;
pub mod tdd;

pub use backends::{
    AzureblobDomain, AzureblobSystem, FsDomain, FsSystem, GcsDomain, GcsSystem, PostgresDomain,
    PostgresSystem, S3Domain, S3System,
};

/// The identity keys [`Attrs`] adds on top of a descriptor's `meta`, and the
/// separator between a backend's name and a key in the environment. The tests
/// assert the literals rather than these, so renaming a value is caught.
const KEY_NAME: &str = "NAME";
const KEY_ENTITIES: &str = "ENTITIES";
const KEY_MODEL: &str = "MODEL";
const KEY_DATA: &str = "DATA";
const KEY_EXTERNAL_NAME: &str = "EXTERNAL_NAME";
const KEY_SYS_NAME: &str = "SYS_NAME";
const ENV_KEY_SEPARATOR: &str = "__";

/// Which family a backend struct belongs to. `system_orders` and
/// `domain_orders` are distinct backends that happen to share a name, so every
/// lookup is keyed by the pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    Domain,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::System => "system",
            Role::Domain => "domain",
        }
    }

    /// `skip` | `fail` (default `fail`) when the backend cannot be reached.
    pub fn on_missing_env(self) -> &'static str {
        match self {
            Role::System => "TD_TEST_ON_MISSING_SYSTEM",
            Role::Domain => "TD_TEST_ON_MISSING_DOMAIN",
        }
    }

    /// Comma-separated backend `name`s to skip even when present.
    pub fn skip_env(self) -> &'static str {
        match self {
            Role::System => "TD_TEST_SKIP_SYSTEMS",
            Role::Domain => "TD_TEST_SKIP_DOMAINS",
        }
    }

    /// tdd's store dir for this family, when not its default one.
    pub fn store_env(self) -> &'static str {
        match self {
            Role::System => "TDD_SYSTEM_DIR",
            Role::Domain => "TDD_DOMAIN_DIR",
        }
    }
}

// --------------------------------------------------------------------------
// Attribute projection
// --------------------------------------------------------------------------

/// The flat `{KEY: value}` view a backend struct maps its fields onto: the
/// descriptor's connection `meta` (already upper-cased keys) plus the identity
/// keys — `NAME`, and for a domain `MODEL` / `DATA` / `ENTITIES` /
/// `EXTERNAL_NAME` / `SYS_NAME`.
#[derive(Debug, Clone, Default)]
pub struct Attrs {
    strings: BTreeMap<String, String>,
    entities: BTreeMap<String, i64>,
    has_entities: bool,
}

impl Attrs {
    pub fn get(&self, key: &str) -> Option<&str> {
        self.strings.get(key).map(String::as_str)
    }

    pub fn entities(&self) -> Option<&BTreeMap<String, i64>> {
        self.has_entities.then_some(&self.entities)
    }

    /// The keys present, for the "descriptor missing key(s)" hint.
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.strings.keys().map(String::as_str)
    }

    /// The same flat view, read from `<NAME>__<KEY>` environment variables
    /// instead of a descriptor — the shape `tdd domain info --format export
    /// --prefix <NAME>` emits.
    ///
    /// `NAME` is seeded from the backend's store name (the env set carries the
    /// descriptor's `meta`, not its identity) and `ENTITIES` is read as a JSON
    /// object so a seeded domain's row counts survive the round trip. `None`
    /// when the prefix names nothing, which is the signal to read the descriptor.
    pub(crate) fn from_env(name: &str) -> Option<Self> {
        let prefix = format!("{}{ENV_KEY_SEPARATOR}", name.to_uppercase());
        let mut attrs = Attrs::default();
        for (key, value) in std::env::vars() {
            let Some(key) = key.strip_prefix(&prefix) else {
                continue;
            };
            if key == KEY_ENTITIES {
                // Absent or unparseable leaves `entities` unset, exactly as for a
                // domain that create-data has not run against.
                if let Ok(entities) = serde_json::from_str(&value) {
                    attrs.entities = entities;
                    attrs.has_entities = true;
                }
                continue;
            }
            attrs.strings.insert(key.to_string(), value);
        }
        if attrs.strings.is_empty() && !attrs.has_entities {
            return None;
        }
        attrs
            .strings
            .entry(KEY_NAME.to_string())
            .or_insert_with(|| name.to_string());
        Some(attrs)
    }

    pub(crate) fn of(descriptor: &tdd::Descriptor) -> Self {
        let mut attrs = Attrs::default();
        attrs.strings.extend(descriptor.meta.clone());
        attrs
            .strings
            .insert(KEY_NAME.into(), descriptor.name.clone());
        if descriptor.role == Role::Domain {
            if let Some(model) = &descriptor.model {
                attrs.strings.insert(KEY_MODEL.into(), model.clone());
            }
            if let Some(data) = &descriptor.data {
                attrs.strings.insert(KEY_DATA.into(), data.clone());
            }
            if !descriptor.entities.is_empty() {
                attrs.entities = descriptor.entities.clone();
                attrs.has_entities = true;
            }
            attrs
                .strings
                .insert(KEY_EXTERNAL_NAME.into(), descriptor.external_name.clone());
            attrs
                .strings
                .insert(KEY_SYS_NAME.into(), descriptor.sys_name.clone());
        }
        attrs
    }
}

/// How one backend-struct field is read out of [`Attrs`]. Implemented for the
/// field types the descriptors can produce; `Option<T>` fields are the ones a
/// descriptor may legitimately omit (`MODEL` / `DATA` before create-model).
pub trait FromAttr: Sized {
    fn from_attr(attrs: &Attrs, key: &str) -> Result<Self, String>;
}

impl FromAttr for String {
    fn from_attr(attrs: &Attrs, key: &str) -> Result<Self, String> {
        attrs
            .get(key)
            .map(str::to_string)
            .ok_or_else(|| key.to_string())
    }
}

impl FromAttr for PathBuf {
    fn from_attr(attrs: &Attrs, key: &str) -> Result<Self, String> {
        attrs
            .get(key)
            .map(PathBuf::from)
            .ok_or_else(|| key.to_string())
    }
}

impl FromAttr for Option<String> {
    fn from_attr(attrs: &Attrs, key: &str) -> Result<Self, String> {
        Ok(attrs.get(key).map(str::to_string))
    }
}

impl FromAttr for Option<BTreeMap<String, i64>> {
    fn from_attr(attrs: &Attrs, _key: &str) -> Result<Self, String> {
        Ok(attrs.entities().cloned())
    }
}

// --------------------------------------------------------------------------
// The backend contract
// --------------------------------------------------------------------------

/// A typed view of a tdd-provisioned system or domain.
///
/// Implemented by [`define_backend!`](crate::define_backend), which derives
/// [`Backend::build`] from the struct's fields.
pub trait Backend: Sized + Clone + Send + Sync + 'static {
    /// The vendor slug the descriptor must carry, e.g. `s3`.
    const SYSTEM_TYPE: &'static str;
    /// Which family of the tdd store the descriptor is read from.
    const ROLE: Role;

    fn build(attrs: &Attrs) -> Result<Self, String>;
}

/// A [`Backend`] that is a domain. [`define_backend!`](crate::define_backend)
/// implements exactly one of this and [`SystemBackend`], which is what makes
/// `#[td_test::domain(...)]` on a system struct a compile error rather than a
/// lookup in the wrong store dir.
pub trait DomainBackend: Backend {}

/// A [`Backend`] that is a system. See [`DomainBackend`].
pub trait SystemBackend: Backend {}

/// A unique child of `root` for one test to own: `<root>/<area>/<uuid4>`.
///
/// Port of the Python connectors' `run_prefix`. A tdd domain is provisioned once
/// and shared by the whole run, so tests that write must carve out their own
/// corner; nothing deletes it, and a `tdd system purge` reclaims the lot.
pub fn run_prefix(root: impl AsRef<str>, area: &str) -> String {
    let parent = root.as_ref().trim_matches('/');
    let suffix = format!("{area}/{}", uuid::Uuid::new_v4().simple());
    if parent.is_empty() {
        suffix
    } else {
        format!("{parent}/{suffix}")
    }
}

// --------------------------------------------------------------------------
// Resolution
// --------------------------------------------------------------------------

/// Which backend a cache entry is: `(family, store, name, struct)`, the store
/// being `$TDD_SYSTEM_DIR` / `$TDD_DOMAIN_DIR` (`None` for tdd's own), exactly
/// like the Python fixture's session cache.
type CacheKey = (Role, Option<String>, String, TypeId);

type BackendCache = Mutex<HashMap<CacheKey, Arc<dyn Any + Send + Sync>>>;

static CACHE: LazyLock<BackendCache> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// Why a backend could not be handed to the test.
#[derive(Debug)]
enum Unavailable {
    /// The backend is out of reach: not in the store, no tdd or no crypto key to
    /// read it with, or the name is listed in `TD_TEST_SKIP_*`.
    Skip(String),
    /// Anything the operator has to fix: wrong role, wrong system type, a
    /// descriptor tdd cannot read, a missing required key.
    Fail(String),
}

fn on_missing_is_skip(role: Role) -> Result<bool, String> {
    let env = role.on_missing_env();
    match std::env::var(env).unwrap_or_default().trim().to_lowercase() {
        s if s.is_empty() || s == "fail" => Ok(false),
        s if s == "skip" => Ok(true),
        other => Err(format!("${env} must be 'skip' or 'fail' (got '{other}')")),
    }
}

/// Apply the per-family on-missing policy to a backend that cannot be reached.
///
/// Not in the store, no tdd and no crypto key all land here. Python fails
/// outright without tdd or the key, but a tool or key the developer does not
/// have is exactly the "backend unavailable" the policy exists to govern —
/// routing it through the policy is what keeps `TD_TEST_ON_MISSING_*=skip` a
/// truthful promise.
fn unreachable(role: Role, reason: &str, remedy: &str) -> Unavailable {
    match on_missing_is_skip(role) {
        Err(msg) => Unavailable::Fail(msg),
        Ok(true) => Unavailable::Skip(format!(
            "{reason}, so this test is skipped (${}=skip)",
            role.on_missing_env()
        )),
        Ok(false) => Unavailable::Fail(format!(
            "{reason}. {remedy}, or set ${}=skip to skip instead of fail.",
            role.on_missing_env()
        )),
    }
}

fn is_skipped_by_name(role: Role, name: &str) -> bool {
    std::env::var(role.skip_env())
        .unwrap_or_default()
        .split(',')
        .any(|listed| listed.trim() == name && !name.is_empty())
}

fn resolve<B: Backend>(name: &str) -> Result<B, Unavailable> {
    let role = B::ROLE;
    let kind = role.as_str();

    // A backend the developer asked to skip is skipped whether or not it is
    // reachable at all.
    if is_skipped_by_name(role, name) {
        return Err(Unavailable::Skip(format!(
            "requested {kind} '{name}' is listed in ${}, so this test is skipped",
            role.skip_env()
        )));
    }

    let key = (
        role,
        std::env::var(role.store_env()).ok(),
        name.to_string(),
        TypeId::of::<B>(),
    );
    if let Some(cached) = CACHE.lock().unwrap().get(&key) {
        return Ok(cached
            .downcast_ref::<B>()
            .expect("cached by TypeId")
            .clone());
    }

    // Then the environment, ahead of the descriptor and of needing tdd at all: a
    // complete `<NAME>__<KEY>` set is the whole backend, where asking tdd costs
    // a process start — and the harness runs a process per test, so it would
    // pay that once per test.
    //
    // An env set that does not build a whole backend is not an error; it falls
    // through to the descriptor, which stays the source of truth. That is what
    // keeps a collision harmless, `<NAME>__<KEY>` being the shape tdd's own
    // create-time options already use.
    if let Some(attrs) = Attrs::from_env(name) {
        match B::build(&attrs) {
            Ok(built) => return Ok(cache(key, built)),
            // Not an error, but never silent: a half-set prefix is far more
            // likely a typo or a stale export than a deliberate act. Nothing
            // read from the environment is kept — the descriptor supplies every
            // value, so the two are never mixed.
            Err(missing) => eprintln!(
                "[td-test] WARNING: ${}__* is set but does not describe a whole \
                 {kind} '{name}' (missing key(s): {missing}); ignoring it and \
                 reading the descriptor instead",
                name.to_uppercase()
            ),
        }
    }

    // Before asking tdd: without the key the store is unreadable, so the backend
    // is unavailable rather than broken.
    if std::env::var(tdd::ENV_CRYPTO_KEY)
        .unwrap_or_default()
        .is_empty()
    {
        return Err(unreachable(
            role,
            &format!(
                "cannot read the {kind} '{name}': ${} is not set",
                tdd::ENV_CRYPTO_KEY
            ),
            "Load it with 'tdd env set --crypto'",
        ));
    }
    if !tdd::exe().is_file() {
        return Err(unreachable(
            role,
            &format!(
                "cannot read the {kind} '{name}': tdd is not installed at {}",
                tdd::exe().display()
            ),
            "Install it with 'cargo make permit'",
        ));
    }

    let descriptor = match tdd::descriptor(role, name) {
        Ok(descriptor) => descriptor,
        Err(why) => return Err(not_found_or_fail(role, name, why)),
    };
    if descriptor.role != role {
        return Err(Unavailable::Fail(format!(
            "'{name}' is a {} descriptor, not a {kind}",
            descriptor.role.as_str()
        )));
    }
    if descriptor.system != B::SYSTEM_TYPE {
        return Err(Unavailable::Fail(format!(
            "'{name}' is a '{}' {kind}, but was requested as a '{}' one",
            descriptor.system,
            B::SYSTEM_TYPE
        )));
    }

    let attrs = Attrs::of(&descriptor);
    let built = B::build(&attrs).map_err(|missing| {
        Unavailable::Fail(format!(
            "'{name}': cannot build the {kind} from its descriptor (missing key(s): {missing})"
        ))
    })?;
    Ok(cache(key, built))
}

/// A backend tdd could not read: per the missing policy when the store has no
/// such name, else a failure with tdd's own message — the backend is there and
/// something else is wrong.
fn not_found_or_fail(role: Role, name: &str, why: String) -> Unavailable {
    let kind = role.as_str();
    match tdd::names(role) {
        Ok(names) if !names.iter().any(|stored| stored == name) => unreachable(
            role,
            &format!("provisioned {kind} '{name}' is not in the tdd store"),
            &format!("Create it with 'tdd {kind} create'"),
        ),
        _ => Unavailable::Fail(format!("{kind} '{name}': {why}")),
    }
}

/// Memoize a built backend for the rest of this process.
fn cache<B: Backend>(key: CacheKey, built: B) -> B {
    CACHE
        .lock()
        .unwrap()
        .insert(key, Arc::new(built.clone()) as Arc<dyn Any + Send + Sync>);
    built
}

/// Resolve the domain `store_name` for `test_name`, or `None` to skip the test.
///
/// What `#[td_test::domain("<store_name>")]` expands to, binding
/// `domain_<store_name>`. Synchronous —
/// reading a descriptor is one tdd process, so a test needs no runtime to ask
/// for a backend, and sync tests work the same as async ones.
pub fn resolve_domain<B: DomainBackend>(store_name: &str, test_name: &str) -> Option<B> {
    resolve_or_skip::<B>(store_name, test_name)
}

/// The [`resolve_domain`] sibling for systems, behind
/// `#[td_test::system("<store_name>")]`.
pub fn resolve_system<B: SystemBackend>(store_name: &str, test_name: &str) -> Option<B> {
    resolve_or_skip::<B>(store_name, test_name)
}

fn resolve_or_skip<B: Backend>(store_name: &str, test_name: &str) -> Option<B> {
    match resolve::<B>(store_name) {
        Ok(backend) => Some(backend),
        Err(Unavailable::Skip(msg)) => {
            eprintln!("[td-test] SKIP: {test_name}. {msg}");
            None
        }
        Err(Unavailable::Fail(msg)) => {
            panic!("Backend not usable, test: {test_name}. {msg}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::process::Command;

    fn domain() -> tdd::Descriptor {
        tdd::parse(
            r#"
            {
                "role": "domain",
                "system": "s3",
                "name": "landing",
                "sys_name": "s30",
                "external_name": "d_tucu_0_landing",
                "meta": {
                    "BUCKET": "b",
                    "PREFIX": "d_tucu_0_landing"
                },
                "model": "world",
                "data": null,
                "entities": {
                    "csv/city_1.csv": 7
                }
            }
            "#,
        )
    }

    #[test]
    fn test_attrs_of_domain() {
        let attrs = Attrs::of(&domain());
        assert_eq!(attrs.get("BUCKET"), Some("b"));
        assert_eq!(attrs.get("NAME"), Some("landing"));
        assert_eq!(attrs.get("EXTERNAL_NAME"), Some("d_tucu_0_landing"));
        assert_eq!(attrs.get("SYS_NAME"), Some("s30"));
        assert_eq!(attrs.get("MODEL"), Some("world"));
        // `data` is unset until create-data runs, so the key is absent.
        assert_eq!(attrs.get("DATA"), None);
        assert_eq!(attrs.entities().unwrap()["csv/city_1.csv"], 7);
    }

    #[test]
    fn test_attrs_of_system_has_no_domain_keys() {
        let attrs = Attrs::of(&tdd::parse(
            r#"
            {
                "role": "system",
                "system": "s3",
                "name": "s30",
                "meta": {
                    "SYS_PATH": "root"
                }
            }
            "#,
        ));
        assert_eq!(attrs.get("SYS_PATH"), Some("root"));
        assert_eq!(attrs.get("NAME"), Some("s30"));
        assert_eq!(attrs.get("EXTERNAL_NAME"), None);
        assert!(attrs.entities().is_none());
    }

    #[test]
    fn test_entities_absent_when_empty() {
        let mut descriptor = domain();
        descriptor.entities.clear();
        assert!(Attrs::of(&descriptor).entities().is_none());
    }

    #[test]
    fn test_from_attr() {
        let attrs = Attrs::of(&domain());
        assert_eq!(String::from_attr(&attrs, "BUCKET").unwrap(), "b");
        assert_eq!(
            PathBuf::from_attr(&attrs, "BUCKET").unwrap(),
            PathBuf::from("b")
        );
        assert_eq!(String::from_attr(&attrs, "NOPE").unwrap_err(), "NOPE");
        assert_eq!(Option::<String>::from_attr(&attrs, "NOPE").unwrap(), None);
        assert_eq!(
            Option::<String>::from_attr(&attrs, "MODEL")
                .unwrap()
                .as_deref(),
            Some("world")
        );
    }

    /// The prefixes here are deliberately names no provisioned backend can have:
    /// `from_env` reads the ambient environment, so a test naming a real backend
    /// would break the moment that backend's vars are exported for a run.
    #[test]
    fn test_attrs_from_env() {
        temp_env::with_vars(
            [
                ("ZZ_PROBE_ONE__HOST", Some("localhost")),
                ("ZZ_PROBE_ONE__PORT", Some("56018")),
                ("ZZ_PROBE_ONE__DB", Some("d_tucu_0_apiserver_postgres")),
                ("ZZ_PROBE_TWO__HOST", Some("nope")),
            ],
            || {
                let attrs = Attrs::from_env("zz_probe_one").unwrap();
                assert_eq!(attrs.get("HOST"), Some("localhost"));
                assert_eq!(attrs.get("PORT"), Some("56018"));
                // the store name fills NAME, which the env set does not carry
                assert_eq!(attrs.get("NAME"), Some("zz_probe_one"));
                // another backend's prefix is not ours
                assert_eq!(attrs.get("DB"), Some("d_tucu_0_apiserver_postgres"));
                assert_eq!(attrs.keys().count(), 4);
                assert!(attrs.entities().is_none());
            },
        );
    }

    #[test]
    fn test_attrs_from_env_is_none_without_the_prefix() {
        temp_env::with_var("ZZ_PROBE_OTHER__HOST", Some("x"), || {
            assert!(Attrs::from_env("zz_probe_absent").is_none());
        });
    }

    /// A seeded domain's row counts have to survive the env round trip, or
    /// `staged()` / `tables()` would silently come back empty.
    #[test]
    fn test_attrs_from_env_reads_entities_as_json() {
        temp_env::with_vars(
            [
                ("ZZ_PROBE_SEEDED__HOST", Some("h")),
                (
                    "ZZ_PROBE_SEEDED__ENTITIES",
                    Some(r#"{"city": 4079, "country": 239}"#),
                ),
            ],
            || {
                let attrs = Attrs::from_env("zz_probe_seeded").unwrap();
                assert_eq!(attrs.entities().unwrap()["city"], 4079);
                assert_eq!(attrs.entities().unwrap()["country"], 239);
            },
        );
        // Unparseable is treated as absent, not as a failure.
        temp_env::with_vars(
            [
                ("ZZ_PROBE_SEEDED__HOST", Some("h")),
                ("ZZ_PROBE_SEEDED__ENTITIES", Some("{")),
            ],
            || {
                assert!(
                    Attrs::from_env("zz_probe_seeded")
                        .unwrap()
                        .entities()
                        .is_none()
                );
            },
        );
    }

    #[test]
    fn test_role_envs() {
        assert_eq!(Role::System.as_str(), "system");
        assert_eq!(Role::Domain.as_str(), "domain");
        assert_eq!(Role::System.on_missing_env(), "TD_TEST_ON_MISSING_SYSTEM");
        assert_eq!(Role::Domain.on_missing_env(), "TD_TEST_ON_MISSING_DOMAIN");
        assert_eq!(Role::System.skip_env(), "TD_TEST_SKIP_SYSTEMS");
        assert_eq!(Role::Domain.skip_env(), "TD_TEST_SKIP_DOMAINS");
    }

    #[test]
    fn test_on_missing_policy() {
        let env = Role::Domain.on_missing_env();
        temp_env::with_var_unset(env, || {
            assert!(!on_missing_is_skip(Role::Domain).unwrap());
        });
        temp_env::with_var(env, Some("fail"), || {
            assert!(!on_missing_is_skip(Role::Domain).unwrap());
        });
        temp_env::with_var(env, Some(" Skip "), || {
            assert!(on_missing_is_skip(Role::Domain).unwrap());
        });
        temp_env::with_var(env, Some("maybe"), || {
            assert!(on_missing_is_skip(Role::Domain).unwrap_err().contains(env));
        });
    }

    /// The two ways a backend can be out of reach both honour the policy: a
    /// developer with `skip` set must be able to run the suite whether they are
    /// missing the descriptor or only the key to open it.
    #[test]
    fn test_unreachable_follows_the_policy() {
        let env = Role::Domain.on_missing_env();
        temp_env::with_var(env, Some("skip"), || {
            let reason = matches!(
                unreachable(Role::Domain, "no descriptor", "Create it"),
                Unavailable::Skip(_)
            );
            assert!(reason);
        });
        temp_env::with_var(env, Some("fail"), || {
            let Unavailable::Fail(msg) = unreachable(Role::Domain, "no descriptor", "Create it")
            else {
                panic!("expected a failure");
            };
            assert!(msg.contains("no descriptor"), "got: {msg}");
            assert!(msg.contains("Create it"), "got: {msg}");
            assert!(msg.contains(env), "got: {msg}");
        });
        temp_env::with_var(env, Some("bogus"), || {
            assert!(matches!(
                unreachable(Role::Domain, "no descriptor", "Create it"),
                Unavailable::Fail(_)
            ));
        });
    }

    const FAKE_KEY: &str = "a-test-secret-long-enough";

    /// A scratch tdd store for one test: `(system dir, domain dir)`.
    fn scratch_store(tag: &str) -> (PathBuf, PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "td-test-tdd-{tag}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        (root.join("system"), root.join("domain"))
    }

    /// Run the real `tdd <args> --descriptor-only` against the scratch store:
    /// the descriptors a create writes, with nothing provisioned. The fake
    /// `S3__*` config is what an s3 create reads.
    fn tdd_create(store: &(PathBuf, PathBuf), args: &[&str]) {
        let output = Command::new(tdd::exe())
            .args(args)
            .args(["--descriptor-only", "--force", "--format", "json"])
            .env(Role::System.store_env(), &store.0)
            .env(Role::Domain.store_env(), &store.1)
            .env(tdd::ENV_CRYPTO_KEY, FAKE_KEY)
            .envs([
                ("S3__BUCKET", "a-bucket"),
                ("S3__REGION", "us-east-1"),
                ("S3__ACCESS_KEY", "ak"),
                ("S3__SECRET_KEY", "sk"),
                ("S3__SYS_PATH", "root"),
            ])
            .output()
            .expect("tdd runs");
        assert!(
            output.status.success(),
            "tdd {args:?} failed:\n{}",
            String::from_utf8_lossy(&output.stdout)
        );
    }

    fn with_store<T>(store: &(PathBuf, PathBuf), f: impl FnOnce() -> T) -> T {
        temp_env::with_vars(
            [
                (Role::System.store_env(), Some(path_str(&store.0))),
                (Role::Domain.store_env(), Some(path_str(&store.1))),
                (tdd::ENV_CRYPTO_KEY, Some(FAKE_KEY)),
                (Role::Domain.on_missing_env(), None),
            ],
            f,
        )
    }

    fn path_str(path: &Path) -> &str {
        path.to_str().expect("a utf-8 temp dir")
    }

    /// The whole path, against descriptors tdd itself writes: it fails the
    /// moment tdd's JSON and this reader disagree.
    #[test]
    fn test_resolves_a_domain_tdd_wrote() {
        let store = scratch_store("resolve");
        tdd_create(
            &store,
            &["system", "create", "--type", "s3", "--sys", "s30"],
        );
        tdd_create(
            &store,
            &["domain", "create", "--sys", "s30", "--domain", "landing"],
        );
        with_store(&store, || {
            let domain = resolve::<S3Domain>("landing").unwrap();
            assert_eq!(domain.name, "landing");
            assert_eq!(domain.bucket, "a-bucket");
            assert_eq!(domain.access_key, "ak");
            assert!(
                domain.prefix.ends_with("_landing"),
                "got: {}",
                domain.prefix
            );
            assert_eq!(
                domain.url().unwrap().as_str(),
                format!("s3://a-bucket/root/{}", domain.prefix)
            );
            let Err(Unavailable::Fail(msg)) = resolve::<FsDomain>("landing") else {
                panic!("an s3 domain must not build an fs one");
            };
            assert!(msg.contains("'s3'"), "got: {msg}");
        });
        std::fs::remove_dir_all(store.0.parent().unwrap()).unwrap();
    }

    /// Not in the store follows the policy; it is never a bare tdd error.
    #[test]
    fn test_a_backend_not_in_the_store_follows_the_policy() {
        let store = scratch_store("absent");
        with_store(&store, || {
            let Err(Unavailable::Fail(msg)) = resolve::<S3Domain>("absent") else {
                panic!("expected a failure");
            };
            assert!(msg.contains("not in the tdd store"), "got: {msg}");
            assert!(msg.contains(Role::Domain.on_missing_env()), "got: {msg}");
        });
        with_store(&store, || {
            temp_env::with_var(Role::Domain.on_missing_env(), Some("skip"), || {
                assert!(matches!(
                    resolve::<S3Domain>("absent"),
                    Err(Unavailable::Skip(_))
                ));
            })
        });
    }

    /// No key is unavailable, not broken, and tdd is not even asked.
    #[test]
    fn test_missing_crypto_key_follows_the_policy() {
        temp_env::with_vars(
            [
                (tdd::ENV_CRYPTO_KEY, None),
                (Role::Domain.on_missing_env(), Some("skip")),
            ],
            || {
                let Err(Unavailable::Skip(msg)) = resolve::<S3Domain>("zz_probe_nokey") else {
                    panic!("expected a skip");
                };
                assert!(msg.contains(tdd::ENV_CRYPTO_KEY), "got: {msg}");
            },
        );
        temp_env::with_vars_unset([tdd::ENV_CRYPTO_KEY, Role::Domain.on_missing_env()], || {
            assert!(matches!(
                resolve::<S3Domain>("zz_probe_nokey"),
                Err(Unavailable::Fail(_))
            ));
        });
    }

    /// An incomplete prefix must not be half-applied: resolution falls through
    /// to tdd, so a partial env set cannot smuggle a value in.
    #[test]
    fn test_incomplete_env_falls_through_to_the_descriptor() {
        let store = scratch_store("partial-env");
        with_store(&store, || {
            // BUCKET/REGION/... are missing, so this cannot build an S3Domain.
            temp_env::with_var("ZZ_PROBE_PARTIAL__HOST", Some("localhost"), || {
                // Reaching tdd at all is the proof: the store is empty, so the
                // error names the store, not the env set.
                let Err(Unavailable::Fail(msg)) = resolve::<S3Domain>("zz_probe_partial") else {
                    panic!("expected tdd to be asked");
                };
                assert!(msg.contains("not in the tdd store"), "got: {msg}");
            })
        });
    }

    #[test]
    fn test_skip_by_name() {
        let env = Role::Domain.skip_env();
        temp_env::with_var_unset(env, || {
            assert!(!is_skipped_by_name(Role::Domain, "landing"));
        });
        temp_env::with_var(env, Some("other, landing ,more"), || {
            assert!(is_skipped_by_name(Role::Domain, "landing"));
            assert!(!is_skipped_by_name(Role::Domain, "land"));
        });
        // An empty list must not swallow every backend.
        temp_env::with_var(env, Some(""), || {
            assert!(!is_skipped_by_name(Role::Domain, "landing"));
        });
    }
}

/// The `#[td_test::domain]` / `#[td_test::system]` attributes themselves.
///
/// Each shape below is `#[ignore]`d so the harness never runs it on its own —
/// it would resolve against whatever store the developer happens to have. They
/// are driven instead by [`every_shape_skips_when_the_store_is_absent`], which
/// points the store at nothing and turns the skip policy on, so the macro's
/// whole expansion runs and takes the skip branch. Their bodies panic on the
/// run branch, so a mis-set env fails the driver instead of passing vacuously.
#[cfg(test)]
mod attribute_tests {
    use super::{S3Domain, S3System};

    const NO_STORE: &str = "/nonexistent/tdd/store";

    fn with_skip_policy<T>(f: impl FnOnce() -> T) -> T {
        temp_env::with_vars(
            [
                (super::Role::System.store_env(), Some(NO_STORE)),
                (super::Role::Domain.store_env(), Some(NO_STORE)),
                (
                    super::tdd::ENV_CRYPTO_KEY,
                    Some("a-test-secret-long-enough"),
                ),
                (super::Role::Domain.on_missing_env(), Some("skip")),
                (super::Role::System.on_missing_env(), Some("skip")),
            ],
            f,
        )
    }

    #[td_test::system("s3")]
    #[test]
    #[ignore = "driven by every_shape_skips_when_the_store_is_absent"]
    fn sync_test_takes_a_system(system_s3: S3System) {
        panic!("should have skipped, got {}", system_s3.name);
    }

    #[td_test::domain("s3")]
    #[tokio::test]
    #[ignore = "driven by every_shape_skips_when_the_store_is_absent"]
    async fn async_test_takes_a_domain(domain_s3: S3Domain) {
        panic!("should have skipped, got {}", domain_s3.name);
    }

    /// Several backends at once, including two domains of the same type — the
    /// binding is by parameter name, so nothing is ambiguous.
    #[td_test::domain("s3")]
    #[td_test::domain("s3_world_xs")]
    #[td_test::system("s3")]
    #[tokio::test]
    #[ignore = "driven by every_shape_skips_when_the_store_is_absent"]
    async fn stacked_backends(
        domain_s3: S3Domain,
        domain_s3_world_xs: S3Domain,
        system_s3: S3System,
    ) {
        panic!(
            "should have skipped, got {} {} {}",
            domain_s3.name, domain_s3_world_xs.name, system_s3.name
        );
    }

    /// A fallible test skips with `Ok(())` rather than by panicking.
    #[td_test::system("s3")]
    #[test]
    #[ignore = "driven by every_shape_skips_when_the_store_is_absent"]
    fn fallible_test_skips_cleanly(system_s3: S3System) -> Result<(), String> {
        Err(format!("should have skipped, got {}", system_s3.name))
    }

    /// `#[tokio::test]` rewrites its function to a synchronous one, so every
    /// shape above is called the same way here.
    #[test]
    fn every_shape_skips_when_the_store_is_absent() {
        with_skip_policy(sync_test_takes_a_system);
        with_skip_policy(async_test_takes_a_domain);
        with_skip_policy(stacked_backends);
        with_skip_policy(|| assert_eq!(fallible_test_skips_cleanly(), Ok(())));
    }
}
