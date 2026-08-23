//
// Copyright 2026 Tabsdata Inc.
//

//! Inject tdx-provisioned systems / domains into tests as typed structs.
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
//! The attribute argument is the backend's name in the tdx store, and the
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
//! Resolution reads `$TD_BACKENDS_DIR/{systems2,domains2}/<name>`, decrypts it
//! with `$TD_BACKENDS_CRYPTO_KEY`, checks the descriptor's role and
//! `system_type` match the requested struct, and projects its `meta` onto the
//! struct's fields — each field pulled from the descriptor key of the same
//! (upper-cased) name. Loads are cached per `(descriptor path, struct)` for the
//! lifetime of the test binary.
//!
//! Unlike the `reqs` fixtures, nothing here is created or torn down: a domain is
//! provisioned by `tdx-domain create` before the run and dropped by
//! `tdx-domain destroy` after it. Per-test isolation is the test's job — carve
//! out a corner of the shared domain with [`run_prefix`].
//!
//! Env, split per family so systems and domains are governed independently:
//! `TD_TEST_ON_MISSING_SYSTEM` / `TD_TEST_ON_MISSING_DOMAIN` = `skip` | `fail`
//! (default) when the backend cannot be reached — no descriptor, or no
//! `TD_BACKENDS_CRYPTO_KEY` to open it with; `TD_TEST_SKIP_SYSTEMS` /
//! `TD_TEST_SKIP_DOMAINS` = comma-separated backend `name`s to skip even when
//! present. A backend that is reachable but unusable (wrong type, descriptor
//! missing a key the struct needs, or a legacy `TDX2` descriptor awaiting
//! `tdx-system upgrade`) always fails, whatever the policy says.

use std::any::{Any, TypeId};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex};

pub mod backends;
pub mod crypto;
mod define;
pub mod descriptor;
pub mod store;

pub use backends::{
    AzureblobDomain, AzureblobSystem, FsDomain, FsSystem, GcsDomain, GcsSystem, S3Domain, S3System,
};

/// The identity keys [`Attrs`] adds on top of a descriptor's `meta`. The tests
/// assert the literals rather than these, so renaming a value is caught.
const KEY_NAME: &str = "NAME";
const KEY_MODEL: &str = "MODEL";
const KEY_DATA: &str = "DATA";
const KEY_EXTERNAL_NAME: &str = "EXTERNAL_NAME";
const KEY_SYS_NAME: &str = "SYS_NAME";

/// Which family a backend struct belongs to. `system_orders` and
/// `domain_orders` are distinct backends that happen to share a name, so every
/// lookup is keyed by the pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

    fn of(info: &descriptor::Info) -> Self {
        let mut attrs = Attrs::default();
        match info {
            descriptor::Info::System(i) => {
                attrs.strings.extend(i.meta.clone());
                attrs.strings.insert(KEY_NAME.into(), i.name.clone());
            }
            descriptor::Info::Domain(i) => {
                attrs.strings.extend(i.meta.clone());
                attrs.strings.insert(KEY_NAME.into(), i.name.clone());
                if let Some(model) = &i.model {
                    attrs.strings.insert(KEY_MODEL.into(), model.clone());
                }
                if let Some(data) = &i.data {
                    attrs.strings.insert(KEY_DATA.into(), data.clone());
                }
                if !i.entities.is_empty() {
                    attrs.entities = i.entities.clone();
                    attrs.has_entities = true;
                }
                attrs
                    .strings
                    .insert(KEY_EXTERNAL_NAME.into(), i.external_name.clone());
                attrs
                    .strings
                    .insert(KEY_SYS_NAME.into(), i.sys_name.clone());
            }
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

/// A typed view of a tdx-provisioned system or domain.
///
/// Implemented by [`define_backend!`](crate::define_backend), which derives
/// [`Backend::build`] from the struct's fields.
pub trait Backend: Sized + Clone + Send + Sync + 'static {
    /// The vendor slug the descriptor must carry, e.g. `s3`.
    const SYSTEM_TYPE: &'static str;
    /// Which store dir the descriptor is read from.
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
/// Port of the Python connectors' `run_prefix`. A tdx domain is provisioned once
/// and shared by the whole run, so tests that write must carve out their own
/// corner; nothing deletes it, and a `tdx-system purge` reclaims the lot.
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

/// Built backends, keyed by `(descriptor path, struct)` exactly like the Python
/// fixture's session cache.
type BackendCache = Mutex<HashMap<(PathBuf, TypeId), Arc<dyn Any + Send + Sync>>>;

static CACHE: LazyLock<BackendCache> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// Why a backend could not be handed to the test.
#[derive(Debug)]
enum Unavailable {
    /// The backend is out of reach: no descriptor, no crypto key to open it
    /// with, or the name is listed in `TD_TEST_SKIP_*`.
    Skip(String),
    /// Anything the operator has to fix: wrong role, wrong system type, a
    /// descriptor the crypto key cannot open, a missing required key.
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
/// Both no-descriptor and no-crypto-key land here. Python raises outright on the
/// missing key, but a key the developer does not have is exactly the "backend
/// unavailable" the policy exists to govern — routing it through the policy is
/// what keeps `TD_TEST_ON_MISSING_*=skip` a truthful promise.
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
    let path = store::path_of(role, name);
    let kind = role.as_str();

    if !path.exists() {
        return Err(unreachable(
            role,
            &format!(
                "provisioned {kind} '{name}' was not found at {}",
                path.display()
            ),
            &format!("Create it with tdx-{kind} create"),
        ));
    }
    // Before touching the file: without the key the descriptor is unreadable, so
    // the backend is unavailable rather than broken.
    if let Err(why) = crypto::require_secret() {
        return Err(unreachable(
            role,
            &format!("cannot read the {kind} '{name}' descriptor: {why}"),
            "Export it from 1Password (see the backends make task)",
        ));
    }
    if is_skipped_by_name(role, name) {
        return Err(Unavailable::Skip(format!(
            "requested {kind} '{name}' is listed in ${}, so this test is skipped",
            role.skip_env()
        )));
    }

    let key = (path.clone(), TypeId::of::<B>());
    if let Some(cached) = CACHE.lock().unwrap().get(&key) {
        return Ok(cached
            .downcast_ref::<B>()
            .expect("cached by TypeId")
            .clone());
    }

    let info = descriptor::load(&path).map_err(Unavailable::Fail)?;
    if info.role() != role {
        return Err(Unavailable::Fail(format!(
            "'{name}' is a {} descriptor, not a {kind}",
            info.role().as_str()
        )));
    }
    if info.system() != B::SYSTEM_TYPE {
        return Err(Unavailable::Fail(format!(
            "'{name}' is a '{}' {kind}, but was requested as a '{}' one",
            info.system(),
            B::SYSTEM_TYPE
        )));
    }

    let attrs = Attrs::of(&info);
    let built = B::build(&attrs).map_err(|missing| {
        Unavailable::Fail(format!(
            "'{name}': cannot build the {kind} from its descriptor (missing key(s): {missing})"
        ))
    })?;
    CACHE
        .lock()
        .unwrap()
        .insert(key, Arc::new(built.clone()) as Arc<dyn Any + Send + Sync>);
    Ok(built)
}

/// Resolve the domain `store_name` for `test_name`, or `None` to skip the test.
///
/// What `#[td_test::domain("<store_name>")]` expands to, binding
/// `domain_<store_name>`. Synchronous —
/// reading a descriptor is a file read and a key derivation, so a test needs no
/// runtime to ask for a backend, and sync tests work the same as async ones.
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
    use crate::domain::descriptor::{DomainInfo, Info, SystemInfo};

    fn domain_info() -> Info {
        Info::Domain(DomainInfo {
            system: "s3".into(),
            name: "landing".into(),
            uuid: "u".into(),
            started: "t".into(),
            sys_name: "s30".into(),
            external_name: "d_tucu_0_landing".into(),
            meta: BTreeMap::from([
                ("BUCKET".to_string(), "b".to_string()),
                ("PREFIX".to_string(), "d_tucu_0_landing".to_string()),
            ]),
            model: Some("world".into()),
            data: None,
            entities: BTreeMap::from([("csv/city_1.csv".to_string(), 7)]),
            seed: 0,
        })
    }

    #[test]
    fn test_attrs_of_domain() {
        let attrs = Attrs::of(&domain_info());
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
        let attrs = Attrs::of(&Info::System(SystemInfo {
            system: "s3".into(),
            name: "s30".into(),
            uuid: "u".into(),
            started: "t".into(),
            meta: BTreeMap::from([("SYS_PATH".to_string(), "root".to_string())]),
        }));
        assert_eq!(attrs.get("SYS_PATH"), Some("root"));
        assert_eq!(attrs.get("NAME"), Some("s30"));
        assert_eq!(attrs.get("EXTERNAL_NAME"), None);
        assert!(attrs.entities().is_none());
    }

    #[test]
    fn test_entities_absent_when_empty() {
        let Info::Domain(mut d) = domain_info() else {
            unreachable!()
        };
        d.entities.clear();
        assert!(Attrs::of(&Info::Domain(d)).entities().is_none());
    }

    #[test]
    fn test_from_attr() {
        let attrs = Attrs::of(&domain_info());
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

    /// A present descriptor with no crypto key is unavailable, not broken.
    #[test]
    fn test_missing_crypto_key_skips_when_policy_allows() {
        let store_dir = std::env::temp_dir().join("td-test-domain-nokey/domains2");
        std::fs::create_dir_all(&store_dir).unwrap();
        std::fs::write(
            store_dir.join("landing"),
            "-----BEGIN TDX2 DESCRIPTOR-----\n",
        )
        .unwrap();

        temp_env::with_vars(
            [
                (
                    store::ENV_BACKENDS_DIR,
                    Some(store_dir.parent().unwrap().to_str().unwrap()),
                ),
                (crypto::ENV_CRYPTO_KEY, None),
                (Role::Domain.on_missing_env(), Some("skip")),
            ],
            || {
                let Err(Unavailable::Skip(msg)) = resolve::<S3Domain>("landing") else {
                    panic!("expected a skip");
                };
                assert!(msg.contains(crypto::ENV_CRYPTO_KEY), "got: {msg}");
            },
        );
        temp_env::with_vars(
            [
                (
                    store::ENV_BACKENDS_DIR,
                    Some(store_dir.parent().unwrap().to_str().unwrap()),
                ),
                (crypto::ENV_CRYPTO_KEY, None),
                (Role::Domain.on_missing_env(), None),
            ],
            || {
                assert!(matches!(
                    resolve::<S3Domain>("landing"),
                    Err(Unavailable::Fail(_))
                ));
            },
        );
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

    const NO_STORE: &str = "/nonexistent/tdx/store";

    fn with_skip_policy<T>(f: impl FnOnce() -> T) -> T {
        temp_env::with_vars(
            [
                (super::store::ENV_BACKENDS_DIR, Some(NO_STORE)),
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
