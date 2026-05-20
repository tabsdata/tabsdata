//
// Copyright 2026 Tabs Data Inc.
//

//! `db`-flavor emission: one `#[::tokio::test]` per backend, with the user's
//! body hoisted into a shared `__td_test_inner_<name>` helper at the parent
//! module level so per-backend arms don't duplicate the body and the helper
//! retains access to private items in the same `mod tests`.

use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};

use darling::FromMeta;
use proc_macro2::{Ident, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::TypePath;

use crate::common::{EmitCtx, ret_sig_and_skip};

/// Env var that overrides the default postgres bucket count. Read once at
/// proc-macro expansion time. Cargo does NOT track this for incremental
/// recompilation (it's `std::env::var`, not `tracked_env`), so changes
/// require a `cargo clean` (or at minimum a rebuild of the consumer crate
/// whose tests you want re-bucketed) to take effect.
const POSTGRES_PARALLELISM_ENV: &str = "TD_TEST_POSTGRES_PARALLELISM";
const POSTGRES_PARALLELISM_DEFAULT: u64 = 8;

/// Effective bucket count, env-overridable. Zero or non-numeric values fall
/// back to the default rather than silently disabling serialization.
static POSTGRES_PARALLELISM: LazyLock<u64> = LazyLock::new(|| {
    std::env::var(POSTGRES_PARALLELISM_ENV)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(POSTGRES_PARALLELISM_DEFAULT)
});

/// Round-robin counter, advanced once per `#[td_test::test(db)]` expansion.
/// Per-crate (each rustc proc-macro process resets it), so within a crate
/// postgres tests are evenly distributed across the N buckets with no
/// clustering. Source order determines the assignment, so the bucket of a
/// given test is stable across rebuilds as long as you don't reorder tests.
static POSTGRES_BUCKET: AtomicU64 = AtomicU64::new(0);

/// The backends the macro emits arms for. Order is significant: the first
/// backend's arm keeps the original test fn name (matches IDE `--exact <ident>`
/// gutter clicks); subsequent backends get `_<name>` suffixed names. Postgres
/// is first because it's the default backend (see `DEFAULT_BACKENDS` in
/// `td-test/src/db/mod.rs`) — keeping the IDE click in sync with what
/// `cargo test` actually runs by default.
#[derive(Copy, Clone)]
pub(crate) enum Backend {
    Postgres,
    Sqlite,
}

impl Backend {
    pub(crate) const ALL: &'static [Backend] = &[Backend::Postgres, Backend::Sqlite];

    fn name(self) -> &'static str {
        match self {
            Backend::Postgres => "postgres",
            Backend::Sqlite => "sqlite",
        }
    }

    /// Path to the provider type under `::td_test::db::`. Emitted directly via
    /// `quote!` so we avoid a per-invocation `syn::parse_str` round trip.
    fn provider_path(self) -> TokenStream2 {
        match self {
            Backend::Postgres => quote! { postgres::PostgresProvider },
            Backend::Sqlite => quote! { sqlite::SqliteProvider },
        }
    }

    /// Postgres tests share an embedded server's filesystem and System V IPC
    /// resources, so we file-lock them across processes. Sqlite arms
    /// provision per-test temp files and don't need cross-process
    /// serialization. (See `td_database::test_utils::postgres` for the full
    /// rationale and the env knob that tunes the bucket count.)
    fn needs_file_serial(self) -> bool {
        matches!(self, Backend::Postgres)
    }
}

/// Per-backend arm: a precomputed setup expression tied to which backend
/// produced it. Gated at the call site by `::td_test::__cfg_<name>!` rather
/// than `#[cfg(<name>)]` so downstream crates don't need a build.rs
/// trampoline to declare the backend cfgs.
pub(crate) struct DbArm {
    pub(crate) backend: Backend,
    pub(crate) setup_expr: TokenStream2,
}

/// A wrapper around `syn::Path` that implements `FromMeta` for darling parsing.
///
/// Allows `fixture = some::path::Type` in proc macro attributes.
#[derive(Debug, Clone)]
pub(crate) struct FixturePath(syn::Path);

impl FromMeta for FixturePath {
    fn from_expr(expr: &syn::Expr) -> darling::Result<Self> {
        match expr {
            syn::Expr::Path(expr_path) => Ok(FixturePath(expr_path.path.clone())),
            _ => Err(darling::Error::unexpected_expr_type(expr)),
        }
    }
}

pub(crate) fn db_arms(fixtures: &[FixturePath]) -> Vec<DbArm> {
    let fixture_builders = fixtures
        .iter()
        .map(|f| {
            let path = &f.0;
            quote! { <#path as ::td_schema::schema::Fixture>::build }
        })
        .collect::<Vec<_>>();
    // No fixtures → install the full schema; with fixtures, the fixture is
    // responsible for setting up its own tables.
    let install_schema = fixture_builders.is_empty();

    Backend::ALL
        .iter()
        .map(|&backend| {
            let provider = backend.provider_path();
            let fixture_builders = fixture_builders.clone();
            DbArm {
                backend,
                setup_expr: quote! {
                    ::td_test::db::DbTestSetup::<::td_test::db::#provider>::new(
                        #install_schema,
                        vec![#(#fixture_builders),*],
                    ).setup().await
                },
            }
        })
        .collect()
}

pub(crate) fn emit_db_wrapper(
    ctx: &EmitCtx<'_>,
    fn_arg: &(Ident, TypePath),
    arms: &[DbArm],
) -> TokenStream2 {
    let (arg_name, arg_ty) = fn_arg;
    let (ret_sig, ret_if_skip) = ret_sig_and_skip(ctx.ret_ty);
    let inner_name = format_ident!("__td_test_inner_{}", ctx.original_name);

    let EmitCtx {
        original_name,
        vis,
        attrs,
        body,
        ret_ty,
        tokio_attr,
    } = ctx;

    let test_fns = arms.iter().enumerate().map(|(i, arm)| {
        let test_name = if i == 0 {
            (*original_name).clone()
        } else {
            format_ident!("{}_{}", original_name, arm.backend.name())
        };
        let cfg_macro = format_ident!("__cfg_{}", arm.backend.name());
        let setup = &arm.setup_expr;
        let serial_attr = serial_attr_for(arm.backend);
        quote! {
            ::td_test::#cfg_macro! {
                #(#attrs)*
                #tokio_attr
                #serial_attr
                #vis async fn #test_name() #ret_sig {
                    use ::td_test::__private::TestSetup;
                    match #setup {
                        ::td_test::__private::TestSetupExecution::Skip => {
                            eprintln!("[td-test] SKIP: {}", stringify!(#test_name));
                            #ret_if_skip
                        }
                        ::td_test::__private::TestSetupExecution::Run(t) => {
                            #inner_name(t).await
                        }
                    }
                }
            }
        }
    });

    quote! {
        #(#attrs)*
        #[allow(dead_code)]
        async fn #inner_name(#arg_name: #arg_ty) #ret_ty
            #body

        #(#test_fns)*
    }
}

/// Bucketed file-serial attribute for backends that need it. The lock name
/// is `<backend>_<bucket>`, where bucket is round-robin from a per-process
/// counter (see `POSTGRES_BUCKET`).
fn serial_attr_for(backend: Backend) -> TokenStream2 {
    if !backend.needs_file_serial() {
        return quote! {};
    }
    let bucket = POSTGRES_BUCKET.fetch_add(1, Ordering::Relaxed) % *POSTGRES_PARALLELISM;
    let lock_name = format_ident!("{}_{}", backend.name(), bucket);
    quote! { #[::td_test::file_serial(#lock_name, crate = ::td_test::serial_test)] }
}
