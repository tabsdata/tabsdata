//
// Copyright 2026 Tabsdata Inc.
//

//! `db`-flavor emission: one `#[::tokio::test]` per backend, with the user's
//! body hoisted into a shared `__td_test_inner_<name>` helper at the parent
//! module level so per-backend arms don't duplicate the body and the helper
//! retains access to private items in the same `mod tests`.

use darling::FromMeta;
use darling::ast::NestedMeta;
use proc_macro2::{Ident, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::TypePath;

use crate::common::{EmitCtx, ret_sig_and_skip};

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
}

/// Per-backend arm: a precomputed setup expression tied to which backend
/// produced it. Gated at the call site by `::td_test::__cfg_<name>!` rather
/// than `#[cfg(<name>)]` so downstream crates don't need a build.rs
/// trampoline to declare the backend cfgs.
pub(crate) struct DbArm {
    pub(crate) backend: Backend,
    pub(crate) setup_expr: TokenStream2,
}

/// The components a `db` test provisions, parsed from `db` (bare) or
/// `db(Type, ...)`. Empty means the default apiserver component; otherwise each
/// listed type's pool is provisioned (on its own schema) and injected in order.
#[derive(Debug, Clone, Default)]
pub(crate) struct DbComponents(Vec<syn::Path>);

impl FromMeta for DbComponents {
    /// Bare `db` — default to the apiserver component.
    fn from_word() -> darling::Result<Self> {
        Ok(Self::default())
    }
    /// `db(TypeA, TypeB)` — one component type path per pool.
    fn from_list(items: &[NestedMeta]) -> darling::Result<Self> {
        let paths = items
            .iter()
            .map(|item| match item {
                NestedMeta::Meta(syn::Meta::Path(p)) => Ok(p.clone()),
                other => Err(darling::Error::custom(
                    "expected a component type, e.g. `db(AiAgentComponent)`",
                )
                .with_span(other)),
            })
            .collect::<darling::Result<Vec<_>>>()?;
        Ok(Self(paths))
    }
}

impl DbComponents {
    /// Resolved component type paths — the default apiserver component when the
    /// list is empty.
    pub(crate) fn paths(&self) -> Vec<TokenStream2> {
        if self.0.is_empty() {
            vec![quote! { ::td_database::components::ApiserverComponent }]
        } else {
            self.0.iter().map(|p| quote! { #p }).collect()
        }
    }
}

pub(crate) fn db_arms(test_name: &str, components: &[TokenStream2]) -> Vec<DbArm> {
    Backend::ALL
        .iter()
        .map(|&backend| {
            let provider = backend.provider_path();
            let components = components.to_vec();
            DbArm {
                backend,
                // A `const` binds the component list to `'static` (each `&Component`
                // const-promotes), satisfying the harness's `&'static [...]` input.
                setup_expr: quote! {
                    {
                        const __TD_TEST_COMPONENTS:
                            &[&dyn ::td_database::components::SchemaComponent] =
                            &[#(& #components),*];
                        ::td_test::db::DbTestSetup::<::td_test::db::#provider>::new(
                            #test_name,
                            __TD_TEST_COMPONENTS,
                        ).setup().await
                    }
                },
            }
        })
        .collect()
}

pub(crate) fn emit_db_wrapper(
    ctx: &EmitCtx<'_>,
    fn_args: &[(Ident, TypePath)],
    arms: &[DbArm],
) -> TokenStream2 {
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

    let arg_decls = fn_args.iter().map(|(name, ty)| quote! { #name: #ty });
    // One serving pool per declared arg, drawn from the harness's ordered vec.
    let arg_takes = fn_args.iter().map(|_| {
        quote! { __td_test_pools.next().expect("td-test: missing serving pool") }
    });
    let arg_takes: Vec<_> = arg_takes.collect();

    let test_fns = arms.iter().enumerate().map(|(i, arm)| {
        let test_name = if i == 0 {
            (*original_name).clone()
        } else {
            format_ident!("{}_{}", original_name, arm.backend.name())
        };
        let cfg_macro = format_ident!("__cfg_{}", arm.backend.name());
        let setup = &arm.setup_expr;
        let arg_takes = arg_takes.clone();
        quote! {
            ::td_test::#cfg_macro! {
                #(#attrs)*
                #tokio_attr
                #vis async fn #test_name() #ret_sig {
                    use ::td_test::__private::TestSetup;
                    match #setup {
                        ::td_test::__private::TestSetupExecution::Skip => {
                            eprintln!("[td-test] SKIP: {}", stringify!(#test_name));
                            #ret_if_skip
                        }
                        ::td_test::__private::TestSetupExecution::Run(__td_test_pools, __td_teardown) => {
                            let mut __td_test_pools = __td_test_pools.into_iter();
                            let __td_result = #inner_name(#(#arg_takes),*).await;
                            // Release this test's database resources (drop its
                            // schemas) before returning its result.
                            __td_teardown.await;
                            __td_result
                        }
                    }
                }
            }
        }
    });

    quote! {
        #(#attrs)*
        #[allow(dead_code)]
        async fn #inner_name(#(#arg_decls),*) #ret_ty
            #body

        #(#test_fns)*
    }
}
