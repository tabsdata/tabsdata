//
// Copyright 2026 Tabsdata Inc.
//

//! `db`-flavor emission: one `#[::tokio::test]` with the user's body hoisted
//! into a shared `__td_test_inner_<name>` helper at the parent module level, so
//! the helper retains access to private items in the same `mod tests`.

use darling::FromMeta;
use darling::ast::NestedMeta;
use proc_macro2::{Ident, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::TypePath;

use crate::common::{EmitCtx, ret_sig_and_skip};

/// What a `db` test provisions, parsed from `db` (bare) or
/// `db(Type, ..., domain = "name")`. No types means the default apiserver
/// component; otherwise each listed type's pool is provisioned (on its own
/// schema) and injected in order. `domain` names the tdd domain whose database
/// the schemas go in, for a test that needs what only that domain has.
#[derive(Debug, Clone, Default)]
pub(crate) struct DbComponents {
    paths: Vec<syn::Path>,
    domain: Option<String>,
}

impl FromMeta for DbComponents {
    /// Bare `db` — default to the apiserver component.
    fn from_word() -> darling::Result<Self> {
        Ok(Self::default())
    }
    /// `db(TypeA, TypeB, domain = "name")` — one component type path per pool,
    /// and at most one domain.
    fn from_list(items: &[NestedMeta]) -> darling::Result<Self> {
        let mut db = Self::default();
        for item in items {
            match item {
                NestedMeta::Meta(syn::Meta::Path(p)) => db.paths.push(p.clone()),
                NestedMeta::Meta(syn::Meta::NameValue(nv)) if nv.path.is_ident("domain") => {
                    if db.domain.is_some() {
                        return Err(darling::Error::duplicate_field("domain").with_span(nv));
                    }
                    let domain = String::from_expr(&nv.value)?;
                    if domain.trim().is_empty() {
                        return Err(darling::Error::custom(
                            "names no domain — expected its name in the tdd store, e.g. \
                             `db(domain = \"apiserver_postgres_own\")`",
                        )
                        .with_span(&nv.value));
                    }
                    db.domain = Some(domain);
                }
                other => {
                    return Err(darling::Error::custom(
                        "expected a component type or `domain = \"...\"`, e.g. \
                         `db(AiAgentComponent)`",
                    )
                    .with_span(other));
                }
            }
        }
        Ok(db)
    }
}

impl DbComponents {
    /// Resolved component type paths — the default apiserver component when the
    /// list is empty.
    pub(crate) fn paths(&self) -> Vec<TokenStream2> {
        if self.paths.is_empty() {
            vec![quote! { ::td_database::components::ApiserverComponent }]
        } else {
            self.paths.iter().map(|p| quote! { #p }).collect()
        }
    }

    pub(crate) fn domain(&self) -> Option<&str> {
        self.domain.as_deref()
    }
}

/// The setup expression: provision `components` for `test_name`, in `domain`'s
/// database when the test names one.
pub(crate) fn db_setup(
    test_name: &str,
    components: &[TokenStream2],
    domain: Option<&str>,
) -> TokenStream2 {
    let components = components.to_vec();
    let domain = match domain {
        Some(name) => quote! { ::std::option::Option::Some(#name) },
        None => quote! { ::std::option::Option::None },
    };
    // A `const` binds the component list to `'static` (each `&Component`
    // const-promotes), satisfying the harness's `&'static [...]` input.
    quote! {
        {
            const __TD_TEST_COMPONENTS:
                &[&dyn ::td_database::components::SchemaComponent] =
                &[#(& #components),*];
            ::td_test::db::DbTestSetup::new(#test_name, __TD_TEST_COMPONENTS, #domain)
                .setup()
                .await
        }
    }
}

pub(crate) fn emit_db_wrapper(
    ctx: &EmitCtx<'_>,
    fn_args: &[(Ident, TypePath)],
    setup: &TokenStream2,
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
    let arg_takes: Vec<_> = fn_args
        .iter()
        .map(|_| quote! { __td_test_pools.next().expect("td-test: missing serving pool") })
        .collect();

    quote! {
        #(#attrs)*
        #[allow(dead_code)]
        async fn #inner_name(#(#arg_decls),*) #ret_ty
            #body

        #(#attrs)*
        #tokio_attr
        #vis async fn #original_name() #ret_sig {
            use ::td_test::__private::TestSetup;
            match #setup {
                ::td_test::__private::TestSetupExecution::Skip => {
                    eprintln!("[td-test] SKIP: {}", stringify!(#original_name));
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
