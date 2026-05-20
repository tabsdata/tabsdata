//
// Copyright 2026 Tabs Data Inc.
//

//! Entry point and argument parsing for `#[td_test::test(...)]`. The actual
//! per-flavor code emission lives in [`crate::db_arm`] and [`crate::requires_arm`].

extern crate proc_macro;

use darling::FromMeta;
use darling::ast::NestedMeta;
use darling::util::Flag;
use proc_macro::TokenStream;
use proc_macro2::{Ident, Span, TokenStream as TokenStream2};
use quote::quote;
use syn::ItemFn;
use td_shared::meta_parser::SynMetaOrLit;

use crate::common::{EmitCtx, get_fn_args};
use crate::db_arm::{FixturePath, db_arms, emit_db_wrapper};
use crate::requires_arm::{emit_requires, requires_setup_expr};

/// Top-level attribute arguments. The two test flavors (`db`, `requires`) are
/// modeled as siblings at the top level rather than nested variants — this
/// gives a flatter grammar (`#[td_test::test(db, fixture = Foo, flavor = "multi_thread")]`)
/// at the cost of needing a runtime check that exactly one of them is set.
#[derive(Debug, FromMeta)]
struct TestArgs {
    /// Marker for a db-backed test. Presence-only.
    #[darling(default)]
    db: Flag,
    /// Fixtures applied after schema upgrade. Only valid alongside `db`.
    #[darling(default, multiple, rename = "fixture")]
    fixtures: Vec<FixturePath>,

    /// External-resource requirements provider type. Presence selects the
    /// requires-flavor; the test runs only when its env vars are present.
    #[darling(default)]
    requires: Option<Ident>,
    /// Env-var prefix that scopes the requires lookup. Required when `requires`
    /// is set.
    #[darling(default)]
    env_prefix: Option<SynMetaOrLit>,
}

/// Names of attribute keys handled directly by this macro. Anything not in this
/// list is forwarded verbatim to `#[::tokio::test(...)]`, so e.g.
/// `flavor = "multi_thread"`, `worker_threads = N`, `start_paused = true`, or
/// any future tokio knob works without code changes here.
const TD_TEST_KEYS: &[&str] = &["db", "fixture", "requires", "env_prefix"];

pub fn scoped_test(args: TokenStream, item: TokenStream) -> TokenStream {
    match try_scoped_test(args, item) {
        Ok(ts) => ts.into(),
        Err(e) => e.write_errors().into(),
    }
}

fn try_scoped_test(args: TokenStream, item: TokenStream) -> darling::Result<TokenStream2> {
    let nested = NestedMeta::parse_meta_list(args.into())?;
    // Partition the attribute list: keys we recognize go through darling into
    // `TestArgs`; everything else is forwarded verbatim into `#[::tokio::test(...)]`.
    // This lets call sites use any tokio knob (current or future) without
    // touching this macro.
    let (td_meta, tokio_meta): (Vec<_>, Vec<_>) = nested.into_iter().partition(|n| match n {
        NestedMeta::Meta(m) => m
            .path()
            .get_ident()
            .is_some_and(|id| TD_TEST_KEYS.contains(&id.to_string().as_str())),
        NestedMeta::Lit(_) => false,
    });
    let args = TestArgs::from_list(&td_meta)?;
    let input: ItemFn = syn::parse(item).map_err(darling::Error::from)?;

    let tokio_attr = if tokio_meta.is_empty() {
        quote! { #[::tokio::test] }
    } else {
        quote! { #[::tokio::test(#(#tokio_meta),*)] }
    };

    let ctx = EmitCtx {
        original_name: &input.sig.ident,
        vis: &input.vis,
        attrs: &input.attrs,
        body: &input.block,
        ret_ty: &input.sig.output,
        tokio_attr: &tokio_attr,
    };

    match (args.db.is_present(), &args.requires) {
        (true, None) => {
            let fn_arg = get_fn_args(&input.sig, &Ident::new("TdPool", Span::call_site()))?;
            let arms = db_arms(&args.fixtures);
            Ok(emit_db_wrapper(&ctx, &fn_arg, &arms))
        }
        (false, Some(reqs)) => {
            let env_prefix = args.env_prefix.as_ref().ok_or_else(|| {
                darling::Error::custom(
                    "`env_prefix = \"...\"` is required when using `requires = ...`",
                )
            })?;
            if !args.fixtures.is_empty() {
                return Err(darling::Error::custom(
                    "`fixture = ...` is only valid with the `db` flavor",
                ));
            }
            let fn_arg = get_fn_args(&input.sig, reqs)?;
            let setup_expr = requires_setup_expr(&ctx.original_name.to_string(), reqs, env_prefix);
            Ok(emit_requires(&ctx, &fn_arg, &setup_expr))
        }
        (true, Some(_)) => Err(darling::Error::custom(
            "`db` and `requires = ...` are mutually exclusive — pick one",
        )),
        (false, None) => Err(darling::Error::custom(
            "must specify either `db` or `requires = <Type>`",
        )),
    }
}
