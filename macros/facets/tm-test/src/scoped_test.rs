//
// Copyright 2025 Tabs Data Inc.
//

extern crate proc_macro;

use darling::FromMeta;
use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use quote::quote;
use syn::{FnArg, ItemFn, Signature, Type, TypePath, parse_macro_input};
use td_shared::meta_parser::{OptionWrapper, SynMetaOrLit};
use td_shared::parse_meta;

#[derive(Debug, FromMeta)]
#[allow(non_camel_case_types)]
struct TestType {
    #[darling(flatten)]
    typed: Typed,
}

#[derive(Debug, FromMeta)]
#[allow(non_camel_case_types)]
enum Typed {
    #[darling(rename = "sqlx")]
    Sqlx(OptionWrapper<SqlxArguments>),
    #[darling(rename = "when")]
    When(ReqsArguments),
}

struct InnerFnSetup {
    fn_args: Vec<(Ident, TypePath)>,
    setup: proc_macro2::TokenStream,
}

pub fn scoped_test(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_meta!(TestType, args).unwrap();
    let input = parse_macro_input!(item as ItemFn);

    let func_vis = &input.vis;
    let func_sig = &input.sig;
    let func_body = &input.block;
    let func_attrs = &input.attrs;

    // Test setup generation
    let fn_setup = match args.typed {
        Typed::Sqlx(t) => sqlx_test(func_sig, t.into()),
        Typed::When(t) => reqs_test(func_sig, t),
    };
    let (arg_names, arg_types): (Vec<_>, Vec<_>) = fn_setup
        .fn_args
        .iter()
        .map(|(name, ty)| (name, ty))
        .collect();
    let test_setup = &fn_setup.setup;

    // Generate the expanded test function
    // Remove all arguments from the function signature and propagate return type
    let mut func_sig = func_sig.clone();
    func_sig.inputs.clear();
    let ret_ty = &func_sig.output;
    let (ret_sig, ret_if_skip) = if let syn::ReturnType::Default = ret_ty {
        (quote! { -> () }, quote! {})
    } else {
        // Only Result<(), Err> is supported
        (quote! { #ret_ty }, quote! { Ok(()) })
    };

    let test_name = &func_sig.ident;
    let expanded = quote! {
        #(#func_attrs)*
        #func_vis #func_sig {
            use td_test::TestSetup;

            async fn inner(#(#arg_names: #arg_types),*) #ret_sig
                #func_body

            match #test_setup {
                td_test::TestSetupExecution::Skip => {
                    println!("Test {} skipped", stringify!(#test_name));
                    #ret_if_skip
                },
                td_test::TestSetupExecution::Run(t) => {
                    inner(t).await
                }
            }
        }
    };

    expanded.into()
}

/// A wrapper around `syn::Path` that implements `FromMeta` for darling parsing.
///
/// Allows `fixture = some::path::Type` in proc macro attributes.
#[derive(Debug, Clone)]
struct FixturePath(syn::Path);

impl FromMeta for FixturePath {
    fn from_expr(expr: &syn::Expr) -> darling::Result<Self> {
        match expr {
            syn::Expr::Path(expr_path) => Ok(FixturePath(expr_path.path.clone())),
            _ => Err(darling::Error::unexpected_expr_type(expr)),
        }
    }
}

#[derive(Debug, Default, FromMeta)]
pub struct SqlxArguments {
    #[darling(multiple, rename = "fixture")]
    fixtures: Vec<FixturePath>,
}

fn sqlx_test(func_sig: &Signature, args: Option<SqlxArguments>) -> InnerFnSetup {
    let (install_schema, fixtures) = match args {
        Some(args) => {
            let fixtures = args
                .fixtures
                .iter()
                .map(|f| {
                    let path = &f.0;
                    quote! { <#path as td_schema::schema::Fixture>::build }
                })
                .collect::<Vec<_>>();
            // Install schema if no fixtures provided
            let install_schema = fixtures.is_empty();
            (install_schema, fixtures)
        }
        None => (true, vec![]),
    };

    // Check if the function has a TdPool parameter
    // Get fn arg and its type
    let (name, ty) = get_fn_args(func_sig, &Ident::new("TdPool", Span::call_site()));

    let setup = quote! {
        td_test::sqlx::SqlxTestSetup::new(#install_schema, vec![#(#fixtures),*]).setup().await
    };

    InnerFnSetup {
        fn_args: vec![(name, ty)],
        setup,
    }
}

#[derive(Debug, FromMeta)]
pub struct ReqsArguments {
    reqs: Ident,
    env_prefix: SynMetaOrLit,
    #[darling(default)]
    do_not_fail_reqs: bool, // only used for unit testing of the skip functionality
}

fn reqs_test(func_sig: &Signature, args: ReqsArguments) -> InnerFnSetup {
    let reqs = &args.reqs;
    let env_prefix = &args.env_prefix;
    let do_not_fail = args.do_not_fail_reqs;
    let test_name = &func_sig.ident.to_string();

    // Get fn arg and its type
    let (name, ty) = get_fn_args(func_sig, reqs);

    let setup = quote! {
        td_test::reqs::ReqsTestSetup::<#reqs>::new(#test_name, #env_prefix, #do_not_fail).setup().await
    };

    InnerFnSetup {
        fn_args: vec![(name, ty)],
        setup,
    }
}

fn get_fn_args(func_sig: &Signature, kind: &Ident) -> (Ident, TypePath) {
    func_sig
        .inputs
        .iter()
        .find_map(|arg| {
            if let FnArg::Typed(pat_type) = arg
                && let Type::Path(type_path) = &*pat_type.ty
                && type_path
                    .path
                    .segments
                    .iter()
                    .any(|segment| segment.ident == *kind)
                && let syn::Pat::Ident(pat_ident) = &*pat_type.pat
            {
                return Some((pat_ident.ident.clone(), type_path.clone()));
            }
            None
        })
        .unwrap_or_else(|| panic!("Function must have a {kind} parameter"))
}
