//
// Copyright 2025 Tabs Data Inc.
//

extern crate proc_macro;

use darling::FromMeta;
use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use quote::quote;
use std::path::PathBuf;
use syn::{parse_macro_input, FnArg, ItemFn, LitStr, Signature, Type, TypePath};
use td_shared::meta_parser::{some_or_none, OptionWrapper, SynMetaOrLit};
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
        #[tokio::test]
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

#[derive(Debug, Default, FromMeta)]
pub struct SqlxArguments {
    migrator: Option<SynMetaOrLit>,
    #[darling(multiple, rename = "fixture")]
    fixtures: Vec<LitStr>,
}

fn sqlx_test(func_sig: &Signature, args: Option<SqlxArguments>) -> InnerFnSetup {
    let (migrator, fixtures) = match args {
        Some(args) => {
            let migrator = some_or_none(args.migrator);
            let fixtures = args
                .fixtures
                .iter()
                .map(|f| {
                    PathBuf::new()
                        .join("fixtures")
                        .join(f.value())
                        .with_extension("sql")
                })
                .map(|f| {
                    let f = f.to_string_lossy();
                    quote! { include_str!(#f) }
                })
                .collect::<Vec<_>>();
            (migrator, fixtures)
        }
        None => (quote! { None }, vec![]),
    };

    // Check if the function has a DbPool parameter
    // Get fn arg and its type
    let (name, ty) = get_fn_args(func_sig, &Ident::new("DbPool", Span::call_site()));

    let setup = quote! {
        td_test::sqlx::SqlxTestSetup::new(#migrator, vec![#(#fixtures),*]).setup().await
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
}

fn reqs_test(func_sig: &Signature, args: ReqsArguments) -> InnerFnSetup {
    let reqs = &args.reqs;
    let env_prefix = &args.env_prefix;
    let test_name = &func_sig.ident.to_string();

    // Get fn arg and its type
    let (name, ty) = get_fn_args(func_sig, reqs);

    let setup = quote! {
        td_test::reqs::ReqsTestSetup::<#reqs>::new(#test_name, #env_prefix).setup().await
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
            if let FnArg::Typed(pat_type) = arg {
                if let Type::Path(type_path) = &*pat_type.ty {
                    if type_path
                        .path
                        .segments
                        .iter()
                        .any(|segment| segment.ident == *kind)
                    {
                        if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
                            return Some((pat_ident.ident.clone(), type_path.clone()));
                        }
                    }
                }
            }
            None
        })
        .unwrap_or_else(|| panic!("Function must have a {kind} parameter"))
}
