//
// Copyright 2025 Tabs Data Inc.
//

extern crate proc_macro;

use darling::FromMeta;
use proc_macro::TokenStream;
use quote::quote;
use std::path::PathBuf;
use syn::{parse_macro_input, FnArg, ItemFn, LitStr, Type};
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
}

#[derive(Debug, Default, FromMeta)]
struct SqlxArguments {
    migrator: Option<SynMetaOrLit>,
    #[darling(multiple, rename = "fixture")]
    fixtures: Vec<LitStr>,
}

pub fn scoped_test(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_meta!(TestType, args).unwrap();
    let input = parse_macro_input!(item as ItemFn);

    match args.typed {
        Typed::Sqlx(t) => sqlx_test(&input, t.into()),
    }
}

fn sqlx_test(input: &ItemFn, args: Option<SqlxArguments>) -> TokenStream {
    let func_vis = &input.vis;
    let func_sig = &input.sig;
    let func_body = &input.block;
    let func_attrs = &input.attrs;

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
        None => (quote! {}, vec![]),
    };

    // Check if the function has a DbPool parameter
    let (db, db_ty) = func_sig
        .inputs
        .iter()
        .find_map(|arg| {
            if let FnArg::Typed(pat_type) = arg {
                if let Type::Path(type_path) = &*pat_type.ty {
                    if type_path
                        .path
                        .segments
                        .iter()
                        .any(|segment| segment.ident == "DbPool")
                    {
                        if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
                            return Some((pat_ident.ident.clone(), type_path.clone()));
                        }
                    }
                }
            }
            None
        })
        .unwrap_or_else(|| panic!("Function must have a DbPool parameter"));

    // Remove all arguments from the function signature and propagate return type
    let mut func_sig = func_sig.clone();
    func_sig.inputs.clear();
    let ret_sig = if let syn::ReturnType::Default = &func_sig.output {
        quote! { -> () }
    } else {
        let ret_ty = &func_sig.output;
        quote! { #ret_ty }
    };

    let expanded = quote! {
        #[tokio::test]
        #(#func_attrs)*
        #func_vis #func_sig {
            async fn inner(#db: #db_ty) #ret_sig
                #func_body

            let db = td_test::sqlx::setup_test_db(
                #migrator, vec![#(#fixtures),*]
            ).await;
            inner(#db).await
        }
    };

    expanded.into()
}
