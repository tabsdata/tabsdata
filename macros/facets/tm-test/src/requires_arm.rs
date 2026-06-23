//
// Copyright 2026 Tabs Data Inc.
//

//! `requires`-flavor emission: one test fn at module level with the original
//! name, no backend matrix. Runs only when the provider's env vars are
//! present; otherwise prints `SKIP` and returns the appropriate zero value
//! for the function's return type.

use proc_macro2::{Ident, TokenStream as TokenStream2};
use quote::quote;
use syn::TypePath;

use crate::common::{EmitCtx, ret_sig_and_skip};
use td_shared::meta_parser::SynMetaOrLit;

pub(crate) fn requires_setup_expr(
    test_name: &str,
    reqs: &Ident,
    env_prefix: &SynMetaOrLit,
) -> TokenStream2 {
    quote! {
        ::td_test::reqs::ReqsTestSetup::<#reqs>::new(#test_name, #env_prefix).setup().await
    }
}

pub(crate) fn emit_requires(
    ctx: &EmitCtx<'_>,
    fn_arg: &(Ident, TypePath),
    setup_expr: &TokenStream2,
) -> TokenStream2 {
    let (arg_name, arg_ty) = fn_arg;
    let (ret_sig, ret_if_skip) = ret_sig_and_skip(ctx.ret_ty);

    let EmitCtx {
        original_name,
        vis,
        attrs,
        body,
        tokio_attr,
        ..
    } = ctx;

    quote! {
        #(#attrs)*
        #tokio_attr
        #vis async fn #original_name() #ret_sig {
            use ::td_test::__private::TestSetup;

            async fn inner(#arg_name: #arg_ty) #ret_sig
                #body

            match #setup_expr {
                ::td_test::__private::TestSetupExecution::Skip => {
                    eprintln!("[td-test] SKIP: {}", stringify!(#original_name));
                    #ret_if_skip
                },
                ::td_test::__private::TestSetupExecution::Run(t, __td_teardown) => {
                    let __td_result = inner(t).await;
                    __td_teardown.await;
                    __td_result
                }
            }
        }
    }
}
