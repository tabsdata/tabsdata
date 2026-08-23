//
// Copyright 2026 Tabsdata Inc.
//

//! Helpers shared between the `db`, `requires` and `domain` / `system` arm
//! emitters.

use proc_macro2::{Ident, TokenStream as TokenStream2};
use quote::quote;
use syn::{FnArg, Signature, Type, TypePath};

/// Per-test bag of inputs shared by both arm emitters. Built once in
/// [`crate::scoped_test::scoped_test`] so each emitter doesn't take a 9-arg
/// signature.
pub(crate) struct EmitCtx<'a> {
    pub original_name: &'a Ident,
    pub vis: &'a syn::Visibility,
    pub attrs: &'a [syn::Attribute],
    pub body: &'a syn::Block,
    pub ret_ty: &'a syn::ReturnType,
    pub tokio_attr: &'a TokenStream2,
}

/// Collect every function parameter whose type path contains `kind` as a
/// segment, in declaration order, as `(param_name, param_type)`. Used by the db
/// flavor, where the test takes one pool per requested component.
pub(crate) fn get_all_fn_args(func_sig: &Signature, kind: &Ident) -> Vec<(Ident, TypePath)> {
    func_sig
        .inputs
        .iter()
        .filter_map(|arg| {
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
        .collect()
}

/// Find the first function parameter whose type path contains `kind` as a
/// segment and return `(param_name, param_type)`.
pub(crate) fn get_fn_args(
    func_sig: &Signature,
    kind: &Ident,
) -> darling::Result<(Ident, TypePath)> {
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
        .ok_or_else(|| {
            darling::Error::custom(format!("function must have a `{kind}` parameter"))
                .with_span(&func_sig.inputs)
        })
}

/// Build the return-type signature and the skip-path return expression for an
/// emitted test wrapper, which must echo the inner fn's return type: a
/// unit-returning test gets no `->` and an empty skip expression; a fallible
/// test keeps its `-> Result<...>` and skips with `Ok(())`.
pub(crate) fn ret_sig_and_skip(ret_ty: &syn::ReturnType) -> (TokenStream2, TokenStream2) {
    match ret_ty {
        syn::ReturnType::Default => (quote! {}, quote! {}),
        ret => (quote! { #ret }, quote! { Ok(()) }),
    }
}

/// Emit a test wrapper around a setup that either yields one injected value or
/// skips. Shared by the `requires` and `domain` / `system` flavors, which differ
/// only in the `TestSetup` they build.
pub(crate) fn emit_injected_arg(
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
