//
// Copyright 2026 Tabs Data Inc.
//

//! Helpers shared between the `db` and `requires` arm emitters.

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

/// Build the return-type signature and skip-path expression for an emitted
/// test wrapper. Unit-returning tests use the default (no `->`) signature so
/// `#[file_serial(...)]` introspects them as unit-typed; writing `-> ()`
/// explicitly trips file_serial into picking its Result-typed helper, which
/// then fails to type-check.
pub(crate) fn ret_sig_and_skip(ret_ty: &syn::ReturnType) -> (TokenStream2, TokenStream2) {
    match ret_ty {
        syn::ReturnType::Default => (quote! {}, quote! {}),
        ret => (quote! { #ret }, quote! { Ok(()) }),
    }
}
