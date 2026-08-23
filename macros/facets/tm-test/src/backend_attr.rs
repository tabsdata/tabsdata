//
// Copyright 2026 Tabsdata Inc.
//

//! `#[td_test::domain("<name>")]` / `#[td_test::system("<name>")]`
//!
//! Each attribute names a backend in the tdx store, claims the parameter called
//! `domain_<name>` / `system_<name>`, resolves the backend into it, and hands the
//! rest of the function on untouched. That is the Python fixtures' naming, where
//! `@s3_domain("orders")` injects `domain_orders`.
//! Stacking is therefore just nesting: the outermost attribute expands first,
//! strips its parameter, and re-emits the attributes below it — so the test
//! attribute at the bottom (`#[test]`, `#[tokio::test]`, …) finally sees a
//! zero-argument function, which is the only shape it accepts.
//!
//! Nothing here is async: resolution is a file read and a key derivation, so the
//! same emission serves sync and async tests.

use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::spanned::Spanned;
use syn::{FnArg, Ident, ItemFn, LitStr, Pat, Type};

use crate::common::ret_sig_and_skip;

/// Which store a given attribute reads, and the parameter prefix it requires.
#[derive(Clone, Copy)]
pub(crate) enum Role {
    Domain,
    System,
}

impl Role {
    fn prefix(self) -> &'static str {
        match self {
            Role::Domain => "domain_",
            Role::System => "system_",
        }
    }

    fn attr(self) -> &'static str {
        match self {
            Role::Domain => "domain",
            Role::System => "system",
        }
    }

    fn resolver(self) -> Ident {
        let name = match self {
            Role::Domain => "resolve_domain",
            Role::System => "resolve_system",
        };
        Ident::new(name, Span::call_site())
    }
}

pub(crate) fn backend_attr(
    role: Role,
    args: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    match try_backend_attr(role, args, item) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn try_backend_attr(
    role: Role,
    args: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> syn::Result<TokenStream2> {
    let binding: LitStr = syn::parse(args).map_err(|_| {
        syn::Error::new(
            Span::call_site(),
            format!(
                "expected the backend name as a string, e.g. \
                 #[td_test::{attr}(\"orders\")]",
                attr = role.attr(),
            ),
        )
    })?;
    let mut input: ItemFn = syn::parse(item)?;

    let store_name = binding.value();
    if store_name.is_empty() {
        return Err(syn::Error::new(
            binding.span(),
            format!(
                "names no backend — expected the name it has in the tdx store, e.g. \
                 #[td_test::{attr}(\"orders\")]",
                attr = role.attr(),
            ),
        ));
    }
    // The prefix belongs to the parameter, not to the argument. Catching it here
    // beats letting it through and reporting a missing `domain_domain_orders`.
    if let Some(bare) = store_name.strip_prefix(role.prefix()) {
        return Err(syn::Error::new(
            binding.span(),
            format!(
                "'{store_name}' carries the '{prefix}' prefix — pass the backend name on its \
                 own ('{bare}'); the parameter it fills is '{prefix}{bare}'",
                prefix = role.prefix(),
            ),
        ));
    }
    // The parameter the backend is bound to, named as the Python fixtures are.
    let name = format!("{}{store_name}", role.prefix());

    let (arg_name, arg_ty) = take_arg(&mut input, &name, role)?;
    // The signature is re-emitted verbatim (minus the claimed parameter), so only
    // the skip-path value is needed: `()` for a plain test, `Ok(())` for a
    // fallible one.
    let (_, ret_if_skip) = ret_sig_and_skip(&input.sig.output);

    let test_name = input.sig.ident.to_string();
    let resolver = role.resolver();
    let attrs = &input.attrs;
    let vis = &input.vis;
    let sig = &input.sig;
    let body = &input.block;

    Ok(quote! {
        #(#attrs)*
        #vis #sig {
            let #arg_name: #arg_ty = match ::td_test::domain::#resolver(#store_name, #test_name) {
                ::std::option::Option::Some(__td_backend) => __td_backend,
                ::std::option::Option::None => return #ret_if_skip,
            };
            #body
        }
    })
}

/// Remove the parameter called `name` from `input`'s signature and return its
/// identifier and type. Erroring here is what turns a typo into a pointed
/// message instead of "the test function cannot accept arguments" from whatever
/// test attribute sits below.
fn take_arg(input: &mut ItemFn, name: &str, role: Role) -> syn::Result<(Ident, Type)> {
    let found = input.sig.inputs.iter().position(|arg| match arg {
        FnArg::Typed(pat_type) => match &*pat_type.pat {
            Pat::Ident(pat_ident) => pat_ident.ident == name,
            _ => false,
        },
        FnArg::Receiver(_) => false,
    });
    let Some(index) = found else {
        let declared: Vec<String> = input
            .sig
            .inputs
            .iter()
            .filter_map(|arg| match arg {
                FnArg::Typed(pat_type) => match &*pat_type.pat {
                    Pat::Ident(pat_ident) => Some(pat_ident.ident.to_string()),
                    _ => None,
                },
                FnArg::Receiver(_) => None,
            })
            .collect();
        let has = if declared.is_empty() {
            "it takes none".to_string()
        } else {
            format!("it takes: {}", declared.join(", "))
        };
        return Err(syn::Error::new(
            input.sig.inputs.span(),
            format!(
                "#[td_test::{attr}(\"{store}\")] found no parameter called '{name}' — {has}",
                attr = role.attr(),
                store = name.strip_prefix(role.prefix()).unwrap_or(name),
            ),
        ));
    };
    let arg = input.sig.inputs.iter().nth(index).cloned().unwrap();
    let FnArg::Typed(pat_type) = arg else {
        unreachable!("filtered to typed args above");
    };
    let Pat::Ident(pat_ident) = &*pat_type.pat else {
        unreachable!("filtered to ident patterns above");
    };
    let ident = pat_ident.ident.clone();
    let ty = (*pat_type.ty).clone();

    let mut kept = syn::punctuated::Punctuated::new();
    for (i, arg) in input.sig.inputs.iter().enumerate() {
        if i != index {
            kept.push(arg.clone());
        }
    }
    input.sig.inputs = kept;
    Ok((ident, ty))
}
