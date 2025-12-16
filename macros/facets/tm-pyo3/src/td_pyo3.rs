//
// Copyright 2025 Tabs Data Inc.
//

use proc_macro::TokenStream;
use quote::quote;
use syn::{Ident, ItemConst, parse_macro_input};

/// Attribute macro that exports a const as a Python module variable.
///
/// # Usage
///
/// ```rust
/// const MODULE_PATH: &str = "module.path";
///
/// #[pyconstant(MODULE_PATH)]
/// pub const CONSTANT: &str = "constant";
/// ```
///
/// This expands to:
///
/// ```rust
/// pub const CONSTANT: &str = "constant";
/// module_variable!(
///     MODULE_PATH,
///     "CONSTANT",
///     &'static str,
///     CONSTANT
/// );
/// ```
pub fn pyconstant_impl(args: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the module path constant identifier from the attribute arguments.
    let module_path_ident = parse_macro_input!(args as Ident);

    // Parse the const item.
    let const_item = parse_macro_input!(item as ItemConst);

    // Extract information from the const item.
    let vis = &const_item.vis;
    let const_token = &const_item.const_token;
    let ident = &const_item.ident;
    let ty = &const_item.ty;
    let expr = &const_item.expr;

    // Convert the const name to a string literal for the module_variable! macro.
    let ident_str = ident.to_string();

    // Generate the expanded code.
    let expanded = quote! {
        #vis #const_token #ident: #ty = #expr;
        module_variable!(
            #module_path_ident,
            #ident_str,
            #ty,
            #ident
        );
    };

    TokenStream::from(expanded)
}
