//
// Copyright 2024 Tabs Data Inc.
//

extern crate proc_macro;

use darling::ast::NestedMeta;
use darling::{Error, FromMeta};
use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use quote::quote;
use syn::ItemFn;

const TABSDATALIB: &str = "tabsdatalib";

/// Macro to 'pause' a program until a condition is met.
/// Used mainly to wait until a debugger session is attached to program (especially when launched
/// as an external command or outside your IDE.
/// To use this function:
/// - Set environment variable TABSDATA_ATTACH to true/yes/1, or create a file with name .{signal} in
///   folder ${home}/.tabsdata/.attach/ with contents attach = true/yes/1.
/// - Add a breakpoint at the loop of this function.
/// - Start your process as normal.
/// - Attach a debugger to your process.
/// - When the breakpoint is hit, change the variable condition to exit the loop.
#[proc_macro_attribute]
pub fn attach(args: TokenStream, item: TokenStream) -> TokenStream {
    let attr_args = match NestedMeta::parse_meta_list(args.into()) {
        Ok(v) => v,
        Err(e) => {
            return TokenStream::from(Error::from(e).write_errors());
        }
    };
    let input = syn::parse_macro_input!(item as ItemFn);
    let args = match Arguments::from_list(&attr_args) {
        Ok(v) => v,
        Err(e) => {
            return TokenStream::from(e.write_errors());
        }
    };
    let func_body = &input.block;
    let func_sig = &input.sig;
    let signal = args.signal.to_string();
    let attach_path = match crate_name(TABSDATALIB) {
        Ok(FoundCrate::Itself) => quote!(crate::attach::wait_for_attach),
        Ok(FoundCrate::Name(external_name)) => {
            let external_name = syn::Ident::new(&external_name, proc_macro2::Span::call_site());
            quote!(#external_name::attach::wait_for_attach)
        }
        Err(_) => quote!(td_common::attach::wait_for_attach),
    };

    let expanded = quote! {
        #func_sig {
            #[cfg(debug_assertions)] {
                #attach_path(#signal);
            }
            #func_body
        }
    };
    TokenStream::from(expanded)
}

#[derive(Debug, FromMeta)]
struct Arguments {
    signal: String,
}
