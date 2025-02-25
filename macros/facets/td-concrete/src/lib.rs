//
// Copyright 2024 Tabs Data Inc.
//

extern crate proc_macro;
use crate::concrete::generic_remover;
use proc_macro::TokenStream;

mod concrete;

#[proc_macro_attribute]
pub fn concrete(args: TokenStream, item: TokenStream) -> TokenStream {
    generic_remover(args, item)
}
