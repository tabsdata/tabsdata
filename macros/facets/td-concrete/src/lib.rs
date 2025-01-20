//
// Copyright 2024 Tabs Data Inc.
//

extern crate proc_macro;
use proc_macro::TokenStream;

use crate::concrete::generic_remover;

mod concrete;

#[proc_macro_attribute]
pub fn concrete(args: TokenStream, item: TokenStream) -> TokenStream {
    generic_remover(args, item)
}
