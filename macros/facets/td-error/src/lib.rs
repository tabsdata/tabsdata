//
// Copyright 2024 Tabs Data Inc.
//

extern crate proc_macro;
use crate::td_error::td_error_impl;
use proc_macro::TokenStream;

mod td_error;

/// Macro that generates required impls for a tabsdata error enum.
#[proc_macro_attribute]
pub fn td_error(_args: TokenStream, item: TokenStream) -> TokenStream {
    td_error_impl(item)
}
