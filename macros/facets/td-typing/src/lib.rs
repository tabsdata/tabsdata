//
// Copyright 2024 Tabs Data Inc.
//

extern crate proc_macro;
use proc_macro::TokenStream;

use crate::service_type::service_type_impl;

mod service_type;

#[proc_macro_attribute]
pub fn service_type(args: TokenStream, item: TokenStream) -> TokenStream {
    service_type_impl(args, item)
}
