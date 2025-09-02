//
// Copyright 2024 Tabs Data Inc.
//

extern crate proc_macro;
use proc_macro::TokenStream;

use crate::attributes::utoipa_path;

mod attributes;
mod router_ext;

#[proc_macro_attribute]
pub fn apiserver_path(args: TokenStream, item: TokenStream) -> TokenStream {
    // Same as utoipa::path, but with some extra logic to extract types from Axum handlers
    utoipa_path(args, item)
}

#[proc_macro_attribute]
pub fn router_ext(attr: TokenStream, item: TokenStream) -> TokenStream {
    router_ext::router_ext(attr, item)
}
