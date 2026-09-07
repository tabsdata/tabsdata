//
// Copyright 2024 Tabsdata Inc.
//

extern crate proc_macro;
use proc_macro::TokenStream;

use crate::attributes::utoipa_path;

mod attributes;
mod mount;
mod word;

#[proc_macro_attribute]
pub fn apiserver_path(args: TokenStream, item: TokenStream) -> TokenStream {
    // Same as utoipa::path, but with some extra logic to extract types from Axum handlers
    utoipa_path(args, item)
}
