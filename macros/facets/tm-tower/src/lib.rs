//
// Copyright 2025 Tabs Data Inc.
//

extern crate proc_macro;
use proc_macro::TokenStream;

mod layer;
mod provider;

#[proc_macro_attribute]
pub fn layer(args: TokenStream, item: TokenStream) -> TokenStream {
    // Alias to utoipa_path, used to find ApiServer paths
    layer::layer(args, item)
}

#[proc_macro_attribute]
pub fn provider(args: TokenStream, item: TokenStream) -> TokenStream {
    // Alias to utoipa_path, used to find ApiServer paths
    provider::provider(args, item)
}
