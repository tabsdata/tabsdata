//
// Copyright 2025 Tabs Data Inc.
//

extern crate proc_macro;
use proc_macro::TokenStream;

mod derive_service_factory;
mod layer;
mod service_factory;

#[proc_macro_attribute]
pub fn layer(args: TokenStream, item: TokenStream) -> TokenStream {
    // Alias to utoipa_path, used to find ApiServer paths
    layer::layer(args, item)
}

#[proc_macro_attribute]
pub fn service_factory(args: TokenStream, item: TokenStream) -> TokenStream {
    // Alias to utoipa_path, used to find ApiServer paths
    service_factory::service_factory(args, item)
}

#[proc_macro_derive(ServiceFactory)]
pub fn derive_service_factory(input: TokenStream) -> TokenStream {
    derive_service_factory::derive_service_factory(input)
}
