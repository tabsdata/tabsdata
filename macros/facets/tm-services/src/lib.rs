//
// Copyright 2025 Tabs Data Inc.
//

extern crate proc_macro;
use proc_macro::TokenStream;

mod derive_field_accessor;
mod derive_service_factory;
mod service_factory;

#[proc_macro_attribute]
pub fn service_factory(args: TokenStream, item: TokenStream) -> TokenStream {
    // Alias to utoipa_path, used to find ApiServer paths
    service_factory::service_factory(args, item)
}

#[proc_macro_derive(ServiceFactory)]
pub fn derive_service_factory(input: TokenStream) -> TokenStream {
    derive_service_factory::derive_service_factory(input)
}

#[proc_macro_derive(FieldAccessors, attributes(field_accessor))]
pub fn derive_from_ref(input: TokenStream) -> TokenStream {
    derive_field_accessor::derive_field_accessor(input)
}
