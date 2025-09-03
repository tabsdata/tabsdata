//
// Copyright 2025 Tabs Data Inc.
//

use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemStruct, parse_macro_input};

pub fn url_param(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);

    if !input.generics.params.is_empty() {
        panic!("the struct must not have generics");
    }

    let expanded = quote! {
        #[derive(Debug, Clone, Eq, PartialEq, td_type::DtoType, utoipa::IntoParams, derive_builder::Builder, getset::Getters, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
        #[builder(try_setter, setter(into))]
        #[getset(get = "pub")]
        #input
    };

    expanded.into()
}
