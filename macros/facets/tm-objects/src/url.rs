//
// Copyright 2025 Tabs Data Inc.
//

use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemStruct, parse_macro_input};

/// Attribute macro that wraps a struct with URL parameter derives (DtoType, IntoParams, Builder, etc.).
pub fn url_param(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);

    if !input.generics.params.is_empty() {
        return syn::Error::new_spanned(&input.generics, "the struct must not have generics")
            .to_compile_error()
            .into();
    }

    let expanded = quote! {
        #[derive(Debug, Clone, ta_objects::DtoType, utoipa::IntoParams, derive_builder::Builder, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
        #[builder(try_setter, setter(into))]
        #input
    };

    expanded.into()
}
