//
// Copyright 2025 Tabsdata Inc.
//

use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, Fields, ItemStruct, parse_macro_input};

/// Attribute macro that wraps a struct with URL path-parameter derives; the
/// `PathParams` facet binds the placeholder names to the field names.
pub fn url_param(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);

    if !input.generics.params.is_empty() {
        return syn::Error::new_spanned(&input.generics, "the struct must not have generics")
            .to_compile_error()
            .into();
    }

    let expanded = quote! {
        #[derive(Debug, Clone, ta_objects::DtoType, ta_objects::PathParams, utoipa::IntoParams, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
        #input
    };

    expanded.into()
}

/// Attribute macro that wraps a struct with URL query-parameter derives.
pub fn query_param(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);

    if !input.generics.params.is_empty() {
        return syn::Error::new_spanned(&input.generics, "the struct must not have generics")
            .to_compile_error()
            .into();
    }

    let expanded = quote! {
        #[derive(Debug, Clone, ta_objects::DtoType, utoipa::IntoParams, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
        #input
    };

    expanded.into()
}

pub fn derive_path_params(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let ident = &input.ident;

    let fields = match &input.data {
        syn::Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => {
                return syn::Error::new_spanned(ident, "PathParams requires named fields")
                    .to_compile_error()
                    .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(ident, "PathParams requires a struct")
                .to_compile_error()
                .into();
        }
    };

    let idents: Vec<_> = fields.iter().map(|f| f.ident.as_ref().unwrap()).collect();
    let names: Vec<String> = idents.iter().map(|ident| ident.to_string()).collect();

    let expanded = quote! {
        impl ta_objects::PathParams for #ident {
            fn names() -> Vec<&'static str> {
                vec![#(#names),*]
            }
            fn values(&self) -> Vec<String> {
                vec![#(self.#idents.to_string()),*]
            }
        }
    };

    expanded.into()
}
