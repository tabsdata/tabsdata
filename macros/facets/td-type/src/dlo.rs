//
// Copyright 2025 Tabs Data Inc.
//

use crate::type_builder::{parse_input_item_struct, td_type};
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{DeriveInput, ItemStruct, parse_macro_input};

pub fn dlo(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);

    let expanded = quote! {
        #[derive(Debug, Clone, Eq, PartialEq, td_type::DloType, derive_builder::Builder, getset::Getters, serde::Serialize, serde::Deserialize, sqlx::FromRow)]
        #[builder(try_setter, setter(into))]
        #[getset(get = "pub")]
        #input
    };

    expanded.into()
}

pub fn dlo_type(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let item = parse_input_item_struct(&input);

    // Td type
    let td_type = td_type(&input, &item);

    // Typed generic
    let ident = &item.ident;
    let (impl_generics, ty_generics, where_clause) = item.generics.split_for_impl();

    let builder_type = format_ident!("{}Builder", &item.ident);
    let expanded = quote! {
        impl #impl_generics crate::types::DataLogicObject for #ident #ty_generics #where_clause {
            type Builder = #builder_type #ty_generics;
        }

        #td_type
    };

    expanded.into()
}
