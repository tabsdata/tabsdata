//
// Copyright 2025 Tabsdata Inc.
//

use crate::type_builder::{parse_input_item_struct, td_type};
use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, ItemStruct, parse_macro_input};

/// Attribute macro that wraps a struct with DLO derives (DloType, Builder).
pub fn dlo(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);
    quote! {
        #[derive(Debug, Clone, ta_objects::DloType, derive_builder::Builder)]
        #[builder(try_setter, setter(into))]
        #input
    }
    .into()
}

/// Derive macro that generates the Dlo trait impl and builder machinery for a DLO struct.
pub fn dlo_type(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let item = match parse_input_item_struct(&input) {
        Ok(item) => item,
        Err(e) => return e.to_compile_error().into(),
    };
    let td_type = td_type(&input, &item);

    let ident = &item.ident;
    let (impl_generics, ty_generics, where_clause) = item.generics.split_for_impl();

    quote! {
        impl #impl_generics ta_objects::Dlo for #ident #ty_generics #where_clause {}
        #td_type
    }
    .into()
}
