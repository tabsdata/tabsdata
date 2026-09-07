//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo_fields::dlo_dto_flat_machinery;
use crate::type_builder::parse_input_item_struct;
use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, ItemStruct, parse_macro_input};

/// Attribute macro that wraps a struct with DLO derives (DloType).
pub fn dlo(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);
    quote! {
        #[derive(Debug, Clone, ta_objects::DloType)]
        #input
    }
    .into()
}

/// Derive macro that generates the Dlo trait impl for a DLO struct.
pub fn dlo_type(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let item = match parse_input_item_struct(&input) {
        Ok(item) => item,
        Err(e) => return e.to_compile_error().into(),
    };

    let ident = &item.ident;
    let (impl_generics, ty_generics, where_clause) = item.generics.split_for_impl();

    let flat_machinery = match dlo_dto_flat_machinery(&item) {
        Ok(ts) => ts,
        Err(e) => return e.to_compile_error().into(),
    };

    quote! {
    impl #impl_generics ta_objects::Dlo for #ident #ty_generics #where_clause {}
    #flat_machinery
    }
    .into()
}
