//
// Copyright 2025 Tabs Data Inc.
//

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

pub fn derive_field_accessor(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_ident = input.ident;

    let mut impls = Vec::new();

    if let Data::Struct(data_struct) = &input.data
        && let Fields::Named(fields) = &data_struct.fields
    {
        for field in &fields.named {
            let fname = field.ident.as_ref().unwrap();
            let ftype = &field.ty;

            impls.push(quote! {
            impl ::ta_services::factory::FieldAccessor<#struct_ident> for #ftype {
                fn get_field(state: &#struct_ident) -> Self {
                    state.#fname.clone()
              }
               }
            });
        }
    }

    let output = quote! {
        #(#impls)*
    };

    output.into()
}
