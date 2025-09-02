//
// Copyright 2025 Tabs Data Inc.
//

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

pub fn derive_service_factory(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let fields = if let syn::Data::Struct(syn::DataStruct {
        fields: syn::Fields::Named(ref fields),
        ..
    }) = input.data
    {
        &fields.named
    } else {
        panic!("ServiceFactory can only be derived for structs with named fields");
    };

    let (inits, field_types): (Vec<_>, Vec<_>) = fields
        .iter()
        .map(|f| {
            let fn_name = &f.ident;
            let ty = &f.ty;
            (
                quote! {
                    #fn_name: <#ty as ::td_tower::factory::ServiceFactory<C>>::build(ctx)
                },
                quote! { #ty },
            )
        })
        .collect();

    let expanded = quote! {
        impl<C> ::td_tower::factory::ServiceFactory<C> for #name
        where
            #( #field_types: ::td_tower::factory::ServiceFactory<C, Service = #field_types>, )*
        {
            type Service = Self;

            fn build(ctx: &C) -> Self::Service {
                Self {
                    #(#inits),*
                }
            }
        }
    };
    TokenStream::from(expanded)
}
