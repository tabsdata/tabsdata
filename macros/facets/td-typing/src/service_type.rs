//
// Copyright 2024 Tabs Data Inc.
//

extern crate proc_macro;

use proc_macro::TokenStream;

use quote::quote;
use syn::{parse_macro_input, FieldsUnnamed, ItemStruct};

/// The main procedural macro that generates required impls to use a struct as a service type.
/// It only supports structs with a single unnamed field.
pub fn service_type_impl(_: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the input as a type alias
    let input = parse_macro_input!(item as ItemStruct);

    // Get the name of the struct
    let name = &input.ident;

    // Get the type of the field and ensure the struct has 1 unnamed field
    let field_type = match &input.fields {
        syn::Fields::Unnamed(FieldsUnnamed { unnamed, .. }) if unnamed.len() == 1 => {
            &unnamed.first().unwrap().ty
        }
        _ => panic!("Expected a struct with a single unnamed field"),
    };

    let expanded = quote! {
        // Keep the original struct definition
        #input

        // Generate the implementations
        impl #name {
            pub fn new(name: impl Into<#field_type>) -> Self {
                Self(name.into())
            }
        }

        impl AsRef<#field_type> for #name {
            fn as_ref(&self) -> &#field_type {
                &self.0
            }
        }

        impl std::ops::Deref for #name {
            type Target = #field_type;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl From<#name> for #field_type {
            fn from(id: #name) -> #field_type {
                id.0
            }
        }

        impl From<&#name> for #field_type {
            fn from(id: &#name) -> #field_type {
                id.0.clone()
            }
        }

        impl td_objects::dlo::Creator<#field_type> for #name {
            fn create(value: impl Into<#field_type>) -> Self {
                Self(value.into())
            }
        }
        impl td_objects::dlo::Value<#field_type> for #name {
            fn value(&self) -> &#field_type {
                &self.0
            }
        }
    };

    TokenStream::from(expanded)
}
