//
// Copyright 2024 Tabs Data Inc.
//

extern crate proc_macro;

use proc_macro::TokenStream;

use quote::{format_ident, quote};
use syn::{parse_macro_input, ItemEnum};

pub fn td_error_impl(input: TokenStream) -> TokenStream {
    // Parse the input as a type alias
    let input = parse_macro_input!(input as ItemEnum);

    // Get the name of the struct
    let name = &input.ident;

    let discriminant_enum = format_ident!("{}Discriminants", name);

    let expanded = quote! {

        #[repr(u16)]
        #[derive(Debug, thiserror::Error, strum_macros::EnumDiscriminants)]
        #input

        impl #name {
            fn variant_index(&self) -> u16 {
               let discriminant: #discriminant_enum =  self.into();
                discriminant as u16
            }
        }

        impl td_common::error::TdDomainError for #name {
            fn domain(&self) -> &'static str {
                stringify!(#name)
            }

            fn code(&self) -> String {
                format!("{}::{:04}", self.domain(), self.variant_index())
            }

            fn api_error(&self) -> td_common::error::ApiError {
                td_common::error::ApiError::from(self.variant_index())
            }
        }

        impl From<#name> for td_common::error::TdError {
            fn from(error: #name) -> Self {
                Self::new(error)
            }
        }
    };
    expanded.into()
}
