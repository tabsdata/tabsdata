//
// Copyright 2025 Tabsdata Inc.
//

//! Proc macros to generate default derives for the dao/dlo/dto types.

use syn::{DeriveInput, ItemStruct};

/// Extract the [`ItemStruct`] from a [`DeriveInput`], returning a compile error
/// if the input is not a struct.
pub fn parse_input_item_struct(input: &DeriveInput) -> Result<ItemStruct, syn::Error> {
    let input = input.clone();
    let ident_span = input.ident.clone();
    match input.data {
        syn::Data::Struct(data) => Ok(ItemStruct {
            attrs: input.attrs,
            vis: input.vis,
            struct_token: data.struct_token,
            ident: input.ident,
            generics: input.generics,
            fields: data.fields,
            semi_token: data.semi_token,
        }),
        _ => Err(syn::Error::new_spanned(
            ident_span,
            "Dxo can only be derived for structs",
        )),
    }
}
