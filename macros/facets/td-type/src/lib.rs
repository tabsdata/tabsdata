//
// Copyright 2024 Tabs Data Inc.
//

extern crate proc_macro;
use crate::service_type::service_type_impl;
use proc_macro::TokenStream;

mod service_type;
mod type_builder;
mod typed_types;

#[proc_macro_attribute]
pub fn service_type(args: TokenStream, item: TokenStream) -> TokenStream {
    service_type_impl(args, item)
}

#[proc_macro_derive(TdType, attributes(td_type, sqlx))]
pub fn td_type(input: TokenStream) -> TokenStream {
    type_builder::td_type(input)
}

#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn Dao(args: TokenStream, item: TokenStream) -> TokenStream {
    type_builder::dao(args, item)
}

#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn Dlo(args: TokenStream, item: TokenStream) -> TokenStream {
    type_builder::dlo(args, item)
}

#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn Dto(args: TokenStream, item: TokenStream) -> TokenStream {
    type_builder::dto(args, item)
}

#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn IdNameParam(args: TokenStream, item: TokenStream) -> TokenStream {
    type_builder::id_name_param(args, item)
}

#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn NestedParam(args: TokenStream, item: TokenStream) -> TokenStream {
    type_builder::nested_param(args, item)
}

#[proc_macro_attribute]
pub fn typed(args: TokenStream, item: TokenStream) -> TokenStream {
    typed_types::typed_basic(args, item)
}
