//
// Copyright 2024 Tabs Data Inc.
//

extern crate proc_macro;
use crate::service_type::service_type_impl;
use proc_macro::TokenStream;

mod dao;
mod dlo;
mod dto;
mod service_type;
mod type_builder;
mod typed_types;
mod url;

#[proc_macro_attribute]
pub fn service_type(args: TokenStream, item: TokenStream) -> TokenStream {
    service_type_impl(args, item)
}

#[proc_macro_derive(DaoType, attributes(td_type, dao, sqlx))]
pub fn dao_type(input: TokenStream) -> TokenStream {
    dao::dao_type(input)
}

#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn Dao(args: TokenStream, item: TokenStream) -> TokenStream {
    dao::dao(args, item)
}

#[proc_macro_derive(DloType, attributes(td_type, dlo))]
pub fn dlo_type(input: TokenStream) -> TokenStream {
    dlo::dlo_type(input)
}

#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn Dlo(args: TokenStream, item: TokenStream) -> TokenStream {
    dlo::dlo(args, item)
}

#[proc_macro_derive(DtoType, attributes(td_type, dto))]
pub fn dto_type(input: TokenStream) -> TokenStream {
    dto::dto_type(input)
}

#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn Dto(args: TokenStream, item: TokenStream) -> TokenStream {
    dto::dto(args, item)
}

#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn UrlParam(args: TokenStream, item: TokenStream) -> TokenStream {
    url::url_param(args, item)
}

#[proc_macro_attribute]
pub fn typed(args: TokenStream, item: TokenStream) -> TokenStream {
    typed_types::typed_basic(args, item)
}

#[proc_macro_attribute]
pub fn typed_enum(args: TokenStream, item: TokenStream) -> TokenStream {
    typed_types::typed_enum(args, item)
}
