//
// Copyright 2024 Tabs Data Inc.
//

extern crate proc_macro;
use proc_macro::TokenStream;

use crate::attributes::{utoipa_path, utoipa_schema, utoipa_tag};
use crate::finder::utoipa_docs;

mod attributes;
mod finder;
mod status;

// Attribute generation macros
#[proc_macro_attribute]
pub fn api_server_path(args: TokenStream, item: TokenStream) -> TokenStream {
    // Alias to utoipa_path, used to find ApiServer paths
    utoipa_path(args, item)
}

#[proc_macro_attribute]
pub fn api_server_schema(args: TokenStream, item: TokenStream) -> TokenStream {
    // Alias to utoipa_schema, used to find ApiServer schemas
    utoipa_schema(args, item)
}

#[proc_macro]
pub fn api_server_tag(args: TokenStream) -> TokenStream {
    // Alias to utoipa_schema, used to find ApiServer tags
    utoipa_tag(args)
}

// Docs generation macros
#[proc_macro_attribute]
pub fn api_server_docs(args: TokenStream, item: TokenStream) -> TokenStream {
    utoipa_docs(args, item)
}

#[proc_macro]
pub fn status(input: TokenStream) -> TokenStream {
    status::status(input)
}

#[proc_macro]
pub fn get_status(input: TokenStream) -> TokenStream {
    status::get_status(input)
}

#[proc_macro]
pub fn list_status(input: TokenStream) -> TokenStream {
    status::list_status(input)
}

#[proc_macro]
pub fn create_status(input: TokenStream) -> TokenStream {
    status::create_status(input)
}

#[proc_macro]
pub fn update_status(input: TokenStream) -> TokenStream {
    status::update_status(input)
}

#[proc_macro]
pub fn delete_status(input: TokenStream) -> TokenStream {
    status::delete_status(input)
}

#[proc_macro]
pub fn auth_status_raw(input: TokenStream) -> TokenStream {
    status::auth_status_raw(input)
}
