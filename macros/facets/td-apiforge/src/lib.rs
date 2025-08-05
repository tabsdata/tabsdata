//
// Copyright 2024 Tabs Data Inc.
//

extern crate proc_macro;
use proc_macro::TokenStream;

use crate::attributes::{utoipa_path, utoipa_schema, utoipa_tag};
use crate::finder::utoipa_docs;

mod attributes;
mod finder;

// Attribute generation macros
#[proc_macro_attribute]
pub fn apiserver_path(args: TokenStream, item: TokenStream) -> TokenStream {
    // Alias to utoipa_path, used to find ApiServer paths
    utoipa_path(args, item)
}

#[proc_macro_attribute]
pub fn apiserver_schema(args: TokenStream, item: TokenStream) -> TokenStream {
    // Alias to utoipa_schema, used to find ApiServer schemas
    utoipa_schema(args, item)
}

#[proc_macro]
pub fn apiserver_tag(args: TokenStream) -> TokenStream {
    // Alias to utoipa_schema, used to find ApiServer tags
    utoipa_tag(args)
}

// Docs generation macros
#[proc_macro_attribute]
pub fn apiserver_docs(args: TokenStream, item: TokenStream) -> TokenStream {
    utoipa_docs(args, item)
}
