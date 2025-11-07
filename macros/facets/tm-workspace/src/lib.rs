//
// Copyright 2025 Tabs Data Inc.
//

mod repository;
mod root;

use proc_macro::TokenStream;

#[proc_macro]
pub fn workspace_root(input: TokenStream) -> TokenStream {
    root::workspace_root(input)
}

#[proc_macro]
pub fn repositories_metadata(input: TokenStream) -> TokenStream {
    repository::repositories_metadata(input)
}
