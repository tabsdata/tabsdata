//
// Copyright 2025 Tabs Data Inc.
//

use proc_macro::TokenStream;
use quote::quote;
use serde::Deserialize;
use std::fs;
use syn::{Expr, Ident, Token, parse::Parse, parse::ParseStream, parse_macro_input};

const REPOSITORIES_YAML: &str = "variant/resources/about/repositories.yaml";

#[derive(Debug, Deserialize)]
struct Repository {
    prefix: String,
}

#[derive(Debug, Deserialize)]
struct Repositories {
    repositories: Vec<Repository>,
}

struct MacroArguments {
    sections: Expr,
    git_data: Expr,
    macro_name: Ident,
}

impl Parse for MacroArguments {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let sections: Expr = input.parse()?;
        input.parse::<Token![,]>()?;
        let git_data: Expr = input.parse()?;
        input.parse::<Token![,]>()?;
        let macro_name: Ident = input.parse()?;
        Ok(MacroArguments {
            sections,
            git_data,
            macro_name,
        })
    }
}

pub fn repositories_metadata(input: TokenStream) -> TokenStream {
    let arguments = parse_macro_input!(input as MacroArguments);
    let sections = arguments.sections;
    let git_data = arguments.git_data;
    let macro_name = arguments.macro_name;
    let workspace_root = crate::root::get_workspace_root_for_repository();
    let repositories_metadata_path = workspace_root.join(REPOSITORIES_YAML);
    let repositories_metadata =
        fs::read_to_string(&repositories_metadata_path).unwrap_or_else(|error| {
            panic!(
                "Failed to read repositories metadata at {:?}: {}",
                repositories_metadata_path, error
            )
        });
    let repositories: Repositories = serde_yaml::from_str(&repositories_metadata)
        .unwrap_or_else(|e| panic!("Failed to parse repositories metadata: {}", e));
    let repositories_prefixes: Vec<_> = repositories
        .repositories
        .iter()
        .map(|repository| {
            let prefix = &repository.prefix;
            quote! { #prefix }
        })
        .collect();
    let expanded = quote! {
        #macro_name!(#sections, #git_data, #(#repositories_prefixes),*)
    };
    TokenStream::from(expanded)
}
