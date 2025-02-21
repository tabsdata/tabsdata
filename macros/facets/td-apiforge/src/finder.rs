//
//  Copyright 2024 Tabs Data Inc.
//

extern crate proc_macro;

use proc_macro::{Span, TokenStream};
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::attributes::UtoipaTagArguments;
use crate::status::{CTX_MACRO_NAME, CTX_PREFIX};
use darling::FromMeta;
use heck::ToUpperCamelCase;
use quote::{format_ident, quote};
use syn::{parse_macro_input, Expr, File, Ident, Item, ItemMacro};
use td_shared::parse_meta;
use td_shared::project::get_project_root;
use walkdir::WalkDir;

const DEFAULT_TAGS_MACRO: &str = "api_server_tag";
const DEFAULT_PATHS_ATTRIBUTE: &str = "api_server_path";
const DEFAULT_SCHEMA_ATTRIBUTE: &str = "api_server_schema";

const CTX_STATUS_FILE: &str = "status.rs";

#[derive(FromMeta)]
struct UtoipaDocsArguments {
    title: String,
    version: String,
    #[darling(default, multiple, rename = "modifier")]
    modifiers: Vec<Option<Expr>>,
    #[darling(default, multiple, rename = "server")]
    servers: Vec<Option<Expr>>,
    root_dir: Option<String>,
    tags_attribute: Option<Ident>,
    paths_attribute: Option<Ident>,
    schemas_attribute: Option<Ident>,
}

/// The main procedural macro that generates the OpenAPI documentation for the given crate.
/// It collects all the tags, paths, and schemas from the files in the given directory.
/// Custom attributes might be used to gain control over the generated OpenAPI documentation.
/// Defaults are used in API Server.
pub fn utoipa_docs(args: TokenStream, item: TokenStream) -> TokenStream {
    let base_root_dir: String = get_project_root();
    let base_root_dir = base_root_dir.as_str();
    let default_root_dir = format!("{}/server/binaries/td-server/src/lib", base_root_dir);

    let parsed_args = parse_meta!(UtoipaDocsArguments, args).unwrap();

    let title = parsed_args.title;
    let version = parsed_args.version;
    let modifiers = parsed_args.modifiers;
    let servers = parsed_args.servers;
    let root_dir = parsed_args
        .root_dir
        .unwrap_or_else(|| default_root_dir.to_string());

    let tags_attribute = parsed_args
        .tags_attribute
        .unwrap_or_else(|| Ident::new(DEFAULT_TAGS_MACRO, Span::call_site().into()));
    let paths_attribute = parsed_args
        .paths_attribute
        .unwrap_or_else(|| Ident::new(DEFAULT_PATHS_ATTRIBUTE, Span::call_site().into()));
    let schemas_attribute = parsed_args
        .schemas_attribute
        .unwrap_or_else(|| Ident::new(DEFAULT_SCHEMA_ATTRIBUTE, Span::call_site().into()));

    let mut ctx_file = PathBuf::new();
    ctx_file.push(file!());
    ctx_file.pop();
    ctx_file.push(CTX_STATUS_FILE);
    let ctx_macro_gen_idents = extract_ctx_macro_idents(ctx_file);

    let (tags_found, found_paths, found_schemas) = WalkDir::new(&root_dir)
        .follow_links(true)
        .follow_root_links(true)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().and_then(|s| s.to_str()) == Some("rs"))
        .map(|entry| {
            let content = std::fs::read_to_string(entry.path()).expect("Failed to read file");
            let syntax = syn::parse_file(&content).expect("Failed to parse file");
            let crate_path = get_module_path(entry.path(), &root_dir);
            find_in_file(
                &crate_path,
                &syntax,
                &tags_attribute,
                &paths_attribute,
                &schemas_attribute,
                &ctx_macro_gen_idents,
            )
        })
        .fold(
            (Vec::new(), Vec::new(), Vec::new()),
            |(mut tags, mut paths, mut schemas), (tag, path, schema)| {
                tags.extend(tag);

                if let Some(duplicate) = has_duplicates_idents(&paths, &path) {
                    panic!(
                        "Duplicate path found: {}",
                        duplicate.segments.last().unwrap().ident
                    );
                } else {
                    paths.extend(path);
                }

                if let Some(duplicate) = has_duplicates_idents(&schemas, &schema) {
                    panic!(
                        "Duplicate schema found: {}",
                        duplicate.segments.last().unwrap().ident
                    );
                } else {
                    schemas.extend(schema);
                }

                (tags, paths, schemas)
            },
        );

    let item = parse_macro_input!(item as Item);
    let output = quote! {
        #[derive(utoipa::OpenApi)]
        #[openapi(
            modifiers(#(#modifiers),*),
            servers(#(#servers),*),
            info(
                title = #title,
                version = #version,
                contact(),
                description = "",
            ),
            tags(
                #(#tags_found,)*
            ),
            paths(
                #(#found_paths,)*
            ),
            components(
                schemas(
                    #(#found_schemas,)*
                )
            ),
        )]
        #item
    };

    output.into()
}

/// Extract all macro idents from the given file.
fn extract_ctx_macro_idents(path: impl Into<PathBuf>) -> Vec<Ident> {
    let mut file = std::fs::File::open(path.into()).unwrap();
    let mut buffer = String::new();
    file.read_to_string(&mut buffer).unwrap();

    let parsed_macros: Vec<ItemMacro> = syn::parse::<File>(buffer.parse().unwrap())
        .expect("Failed to parse file")
        .items
        .into_iter()
        .filter_map(|item| {
            if let Item::Macro(m) = item {
                Some(m)
            } else {
                None
            }
        })
        .collect();

    let mut idents = Vec::new();
    for item_macro in parsed_macros {
        if item_macro.mac.path.is_ident(CTX_MACRO_NAME) {
            let tokens = item_macro.mac.tokens.clone();
            let mut iter = tokens.into_iter();
            if let Some(proc_macro2::TokenTree::Ident(ident)) = iter.next() {
                idents.push(ident);
            }
        }
    }
    idents
}

/// Find all tags, paths, and schemas in the given file.
/// The tags are found in macros definitions, while paths and schemas are found in other proc
/// macros attributes and associated types.
fn find_in_file(
    module: &str,
    syntax: &File,
    tags_attribute: &Ident,
    paths_attribute: &Ident,
    schemas_attribute: &Ident,
    ctx_macro_gen_idents: &[Ident],
) -> (
    Vec<proc_macro2::TokenStream>,
    Vec<syn::Path>,
    Vec<syn::Path>,
) {
    let mut found_tags = Vec::new();
    let mut found_paths = Vec::new();
    let mut found_schemas = Vec::new();
    for item in &syntax.items {
        // First, look for tags in macros
        if let Item::Macro(item) = item {
            if item.mac.path.is_ident(tags_attribute) {
                let input = item.mac.tokens.clone();
                let tag = parse_meta!(UtoipaTagArguments, input).unwrap();
                let name = tag.name();
                let description = tag.description();
                let syn_tag = quote! {
                    (name = #name, description = #description)
                };
                found_tags.push(syn_tag);
            } else if let Some(ident) = item.mac.path.get_ident() {
                // And also look for macro generated schemas.
                if ctx_macro_gen_idents.contains(ident) {
                    let macro_name = ident.to_string().to_upper_camel_case();
                    let input = item.mac.tokens.clone();
                    let schema_struct = syn::parse::<syn::Ident>(input.into()).unwrap();
                    let schema_ident =
                        format_ident!("{}{}{}", CTX_PREFIX, macro_name, schema_struct);
                    let path = syn::parse_str(&format!("{}::{}", module, schema_ident)).unwrap();
                    found_schemas.push(path);
                }
            }
        }

        // And then, look for regular attributes
        let (ident, attrs) = match item {
            Item::Struct(item) => (&item.ident, &item.attrs),
            Item::Enum(item) => (&item.ident, &item.attrs),
            Item::Type(item) => (&item.ident, &item.attrs),
            Item::Fn(item) => (&item.sig.ident, &item.attrs),
            _ => continue,
        };

        for attr in attrs {
            if attr.path().is_ident(paths_attribute) {
                let path = syn::parse_str(&format!("{}::{}", module, ident)).unwrap();
                found_paths.push(path);
            }
            if attr.path().is_ident(schemas_attribute) {
                let path = syn::parse_str(&format!("{}::{}", module, ident)).unwrap();
                found_schemas.push(path);
            }
        }
    }

    (found_tags, found_paths, found_schemas)
}

/// Get the module path from the given file path, stripping the root directory.
/// If the crate is not in the root directory, it will fail.
fn get_module_path(path: &Path, root_dir: &str) -> String {
    let to_strip = Path::new(root_dir);
    let path = path.strip_prefix(to_strip).unwrap();

    let segments = path
        .iter()
        .map(|s| s.to_str().unwrap().to_string())
        .collect::<Vec<_>>();

    // Collect all segments
    let mut remaining_segments: Vec<_> = segments;

    // Prepend 'crate' to the remaining segments
    remaining_segments.insert(0, "crate".to_string());

    // Remove mod
    let remaining_segments: Vec<_> = remaining_segments
        .into_iter()
        .filter(|s| *s != "mod.rs")
        .collect();

    // Join the remaining segments to form the module path
    let result = remaining_segments.join("::");
    if result.ends_with(".rs") {
        result.trim_end_matches(".rs").to_string()
    } else {
        result
    }
}

fn has_duplicates_idents<'a>(
    slice: &'a [syn::Path],
    extended: &'a [syn::Path],
) -> Option<&'a syn::Path> {
    if let Some(duplicate) = slice.iter().find(|s| {
        extended
            .iter()
            .any(|s2| s.segments.last().unwrap().ident == s2.segments.last().unwrap().ident)
    }) {
        Some(duplicate)
    } else {
        None
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    extern crate proc_macro;

    #[test]
    fn test_get_module_path() {
        let path = Path::new("src/lib/test_module.rs");
        let root_dir = "src/lib";
        let module_path = get_module_path(path, root_dir);
        assert_eq!(module_path, "crate::test_module");
    }
}
