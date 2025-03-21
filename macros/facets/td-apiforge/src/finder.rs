//
//  Copyright 2024 Tabs Data Inc.
//

extern crate proc_macro;

use crate::attributes::UtoipaTagArguments;
use crate::status::{CTX_MACRO_NAME, CTX_PREFIX};
use darling::FromMeta;
use heck::ToUpperCamelCase;
use proc_macro::{Span, TokenStream};
use quote::{format_ident, quote};
use std::path::Path;
use syn::{parse_macro_input, Expr, File, Ident, Item, ItemMacro};
use td_shared::meta_parser::SynMetaOrLit;
use td_shared::parse_meta;
use td_shared::project::get_project_root;
use walkdir::WalkDir;

const DEFAULT_TAGS_MACROS: &[&str] = &["apiserver_tag"];
const DEFAULT_PATHS_ATTRIBUTES: &[&str] = &["apiserver_path"];
const DEFAULT_SCHEMA_ATTRIBUTES: &[&str] = &["apiserver_schema", "Dto", "typed"];

const CTX_STATUS_FILE: &str = include_str!("status.rs");

#[derive(FromMeta)]
struct UtoipaDocsArguments {
    title: String,
    version: SynMetaOrLit,
    #[darling(default, multiple, rename = "modifier")]
    modifiers: Vec<Option<Expr>>,
    #[darling(default, multiple, rename = "server")]
    servers: Vec<Option<Expr>>,

    #[darling(default, multiple, rename = "tags_attribute")]
    tags_attributes: Vec<Option<Ident>>,
    #[darling(default, multiple, rename = "paths_attribute")]
    paths_attributes: Vec<Option<Ident>>,
    #[darling(default, multiple, rename = "schemas_attribute")]
    schemas_attributes: Vec<Option<Ident>>,

    #[darling(default, multiple, rename = "crate_dir")]
    crate_dirs: Vec<Option<CrateDir>>,
}

#[derive(Debug, Clone, FromMeta)]
struct CrateDir {
    name: String,
    dir: String,
}

trait IntoIdent {
    fn to_ident(&self) -> Ident;
}

impl IntoIdent for &str {
    fn to_ident(&self) -> Ident {
        Ident::new(self, Span::call_site().into())
    }
}

impl UtoipaDocsArguments {
    fn title(&self) -> &str {
        &self.title
    }

    fn version(&self) -> &SynMetaOrLit {
        &self.version
    }

    fn modifiers(&self) -> Vec<&Expr> {
        self.modifiers.iter().filter_map(|f| f.as_ref()).collect()
    }

    fn servers(&self) -> Vec<&Expr> {
        self.servers.iter().filter_map(|f| f.as_ref()).collect()
    }

    fn some_or_default<T: Clone>(option: &[Option<T>], default: &[T]) -> Vec<T> {
        let list: Vec<_> = option.iter().filter_map(|f| f.as_ref()).cloned().collect();
        if list.is_empty() {
            default.to_vec()
        } else {
            list
        }
    }

    fn tags_attributes(&self) -> Vec<Ident> {
        let default: Vec<_> = DEFAULT_TAGS_MACROS.iter().map(|s| s.to_ident()).collect();
        Self::some_or_default(&self.tags_attributes, &default)
    }

    fn paths_attributes(&self) -> Vec<Ident> {
        let default: Vec<_> = DEFAULT_PATHS_ATTRIBUTES
            .iter()
            .map(|s| s.to_ident())
            .collect();
        Self::some_or_default(&self.paths_attributes, &default)
    }

    fn schemas_attributes(&self) -> Vec<Ident> {
        let default: Vec<_> = DEFAULT_SCHEMA_ATTRIBUTES
            .iter()
            .map(|s| s.to_ident())
            .collect();
        Self::some_or_default(&self.schemas_attributes, &default)
    }

    fn default_crate_dirs() -> Vec<CrateDir> {
        let base_root_dir: String = get_project_root();
        vec![
            CrateDir {
                name: "crate".to_string(),
                dir: format!("{}/server/binaries/td-server/src/lib", base_root_dir),
            },
            CrateDir {
                name: "td_objects".to_string(),
                dir: format!("{}/server/libraries/td-objects/src", base_root_dir),
            },
        ]
    }

    fn crate_dirs(&self) -> Vec<CrateDir> {
        let default = Self::default_crate_dirs();
        Self::some_or_default(&self.crate_dirs, &default)
    }
}

/// The main procedural macro that generates the OpenAPI documentation for the given crate.
/// It collects all the tags, paths, and schemas from the files in the given directory.
/// Custom attributes might be used to gain control over the generated OpenAPI documentation.
/// Defaults are used in API Server.
pub fn utoipa_docs(args: TokenStream, item: TokenStream) -> TokenStream {
    let parsed_args = parse_meta!(UtoipaDocsArguments, args).unwrap();
    let item = parse_macro_input!(item as Item);

    let ctx_macro_gen_idents = extract_ctx_macro_idents();

    let (mut tags_found, mut found_paths, mut found_schemas) = (Vec::new(), Vec::new(), Vec::new());
    for crate_dir in parsed_args.crate_dirs() {
        WalkDir::new(&crate_dir.dir)
            .follow_links(true)
            .follow_root_links(true)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().and_then(|s| s.to_str()) == Some("rs"))
            .map(|entry| {
                let content = std::fs::read_to_string(entry.path()).expect("Failed to read file");
                let syntax = syn::parse_file(&content).expect("Failed to parse file");
                let crate_path = get_module_path(entry.path(), &crate_dir);
                find_in_file(
                    &crate_path,
                    &syntax,
                    &parsed_args.tags_attributes(),
                    &parsed_args.paths_attributes(),
                    &parsed_args.schemas_attributes(),
                    &ctx_macro_gen_idents,
                )
            })
            .for_each(|(tag, path, schema)| {
                if let Some(duplicate) = has_duplicates_idents(&found_paths, &path) {
                    panic!(
                        "Duplicate path found: {}",
                        duplicate.segments.last().unwrap().ident
                    );
                } else {
                    found_paths.extend(path);
                }

                if let Some(duplicate) = has_duplicates_idents(&found_schemas, &schema) {
                    panic!(
                        "Duplicate schema found: {}",
                        duplicate.segments.last().unwrap().ident
                    );
                } else {
                    found_schemas.extend(schema);
                }

                tags_found.extend(tag);
            });
    }

    let title = parsed_args.title();
    let version = parsed_args.version();
    let modifiers = parsed_args.modifiers();
    let servers = parsed_args.servers();

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
fn extract_ctx_macro_idents() -> Vec<Ident> {
    let parsed_macros: Vec<ItemMacro> = syn::parse::<File>(CTX_STATUS_FILE.parse().unwrap())
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
    tags_attribute: &[Ident],
    paths_attribute: &[Ident],
    schemas_attributes: &[Ident],
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
            if is_last_segment_path(&item.mac.path, tags_attribute) {
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
            if is_last_segment_path(attr.path(), paths_attribute) {
                let path = syn::parse_str(&format!("{}::{}", module, ident)).unwrap();
                found_paths.push(path);
            }
            if is_last_segment_path(attr.path(), schemas_attributes) {
                let path = syn::parse_str(&format!("{}::{}", module, ident)).unwrap();
                found_schemas.push(path);
            }
        }
    }

    (found_tags, found_paths, found_schemas)
}

fn is_last_segment_path(path: &syn::Path, idents: &[Ident]) -> bool {
    path.segments
        .last()
        .map(|segment| idents.iter().any(|attr| attr == &segment.ident))
        .unwrap_or(false)
}

/// Get the module path from the given file path, stripping the root directory.
/// If the crate is not in the root directory, it will fail.
fn get_module_path(path: &Path, crate_dir: &CrateDir) -> String {
    let to_strip = Path::new(&crate_dir.dir);
    let path = path.strip_prefix(to_strip).unwrap();

    let segments = path
        .iter()
        .map(|s| s.to_str().unwrap().to_string())
        .collect::<Vec<_>>();

    // Collect all segments
    let mut remaining_segments: Vec<_> = segments;

    // Prepend 'crate' to the remaining segments
    remaining_segments.insert(0, crate_dir.name.to_string());

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
        let crate_dir = CrateDir {
            name: "crate".to_string(),
            dir: "src/lib".to_string(),
        };
        let module_path = get_module_path(path, &crate_dir);
        assert_eq!(module_path, "crate::test_module");
    }
}
