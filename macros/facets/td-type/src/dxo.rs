//
// Copyright 2025 Tabs Data Inc.
//

use crate::dao::DaoArguments;
use darling::FromAttributes;
use proc_macro::TokenStream;
use quote::quote;
use std::collections::HashSet;
use syn::{Attribute, Fields, Ident, Item, ItemMod, ItemStruct, Result, parse_macro_input};

pub fn dxo(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the module containing all dxo structs
    let input_mod = parse_macro_input!(item as ItemMod);
    let (_, mut items) = match input_mod.content {
        Some((brace, items)) => (brace, items),
        None => {
            return syn::Error::new_spanned(input_mod, "Expected module with braces")
                .to_compile_error()
                .into();
        }
    };

    // Collect mutable references to all structs
    let mut structs: Vec<&mut ItemStruct> = items
        .iter_mut()
        .filter_map(|item| match item {
            Item::Struct(s) => Some(s),
            _ => None,
        })
        .collect();

    // Merge base structs
    for i in 0..structs.len() {
        let (left, right) = structs.split_at_mut(i);
        let (item, rest) = right.split_first_mut().unwrap();

        if let Some(base_ident) = find_base_struct(&item.attrs)
            && let Some(base_item) = left
                .iter_mut()
                .chain(rest.iter_mut())
                .find(|d| d.ident == base_ident)
        {
            // Merge attributes
            item.attrs = match merge_dxo_attrs(base_item, item) {
                Ok(attrs) => attrs,
                Err(e) => {
                    return e.to_compile_error().into();
                }
            };

            // Move fields from base to derived
            move_fields(base_item, item);
        }
    }

    // Generate the output module, keeping non-struct items intact
    let mod_token = &input_mod.mod_token;
    let input_mod_ident = &input_mod.ident;

    // Making it always private, and re-exporting contents.
    let expanded = quote! {
        #mod_token #input_mod_ident {
            #(#items)*
        }
        pub use #input_mod_ident::*;
    };
    TokenStream::from(expanded)
}

/// Detect struct-level inheritance via #[inherits(BaseStruct)]
fn find_base_struct(attrs: &[Attribute]) -> Option<Ident> {
    for attr in attrs {
        if attr.path().is_ident("inherits") {
            // Parse tokens inside parentheses as a single Ident
            if let Ok(ident) = attr.parse_args::<Ident>() {
                return Some(ident);
            }
        }
    }
    None
}

#[derive(Debug)]
enum DxoType {
    Dao,
    Dlo,
    Dto,
}

/// Merge attributes from base into child
fn merge_dxo_attrs(base: &ItemStruct, child: &ItemStruct) -> Result<Vec<Attribute>> {
    fn find_dxo_type(s: &ItemStruct) -> Result<DxoType> {
        for attr in &s.attrs {
            if let Some(path) = attr.path().segments.last() {
                match path.ident.to_string().as_str() {
                    "Dao" => return Ok(DxoType::Dao),
                    "Dto" => return Ok(DxoType::Dto),
                    "Dlo" => return Ok(DxoType::Dlo),
                    _ => continue,
                }
            }
        }
        Err(syn::Error::new_spanned(
            s,
            format!("Missing `dXo` attribute in struct {}", s.ident),
        ))
    }

    let base_dxo = find_dxo_type(base)?;
    let child_dxo = find_dxo_type(child)?;

    // First, readd dxo attribute of child
    let mut attrs: Vec<Attribute> = Vec::new();

    // Merge dxo-specific attributes based on types
    match (base_dxo, child_dxo) {
        (DxoType::Dao, DxoType::Dao) => {
            // keep other child attrs
            for attr in &child.attrs {
                if !attr.path().is_ident("dao") {
                    attrs.push(attr.clone());
                }
            }

            // and merge dao attrs
            let base_dao_args = DaoArguments::from_attributes(&base.attrs)
                .expect("failed to parse base attributes");
            let child_dao_args = DaoArguments::from_attributes(&child.attrs)
                .expect("failed to parse child attributes");
            let merged_dao_args = base_dao_args.override_with(child_dao_args);
            let dao_attr = syn::parse_quote! { #merged_dao_args };
            attrs.push(dao_attr);
        }
        (DxoType::Dao, DxoType::Dlo) => {
            // do nothing, keep child attrs
            attrs.extend(child.attrs.clone())
        }
        (DxoType::Dao, DxoType::Dto) => {
            // do nothing, keep child attrs
            attrs.extend(child.attrs.clone())
        }
        (DxoType::Dlo, DxoType::Dlo) => {
            // do nothing, keep child attrs
            attrs.extend(child.attrs.clone())
        }
        (DxoType::Dto, DxoType::Dto) => {
            // do nothing, keep child attrs
            attrs.extend(child.attrs.clone())
        }
        _ => Err(syn::Error::new_spanned(
            child,
            format!(
                "Base ({}) and child ({}) dxo types not supported for inheritance",
                base.ident, child.ident
            ),
        ))?,
    }

    Ok(attrs)
}

/// Move fields from base struct into derived struct, keeping the order. Ignore
/// fields marked with #[ignore].
fn move_fields(base: &ItemStruct, derived: &mut ItemStruct) {
    match (&base.fields, &mut derived.fields) {
        (Fields::Named(base_named), Fields::Named(derived_named)) => {
            // Collect all idents in derived struct with #[ignore]
            let ignored_idents: HashSet<Ident> = derived_named
                .named
                .iter()
                .filter_map(|f| {
                    if f.attrs.iter().any(|attr| attr.path().is_ident("ignore")) {
                        f.ident.clone()
                    } else {
                        None
                    }
                })
                .collect();
            // Filter out #[ignore] from both base and derived fields, and skip any field whose ident is in ignored_idents
            let base_fields: Vec<_> = base_named
                .named
                .iter()
                .filter(|f| {
                    if let Some(ident) = &f.ident {
                        !ignored_idents.contains(ident)
                    } else {
                        true
                    }
                })
                .cloned()
                .collect();
            let derived_fields: Vec<_> = derived_named
                .named
                .iter()
                .filter(|f| {
                    if let Some(ident) = &f.ident {
                        !ignored_idents.contains(ident)
                            && !f.attrs.iter().any(|attr| attr.path().is_ident("ignore"))
                    } else {
                        true
                    }
                })
                .cloned()
                .collect();
            // Now merge fields, keeping order: base fields first (overridden by derived if present), then any extra derived fields
            let mut new_fields = syn::punctuated::Punctuated::new();
            let mut used_idents = HashSet::new();
            for base_field in base_fields.iter() {
                let ident = base_field.ident.as_ref();
                let derived_field = derived_fields.iter().find(|d| d.ident.as_ref() == ident);
                let field = match derived_field {
                    Some(df) => df.clone(),
                    None => {
                        let mut bf = base_field.clone();
                        bf.attrs.clear();
                        bf
                    }
                };
                used_idents.insert(ident);
                new_fields.push(field);
            }
            for derived_field in derived_fields.iter() {
                let ident = derived_field.ident.as_ref();
                if !used_idents.contains(&ident) {
                    new_fields.push(derived_field.clone());
                }
            }
            derived_named.named = new_fields;
        }
        (Fields::Unnamed(base_unnamed), Fields::Unnamed(derived_unnamed)) => {
            // For unnamed fields, we can skip any derived field with #[ignore]
            let ignored_indices: HashSet<_> = derived_unnamed
                .unnamed
                .iter()
                .enumerate()
                .filter_map(|(i, f)| {
                    if f.attrs.iter().any(|attr| attr.path().is_ident("ignore")) {
                        Some(i)
                    } else {
                        None
                    }
                })
                .collect();
            let base_fields: Vec<_> = base_unnamed
                .unnamed
                .iter()
                .enumerate()
                .filter(|(i, _)| !ignored_indices.contains(i))
                .map(|(_, f)| f.clone())
                .collect();
            let derived_fields: Vec<_> = derived_unnamed
                .unnamed
                .iter()
                .enumerate()
                .filter(|(i, f)| {
                    !ignored_indices.contains(i)
                        && !f.attrs.iter().any(|attr| attr.path().is_ident("ignore"))
                })
                .map(|(_, f)| f.clone())
                .collect();
            // Now merge fields, keeping order: base fields first (overridden by derived if present), then any extra derived fields
            let mut new_fields = syn::punctuated::Punctuated::new();
            let base_len = base_fields.len();
            for (i, base_field) in base_fields.iter().enumerate().take(base_len) {
                let field = derived_fields.get(i).cloned().unwrap_or_else(|| {
                    let mut bf = base_field.clone();
                    bf.attrs.clear();
                    bf
                });
                new_fields.push(field);
            }
            for derived_field in derived_fields.iter().skip(base_len) {
                new_fields.push(derived_field.clone());
            }
            derived_unnamed.unnamed = new_fields;
        }
        _ => {}
    }
}
