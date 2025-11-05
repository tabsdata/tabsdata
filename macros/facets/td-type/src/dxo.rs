//
// Copyright 2025 Tabs Data Inc.
//

use crate::dao::DaoArguments;
use darling::FromAttributes;
use proc_macro::TokenStream;
use quote::quote;
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
    let vis = &input_mod.vis;
    let mod_token = &input_mod.mod_token;
    let input_mod_ident = &input_mod.ident;
    let expanded = quote! {
        #vis #mod_token #input_mod_ident {
            #(#items)*
        }
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

/// Move fields from base struct into derived struct, keeping the order.
fn move_fields(base: &ItemStruct, derived: &mut ItemStruct) {
    match (&base.fields, &mut derived.fields) {
        (Fields::Named(base_named), Fields::Named(derived_named)) => {
            let mut new_fields = syn::punctuated::Punctuated::new();
            let mut used_idents = std::collections::HashSet::new();
            // For each base field, use derived's definition if present, else base's
            for base_field in base_named.named.iter() {
                let ident = base_field.ident.as_ref();
                let derived_field = derived_named
                    .named
                    .iter()
                    .find(|d| d.ident.as_ref() == ident);
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
            // Add remaining derived fields not present in base
            for derived_field in derived_named.named.iter() {
                let ident = derived_field.ident.as_ref();
                if !used_idents.contains(&ident) {
                    new_fields.push(derived_field.clone());
                }
            }
            derived_named.named = new_fields;
        }
        (Fields::Unnamed(base_unnamed), Fields::Unnamed(derived_unnamed)) => {
            let mut new_fields = syn::punctuated::Punctuated::new();
            let base_len = base_unnamed.unnamed.len();
            // For each base field position, use derived's if present, else base's
            for i in 0..base_len {
                let field = derived_unnamed.unnamed.get(i).cloned().unwrap_or_else(|| {
                    let mut bf = base_unnamed.unnamed[i].clone();
                    bf.attrs.clear();
                    bf
                });
                new_fields.push(field);
            }
            // Add remaining derived fields
            for i in base_len..derived_unnamed.unnamed.len() {
                new_fields.push(derived_unnamed.unnamed[i].clone());
            }
            derived_unnamed.unnamed = new_fields;
        }
        _ => {}
    }
}
