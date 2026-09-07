//
// Copyright 2026 Tabsdata Inc.
//

//! Merged-field machinery for flattened DXO structs: each derive emits a
//! `__dxo_fields_<Ident>` macro_rules callback carrying its `name: Type`
//! list (a flattened struct's forwards through its base's, tagging base
//! fields with their `Iden`-enum chain). A flattened derive invokes the
//! base's callback with a continuation below, which thus sees the merged
//! list: `__dao_has_columns` bans duplicate field types/names and the
//! database-owned column types on a write DAO, emits the
//! `HasColumn<T>` impls and re-names each base column as a flat associated
//! const on the derived `Iden`; `__dxo_check_flat_names` bans duplicate names only
//! (the serde/sqlx flatten aliasing net for DLOs/DTOs).
//!
//! ```text
//! __dxo_fields_X! { $cb:path => { $($ctx)* } [$($d)*] $($acc)* }
//!   → $cb! { { $($ctx)* } $($acc)* [$($d)*] { field : Type } ... }        (leaf)
//!   → __dxo_fields_Base! { $cb => { ctx } [$($d)* BaseIden] $($acc)* … }  (flatten)
//! ```

use crate::dao::iden_variant;
use crate::dao_marks::{option_inner_type, type_names};
use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::quote;
use std::collections::HashMap;
use syn::parse::{Parse, ParseStream};
use syn::{Path, Type, braced, bracketed, parse_macro_input};

/// A merged-list field: its owner's `Iden`-chain markers (empty = own field).
struct MergedField {
    markers: Vec<Path>,
    name: Ident,
    ty: Type,
}

fn parse_merged_fields(input: ParseStream) -> syn::Result<Vec<MergedField>> {
    let mut fields = Vec::new();
    while !input.is_empty() {
        let markers_content;
        bracketed!(markers_content in input);
        let mut markers = Vec::new();
        while !markers_content.is_empty() {
            markers.push(markers_content.parse::<Path>()?);
        }
        let field_content;
        braced!(field_content in input);
        let name: Ident = field_content.parse()?;
        field_content.parse::<syn::Token![:]>()?;
        let ty: Type = field_content.parse()?;
        fields.push(MergedField { markers, name, ty });
    }
    Ok(fields)
}

fn type_key(ty: &Type) -> String {
    quote!(#ty).to_string()
}

fn duplicate_name_errors(
    derived: &Ident,
    fields: &[MergedField],
    what: &str,
) -> Vec<proc_macro2::TokenStream> {
    let mut seen: HashMap<String, &Ident> = HashMap::new();
    let mut errors = Vec::new();
    for f in fields {
        if let Some(first) = seen.insert(f.name.to_string(), &f.name) {
            let msg = format!(
                "duplicate field name `{first}` in the {what} field list of `{derived}` — \
                 the flattened base already has a field with this name",
            );
            errors.push(quote! { compile_error!(#msg); });
        }
    }
    errors
}

struct HasColumnsInput {
    derived: Ident,
    derived_iden: Path,
    bans_project_id: bool,
    bans_valid_to: bool,
    fields: Vec<MergedField>,
}

impl Parse for HasColumnsInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let ctx;
        braced!(ctx in input);
        let derived: Ident = ctx.parse()?;
        ctx.parse::<syn::Token![,]>()?;
        let derived_iden: Path = ctx.parse()?;
        ctx.parse::<syn::Token![,]>()?;
        let bans_project_id: syn::LitBool = ctx.parse()?;
        ctx.parse::<syn::Token![,]>()?;
        let bans_valid_to: syn::LitBool = ctx.parse()?;
        let fields = parse_merged_fields(input)?;
        Ok(HasColumnsInput {
            derived,
            derived_iden,
            bans_project_id: bans_project_id.value(),
            bans_valid_to: bans_valid_to.value(),
            fields,
        })
    }
}

/// Flattened-DAO continuation: dup-type/dup-name bans over the merged list,
/// then the `HasColumn<T>` impls with `Base(..)`-nested columns.
pub fn dao_has_columns(input: TokenStream) -> TokenStream {
    let HasColumnsInput {
        derived,
        derived_iden,
        bans_project_id,
        bans_valid_to,
        fields,
    } = parse_macro_input!(input as HasColumnsInput);

    let mut out = duplicate_name_errors(&derived, &fields, "merged (flattened)");

    if bans_valid_to && fields.iter().any(|f| type_names(&f.ty, "Scd2ValidTo")) {
        let msg = format!(
            "a write DAO cannot carry a `Scd2ValidTo` column (here through a flattened \
             base) on `{derived}`: the close-out trigger owns the interval end",
        );
        out.push(quote! { compile_error!(#msg); });
    }

    if bans_project_id && fields.iter().any(|f| type_names(&f.ty, "ProjectId")) {
        let msg = format!(
            "a `project_scoped` insert DAO cannot carry a `ProjectId` column (here \
             through a flattened base) on `{derived}`: the `project_isolation` policy \
             defaults it from the connection",
        );
        out.push(quote! { compile_error!(#msg); });
    }

    let mut seen_types: HashMap<String, &Ident> = HashMap::new();
    for f in &fields {
        if let Some(first) = seen_types.insert(type_key(&f.ty), &f.name) {
            let ty = type_key(&f.ty);
            let (a, b) = (first, &f.name);
            let msg = format!(
                "duplicate column type `{ty}` on fields `{a}` and `{b}` of `{derived}` \
                 (including flattened base fields) — give each column a distinct newtype",
            );
            out.push(quote! { compile_error!(#msg); });
        }
    }

    if out.is_empty() {
        let mut inner_seen: HashMap<String, &Ident> = HashMap::new();
        for f in &fields {
            let ty = &f.ty;
            let variant = iden_variant(&f.name);
            let mut col = match f.markers.last() {
                None => quote! { #derived_iden::#variant },
                Some(owner) => quote! { #owner::#variant },
            };
            if !f.markers.is_empty() {
                for m in f.markers.iter().rev().skip(1) {
                    col = quote! { #m::Base(#col) };
                }
                col = quote! { #derived_iden::Base(#col) };
            }
            out.push(quote! {
                impl ta_objects::HasColumn<#ty> for #derived {
                    const COL: Self::Iden = #col;
                }
            });
            // A base column is named on the flattened DAO's own `Iden` too, so
            // raw sea_query spells it `XIden::Col`, not `XIden::Base(YIden::Col)`.
            if !f.markers.is_empty() {
                out.push(quote! {
                    #[allow(non_upper_case_globals)]
                    impl #derived_iden {
                        pub const #variant: Self = #col;
                    }
                });
            }
            // A nullable column answers to its inner type as well, unless
            // another column already claims that type.
            if let Some(inner) = option_inner_type(ty) {
                let key = type_key(inner);
                if !seen_types.contains_key(&key) && inner_seen.insert(key, &f.name).is_none() {
                    out.push(quote! {
                        impl ta_objects::HasColumn<#inner> for #derived {
                            const COL: Self::Iden = #col;
                        }
                    });
                }
            }
        }
    }

    quote! { #(#out)* }.into()
}

struct CheckFlatNamesInput {
    derived: Ident,
    fields: Vec<MergedField>,
}

impl Parse for CheckFlatNamesInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let ctx;
        braced!(ctx in input);
        let derived: Ident = ctx.parse()?;
        let fields = parse_merged_fields(input)?;
        Ok(CheckFlatNamesInput { derived, fields })
    }
}

/// Flattened-DLO/DTO continuation: name-collision check only.
pub fn check_flat_names(input: TokenStream) -> TokenStream {
    let CheckFlatNamesInput { derived, fields } = parse_macro_input!(input as CheckFlatNamesInput);
    let errors = duplicate_name_errors(&derived, &fields, "serde/sqlx-flattened");
    quote! { #(#errors)* }.into()
}

/// The struct's `__dxo_fields_<Ident>` callback; `forward` = the base's
/// callback and the marker its fields carry (its `Iden` enum for DAOs; the
/// base type itself for DLOs/DTOs, whose markers are never consumed).
pub fn fields_callback(
    ident: &Ident,
    fields: &[(&Ident, &Type)],
    forward: Option<(Ident, &Path)>,
) -> proc_macro2::TokenStream {
    let cb_name = callback_ident(ident);
    let names = fields.iter().map(|(n, _)| *n);
    let tys = fields.iter().map(|(_, t)| *t);
    let body = match forward {
        None => quote! {
            $cb! { { $($ctx)* } $($acc)* #( [$($d)*] { #names : #tys } )* }
        },
        Some((base_cb, base_marker)) => quote! {
            #base_cb! { $cb => { $($ctx)* } [$($d)* #base_marker] $($acc)* #( [$($d)*] { #names : #tys } )* }
        },
    };
    quote! {
        #[allow(unused_macros)]
        macro_rules! #cb_name {
            ($cb:path => { $($ctx:tt)* } [$($d:tt)*] $($acc:tt)*) => {
                #body
            };
        }
    }
}

/// `__dxo_fields_<Ident>`.
pub fn callback_ident(ident: &Ident) -> Ident {
    quote::format_ident!("__dxo_fields_{}", ident)
}

fn is_flat_attr(f: &syn::Field, tool: &str) -> bool {
    f.attrs.iter().any(|a| {
        a.path().is_ident(tool)
            && a.parse_args::<Ident>()
                .map(|ident| ident == "flatten")
                .unwrap_or(false)
    })
}

/// DLO/DTO derive add-on: the fields callback plus, when the struct
/// serde/sqlx-flattens a base, the merged duplicate-name check.
pub fn dlo_dto_flat_machinery(item: &syn::ItemStruct) -> syn::Result<proc_macro2::TokenStream> {
    let ident = &item.ident;
    let syn::Fields::Named(named) = &item.fields else {
        return Ok(quote! {});
    };
    if !item.generics.params.is_empty() {
        return Ok(quote! {});
    }

    let flat_fields: Vec<&syn::Field> = named
        .named
        .iter()
        .filter(|f| is_flat_attr(f, "serde") || is_flat_attr(f, "sqlx"))
        .collect();
    if flat_fields.len() > 1 {
        return Err(syn::Error::new_spanned(
            ident,
            "at most one flattened field is supported",
        ));
    }
    let base_ty = match flat_fields.first() {
        None => None,
        Some(f) => {
            let Type::Path(tp) = &f.ty else {
                return Err(syn::Error::new_spanned(
                    &f.ty,
                    "a flattened field must be a plain DXO struct type",
                ));
            };
            Some(tp.path.clone())
        }
    };

    let own: Vec<(&Ident, &Type)> = named
        .named
        .iter()
        .filter(|f| !is_flat_attr(f, "serde") && !is_flat_attr(f, "sqlx"))
        .map(|f| (f.ident.as_ref().expect("named field"), &f.ty))
        .collect();

    let (forward, name_check) = match &base_ty {
        None => (None, quote! {}),
        Some(base) => {
            let base_cb = callback_ident(&base.segments.last().expect("non-empty type path").ident);
            let names = own.iter().map(|(n, _)| *n);
            let tys = own.iter().map(|(_, t)| *t);
            let check = quote! {
                #base_cb! {
                    ta_objects::__dxo_check_flat_names => { #ident } [#base]
                    #( [] { #names : #tys } )*
                }
            };
            (Some((base_cb, base)), check)
        }
    };

    let callback = fields_callback(ident, &own, forward);
    Ok(quote! {
        #callback
        #name_check
    })
}
