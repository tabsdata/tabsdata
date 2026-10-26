//
// Copyright 2025 Tabsdata Inc.
//

use crate::dao_marks::option_inner_type;
use crate::dto_tuples::reject_tuples;
use crate::dxo_fields::dlo_dto_flat_machinery;
use crate::type_builder::parse_input_item_struct;
use darling::{FromDeriveInput, FromField, FromMeta};
use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::quote;
use syn::{DeriveInput, ItemStruct, parse_macro_input};

/// Attribute macro that wraps a struct with DTO derives (DtoType, Builder, Serialize, etc.).
pub fn dto(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);
    if let Err(e) = reject_tuples(&input.fields) {
        return e.to_compile_error().into();
    }

    let vis = &input.vis;
    let struct_token = &input.struct_token;
    let attrs = &input.attrs;
    let ident = &input.ident;
    let fields = &input.fields;
    let (impl_generics, _ty_generics, where_clause) = input.generics.split_for_impl();

    let fields_with_docs = fields.iter().map(|field| {
        let doc_comments = DtoFieldArguments::from_field(field)
            .ok()
            .into_iter()
            .flat_map(|d| d.list)
            .map(|arg| {
                let mut doc = String::new();
                if arg.pagination_by.is_some() {
                    doc.push_str("pagination_by, ");
                }
                if arg.filter {
                    doc.push_str("filter, ");
                }
                if arg.filter_like {
                    doc.push_str("filter_like, ");
                }
                if arg.order_by {
                    doc.push_str("order_by, ");
                }
                if arg.tags {
                    doc.push_str("tags, ");
                }
                let doc = doc.trim_end_matches(", ").to_string();
                quote! { #[doc = #doc] }
            });
        // Tri-state `Option<Option<T>>` update fields need `double_option` (plus the
        // `default` that maps an absent field to the outer `None`) to tell a present
        // `null` (clear) from an absent field (unchanged).
        let serde_tristate = is_double_option_type(field).then(|| {
            quote! { #[serde(default, deserialize_with = "ta_objects::serde_utils::double_option")] }
        });
        quote! { #serde_tristate #(#doc_comments)* #field, }
    });

    quote! {
        #[derive(
            Debug, Clone, ta_objects::DtoType,
            serde::Serialize, serde::Deserialize, utoipa::ToSchema
        )]
        #(#attrs)*
        #vis #struct_token #ident #impl_generics #where_clause {
            #(#fields_with_docs)*
        }
    }
    .into()
}

/// `true` when the field type is `Option<Option<…>>`.
fn is_double_option_type(f: &syn::Field) -> bool {
    matches!(option_inner_type(&f.ty), Some(inner) if option_inner_type(inner).is_some())
}

#[derive(Debug, FromDeriveInput)]
#[darling(attributes(dto))]
struct DtoArguments {
    list: Option<ListArguments>,
}

#[derive(Debug, FromMeta)]
struct ListArguments {
    #[darling(default)]
    on: Option<syn::Path>,
}

#[derive(Debug, Clone, Copy)]
enum PaginationDirection {
    Asc,
    Desc,
}

impl FromMeta for PaginationDirection {
    fn from_string(value: &str) -> darling::Result<Self> {
        match value {
            "+" => Ok(Self::Asc),
            "-" => Ok(Self::Desc),
            other => Err(darling::Error::unknown_value(other)),
        }
    }
}

#[derive(FromField)]
#[darling(attributes(dto))]
struct DtoFieldArguments {
    #[darling(multiple)]
    list: Vec<FieldListArguments>,
}

#[derive(FromMeta)]
struct FieldListArguments {
    #[darling(default)]
    pagination_by: Option<PaginationDirection>,
    #[darling(default)]
    filter: bool,
    #[darling(default)]
    filter_like: bool,
    #[darling(default)]
    order_by: bool,
    /// The field is a taggable entity's id: the list filters by that
    /// entity's tags as `<entity>.<key>:<op>:<value>`.
    #[darling(default)]
    tags: bool,
    /// The DAO column's type, when it differs from this DTO field's — the
    /// column is resolved by type, so only a retyped field needs it.
    #[darling(default)]
    col: Option<syn::Path>,
}

/// Derive macro that generates the Dto and optional ListDto trait impls for a DTO struct.
pub fn dto_type(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let parsed = match DtoArguments::from_derive_input(&input) {
        Ok(p) => p,
        Err(e) => return e.write_errors().into(),
    };
    let item = match parse_input_item_struct(&input) {
        Ok(item) => item,
        Err(e) => return e.to_compile_error().into(),
    };

    let ident = &item.ident;
    let fields = &item.fields;
    let (impl_generics, ty_generics, where_clause) = item.generics.split_for_impl();

    let dto = quote! {
        impl #impl_generics ta_objects::Dto for #ident #ty_generics #where_clause {}
    };

    let list_query = match parsed.list.as_ref().and_then(|l| l.on.as_ref()) {
        None => quote! {},
        Some(dao_ident) => match gen_list_query(
            ident,
            fields,
            dao_ident,
            &impl_generics,
            &ty_generics,
            where_clause,
        ) {
            Ok(ts) => ts,
            Err(e) => return e.to_compile_error().into(),
        },
    };

    let flat_machinery = match dlo_dto_flat_machinery(&item) {
        Ok(ts) => ts,
        Err(e) => return e.to_compile_error().into(),
    };

    quote! {
        #dto
        #list_query
        #flat_machinery
    }
    .into()
}

fn gen_list_query(
    ident: &Ident,
    fields: &syn::Fields,
    dao_ident: &syn::Path,
    impl_generics: &syn::ImplGenerics,
    ty_generics: &syn::TypeGenerics,
    where_clause: Option<&syn::WhereClause>,
) -> syn::Result<proc_macro2::TokenStream> {
    struct FieldMeta<'a> {
        ident: &'a Ident,
        ty: &'a syn::Type,
        dao_col_ty: Option<syn::Path>,
        is_pagination: bool,
        pagination_dir: PaginationDirection,
        is_order_by: bool,
        is_filter: bool,
        is_filter_like: bool,
        is_tags: bool,
    }

    let mut pagination_field: Option<&Ident> = None;
    let mut metas: Vec<FieldMeta> = Vec::new();

    for f in fields.iter() {
        let f_ident = f.ident.as_ref().unwrap();
        let dto_args = DtoFieldArguments::from_field(f)
            .map_err(|e| syn::Error::new_spanned(f_ident, e.to_string()))?;

        let dao_col_ty = dto_args.list.iter().find_map(|l| l.col.clone());

        let mut meta = FieldMeta {
            ident: f_ident,
            ty: &f.ty,
            dao_col_ty,
            is_pagination: false,
            pagination_dir: PaginationDirection::Asc,
            is_order_by: false,
            is_filter: false,
            is_filter_like: false,
            is_tags: false,
        };

        for list_arg in dto_args.list {
            if let Some(dir) = list_arg.pagination_by {
                if pagination_field.is_some() {
                    return Err(syn::Error::new_spanned(
                        ident,
                        "Only one field can be marked as pagination_by",
                    ));
                }
                meta.is_pagination = true;
                meta.is_order_by = true;
                meta.pagination_dir = dir;
                pagination_field = Some(f_ident);
            }
            if list_arg.order_by {
                meta.is_order_by = true;
            }
            if list_arg.filter {
                meta.is_filter = true;
            }
            if list_arg.filter_like {
                meta.is_filter_like = true;
            }
            if list_arg.tags {
                meta.is_tags = true;
            }
        }

        metas.push(meta);
    }

    if pagination_field.is_none() {
        return Err(syn::Error::new_spanned(
            ident,
            "#[dto(list(on = ...))] requires exactly one pagination_by field \
             naming a column unique within the listing",
        ));
    }

    let listed: Vec<_> = metas
        .iter()
        .filter(|m| m.is_filter || m.is_filter_like || m.is_order_by || m.is_tags)
        .collect();
    let fields = listed.iter().map(|m| {
        let name = m.ident.to_string();
        let f_ident = m.ident;
        let ty = m.ty;
        let col_ty = match &m.dao_col_ty {
            Some(path) => quote! { #path },
            None => quote! { #ty },
        };
        let roles = [
            (m.is_filter, quote! { Filter }),
            (m.is_filter_like, quote! { Like }),
            (m.is_order_by, quote! { Order }),
        ]
        .into_iter()
        .filter(|(marked, _)| *marked)
        .map(|(_, role)| quote! { ta_objects::ListRole::#role });
        let tags = match m.is_tags {
            true => quote! {
                Some(ta_objects::ListTags {
                    entity: <#ty as ta_objects::Taggable>::ENTITY,
                    entity_type: || {
                        ta_objects::SeaValue::as_sea_expr(&<#ty as ta_objects::Taggable>::ENTITY_TYPE)
                    },
                })
            },
            false => quote! { None },
        };
        quote! {
            ta_objects::ListField {
                name: #name,
                column: <#dao_ident as ta_objects::HasColumn<#col_ty>>::COL,
                roles: &[#(#roles),*],
                parse: |value| {
                    <#ty as ta_objects::ListFilterStr>::parse_filter_str(value)
                        .map(|value| Box::new(value) as Box<dyn ta_objects::SeaValue>)
                },
                value: |dto: &Self| ta_objects::ListFilterStr::as_string(&dto.#f_ident),
                tags: #tags,
            },
        }
    });
    let pag_index = listed.iter().position(|m| m.is_pagination).unwrap();
    let pag_dir = match listed[pag_index].pagination_dir {
        PaginationDirection::Desc => quote! { ta_objects::SortDir::Desc },
        PaginationDirection::Asc => quote! { ta_objects::SortDir::Asc },
    };
    let tags_marker = match listed.iter().any(|m| m.is_tags) {
        true => quote! { ta_objects::SomeTags },
        false => quote! { ta_objects::NoTags },
    };

    Ok(quote! {
        impl #impl_generics ta_objects::ListDto for #ident #ty_generics #where_clause {
            type Tags = #tags_marker;

            const LIST_FIELDS: &'static [ta_objects::ListField<Self>] = &[#(#fields)*];

            const PAGINATION_BY: (&'static ta_objects::ListField<Self>, ta_objects::SortDir) =
                (&Self::LIST_FIELDS[#pag_index], #pag_dir);
        }
    })
}
