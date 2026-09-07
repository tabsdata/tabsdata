//
// Copyright 2025 Tabsdata Inc.
//

use crate::dao::option_inner_type;
use crate::type_builder::parse_input_item_struct;
use darling::{FromDeriveInput, FromField, FromMeta};
use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::{format_ident, quote};
use syn::{DeriveInput, ItemStruct, parse_macro_input};

/// Attribute macro that wraps a struct with DTO derives (DtoType, Builder, Serialize, etc.).
pub fn dto(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);

    let vis = &input.vis;
    let struct_token = &input.struct_token;
    let attrs = &input.attrs;
    let ident = &input.ident;
    let fields = &input.fields;
    let (impl_generics, _ty_generics, where_clause) = input.generics.split_for_impl();

    // Generate doc comments for fields summarising their list roles.
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
    on: Option<Ident>,
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
    pagination_by: Option<String>,
    #[darling(default)]
    filter: bool,
    #[darling(default)]
    filter_like: bool,
    #[darling(default)]
    order_by: bool,
    /// DAO column backing this DTO field when the names differ.
    #[darling(default)]
    col: Option<String>,
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

    // DataTransferObject are always emitted.
    let dto = quote! {
        impl #impl_generics ta_objects::Dto for #ident #ty_generics #where_clause {}
    };

    // ListDto is only emitted when #[dto(list(on = SomeDao))] is present.
    let list_query = match parsed.list.as_ref().and_then(|l| l.on.as_ref()) {
        None => quote! {},
        Some(dao_ident) => match gen_list_query(
            ident,
            fields,
            dao_ident,
            &impl_generics,
            &ty_generics,
            where_clause,
            item.generics.params.is_empty(),
        ) {
            Ok(ts) => ts,
            Err(e) => return e.to_compile_error().into(),
        },
    };

    quote! {
        #dto
        #list_query
    }
    .into()
}

/// Generate the `ListDto` trait implementation for query filtering, ordering, and pagination.
#[allow(clippy::too_many_arguments)]
fn gen_list_query(
    ident: &Ident,
    fields: &syn::Fields,
    dao_ident: &Ident,
    impl_generics: &syn::ImplGenerics,
    ty_generics: &syn::TypeGenerics,
    where_clause: Option<&syn::WhereClause>,
    registrable: bool,
) -> syn::Result<proc_macro2::TokenStream> {
    // Collect per-field metadata in one pass.
    struct FieldMeta<'a> {
        ident: &'a Ident,
        ty: &'a syn::Type,
        dao_field: Option<String>, // DAO field name this DTO field maps to
        is_pagination: bool,
        pagination_dir: String, // "+" | "-"
        is_order_by: bool,
        is_filter: bool,
        is_filter_like: bool,
    }

    let mut pagination_field: Option<&Ident> = None;
    let mut metas: Vec<FieldMeta> = Vec::new();

    for f in fields.iter() {
        let f_ident = f.ident.as_ref().unwrap();
        let dto_args = DtoFieldArguments::from_field(f)
            .map_err(|e| syn::Error::new_spanned(f_ident, e.to_string()))?;

        let dao_field = dto_args.list.iter().find_map(|l| l.col.clone());

        let mut meta = FieldMeta {
            ident: f_ident,
            ty: &f.ty,
            dao_field,
            is_pagination: false,
            pagination_dir: "+".into(),
            is_order_by: false,
            is_filter: false,
            is_filter_like: false,
        };

        for list_arg in dto_args.list {
            if let Some(dir) = list_arg.pagination_by {
                if pagination_field.is_some() {
                    return Err(syn::Error::new_spanned(
                        ident,
                        "Only one field can be marked as pagination_by",
                    ));
                }
                let dir = if dir == "-" { "-".into() } else { "+".into() };
                meta.is_pagination = true;
                meta.is_order_by = true; // pagination field is always orderable
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
        }

        metas.push(meta);
    }

    let pag_field = pagination_field.ok_or_else(|| {
        syn::Error::new_spanned(
            ident,
            "#[dto(list(on = ...))] requires exactly one pagination_by field \
             naming a column unique within the listing",
        )
    })?;

    // The DAO's Iden enum — by convention FooDB → FooDBIden
    let dao_iden = format_ident!("{}Iden", dao_ident);

    // The DAO column backing a DTO field: `col = "..."` override, else same
    // name. Resolved through the DAO Iden's `FromStr` — a column of a
    // `#[dao(flatten)]` base is not a direct variant of the DAO's Iden enum.
    let dao_col_name =
        |f: &FieldMeta| -> String { f.dao_field.clone().unwrap_or_else(|| f.ident.to_string()) };
    let dao_col_expr = |f: &FieldMeta| -> proc_macro2::TokenStream {
        let col = dao_col_name(f);
        quote! { <#dao_iden as std::str::FromStr>::from_str(#col).ok() }
    };

    // filter_col match arms
    let filter_arms = metas.iter().filter(|m| m.is_filter).map(|m| {
        let name = m.ident.to_string();
        let col = dao_col_expr(m);
        quote! { #name => #col, }
    });

    // filter_like_col match arms
    let filter_like_arms = metas.iter().filter(|m| m.is_filter_like).map(|m| {
        let name = m.ident.to_string();
        let col = dao_col_expr(m);
        quote! { #name => #col, }
    });

    // order_by_col match arms
    let order_by_arms = metas.iter().filter(|m| m.is_order_by).map(|m| {
        let name = m.ident.to_string();
        let col = dao_col_expr(m);
        quote! { #name => #col, }
    });

    // pagination_col
    let pag_meta = metas.iter().find(|m| m.is_pagination).unwrap();
    let pag_col_name = dao_col_name(pag_meta);
    let pag_dir = if pag_meta.pagination_dir == "-" {
        quote! { sea_query::Order::Desc }
    } else {
        quote! { sea_query::Order::Asc }
    };

    // parse_filter_value match arms — all fields that participate in filtering
    // or ordering, matched by column name (the Iden may be a nested enum).
    let filterable_fields: Vec<_> = metas
        .iter()
        .filter(|m| m.is_filter || m.is_order_by)
        .collect();
    let parse_arms = filterable_fields.iter().map(|m| {
        let col = dao_col_name(m);
        let ty = m.ty;
        quote! {
            #col => <#ty as ta_objects::ListFilterStr>::parse_filter_str(value)
                .map(|v: #ty| Box::new(v) as Box<dyn ta_objects::SeaValue>)
                .map(Some),
        }
    });

    // order_by_value match arms
    let order_by_value_arms = metas.iter().filter(|m| m.is_order_by).map(|m| {
        let col = dao_col_name(m);
        let f_ident = m.ident;
        quote! { #col => Some(self.#f_ident.as_string()), }
    });

    // Field-to-column resolution happens at runtime: forcing every declared
    // role here turns a field/`col = ".."` typo into a registry-walking test
    // failure instead of a silently dead filter or a first-call panic.
    let filter_names: Vec<String> = metas
        .iter()
        .filter(|m| m.is_filter)
        .map(|m| m.ident.to_string())
        .collect();
    let filter_like_names: Vec<String> = metas
        .iter()
        .filter(|m| m.is_filter_like)
        .map(|m| m.ident.to_string())
        .collect();
    let order_by_names: Vec<String> = metas
        .iter()
        .filter(|m| m.is_order_by)
        .map(|m| m.ident.to_string())
        .collect();
    let list_dto_registration = if registrable {
        quote! {
            #[cfg(not(test))]
            ta_objects::inventory::submit! {
                ta_objects::RegisteredListDto {
                    name: stringify!(#ident),
                    validate: || {
                        let _ = <#ident as ta_objects::ListDto>::pagination_col();
                        #(assert!(
                            <#ident as ta_objects::ListDto>::filter_col(#filter_names).is_some(),
                            concat!(
                                stringify!(#ident), ": filter field `", #filter_names,
                                "` does not resolve to a DAO column",
                            ),
                        );)*
                        #(assert!(
                            <#ident as ta_objects::ListDto>::filter_like_col(#filter_like_names)
                                .is_some(),
                            concat!(
                                stringify!(#ident), ": filter_like field `", #filter_like_names,
                                "` does not resolve to a DAO column",
                            ),
                        );)*
                        #(assert!(
                            <#ident as ta_objects::ListDto>::order_by_col(#order_by_names)
                                .is_some(),
                            concat!(
                                stringify!(#ident), ": order_by field `", #order_by_names,
                                "` does not resolve to a DAO column",
                            ),
                        );)*
                    },
                }
            }
        }
    } else {
        quote! {}
    };

    Ok(quote! {
        #list_dto_registration

        impl #impl_generics ta_objects::ListDto for #ident #ty_generics #where_clause {

            fn filter_col(field: &str) -> Option<<Self::Dao as ta_objects::TypedColumns>::Iden> {
                match field {
                    #(#filter_arms)*
                    _ => None,
                }
            }

            fn filter_like_col(field: &str) -> Option<<Self::Dao as ta_objects::TypedColumns>::Iden> {
                match field {
                    #(#filter_like_arms)*
                    _ => None,
                }
            }

            fn order_by_col(field: &str) -> Option<<Self::Dao as ta_objects::TypedColumns>::Iden> {
                match field {
                    #(#order_by_arms)*
                    _ => None,
                }
            }

            fn pagination_col() -> (<Self::Dao as ta_objects::TypedColumns>::Iden, sea_query::Order) {
                let col = <#dao_iden as std::str::FromStr>::from_str(#pag_col_name)
                    .expect("pagination_by names a DAO column");
                (col, #pag_dir)
            }

            fn parse_filter_value(
                field: &<Self::Dao as ta_objects::TypedColumns>::Iden,
                value: &str,
            ) -> Result<Option<Box<dyn ta_objects::SeaValue>>, td_error::TdError> {
                match sea_query::Iden::to_string(field).as_str() {
                    #(#parse_arms)*
                    _ => Ok(None),
                }
            }

            fn pagination_value(&self) -> String {
                self.#pag_field.to_string()
            }

            fn order_by_value(&self, field: &<Self::Dao as ta_objects::TypedColumns>::Iden) -> Option<String> {
                use ta_objects::ListFilterStr;
                match sea_query::Iden::to_string(field).as_str() {
                    #(#order_by_value_arms)*
                    _ => None,
                }
            }

        }
    })
}
