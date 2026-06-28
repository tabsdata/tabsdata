//
// Copyright 2025 Tabs Data Inc.
//

use crate::dao::{iden_variant, option_inner_type};
use crate::type_builder::{DxoFields, parse_input_item_struct, td_type};
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

    // If #[dto(list(on = X))] is present, inject #[dxo(builder(try_from = X))]
    // automatically — no need to write it manually on the struct.
    let list_dao = DtoArguments::from_derive_input(&input.clone().into())
        .ok()
        .and_then(|a| a.list)
        .and_then(|l| l.on);
    let injected_builder = list_dao.map(|dao| {
        quote! {
            #[dxo(builder(try_from = #dao))]
        }
    });

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
            Debug, Clone, ta_objects::DtoType, derive_builder::Builder,
            serde::Serialize, serde::Deserialize, utoipa::ToSchema
        )]
        #[builder(try_setter, setter(into))]
        #injected_builder
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
    let td_type = td_type(&input, &item);

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
        ) {
            Ok(ts) => ts,
            Err(e) => return e.to_compile_error().into(),
        },
    };

    quote! {
        #dto
        #list_query
        #td_type
    }
    .into()
}

/// Generate the `ListDto` trait implementation for query filtering, ordering, and pagination.
fn gen_list_query(
    ident: &Ident,
    fields: &syn::Fields,
    dao_ident: &Ident,
    impl_generics: &syn::ImplGenerics,
    ty_generics: &syn::TypeGenerics,
    where_clause: Option<&syn::WhereClause>,
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
        let td_args = DxoFields::from_field(f)
            .map_err(|e| syn::Error::new_spanned(f_ident, e.to_string()))?;
        let dto_args = DtoFieldArguments::from_field(f)
            .map_err(|e| syn::Error::new_spanned(f_ident, e.to_string()))?;

        // DAO field mapping from #[dxo(builder(try_from = DaoIdent, field = "dao_field"))]
        let dao_field = td_args
            .builder
            .iter()
            .find(|b| b.try_from.as_ref().is_none_or(|t| t == dao_ident))
            .and_then(|b| b.field.as_ref())
            .map(|f| f.to_string());

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
            "#[dto(list(on = ...))] requires exactly one pagination_by field",
        )
    })?;

    // The DAO's Iden enum — by convention FooDB → FooDBIden
    let dao_iden = format_ident!("{}Iden", dao_ident);

    // Resolve a DTO field name to the DAO Iden variant.
    // If a dao_field mapping exists, use that; otherwise assume same name.
    let dao_col_for = |f: &FieldMeta| -> proc_macro2::TokenStream {
        let dao_field_name = f.dao_field.clone().unwrap_or_else(|| f.ident.to_string());
        let variant = iden_variant(&format_ident!("{}", &dao_field_name));
        quote! { #dao_iden::#variant }
    };

    // filter_col match arms
    let filter_arms = metas.iter().filter(|m| m.is_filter).map(|m| {
        let name = m.ident.to_string();
        let col = dao_col_for(m);
        quote! { #name => Some(#col), }
    });

    // filter_like_col match arms
    let filter_like_arms = metas.iter().filter(|m| m.is_filter_like).map(|m| {
        let name = m.ident.to_string();
        let col = dao_col_for(m);
        quote! { #name => Some(#col), }
    });

    // order_by_col match arms
    let order_by_arms = metas.iter().filter(|m| m.is_order_by).map(|m| {
        let name = m.ident.to_string();
        let col = dao_col_for(m);
        quote! { #name => Some(#col), }
    });

    // pagination_col
    let pag_meta = metas.iter().find(|m| m.is_pagination).unwrap();
    let pag_col = dao_col_for(pag_meta);
    let pag_dir = if pag_meta.pagination_dir == "-" {
        quote! { sea_query::Order::Desc }
    } else {
        quote! { sea_query::Order::Asc }
    };

    // parse_filter_value match arms — all fields that participate in filtering or ordering
    let filterable_fields: Vec<_> = metas
        .iter()
        .filter(|m| m.is_filter || m.is_order_by)
        .collect();
    let parse_arms = filterable_fields.iter().map(|m| {
        let col = dao_col_for(m);
        let ty = m.ty;
        quote! {
            #col => <#ty as ta_objects::ListFilterStr>::parse_filter_str(value)
                .map(|v: #ty| Box::new(v) as Box<dyn ta_objects::SeaValue>)
                .map(Some),
        }
    });

    // order_by_value match arms
    let order_by_value_arms = metas.iter().filter(|m| m.is_order_by).map(|m| {
        let name = dao_col_for(m);
        let f_ident = m.ident;
        quote! { #name => Some(self.#f_ident.as_string()), }
    });

    Ok(quote! {
        impl #impl_generics ta_objects::ListDto for #ident #ty_generics #where_clause {
            type Dao = #dao_ident;

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
                (#pag_col, #pag_dir)
            }

            fn parse_filter_value(
                field: &<Self::Dao as ta_objects::TypedColumns>::Iden,
                value: &str,
            ) -> Result<Option<Box<dyn ta_objects::SeaValue>>, td_error::TdError> {
                match field {
                    #(#parse_arms)*
                    _ => Ok(None),
                }
            }

            fn pagination_value(&self) -> String {
                self.#pag_field.to_string()
            }

            fn order_by_value(&self, field: &<Self::Dao as ta_objects::TypedColumns>::Iden) -> Option<String> {
                use ta_objects::ListFilterStr;
                match field {
                    #(#order_by_value_arms)*
                    _ => None,
                }
            }

            fn from_dao(dao: &Self::Dao) -> Result<Self, td_error::TdError> {
                use ta_objects::Buildable;
                let builder = <Self as Buildable>::Builder::try_from(dao)?;
                Self::build(&builder)
            }
        }
    })
}
