//
// Copyright 2025 Tabs Data Inc.
//

use crate::type_builder::{parse_input_item_struct, td_type};
use darling::{FromDeriveInput, FromMeta};
use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::{format_ident, quote, ToTokens};
use syn::{parse_macro_input, DeriveInput, Fields, ItemStruct, Type};

pub fn dao(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);

    // Expansion
    let expanded = quote! {
        #[derive(Debug, Clone, Eq, PartialEq, td_type::DaoType, derive_builder::Builder, getset::Getters, serde::Serialize, serde::Deserialize, sqlx::FromRow)]
        #[builder(try_setter, setter(into))]
        #[getset(get = "pub")]
        #input
    };

    expanded.into()
}

#[derive(FromDeriveInput)]
#[darling(attributes(dao))]
struct DaoArguments {
    sql_table: Option<String>,
    order_by: Option<String>,
    partition_by: Option<String>,
    versioned_at: Option<VersionedAtArguments>,
    recursive: Option<DaoRecursiveArguments>,
}

#[derive(FromMeta)]
struct VersionedAtArguments {
    order_by: String,
    condition_by: String,
}

#[derive(FromMeta)]
struct DaoRecursiveArguments {
    up: String,
    down: String,
}

pub fn dao_type(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let parsed_args = DaoArguments::from_derive_input(&input).unwrap();
    let item = parse_input_item_struct(&input);

    // Td type
    let td_type = td_type(&input, &item);

    // Typed generic
    let ident = &item.ident;
    let fields = &item.fields;
    let (impl_generics, ty_generics, where_clause) = item.generics.split_for_impl();

    let field_names = gen_fields_as_list(fields);
    let field_types = gen_field_types_as_list(fields);
    let immutable_field_names = gen_immutable_fields_as_list(fields);

    // Dao specifics
    let sql_table = match parsed_args.sql_table {
        Some(table) => {
            let table = table.as_str();
            quote! { #table }
        }
        None => {
            let table = item.ident.to_string().to_lowercase();
            quote! { #table }
        }
    };
    let order_by = match parsed_args.order_by {
        Some(order_by) => {
            let order_by = order_by.as_str();
            quote! { concat!("ORDER BY ", #order_by) }
        }
        None => {
            quote! { "ORDER BY 1 DESC" }
        }
    };
    let partition_by = match parsed_args.partition_by {
        Some(partition_by) => {
            let partition_by = partition_by.as_str();
            let partition_by_type = type_for_field(fields, partition_by);
            quote! {
                impl #impl_generics crate::types::PartitionBy for #ident #ty_generics #where_clause {
                    type PartitionBy = #partition_by_type;
                    fn partition_by() -> &'static str {
                        #partition_by
                    }
                }
            }
        }
        None => {
            quote! {}
        }
    };
    let versioned_at = match parsed_args.versioned_at {
        Some(versioned_at) => {
            let order_by = versioned_at.order_by.as_str();
            let order_type = type_for_field(fields, order_by);

            let condition_by = versioned_at.condition_by.as_str();
            let condition_type = type_for_field(fields, condition_by);

            quote! {
                impl #impl_generics crate::types::VersionedAt for #ident #ty_generics #where_clause {
                    type Order = #order_type;
                    fn order_by() -> &'static str {
                        #order_by
                    }

                    type Condition = #condition_type;
                    fn condition_by() -> &'static str {
                        #condition_by
                    }
                }
            }
        }
        None => {
            quote! {}
        }
    };
    let recursive = match parsed_args.recursive {
        Some(recursive) => {
            let up = recursive.up;
            let down = recursive.down;
            let up_type = type_for_field(fields, &up);
            let down_type = type_for_field(fields, &down);

            if up_type != down_type {
                panic!("Recursive types must be the same");
            }

            quote! {
                impl #impl_generics crate::types::Recursive for #ident #ty_generics #where_clause {
                    type Recursive = #up_type;

                    fn recurse_up() -> &'static str {
                        #up
                    }

                    fn recurse_down() -> &'static str {
                        #down
                    }
                }
            }
        }
        None => {
            quote! {}
        }
    };

    let builder_type = format_ident!("{}Builder", &item.ident);
    let expanded = quote! {
        impl #impl_generics crate::types::DataAccessObject for #ident #ty_generics #where_clause {
            type Builder = #builder_type #ty_generics;

            fn sql_table() -> &'static str {
                #sql_table
            }

            fn order_by() -> &'static str {
                #order_by
            }

            fn fields() -> &'static [&'static str] {
                &[#(stringify!(#field_names)),*]
            }

            fn immutable_fields() -> &'static [&'static str] {
                &[#(stringify!(#immutable_field_names)),*]
            }

            fn sql_field_for_type(val: &str) -> Option<&'static str> {
                match val {
                    #(
                        v if v == std::any::type_name::<#field_types>() => Some(stringify!(#field_names)),
                    )*
                    _ => None,
                }
            }

            fn values_query_builder(
                &self,
                sql: String,
                bindings: &[&str],
            ) -> sqlx::QueryBuilder<'_, sqlx::Sqlite> {
                let mut query_builder = sqlx::QueryBuilder::new(sql);
                query_builder.push_values(std::iter::once(self), |mut b, dao| {
                    #(
                        if bindings.contains(&stringify!(#field_names)) {
                            b.push_bind(&dao.#field_names);
                        }
                    )*
                });
                query_builder
            }

            fn tuples_query_builder(
                &self,
                sql: String,
                bindings: &[&str],
            ) -> sqlx::QueryBuilder<'_, sqlx::Sqlite> {
                let mut query_builder = sqlx::QueryBuilder::new(sql);
                let mut separated = query_builder.separated(", ");
                #(
                    let field_name = stringify!(#field_names);
                    if bindings.contains(&field_name) {
                        if Self::immutable_fields().contains(&field_name) {
                            separated.push(format!(r"{v} = CASE WHEN {v} IS NULL THEN ", v = field_name));
                            separated.push_bind_unseparated(&self.#field_names);
                            separated.push_unseparated(format!(r" ELSE {v} END", v = field_name));
                        } else {
                            separated.push(format!("{v} = COALESCE(", v = field_name));
                            separated.push_bind_unseparated(&self.#field_names);
                            separated.push_unseparated(format!(", {v})", v = field_name));
                        }
                    }
                )*
                query_builder
            }
        }

        #td_type
        #partition_by
        #versioned_at
        #recursive
    };

    expanded.into()
}

// Option<T> and T is the same, as NULL comparisons are always False in Sql
fn type_for_field<'a>(fields: &'a Fields, field_name: &str) -> &'a Type {
    let field_type = fields
        .iter()
        .find_map(|f| {
            if f.ident.as_ref().is_some_and(|ident| ident == field_name) {
                Some(&f.ty)
            } else {
                None
            }
        })
        .unwrap_or_else(|| panic!("Field {} not found in struct", field_name));

    if let Type::Path(type_path) = field_type {
        if let Some(segment) = type_path.path.segments.last() {
            if segment.ident == "Option" {
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    if let Some(syn::GenericArgument::Type(inner_type)) = args.args.first() {
                        return inner_type;
                    }
                }
            }
        }
    }

    field_type
}

pub fn gen_fields_as_list(fields: &Fields) -> Vec<&Ident> {
    fields
        .iter()
        .filter(|f| {
            // Check if the field does NOT have the `#[sqlx(skip)]` attribute
            !f.attrs.iter().any(|attr| {
                attr.path().is_ident("sqlx") && attr.to_token_stream().to_string().contains("skip")
            })
        })
        .filter_map(|f| f.ident.as_ref())
        .collect()
}

fn gen_immutable_fields_as_list(fields: &Fields) -> Vec<&Ident> {
    fields
        .iter()
        .filter(|f| {
            // Check if the field does have the `#[dao(immutable)]` attribute
            f.attrs.iter().any(|attr| {
                attr.path().is_ident("dao")
                    && attr.to_token_stream().to_string().contains("immutable")
            })
        })
        .filter_map(|f| f.ident.as_ref())
        .collect()
}

fn gen_field_types_as_list(fields: &Fields) -> Vec<&Type> {
    fields
        .iter()
        .filter(|f| {
            // Check if the field does NOT have the `#[sqlx(skip)]` attribute
            !f.attrs.iter().any(|attr| {
                attr.path().is_ident("sqlx") && attr.to_token_stream().to_string().contains("skip")
            })
        })
        .map(|f| &f.ty)
        .collect()
}
