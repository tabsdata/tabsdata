//
// Copyright 2025 Tabs Data Inc.
//

use crate::type_builder::{TdTypeFields, parse_input_item_struct, td_type};
use darling::{FromDeriveInput, FromField, FromMeta};
use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::{format_ident, quote};
use std::collections::HashMap;
use syn::{DeriveInput, ItemStruct, parse_macro_input};

pub fn dto(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);

    let vis = &input.vis;
    let struct_token = &input.struct_token;
    let attrs = &input.attrs;
    let ident = &input.ident;
    let fields = &input.fields;
    let (impl_generics, _ty_generics, where_clause) = input.generics.split_for_impl();

    // Generate doc comments for fields
    let fields_with_docs = fields.iter().map(|field| {
        let field_args = DtoFieldArguments::from_field(field).unwrap();

        // Generate doc comments based on field arguments
        let doc_comments = field_args.list.into_iter().map(|arg| {
            let mut field_doc = String::new();
            if arg.pagination_by.is_some() {
                field_doc.push_str("pagination_by, ");
            }
            if arg.filter {
                field_doc.push_str("filter, ");
            }
            if arg.filter_like {
                field_doc.push_str("filter_like, ");
            }
            if arg.order_by {
                field_doc.push_str("order_by, ");
            }
            let field_doc = field_doc.trim_end_matches(", ");

            quote! {
                #[doc = #field_doc]
            }
        });

        quote! {
            #(#doc_comments)*
            #field,
        }
    });

    let expanded = quote! {
        #[derive(Debug, Clone, td_type::DtoType, derive_builder::Builder, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
        #[builder(try_setter, setter(into))]
        #(#attrs)*
        #vis #struct_token #ident #impl_generics #where_clause {
            #(#fields_with_docs)*
        }
    };

    expanded.into()
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

pub fn dto_type(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let parsed_args = DtoArguments::from_derive_input(&input).unwrap();
    let item = parse_input_item_struct(&input);

    // Td type
    let td_type = td_type(&input, &item);

    // Typed generic
    let ident = &item.ident;
    let fields = &item.fields;
    let (impl_generics, ty_generics, where_clause) = item.generics.split_for_impl();

    // Dto specifics (list_query)
    let list_query = if let Some(list_on) = parsed_args
        .list
        .as_ref()
        .and_then(|list_args| list_args.on.as_ref())
    {
        // if it is a list DTO, we need to implement the ListQuery trait and check the fields are valid
        let (
            pagination_by,
            pagination_order,
            order_by_fields,
            filter_fields,
            filter_like_fields,
            field_type_map,
            field_dao_mapping,
        ) = fields.iter().fold(
            (
                None,
                String::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                HashMap::new(),
                HashMap::new(),
            ),
            |(
                mut pagination_by,
                mut pagination_order,
                mut order_by_fields,
                mut filter_fields,
                mut filter_like_fields,
                mut field_type_map,
                mut field_dao_mapping,
            ),
             f| {
                // Field mapping inverse path, from type to builder field
                let td_type_args = TdTypeFields::from_field(f).unwrap();

                // Given that try_from Dao Builder is required for list(on) DTOs,
                // we need to do the inverse builder mapping for query fields.
                for builder_arg in &td_type_args.builder {
                    if builder_arg.try_from.as_ref().is_none_or(|t| t == list_on)
                        && let Some(name) = &builder_arg.field
                    {
                        field_dao_mapping.insert(f.ident.as_ref().unwrap(), name.to_string());
                    }
                }

                // List arguments
                for args in DtoFieldArguments::from_field(f).unwrap().list {
                    if let Some(pag) = args.pagination_by {
                        if pagination_by.is_some() {
                            panic!("Only one field can be marked as pagination_by");
                        }
                        if pag != "+" && pag != "-" && !pag.is_empty() {
                            panic!(
                                "Unsupported pagination by {pag}. Only empty, + or - is allowed"
                            );
                        }
                        pagination_by = Some(f.ident.as_ref().unwrap());
                        pagination_order = pag;

                        order_by_fields.push(f.ident.as_ref().unwrap());
                        field_type_map.insert(f.ident.as_ref().unwrap(), &f.ty);
                    } else if args.order_by {
                        order_by_fields.push(f.ident.as_ref().unwrap());
                        field_type_map.insert(f.ident.as_ref().unwrap(), &f.ty);
                    }
                    if args.filter {
                        filter_fields.push(f.ident.as_ref().unwrap());
                        field_type_map.insert(f.ident.as_ref().unwrap(), &f.ty);
                    }
                    if args.filter_like {
                        filter_like_fields.push(f.ident.as_ref().unwrap());
                        field_type_map.insert(f.ident.as_ref().unwrap(), &f.ty);
                    }
                }
                (
                    pagination_by,
                    pagination_order,
                    order_by_fields,
                    filter_fields,
                    filter_like_fields,
                    field_type_map,
                    field_dao_mapping,
                )
            },
        );

        if pagination_by.is_none() {
            panic!("A field must be marked as pagination_by");
        }

        let field_names = field_type_map.keys();
        let field_types = field_type_map.values();

        let field_dao_names = field_dao_mapping.keys();
        let field_dao_mapping = field_dao_mapping.values();

        quote! {
            impl #impl_generics crate::types::ListQuery for #ident #ty_generics #where_clause {
                type Dao = #list_on;

                fn try_from_dao(dao: &Self::Dao) -> Result<Self, td_error::TdError> {
                    let builder = <#ident as crate::types::DataTransferObject>::Builder::try_from(dao)?;
                    builder.build().map_err(Into::into)
                }

                fn map_dao_field(name: &str) -> String {
                    match name {
                        #(
                            stringify!(#field_dao_names) => #field_dao_mapping.to_string(),
                        )*
                        _ => name.to_string(),
                    }
                }

                fn map_sql_entity_value(
                    name: &str,
                    filter_value: &str,
                ) -> Result<Option<Box<dyn crate::types::SqlEntity>>, td_error::TdError> {
                    use crate::types::SqlEntity;
                    match name {
                        #(
                            stringify!(#field_names) => <#field_types>::from_display(filter_value)
                                .map(|v| Box::new(v) as Box<dyn crate::types::SqlEntity>)
                                .map(Some)
                                .map_err(Into::into),
                        )*
                        _ => Ok(None)
                    }
                }

                fn pagination_by() -> &'static str {
                    concat!(stringify!(#pagination_by), #pagination_order)
                }

                fn pagination_value(&self) -> String {
                    use crate::types::SqlEntity;
                    self.#pagination_by.as_display()
                }

                fn order_by_fields() -> &'static [&'static str] {
                    &[#(stringify!(#order_by_fields)),*]
                }

                fn order_by_str_value(&self, ordered_by_field: &Option<String>) -> Option<String> {
                    use crate::types::SqlEntity;
                    if let Some(ordered_by_field) = ordered_by_field {
                        match ordered_by_field.as_str() {
                            #(stringify!(#order_by_fields) => Some(self.#order_by_fields.as_display()),)*
                            _ => None,
                        }
                    } else {
                        None
                    }
                }

                fn filter_by_fields() -> &'static [&'static str] {
                    &[#(stringify!(#filter_fields)),*]
                }

                fn filter_by_like_fields() -> &'static [&'static str] {
                    &[#(stringify!(#filter_like_fields)),*]
                }

            }
        }
    } else {
        quote! {}
    };

    let builder_type = format_ident!("{}Builder", &item.ident);
    let expanded = quote! {
        impl #impl_generics crate::types::DataTransferObject for #ident #ty_generics #where_clause {
            type Builder = #builder_type #ty_generics;
        }

        #list_query
        #td_type
    };

    expanded.into()
}
