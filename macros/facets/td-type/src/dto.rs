//
// Copyright 2025 Tabs Data Inc.
//

use crate::dao::gen_fields_as_list;
use crate::type_builder::{parse_input_item_struct, td_type, TdTypeFields};
use darling::{FromDeriveInput, FromField, FromMeta};
use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::{format_ident, quote};
use syn::{parse_macro_input, DeriveInput, ItemStruct};

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
        let field_vis = &field.vis;
        let field_args = DtoFieldArguments::from_field(field).unwrap();

        // Generate doc comments based on field arguments
        let doc_comments = field_args.list.iter().map(|arg| {
            let mut field_doc = String::new();
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
            #field_vis #field,
        }
    });

    let expanded = quote! {
        #[derive(Debug, Clone, Eq, PartialEq, td_type::DtoType, derive_builder::Builder, getset::Getters, serde::Serialize, serde::Deserialize)]
        #[td_apiforge::apiserver_schema]
        #[builder(try_setter, setter(into))]
        #[getset(get = "pub")]
        #(#attrs)*
        #vis #struct_token #ident #impl_generics #where_clause {
            #(#fields_with_docs)*
        }
    };

    expanded.into()
}

#[derive(FromDeriveInput)]
#[darling(attributes(dto))]
struct DtoArguments {
    list: Option<ListArguments>,
}

#[derive(FromMeta)]
struct ListArguments {
    #[darling(default)]
    on: Option<Ident>,
    #[darling(default)]
    natural_order_by: Option<String>,
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
        let natural_order_by = parsed_args
            .list
            .as_ref()
            .and_then(|list_args| list_args.natural_order_by.clone())
            .unwrap_or(gen_fields_as_list(fields)[0].to_string()); // use first field as default

        let (order_by_fields, filter_fields, filter_like_fields) = fields.iter().fold(
            (Vec::new(), Vec::new(), Vec::new()),
            |(mut order_by_fields, mut filter_fields, mut filter_like_fields), f| {
                let td_type_args = TdTypeFields::from_field(f).unwrap();
                for args in DtoFieldArguments::from_field(f).unwrap().list {
                    let field = td_type_args.builder.iter().find_map(|arg| {
                        if arg.field.is_none()
                            || arg.field == Some(f.ident.as_ref().unwrap().to_string())
                        {
                            arg.try_from.as_ref()
                        } else {
                            None
                        }
                    });
                    let field = field.unwrap_or(f.ident.as_ref().unwrap()).to_string();
                    let field = quote! { #field };

                    if args.order_by {
                        order_by_fields.push(field.clone());
                    }
                    if args.filter {
                        filter_fields.push(field.clone());
                    }
                    if args.filter_like {
                        filter_like_fields.push(field);
                    }
                }
                (order_by_fields, filter_fields, filter_like_fields)
            },
        );

        quote! {
            impl #impl_generics crate::types::ListQuery for #ident #ty_generics #where_clause {
                type Dao = #list_on;

                fn try_from_dao(dao: &Self::Dao) -> Result<Self, td_error::TdError> {
                    let builder = <#ident as crate::types::DataTransferObject>::Builder::try_from(dao)?;
                    builder.build().map_err(Into::into)
                }

                fn natural_order_by() -> &'static str {
                    #natural_order_by
                }

                fn order_by_fields() -> &'static [&'static str] {
                    &[#(#order_by_fields),*]
                }

                fn filter_by_fields() -> &'static [&'static str] {
                    &[#(#filter_fields),*]
                }

                fn filter_by_like_fields() -> &'static [&'static str] {
                    &[#(#filter_like_fields),*]
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
