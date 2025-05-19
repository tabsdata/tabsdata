//
// Copyright 2025 Tabs Data Inc.
//

use crate::type_builder::{parse_input_item_struct, td_type};
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
        let (pagination_by, pagination_order, order_by_fields, filter_fields, filter_like_fields) =
            fields.iter().fold(
                (None, String::new(), Vec::new(), Vec::new(), Vec::new()),
                |(
                    mut pagination_by,
                    mut pagination_order,
                    mut order_by_fields,
                    mut filter_fields,
                    mut filter_like_fields,
                ),
                 f| {
                    for args in DtoFieldArguments::from_field(f).unwrap().list {
                        if let Some(pag) = args.pagination_by {
                            if pagination_by.is_some() {
                                panic!("Only one field can be marked as pagination_by");
                            }
                            if pag != "+" && pag != "-" && !pag.is_empty() {
                                panic!(
                                    "Unsupported pagination by {}. Only empty, + or - is allowed",
                                    pag
                                );
                            }
                            pagination_by = Some(f.ident.as_ref().unwrap());
                            pagination_order = pag;

                            order_by_fields.push(f.ident.as_ref().unwrap());
                        } else if args.order_by {
                            order_by_fields.push(f.ident.as_ref().unwrap());
                        }
                        if args.filter {
                            filter_fields.push(f.ident.as_ref().unwrap());
                        }
                        if args.filter_like {
                            filter_like_fields.push(f.ident.as_ref().unwrap());
                        }
                    }
                    (
                        pagination_by,
                        pagination_order,
                        order_by_fields,
                        filter_fields,
                        filter_like_fields,
                    )
                },
            );

        if pagination_by.is_none() {
            panic!("A field must be marked as pagination_by");
        }

        quote! {
            impl #impl_generics crate::types::ListQuery for #ident #ty_generics #where_clause {
                type Dao = #list_on;

                fn try_from_dao(dao: &Self::Dao) -> Result<Self, td_error::TdError> {
                    let builder = <#ident as crate::types::DataTransferObject>::Builder::try_from(dao)?;
                    builder.build().map_err(Into::into)
                }

                fn pagination_by() -> &'static str {
                    concat!(stringify!(#pagination_by), #pagination_order)
                }

                fn pagination_value(&self) -> String {
                    self.#pagination_by().to_string()
                }

                fn order_by_fields() -> &'static [&'static str] {
                    &[#(stringify!(#order_by_fields)),*]
                }

                fn order_by_str_value(&self, ordered_by_field: &Option<String>) -> Option<String> {
                    if let Some(ordered_by_field) = ordered_by_field {
                        match ordered_by_field.as_str() {
                            #(stringify!(#order_by_fields) => Some(self.#order_by_fields().to_string()),)*
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
