//
// Copyright 2025 Tabs Data Inc.
//

//! Proc macros to generate default derives for the dao/dlo/dto types.

use darling::{FromDeriveInput, FromField, FromMeta};
use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::{format_ident, quote, ToTokens};
use syn::{parse_macro_input, DeriveInput, Fields, ItemStruct, Type};
use td_shared::parse_meta;

#[derive(FromMeta)]
struct DaoArguments {
    sql_table: Option<String>,
}

pub fn dao(args: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);
    let parsed_args = parse_meta!(DaoArguments, args).unwrap();

    let sql_table = match parsed_args.sql_table {
        Some(table) => {
            let table = table.as_str();
            quote! { #table }
        }
        None => {
            let table = input.ident.to_string().to_lowercase();
            quote! { #table }
        }
    };
    let ident = &input.ident;
    let fields = &input.fields;
    let ty_generics = &input.generics;
    let where_clause = &input.generics.where_clause;

    let field_names = gen_fields_as_list(fields);
    let field_types = gen_field_types_as_list(fields);

    let expanded = quote! {
        #[derive(Debug, Default, Clone, td_type::TdType, derive_builder::Builder, getset::Getters, serde::Serialize, serde::Deserialize, sqlx::FromRow)]
        #[builder(try_setter, setter(into))]
        #[getset(get = "pub")]
        #input

        impl<#ty_generics> crate::types::DataAccessObject for #ident #ty_generics #where_clause {
            fn sql_table() -> &'static str {
                #sql_table
            }

            fn fields() -> &'static [&'static str] {
                &[#(stringify!(#field_names)),*]
            }

            fn sql_field_for_type<E: crate::types::SqlEntity>() -> Option<&'static str> {
                match std::any::type_name::<E>() {
                    #(
                        id if id == std::any::type_name::<#field_types>() => Some(stringify!(#field_names)),
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
                    if bindings.contains(&stringify!(#field_names)) {
                        separated.push(format!("{} = ", stringify!(#field_names)));
                        separated.push_bind_unseparated(&self.#field_names);
                    }
                )*
                query_builder
            }
        }
    };

    expanded.into()
}

pub fn dlo(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);

    let ident = &input.ident;
    let ty_generics = &input.generics;
    let where_clause = &input.generics.where_clause;

    let expanded = quote! {
        #[derive(Debug, Default, Clone, td_type::TdType, derive_builder::Builder, getset::Getters, serde::Serialize, serde::Deserialize, sqlx::FromRow)]
        #[builder(try_setter, setter(into))]
        #[getset(get = "pub")]
        #input

        impl<#ty_generics> crate::types::DataLogicObject for #ident #ty_generics #where_clause {}
    };

    expanded.into()
}

pub fn dto(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);

    let ident = &input.ident;
    let ty_generics = &input.generics;
    let where_clause = &input.generics.where_clause;

    let expanded = quote! {
        #[td_apiforge::apiserver_schema]
        #[derive(Debug, Default, Clone, td_type::TdType, derive_builder::Builder, getset::Getters, serde::Serialize, serde::Deserialize)]
        #[builder(try_setter, setter(into))]
        #[getset(get = "pub")]
        #input

        impl<#ty_generics> crate::types::DataTransferObject for #ident #ty_generics #where_clause {}
    };

    expanded.into()
}

pub fn url_param(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);

    if !input.generics.params.is_empty() {
        panic!("the struct must not have generics");
    }

    let expanded = quote! {
        #[td_apiforge::apiserver_schema]
        #[derive(Debug, Default, Clone, td_type::TdType, utoipa::IntoParams, derive_builder::Builder, getset::Getters, serde::Serialize, serde::Deserialize)]
        #input
    };

    expanded.into()
}

/// Derive type macro
#[derive(FromDeriveInput)]
#[darling(attributes(td_type))]
struct TdTypeArgs {
    #[darling(multiple)]
    builder: Vec<TdTypeArg>,
    #[darling(multiple)]
    updater: Vec<TdTypeArg>,
}

#[derive(FromMeta)]
struct TdTypeArg {
    try_from: Option<Ident>,
    #[darling(default)]
    skip_all: bool,
}

#[derive(FromField)]
#[darling(attributes(td_type))]
struct TdTypeFields {
    #[darling(multiple)]
    builder: Vec<TdTryFromField>,
    #[darling(multiple)]
    updater: Vec<TdTryFromField>,
    #[darling(default)]
    setter: bool,
    #[darling(default)]
    extractor: bool,
}

#[derive(FromMeta)]
struct TdTryFromField {
    try_from: Option<Ident>,
    #[darling(default)]
    skip: bool,
    #[darling(default)]
    include: bool,
    field: Option<String>,
}

pub fn td_type(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let args = TdTypeArgs::from_derive_input(&input).unwrap();
    let item = match input.data {
        syn::Data::Struct(data) => ItemStruct {
            attrs: input.attrs,
            vis: input.vis,
            struct_token: data.struct_token,
            ident: input.ident,
            generics: input.generics,
            fields: data.fields,
            semi_token: data.semi_token,
        },
        _ => panic!("TdType can only be derived for structs"),
    };

    let type_ = &item.ident;
    let generics = &item.generics;
    let fields = &item.fields;

    let builder_type = format_ident!("{}Builder", type_);
    let builder_error_type = format_ident!("{}BuilderError", type_);

    let field_names: Vec<_> = fields
        .iter()
        .map(|field| field.ident.as_ref().unwrap())
        .collect();

    let td_types_froms = gen_td_types_froms(&args, &item);
    let error_impl = gen_error(type_);

    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let expanded = quote! {
        impl #impl_generics #type_ #ty_generics #where_clause {

            pub fn builder() -> #builder_type #ty_generics {
                #builder_type::default()
            }

            pub fn to_builder(&self) -> #builder_type #ty_generics {
                let mut builder = #builder_type::default();
                builder #( .#field_names(self.#field_names.clone()) )*;
                builder
            }
        }

        impl TryFrom<&#builder_type #ty_generics> for #type_ #ty_generics {
            type Error = #builder_error_type;
            fn try_from(from: &#builder_type #ty_generics) -> Result<Self, Self::Error> {
                from.build()
            }
        }

        impl From<()> for #builder_type #ty_generics {
            fn from(from: ()) -> Self {
                #type_::builder()
            }
        }

        #td_types_froms
        #error_impl
    };

    expanded.into()
}

/// Very similar to td_error impl for generated Builder Error enum.
fn gen_error(type_: &Ident) -> proc_macro2::TokenStream {
    let builder_error_type = format_ident!("{}BuilderError", type_);

    quote! {
        impl #builder_error_type {
            fn variant_index(&self) -> u16 {
               5000
            }
        }

        impl td_error::TdDomainError for #builder_error_type {
            fn domain(&self) -> &'static str {
                stringify!(#builder_error_type)
            }

            fn code(&self) -> String {
                format!("{}::{:04}", self.domain(), self.variant_index())
            }

            fn api_error(&self) -> td_error::ApiError {
                td_error::ApiError::from(self.variant_index())
            }
        }

        impl From<#builder_error_type> for td_error::TdError {
            fn from(error: #builder_error_type) -> Self {
                Self::new(error)
            }
        }
    }
}

fn gen_td_types_froms(args: &TdTypeArgs, target: &ItemStruct) -> proc_macro2::TokenStream {
    let mut expanded = quote! {};
    for arg in &args.builder {
        if let Some(try_from) = &arg.try_from {
            expanded.extend(gen_try_from(try_from, target, arg.skip_all));
        }
    }
    for arg in &args.updater {
        if let Some(update_from) = &arg.try_from {
            expanded.extend(gen_updated_from(update_from, target, arg.skip_all));
        }
    }
    expanded.extend(gen_td_type_field_getset(target));
    expanded
}

fn gen_try_from(from: &Ident, target: &ItemStruct, skip_all: bool) -> proc_macro2::TokenStream {
    let to = &target.ident;
    let builder = format_ident!("{}Builder", to);
    let builder_error_type = format_ident!("{}BuilderError", to);

    let initializers = gen_from_fields_initializers(
        FieldsType::Builder,
        from,
        target,
        &builder_error_type,
        skip_all,
    );
    let (impl_generics, ty_generics, where_clause) = target.generics.split_for_impl();

    let expanded = quote! {
        impl #impl_generics TryFrom<& #from #ty_generics> for #builder #ty_generics #where_clause {
            type Error = #builder_error_type;
            fn try_from(from: & #from #ty_generics) -> Result<Self, Self::Error> {
                let mut builder = #builder::default();
                builder #(#initializers)*;
                Ok(builder)
            }
        }
    };

    expanded
}

fn gen_updated_from(from: &Ident, target: &ItemStruct, skip_all: bool) -> proc_macro2::TokenStream {
    let to = &target.ident;
    let builder = format_ident!("{}Builder", to);
    let builder_error_type = format_ident!("{}BuilderError", to);

    let initializers = gen_from_fields_initializers(
        FieldsType::Updater,
        from,
        target,
        &builder_error_type,
        skip_all,
    );
    let (_, ty_generics, where_clause) = target.generics.split_for_impl();

    let expanded = quote! {
        impl TryFrom<(& #from #ty_generics, #builder #ty_generics)> for #builder #ty_generics #where_clause {
            type Error = #builder_error_type;
            fn try_from(value: (& #from #ty_generics, #builder #ty_generics)) -> Result<Self, Self::Error> {
                let (from, mut this) = value;
                this #(#initializers)*;
                Ok(this)
            }
        }
    };

    expanded
}

fn gen_td_type_field_getset(target: &ItemStruct) -> proc_macro2::TokenStream {
    let to = &target.ident;
    let (impl_generics, ty_generics, where_clause) = target.generics.split_for_impl();

    let mut expanded = quote! {};

    for field in target.fields.iter() {
        let td_type_fields = TdTypeFields::from_field(field).unwrap();
        if td_type_fields.extractor {
            let field_name = field.ident.as_ref().unwrap();
            let field_type = &field.ty;
            expanded.extend(quote! {
                impl #impl_generics From<& #to #ty_generics> for #field_type #where_clause {
                    fn from(from: & #to #ty_generics) -> Self {
                        from.#field_name.clone()
                    }
                }
            });
        } else if td_type_fields.setter {
            let field_name = field.ident.as_ref().unwrap();
            let field_type = &field.ty;
            let builder_type = format_ident!("{}Builder", to);

            expanded.extend(quote! {
                impl From<(&#field_type, #builder_type #ty_generics)> for #builder_type #ty_generics #where_clause {
                    fn from(value: (&#field_type, #builder_type #ty_generics)) -> Self {
                        let (from, mut this) = value;
                        this.#field_name::<#field_type>(from.clone());
                        this
                    }
                }
            });
        }
    }

    expanded
}

enum FieldsType {
    Builder,
    Updater,
}

fn gen_from_fields_initializers(
    fields_type: FieldsType,
    from: &Ident,
    target: &ItemStruct,
    builder_error_type: &Ident,
    skip_all: bool,
) -> Vec<proc_macro2::TokenStream> {
    let fields = &target.fields;

    let mut initializers = vec![];
    for field in fields.iter() {
        let td_type_fields = TdTypeFields::from_field(field).unwrap();
        let from_fields = match fields_type {
            FieldsType::Builder => &td_type_fields.builder,
            FieldsType::Updater => &td_type_fields.updater,
        };
        match should_include_from_field(from_fields, from, skip_all) {
            IncludeField::Include => {
                let field_type = field.ty.clone();
                let field_name = field.ident.as_ref().unwrap();
                let initializer = quote! {
                    .#field_name::<#field_type>(
                        from
                        .#field_name()
                        .clone()
                        .try_into()
                        .map_err(|e| #builder_error_type::ValidationError(format!("{}", e)))?,
                    )
                };
                initializers.push(initializer);
            }
            IncludeField::Skip => {}
            IncludeField::Rename(name) => {
                let field_type = field.ty.clone();
                let field_name = field.ident.as_ref().unwrap();
                let renamed_field = format_ident!("{}", name);
                let initializer = quote! {
                    .#field_name::<#field_type>(
                        from
                        .#renamed_field()
                        .clone()
                        .try_into()
                        .map_err(|e| #builder_error_type::ValidationError(format!("{}", e)))?,
                    )
                };
                initializers.push(initializer);
            }
        }
    }

    initializers
}

enum IncludeField {
    Include,
    Skip,
    Rename(String),
}

fn should_include_from_field(
    from_args: &[TdTryFromField],
    from: &Ident,
    skip_all: bool,
) -> IncludeField {
    let find_field = |pred: fn(&TdTryFromField) -> bool| {
        from_args
            .iter()
            .find(|f| pred(f) && f.try_from.as_ref().is_none_or(|a| a == from))
    };

    if skip_all {
        if let Some(f) = find_field(|f| f.include) {
            return f
                .field
                .clone()
                .map_or(IncludeField::Include, IncludeField::Rename);
        }
        if let Some(f) = find_field(|f| f.field.is_some()) {
            return IncludeField::Rename(f.field.clone().unwrap());
        }
        IncludeField::Skip
    } else if from_args.is_empty() {
        IncludeField::Include
    } else {
        if find_field(|f| f.skip).is_some() {
            return IncludeField::Skip;
        }
        if let Some(f) = find_field(|f| f.field.is_some()) {
            return IncludeField::Rename(f.field.clone().unwrap());
        }
        IncludeField::Include
    }
}

fn gen_fields_as_list(fields: &Fields) -> Vec<&Ident> {
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
