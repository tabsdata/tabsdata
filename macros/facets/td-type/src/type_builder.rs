//
// Copyright 2025 Tabs Data Inc.
//

use darling::{FromDeriveInput, FromField, FromMeta};
use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::{format_ident, quote, ToTokens};
use syn::{parse_macro_input, DeriveInput, Fields, ItemStruct};

/// Proc macros to generate default derives for the dao/dlo/dto types.
pub fn dlo(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);

    let expanded = quote! {
        #[derive(Debug, Default, Clone, td_type::TdType, derive_builder::Builder, getset::Getters, serde::Serialize, serde::Deserialize, sqlx::FromRow)]
        #[builder(setter(into))]
        #[getset(get = "pub")]
        #input
    };

    expanded.into()
}

pub fn dao(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);

    let expanded = quote! {
        #[derive(Debug, Default, Clone, td_type::TdType, derive_builder::Builder, getset::Getters, serde::Serialize, serde::Deserialize, sqlx::FromRow)]
        #[builder(setter(into))]
        #[getset(get = "pub")]
        #input
    };

    expanded.into()
}

pub fn dto(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);

    let expanded = quote! {
        #[td_apiforge::api_server_schema]
        #[derive(Debug, Default, Clone, td_type::TdType, derive_builder::Builder, getset::Getters, serde::Serialize, serde::Deserialize)]
        #[builder(setter(into))]
        #[getset(get = "pub")]
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
    builder: Vec<TdTypeField>,
}

#[derive(FromMeta)]
struct TdTypeField {
    try_from: Option<Ident>,
    #[darling(default)]
    skip: bool,
    #[darling(default)]
    include: bool,
    field: Option<String>,
    #[darling(default)]
    default: bool,
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

    let mut setters = vec![];
    for field in fields.iter() {
        let field_name = field.ident.as_ref().unwrap();
        let setting = quote! {
            .#field_name(self.#field_name.clone())
        };
        setters.push(setting);
    }

    let froms = gen_froms(&args, &item);
    let fields_list = gen_fields_as_list(fields);
    let error_impl = gen_error(type_);

    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let expanded = quote! {
        impl #impl_generics #type_ #ty_generics #where_clause {

            pub fn builder() -> #builder_type #ty_generics {
                #builder_type::default()
            }

            pub fn to_builder(&self) -> #builder_type #ty_generics {
                let mut builder = #builder_type::default();
                builder #(#setters)*;
                builder
            }

            pub fn fields() -> &'static [&'static str] {
                #fields_list
            }
        }

        #froms
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

        impl td_common::error::TdDomainError for #builder_error_type {
            fn domain(&self) -> &'static str {
                stringify!(#builder_error_type)
            }

            fn code(&self) -> String {
                format!("{}::{:04}", self.domain(), self.variant_index())
            }

            fn api_error(&self) -> td_common::error::ApiError {
                td_common::error::ApiError::from(self.variant_index())
            }
        }

        impl From<#builder_error_type> for td_common::error::TdError {
            fn from(error: #builder_error_type) -> Self {
                Self::new(error)
            }
        }
    }
}

fn gen_froms(args: &TdTypeArgs, target: &ItemStruct) -> proc_macro2::TokenStream {
    let mut expanded = quote! {};
    for arg in &args.builder {
        if let Some(from) = &arg.try_from {
            expanded.extend(gen_from(from, target, arg.skip_all));
        }
    }
    expanded
}

fn gen_from(from: &Ident, target: &ItemStruct, skip_all: bool) -> proc_macro2::TokenStream {
    let to = &target.ident;
    let fields = &target.fields;

    let builder_error_type = format_ident!("{}BuilderError", to);

    let mut initializers = vec![];
    for field in fields.iter() {
        match should_include_field(field, from, skip_all) {
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
            IncludeField::Default => {
                let field_name = field.ident.as_ref().unwrap();
                let field_type = &field.ty;
                let field_type = quote! { #field_type }.to_string();
                let field_type_without_generics = field_type.split('<').next().unwrap().trim();
                let field_type_without_generics = format_ident!("{}", field_type_without_generics);
                let initializer = quote! {
                    .#field_name(#field_type_without_generics::default())
                };
                initializers.push(initializer);
            }
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

    let builder = format_ident!("{}Builder", to);

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

enum IncludeField {
    Include,
    Skip,
    Default,
    Rename(String),
}

fn should_include_field(field: &syn::Field, from: &Ident, skip_all: bool) -> IncludeField {
    let td_type_fields = TdTypeFields::from_field(field).unwrap();

    fn none_or_eq<T: PartialEq>(a: &Option<T>, b: &T) -> bool {
        // If Ident is not present or equal to the from field.
        a.as_ref().is_none_or(|a| a == b)
    }

    if skip_all {
        // If skipping all, we only include the fields explicitly marked with `include`
        if let Some(f) = td_type_fields
            .builder
            .iter()
            .find(|f| f.include && none_or_eq(&f.try_from, from))
        {
            if let Some(rename) = &f.field {
                IncludeField::Rename(rename.clone())
            } else {
                IncludeField::Include
            }
        } else {
            IncludeField::Skip
        }
    } else if td_type_fields.builder.is_empty() {
        // If no specific builder fields are defined, include the field
        IncludeField::Include
    } else {
        // Otherwise, include the field unless it is explicitly marked with `skip`
        if let Some(_f) = td_type_fields
            .builder
            .iter()
            .find(|f| f.skip && none_or_eq(&f.try_from, from))
        {
            IncludeField::Skip
        } else if let Some(_f) = td_type_fields
            .builder
            .iter()
            .find(|f| f.default && none_or_eq(&f.try_from, from))
        {
            IncludeField::Default
        } else if let Some(f) = td_type_fields
            .builder
            .iter()
            .find(|f| f.field.is_some() && none_or_eq(&f.try_from, from))
        {
            IncludeField::Rename(f.field.clone().unwrap())
        } else {
            IncludeField::Include
        }
    }
}

fn gen_fields_as_list(fields: &Fields) -> proc_macro2::TokenStream {
    let field_names = fields
        .iter()
        .filter(|f| {
            // Check if the field does NOT have the `#[sqlx(skip)]` attribute
            !f.attrs.iter().any(|attr| {
                attr.path().is_ident("sqlx") && attr.to_token_stream().to_string().contains("skip")
            })
        })
        .filter_map(|f| f.ident.as_ref())
        .map(|ident| ident.to_string())
        .collect::<Vec<_>>();

    quote! {
        &[#(#field_names),*]
    }
}
