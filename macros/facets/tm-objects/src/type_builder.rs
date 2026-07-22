//
// Copyright 2025 Tabsdata Inc.
//

//! Proc macros to generate default derives for the dao/dlo/dto types.

use darling::{FromDeriveInput, FromField, FromMeta};
use proc_macro2::Ident;
use quote::{format_ident, quote};
use syn::{DeriveInput, ItemStruct};

/// Extract the [`ItemStruct`] from a [`DeriveInput`], returning a compile error
/// if the input is not a struct.
pub fn parse_input_item_struct(input: &DeriveInput) -> Result<ItemStruct, syn::Error> {
    let input = input.clone();
    let ident_span = input.ident.clone();
    match input.data {
        syn::Data::Struct(data) => Ok(ItemStruct {
            attrs: input.attrs,
            vis: input.vis,
            struct_token: data.struct_token,
            ident: input.ident,
            generics: input.generics,
            fields: data.fields,
            semi_token: data.semi_token,
        }),
        _ => Err(syn::Error::new_spanned(
            ident_span,
            "Dxo can only be derived for structs",
        )),
    }
}

/// Parsed `#[dxo(...)]` struct-level attributes (builder/updater conversions).
#[derive(FromDeriveInput)]
#[darling(attributes(dxo))]
struct DxoArguments {
    #[darling(multiple)]
    builder: Vec<DxoArgument>,
    #[darling(multiple)]
    updater: Vec<DxoArgument>,
}

#[derive(FromMeta)]
struct DxoArgument {
    try_from: Option<Ident>,
    #[darling(default)]
    skip_all: bool,
}

#[allow(dead_code)]
#[derive(FromField)]
#[darling(attributes(dxo))]
pub struct DxoFields {
    #[darling(multiple)]
    pub builder: Vec<TryFromField>,
    #[darling(multiple)]
    updater: Vec<TryFromField>,
    #[darling(default)]
    setter: bool,
    #[darling(default)]
    extractor: bool,
    #[darling(default)]
    immutable: bool,
}

#[derive(FromMeta)]
pub struct TryFromField {
    pub try_from: Option<Ident>,
    #[darling(default)]
    skip: bool,
    #[darling(default)]
    include: bool,
    pub field: Option<String>,
}

/// Generate `Buildable` impl and `TryFrom`/`TryUpdater` conversions for a struct.
pub fn td_type(input: &DeriveInput, item: &ItemStruct) -> proc_macro2::TokenStream {
    let args = match DxoArguments::from_derive_input(input) {
        Ok(a) => a,
        Err(e) => return e.write_errors(),
    };

    let ident = &item.ident;
    let generics = &item.generics;
    let fields = &item.fields;

    let builder_type = format_ident!("{}Builder", ident);

    let field_names: Vec<_> = fields
        .iter()
        .map(|field| field.ident.as_ref().unwrap())
        .collect();

    let conversions = gen_conversions(&args, item);
    let error_impl = gen_builder_error_impl(ident);

    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let expanded = quote! {
        impl #impl_generics ta_objects::Buildable for #ident #ty_generics #where_clause {
            type Builder = #builder_type #ty_generics;

            fn builder() -> Self::Builder {
                #builder_type::default()
            }

            fn into_builder(self) -> Self::Builder {
                let mut builder = #builder_type::default();
                builder #( .#field_names(self.#field_names) )*;
                builder
            }

            fn build(builder: &Self::Builder) -> Result<Self, td_error::TdError>
            where
                Self: Sized,
            {
                builder.build().map_err(Into::into)
            }
        }

        #conversions
        #error_impl
    };

    expanded
}

/// Generate `TdDomainError` and `From<…Error> for TdError` impls for the builder error type.
fn gen_builder_error_impl(ident: &Ident) -> proc_macro2::TokenStream {
    let builder_error_type = format_ident!("{}BuilderError", ident);

    quote! {
        impl #builder_error_type {
            fn variant_index(&self) -> u16 {
               5000
            }
        }

        impl td_error::TdDomainError for #builder_error_type {
            fn domain(&self) -> &str {
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

fn gen_conversions(args: &DxoArguments, target: &ItemStruct) -> proc_macro2::TokenStream {
    let mut expanded = quote! {};
    for arg in &args.builder {
        if let Some(try_from) = &arg.try_from {
            expanded.extend(gen_conversion(
                FieldsType::Builder,
                try_from,
                target,
                arg.skip_all,
            ));
        }
    }
    for arg in &args.updater {
        if let Some(update_from) = &arg.try_from {
            expanded.extend(gen_conversion(
                FieldsType::Updater,
                update_from,
                target,
                arg.skip_all,
            ));
        }
    }
    expanded.extend(gen_field_accessors(target));
    expanded
}

/// Generate either a `TryFrom` (builder) or `TryUpdater` (updater) impl for
/// converting `from` into `target`'s builder.
fn gen_conversion(
    fields_type: FieldsType,
    from: &Ident,
    target: &ItemStruct,
    skip_all: bool,
) -> proc_macro2::TokenStream {
    let to = &target.ident;
    let builder = format_ident!("{}Builder", to);
    let builder_error_type = format_ident!("{}BuilderError", to);

    let initializers =
        match gen_field_initializers(fields_type, from, target, &builder_error_type, skip_all) {
            Ok(i) => i,
            Err(e) => return e.to_compile_error(),
        };
    let (impl_generics, ty_generics, where_clause) = target.generics.split_for_impl();

    match fields_type {
        FieldsType::Builder => quote! {
            impl #impl_generics TryFrom<& #from #ty_generics> for #builder #ty_generics #where_clause {
                type Error = #builder_error_type;
                fn try_from(from: & #from #ty_generics) -> Result<Self, Self::Error> {
                    let mut builder = #builder::default();
                    builder #(#initializers)*;
                    Ok(builder)
                }
            }
        },
        FieldsType::Updater => quote! {
            impl ta_objects::TryUpdater<#from #ty_generics> for #builder #ty_generics #where_clause {
                type Error = #builder_error_type;
                fn try_update_from(&mut self, from: & #from #ty_generics) -> Result<&mut Self, Self::Error> {
                    self #(#initializers)*;
                    Ok(self)
                }
            }
        },
    }
}

fn gen_field_accessors(target: &ItemStruct) -> proc_macro2::TokenStream {
    let to = &target.ident;
    let (impl_generics, ty_generics, where_clause) = target.generics.split_for_impl();

    let mut expanded = quote! {};

    for field in target.fields.iter() {
        let field_attrs = match DxoFields::from_field(field) {
            Ok(f) => f,
            Err(e) => {
                expanded.extend(syn::Error::new_spanned(field, e.to_string()).to_compile_error());
                continue;
            }
        };
        if field_attrs.extractor {
            let field_name = field.ident.as_ref().unwrap();
            let field_type = &field.ty;
            expanded.extend(quote! {
                impl #impl_generics ta_objects::Extractor<#field_type> for #to #ty_generics #where_clause {
                    fn extract(&self) -> #field_type {
                        self.#field_name.clone()
                    }
                }
            });
        };
        if field_attrs.setter {
            let field_name = field.ident.as_ref().unwrap();
            let field_type = &field.ty;
            let builder_type = format_ident!("{}Builder", to);

            expanded.extend(quote! {
                impl ta_objects::Setter<#field_type> for #builder_type #ty_generics #where_clause {
                    fn set(&mut self, value: &#field_type) -> &mut Self {
                        self.#field_name::<#field_type>(value.clone());
                        self
                    }
                }
            });
        }
    }

    expanded
}

#[derive(Clone, Copy)]
enum FieldsType {
    Builder,
    Updater,
}

fn gen_field_initializers(
    fields_type: FieldsType,
    from: &Ident,
    target: &ItemStruct,
    builder_error_type: &Ident,
    skip_all: bool,
) -> Result<Vec<proc_macro2::TokenStream>, syn::Error> {
    let fields = &target.fields;

    let mut initializers = vec![];
    for field in fields.iter() {
        let field_attrs = DxoFields::from_field(field)
            .map_err(|e| syn::Error::new_spanned(field, e.to_string()))?;
        let conversion_attrs = match fields_type {
            FieldsType::Builder => &field_attrs.builder,
            FieldsType::Updater => &field_attrs.updater,
        };
        let include = should_include_from_field(conversion_attrs, from, skip_all);
        if let IncludeField::Skip = include {
            continue;
        }
        let field_type = field.ty.clone();
        let field_name = field.ident.as_ref().unwrap();
        let source_field = match include {
            IncludeField::Rename(ref name) => format_ident!("{}", name),
            _ => field_name.clone(),
        };
        // Name the source and target of the failing hop so multi-hop DXO
        // conversions point at the exact field that failed, not just the
        // inner validation message.
        let context = format!(
            "{}.{} -> {}.{}",
            from, source_field, target.ident, field_name
        );
        let initializer = quote! {
            .#field_name::<#field_type>(
                from
                .#source_field
                .clone()
                .try_into()
                .map_err(|e| #builder_error_type::ValidationError(format!("{}: {}", #context, e)))?,
            )
        };
        initializers.push(initializer);
    }

    Ok(initializers)
}

enum IncludeField {
    Include,
    Skip,
    Rename(String),
}

fn should_include_from_field(
    from_args: &[TryFromField],
    from: &Ident,
    skip_all: bool,
) -> IncludeField {
    let find_field = |pred: fn(&TryFromField) -> bool| {
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
