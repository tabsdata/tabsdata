//
// Copyright 2024 Tabs Data Inc.
//

extern crate proc_macro;

use proc_macro::TokenStream;
use std::ops::Deref;

use darling::FromMeta;
use quote::quote;
use syn::{
    parse_macro_input, visit::Visit, Attribute, Fields, File, GenericArgument, GenericParam,
    Generics, Ident, ItemEnum, ItemStruct, ItemType, Path, PathArguments, Type,
};
use walkdir::WalkDir;

use td_shared::parse_meta;
use td_shared::project::get_project_root;

#[derive(FromMeta)]
struct ConcreteArguments {
    #[darling(default, multiple, rename = "into")]
    additional_into: Vec<Option<Path>>,
    root_dir: Option<String>,
}

/// The main procedural macro that generates concrete types from type aliases.
/// The type in which the attribute works does not work as an alias anymore, but as a concrete type.
pub fn generic_remover(args: TokenStream, item: TokenStream) -> TokenStream {
    let default_root_dir: String = get_project_root();
    let default_root_dir = default_root_dir.as_str();

    // Parse the input as a type alias
    let input = parse_macro_input!(item as ItemType);
    let parsed_args = parse_meta!(ConcreteArguments, args).unwrap();

    let root_dir = parsed_args
        .root_dir
        .unwrap_or_else(|| default_root_dir.to_string());

    // attrs
    // type alias_name = ty
    let alias_name = &input.ident;
    let type_alias = &input.ty;
    let alias_attributes = &input.attrs;

    // Extract the type path from the alias
    let type_path = match &type_alias.deref() {
        Type::Path(type_path) => &type_path.path,
        _ => panic!("concrete can only be used with type aliases"),
    };

    // Look for the type we want to copy (this is, the type we want to concrete).
    // Note that we assume there is only one type with the given name.
    let type_alias_name = type_path.segments.last().unwrap().ident.clone();
    let mut type_finder = TypeFinder::new(&type_alias_name);
    for entry in WalkDir::new(&root_dir)
        .follow_links(true)
        .follow_root_links(true)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if entry.path().extension().is_some_and(|ext| ext == "rs") {
            let file_content = std::fs::read_to_string(entry.path()).expect("Failed to read file");
            let parsed_file: File = syn::parse_file(&file_content).expect("Failed to parse file");
            type_finder.visit_file(&parsed_file);
            if type_finder.found_type.is_some() {
                break;
            }
        }
    }

    let found_type = match type_finder.found_type {
        Some(t) => t,
        None => panic!("Type {} not found", &type_alias_name),
    };

    // Get the generics from the original types
    let generics = match &found_type {
        TypeItem::Enum(_, generics) | TypeItem::Struct(_, generics) => generics
            .params
            .iter()
            .map(|param| match param {
                GenericParam::Type(ty) => Some(ty.ident.clone()),
                _ => None,
            })
            .filter_map(|ident| ident.is_some().then(|| ident.unwrap()))
            .collect::<Vec<_>>(),
    };

    // Extract the concretes types to use
    let concrete_types = extract_generics(type_path);

    // Else, we have a problem
    assert_eq!(generics.len(), concrete_types.len());

    match found_type {
        TypeItem::Enum(node, _) => concrete_enum(
            alias_name,
            alias_attributes,
            concrete_types,
            generics,
            node,
            parsed_args.additional_into,
        ),
        TypeItem::Struct(node, _) => concrete_struct(
            alias_name,
            alias_attributes,
            concrete_types,
            generics,
            node,
            parsed_args.additional_into,
        ),
    }
}

/// Generates a concrete enum from a given type alias.
fn concrete_enum(
    alias_name: &Ident,
    alias_attributes: &Vec<Attribute>,
    concrete_types: Vec<Type>,
    generics: Vec<Ident>,
    type_input: ItemEnum,
    additional_intos: Vec<Option<Path>>,
) -> TokenStream {
    // name and fields of the existing struct
    let name = &type_input.ident;
    let variants = &type_input.variants;

    let (variants, enum_values, into_enum_values): (Vec<_>, Vec<_>, Vec<_>) = variants
        .iter()
        .map(|variant| {
            let variant_name = &variant.ident;
            let fields = &variant.fields;
            let new_fields = replace_all_generics(fields, &generics, &concrete_types);

            let new_field_tokens = if new_fields.is_empty() {
                quote! { #variant_name }
            } else {
                quote! { #variant_name(#(#new_fields),*) }
            };

            let variables = fields
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    let var_name = Ident::new(&format!("x{}", i), variant_name.span());
                    quote! { #var_name }
                })
                .collect::<Vec<_>>();

            let (enum_values, into_enum_values) = if variables.is_empty() {
                let empty_value = quote! { #variant_name };
                (empty_value.clone(), empty_value)
            } else {
                let enum_value = quote! { #variant_name(#(#variables),*) };
                let into_enum_value = quote! { #variant_name(#(#variables.into()),*) };
                (enum_value, into_enum_value)
            };

            (new_field_tokens, enum_values, into_enum_values)
        })
        .fold(
            (Vec::new(), Vec::new(), Vec::new()),
            |(mut variants, mut enum_values, mut into_enum_values),
             (new_field_tokens, enum_value, into_enum_value)| {
                variants.push(new_field_tokens);
                enum_values.push(enum_value);
                into_enum_values.push(into_enum_value);
                (variants, enum_values, into_enum_values)
            },
        );

    let attributes: Vec<_> = type_input.attrs.iter().map(|attr| quote!(#attr)).collect();
    let enum_definition = quote! {
        #(#alias_attributes)*
        #(#attributes)*
        pub enum #alias_name {
            #(#variants),*
        }
    };

    let to_impl_definition = quote! {
        impl From<#alias_name> for #name<#(#concrete_types,)*> {
            fn from(value: #alias_name) -> #name<#(#concrete_types,)*> {
                match value {
                    #(
                        #alias_name::#enum_values => #name::#into_enum_values,
                    )*
                }
            }
        }
    };

    let mut from_impls = TokenStream::new();
    if !additional_intos.is_empty() {
        for extra_impl in additional_intos {
            let extra_impl = extra_impl.unwrap();
            let extra_impl_definition = quote! {
                impl From<#alias_name> for #extra_impl {
                    fn from(value: #alias_name) -> #extra_impl {
                        match value {
                            #(
                                #alias_name::#enum_values => #extra_impl::#into_enum_values,
                            )*
                        }
                    }
                }
            };
            from_impls.extend(TokenStream::from(extra_impl_definition));
        }
    }

    let from_impl_definition = quote! {
        impl From<#name<#(#concrete_types,)*>> for #alias_name {
            fn from(value: #name<#(#concrete_types,)*>) -> #alias_name {
                match value {
                    #(
                        #name::#enum_values => #alias_name::#into_enum_values,
                    )*
                }
            }
        }
    };

    let mut output = TokenStream::new();
    output.extend(TokenStream::from(enum_definition));
    output.extend(TokenStream::from(to_impl_definition));
    output.extend(TokenStream::from(from_impl_definition));
    output.extend(from_impls);
    output
}

/// Generate the concrete struct from the given type alias.
/// Builder and getters derive is needed in the original struct (and therefore the new one).
fn concrete_struct(
    alias_name: &Ident,
    alias_attributes: &Vec<Attribute>,
    concrete_types: Vec<Type>,
    generics: Vec<Ident>,
    type_input: ItemStruct,
    additional_intos: Vec<Option<Path>>,
) -> TokenStream {
    // name and fields of the existing struct
    let name = &type_input.ident;
    let fields = &type_input.fields;
    let attributes = type_input.attrs;
    let new_fields = replace_all_generics(fields, &generics, &concrete_types);

    let struct_definition = quote! {
        #(#alias_attributes)*
        #(#attributes)*
        pub struct #alias_name {
            #(#new_fields),*
        }
    };

    // Generate field assignments to convert from and into
    let mut field_assignments = Vec::new();
    for field in fields {
        let field_name = &field.ident;

        // Option is a special type, we can handle it on its own
        if is_option_type(&field.ty) {
            field_assignments.push(quote! {
                .#field_name(value.#field_name().clone().map(|x| x.into()))
            });
        } else {
            field_assignments.push(quote! {
                .#field_name(value.#field_name().clone().into())
            });
        }
    }

    let builder_name = Ident::new(&format!("{}Builder", name), name.span());
    let to_impl_definition = {
        quote! {
            impl From<#alias_name> for #name<#(#concrete_types,)*> {
                fn from(value: #alias_name) -> #name<#(#concrete_types,)*> {
                    #builder_name::default()
                        #(#field_assignments)*
                        .build()
                        .unwrap()
                }
            }
        }
    };

    let mut from_impls = TokenStream::new();
    if !additional_intos.is_empty() {
        for extra_impl in additional_intos {
            let extra_impl_definition = quote! {
                    impl From<#alias_name> for #extra_impl {
                    fn from(value: #alias_name) -> Self {
                        #builder_name::default()
                            #(#field_assignments)*
                            .build()
                            .unwrap()
                    }
                }
            };
            from_impls.extend(TokenStream::from(extra_impl_definition));
        }
    }

    let builder_name = Ident::new(&format!("{}Builder", alias_name), alias_name.span());
    let from_impl_definition = {
        let mut field_assignments = Vec::new();
        for field in fields {
            let field_name = &field.ident;
            field_assignments.push(quote! {
                .#field_name(value.#field_name().clone().into())
            });
        }
        quote! {
            impl From<#name<#(#concrete_types,)*>> for #alias_name {
                fn from(value: #name<#(#concrete_types,)*>) -> #alias_name {
                    #builder_name::default()
                        #(#field_assignments)*
                        .build()
                        .unwrap()
                }
            }
        }
    };

    let mut output = TokenStream::new();
    output.extend(TokenStream::from(struct_definition));
    output.extend(TokenStream::from(to_impl_definition));
    output.extend(TokenStream::from(from_impl_definition));
    output.extend(from_impls);
    output
}

/// Extracts first level depth generics types from a given path.
fn extract_generics(path: &Path) -> Vec<Type> {
    let mut generics = Vec::new();

    for segment in &path.segments {
        if let PathArguments::AngleBracketed(angle_bracketed) = &segment.arguments {
            for arg in &angle_bracketed.args {
                if let GenericArgument::Type(ty) = arg {
                    generics.push(ty.clone());
                }
            }
        }
    }
    generics
}

/// Replace all generics types in the given field. Generics and concretes types must have the same
/// amount of elements.
fn replace_all_generics(
    fields: &Fields,
    generics: &Vec<Ident>,
    concrete_types: &Vec<Type>,
) -> Vec<proc_macro2::TokenStream> {
    fields
        .iter()
        .map(|field| {
            let field_ty = &field.ty;
            let field_type = if let Type::Path(mut field_type) = field_ty.clone() {
                let path = &mut field_type.path;
                replace_generics(path, generics, concrete_types)
            } else {
                panic!("Only path types are supported");
            };
            let new_field_type = quote! {
                #field_type
            };
            let field_name = &field.ident;
            if field_name.is_some() {
                quote! { #field_name: #new_field_type }
            } else {
                quote! { #new_field_type }
            }
        })
        .collect::<Vec<_>>()
}

/// Replace all generics types in a given path. Generics and concretes types must have the same
/// amount of elements.
fn replace_generics(path: &mut Path, generics: &Vec<Ident>, concretes: &Vec<Type>) -> Path {
    assert_eq!(generics.len(), concretes.len());

    // First pass: handle nested generics
    for segment in &mut path.segments {
        if let PathArguments::AngleBracketed(angle_bracketed) = &mut segment.arguments {
            for arg in angle_bracketed.args.iter_mut() {
                if let GenericArgument::Type(Type::Path(type_path)) = arg {
                    replace_generics(&mut type_path.path, generics, concretes);
                }
            }
        }
    }

    // Second pass: replace the generics with concrete types
    for segment in &mut path.segments {
        if let Some(index) = generics.iter().position(|g| g == &segment.ident) {
            let concrete = &concretes[index];
            (segment.ident, segment.arguments) = match concrete {
                Type::Path(type_path) => (
                    type_path.path.segments.last().unwrap().ident.clone(),
                    type_path.path.segments.last().unwrap().arguments.clone(),
                ),
                _ => panic!("Expected a Type::Path"),
            };
        }
    }
    path.clone()
}

/// Check if the given type is an Option type. Useful to use in From traits that have this type.
fn is_option_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            if segment.ident == "Option" {
                return true;
            }
        }
    }
    false
}

/// Type finder visitor which will add the found type to the `found_type` node if the ident matches.
/// It only looks for structs and enums.
struct TypeFinder {
    target_type: Ident,
    found_type: Option<TypeItem>,
}

enum TypeItem {
    Enum(ItemEnum, Generics),
    Struct(ItemStruct, Generics),
}

impl TypeFinder {
    fn new(target_type: &Ident) -> Self {
        Self {
            target_type: target_type.clone(),
            found_type: None,
        }
    }
}

impl<'ast> Visit<'ast> for TypeFinder {
    fn visit_item_enum(&mut self, node: &'ast ItemEnum) {
        if node.ident == self.target_type {
            self.found_type = Some(TypeItem::Enum(node.clone(), node.generics.clone()));
        }
        syn::visit::visit_item_enum(self, node);
    }

    fn visit_item_struct(&mut self, node: &'ast ItemStruct) {
        if node.ident == self.target_type {
            self.found_type = Some(TypeItem::Struct(node.clone(), node.generics.clone()));
        }
        syn::visit::visit_item_struct(self, node);
    }
}
