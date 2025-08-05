//
//  Copyright 2024 Tabs Data Inc.
//

use proc_macro::{Span, TokenStream};

use darling::FromMeta;
use getset::Getters;
use quote::quote;
use syn::__private::TokenStream2;
use syn::{
    parse_macro_input, FnArg, GenericArgument, Ident, Item, ItemFn, LitStr, PathArguments,
    ReturnType, Type,
};

use td_shared::parse_meta;

#[derive(FromMeta)]
struct UtoipaPathArguments {
    #[darling(default)]
    method: Option<Ident>,
    #[darling(default)]
    path: Option<Ident>,
    #[darling(default)]
    tag: Option<Ident>,
    #[darling(default)]
    // In general impl of Responses, we can define which is the type we are documenting.
    override_response: Option<Ident>,
}

/// Utoipa path attribute macro generator. It takes the Axum fn as an argument, as well as
/// the method, path, and tag attributes. It generates the appropriate utoipa::path attribute.
pub fn utoipa_path(args: TokenStream, item: TokenStream) -> TokenStream {
    let parsed_args = parse_meta!(UtoipaPathArguments, args).unwrap();

    let input = parse_macro_input!(item as ItemFn);

    // Extract method, path, and tag from the attribute arguments
    let method = parsed_args.method;
    let path = parsed_args.path;
    let tag = parsed_args.tag;

    // Extract the type of the `Query` parameter from the function signature
    let query_params = extract_type_in_generic_argument(&input, "Query");

    // Extract the type of the `Path` parameter from the function signature
    let path_params = extract_type_in_generic_argument(&input, "Path");

    // Extract the type of the `Json` request body from the function signature
    let request_body = extract_type_in_generic_argument(&input, "Json");
    let request_body = request_body.first();

    // Extract the Multipart type from the function signature
    let has_multipart = has_type(&input, "Multipart");

    // Extract the Stream Request type from the function signature
    let has_request = has_type(&input, "Request");

    if more_than_one(request_body.is_some(), has_multipart, has_request) {
        panic!(
            "Cannot have more than one of: [Json - Multipart - Request] extractor at the same time"
        );
    }

    // Extract the response types from the Result type in the function signature
    let (ok_response, err_response) = extract_result_types(&input, &parsed_args.override_response);

    // Generate concrete response struct for the `Ok` response.
    // This type is never used directly, but it is used to generate the schema for the response.
    let ok_response_struct = if let Type::ImplTrait(_) = ok_response {
        quote! {}
    } else {
        // Note that we enforce handlers names to be unique, so these structs will also be unique.
        let fn_name = &input.sig.ident;
        let struct_name = response_alias_struct_name(fn_name);
        quote! {
            #[allow(non_camel_case_types, unused)]
            #[td_apiforge::apiserver_schema]
            pub struct #struct_name(#ok_response);
        }
    };

    // Generate the `#[utoipa::path(...)]` attribute
    let mut utoipa_attr = quote! {
        #method,
        path = #path,
        tag = #tag,
    };

    let mut params = quote! {};

    for path_param in path_params.iter() {
        params = quote! {
            #params
            #path_param,
        };
    }

    for query_param in query_params.iter() {
        params = quote! {
            #params
            #query_param,
        };
    }

    if !query_params.is_empty() || !path_params.is_empty() {
        utoipa_attr = quote! {
            #utoipa_attr
            params(#params),
        };
    }

    if let Some(request_body) = request_body {
        utoipa_attr = quote! {
            #utoipa_attr
            request_body = #request_body,
        };
    }

    if has_multipart {
        utoipa_attr = quote! {
            #utoipa_attr
            request_body(content_type = "multipart/form-data", content = FileUpload),
        };
    }

    if has_request {
        utoipa_attr = quote! {
            #utoipa_attr
            request_body(content_type = "application/octet-stream", content = FileUpload),
        };
    }

    utoipa_attr = quote! {
        #utoipa_attr
        responses(#ok_response, #err_response),
    };

    utoipa_attr = quote! {
        #utoipa_attr
        security(
            ("Token" = [])
        ),
    };

    // Combine the generated attribute, type alias, and macro invocation with the original function
    let output = quote! {
        #ok_response_struct

        #[utoipa::path(
            #utoipa_attr
        )]
        #input
    };

    output.into()
}

pub fn response_alias_struct_name(fn_name: &Ident) -> Ident {
    Ident::new(
        &format!(
            "__response_{fn_name\
    }"
        ),
        fn_name.span(),
    )
}

fn more_than_one(a: bool, b: bool, c: bool) -> bool {
    let count = a as u8 + b as u8 + c as u8;
    count > 1
}

fn has_type(input: &ItemFn, method_type: &str) -> bool {
    input.sig.inputs.iter().any(|arg| {
        if let FnArg::Typed(pat_type) = arg {
            if let Type::Path(type_path) = &*pat_type.ty {
                return type_path
                    .path
                    .segments
                    .iter()
                    .any(|seg| seg.ident == method_type);
            }
        }
        false
    })
}
/// Extracts the type of the generic argument in the arguments of a given type signature.
fn extract_type_in_generic_argument(input: &ItemFn, generic_attribute: &str) -> Vec<Ident> {
    let mut vec = Vec::new();
    input.sig.inputs.iter().for_each(|arg| {
        if let FnArg::Typed(pat_type) = arg {
            if let Type::Path(type_path) = &*pat_type.ty {
                if has_generic_attribute(&type_path.path, generic_attribute) {
                    let type_ = extract_first_generic_argument(&type_path.path);
                    if let Some(type_) = type_ {
                        vec.push(type_);
                    }
                }
            }
        }
    });
    vec
}

/// Checks if the path contains the specified generic attribute.
fn has_generic_attribute(path: &syn::Path, generic_attribute: &str) -> bool {
    path.segments
        .iter()
        .any(|seg| seg.ident == generic_attribute)
}

/// Extracts the identifier of the first generic argument if it is a path type. We assume that the
/// only have one generic argument, because in API calls we will have everything in a single
/// final struct.
fn extract_first_generic_argument(path: &syn::Path) -> Option<Ident> {
    if let PathArguments::AngleBracketed(args) = &path.segments.last().unwrap().arguments {
        if let Some(GenericArgument::Type(Type::Path(type_path))) = args.args.first() {
            return Some(type_path.path.segments.last().unwrap().ident.clone());
        }
    }
    None
}

/// Extracts the Ok and Err response types from the Result type in the function signature.
fn extract_result_types(input: &ItemFn, override_response: &Option<Ident>) -> (Type, Type) {
    let return_type = match &input.sig.output {
        ReturnType::Type(_, ty) => ty,
        _ => panic!("Expected path type in function return type"),
    };

    let type_path = match &**return_type {
        Type::Path(type_path) => type_path,
        _ => panic!("Expected path type in function return type"),
    };

    let segment = type_path
        .path
        .segments
        .last()
        .expect("Expected path segment in Result type");

    let args = match &segment.arguments {
        PathArguments::AngleBracketed(args) => args,
        _ => panic!("Expected angle bracketed arguments in Result type"),
    };

    if args.args.len() != 2 {
        panic!("Expected two generic arguments in Result type");
    }

    let ok_response = match &args.args[0] {
        GenericArgument::Type(ty) => match ty {
            Type::ImplTrait(_) => {
                // Use override_response if provided, otherwise fallback to a default or panic
                if let Some(override_response) = override_response {
                    syn::parse_quote! { #override_response }
                } else {
                    panic!("Cannot extract schema from `impl Trait` without override_response");
                }
            }
            _ => ty.clone(),
        },
        _ => panic!("Expected type for Ok generic argument"),
    };

    let err_response = if let GenericArgument::Type(ty) = &args.args[1] {
        ty.clone()
    } else {
        panic!("Expected path type in Err generic argument");
    };

    (ok_response, err_response)
}

/// Generates the proper utoipa::ToSchema derives. The idea to do it through this macro indirection
/// is to allow to finer attribute search when generating the docs.
pub fn utoipa_schema(args: TokenStream, item: TokenStream) -> TokenStream {
    let item = parse_macro_input!(item as Item);
    let args: TokenStream2 = args.into();

    quote!(
        #[derive(utoipa::ToSchema)]
        #args
        #item
    )
    .into()
}

#[derive(FromMeta, Getters)]
#[getset(get = "pub")]
pub struct UtoipaTagArguments {
    name: LitStr,
    description: LitStr,
}

/// Generates the proper utoipa Tag. The idea to do it through this macro indirection
/// is to allow to find attribute search when generating the docs.
pub fn utoipa_tag(args: TokenStream) -> TokenStream {
    let parsed_args = parse_meta!(UtoipaTagArguments, args).unwrap();

    let const_name = Ident::new(
        format!("{}_TAG", parsed_args.name.value().to_uppercase()).as_str(),
        Span::call_site().into(),
    );
    let name = parsed_args.name;

    quote!(
        pub const #const_name: &str = #name;
    )
    .into()
}
