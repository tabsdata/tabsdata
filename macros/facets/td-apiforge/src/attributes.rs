//
//  Copyright 2024 Tabs Data Inc.
//

use proc_macro::TokenStream;

use darling::FromMeta;
use quote::quote;
use syn::{
    parse_macro_input, FnArg, GenericArgument, Ident, ItemFn, PathArguments, ReturnType, Type,
};

use td_shared::parse_meta;

#[derive(FromMeta)]
pub struct UtoipaPathArguments {
    #[darling(default)]
    method: Option<Ident>,
    #[darling(default)]
    path: Option<Ident>,
    #[darling(default)]
    tag: Option<Ident>,
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
    let (ok_response, err_response) = extract_result_types(&input);

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
        #[utoipa::path(
            #utoipa_attr
        )]
        #input
    };

    output.into()
}

fn more_than_one(a: bool, b: bool, c: bool) -> bool {
    let count = a as u8 + b as u8 + c as u8;
    count > 1
}

fn has_type(input: &ItemFn, method_type: &str) -> bool {
    input.sig.inputs.iter().any(|arg| {
        if let FnArg::Typed(pat_type) = arg
            && let Type::Path(type_path) = &*pat_type.ty
        {
            return type_path
                .path
                .segments
                .iter()
                .any(|seg| seg.ident == method_type);
        }
        false
    })
}
/// Extracts the type of the generic argument in the arguments of a given type signature.
pub fn extract_type_in_generic_argument(input: &ItemFn, generic_attribute: &str) -> Vec<Type> {
    let mut vec = Vec::new();
    input.sig.inputs.iter().for_each(|arg| {
        if let FnArg::Typed(pat_type) = arg
            && let Type::Path(type_path) = &*pat_type.ty
            && has_generic_attribute(&type_path.path, generic_attribute)
        {
            let type_ = extract_first_generic_argument(&type_path.path);
            if let Some(type_) = type_ {
                vec.push(type_);
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
fn extract_first_generic_argument(path: &syn::Path) -> Option<Type> {
    if let PathArguments::AngleBracketed(args) = &path.segments.last().unwrap().arguments
        && let Some(GenericArgument::Type(ty)) = args.args.first()
    {
        return Some(ty.clone());
    }
    None
}

/// Extracts the Ok and Err response types from the Result type in the function signature.
pub fn extract_result_types(input: &ItemFn) -> (Type, Type) {
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

    let ok_response = if let GenericArgument::Type(ty) = &args.args[0] {
        ty.clone()
    } else {
        panic!("Expected type for Ok generic argument");
    };

    let err_response = if let GenericArgument::Type(ty) = &args.args[1] {
        ty.clone()
    } else {
        panic!("Expected path type in Err generic argument");
    };

    (ok_response, err_response)
}
