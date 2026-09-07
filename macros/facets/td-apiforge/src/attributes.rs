//
//  Copyright 2024 Tabsdata Inc.
//

use proc_macro::TokenStream;

use darling::FromMeta;
use quote::{format_ident, quote};
use syn::{
    FnArg, GenericArgument, Ident, ItemFn, LitStr, PathArguments, ReturnType, Type,
    parse_macro_input,
};

use td_shared::parse_meta;

#[derive(FromMeta)]
pub struct UtoipaPathArguments {
    #[darling(default)]
    mount: Option<Ident>,
    /// The endpoint's one-line description; becomes the fn's doc comment and,
    /// through it, the OpenAPI summary.
    #[darling(default)]
    doc: Option<LitStr>,
    #[darling(default)]
    method: Option<Ident>,
    #[darling(default)]
    path: Option<LitStr>,
    #[darling(default)]
    tag: Option<Ident>,
    /// Document the OK response inline with this content type instead of
    /// using the OK return type's `IntoResponses` impl. The body schema is
    /// `Vec<u8>` (opaque bytes).
    #[darling(default)]
    content_type: Option<String>,
}

fn mount_variant(mount: &Option<Ident>, input: &ItemFn) -> Result<Ident, syn::Error> {
    let mount = mount.as_ref().ok_or_else(|| {
        syn::Error::new_spanned(
            &input.sig.ident,
            "missing mount class: `#[apiserver_path(mount = <unauthenticated|global|rbac|simple|project|internal|root>, ...)]`",
        )
    })?;
    let variant = match mount.to_string().as_str() {
        "unauthenticated" => "Unauthenticated",
        "global" => "Global",
        "rbac" => "Rbac",
        "simple" => "Simple",
        "project" => "Project",
        "internal" => "Internal",
        "root" => "Root",
        other => {
            return Err(syn::Error::new(
                mount.span(),
                format!(
                    "unknown mount `{other}`; expected one of unauthenticated, global, rbac, simple, project, internal, root"
                ),
            ));
        }
    };
    Ok(Ident::new(variant, mount.span()))
}

/// Utoipa path attribute macro generator. It takes the Axum fn as an argument, as well as
/// the mount, method, path, and tag attributes. It generates the appropriate utoipa::path
/// attribute plus the route's inventory registration.
pub fn utoipa_path(args: TokenStream, item: TokenStream) -> TokenStream {
    let parsed_args = parse_meta!(UtoipaPathArguments, args).unwrap();

    let input = parse_macro_input!(item as ItemFn);

    let mount = match mount_variant(&parsed_args.mount, &input) {
        Ok(m) => m,
        Err(e) => return e.to_compile_error().into(),
    };

    // Extract method, path, and tag from the attribute arguments
    let method = parsed_args.method;
    let Some(doc) = parsed_args.doc else {
        return syn::Error::new_spanned(
            &input.sig.ident,
            "missing endpoint doc: `#[apiserver_path(doc = \"...\", ...)]`",
        )
        .to_compile_error()
        .into();
    };
    let Some(path) = parsed_args.path else {
        return syn::Error::new_spanned(
            &input.sig.ident,
            "missing route path: `#[apiserver_path(path = \"/...\", ...)]`",
        )
        .to_compile_error()
        .into();
    };
    let path = Some(path);
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

    // The `{project}` segment of `/projects/{project}/...` routes is resolved
    // in middleware and read via the `Project` extractor,
    // never a `Path<_>` — so utoipa can't see it. Emit it explicitly (first,
    // matching URL order) whenever the handler takes one of those extractors.
    // The raw `Extension<ProjectId>` form is rejected: it would silently skip
    // that documentation.
    if let Some(offender) = project_id_extension_arg(&input) {
        return syn::Error::new_spanned(
            offender,
            "Extension<ProjectId> is not allowed in apiserver handlers; take the `Project` extractor so the project path parameter is documented",
        )
        .to_compile_error()
        .into();
    }
    let project_scoped = has_project_extractor(&input);

    let mut params = quote! {};

    if project_scoped {
        params = quote! {
            ("project" = String, Path, description = "Project name or id"),
        };
    }

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

    if project_scoped || !query_params.is_empty() || !path_params.is_empty() {
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

    if let Some(ct) = parsed_args.content_type.as_deref() {
        utoipa_attr = quote! {
            #utoipa_attr
            responses(
                (status = 200, description = "OK", body = Vec<u8>, content_type = #ct),
                #err_response
            ),
        };
    } else {
        utoipa_attr = quote! {
            #utoipa_attr
            responses(#ok_response, #err_response),
        };
    }

    utoipa_attr = quote! {
        #utoipa_attr
        security(
            ("Token" = [])
        ),
    };

    // The route's registration: a one-route `OpenApiRouter` build fn (the
    // handler plus its schema components, state applied) submitted to the
    // inventory registry under the declared mount.
    let mut state_types = extract_type_in_generic_argument(&input, "State");
    state_types.dedup_by_key(|ty| quote!(#ty).to_string());
    let state_application = match state_types.as_slice() {
        [] => quote! {
            let _ = states;
            let state = ();
        },
        [ty] => quote! {
            let state = states.get::<#ty>().clone();
        },
        _ => {
            return syn::Error::new_spanned(
                &input.sig.ident,
                "route registration requires the handler to extract a single State<T> type",
            )
            .to_compile_error()
            .into();
        }
    };
    let fn_ident = &input.sig.ident;
    let fn_name = fn_ident.to_string();
    let path_const = format_ident!("{}_PATH", fn_name.to_uppercase());
    let api_ident = format_ident!("__api_{}", fn_ident);
    let build_ident = format_ident!("__registry_build_{}", fn_ident);
    let mut schema_components = vec![ok_response.clone(), err_response.clone()];
    schema_components.extend(query_params.iter().cloned());
    schema_components.extend(path_params.iter().cloned());

    let output = quote! {
        /// This route's URL template, as declared on the handler. Referenced by
        /// tests and by the topology assembly; unused for most routes.
        #[allow(dead_code)]
        pub const #path_const: &str = #path;

        #[utoipa::path(
            #utoipa_attr
        )]
        #[doc = #doc]
        #input

        #[allow(non_camel_case_types)]
        #[derive(utoipa::OpenApi)]
        #[openapi(components(schemas( #(#schema_components),* )))]
        struct #api_ident;

        fn #build_ident(
            states: &::ta_apiserver::router::RouterStates,
        ) -> utoipa_axum::router::OpenApiRouter {
            #state_application
            utoipa_axum::router::OpenApiRouter::with_openapi(
                <#api_ident as utoipa::OpenApi>::openapi(),
            )
            .routes(utoipa_axum::routes!(#fn_ident))
            .with_state(state)
        }

        ::ta_apiserver::router::inventory::submit! {
            ::ta_apiserver::router::RouterRegistration {
                name: #fn_name,
                mount: ::ta_apiserver::router::Mount::#mount,
                build: #build_ident,
            }
        }
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
/// True if the handler takes the `Project` extractor — the
/// forms that carry the resolved `/projects/{project}/...` segment. utoipa
/// can't otherwise see it, since it's resolved in middleware, not a `Path<_>`.
fn has_project_extractor(input: &ItemFn) -> bool {
    input.sig.inputs.iter().any(|arg| {
        if let FnArg::Typed(pat_type) = arg
            && let Type::Path(type_path) = &*pat_type.ty
            && let Some(seg) = type_path.path.segments.last()
        {
            return seg.ident == "Project";
        }
        false
    })
}

/// The banned `Extension<ProjectId>` argument, if the handler takes one — the
/// raw form that predates the `Project` extractor and
/// would skip the documented `{project}` parameter.
fn project_id_extension_arg(input: &ItemFn) -> Option<&FnArg> {
    input.sig.inputs.iter().find(|arg| {
        if let FnArg::Typed(pat_type) = arg
            && let Type::Path(type_path) = &*pat_type.ty
            && has_generic_attribute(&type_path.path, "Extension")
            && let Some(Type::Path(inner)) = extract_first_generic_argument(&type_path.path)
        {
            return inner
                .path
                .segments
                .last()
                .map(|s| s.ident == "ProjectId")
                .unwrap_or(false);
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
