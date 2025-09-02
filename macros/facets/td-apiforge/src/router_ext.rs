//
// Copyright 2025 Tabs Data Inc.
//

use crate::attributes::{extract_result_types, extract_type_in_generic_argument};
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use std::sync::atomic::{AtomicUsize, Ordering};
use syn::{
    parse::{Parse, ParseStream}, parse_macro_input, FnArg, GenericArgument, Ident, ItemMod, PathArguments,
    Result as SynResult,
    Type,
};

struct RouterAttr {
    name: Ident,
}

impl Parse for RouterAttr {
    fn parse(input: ParseStream) -> SynResult<Self> {
        let name_val: Ident = input.parse()?;
        Ok(RouterAttr { name: name_val })
    }
}

pub fn router_ext(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the attribute arguments
    let RouterAttr { name: router_name } = parse_macro_input!(attr as RouterAttr);

    // Parse the module containing all route functions
    let input_mod = parse_macro_input!(item as ItemMod);
    let (_, mut items) = match input_mod.content {
        Some((brace, items)) => (brace, items),
        None => {
            return syn::Error::new_spanned(input_mod, "Expected module with braces")
                .to_compile_error()
                .into();
        }
    };
    let input_mod_ident = &input_mod.ident;

    fn make_unique_ident(orig_name: &syn::Ident) -> syn::Ident {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);

        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        format_ident!("{}_{}", orig_name, unique)
    }

    // Recollect all items
    let (mut items, api_server_route_fns, mut state_types, schema_components) =
        items.iter_mut().fold(
            (Vec::new(), Vec::new(), Vec::new(), Vec::new()),
            |(mut items, mut api_server_route_fns, mut state_types, mut schema_components),
             item| {
                if let syn::Item::Fn(f) = item
                    && f.attrs
                        .iter()
                        .any(|attr| attr.path().is_ident("apiserver_path"))
                {
                    // We rename the fn to ensure different fn names so swagger behaves properly
                    let new_fn_ident = make_unique_ident(&f.sig.ident);
                    f.sig.ident = new_fn_ident.clone();
                    api_server_route_fns.push(new_fn_ident.clone());

                    // Collect states to generate trait bounds
                    for input in &f.sig.inputs {
                        if let FnArg::Typed(pat_type) = input
                            && let Type::Path(type_path) = &*pat_type.ty
                        {
                            let segments = &type_path.path.segments;
                            if let Some(seg) = segments.last()
                                && seg.ident == "State"
                                && let PathArguments::AngleBracketed(args) = &seg.arguments
                            {
                                for arg in &args.args {
                                    if let GenericArgument::Type(ty) = arg {
                                        state_types.push(ty.clone());
                                    }
                                }
                            }
                        }
                    }

                    // (NOTE: the utoipa::OpenApi derive to add schemas should not be necessary, and we
                    //        will probably be able to remove it in the future.
                    //        However, for now, utoipa does not automatically collect response and params schemas from
                    //        concrete types, only tuples. Check [`utoipa_gen::openapi::path::response::Response::get_component_schemas`]
                    let (ok_response, err_response) = extract_result_types(f);
                    schema_components.push(ok_response);
                    schema_components.push(err_response);
                    let query_params = extract_type_in_generic_argument(f, "Query");
                    schema_components.extend(query_params);
                    let path_params = extract_type_in_generic_argument(f, "Path");
                    schema_components.extend(path_params);
                }
                items.push(item.clone());

                (items, api_server_route_fns, state_types, schema_components)
            },
        );

    // Deduplicate schema components
    state_types.dedup();

    let router_impl = quote! {
        impl<S> ::ta_apiserver::router::RouterExtension<S> for super::#router_name
        where
            S: Clone + Send + Sync + 'static,
            #(#state_types: axum::extract::FromRef<S>,)*
        {
            fn router(state: S) -> utoipa_axum::router::OpenApiRouter {
                #[derive(utoipa::OpenApi)]
                #[openapi(components(schemas( #(#schema_components),* )))]
                struct Api;

                utoipa_axum::router::OpenApiRouter::with_openapi(<Api as utoipa::OpenApi>::openapi())
                    #(.routes(utoipa_axum::routes!(#api_server_route_fns)))*
                    .with_state(state)
            }
        }
    };

    // Append router impl
    items.push(syn::parse2(router_impl).unwrap());

    let expanded = quote! {
        pub struct #router_name;

        mod #input_mod_ident {
            #(#items)*
        }
    };

    TokenStream::from(expanded)
}
