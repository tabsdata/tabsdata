//
// Copyright 2025 Tabsdata Inc.
//

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::{quote, quote_spanned};
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{FnArg, Ident, ItemFn, ReturnType, Token, Type, parse_macro_input, parse_quote};

struct FactoryArgs {
    /// The struct the entry method is added to; `crate::Services` by default.
    on: Option<syn::Path>,
}

impl Parse for FactoryArgs {
    fn parse(input: ParseStream) -> Result<Self, syn::Error> {
        let mut on = None;

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;

            match key.to_string().as_str() {
                "on" => on = Some(input.parse()?),
                _ => return Err(syn::Error::new(key.span(), "Unknown attribute key")),
            }

            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(FactoryArgs { on })
    }
}

pub fn service_factory(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as FactoryArgs);
    let func = parse_macro_input!(item as ItemFn);
    match expand(args, func) {
        Ok(ts) => ts,
        Err(e) => e.to_compile_error().into(),
    }
}

/// How each fn parameter of the service fn is provisioned. The request is
/// always the **last** parameter (`_request: ()` if the service takes none);
/// `&mut ReqCtx` is the warnings accumulator (a wrapper-local, the one
/// special case); every other parameter is extracted.
enum FnParam {
    Extracted(usize),
    ReqCtx,
    Request,
}

/// An extracted parameter, as declared: `&mut T` is `ExtractMut` — the wrapper
/// keeps it and ends it once the body returns — and `T` is `Extract`, moved in.
/// The traits are implemented on the declared form, so the two cannot be
/// confused.
struct Extracted {
    ty: Type,
    by_ref: bool,
}

fn last_segment(ty: &Type) -> Option<&syn::PathSegment> {
    match ty {
        Type::Path(p) => p.path.segments.last(),
        Type::Reference(r) => last_segment(&r.elem),
        _ => None,
    }
}

fn is_req_ctx(ty: &Type) -> bool {
    matches!(ty, Type::Reference(r) if r.mutability.is_some())
        && last_segment(ty).is_some_and(|s| s.ident == "ReqCtx")
}

fn expand(args: FactoryArgs, mut func: ItemFn) -> Result<TokenStream, syn::Error> {
    if func.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            &func.sig,
            "a service fn must be `async fn`",
        ));
    }

    let target: syn::Path = args.on.unwrap_or_else(|| parse_quote!(crate::Services));
    // The entry method takes the fn's own identifier (span preserved, so IDE
    // navigation between call sites and the fn body works); the fn itself is
    // renamed out of the way.
    let entry_name = func.sig.ident.clone();
    let func_name = syn::Ident::new(&format!("__{entry_name}"), proc_macro2::Span::call_site());
    func.sig.ident = func_name.clone();

    // Response type from `-> Result<Res, TdError>`.
    let res_ty: Type = match &func.sig.output {
        ReturnType::Type(_, ty) => match ty.as_ref() {
            Type::Path(p) if p.path.segments.last().is_some_and(|s| s.ident == "Result") => {
                match &p.path.segments.last().unwrap().arguments {
                    syn::PathArguments::AngleBracketed(a) => match a.args.first() {
                        Some(syn::GenericArgument::Type(t)) => t.clone(),
                        _ => {
                            return Err(syn::Error::new_spanned(
                                ty,
                                "service fn must return `Result<Res, TdError>`",
                            ));
                        }
                    },
                    _ => {
                        return Err(syn::Error::new_spanned(
                            ty,
                            "service fn must return `Result<Res, TdError>`",
                        ));
                    }
                }
            }
            _ => {
                return Err(syn::Error::new_spanned(
                    ty,
                    "service fn must return `Result<Res, TdError>`",
                ));
            }
        },
        ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                &func.sig,
                "service fn must return `Result<Res, TdError>`",
            ));
        }
    };

    if let Some(param) = func.sig.generics.params.first() {
        return Err(syn::Error::new_spanned(
            param,
            "a service fn cannot be generic; take runtime values via its request",
        ));
    }

    // Classify parameters, in declaration order: the last one is the request,
    // the rest are `&mut ReqCtx` or `Extract`ed.
    let param_count = func.sig.inputs.len();
    if param_count == 0 {
        return Err(syn::Error::new_spanned(
            &func.sig,
            "a service fn's last parameter is its request; one that takes none \
             declares `_request: ()`",
        ));
    }
    let mut kinds: Vec<FnParam> = Vec::new();
    let mut extracted: Vec<Extracted> = Vec::new();
    let mut has_req_ctx = false;
    let mut req_ty: Option<Type> = None;

    for (i, input) in func.sig.inputs.iter().enumerate() {
        let pat_ty = match input {
            FnArg::Typed(t) => t,
            FnArg::Receiver(r) => {
                return Err(syn::Error::new_spanned(r, "service fn cannot take self"));
            }
        };
        if i == param_count - 1 {
            if is_req_ctx(&pat_ty.ty) {
                return Err(syn::Error::new_spanned(
                    pat_ty,
                    "the request must be the last parameter; a service without one \
                     declares `_request: ()`",
                ));
            }
            req_ty = Some(pat_ty.ty.as_ref().clone());
            kinds.push(FnParam::Request);
        } else if is_req_ctx(&pat_ty.ty) {
            if has_req_ctx {
                return Err(syn::Error::new_spanned(
                    pat_ty,
                    "duplicate `ReqCtx` parameter",
                ));
            }
            has_req_ctx = true;
            kinds.push(FnParam::ReqCtx);
        } else {
            let extract = match pat_ty.ty.as_ref() {
                Type::Reference(r) if r.mutability.is_some() => Extracted {
                    ty: pat_ty.ty.as_ref().clone(),
                    by_ref: true,
                },
                Type::Reference(r) => {
                    return Err(syn::Error::new_spanned(
                        r,
                        "a service parameter is taken by value or by `&mut`: `&mut T` is \
                         owned by the wrapper and ended when the service returns, `T` is \
                         owned by the body",
                    ));
                }
                ty => Extracted {
                    ty: ty.clone(),
                    by_ref: false,
                },
            };
            kinds.push(FnParam::Extracted(extracted.len()));
            extracted.push(extract);
        }
    }

    let req_ty = req_ty.map(|t| quote! { #t }).unwrap_or(quote! { () });

    // Extraction bindings, in declaration order — errors point at the param.
    // A failed extraction first ends every already-kept parameter (newest
    // first), so an earlier connection's rollback is awaited, not drop-queued.
    let bindings: Vec<proc_macro2::TokenStream> = extracted
        .iter()
        .enumerate()
        .map(|(i, e)| {
            let name = syn::Ident::new(&format!("__p{i}"), proc_macro2::Span::call_site());
            let ty = &e.ty;
            let (binding, trait_path) = if e.by_ref {
                (
                    quote! { let mut #name },
                    quote! { ::ta_services::factory::ExtractMut<Self, #req_ty> },
                )
            } else {
                (
                    quote! { let #name },
                    quote! { ::ta_services::factory::Extract<Self, #req_ty> },
                )
            };
            let abandons: Vec<proc_macro2::TokenStream> = extracted[..i]
                .iter()
                .enumerate()
                .filter(|(_, prior)| prior.by_ref)
                .rev()
                .map(|(j, prior)| {
                    let prior_name =
                        syn::Ident::new(&format!("__p{j}"), proc_macro2::Span::call_site());
                    let prior_ty = &prior.ty;
                    quote! {
                        ::ta_services::factory::abandon_param::<#prior_ty, Self, #req_ty>(
                            #prior_name,
                            &__e,
                            __pass,
                        )
                        .await;
                    }
                })
                .collect();
            if abandons.is_empty() {
                quote_spanned! { ty.span() =>
                    #binding = <#ty as #trait_path>::extract(self, &__request_pass).await?;
                }
            } else {
                quote_spanned! { ty.span() =>
                    #binding = match <#ty as #trait_path>::extract(self, &__request_pass).await {
                        Ok(__v) => __v,
                        Err(__e) => {
                            #(#abandons)*
                            return Err(__e);
                        }
                    };
                }
            }
        })
        .collect();

    // Call arguments, in the fn's declared order.
    let call_args: Vec<proc_macro2::TokenStream> = kinds
        .iter()
        .map(|k| match k {
            FnParam::Extracted(i) => {
                let name = syn::Ident::new(&format!("__p{i}"), proc_macro2::Span::call_site());
                if extracted[*i].by_ref {
                    quote! { &mut #name }
                } else {
                    quote! { #name }
                }
            }
            FnParam::ReqCtx => quote! { &mut __req_ctx },
            FnParam::Request => quote! { __request_pass },
        })
        .collect();

    // Endings run newest-first, the reverse of acquisition.
    let finishes: Vec<proc_macro2::TokenStream> = extracted
        .iter()
        .enumerate()
        .filter(|(_, e)| e.by_ref)
        .rev()
        .map(|(i, e)| {
            let name = syn::Ident::new(&format!("__p{i}"), proc_macro2::Span::call_site());
            let ty = &e.ty;
            quote! {
                let __out = ::ta_services::factory::finish_param::<#ty, Self, #req_ty, _>(
                    #name,
                    __out,
                    __pass,
                    &mut __retry,
                )
                .await;
            }
        })
        .collect();

    let unit = entry_name.to_string();
    let retried = extracted.iter().any(|e| e.by_ref);
    let run = if retried {
        quote! {
            let mut __pass = ::ta_services::factory::Pass::first(#unit);
            loop {
                #[allow(unused_variables, unused_mut)]
                let mut __req_ctx = ::ta_services::ctx_service::ReqCtx::default();
                let __request_pass = ::core::clone::Clone::clone(&__request);
                #(#bindings)*
                let __out = Self::#func_name(#(#call_args),*).await;
                let mut __retry = false;
                #(#finishes)*
                if __retry {
                    __pass = __pass.next();
                    continue;
                }
                return Ok(::ta_services::ctx_service::CtxResponse::new(
                    __out?,
                    __req_ctx,
                ));
            }
        }
    } else {
        quote! {
            #[allow(unused_variables, unused_mut)]
            let mut __req_ctx = ::ta_services::ctx_service::ReqCtx::default();
            let __request_pass = __request;
            #(#bindings)*
            let __out = Self::#func_name(#(#call_args),*).await;
            Ok(::ta_services::ctx_service::CtxResponse::new(
                __out?,
                __req_ctx,
            ))
        }
    };

    Ok(TokenStream::from(quote! {
        impl #target {
            #func

            /// Run the service: extract the fn's parameters, execute it, end
            /// the ones it borrowed, and fold the accumulated messages into the
            /// response envelope. The body's result is held rather than
            /// propagated, so a failing body still reaches those endings. When
            /// an ended parameter asks for it, the whole service runs again on
            /// a fresh copy of the request, and only the last run's messages
            /// are kept.
            pub async fn #entry_name(
                &self,
                __request: #req_ty,
            ) -> Result<::ta_services::ctx_service::CtxResponse<#res_ty>, td_error::TdError> {
                #run
            }
        }
    }))
}
