//
// Copyright 2025 Tabs Data Inc.
//

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::token::Comma;
use syn::{parse_macro_input, parse_quote, FnArg, Ident, ItemFn, ReturnType, Token, Type};

struct ProviderArgs {
    name: Ident,
    request: Type,
    response: Type,
    connection: Option<Type>,
    context: Vec<Type>,
}

impl Parse for ProviderArgs {
    fn parse(input: ParseStream) -> Result<Self, syn::Error> {
        let mut name = None;
        let mut request = None;
        let mut response = None;
        let mut connection = None;
        let mut context = Vec::new();

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;

            match key.to_string().as_str() {
                "name" => name = Some(input.parse()?),
                "request" => request = Some(input.parse()?),
                "response" => response = Some(input.parse()?),
                "connection" => connection = Some(input.parse()?),
                "context" => {
                    let ctx: Type = input.parse()?;
                    context.push(ctx);
                }
                _ => return Err(syn::Error::new(key.span(), "Unknown attribute key")),
            }

            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(ProviderArgs {
            name: name.ok_or_else(|| syn::Error::new(input.span(), "Missing `name`"))?,
            request: request.ok_or_else(|| syn::Error::new(input.span(), "Missing `request`"))?,
            response: response
                .ok_or_else(|| syn::Error::new(input.span(), "Missing `response`"))?,
            connection,
            context,
        })
    }
}

pub fn provider(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as ProviderArgs);
    let mut func = parse_macro_input!(item as ItemFn);

    // Validate fn has no input arguments
    if !func.sig.inputs.is_empty() {
        return syn::Error::new_spanned(
            func.sig,
            "Provider function must not have any input arguments",
        )
        .to_compile_error()
        .into();
    }

    // Fail if async
    if func.sig.asyncness.is_some() {
        return syn::Error::new_spanned(func.sig, "Provider function must not be async")
            .to_compile_error()
            .into();
    }

    // Inject `Req, Res` into generics
    func.sig.generics.params.insert(0, parse_quote! { Req });
    func.sig.generics.params.insert(1, parse_quote! { Res });

    // Inject In's bounds into the where clause
    let where_clause = func.sig.generics.make_where_clause();
    where_clause.predicates.push(parse_quote! {
        Req: td_tower::default_services::Share
    });
    where_clause.predicates.push(parse_quote! {
        Res: td_tower::default_services::Share
    });

    // Override return type
    func.sig.output = ReturnType::Type(
        syn::token::RArrow::default(),
        Box::new(parse_quote! {
            td_tower::service_provider::ServiceProvider<Req, Res, td_error::TdError>
        }),
    );

    // Parse connection and context arguments
    let func_name = func.sig.ident.clone();
    let name = &args.name;
    let req_ty = &args.request;
    let res_ty = &args.response;
    let (db_input, db_arg, db_provider) = match &args.connection {
        None => (None, None, vec![]),
        Some(prov) => (
            Some(quote! { db: td_database::sql::DbPool, }),
            Some(quote! { db.clone(), }),
            vec![quote! { #prov::new(db), }],
        ),
    };
    let (ctx_input, ctx_arg, ctx_provider): (Vec<_>, Vec<_>, Vec<_>) = args
        .context
        .iter()
        .map(|ty| {
            (
                quote! { #ty: std::sync::Arc<#ty> },
                quote! { #ty },
                quote! { td_tower::default_services::SrvCtxProvider::new(#ty) },
            )
        })
        .collect();

    // Override function input with connection and context arguments
    func.sig.inputs = {
        let mut inputs = Punctuated::<FnArg, Comma>::new();
        if db_input.is_some() {
            inputs.push(parse_quote! { db: td_database::sql::DbPool });
        }
        for ctx in &args.context {
            inputs.push(parse_quote! { #ctx: std::sync::Arc<#ctx> });
        }
        inputs
    };

    // Wrap body in `ServiceProvider`
    let original_block = func.block;
    func.block = parse_quote!({
        tower::builder::ServiceBuilder::new()
            .layer(td_tower::default_services::ServiceEntry::default())
            #(
                .layer(#ctx_provider)
            )*
            #(
                .layer(#db_provider)
            )*
            .layer(#original_block)
            .map_err(td_error::TdError::from)
            .service(td_tower::default_services::ServiceReturn)
            .into_service_provider()
    });

    // Generate provider struct
    TokenStream::from(quote! {
        pub struct #name {
            provider: td_tower::service_provider::ServiceProvider<#req_ty, #res_ty, td_error::TdError>,
            #[cfg(feature = "test_tower_metadata")]
            metadata: td_tower::service_provider::ServiceProvider<(), td_tower::metadata::MetadataMutex, td_error::TdError>,
        }

        #[allow(non_snake_case)]
        impl #name {
            pub fn new(#db_input #(#ctx_input),*) -> Self {
                Self {
                    provider: Self::#func_name(#db_arg #(#ctx_arg.clone()),*),
                    #[cfg(feature = "test_tower_metadata")]
                    metadata: Self::#func_name(#db_arg #(#ctx_arg.clone()),*),
                }
            }

            #[cfg(test)]
            pub async fn with_defaults(#db_input) -> Self {
                use crate::service_default::ServiceDefault;
                Self {
                    provider: Self::#func_name(#db_arg #(#ctx_arg::service_default().await),*),
                    #[cfg(feature = "test_tower_metadata")]
                    metadata: Self::#func_name(#db_arg #(#ctx_arg::service_default().await),*),
                }
            }

            #[cfg(feature = "test_tower_metadata")]
            pub async fn metadata(&self) -> td_tower::metadata::Metadata {
                use td_tower::ctx_service::RawOneshot;
                let service = self.metadata.make().await;
                let metadata_mutex = service.raw_oneshot(()).await.unwrap();
                metadata_mutex.get()
            }

            pub async fn service(
                &self,
            ) -> td_tower::service_provider::TdBoxService<#req_ty, #res_ty, TdError> {
                self.provider.make().await
            }

            #func
        }
    })
}
