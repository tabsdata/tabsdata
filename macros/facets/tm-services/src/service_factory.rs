//
// Copyright 2025 Tabsdata Inc.
//

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::token::Comma;
use syn::{
    FnArg, GenericParam, Ident, ItemFn, ReturnType, Token, Type, parse_macro_input, parse_quote,
};

/// Project scope of a service, declared as `scope = "Project"` or `scope =
/// "Global"`. Required for any service with a `connection`. `Project`
/// services take their target project as part of the input (a
/// `Scoped<Request>`) and run every query under `SET LOCAL app.project_id`;
/// `Global` services span all projects (`SET LOCAL app.bypass_rls = on`). A
/// `grep 'scope = "Global"'` is the audit list of cross-project escape
/// hatches.
enum Scope {
    Project,
    Global,
}

struct ProviderArgs {
    name: Ident,
    request: Type,
    response: Type,
    /// The primary connection provider, optionally domain-typed
    /// (`TransactionProvider<AiAgent>`); bare means the apiserver's.
    connection: Option<Type>,
    context: Vec<Type>,
    /// Additional domains this service may read from (repeatable); each gets
    /// a latent `DomainConnection`. Requires a domain-typed `connection`.
    read_connection: Vec<Type>,
    scope: Option<Scope>,
}

impl Parse for ProviderArgs {
    fn parse(input: ParseStream) -> Result<Self, syn::Error> {
        let mut name = None;
        let mut request = None;
        let mut response = None;
        let mut connection = None;
        let mut context = Vec::new();
        let mut read_connection = Vec::new();
        let mut scope = None;

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
                "read_connection" => {
                    let domain: Type = input.parse()?;
                    read_connection.push(domain);
                }
                "scope" => {
                    let value: syn::LitStr = input.parse()?;
                    scope = Some(match value.value().as_str() {
                        "Project" => Scope::Project,
                        "Global" => Scope::Global,
                        _ => {
                            return Err(syn::Error::new(
                                value.span(),
                                "`scope` must be \"Project\" or \"Global\"",
                            ));
                        }
                    });
                }
                _ => return Err(syn::Error::new(key.span(), "Unknown attribute key")),
            }

            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }

        let connection = connection;
        let scope = scope;
        // `scope` is mandatory exactly when the service owns a connection: that
        // is when there is a transaction whose `app.project_id` GUC must be set.
        if connection.is_some() && scope.is_none() {
            return Err(syn::Error::new(
                input.span(),
                "a service with a `connection` must declare `scope = Project` or `scope = Global`",
            ));
        }
        if connection.is_none() && scope.is_some() {
            return Err(syn::Error::new(
                input.span(),
                "`scope` is only meaningful with a `connection`",
            ));
        }

        Ok(ProviderArgs {
            name: name.ok_or_else(|| syn::Error::new(input.span(), "Missing `name`"))?,
            request: request.ok_or_else(|| syn::Error::new(input.span(), "Missing `request`"))?,
            response: response
                .ok_or_else(|| syn::Error::new(input.span(), "Missing `response`"))?,
            connection,
            context,
            read_connection,
            scope,
        })
    }
}

/// Splits `TransactionProvider<AiAgent>` into base and domain.
fn split_domain(ty: &Type) -> (Type, Option<Type>) {
    if let Type::Path(tp) = ty
        && let Some(last) = tp.path.segments.last()
        && let syn::PathArguments::AngleBracketed(args) = &last.arguments
        && args.args.len() == 1
        && let Some(syn::GenericArgument::Type(domain)) = args.args.first()
    {
        let mut base = tp.clone();
        base.path.segments.last_mut().unwrap().arguments = syn::PathArguments::None;
        return (Type::Path(base), Some(domain.clone()));
    }
    (ty.clone(), None)
}

pub fn service_factory(attr: TokenStream, item: TokenStream) -> TokenStream {
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

    // Calculate generics
    let original_generics = func.sig.generics.clone();
    let (impl_generics, ty_generics, where_clause) = original_generics.split_for_impl();
    let generic_params = &original_generics.params;
    let generic_tys = original_generics
        .params
        .iter()
        .filter_map(|p| match p {
            GenericParam::Type(ty) => Some(ty.ident.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let phantom_data = if !generic_tys.is_empty() {
        quote! {
            _phantom: std::marker::PhantomData<(#(#generic_tys),*)>,
        }
    } else {
        quote! {}
    };

    let phantom_data_init = if !generic_tys.is_empty() {
        quote! {
            _phantom: std::marker::PhantomData,
        }
    } else {
        quote! {}
    };

    // Inject `Req, Res` into generics, moving current generics to struct
    func.sig.generics.params = Punctuated::new();
    func.sig.generics.params.insert(0, parse_quote! { Req });
    func.sig.generics.params.insert(1, parse_quote! { Res });

    // Inject In's bounds into the where clause
    let where_clause_new = func.sig.generics.make_where_clause();
    if let Some(og_where) = &where_clause {
        for predicate in og_where.predicates.iter() {
            where_clause_new.predicates.push(predicate.clone());
        }
    }

    where_clause_new.predicates.push(parse_quote! {
        Req: Send + Sync + 'static
    });
    where_clause_new.predicates.push(parse_quote! {
        Res: Send + Sync + 'static
    });

    // Override return type
    func.sig.output = ReturnType::Type(
        syn::token::RArrow::default(),
        Box::new(parse_quote! {
            ::td_tower::service_provider::ServiceProvider<Req, Res, td_error::TdError>
        }),
    );

    // Parse connection and context arguments
    let func_name = func.sig.ident.clone();
    let name = &args.name;
    let req_ty = &args.request;
    let res_ty = &args.response;

    // Derive the service's actual input type and its scope layer from `scope`
    // (validated against `connection` at parse time):
    //   - Project: the input is `Scoped<Request, ProjectId>` — the caller pairs
    //     the request with its target project. The provider unwraps it, runs
    //     `SET LOCAL app.project_id`, and hands the bare request + project id to
    //     the rest of the stack.
    //   - Global: the input is the bare request; the provider bypasses scope.
    //   - no scope (no connection): no scope layer, bare request.
    let (connection_base, connection_domain) = match &args.connection {
        Some(ty) => {
            let (base, domain) = split_domain(ty);
            (Some(base), domain)
        }
        None => (None, None),
    };
    if !args.read_connection.is_empty() && connection_domain.is_none() {
        return syn::Error::new_spanned(
            &args.name,
            "`read_connection` requires a domain-typed `connection` (e.g. `connection = TransactionProvider<SomeDomain>`)",
        )
        .to_compile_error()
        .into();
    }

    let scope_domain = match &connection_domain {
        None => quote! { ::td_database::domain::Apiserver },
        Some(domain) => quote! { #domain },
    };
    let (actual_req, project_scope_provider) = match &args.scope {
        None => (quote! { #req_ty }, vec![]),
        Some(Scope::Project) => (
            quote! { ::td_database::pool::scoped::Scoped<#req_ty, ::td_objects::types::basic::ProjectId> },
            vec![quote! {
                ::td_tower::project_scope::ProjectScopeProvider::<#req_ty, ::td_objects::types::basic::ProjectId, #scope_domain>::new()
            }],
        ),
        Some(Scope::Global) => (
            quote! { #req_ty },
            vec![quote! { ::td_tower::project_scope::GlobalScopeProvider::<#scope_domain>::new() }],
        ),
    };

    let (mut factory_args, mut factory_types) = (vec![], vec![]);
    let (db_input, db_arg, db_provider) = match (&connection_base, &connection_domain) {
        (None, _) => (vec![], None, vec![]),
        // No domain named: the apiserver's, with the single pool as `db: TdPool`.
        (Some(prov), None) => {
            factory_args.push(quote! { ::td_database::pool::TdPool::get_field(ctx) });
            factory_types.push(quote! { ::td_database::pool::TdPool });
            (
                vec![quote! { db: ::td_database::pool::TdPool }],
                Some(quote! { db.clone(), }),
                vec![quote! {
                    #prov::<::td_database::domain::Apiserver>::new(db),
                }],
            )
        }
        // Named domain: the pools travel as one `DomainPools` parameter; a
        // missing domain pool is a boot-time wiring error, so panic.
        (Some(prov), Some(domain)) => {
            factory_args.push(quote! { ::td_database::domain::DomainPools::get_field(ctx) });
            factory_types.push(quote! { ::td_database::domain::DomainPools });
            let mut providers = vec![quote! {
                #prov::<#domain>::new(
                    pools.get::<#domain>().expect("service domain pool not configured").clone(),
                ),
            }];
            for read_domain in &args.read_connection {
                providers.push(quote! {
                    ::td_tower::default_services::LatentConnectionProvider::<#read_domain>::new(
                        pools.get::<#read_domain>().expect("service read_connection pool not configured").clone(),
                    ),
                });
            }
            (
                vec![quote! { pools: ::td_database::domain::DomainPools }],
                Some(quote! { pools.clone(), }),
                providers,
            )
        }
    };
    let (ctx_input, ctx_arg, ctx_ty, ctx_provider): (Vec<_>, Vec<_>, Vec<_>, Vec<_>) = args
        .context
        .iter()
        .map(|ty| {
            let ty_name = format_ident!(
                "{}",
                format!("{}", quote! { #ty })
                    .replace(" ", "_")
                    .to_lowercase()
            );

            factory_args.push(quote! { std::sync::Arc::<#ty>::get_field(ctx) });
            factory_types.push(quote! { std::sync::Arc<#ty> });
            (
                quote! { #ty_name: std::sync::Arc<#ty> },
                quote! { #ty_name },
                quote! { #ty },
                quote! { td_tower::default_services::SrvCtxProvider::new(#ty_name) },
            )
        })
        .collect();

    // Override function input with connection and context arguments
    func.sig.inputs = {
        let mut inputs = Punctuated::<FnArg, Comma>::new();
        for input in db_input.iter().chain(&ctx_input) {
            inputs.push(parse_quote! { #input });
        }
        inputs
    };

    // Factory generics
    let mut factory_where_clause: syn::WhereClause = parse_quote!(where);
    for ty in &factory_types {
        factory_where_clause
            .predicates
            .push(parse_quote!(#ty: ::ta_services::factory::FieldAccessor<C>));
    }
    if let Some(wc) = &where_clause {
        for predicate in &wc.predicates {
            factory_where_clause.predicates.push(predicate.clone());
        }
    }
    let factory_where_clause = if factory_where_clause.predicates.is_empty() {
        quote! {}
    } else {
        quote! {#factory_where_clause}
    };

    // Wrap body in `ServiceProvider`
    let original_block = func.block.clone();
    func.block = parse_quote!({
        use ::td_tower::service_provider::IntoServiceProvider;
        tower::builder::ServiceBuilder::new()
            .layer(td_tower::default_services::ServiceEntry::default())
            #(
                .layer(#ctx_provider)
            )*
            #(
                .layer(#db_provider)
            )*
            #(
                .layer(#project_scope_provider)
            )*
            .layer(#original_block)
            .map_err(td_error::TdError::from)
            .service(td_tower::default_services::ServiceReturn)
            .into_service_provider()
    });

    // Generate provider struct
    TokenStream::from(quote! {
        pub struct #name #impl_generics {
            provider: ::td_tower::service_provider::ServiceProvider<#actual_req, #res_ty, td_error::TdError>,
            #[cfg(feature = "test_tower_metadata")]
            metadata: ::td_tower::service_provider::ServiceProvider<(), td_tower::metadata::Metadata, td_error::TdError>,
            #phantom_data
        }

        #[allow(non_snake_case)]
        impl #impl_generics #name #ty_generics #where_clause {
            pub fn new(#(#db_input,)* #(#ctx_input),*) -> Self {
                Self {
                    provider: Self::#func_name(#db_arg #(#ctx_arg.clone()),*),
                    #[cfg(feature = "test_tower_metadata")]
                    metadata: Self::#func_name(#db_arg #(#ctx_arg.clone()),*),
                    #phantom_data_init
                }
            }

            #[cfg(feature = "test-utils")]
            pub fn with_defaults(#(#db_input),*) -> Self {
                Self {
                    provider: Self::#func_name(#db_arg #(std::sync::Arc::new(#ctx_ty::default())),*),
                    #[cfg(feature = "test_tower_metadata")]
                    metadata: Self::#func_name(#db_arg #(std::sync::Arc::new(#ctx_ty::default())),*),
                    #phantom_data_init
                }
            }

            #func
        }

        impl<C, #generic_params> ::ta_services::factory::ServiceFactory<C> for #name #ty_generics
        #factory_where_clause
        {
            type Service = Self;
            fn build(ctx: &C) -> Self {
                use ::ta_services::factory::FieldAccessor;
                Self::new(#(#factory_args),*)
            }
        }


        impl #impl_generics ::ta_services::service::TdService for #name #ty_generics #where_clause {
            type Request = #actual_req;
            type Response = #res_ty;
            type Error = td_error::TdError;

            fn service(&self) -> ::td_tower::service_provider::TdBoxService<Self::Request, Self::Response, Self::Error> {
                self.provider.make()
            }

            #[cfg(feature = "test_tower_metadata")]
            async fn metadata(&self) -> td_tower::metadata::Metadata {
                use td_tower::ctx_service::RawOneshot;
                let service = self.metadata.make();
                service.raw_oneshot(()).await.unwrap()
            }
        }
    })
}
