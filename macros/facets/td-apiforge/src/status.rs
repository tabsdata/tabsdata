//
// Copyright 2025 Tabs Data Inc.
//

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    token::Comma,
    Ident, Type,
};

pub const CTX_PREFIX: &str = "Ctx";
pub const CTX_MACRO_NAME: &str = "ctx_macro_gen";

/// Struct to hold the input for the status macro.
struct StatusMacroInput {
    enum_name: Ident,
    variants: Vec<StatusVariant>,
}

/// Struct to represent a variant in the status macro.
struct StatusVariant {
    name: Ident,
    response_type: Option<Type>,
}

impl Parse for StatusMacroInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let enum_name: Ident = input.parse()?;
        let _: Comma = input.parse()?;

        let variants = Punctuated::<StatusVariant, Comma>::parse_terminated(input)?
            .into_iter()
            .collect();

        Ok(Self {
            enum_name,
            variants,
        })
    }
}

impl Parse for StatusVariant {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name: Ident = input.parse()?;

        let response_type = if input.peek(syn::Token![=>]) {
            input.parse::<syn::Token![=>]>()?;
            Some(input.parse()?)
        } else {
            None
        };

        Ok(Self {
            name,
            response_type,
        })
    }
}

/// Macro to generate status enums and their implementations.
pub fn status(input: TokenStream) -> TokenStream {
    let StatusMacroInput {
        enum_name,
        variants,
    } = syn::parse_macro_input!(input as StatusMacroInput);
    let enum_ident = format_ident!("{}", enum_name);

    let variant_definitions = variants.iter().map(|variant| {
        let variant_name = &variant.name;
        if let Some(response_type) = &variant.response_type {
            quote! {
                #[response(status = StatusCode::#variant_name, description = stringify!(#variant_name))]
                #variant_name(#response_type)
            }
        } else {
            quote! {
                #[response(status = StatusCode::#variant_name, description = stringify!(#variant_name))]
                #variant_name
            }
        }
    });

    let response_match_arms = variants.iter().map(|variant| {
        let variant_name = &variant.name;
        if variant.response_type.is_some() {
            quote! {
                Self::#variant_name(response) => (http::StatusCode::#variant_name, axum::Json(serde_json::json!(response))).into_response(),
            }
        } else {
            quote! {
                Self::#variant_name => (http::StatusCode::#variant_name).into_response(),
            }
        }
    });

    let output = quote! {
        #[derive(utoipa::IntoResponses, serde::Serialize)]
        #[allow(dead_code)]
        pub enum #enum_ident {
            #(#variant_definitions,)*
        }

        impl axum::response::IntoResponse for #enum_ident {
            fn into_response(self) -> axum::response::Response {
                match self {
                    #(#response_match_arms)*
                }
            }
        }
    };

    output.into()
}

macro_rules! raw_macro_gen {
    ($name:ident, $status:ident) => {
        /// Macro to generate raw status enums.
        pub fn $name(input: TokenStream) -> TokenStream {
            let response_ident = syn::parse_macro_input!(input as Ident);

            paste::paste! {
                let status_input = quote! {
                    [< $name:camel >],
                    $status => #response_ident,
                };
            }

            let status_output: proc_macro2::TokenStream = status(status_input.into()).into();

            let output = quote! {
                #status_output
            };

            output.into()
        }
    };
}

macro_rules! ctx_macro_gen {
    ($name:ident, $status:ident $(, $($indirection:ty),*)?) => {
        #[allow(unused_mut)]
        /// Macro to generate contextual status enums.
        pub fn $name(input: TokenStream) -> TokenStream {
            let response_ident = syn::parse_macro_input!(input as Ident);

            paste::paste! {
                let name = quote! { [< $name:camel >] };
            }
            let struct_name = format_ident!("{}{}", name.to_string(), response_ident.to_string());

            let mut full_type = quote! { #response_ident };

            let mut type_conversion_impls = quote! {};
            let mut indirections: proc_macro2::TokenStream = quote! {};

            let mut count = 0;
            let indirection_type = quote! { #response_ident };

            $(
                $(
                    let type_name = format_ident!("{}Indirection{}", name.to_string(), count.to_string());
                    let indirection = quote! { $indirection };
                    indirections.extend(quote! {
                        #[td_concrete::concrete]
                        #[td_apiforge::api_server_schema]
                        type #type_name = #indirection<#indirection_type>;
                    });
                    type_conversion_impls.extend(quote! {
                        let value = value.transform(|v| #type_name::from(v));
                    });
                    let indirection_type = quote! { #type_name };
                    full_type = quote! { #indirection<#full_type> };

                    count += 1;
                )*
            )?

            let type_name = format_ident!("{}{}", CTX_PREFIX, struct_name.to_string());
            type_conversion_impls.extend(quote! {
                let value: #type_name = value.into();
            });
            let indirection = quote! { CtxResponse };

            indirections.extend(quote! {
                #[td_concrete::concrete]
                #[td_apiforge::api_server_schema]
                type #type_name = #indirection<#indirection_type>;
            });

            full_type = quote! { #indirection<#full_type> };

            let mut additional_impl = quote! {};
            if count > 0 {
                additional_impl.extend(quote! {
                    impl From<#full_type> for #type_name {
                        fn from(value: #full_type) -> #type_name {
                            #type_conversion_impls
                            value
                        }
                    }
                });
            }

            let status_input = quote! {
                #name,
                $status => #type_name,
            };

            let status_output: proc_macro2::TokenStream = status(status_input.into()).into();

            let output = quote! {
                #status_output
                #indirections
                #additional_impl
            };

            output.into()
        }
    };
}

ctx_macro_gen!(get_status, OK);
ctx_macro_gen!(list_status, OK, ListResponse);
ctx_macro_gen!(create_status, CREATED);
ctx_macro_gen!(update_status, OK);
ctx_macro_gen!(delete_status, OK);

raw_macro_gen!(auth_status_raw, OK);
// raw_macro_gen!(get_status_raw, OK);
