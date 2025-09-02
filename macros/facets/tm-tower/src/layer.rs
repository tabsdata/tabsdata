//
// Copyright 2025 Tabs Data Inc.
//

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, parse_quote, ItemFn, ReturnType};

pub fn layer(_args: TokenStream, item: TokenStream) -> TokenStream {
    let mut func = parse_macro_input!(item as ItemFn);

    // Inject `In` into generics
    func.sig.generics.params.insert(0, parse_quote! { In });

    // Inject In's bounds into the where clause
    let where_clause = func.sig.generics.make_where_clause();
    where_clause.predicates.push(parse_quote! {
        In: tower::Service<
            td_tower::handler::Handler,
            Response = td_tower::handler::Handler,
            Error = td_error::TdError,
            Future: Send
        > + Clone + Send + Sync + 'static
    });

    // Override return type
    func.sig.output = ReturnType::Type(
        syn::token::RArrow::default(),
        Box::new(parse_quote! {
            tower::util::BoxCloneSyncServiceLayer<
                In,
                td_tower::handler::Handler,
                td_tower::handler::Handler,
                td_error::TdError
            >
        }),
    );

    TokenStream::from(quote! { #func })
}
