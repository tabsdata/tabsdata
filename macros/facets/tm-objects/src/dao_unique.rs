//
// Copyright 2026 Tabsdata Inc.
//

//! One addressing impl per unique set, for every argument order, every
//! by-value/by-reference combination of the set's own positions, and with
//! narrowing conditions allowed in the trailing positions. The
//! And-composition rule cannot be a blanket impl in `ta-objects`: it would
//! overlap a composite key's `And2<A, B>` impl, since coherence does not read
//! where-clauses.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::Type;

const MAX_ARITY: usize = 4;

#[derive(Clone, Copy)]
pub enum Under {
    NoScope,
    AnyScope,
    InProject,
}

pub fn addresses(
    ident: &syn::Ident,
    sets: &[Vec<Type>],
    addressing: &syn::Ident,
    under: Under,
) -> TokenStream {
    let (scope_param, scope_arg) = match under {
        Under::NoScope => (quote! {}, quote! {}),
        Under::AnyScope => (
            quote! { __S: td_database::access::DbScope, },
            quote! { , __S },
        ),
        Under::InProject => (quote! {}, quote! { , td_database::access::InProject }),
    };
    let mut out = Vec::new();
    for set in sets {
        for order in permutations(set) {
            for refs in 0..(1u32 << order.len()) {
                let cols: Vec<TokenStream> = order
                    .iter()
                    .enumerate()
                    .map(|(i, t)| {
                        if refs & (1 << i) == 0 {
                            quote! { #t }
                        } else {
                            quote! { &'__a #t }
                        }
                    })
                    .collect();
                if order.len() == 1 && refs == 0 {
                    let only = &order[0];
                    out.push(quote! {
                        impl<#scope_param> ta_objects::#addressing<#ident #scope_arg> for #only {}
                    });
                }
                for extra in 0..=(MAX_ARITY - order.len()) {
                    let arity = order.len() + extra;
                    let extras: Vec<syn::Ident> =
                        (0..extra).map(|i| format_ident!("__X{}", i)).collect();
                    let and = format_ident!("And{}", arity);
                    let lt = if refs == 0 {
                        quote! {}
                    } else {
                        quote! { '__a, }
                    };
                    out.push(quote! {
                        impl <#lt #scope_param #(#extras: ta_objects::sql_condition::SqlCondition<#ident>,)*>
                            ta_objects::#addressing<#ident #scope_arg>
                            for ta_objects::sql_condition::#and<#(#cols,)* #(#extras,)*> {}
                    });
                }
            }
        }
    }
    quote! { #(#out)* }
}

fn permutations(set: &[Type]) -> Vec<Vec<Type>> {
    if set.len() > MAX_ARITY {
        return vec![];
    }
    match set {
        [] => vec![],
        [one] => vec![vec![one.clone()]],
        _ => set
            .iter()
            .enumerate()
            .flat_map(|(i, head)| {
                let rest: Vec<Type> = set
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| *j != i)
                    .map(|(_, t)| t.clone())
                    .collect();
                permutations(&rest)
                    .into_iter()
                    .map(|tail| {
                        std::iter::once(head.clone())
                            .chain(tail)
                            .collect::<Vec<Type>>()
                    })
                    .collect::<Vec<_>>()
            })
            .collect(),
    }
}
