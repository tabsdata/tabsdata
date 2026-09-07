//
//  Copyright 2026 Tabsdata Inc.
//

//! Identifier-valued `#[apiserver_path(...)]` arguments, spelled `key(ident)`.
//!
//! The `key = ident` spelling is not a valid attribute meta item — the
//! right-hand side of `=` must be a literal — and rustfmt re-emits such an
//! attribute verbatim instead of formatting it.

use darling::FromMeta;
use darling::ast::NestedMeta;
use proc_macro2::TokenStream;
use quote::ToTokens;
use syn::{Expr, Ident, Meta};

pub struct Word(pub Ident);

impl Word {
    pub fn spelling_error(expr: &Expr) -> darling::Error {
        darling::Error::custom("identifier arguments are spelled `key(ident)`, not `key = ident`")
            .with_span(expr)
    }
}

impl FromMeta for Word {
    fn from_list(items: &[NestedMeta]) -> darling::Result<Self> {
        match items {
            [NestedMeta::Meta(Meta::Path(path))] => {
                path.get_ident().cloned().map(Word).ok_or_else(|| {
                    darling::Error::custom("expected a single identifier").with_span(path)
                })
            }
            [item] => Err(darling::Error::custom("expected a single identifier").with_span(item)),
            _ => Err(darling::Error::custom(
                "expected a single identifier in parentheses",
            )),
        }
    }

    fn from_expr(expr: &Expr) -> darling::Result<Self> {
        Err(Self::spelling_error(expr))
    }
}

impl ToTokens for Word {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        self.0.to_tokens(tokens);
    }
}
