//
//  Copyright 2026 Tabsdata Inc.
//

//! The `mount(<class>)` argument of `#[apiserver_path(...)]`: the router the
//! route registers itself into.

use darling::FromMeta;
use darling::ast::NestedMeta;
use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::Expr;

use crate::word::Word;

#[derive(Debug, Clone, Copy)]
pub enum Mount {
    Unauthenticated,
    Global,
    Rbac,
    Simple,
    Project,
    Internal,
    Root,
}

impl Mount {
    const NAMES: &'static [&'static str] = &[
        "unauthenticated",
        "global",
        "rbac",
        "simple",
        "project",
        "internal",
        "root",
    ];
}

impl FromMeta for Mount {
    fn from_string(value: &str) -> darling::Result<Self> {
        match value {
            "unauthenticated" => Ok(Mount::Unauthenticated),
            "global" => Ok(Mount::Global),
            "rbac" => Ok(Mount::Rbac),
            "simple" => Ok(Mount::Simple),
            "project" => Ok(Mount::Project),
            "internal" => Ok(Mount::Internal),
            "root" => Ok(Mount::Root),
            other => Err(darling::Error::custom(format!(
                "unknown mount `{other}`; expected one of {}",
                Mount::NAMES.join(", ")
            ))),
        }
    }

    fn from_list(items: &[NestedMeta]) -> darling::Result<Self> {
        let Word(ident) = Word::from_list(items)?;
        Self::from_string(&ident.to_string()).map_err(|e| e.with_span(&ident))
    }

    fn from_expr(expr: &Expr) -> darling::Result<Self> {
        Err(Word::spelling_error(expr))
    }
}

impl ToTokens for Mount {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let variant = match self {
            Mount::Unauthenticated => quote!(Unauthenticated),
            Mount::Global => quote!(Global),
            Mount::Rbac => quote!(Rbac),
            Mount::Simple => quote!(Simple),
            Mount::Project => quote!(Project),
            Mount::Internal => quote!(Internal),
            Mount::Root => quote!(Root),
        };
        tokens.extend(variant);
    }
}
