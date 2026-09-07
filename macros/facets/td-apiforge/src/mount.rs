//
//  Copyright 2026 Tabsdata Inc.
//

//! The `mount = <class>` argument of `#[apiserver_path(...)]`: the router the
//! route registers itself into.

use darling::FromMeta;
use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::Expr;

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

    fn from_expr(expr: &Expr) -> darling::Result<Self> {
        match expr {
            Expr::Path(path) => path
                .path
                .get_ident()
                .ok_or_else(|| darling::Error::unexpected_expr_type(expr))
                .and_then(|ident| Self::from_string(&ident.to_string())),
            Expr::Lit(lit) => Self::from_value(&lit.lit),
            other => Err(darling::Error::unexpected_expr_type(other)),
        }
        .map_err(|e| e.with_span(expr))
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
