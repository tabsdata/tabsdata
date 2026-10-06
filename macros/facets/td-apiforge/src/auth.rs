//
//  Copyright 2026 Tabsdata Inc.
//

//! The `auth(<credential>, ...)` argument of `#[apiserver_path(...)]`: the
//! credentials a route accepts, independent of its placement.
//! `none` and `internal` stand alone; the bearer kinds combine.

use darling::FromMeta;
use darling::ast::NestedMeta;
use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::{Expr, Meta};

use crate::word::Word;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Auth {
    None,
    Internal,
    Session,
    ServiceToken,
}

impl Auth {
    pub const NAMES: &'static [&'static str] = &["none", "internal", "session", "service_token"];

    fn from_name(name: &str) -> Option<Self> {
        match name {
            "none" => Some(Auth::None),
            "internal" => Some(Auth::Internal),
            "session" => Some(Auth::Session),
            "service_token" => Some(Auth::ServiceToken),
            _ => None,
        }
    }

    fn standalone(self) -> bool {
        matches!(self, Auth::None | Auth::Internal)
    }
}

pub struct AuthSet(pub Vec<Auth>);

impl FromMeta for AuthSet {
    fn from_list(items: &[NestedMeta]) -> darling::Result<Self> {
        if items.is_empty() {
            return Err(darling::Error::custom(format!(
                "auth needs at least one credential; expected some of {}",
                Auth::NAMES.join(", ")
            )));
        }
        let mut accepted = Vec::with_capacity(items.len());
        for item in items {
            let path = match item {
                NestedMeta::Meta(Meta::Path(path)) => path,
                other => {
                    return Err(
                        darling::Error::custom("auth credentials are bare identifiers")
                            .with_span(other),
                    );
                }
            };
            let ident = path.get_ident().ok_or_else(|| {
                darling::Error::custom("auth credentials are bare identifiers").with_span(path)
            })?;
            let auth = Auth::from_name(&ident.to_string()).ok_or_else(|| {
                darling::Error::custom(format!(
                    "unknown credential `{ident}`; expected one of {}",
                    Auth::NAMES.join(", ")
                ))
                .with_span(ident)
            })?;
            if accepted.contains(&auth) {
                return Err(
                    darling::Error::custom(format!("`{ident}` is listed twice")).with_span(ident)
                );
            }
            accepted.push(auth);
        }
        if accepted.len() > 1 && accepted.iter().any(|auth| auth.standalone()) {
            return Err(darling::Error::custom(
                "`none` and `internal` stand alone; they cannot be combined with other credentials",
            ));
        }
        Ok(AuthSet(accepted))
    }

    fn from_expr(expr: &Expr) -> darling::Result<Self> {
        Err(Word::spelling_error(expr))
    }
}

impl ToTokens for AuthSet {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let variants = self.0.iter().map(|auth| match auth {
            Auth::None => quote!(::ta_apiserver::router::Auth::None),
            Auth::Internal => quote!(::ta_apiserver::router::Auth::Internal),
            Auth::Session => quote!(::ta_apiserver::router::Auth::Session),
            Auth::ServiceToken => quote!(::ta_apiserver::router::Auth::ServiceToken),
        });
        tokens.extend(quote!(&[#(#variants),*]));
    }
}
