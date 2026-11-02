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

macro_rules! word_enum {
    ($name:ident, $key:literal, { $($spelling:literal => $variant:ident),+ $(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            const NAMES: &'static [&'static str] = &[$($spelling),+];

            fn from_name(name: &str) -> Option<Self> {
                match name {
                    $($spelling => Some($name::$variant),)+
                    _ => None,
                }
            }
        }

        impl ::darling::FromMeta for $name {
            fn from_list(items: &[::darling::ast::NestedMeta]) -> darling::Result<Self> {
                let $crate::word::Word(ident) = <$crate::word::Word as ::darling::FromMeta>::from_list(items)?;
                Self::from_name(&ident.to_string()).ok_or_else(|| {
                    darling::Error::custom(format!(
                        "unknown {} `{ident}`; expected one of {}",
                        $key,
                        Self::NAMES.join(", ")
                    ))
                    .with_span(&ident)
                })
            }

            fn from_expr(expr: &::syn::Expr) -> darling::Result<Self> {
                Err($crate::word::Word::spelling_error(expr))
            }
        }

        impl ::quote::ToTokens for $name {
            fn to_tokens(&self, tokens: &mut ::proc_macro2::TokenStream) {
                tokens.extend(match self {
                    $($name::$variant => ::quote::quote!(::ta_apiserver::router::$name::$variant),)+
                });
            }
        }
    };
}

pub(crate) use word_enum;
