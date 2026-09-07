//
//  Copyright 2024 Tabsdata Inc.
//

use darling::FromMeta;
use quote::ToTokens;
use std::any::Any;

/// A macro to parse meta attributes using the `darling` crate.
///
/// This macro takes a struct that implements `darling::FromMeta` and a `TokenStream`
/// of attribute arguments, and returns an instance of the struct populated with the
/// parsed attribute values. If parsing fails, it returns a `TokenStream` containing
/// the error messages.
///
/// # Arguments
///
/// * `$meta_struct` - The struct type that implements `darling::FromMeta`.
/// * `$args` - The `TokenStream` of attribute arguments to be parsed.
#[macro_export]
macro_rules! parse_meta {
    ($meta_struct:ident, $args:expr) => {
        || -> Result<$meta_struct, proc_macro::TokenStream> {
            let attr_args = match darling::ast::NestedMeta::parse_meta_list($args.into()) {
                Ok(v) => Ok(v),
                Err(e) => Err(proc_macro::TokenStream::from(
                    darling::Error::from(e).write_errors(),
                )),
            }?;

            let args = match $meta_struct::from_list(&attr_args) {
                Ok(v) => Ok(v),
                Err(e) => Err(proc_macro::TokenStream::from(e.write_errors())),
            }?;

            Ok(args)
        }()
    };
}

/// A helper struct to parse a `syn::Meta` into a `SynMetaOrLit`. It allows for fn calls,
/// literals, constants, etc.
#[derive(Debug, Clone)]
pub enum SynMetaOrLit {
    Lit(syn::Lit),
    Path(syn::Path),
    ExprConst(syn::ExprConst),
    ExprPath(syn::ExprPath),
    ExprCall(syn::ExprCall),
    ExprClosure(syn::ExprClosure),
}

impl ToTokens for SynMetaOrLit {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        match self {
            SynMetaOrLit::Lit(l) => l.to_tokens(tokens),
            SynMetaOrLit::Path(p) => p.to_tokens(tokens),
            SynMetaOrLit::ExprConst(e) => e.to_tokens(tokens),
            SynMetaOrLit::ExprPath(e) => e.to_tokens(tokens),
            SynMetaOrLit::ExprCall(e) => e.to_tokens(tokens),
            SynMetaOrLit::ExprClosure(e) => e.to_tokens(tokens),
        }
    }
}

impl SynMetaOrLit {
    pub fn value(&self) -> Box<dyn Any> {
        match self {
            SynMetaOrLit::Lit(lit) => match lit {
                syn::Lit::Str(lit_str) => Box::new(lit_str.value()),
                syn::Lit::Int(lit_int) => Box::new(lit_int.base10_digits().to_owned()),
                syn::Lit::Float(lit_float) => Box::new(lit_float.base10_digits().to_owned()),
                syn::Lit::Bool(lit_bool) => Box::new(lit_bool.value()),
                syn::Lit::Char(lit_char) => Box::new(lit_char.value()),
                syn::Lit::Byte(lit_byte) => Box::new(lit_byte.value()),
                syn::Lit::ByteStr(lit_byte_str) => Box::new(lit_byte_str.value()),
                _ => panic!("Unexpected literal type"),
            },
            SynMetaOrLit::Path(path) => Box::new(path.to_token_stream().to_string()),
            SynMetaOrLit::ExprConst(expr_const) => {
                Box::new(expr_const.to_token_stream().to_string())
            }
            SynMetaOrLit::ExprPath(expr_path) => Box::new(expr_path.to_token_stream().to_string()),
            SynMetaOrLit::ExprCall(expr_call) => Box::new(expr_call.to_token_stream().to_string()),
            SynMetaOrLit::ExprClosure(expr_call) => {
                Box::new(expr_call.to_token_stream().to_string())
            }
        }
    }
}

impl FromMeta for SynMetaOrLit {
    fn from_meta(meta: &syn::Meta) -> darling::Result<Self> {
        match meta {
            syn::Meta::NameValue(syn::MetaNameValue { value, .. }) => match value {
                syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(lit_str),
                    ..
                }) => Ok(SynMetaOrLit::Lit(lit_str.clone().into())),
                syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Int(lit_int),
                    ..
                }) => Ok(SynMetaOrLit::Lit(lit_int.clone().into())),
                syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Float(lit_float),
                    ..
                }) => Ok(SynMetaOrLit::Lit(lit_float.clone().into())),
                syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Bool(lit_bool),
                    ..
                }) => Ok(SynMetaOrLit::Lit(lit_bool.clone().into())),
                syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Char(lit_char),
                    ..
                }) => Ok(SynMetaOrLit::Lit(lit_char.clone().into())),
                syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Byte(lit_byte),
                    ..
                }) => Ok(SynMetaOrLit::Lit(lit_byte.clone().into())),
                syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::ByteStr(lit_byte_str),
                    ..
                }) => Ok(SynMetaOrLit::Lit(lit_byte_str.clone().into())),
                syn::Expr::Path(expr_path) => Ok(SynMetaOrLit::ExprPath(expr_path.clone())),
                syn::Expr::Call(call) => Ok(SynMetaOrLit::ExprCall(call.clone())),
                syn::Expr::Closure(closure) => Ok(SynMetaOrLit::ExprClosure(closure.clone())),
                syn::Expr::Const(constant) => Ok(SynMetaOrLit::ExprConst(constant.clone())),
                _ => Err(darling::Error::custom(
                    "Failed parsing syn::Expr as darling meta",
                )),
            },
            syn::Meta::Path(path) => Ok(SynMetaOrLit::Path(path.clone())),
            syn::Meta::List(list) => Ok(SynMetaOrLit::Path(list.path.clone())),
        }
    }
}
