//
// Copyright 2025 Tabsdata Inc.
//

use darling::FromMeta;
use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::{ToTokens, format_ident, quote};
use std::any::type_name;
use std::marker::PhantomData;
use std::str::FromStr;
use syn::{ItemEnum, ItemStruct, Type, parse_macro_input};
use td_shared::meta_parser::{OptionWrapper, SynMetaOrLit, some_or_none};
use td_shared::{downcast_option, parse_meta};

// ---------------------------------------------------------------------------
// Top-level entry point
// ---------------------------------------------------------------------------

#[derive(Debug, FromMeta)]
struct FromTyped {
    #[darling(multiple)]
    try_from: Vec<Ident>,
    #[darling(flatten)]
    typed: Typed,
}

#[derive(Debug, FromMeta)]
#[allow(non_camel_case_types)]
enum Typed {
    #[darling(rename = "string")]
    String(Box<OptionWrapper<TypedString>>),
    #[darling(rename = "i16")]
    i16(OptionWrapper<TypedNumeric<i16>>),
    #[darling(rename = "i32")]
    i32(OptionWrapper<TypedNumeric<i32>>),
    #[darling(rename = "i64")]
    i64(OptionWrapper<TypedNumeric<i64>>),
    #[darling(rename = "f32")]
    f32(OptionWrapper<TypedNumeric<f32>>),
    #[darling(rename = "f64")]
    f64(OptionWrapper<TypedNumeric<f64>>),
    #[darling(rename = "bool")]
    bool(Box<OptionWrapper<TypedBool>>),
    #[darling(rename = "id")]
    Id(OptionWrapper<TypedId>),
    #[darling(rename = "idlogic")]
    IdL(TypedIdL),
    #[darling(rename = "timestamp")]
    Timestamp(OptionWrapper<TypedTimestamp>),
    #[darling(rename = "composed")]
    Composed(ComposedTyped),
}

pub fn typed_basic(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_meta!(FromTyped, args).unwrap();
    let input = parse_macro_input!(item as ItemStruct);

    if !input.fields.is_empty() {
        return syn::Error::new_spanned(&input.ident, "Typed structs must have no fields")
            .to_compile_error()
            .into();
    }

    let name = &input.ident;

    let body = match args.typed {
        Typed::String(t) => typed_string(&input, (*t).into()),
        Typed::i16(t) => typed_int(&input, t.into()),
        Typed::i32(t) => typed_int(&input, t.into()),
        Typed::i64(t) => typed_int(&input, t.into()),
        Typed::f32(t) => typed_float(&input, t.into()),
        Typed::f64(t) => typed_float(&input, t.into()),
        Typed::bool(t) => typed_bool(&input, (*t).into()),
        Typed::Id(t) => typed_id(&input, t.into()),
        Typed::IdL(t) => typed_idlogic(&input, t),
        Typed::Timestamp(t) => typed_timestamp(&input, t.into()),
        Typed::Composed(t) => typed_composed(&input, t),
    };

    // TryFrom sibling newtypes sharing the same inner type.
    // NOTE: assumes inner value is always at field `.0` — true for all generated types.
    // NOTE: calls Self::parse(&val.to_string()) — only works for string-backed types. Numeric/bool
    //       siblings should convert via TryFrom<primitive> instead.
    let try_froms = args.try_from.iter().map(|from| {
        quote! {
            impl TryFrom<#from> for #name {
                type Error = td_error::TdError;
                #[allow(clippy::needless_question_mark)]
                fn try_from(val: #from) -> Result<Self, Self::Error> { Ok(Self::parse(val.0)?) }
            }
            impl TryFrom<&#from> for #name {
                type Error = td_error::TdError;
                #[allow(clippy::needless_question_mark)]
                fn try_from(val: &#from) -> Result<Self, Self::Error> { Ok(Self::parse(val.0.clone())?) }
            }
        }
    });

    quote! { #body #(#try_froms)* }.into()
}

// ---------------------------------------------------------------------------
// Shared codegen helpers
// ---------------------------------------------------------------------------

/// Emits a compact `Debug` impl that delegates to the inner value's Debug.
/// Unlike `#[derive(Debug)]`, this does NOT expand to multi-line with `{:#?}`.
fn impl_compact_debug(name: &Ident) -> proc_macro2::TokenStream {
    quote! {
        impl std::fmt::Debug for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Debug::fmt(&self.0, f)
            }
        }
    }
}

/// Emits a redacted `Debug` impl that hides the inner value.
fn impl_redacted_debug(name: &Ident) -> proc_macro2::TokenStream {
    quote! {
        impl std::fmt::Debug for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "***")
            }
        }
    }
}

/// Emits SeaValue + NullableSeaValue together so the Some/None variants always stay in sync.
fn impl_sea_value(
    name: &Ident,
    some_expr: proc_macro2::TokenStream,
    none_expr: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    quote! {
        impl ta_objects::SeaValue for #name {
            fn as_sea_value(&self) -> sea_query::Value { #some_expr }
            fn as_type_id(&self) -> std::any::TypeId { std::any::TypeId::of::<Self>() }
        }
        impl ta_objects::NullableSeaValue for #name {
            fn null_sea_value() -> sea_query::Value { #none_expr }
        }
    }
}

/// Emits `SqlCondition` for a leaf type (single AND with one column).
fn impl_sql_condition(name: &Ident) -> proc_macro2::TokenStream {
    quote! {
        impl ta_objects::sql_condition::SqlCondition for #name {
            fn resolve<D: ta_objects::DaoCore>(&self) -> Result<sea_query::Condition, ta_objects::errors::SqlError> {
                ta_objects::sql_condition::resolve_leaf_eq::<D>(self)
            }

            fn display_values<D: ta_objects::DaoCore>(&self) -> Vec<(String, String)> {
                ta_objects::sql_condition::leaf_display_values::<D>(self)
            }
        }
    }
}

/// Returns the `(Some(…), None)` `sea_query::Value` pair for a numeric primitive ident.
fn numeric_sea_variants(
    ty: &Ident,
) -> syn::Result<(proc_macro2::TokenStream, proc_macro2::TokenStream)> {
    match ty.to_string().as_str() {
        "i16" => Ok((
            quote! { sea_query::Value::SmallInt(Some(self.0)) },
            quote! { sea_query::Value::SmallInt(None) },
        )),
        "i32" => Ok((
            quote! { sea_query::Value::Int(Some(self.0))      },
            quote! { sea_query::Value::Int(None)      },
        )),
        "i64" => Ok((
            quote! { sea_query::Value::BigInt(Some(self.0))   },
            quote! { sea_query::Value::BigInt(None)   },
        )),
        "f32" => Ok((
            quote! { sea_query::Value::Float(Some(self.0))    },
            quote! { sea_query::Value::Float(None)    },
        )),
        "f64" => Ok((
            quote! { sea_query::Value::Double(Some(self.0))   },
            quote! { sea_query::Value::Double(None)   },
        )),
        _ => Err(syn::Error::new_spanned(
            ty,
            format!("Unsupported numeric type for SeaValue: {ty}"),
        )),
    }
}

/// Emits FromStr + SeaValue + NullableSeaValue + SqlCondition for a string-backed newtype.
fn impl_typed_value_string(name: &Ident) -> proc_macro2::TokenStream {
    let sea_value = impl_sea_value(
        name,
        quote! { sea_query::Value::String(Some(self.0.clone())) },
        quote! { sea_query::Value::String(None) },
    );
    let sql_condition = impl_sql_condition(name);
    quote! {
        impl std::str::FromStr for #name {
            type Err = td_error::TdError;
            fn from_str(s: &str) -> Result<Self, Self::Err> { Self::parse(s).map_err(Into::into) }
        }
        #sea_value
        #sql_condition
    }
}

/// Emits FromStr + SeaValue + NullableSeaValue + SqlCondition for a numeric newtype.
fn impl_typed_value_numeric(
    name: &Ident,
    ty: &Ident,
    err: &Ident,
) -> syn::Result<proc_macro2::TokenStream> {
    let (some_expr, none_expr) = numeric_sea_variants(ty)?;
    let sea_value = impl_sea_value(name, some_expr, none_expr);
    let sql_condition = impl_sql_condition(name);
    Ok(quote! {
        impl std::str::FromStr for #name {
            type Err = td_error::TdError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let v: #ty = s.parse().map_err(#err::Parse)?;
                Self::parse(v).map_err(Into::into)
            }
        }
        #sea_value
        #sql_condition
    })
}

/// Emits FromStr + SeaValue + NullableSeaValue + SqlCondition for a bool newtype.
fn impl_typed_value_bool(name: &Ident, err: &Ident) -> proc_macro2::TokenStream {
    let sea_value = impl_sea_value(
        name,
        quote! { sea_query::Value::Bool(Some(self.0)) },
        quote! { sea_query::Value::Bool(None) },
    );
    let sql_condition = impl_sql_condition(name);
    quote! {
        impl std::str::FromStr for #name {
            type Err = td_error::TdError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                s.parse::<bool>().map(Self).map_err(|e| #err::Parse(e).into())
            }
        }
        #sea_value
        #sql_condition
    }
}

/// Emits FromStr + SeaValue + NullableSeaValue + SqlCondition for an Id newtype.
fn impl_typed_value_id(name: &Ident) -> proc_macro2::TokenStream {
    let sea_value = impl_sea_value(
        name,
        quote! { sea_query::Value::String(Some(self.0.to_string())) },
        quote! { sea_query::Value::String(None) },
    );
    let sql_condition = impl_sql_condition(name);
    quote! {
        impl std::str::FromStr for #name {
            type Err = td_error::TdError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(Self(td_common::id::Id::try_from(s)?))
            }
        }
        #sea_value
        #sql_condition
    }
}

/// Emits FromStr + SeaValue + NullableSeaValue + SqlCondition for an IdL newtype.
fn impl_typed_value_idl(name: &Ident) -> proc_macro2::TokenStream {
    let sea_value = impl_sea_value(
        name,
        quote! { sea_query::Value::String(Some(self.0.padded_str())) },
        quote! { sea_query::Value::String(None) },
    );
    let sql_condition = impl_sql_condition(name);
    quote! {
        impl std::str::FromStr for #name {
            type Err = td_error::TdError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(Self(td_common::logic_id::IdL::parse(&s)?))
            }
        }
        #sea_value
        #sql_condition
    }
}

/// Emits FromStr + SeaValue + NullableSeaValue + SqlCondition for a Timestamp newtype.
fn impl_typed_value_timestamp(name: &Ident) -> proc_macro2::TokenStream {
    let sea_value = impl_sea_value(
        name,
        quote! { sea_query::Value::ChronoDateTimeUtc(Some(*self.0)) },
        quote! { sea_query::Value::ChronoDateTimeUtc(None) },
    );
    let sql_condition = impl_sql_condition(name);
    quote! {
        impl std::str::FromStr for #name {
            type Err = td_error::TdError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(#name(s.parse::<td_common::datetime::Timestamp>()?))
            }
        }
        #sea_value
        #sql_condition
    }
}

/// Emits Deref, AsRef, Borrow, Display, Serialize, Deserialize.
fn impl_common_traits(name: &Ident, inner: &proc_macro2::TokenStream) -> proc_macro2::TokenStream {
    quote! {
        // Deref, AsRef, Borrow, auto-clone
        impl std::ops::Deref for #name {
            type Target = #inner;
            fn deref(&self) -> &Self::Target { &self.0 }
        }
        impl AsRef<#inner> for #name {
            fn as_ref(&self) -> &#inner { &self.0 }
        }
        impl std::borrow::Borrow<#inner> for #name {
            fn borrow(&self) -> &#inner { &self.0 }
        }
        impl From<&#name> for #name {
            fn from(v: &#name) -> #name { v.clone() }
        }
        // Display
        impl std::fmt::Display for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
        // ListFilterStr
        impl ta_objects::ListFilterStr for #name {
            fn parse_filter_str(s: &str) -> Result<Self, td_error::TdError> {
                use std::str::FromStr;
                #name::from_str(s).map_err(Into::into)
            }
            fn as_string(&self) -> String {
                self.to_string()
            }
        }
        // Serde
        impl<'de> serde::Deserialize<'de> for #name {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let inner = #inner::deserialize(d)?;
                #name::try_from(inner).map_err(serde::de::Error::custom)
            }
        }
        impl serde::Serialize for #name {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                self.0.serialize(s)
            }
        }
    }
}

/// Emits TryFrom<String>, TryFrom<&String>, TryFrom<&str> via FromStr.
fn impl_try_from_str(name: &Ident) -> proc_macro2::TokenStream {
    quote! {
        impl TryFrom<String> for #name {
            type Error = td_error::TdError;
            fn try_from(v: String) -> Result<Self, Self::Error> { v.parse() }
        }
        impl TryFrom<&String> for #name {
            type Error = td_error::TdError;
            fn try_from(v: &String) -> Result<Self, Self::Error> { v.parse() }
        }
        impl TryFrom<&str> for #name {
            type Error = td_error::TdError;
            fn try_from(v: &str) -> Result<Self, Self::Error> { v.parse() }
        }
    }
}

/// Emits utoipa ComposeSchema + ToSchema as a string type.
fn impl_utoipa_string(name: &Ident) -> proc_macro2::TokenStream {
    quote! {
        impl utoipa::__dev::ComposeSchema for #name {
            fn compose(_: Vec<utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>>)
                -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
                utoipa::openapi::ObjectBuilder::new()
                    .schema_type(utoipa::openapi::schema::SchemaType::new(
                        utoipa::openapi::schema::Type::String))
                    .into()
            }
        }
        impl utoipa::ToSchema for #name {
            fn name() -> std::borrow::Cow<'static, str> {
                std::borrow::Cow::Borrowed(stringify!(#name))
            }
            fn schemas(_: &mut Vec<(String, utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>)>) {}
        }
    }
}

/// Emits utoipa ComposeSchema + ToSchema as an int64 type.
fn impl_utoipa_int64(name: &Ident) -> proc_macro2::TokenStream {
    quote! {
        impl utoipa::__dev::ComposeSchema for #name {
            fn compose(_: Vec<utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>>)
                -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
                utoipa::openapi::ObjectBuilder::new()
                    .schema_type(utoipa::openapi::schema::SchemaType::new(
                        utoipa::openapi::schema::Type::Integer))
                    .format(Some(utoipa::openapi::schema::SchemaFormat::KnownFormat(
                        utoipa::openapi::schema::KnownFormat::Int64)))
                    .into()
            }
        }
        impl utoipa::ToSchema for #name {
            fn name() -> std::borrow::Cow<'static, str> {
                std::borrow::Cow::Borrowed(stringify!(#name))
            }
            fn schemas(_: &mut Vec<(String, utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>)>) {}
        }
    }
}

// ---------------------------------------------------------------------------
// String
// ---------------------------------------------------------------------------

#[derive(Debug, Default, FromMeta)]
pub struct TypedString {
    #[darling(default)]
    secret: bool,
    default: Option<SynMetaOrLit>,
    len: Option<SynMetaOrLit>,
    min_len: Option<SynMetaOrLit>,
    max_len: Option<SynMetaOrLit>,
    regex: Option<SynMetaOrLit>,
    parser: Option<SynMetaOrLit>,
}

pub fn typed_string(input: &ItemStruct, typed: Option<TypedString>) -> proc_macro2::TokenStream {
    let attrs = &input.attrs;
    let name = &input.ident;
    let err = format_ident!("{}Error", name);

    let secret = typed.as_ref().is_some_and(|t| t.secret);

    let (default, len, min_len, max_len, regex, parser) = match typed {
        None => (None, None, None, None, None, None),
        Some(t) => {
            let len = downcast_option!(t.len, usize);
            let min_len = downcast_option!(t.min_len, usize);
            let max_len = downcast_option!(t.max_len, usize);

            if let (Some(l), Some(mn)) = (len, min_len)
                && l < mn
            {
                return syn::Error::new_spanned(name, "len must be >= min_len").to_compile_error();
            }
            if let (Some(l), Some(mx)) = (len, max_len)
                && l > mx
            {
                return syn::Error::new_spanned(name, "len must be <= max_len").to_compile_error();
            }
            if let (Some(mn), Some(mx)) = (min_len, max_len)
                && mn > mx
            {
                return syn::Error::new_spanned(name, "min_len must be <= max_len")
                    .to_compile_error();
            }

            if let Some(ref d) = downcast_option!(t.default, String) {
                if let Some(l) = len
                    && d.len() != l
                {
                    return syn::Error::new_spanned(name, "default length must equal len")
                        .to_compile_error();
                }
                if let Some(mn) = min_len
                    && d.len() < mn
                {
                    return syn::Error::new_spanned(name, "default length must be >= min_len")
                        .to_compile_error();
                }
                if let Some(mx) = max_len
                    && d.len() > mx
                {
                    return syn::Error::new_spanned(name, "default length must be <= max_len")
                        .to_compile_error();
                }
            }

            (t.default, t.len, t.min_len, t.max_len, t.regex, t.parser)
        }
    };

    let default_impl = match default {
        Some(d) => quote! { impl Default for #name { fn default() -> Self { Self(#d.into()) } } },
        None => quote! {},
    };

    let len_tok = some_or_none(len);
    let min_len_tok = some_or_none(min_len);
    let max_len_tok = some_or_none(max_len);
    let regex_tok = match regex {
        Some(r) => quote! {
            static RE: std::sync::LazyLock<Option<regex::Regex>> =
                std::sync::LazyLock::new(|| Some(regex::Regex::new(&#r).unwrap()));
            RE.clone()
        },
        None => quote! { None },
    };
    let parser_tok = match parser {
        Some(p) => quote! { Some(Box::new(#p)) },
        None => quote! { None },
    };

    let typed_value = impl_typed_value_string(name);
    let common = impl_common_traits(name, &quote! { String });
    let try_from = impl_try_from_str(name);

    let debug_impl = if secret {
        impl_redacted_debug(name)
    } else {
        impl_compact_debug(name)
    };

    quote! {
        #(#attrs)*
        #[derive(Clone, sqlx::Type, utoipa::ToSchema)]
        #[sqlx(transparent)]
        pub struct #name(String);

        #debug_impl

        #[td_error::td_error]
        pub enum #err {
            #[error("'{0}' must have length {1}")]        Len(String, usize),
            #[error("'{0}' shorter than min_len {1}")]    MinLen(String, usize),
            #[error("'{0}' longer than max_len {1}")]     MaxLen(String, usize),
            #[error("'{0}' does not match regex '{1}'")]  Regex(String, String),
            #[error("Parse error: {0}")]                  Parse(#[source] td_error::TdError),
        }

        #default_impl

        impl #name {
            fn len_constraint() -> Option<usize> { #len_tok }
            fn min_len()        -> Option<usize> { #min_len_tok }
            fn max_len()        -> Option<usize> { #max_len_tok }
            fn regex()          -> Option<regex::Regex> { #regex_tok }
            fn custom_parser()  -> Option<Box<dyn Fn(String) -> Result<String, td_error::TdError>>> {
                #parser_tok
            }

            fn parse(val: impl Into<String>) -> Result<Self, #err> {
                let val = val.into();
                match (Self::len_constraint(), Self::min_len(), Self::max_len(),
                       Self::regex(), Self::custom_parser()) {
                    (Some(l), ..)              if val.len() != l     => Err(#err::Len(val, l))?,
                    (_, Some(mn), ..)          if val.len() < mn     => Err(#err::MinLen(val, mn))?,
                    (_, _, Some(mx), ..)       if val.len() > mx     => Err(#err::MaxLen(val, mx))?,
                    (_, _, _, Some(re), _)     if !re.is_match(&val)  => Err(#err::Regex(val, re.to_string()))?,
                    (_, _, _, _, Some(parser)) => Ok(Self(parser(val).map_err(#err::Parse)?)),
                    _                          => Ok(Self(val)),
                }
            }
        }

        #typed_value
        #common
        #try_from
    }
}

// ---------------------------------------------------------------------------
// Numeric (int / float)
// ---------------------------------------------------------------------------

#[derive(Debug, Default, FromMeta)]
pub struct TypedNumeric<T> {
    default: Option<SynMetaOrLit>,
    min: Option<SynMetaOrLit>,
    max: Option<SynMetaOrLit>,
    #[darling(skip)]
    _phantom: PhantomData<T>,
}

pub fn typed_int<T: FromStr + ToTokens + PartialOrd>(
    input: &ItemStruct,
    typed: Option<TypedNumeric<T>>,
) -> proc_macro2::TokenStream {
    let name = &input.ident;
    let body = typed_numeric(input, typed, quote! { std::num::ParseIntError });
    let debug_impl = impl_compact_debug(name);
    quote! {
        #[derive(Clone, Copy, sqlx::Type, utoipa::ToSchema)]
        #[sqlx(transparent)]
        #body
        #debug_impl
    }
}

pub fn typed_float<T: FromStr + ToTokens + PartialOrd>(
    input: &ItemStruct,
    typed: Option<TypedNumeric<T>>,
) -> proc_macro2::TokenStream {
    let name = &input.ident;
    let body = typed_numeric(input, typed, quote! { std::num::ParseFloatError });
    let debug_impl = impl_compact_debug(name);
    quote! {
        #[derive(Clone, Copy, sqlx::Type, utoipa::ToSchema)]
        #[sqlx(transparent)]
        #body
        #debug_impl
    }
}

fn typed_numeric<T: FromStr + ToTokens + PartialOrd>(
    input: &ItemStruct,
    typed: Option<TypedNumeric<T>>,
    parse_err: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let ty = format_ident!("{}", type_name::<T>());
    let name = &input.ident;
    let err = format_ident!("{}Error", name);
    let attrs = &input.attrs;

    let (default_tok, min_tok, max_tok) = match typed {
        None => (quote! { #ty::default() }, quote! { None }, quote! { None }),
        Some(t) => {
            let min = downcast_option!(t.min, T);
            let max = downcast_option!(t.max, T);
            if let (Some(mn), Some(mx)) = (&min, &max)
                && mn > mx
            {
                return syn::Error::new_spanned(name, "min must be <= max").to_compile_error();
            }

            let default = downcast_option!(t.default, T);
            if let Some(ref d) = default {
                if let Some(mn) = &min
                    && d < mn
                {
                    return syn::Error::new_spanned(name, "default must be >= min")
                        .to_compile_error();
                }
                if let Some(mx) = &max
                    && d > mx
                {
                    return syn::Error::new_spanned(name, "default must be <= max")
                        .to_compile_error();
                }
            }

            let default_tok = match t.default {
                Some(d) => quote! { #d },
                None => quote! { #ty::default() },
            };
            (default_tok, some_or_none(t.min), some_or_none(t.max))
        }
    };

    let typed_value = match impl_typed_value_numeric(name, &ty, &err) {
        Ok(ts) => ts,
        Err(e) => return e.to_compile_error(),
    };
    let common = impl_common_traits(name, &quote! { #ty });
    let try_from = impl_try_from_str(name);

    quote! {
        #(#attrs)*
        pub struct #name(#ty);

        #[td_error::td_error]
        pub enum #err {
            #[error("'{0}' < min '{1}'")]  Min(#ty, #ty),
            #[error("'{0}' > max '{1}'")]  Max(#ty, #ty),
            #[error("Parse error: {0}")]   Parse(#[source] #parse_err),
        }

        impl Default for #name {
            fn default() -> Self { Self(#default_tok) }
        }

        impl #name {
            fn min() -> Option<#ty> { #min_tok }
            fn max() -> Option<#ty> { #max_tok }

            fn parse(val: impl Into<#ty>) -> Result<Self, #err> {
                let val = val.into();
                match (Self::min(), Self::max()) {
                    (Some(mn), _) if val < mn => Err(#err::Min(val, mn))?,
                    (_, Some(mx)) if val > mx => Err(#err::Max(val, mx))?,
                    _ => Ok(Self(val)),
                }
            }
        }

        #typed_value
        #common

        impl TryFrom<#ty> for #name {
            type Error = td_error::TdError;
            fn try_from(v: #ty) -> Result<Self, Self::Error> { Self::parse(v).map_err(Into::into) }
        }

        #try_from
    }
}

// ---------------------------------------------------------------------------
// Bool
// ---------------------------------------------------------------------------

#[derive(Debug, Default, FromMeta)]
pub struct TypedBool {
    default: Option<SynMetaOrLit>,
}

pub fn typed_bool(input: &ItemStruct, typed: Option<TypedBool>) -> proc_macro2::TokenStream {
    let attrs = &input.attrs;
    let name = &input.ident;
    let err = format_ident!("{}Error", name);

    let default_tok = match typed.and_then(|t| t.default) {
        Some(d) => quote! { #d },
        None => quote! { false },
    };

    let typed_value = impl_typed_value_bool(name, &err);
    let common = impl_common_traits(name, &quote! { bool });
    let debug_impl = impl_compact_debug(name);

    quote! {
        #(#attrs)*
        #[derive(Clone, Copy, sqlx::Type, utoipa::ToSchema)]
        #[sqlx(transparent)]
        pub struct #name(bool);

        #debug_impl

        #[td_error::td_error]
        pub enum #err {
            #[error("Bool parse error: {0}")] Parse(#[source] std::str::ParseBoolError),
        }

        impl Default for #name {
            fn default() -> Self { Self(#default_tok) }
        }

        impl #name {
            pub const TRUE:  Self = Self(true);
            pub const FALSE: Self = Self(false);
        }

        #typed_value
        #common

        impl From<bool> for #name {
            fn from(v: bool) -> Self { Self(v) }
        }
    }
}

// ---------------------------------------------------------------------------
// Id
// ---------------------------------------------------------------------------

#[derive(Debug, Default, FromMeta)]
pub struct TypedId {
    default: Option<SynMetaOrLit>,
}

pub fn typed_id(input: &ItemStruct, typed: Option<TypedId>) -> proc_macro2::TokenStream {
    let attrs = &input.attrs;
    let name = &input.ident;

    let default_tok = match typed.and_then(|t| t.default) {
        Some(d) => d.to_token_stream(),
        None => quote! { td_common::id::Id::default() },
    };

    let typed_value = impl_typed_value_id(name);
    let common = impl_common_traits(name, &quote! { td_common::id::Id });
    let try_from = impl_try_from_str(name);
    let utoipa = impl_utoipa_string(name);
    let debug_impl = impl_compact_debug(name);

    quote! {
        #(#attrs)*
        #[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, sqlx::Type)]
        #[sqlx(transparent)]
        pub struct #name(td_common::id::Id);

        #debug_impl

        impl Default for #name {
            fn default() -> Self { Self(#default_tok) }
        }

        impl #name {
            fn parse(val: impl Into<td_common::id::Id>) -> Result<Self, td_error::TdError> {
                let val = val.into();
                Ok(Self(val))
            }
        }

        #typed_value
        #common
        #try_from
        #utoipa

        impl From<td_common::id::Id>  for #name { fn from(v: td_common::id::Id)  -> Self { Self(v) } }
        impl From<&td_common::id::Id> for #name { fn from(v: &td_common::id::Id) -> Self { Self(*v) } }
    }
}

// ---------------------------------------------------------------------------
// IdLogic
// ---------------------------------------------------------------------------

#[derive(Debug, FromMeta)]
pub struct TypedIdL {
    key: SynMetaOrLit,
}

pub fn typed_idlogic(input: &ItemStruct, typed: TypedIdL) -> proc_macro2::TokenStream {
    let attrs = &input.attrs;
    let name = &input.ident;
    let gen_var = format_ident!("{}_GENERATOR", name.to_string().to_uppercase());
    let key = &typed.key;
    // The logical ID prefix is always lowercase, regardless of the key's struct-tag casing.
    let key_str = quote!(#key).to_string().to_ascii_lowercase();
    let uninit_msg = format!(
        "{name} generator not initialised — call {name}::init() at startup before generating IDs"
    );

    let typed_value = impl_typed_value_idl(name);
    let common = impl_common_traits(name, &quote! { td_common::logic_id::IdL::<#key> });
    let try_from = impl_try_from_str(name);
    let utoipa = impl_utoipa_string(name);
    let debug_impl = impl_compact_debug(name);

    quote! {
        #[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct #key;

        impl td_common::logic_id::KeyId for #key {
            fn key() -> &'static str { #key_str }
        }

        #(#attrs)*
        #[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, sqlx::Type)]
        #[sqlx(transparent)]
        pub struct #name(td_common::logic_id::IdL<#key>);

        #debug_impl

        static #gen_var: std::sync::OnceLock<td_common::logic_id::IdLGenerator<#key>> =
            std::sync::OnceLock::new();

        impl #name {
            pub fn init(latest: Option<#name>) -> Result<(), td_error::TdError> {
                let latest = latest.map(|id| id.0);
                let generator = td_common::logic_id::IdLGenerator::<#key>::new(&latest)?;
                #gen_var.set(generator).map_err(|_| td_error::api_error!(
                    td_error::ApiError::InputError,
                    concat!(stringify!(#name), " generator already initialized")
                ))
            }

            pub fn new() -> Self {
                #[cfg(feature = "td-test")]
                let generator = #gen_var.get_or_init(|| {
                    td_common::logic_id::IdLGenerator::<#key>::new(&None).unwrap()
                });
                #[cfg(not(feature = "td-test"))]
                let generator = #gen_var.get().expect(#uninit_msg);
                generator.generate().into()
            }
        }

        #typed_value
        #common
        #try_from
        #utoipa

        impl From<td_common::logic_id::IdL<#key>>  for #name { fn from(v: td_common::logic_id::IdL<#key>)  -> Self { Self(v) } }
        impl From<&td_common::logic_id::IdL<#key>> for #name { fn from(v: &td_common::logic_id::IdL<#key>) -> Self { Self(v.clone()) } }
        impl From<#name> for td_common::logic_id::IdL<#key>  { fn from(v: #name) -> Self { v.0 } }
    }
}

// ---------------------------------------------------------------------------
// Timestamp
// ---------------------------------------------------------------------------

#[derive(Debug, Default, FromMeta)]
pub struct TypedTimestamp {
    default: Option<SynMetaOrLit>,
}

pub fn typed_timestamp(
    input: &ItemStruct,
    typed: Option<TypedTimestamp>,
) -> proc_macro2::TokenStream {
    let attrs = &input.attrs;
    let name = &input.ident;
    let err = format_ident!("{}Error", name);

    let default_tok = match typed.and_then(|t| t.default) {
        Some(d) => d.to_token_stream(),
        None => {
            quote! { td_common::datetime::Timestamp::from(td_common::time::UniqueUtc::now_millis()) }
        }
    };

    let typed_value = impl_typed_value_timestamp(name);
    let common = impl_common_traits(name, &quote! { td_common::datetime::Timestamp });
    let try_from = impl_try_from_str(name);
    let utoipa = impl_utoipa_int64(name);
    let debug_impl = impl_compact_debug(name);

    quote! {
        #(#attrs)*
        #[derive(Clone, Copy, sqlx::Type)]
        #[sqlx(transparent)]
        pub struct #name(td_common::datetime::Timestamp);

        #debug_impl

        #[td_error::td_error]
        pub enum #err {
            #[error("Not an integer, cannot parse as timestamp: '{0}'")]  NotAnInteger(String) = 0,
            #[error("Integer '{0}' is not a valid timestamp")]             NotATimestamp(i64)   = 1,
        }

        impl Default for #name {
            fn default() -> Self { Self(#default_tok) }
        }

        impl #name {
            pub fn now() -> Self {
                Self(td_common::datetime::Timestamp::from(td_common::time::UniqueUtc::now_millis()))
            }

            fn parse(val: impl Into<td_common::datetime::Timestamp>) -> Result<Self, td_error::TdError> {
                let val = val.into();
                Ok(Self(val))
            }
        }

        impl TryFrom<td_common::datetime::Timestamp> for #name {
            type Error = td_error::TdError;
            fn try_from(v: td_common::datetime::Timestamp) -> Result<Self, Self::Error> {
                Self::parse(v).map_err(Into::into)
            }
        }

        impl TryFrom<chrono::DateTime<chrono::Utc>> for #name {
            type Error = td_error::TdError;
            fn try_from(val: chrono::DateTime<chrono::Utc>) -> Result<#name, td_error::TdError> {
                #name::parse(val)
            }
        }

        #typed_value
        #common
        #try_from
        #utoipa
    }
}

// ---------------------------------------------------------------------------
// Composed
// ---------------------------------------------------------------------------

#[derive(Debug, FromMeta)]
pub struct ComposedTyped {
    inner: Type,
}

pub fn typed_composed(input: &ItemStruct, typed: ComposedTyped) -> proc_macro2::TokenStream {
    let attrs = &input.attrs;
    let name = &input.ident;
    let inner = &typed.inner;
    let utoipa = impl_utoipa_string(name);
    let try_from = impl_try_from_str(name);
    let debug_impl = impl_compact_debug(name);

    quote! {
        #(#attrs)*
        #[derive(Clone)]
        pub struct #name(#inner);

        #debug_impl

        #utoipa

        impl #name {
            pub fn new(val: impl Into<#inner>) -> Self { Self(val.into()) }
        }

        impl ta_objects::ComposedString for #name {
            fn parse(s: impl Into<String>) -> Result<Self, td_error::TdError> {
                use ta_objects::ComposedString;
                Ok(Self::new(#inner::parse(s)?))
            }
            fn compose(&self) -> String { self.0.compose() }
        }

        impl std::str::FromStr for #name {
            type Err = td_error::TdError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                ta_objects::ComposedString::parse(s)
            }
        }

        impl From<&#name> for #name {
            fn from(v: &#name) -> #name { v.clone() }
        }

        impl From<#inner> for #name {
            fn from(v: #inner) -> #name { Self::new(v) }
        }

        impl std::fmt::Display for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                use ta_objects::ComposedString;
                f.write_str(&self.compose())
            }
        }

        impl ta_objects::SeaValue for #name {
            fn as_sea_value(&self) -> sea_query::Value {
                use ta_objects::ComposedString;
                sea_query::Value::String(Some(self.compose()))
            }
            fn as_type_id(&self) -> std::any::TypeId { std::any::TypeId::of::<Self>() }
        }
        impl ta_objects::NullableSeaValue for #name {
            fn null_sea_value() -> sea_query::Value { sea_query::Value::String(None) }
        }
        impl ta_objects::sql_condition::SqlCondition for #name {
            fn resolve<D: ta_objects::DaoCore>(&self) -> Result<sea_query::Condition, ta_objects::errors::SqlError> {
                ta_objects::sql_condition::resolve_leaf_eq::<D>(self)
            }

            fn display_values<D: ta_objects::DaoCore>(&self) -> Vec<(String, String)> {
                ta_objects::sql_condition::leaf_display_values::<D>(self)
            }
        }

        impl std::ops::Deref for #name {
            type Target = #inner;
            fn deref(&self) -> &Self::Target { &self.0 }
        }

        impl<'de> serde::Deserialize<'de> for #name {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                use ta_objects::ComposedString;
                String::deserialize(d)?.parse().map_err(serde::de::Error::custom)
            }
        }
        impl serde::Serialize for #name {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                use ta_objects::ComposedString;
                self.compose().serialize(s)
            }
        }

        impl<DB: sqlx::Database> sqlx::Type<DB> for #name
        where
            String: sqlx::Type<DB>,
        {
            fn type_info() -> <DB as sqlx::Database>::TypeInfo {
                <String as sqlx::Type<DB>>::type_info()
            }
        }

        impl<'r, DB: sqlx::Database> sqlx::Decode<'r, DB> for #name
        where
            String: sqlx::Decode<'r, DB>,
        {
            fn decode(
                value: <DB as sqlx::Database>::ValueRef<'r>,
            ) -> Result<Self, Box<dyn std::error::Error + 'static + Send + Sync>> {
                let decoded = <String as sqlx::Decode<'r, DB>>::decode(value)?;
                Ok(Self::try_from(decoded).map_err(Box::new)?)
            }
        }

        #try_from
    }
}

// ---------------------------------------------------------------------------
// Tag enum  (`#[typed_tag]`)
// ---------------------------------------------------------------------------
// Newtype variants, each distinguished by a string prefix tag.
// `Display` = tag + inner;  `FromStr` strips the tag, longer tags tried first.

pub fn typed_tag(args: TokenStream, item: TokenStream) -> TokenStream {
    let _args = parse_meta!(TypedEnum, args).unwrap();
    let mut input = parse_macro_input!(item as ItemEnum);
    let name = &input.ident;
    let err = format_ident!("{}Error", name);
    let outer_attrs = &input.attrs;

    // Collect (ident, tag, inner_ty) before mutating the variant list.
    let variant_data: Vec<(Ident, String, syn::Type)> = {
        let mut data = Vec::new();
        for v in input.variants.iter() {
            let tag = v
                .attrs
                .iter()
                .filter(|a| a.path().is_ident("tag"))
                .find_map(|a| a.parse_args::<syn::LitStr>().ok().map(|s| s.value()))
                .ok_or_else(|| {
                    syn::Error::new_spanned(
                        &v.ident,
                        format!(
                            "typed_tag: variant `{}` must have a `#[tag(\"...\")]` attribute",
                            v.ident
                        ),
                    )
                });
            let tag = match tag {
                Ok(t) => t,
                Err(e) => return e.to_compile_error().into(),
            };
            let ty = match &v.fields {
                syn::Fields::Unnamed(f) if f.unnamed.len() == 1 => {
                    f.unnamed.first().unwrap().ty.clone()
                }
                _ => {
                    return syn::Error::new_spanned(
                        &v.ident,
                        format!(
                            "typed_tag: variant `{}` must be a newtype (single unnamed field)",
                            v.ident
                        ),
                    )
                    .to_compile_error()
                    .into();
                }
            };
            data.push((v.ident.clone(), tag, ty));
        }
        data
    };

    // Strip tag attrs so the emitted enum definition is clean.
    for v in &mut input.variants {
        v.attrs.retain(|a| !a.path().is_ident("tag"));
    }

    // Emit variant declarations directly from (now-stripped) input.variants.
    let variant_decls = input.variants.iter().map(|v| {
        let ident = &v.ident;
        let attrs = &v.attrs;
        let fields = &v.fields;
        quote! { #(#attrs)* #ident #fields }
    });

    // `Tagged` for the inner type of each non-empty-tag variant: lets a caller
    // holding the inner id (e.g. a `ProjectId`) render it in this enum's id
    // spelling (`~<id>`) so `FromStr` parses it back as an id, not a name. The
    // empty-tag variant is skipped — it has no prefix, and its inner type often
    // coincides with the id variant's (which would collide). Two *non-empty*-tag
    // variants over the same inner type would emit conflicting impls (a hard
    // coherence error); no such enum exists today.
    let tagged_impls = variant_data
        .iter()
        .filter(|(_, tag, _)| !tag.is_empty())
        .map(|(_, tag, ty)| {
            quote! {
                impl ta_objects::Tagged<#name> for #ty {
                    const PREFIX: &'static str = #tag;
                }
            }
        });

    // Display: the prefix is sourced from the variant's `Tagged<#name>` impl
    // (single source — Display and `tagged()` can't drift), written straight to
    // the formatter so `fmt` allocates nothing. The empty-tag variant has no
    // `Tagged` impl, so it renders the inner value as-is — exactly as before.
    let display_arms = variant_data.iter().map(|(ident, tag, ty)| {
        if tag.is_empty() {
            quote! { Self::#ident(inner) => write!(f, "{}", inner), }
        } else {
            quote! {
                Self::#ident(inner) =>
                    write!(f, "{}{}", <#ty as ta_objects::Tagged<#name>>::PREFIX, inner),
            }
        }
    });

    // SeaValue: delegate as_sea_value and type_id to the active inner variant.
    let sea_value_arms = variant_data.iter().map(|(ident, _, _)| {
        quote! { Self::#ident(inner) => inner.as_sea_value(), }
    });
    let type_id_arms = variant_data.iter().map(|(ident, _, ty)| {
        quote! { Self::#ident(_) => std::any::TypeId::of::<#ty>(), }
    });

    // FromStr: try longer tags first so the empty-tag variant is always the catch-all.
    let mut sorted: Vec<usize> = (0..variant_data.len()).collect();
    sorted.sort_by(|&a, &b| variant_data[b].1.len().cmp(&variant_data[a].1.len()));

    let non_empty_checks = sorted
        .iter()
        .filter(|&&i| !variant_data[i].1.is_empty())
        .map(|&i| {
            let (ident, tag, ty) = &variant_data[i];
            quote! {
                if let Some(rest) = s.strip_prefix(#tag) {
                    return Ok(Self::#ident(rest.parse::<#ty>()?));
                }
            }
        });

    let has_empty_fallback = variant_data.iter().any(|(_, tag, _)| tag.is_empty());
    let fallback = if let Some(&i) = sorted.iter().find(|&&i| variant_data[i].1.is_empty()) {
        let (ident, _, ty) = &variant_data[i];
        quote! { Ok(Self::#ident(s.parse::<#ty>()?)) }
    } else {
        quote! { Err(#err::NoTagMatch(s.to_owned()).into()) }
    };

    let maybe_err_type = if !has_empty_fallback {
        quote! {
            #[td_error::td_error]
            pub enum #err {
                #[error("Cannot parse '{{0}}': no tag matches")] NoTagMatch(String),
            }
        }
    } else {
        quote! {}
    };

    let utoipa = impl_utoipa_string(name);

    quote! {
        #(#outer_attrs)*
        #[derive(Debug, Clone)]
        pub enum #name {
            #(#variant_decls,)*
        }

        #maybe_err_type

        impl From<&#name> for #name {
            fn from(v: &#name) -> #name { v.clone() }
        }

        impl std::str::FromStr for #name {
            type Err = td_error::TdError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                #(#non_empty_checks)*
                #fallback
            }
        }

        impl std::fmt::Display for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    #(#display_arms)*
                }
            }
        }

        #(#tagged_impls)*

        impl ta_objects::SeaValue for #name {
            fn as_sea_value(&self) -> sea_query::Value {
                match self {
                    #(#sea_value_arms)*
                }
            }
            fn as_type_id(&self) -> std::any::TypeId {
                match self {
                    #(#type_id_arms)*
                }
            }
        }

        impl ta_objects::NullableSeaValue for #name {
            fn null_sea_value() -> sea_query::Value { sea_query::Value::String(None) }
        }

        impl ta_objects::sql_condition::SqlCondition for #name {
            fn resolve<D: ta_objects::DaoCore>(&self) -> Result<sea_query::Condition, ta_objects::errors::SqlError> {
                ta_objects::sql_condition::resolve_leaf_eq::<D>(self)
            }

            fn display_values<D: ta_objects::DaoCore>(&self) -> Vec<(String, String)> {
                ta_objects::sql_condition::leaf_display_values::<D>(self)
            }
        }

        impl ta_objects::ListFilterStr for #name {
            fn parse_filter_str(s: &str) -> Result<Self, td_error::TdError> {
                s.parse().map_err(Into::into)
            }
            fn as_string(&self) -> String { self.to_string() }
        }

        impl TryFrom<String> for #name {
            type Error = td_error::TdError;
            fn try_from(s: String) -> Result<Self, Self::Error> { s.parse() }
        }
        impl From<#name> for String {
            fn from(v: #name) -> String { v.to_string() }
        }

        impl serde::Serialize for #name {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                self.to_string().serialize(s)
            }
        }
        impl<'de> serde::Deserialize<'de> for #name {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                String::deserialize(d)?.parse().map_err(serde::de::Error::custom)
            }
        }

        #utoipa
    }
    .into()
}

// ---------------------------------------------------------------------------
// Enum
// ---------------------------------------------------------------------------

#[derive(Debug, Default, FromMeta)]
pub struct TypedEnum {}

pub fn typed_enum(args: TokenStream, item: TokenStream) -> TokenStream {
    let _args = parse_meta!(TypedEnum, args).unwrap();
    let mut input = parse_macro_input!(item as ItemEnum);
    let name = &input.ident;
    let err = format_ident!("{}Error", name);

    for variant in &mut input.variants {
        let renames: Vec<String> = variant
            .attrs
            .iter()
            .filter(|a| a.path().is_ident("typed_enum"))
            .filter_map(|a| a.parse_args::<syn::MetaNameValue>().ok())
            .filter(|mnv| mnv.path.is_ident("rename"))
            .filter_map(|mnv| {
                if let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(s),
                    ..
                }) = mnv.value
                {
                    Some(s.value())
                } else {
                    None
                }
            })
            .collect();

        for r in &renames {
            variant
                .attrs
                .push(syn::parse_quote!(#[strum(to_string = #r)]));
            variant.attrs.push(syn::parse_quote!(#[serde(rename = #r)]));
        }
        variant.attrs.retain(|a| !a.path().is_ident("typed_enum"));

        // Case-insensitive ergonomics for URL/JSON deserialization:
        // accept the variant ident lowercased (`year`, `calendar`,
        // `done`) and the lowercased rename tag (`d`, `rr`) in
        // addition to their canonical forms. Serialization always
        // emits the canonical form.
        let ident_str = variant.ident.to_string();
        let mut aliases: Vec<String> = Vec::new();
        let ident_lower = ident_str.to_lowercase();
        if ident_lower != ident_str {
            aliases.push(ident_lower);
        }
        for r in &renames {
            let r_lower = r.to_lowercase();
            if r_lower != *r && !aliases.contains(&r_lower) {
                aliases.push(r_lower);
            }
        }
        for a in aliases {
            variant.attrs.push(syn::parse_quote!(#[serde(alias = #a)]));
        }
    }

    quote! {
        #[derive(
            Debug, Clone,
            strum::EnumString, strum::Display, strum::EnumIter,
            serde::Serialize, serde::Deserialize, utoipa::ToSchema,
        )]
        #[strum(parse_err_fn = Self::parse_error, parse_err_ty = #err)]
        #input

        impl From<&#name> for #name {
            fn from(v: &#name) -> #name { v.clone() }
        }

        impl #name {
            fn parse_error(s: &str) -> #err { #err::Parse(s.to_owned()) }
        }

        #[td_error::td_error]
        pub enum #err {
            #[error("Unknown variant: '{0}'")] Parse(String),
        }

        impl ta_objects::SeaValue for #name {
            fn as_sea_value(&self) -> sea_query::Value {
                sea_query::Value::String(Some(self.to_string()))
            }
            fn as_type_id(&self) -> std::any::TypeId { std::any::TypeId::of::<Self>() }
        }

        impl ta_objects::NullableSeaValue for #name {
            fn null_sea_value() -> sea_query::Value {
                sea_query::Value::String(None)
            }
        }

        impl ta_objects::sql_condition::SqlCondition for #name {
            fn resolve<D: ta_objects::DaoCore>(&self) -> Result<sea_query::Condition, ta_objects::errors::SqlError> {
                ta_objects::sql_condition::resolve_leaf_eq::<D>(self)
            }

            fn display_values<D: ta_objects::DaoCore>(&self) -> Vec<(String, String)> {
                ta_objects::sql_condition::leaf_display_values::<D>(self)
            }
        }

        impl ta_objects::ListFilterStr for #name {
            fn parse_filter_str(s: &str) -> Result<Self, td_error::TdError> {
                s.parse().map_err(Into::into)
            }
            fn as_string(&self) -> String {
                self.to_string()
            }
        }

        impl<DB: sqlx::Database> sqlx::Type<DB> for #name
        where
            String: sqlx::Type<DB>,
        {
            fn type_info() -> <DB as sqlx::Database>::TypeInfo {
                <String as sqlx::Type<DB>>::type_info()
            }

            fn compatible(ty: &<DB as sqlx::Database>::TypeInfo) -> bool {
                <String as sqlx::Type<DB>>::compatible(ty)
            }
        }

        impl<'r, DB: sqlx::Database> sqlx::Decode<'r, DB> for #name
        where
            String: sqlx::Decode<'r, DB>,
        {
            fn decode(
                value: <DB as sqlx::Database>::ValueRef<'r>,
            ) -> Result<Self, Box<dyn std::error::Error + 'static + Send + Sync>> {
                let decoded = <String as sqlx::Decode<'r, DB>>::decode(value)?;
                use std::str::FromStr;
                Ok(Self::from_str(&decoded).map_err(Box::new)?)
            }
        }
    }
    .into()
}
