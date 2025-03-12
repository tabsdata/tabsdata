//
// Copyright 2025 Tabs Data Inc.
//

use darling::FromMeta;
use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::{format_ident, quote, ToTokens};
use std::any::type_name;
use std::marker::PhantomData;
use std::str::FromStr;
use syn::{parse_macro_input, ItemStruct};
use td_shared::meta_parser::{OptionWrapper, SynMetaOrLit};
use td_shared::{downcast_option, parse_meta};

#[derive(Debug, FromMeta)]
#[allow(non_camel_case_types)]
struct FromTyped {
    #[darling(multiple)]
    try_from: Vec<Ident>,
    #[darling(flatten)]
    typed: Typed,
}

#[derive(Debug, FromMeta)]
#[allow(non_camel_case_types)]
enum Typed {
    // Basic types
    #[darling(rename = "string")]
    String(OptionWrapper<TypedString>),
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
    bool(OptionWrapper<TypedBool>),

    // Complex types
    #[darling(rename = "id")]
    Id(OptionWrapper<TypedId>),
    #[darling(rename = "timestamp")]
    Timestamp(OptionWrapper<TypedTimestamp>),
}

pub fn typed_basic(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_meta!(FromTyped, args).unwrap();
    let input = parse_macro_input!(item as ItemStruct);

    assert_eq!(
        input.fields.len(),
        0,
        "Typed can only be derived for structs without fields"
    );

    let typed = match args.typed {
        Typed::String(t) => typed_string(&input, t.into()),
        Typed::i16(t) => typed_int(&input, t.into()),
        Typed::i32(t) => typed_int(&input, t.into()),
        Typed::i64(t) => typed_int(&input, t.into()),
        Typed::f32(t) => typed_float(&input, t.into()),
        Typed::f64(t) => typed_float(&input, t.into()),
        Typed::bool(t) => typed_bool(&input, t.into()),
        Typed::Id(t) => typed_id(&input, t.into()),
        Typed::Timestamp(t) => typed_timestamp(&input, t.into()),
    };

    let name = &input.ident;
    let froms: Vec<_> = args
        .try_from
        .iter()
        .map(|from| {
            let from = format_ident!("{}", from);
            quote! {
                #[allow(clippy::needless_question_mark)]
                impl TryFrom<#from> for #name {
                    type Error = td_common::error::TdError;
                    fn try_from(val: #from) -> Result<Self, Self::Error> {
                        Ok(Self::parse((&*val).clone())?)
                    }
                }
            }
        })
        .collect();

    let expanded = quote! {
        #typed
        #(#froms)*
    };

    expanded.into()
}

fn some_or_none<T: ToTokens>(option: Option<T>) -> proc_macro2::TokenStream {
    if let Some(v) = option {
        quote! { Some(#v) }
    } else {
        quote! { None }
    }
}

#[derive(Debug, Default, FromMeta)]
pub struct TypedString {
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
    let error_name = format_ident!("{}Error", name);

    let (default, len, min_len, max_len, regex, parser) = if let Some(typed) = typed {
        let len = downcast_option!(typed.len, usize);
        let min_len = downcast_option!(typed.min_len, usize);
        let max_len = downcast_option!(typed.max_len, usize);
        match (len, min_len, max_len) {
            (Some(len), Some(min), _) => {
                assert!(len >= min, "len must be greater than or equal to min_len")
            }
            (Some(len), _, Some(max)) => {
                assert!(len <= max, "len must be less than or equal to max_len")
            }
            (_, Some(min), Some(max)) => {
                assert!(min <= max, "min_len must be less than or equal to max_len")
            }
            _ => {}
        };

        let default = downcast_option!(typed.default, String);
        if let Some(default) = default {
            if let Some(len) = len {
                assert_eq!(
                    default.len(),
                    len,
                    "default value length must be equal to len"
                );
            }
            if let Some(min_len) = min_len {
                assert!(
                    default.len() >= min_len,
                    "default value length must be greater than or equal to min_len"
                );
            }
            if let Some(max_len) = max_len {
                assert!(
                    default.len() <= max_len,
                    "default value length must be less than or equal to max_len"
                );
            }

            let regex = downcast_option!(typed.regex, String);
            if let Some(regex) = regex {
                let re: regex::Regex = regex::Regex::new(&regex).unwrap();
                assert!(re.is_match(&default), "default value must match regex");
            }
        }

        (
            typed.default,
            typed.len,
            typed.min_len,
            typed.max_len,
            typed.regex,
            typed.parser,
        )
    } else {
        (None, None, None, None, None, None)
    };

    let default = if let Some(default) = default {
        quote! {
            #default
        }
    } else {
        quote! { String::default() }
    };
    let len = some_or_none(len);
    let min_len = some_or_none(min_len);
    let max_len = some_or_none(max_len);
    let regex = if let Some(regex) = regex {
        quote! {
            lazy_static::lazy_static! {
                static ref RE: Option<regex::Regex> = Some(regex::Regex::new(&#regex).unwrap());
            };
            RE.clone()
        }
    } else {
        quote! { None }
    };
    let parser = if let Some(parser) = parser {
        quote! { Some(Box::new(#parser)) }
    } else {
        quote! { None }
    };

    let expanded = quote! {
        #(#attrs)*
        #[td_apiforge::api_server_schema]
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, sqlx::Decode, sqlx::Encode)]
        pub struct #name(String);

        #[td_error::td_error]
        pub enum #error_name {
            #[error("String value '{0}' must be of length {1}")]
            Len(String, usize),
            #[error("String value '{0}' cannot be shorter than {1} characters")]
            MinLen(String, usize),
            #[error("String value '{0}' cannot be longer than {1} characters")]
            MaxLen(String, usize),
            #[error("String value '{0}' does not match regex '{1}'")]
            Regex(String, String),
            #[error("Error parsing string value")]
            Parse(#[from] td_common::error::TdError),
        }

        impl Default for #name {
            fn default() -> Self {
                Self(#default.into())
            }
        }

        impl #name {
            fn len() -> Option<usize> {
                #len
            }

            fn min_len() -> Option<usize> {
                #min_len
            }

            fn max_len() -> Option<usize> {
                #max_len
            }

            fn regex() -> Option<regex::Regex> {
                #regex
            }

            fn custom_parser() -> Option<Box<dyn Fn(String) -> Result<String, td_common::error::TdError>>> {
                #parser
            }

            fn parse(val: impl Into<String>) -> Result<Self, #error_name> {
                let val = val.into();
                match (Self::len(), Self::min_len(), Self::max_len(), Self::regex(), Self::custom_parser()) {
                    (Some(len), _, _, _, _) if val.len() != len => Err(#error_name::Len(val, len))?,
                    (_, Some(min), _, _, _) if val.len() < min => Err(#error_name::MinLen(val, min))?,
                    (_, _, Some(max), _, _) if val.len() > max => Err(#error_name::MaxLen(val, max))?,
                    (_, _, _, Some(regex), _) if !regex.is_match(&val) => Err(#error_name::Regex(val, regex.to_string()))?,
                    (_, _, _, _, Some(parser)) => Ok(Self(parser(val).map_err(#error_name::Parse)?)),
                    _ => Ok(Self(val)),
                }
            }
        }

        impl std::ops::Deref for #name {
            type Target = String;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl TryFrom<String> for #name {
            type Error = #error_name;
            fn try_from(val: String) -> Result<#name, Self::Error> {
                #name::parse(val)
            }
        }

        impl TryFrom<&String> for #name {
            type Error = #error_name;
            fn try_from(val: &String) -> Result<#name, Self::Error> {
                #name::parse(val)
            }
        }

        impl TryFrom<&str> for #name {
            type Error = #error_name;
            fn try_from(val: &str) -> Result<#name, Self::Error> {
                #name::parse(val)
            }
        }

        impl std::fmt::Display for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl<'de> serde::Deserialize<'de> for #name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let s = String::deserialize(deserializer)?;
                #name::parse(s).map_err(serde::de::Error::custom)
            }
        }

        impl serde::Serialize for #name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                self.0.serialize(serializer)
            }
        }

        impl sqlx::Type<sqlx::Sqlite> for #name {

            fn type_info() -> <sqlx::Sqlite as sqlx::Database>::TypeInfo {
                <String as sqlx::Type<sqlx::Sqlite>>::type_info()
            }

            fn compatible(ty: &<sqlx::Sqlite as sqlx::Database>::TypeInfo) -> bool {
                <String as sqlx::Type<sqlx::Sqlite>>::compatible(ty)
            }
        }
    };

    expanded
}

#[derive(Debug, Default, FromMeta)]
pub struct TypedNumeric<T> {
    default: Option<SynMetaOrLit>,
    min: Option<SynMetaOrLit>,
    max: Option<SynMetaOrLit>,
    #[darling(skip)]
    phantom_data: PhantomData<T>,
}

pub fn typed_int<T: FromStr + ToTokens + PartialOrd>(
    input: &ItemStruct,
    typed: Option<TypedNumeric<T>>,
) -> proc_macro2::TokenStream {
    let expanded = typed_numeric(input, typed);

    let expanded = quote! {
        #[td_apiforge::api_server_schema]
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, sqlx::Decode, sqlx::Encode)]
        #expanded
    };

    expanded
}

pub fn typed_float<T: FromStr + ToTokens + PartialOrd>(
    input: &ItemStruct,
    typed: Option<TypedNumeric<T>>,
) -> proc_macro2::TokenStream {
    let expanded = typed_numeric(input, typed);

    let expanded = quote! {
        #[td_apiforge::api_server_schema]
        #[derive(Debug, Clone, PartialEq, PartialOrd, sqlx::Decode, sqlx::Encode)]
        #expanded
    };

    expanded
}

pub fn typed_numeric<T: FromStr + ToTokens + PartialOrd>(
    input: &ItemStruct,
    typed: Option<TypedNumeric<T>>,
) -> proc_macro2::TokenStream {
    let type_name = type_name::<T>();
    let int_type = format_ident!("{}", type_name);

    let attrs = &input.attrs;
    let name = &input.ident;
    let error_name = format_ident!("{}Error", name);

    let (default, min, max) = if let Some(typed) = typed {
        let min = downcast_option!(typed.min, T);
        let max = downcast_option!(typed.max, T);
        if let (Some(min), Some(max)) = (&min, &max) {
            assert!(min <= max, "min must be less than or equal to max")
        };

        let default = downcast_option!(typed.default, T);
        if let Some(default) = &default {
            if let Some(min) = &min {
                assert!(
                    default >= min,
                    "default value length must be greater than or equal to min"
                );
            }
            if let Some(max) = &max {
                assert!(
                    default <= max,
                    "default value length must be less than or equal to max"
                );
            }
        }

        (typed.default, typed.min, typed.max)
    } else {
        (None, None, None)
    };

    let default = if let Some(default) = default {
        quote! {
            #default
        }
    } else {
        quote! { #int_type::default() }
    };
    let min = some_or_none(min);
    let max = some_or_none(max);

    let expanded = quote! {
        #(#attrs)*
        pub struct #name(#int_type);

        #[td_error::td_error]
        pub enum #error_name {
            #[error("Value '{0}' cannot be lower than '{1}' characters")]
            Min(#int_type, #int_type),
            #[error("Value '{0}' cannot be higher than '{1}' characters")]
            Max(#int_type, #int_type),
        }

        impl Default for #name {
            fn default() -> Self {
                Self(#default)
            }
        }

        impl #name {
            fn min() -> Option<#int_type> {
                #min
            }

            fn max() -> Option<#int_type> {
                #max
            }

            fn parse(val: impl Into<#int_type>) -> Result<Self, #error_name> {
                let val = val.into();
                match (Self::min(), Self::max()) {
                    (Some(min), _) if val < min => Err(#error_name::Min(val, min))?,
                    (_, Some(max)) if val > max => Err(#error_name::Max(val, max))?,
                    _ => Ok(Self(val)),
                }
            }
        }

        impl std::ops::Deref for #name {
            type Target = #int_type;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl TryFrom<#int_type> for #name {
            type Error = #error_name;
            fn try_from(val: #int_type) -> Result<#name, Self::Error> {
                #name::parse(val)
            }
        }

        impl std::fmt::Display for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl<'de> serde::Deserialize<'de> for #name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let s = #int_type::deserialize(deserializer)?;
                #name::parse(s).map_err(serde::de::Error::custom)
            }
        }

        impl serde::Serialize for #name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                self.0.serialize(serializer)
            }
        }

        impl sqlx::Type<sqlx::Sqlite> for #name {

            fn type_info() -> <sqlx::Sqlite as sqlx::Database>::TypeInfo {
                <#int_type as sqlx::Type<sqlx::Sqlite>>::type_info()
            }

            fn compatible(ty: &<sqlx::Sqlite as sqlx::Database>::TypeInfo) -> bool {
                <#int_type as sqlx::Type<sqlx::Sqlite>>::compatible(ty)
            }
        }

    };

    expanded
}

#[derive(Debug, Default, FromMeta)]
pub struct TypedBool {
    default: Option<SynMetaOrLit>,
}

pub fn typed_bool(input: &ItemStruct, typed: Option<TypedBool>) -> proc_macro2::TokenStream {
    let attrs = &input.attrs;
    let name = &input.ident;

    let default = if let Some(typed) = typed {
        typed.default
    } else {
        None
    };

    let default = if let Some(default) = default {
        quote! { #default }
    } else {
        quote! { bool::default() }
    };

    let expanded = quote! {
        #(#attrs)*
        #[td_apiforge::api_server_schema]
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, sqlx::Decode, sqlx::Encode)]
        pub struct #name(bool);

        impl Default for #name {
            fn default() -> Self {
                Self(#default)
            }
        }

        impl #name {
            fn parse(val: impl Into<bool>) -> Result<Self, td_common::error::TdError> {
                let val = val.into();
                Ok(Self(val))
            }
        }

        impl std::ops::Deref for #name {
            type Target = bool;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl TryFrom<bool> for #name {
            type Error = td_common::error::TdError;
            fn try_from(val: bool) -> Result<#name, td_common::error::TdError> {
                #name::parse(val)
            }
        }

        impl std::fmt::Display for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl<'de> serde::Deserialize<'de> for #name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let s = bool::deserialize(deserializer)?;
                #name::parse(s).map_err(serde::de::Error::custom)
            }
        }

        impl serde::Serialize for #name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                self.0.serialize(serializer)
            }
        }

        impl sqlx::Type<sqlx::Sqlite> for #name {

            fn type_info() -> <sqlx::Sqlite as sqlx::Database>::TypeInfo {
                <bool as sqlx::Type<sqlx::Sqlite>>::type_info()
            }

            fn compatible(ty: &<sqlx::Sqlite as sqlx::Database>::TypeInfo) -> bool {
                <bool as sqlx::Type<sqlx::Sqlite>>::compatible(ty)
            }
        }

    };

    expanded
}

#[derive(Debug, Default, FromMeta)]
pub struct TypedId {
    default: Option<SynMetaOrLit>,
}

pub fn typed_id(input: &ItemStruct, typed: Option<TypedId>) -> proc_macro2::TokenStream {
    let attrs = &input.attrs;
    let name = &input.ident;

    let default = if let Some(typed) = typed {
        typed.default
    } else {
        None
    };

    let default = if let Some(default) = default {
        let default_tokens: proc_macro2::TokenStream = default.to_token_stream();
        quote! { #default_tokens }
    } else {
        quote! { td_common::id::Id::default() }
    };

    let expanded = quote! {
        #(#attrs)*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, sqlx::Decode, sqlx::Encode)]
        pub struct #name(td_common::id::Id);

        impl Default for #name {
            fn default() -> Self {
                Self(#default)
            }
        }

        impl #name {

            fn new() -> Self {
                Self(td_common::id::id())
            }

            fn parse(val: impl Into<td_common::id::Id>) -> Result<Self, td_common::error::TdError> {
                let val = val.into();
                Ok(Self(val))
            }
        }

        impl utoipa::__dev::ComposeSchema for #name {
            fn compose(
                mut generics: Vec<utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>>,
            ) -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
                utoipa::openapi::ObjectBuilder::new()
                    .schema_type(utoipa::openapi::schema::SchemaType::new(
                        utoipa::openapi::schema::Type::String,
                    ))
                    .format(Some(utoipa::openapi::schema::SchemaFormat::KnownFormat(
                        utoipa::openapi::schema::KnownFormat::Uuid,
                    )))
                    .into()
            }
        }
        impl utoipa::ToSchema for #name {
            fn name() -> std::borrow::Cow<'static, str> {
                std::borrow::Cow::Borrowed(stringify!(#name))
            }
            fn schemas(
                schemas: &mut Vec<(
                    String,
                    utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>,
                )>,
            ) {
                schemas.extend([]);
            }
        }

        impl std::ops::Deref for #name {
            type Target = td_common::id::Id;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl TryFrom<td_common::id::Id> for #name {
            type Error = td_common::error::TdError;
            fn try_from(val: td_common::id::Id) -> Result<#name, td_common::error::TdError> {
                #name::parse(val)
            }
        }

        impl std::fmt::Display for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl<'de> serde::Deserialize<'de> for #name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let s = td_common::id::Id::deserialize(deserializer)?;
                #name::parse(s).map_err(serde::de::Error::custom)
            }
        }

        impl serde::Serialize for #name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                self.0.serialize(serializer)
            }
        }

        impl sqlx::Type<sqlx::Sqlite> for #name {

            fn type_info() -> <sqlx::Sqlite as sqlx::Database>::TypeInfo {
                <String as sqlx::Type<sqlx::Sqlite>>::type_info()
            }

            fn compatible(ty: &<sqlx::Sqlite as sqlx::Database>::TypeInfo) -> bool {
                <String as sqlx::Type<sqlx::Sqlite>>::compatible(ty)
            }
        }

    };

    expanded
}

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

    let default = if let Some(typed) = typed {
        typed.default
    } else {
        None
    };

    let default = if let Some(default) = default {
        let default_tokens: proc_macro2::TokenStream = default.to_token_stream();
        quote! { #default_tokens }
    } else {
        quote! {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(async { td_common::time::UniqueUtc::now_millis().await })
            })
        }
    };

    let expanded = quote! {
        #(#attrs)*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, sqlx::Decode, sqlx::Encode)]
        pub struct #name(chrono::DateTime<chrono::Utc>);

        impl Default for #name {
            fn default() -> Self {
                Self(#default)
            }
        }

        impl #name {
            async fn now() -> Self {
                Self(td_common::time::UniqueUtc::now_millis().await)
            }

            fn parse(val: impl Into<chrono::DateTime<chrono::Utc>>) -> Result<Self, td_common::error::TdError> {
                let val = val.into();
                Ok(Self(val))
            }
        }

        impl utoipa::__dev::ComposeSchema for #name {
            fn compose(
                mut generics: Vec<utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>>,
            ) -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
                utoipa::openapi::ObjectBuilder::new()
                    .schema_type(utoipa::openapi::schema::SchemaType::new(
                        utoipa::openapi::schema::Type::Integer,
                    ))
                    .format(Some(utoipa::openapi::schema::SchemaFormat::KnownFormat(
                        utoipa::openapi::schema::KnownFormat::Int64,
                    )))
                    .into()
            }
        }
        impl utoipa::ToSchema for #name {
            fn name() -> std::borrow::Cow<'static, str> {
                std::borrow::Cow::Borrowed(stringify!(#name))
            }
            fn schemas(
                schemas: &mut Vec<(
                    String,
                    utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>,
                )>,
            ) {
                schemas.extend([]);
            }
        }

        impl std::ops::Deref for #name {
            type Target = chrono::DateTime<chrono::Utc>;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl TryFrom<chrono::DateTime<chrono::Utc>> for #name {
            type Error = td_common::error::TdError;
            fn try_from(val: chrono::DateTime<chrono::Utc>) -> Result<#name, td_common::error::TdError> {
                #name::parse(val)
            }
        }

        impl std::fmt::Display for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl<'de> serde::Deserialize<'de> for #name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let s = chrono::DateTime::<chrono::Utc>::deserialize(deserializer)?;
                #name::parse(s).map_err(serde::de::Error::custom)
            }
        }

        impl serde::Serialize for #name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                self.0.serialize(serializer)
            }
        }

        impl sqlx::Type<sqlx::Sqlite> for #name {

            fn type_info() -> <sqlx::Sqlite as sqlx::Database>::TypeInfo {
                <chrono::DateTime<chrono::Utc> as sqlx::Type<sqlx::Sqlite>>::type_info()
            }

            fn compatible(ty: &<sqlx::Sqlite as sqlx::Database>::TypeInfo) -> bool {
                <chrono::DateTime<chrono::Utc> as sqlx::Type<sqlx::Sqlite>>::compatible(ty)
            }
        }

    };

    expanded
}
