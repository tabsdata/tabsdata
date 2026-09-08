//
// Copyright 2026 Tabsdata Inc.
//

//! Brand expansion for the `Typed*` facet derives: `Name<T>(pub T)` (single
//! type parameter as the single field) distinguishes same-typed columns by
//! row role. Everything delegates to the inner type except
//! `SqlCondition`, which keys on the brand.

use proc_macro2::Ident;
use quote::quote;
use syn::DeriveInput;

/// The type parameter when the struct has the brand shape.
pub fn brand_param(input: &DeriveInput) -> Option<Ident> {
    let mut params = input.generics.params.iter();
    let (Some(syn::GenericParam::Type(p)), None) = (params.next(), params.next()) else {
        return None;
    };
    let syn::Data::Struct(s) = &input.data else {
        return None;
    };
    let syn::Fields::Unnamed(f) = &s.fields else {
        return None;
    };
    let [field] = f.unnamed.iter().collect::<Vec<_>>()[..] else {
        return None;
    };
    match &field.ty {
        syn::Type::Path(tp) if tp.path.is_ident(&p.ident) => Some(p.ident.clone()),
        _ => None,
    }
}

pub fn expand_typed(name: &Ident, t: &Ident) -> proc_macro2::TokenStream {
    quote! {
        impl<#t> #name<#t> {
            pub fn into_inner(self) -> #t {
                self.0
            }
        }

        impl<#t> std::ops::Deref for #name<#t> {
            type Target = #t;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl<#t> From<#t> for #name<#t> {
            fn from(v: #t) -> Self {
                Self(v)
            }
        }

        impl<#t: Clone> Clone for #name<#t> {
            fn clone(&self) -> Self {
                Self(self.0.clone())
            }
        }
        impl<#t: Copy> Copy for #name<#t> {}

        impl<#t: std::fmt::Debug> std::fmt::Debug for #name<#t> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_tuple(stringify!(#name)).field(&self.0).finish()
            }
        }

        impl<#t: std::fmt::Display> std::fmt::Display for #name<#t> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }

        impl<#t: PartialEq> PartialEq for #name<#t> {
            fn eq(&self, other: &Self) -> bool {
                self.0 == other.0
            }
        }
        impl<#t: Eq> Eq for #name<#t> {}

        impl<#t: PartialOrd> PartialOrd for #name<#t> {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                self.0.partial_cmp(&other.0)
            }
        }
        impl<#t: Ord> Ord for #name<#t> {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.0.cmp(&other.0)
            }
        }

        impl<#t: std::hash::Hash> std::hash::Hash for #name<#t> {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                self.0.hash(state)
            }
        }

        impl<#t> ta_objects::SeaValue for #name<#t>
        where
            #t: ta_objects::SeaValue + 'static,
        {
            fn as_sea_value(&self) -> sea_query::Value {
                self.0.as_sea_value()
            }
        }

        impl<#t> ta_objects::NullableSeaValue for #name<#t>
        where
            #t: ta_objects::NullableSeaValue + 'static,
        {
            fn null_sea_value() -> sea_query::Value {
                <#t as ta_objects::NullableSeaValue>::null_sea_value()
            }
        }

        impl<D, #t> ta_objects::sql_condition::SqlCondition<D> for #name<#t>
        where
            D: ta_objects::HasColumn<Self>,
            #t: ta_objects::SeaValue + 'static,
        {
            fn resolve(&self) -> sea_query::Condition {
                ta_objects::sql_condition::resolve_leaf_eq::<D, Self>(self)
            }
            fn display_values(&self) -> Vec<(String, String)> {
                ta_objects::sql_condition::leaf_display_values::<D, Self>(self)
            }
        }

        impl<#t> ta_objects::ListFilterStr for #name<#t>
        where
            #t: ta_objects::ListFilterStr,
        {
            fn parse_filter_str(s: &str) -> Result<Self, td_error::TdError> {
                Ok(Self(<#t as ta_objects::ListFilterStr>::parse_filter_str(s)?))
            }
            fn as_string(&self) -> String {
                self.0.as_string()
            }
        }
    }
}

pub fn expand_serde(name: &Ident, t: &Ident) -> proc_macro2::TokenStream {
    quote! {
        impl<#t: serde::Serialize> serde::Serialize for #name<#t> {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                self.0.serialize(serializer)
            }
        }

        impl<'de, #t: serde::Deserialize<'de>> serde::Deserialize<'de> for #name<#t> {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                <#t as serde::Deserialize<'de>>::deserialize(deserializer).map(Self)
            }
        }
    }
}

pub fn expand_sqlx(name: &Ident, t: &Ident) -> proc_macro2::TokenStream {
    quote! {
        impl<DB: sqlx::Database, #t> sqlx::Type<DB> for #name<#t>
        where
            #t: sqlx::Type<DB>,
        {
            fn type_info() -> <DB as sqlx::Database>::TypeInfo {
                <#t as sqlx::Type<DB>>::type_info()
            }
            fn compatible(ty: &<DB as sqlx::Database>::TypeInfo) -> bool {
                <#t as sqlx::Type<DB>>::compatible(ty)
            }
        }

        impl<'q, DB: sqlx::Database, #t> sqlx::encode::Encode<'q, DB> for #name<#t>
        where
            #t: sqlx::encode::Encode<'q, DB>,
        {
            fn encode_by_ref(
                &self,
                buf: &mut <DB as sqlx::Database>::ArgumentBuffer,
            ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
                <#t as sqlx::encode::Encode<'q, DB>>::encode_by_ref(&self.0, buf)
            }
            fn produces(&self) -> Option<<DB as sqlx::Database>::TypeInfo> {
                <#t as sqlx::encode::Encode<'q, DB>>::produces(&self.0)
            }
            fn size_hint(&self) -> usize {
                <#t as sqlx::encode::Encode<'q, DB>>::size_hint(&self.0)
            }
        }

        impl<'r, DB: sqlx::Database, #t> sqlx::decode::Decode<'r, DB> for #name<#t>
        where
            #t: sqlx::decode::Decode<'r, DB>,
        {
            fn decode(
                value: <DB as sqlx::Database>::ValueRef<'r>,
            ) -> Result<Self, sqlx::error::BoxDynError> {
                Ok(Self(<#t as sqlx::decode::Decode<'r, DB>>::decode(value)?))
            }
        }

        impl<#t> sqlx::postgres::PgHasArrayType for #name<#t>
        where
            #t: sqlx::postgres::PgHasArrayType,
        {
            fn array_type_info() -> sqlx::postgres::PgTypeInfo {
                <#t as sqlx::postgres::PgHasArrayType>::array_type_info()
            }
        }
    }
}

pub fn expand_openapi(name: &Ident, t: &Ident) -> proc_macro2::TokenStream {
    quote! {
        impl<#t> utoipa::__dev::ComposeSchema for #name<#t>
        where
            #t: utoipa::__dev::ComposeSchema,
        {
            fn compose(
                schemas: Vec<utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>>,
            ) -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
                <#t as utoipa::__dev::ComposeSchema>::compose(schemas)
            }
        }

        impl<#t> utoipa::ToSchema for #name<#t>
        where
            #t: utoipa::ToSchema + utoipa::__dev::ComposeSchema,
        {
            fn name() -> std::borrow::Cow<'static, str> {
                <#t as utoipa::ToSchema>::name()
            }
            fn schemas(
                schemas: &mut Vec<(
                    String,
                    utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>,
                )>,
            ) {
                <#t as utoipa::ToSchema>::schemas(schemas)
            }
        }
    }
}
