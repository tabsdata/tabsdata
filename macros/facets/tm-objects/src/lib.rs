//
// Copyright 2024 Tabsdata Inc.
//

//! Proc macros for the DXO domain model.
//!
//! A *DXO* is a triple of related types, one per layer of the stack:
//!
//! | Macro    | Layer | Maps to                          |
//! |----------|-------|----------------------------------|
//! | [`Dao`]  | data  | one SQL table or view            |
//! | [`Dlo`]  | logic | an in-memory domain/logic object |
//! | [`Dto`]  | API   | a request/response contract      |
//!
//! Each layer macro generates the relevant trait impls (for `Dao`: an
//! `Iden` column enum, `FromRow` on every shape but an update payload, and
//! the `Dao` trait plus the capability traits `ops` names); the layer macros
//! differ only in which extra impls they add.
//!
//! # Typed values
//!
//! Field values are newtypes and closed string sets built from facet
//! derives — the derive list shows exactly which external contracts a type
//! participates in:
//!
//! | derive         | role                                               |
//! |----------------|----------------------------------------------------|
//! | [`Typed`]      | domain core: the `ta_objects::Typed::try_new` construction gate (from `#[typed(...)]` constraints), conversions, Deref/Display, SeaValue/SqlCondition |
//! | [`TypedEnum`]  | closed string set over unit variants (`#[typed_enum(rename)]`) |
//! | [`TypedTag`]   | prefix-tagged union of newtypes (`#[tag("…")]`)     |
//! | [`TypedSerde`] | wire: Serialize/Deserialize, deserializing through the gate |
//! | [`TypedSqlx`]  | database: Type/Encode/Decode, decoding through the gate |
//! | [`TypedOpenApi`] | API docs: inline schema carrying the constraints  |
//!
//! Every construction path routes through the gate, so a held value always
//! satisfies its constraints; see `ta_objects::typed` for the invariant.
//!
//! On a `Dao` field, `#[dao(immutable)]` gives the column set-once semantics
//! in generated UPDATEs: `CASE WHEN col IS NULL THEN ? ELSE col END`.
//!
//! # `#[dao(flatten)]`
//!
//! A DAO field marked `#[dao(flatten)]` embeds another DAO whose columns this
//! DAO also projects (base first): row decoding delegates via
//! `#[sqlx(flatten)]`, and all column metadata (`Iden`, `columns()`,
//! `order_by()`, `insert_values()`, …) delegates to the base. A structural
//! column named in `#[dao(...)]` args that isn't an own field resolves into
//! the flattened base at any nesting depth, and each base column is re-named
//! as a flat associated const on the derived `Iden`, so raw sea_query spells
//! it `XIden::Col` rather than `XIden::Base(YIden::Col)`.
//!
//! Collisions across the flatten boundary are compile errors: the merged
//! (base + own) field list may not repeat a field name (serde/sqlx flatten
//! would silently alias) nor a field type (type-keyed condition columns,
//! `HasColumn`, would be ambiguous).

extern crate proc_macro;
use proc_macro::TokenStream;

mod brand;
mod dao;
mod dao_marks;
mod dao_unique;
mod dlo;
mod dto;
mod dxo_fields;
mod type_builder;
mod typed_types;
mod url;

#[proc_macro_derive(DaoType, attributes(dao, sqlx))]
pub fn dao_type(input: TokenStream) -> TokenStream {
    dao::dao_type(input)
}

/// Define a DAO: the data-layer type mapping to one SQL relation. Requires
/// `#[dao(table = "…", ops(…))]` or `#[dao(view = "…")]` — a table declares
/// what it may be the subject of, a view is readable by construction.
/// Generates an `Iden` column enum, `FromRow`, the structural traits, and the
/// capability traits `ops` names. See the crate docs.
#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn Dao(args: TokenStream, item: TokenStream) -> TokenStream {
    dao::dao(args, item)
}

#[proc_macro_derive(DloType, attributes(dlo))]
pub fn dlo_type(input: TokenStream) -> TokenStream {
    dlo::dlo_type(input)
}

/// Define a DLO: the logic-layer domain object, not bound to a SQL table.
/// See the crate docs.
#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn Dlo(args: TokenStream, item: TokenStream) -> TokenStream {
    dlo::dlo(args, item)
}

#[proc_macro_derive(DtoType, attributes(dto))]
pub fn dto_type(input: TokenStream) -> TokenStream {
    dto::dto_type(input)
}

/// Define a DTO: the API-layer request/response contract, with
/// list/serialization support. See the crate docs.
#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn Dto(args: TokenStream, item: TokenStream) -> TokenStream {
    dto::dto(args, item)
}

/// Flattened-DAO continuation of `__dxo_fields_*`: merged-list bans +
/// `HasColumn<T>` impls. Internal to the `#[Dao]` expansion.
#[doc(hidden)]
#[proc_macro]
pub fn __dao_has_columns(input: TokenStream) -> TokenStream {
    dxo_fields::dao_has_columns(input)
}

/// Flattened-DLO/DTO continuation of `__dxo_fields_*`: merged duplicate-name
/// ban. Internal to the `#[Dlo]`/`#[Dto]` expansion.
#[doc(hidden)]
#[proc_macro]
pub fn __dxo_check_flat_names(input: TokenStream) -> TokenStream {
    dxo_fields::check_flat_names(input)
}

#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn UrlParam(args: TokenStream, item: TokenStream) -> TokenStream {
    url::url_param(args, item)
}

#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn QueryParam(args: TokenStream, item: TokenStream) -> TokenStream {
    url::query_param(args, item)
}

/// A URL param struct's path placeholders: `NAMES` from the field names,
/// `values()` via `to_string`. Bundled by `#[UrlParam]`.
#[proc_macro_derive(PathParams)]
pub fn path_params(input: TokenStream) -> TokenStream {
    url::derive_path_params(input)
}

/// Newtype over a primitive (`String`, `i16..f64`, `bool`, `Id`, `IdL<K>`,
/// `Timestamp`) or a `ComposedString` type, detected from the single unnamed
/// field. Implements the construction gate (`ta_objects::Typed::try_new`)
/// from the `#[typed(...)]` constraints (`len`/`min_len`/`max_len`/`regex`/
/// `parser`/`secret` for strings, `min`/`max` for numerics, `default`,
/// `try_from` siblings) plus the domain plumbing. External contracts are the
/// facet derives: [`TypedSerde`], [`TypedSqlx`], [`TypedOpenApi`].
#[proc_macro_derive(Typed, attributes(typed))]
pub fn typed(input: TokenStream) -> TokenStream {
    typed_types::derive_typed(input)
}

/// Wire contract facet: Serialize/Deserialize, deserializing through the
/// `ta_objects::Typed::try_new` gate. Requires `#[derive(Typed)]`.
#[proc_macro_derive(TypedSerde)]
pub fn typed_serde(input: TokenStream) -> TokenStream {
    typed_types::derive_typed_serde(input)
}

/// Database contract facet: sqlx Type/Encode/Decode (transparent over the
/// inner), decoding through the `ta_objects::Typed::try_new` gate. Requires
/// `#[derive(Typed)]`.
#[proc_macro_derive(TypedSqlx)]
pub fn typed_sqlx(input: TokenStream) -> TokenStream {
    typed_types::derive_typed_sqlx(input)
}

/// API docs contract facet: utoipa schema (inline, from the inner type).
#[proc_macro_derive(TypedOpenApi)]
pub fn typed_openapi(input: TokenStream) -> TokenStream {
    typed_types::derive_typed_openapi(input)
}

/// Closed string set over unit variants; `#[typed_enum(rename = "…")]` sets
/// the canonical form. FromStr is the gate. External contracts come from the
/// same facet derives as newtypes.
#[proc_macro_derive(TypedEnum, attributes(typed_enum))]
pub fn typed_enum(input: TokenStream) -> TokenStream {
    typed_types::derive_typed_enum(input)
}

/// Newtype variants distinguished by a `#[tag("…")]` string prefix; Display
/// renders tag + inner, FromStr strips the longest matching tag.
#[proc_macro_derive(TypedTag, attributes(tag))]
pub fn typed_tag(input: TokenStream) -> TokenStream {
    typed_types::derive_typed_tag(input)
}
