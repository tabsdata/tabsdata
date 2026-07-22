//
// Copyright 2024 Tabsdata Inc.
//

//! Proc macros for the DXO domain model.
//!
//! A *DXO* is a triple of related types, one per layer of the stack:
//!
//! | Macro    | Layer | Maps to                          |
//! |----------|-------|----------------------------------|
//! | [`Dao`]  | data  | one SQL table (`sqlx::FromRow`)  |
//! | [`Dlo`]  | logic | an in-memory domain/logic object |
//! | [`Dto`]  | API   | a request/response contract      |
//!
//! Each layer macro generates a `…Builder`, the relevant trait impls (for
//! `Dao`: an `Iden` column enum, `FromRow`, and the `Dao` trait), and the
//! conversions declared via `#[dxo(...)]`. Conversions are emitted by
//! `type_builder::gen_conversions`; the layer macros differ only in which
//! extra impls they add.
//!
//! # The `#[dxo(...)]` conversion DSL
//!
//! `#[dxo(...)]` appears at two scopes:
//!
//! **Struct scope** declares whole-struct conversions into *this* type's
//! builder:
//! - `builder(try_from = Src)` generates `TryFrom<&Src> for ThisBuilder`.
//! - `updater(try_from = Src)` generates `TryUpdater<Src> for ThisBuilder`
//!   (used to fold audit context — e.g. `RequestContext` — onto a builder).
//! - `skip_all` starts from *no* fields copied; fields opt in individually.
//!   Without it, fields are copied by matching name.
//!
//! **Field scope** tunes one field within a struct-scope conversion:
//! - `builder(include)` / `updater(include)` — copy this field from the source
//!   (needed under `skip_all`).
//! - `builder(skip)` / `updater(skip)` — drop this field from a name-matched
//!   conversion (the counterpart of `include` without `skip_all`).
//! - `builder(field = "src_name")` — copy from a differently named source field.
//! - `updater(include, field = "user_id")` — fill this field from the named
//!   field of the updater source.
//! - `builder(try_from = Src, …)` — scope the tuning to the conversion from
//!   `Src` only; without it the tuning applies to every declared source.
//!
//! Each copied field goes through `src.field.clone().try_into()?`, so the
//! target field type's `TryFrom`/validation runs during conversion; a failure
//! is reported as `…BuilderError::ValidationError("Src.field -> This.field: …")`.
//!
//! Two more field attributes generate accessor impls:
//! - `#[dxo(extractor)]` — `Extractor<FieldType> for This` (clone the field out).
//! - `#[dxo(setter)]` — `Setter<FieldType> for ThisBuilder`.
//!
//! On a `Dao` field, `#[dxo(immutable)]` gives the column set-once semantics
//! in generated UPDATEs: `CASE WHEN col IS NULL THEN ? ELSE col END`.
//!
//! # Module-level `#[dxo]` and inheritance
//!
//! Applied to a `mod`, [`macro@dxo`] enables field/attribute inheritance between
//! sibling structs: a struct tagged `#[inherits(Base)]` merges `Base`'s fields
//! and `#[dxo]` attributes, and `#[exclude]` drops an inherited field. The
//! module is made private and its contents re-exported.

extern crate proc_macro;
use proc_macro::TokenStream;

mod dao;
mod dlo;
mod dto;
mod dxo;
mod type_builder;
mod typed_types;
mod url;

/// Module attribute enabling field/attribute inheritance between sibling
/// DAO/DLO/DTO structs via `#[inherits(Base)]` and `#[exclude]`. See the crate
/// docs.
#[proc_macro_attribute]
pub fn dxo(attr: TokenStream, item: TokenStream) -> TokenStream {
    dxo::dxo(attr, item)
}

#[proc_macro_derive(DaoType, attributes(inherits, exclude, dxo, dao, sqlx))]
pub fn dao_type(input: TokenStream) -> TokenStream {
    dao::dao_type(input)
}

/// Define a DAO: the data-layer type mapping to one SQL table. Requires
/// `#[dao(sql_table = "…")]`. Generates an `Iden` column enum, `FromRow`, the
/// `Dao` trait, a builder, and any `#[dxo(...)]` conversions. See the crate docs.
#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn Dao(args: TokenStream, item: TokenStream) -> TokenStream {
    dao::dao(args, item)
}

#[proc_macro_derive(DloType, attributes(inherits, exclude, dxo, dlo))]
pub fn dlo_type(input: TokenStream) -> TokenStream {
    dlo::dlo_type(input)
}

/// Define a DLO: the logic-layer domain object. Generates a builder and any
/// `#[dxo(...)]` conversions, without binding to a SQL table. See the crate docs.
#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn Dlo(args: TokenStream, item: TokenStream) -> TokenStream {
    dlo::dlo(args, item)
}

#[proc_macro_derive(DtoType, attributes(inherits, exclude, dxo, dto))]
pub fn dto_type(input: TokenStream) -> TokenStream {
    dto::dto_type(input)
}

/// Define a DTO: the API-layer request/response contract. Generates a builder,
/// `#[dxo(...)]` conversions, and list/serialization support. See the crate docs.
#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn Dto(args: TokenStream, item: TokenStream) -> TokenStream {
    dto::dto(args, item)
}

#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn UrlParam(args: TokenStream, item: TokenStream) -> TokenStream {
    url::url_param(args, item)
}

#[proc_macro_attribute]
#[allow(non_snake_case)]
pub fn QueryParam(args: TokenStream, item: TokenStream) -> TokenStream {
    url::url_param(args, item)
}

#[proc_macro_attribute]
pub fn typed(args: TokenStream, item: TokenStream) -> TokenStream {
    typed_types::typed_basic(args, item)
}

#[proc_macro_attribute]
pub fn typed_enum(args: TokenStream, item: TokenStream) -> TokenStream {
    typed_types::typed_enum(args, item)
}

#[proc_macro_attribute]
pub fn typed_tag(args: TokenStream, item: TokenStream) -> TokenStream {
    typed_types::typed_tag(args, item)
}
