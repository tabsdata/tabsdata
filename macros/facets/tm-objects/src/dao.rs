//
// Copyright 2025 Tabsdata Inc.
//

// The DaoType derive macro generates an Iden enum from struct field names.
// Given a struct field `defined_on`, the generated variant is `DefinedOn`
// (UpperCamelCase via heck). We must use the same conversion when referencing
// variants from the macro, otherwise the generated code won't compile.
//
// Field idents (snake_case) → Iden variants (UpperCamelCase):
//   id           → Id
//   defined_on   → DefinedOn
//   table_id     → TableId
//   partition_by → PartitionBy
//
// The Iden enum itself is named <StructName>Iden and has a leading Table variant:
//   struct TableDB { id, defined_on } → enum TableDBIden { Table, Id, DefinedOn }

use crate::dxo_fields::{callback_ident, fields_callback};
use crate::type_builder::parse_input_item_struct;
use darling::util::{Flag, PathList};
use darling::{FromDeriveInput, FromMeta};
use heck::ToUpperCamelCase;
use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::{format_ident, quote};
use std::collections::HashMap;
use syn::{DeriveInput, Fields, ItemStruct, Type, parse_macro_input};

// ---- Darling argument structs ----------------------------------------------

#[derive(Debug, FromDeriveInput)]
#[darling(attributes(dao))]
pub struct DaoArguments {
    update: Flag,
    sql_table: Option<String>,
    /// The table's `DbDomain`; absent = `ta_objects::Apiserver`.
    db: Option<syn::Path>,
    order_by: Option<OrderByArguments>,
    versioned: Option<VersionedArguments>,
    recursive: Option<RecursiveArguments>,
    data_version: Flag,
    /// Opt-out: this DAO is a truly append-only event log (multiple rows
    /// per partition). Skip the `Dao` impl so callers can't reach for the
    /// bare `BasicQueries::fetch_*` methods — they must walk through
    /// `current()` / `versioned(...)` / `historic(...)`.
    event_log: Flag,
    logical_pointer: Option<LogicalPointerArguments>,
    /// The row's logical key. Emits a `KeyedDao` impl (key = these columns,
    /// values = every other DAO column), unlocking the generic
    /// upsert / insert-or-ignore surface. Must back a `UNIQUE` constraint.
    #[darling(default)]
    key: PathList,
}

#[derive(Debug, FromMeta)]
struct OrderByArguments {
    col: String,
    #[darling(default = "default_order_dir")]
    dir: String,
}

fn default_order_dir() -> String {
    "desc".into()
}

#[derive(Debug, FromMeta)]
struct VersionedArguments {
    pk: Option<String>,
    version_by: Option<String>,
    partition_by: Option<String>,
    current: Option<String>,
}

#[derive(Debug, FromMeta)]
struct RecursiveArguments {
    up: String,
    down: String,
}

#[derive(Debug, FromMeta)]
struct LogicalPointerArguments {
    /// Column on this pointer-event table holding the logical identity
    /// (e.g. `logical_function_run_id`).
    logical_id_col: String,
    /// Column holding the concrete-binding FK (Option-typed); NULL on
    /// Pending / Voided events (e.g. `physical_function_run_id`).
    concrete_id_col: String,
    /// The insert-once identity-row DAO type.
    identity: syn::Path,
    /// Human-readable kind tag for error messages ("LFR" / "LTDV").
    kind: String,
}

// ---- Helpers ---------------------------------------------------------------

/// Look up a field's type by name, returning a compile error if not found.
/// Use inside functions that return `TokenStream` — the `return` unwinds to the
/// enclosing function on error.
macro_rules! require_field {
    ($fields:expr, $name:expr, $context:expr, $span:expr) => {
        match field_type_by_name($fields, $name) {
            Some(ty) => ty,
            None => {
                return syn::Error::new_spanned(
                    $span,
                    format!(concat!($context, ": field '{}' not found in struct"), $name),
                )
                .to_compile_error()
                .into();
            }
        }
    };
}

// ---- Entry points ----------------------------------------------------------

/// Attribute macro that wraps a struct with DAO derives (DaoType, Builder, and
/// optionally FromRow).  When the struct carries `#[dao(update, …)]` the type
/// is update-only and `sqlx::FromRow` is skipped (never read from DB).
pub fn dao(_args: TokenStream, item: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(item as ItemStruct);
    let is_update = is_dao_update_attr(&input);
    // A `#[dao(flatten)]` field decodes by delegating to the base DAO's
    // FromRow impl on the same row.
    if let Fields::Named(named) = &mut input.fields {
        for f in named.named.iter_mut() {
            if is_dao_flatten(f) {
                f.attrs.push(syn::parse_quote! { #[sqlx(flatten)] });
            }
        }
    }
    if is_update {
        quote! {
            #[derive(Debug, Clone, ta_objects::DaoType)]
            #input
        }
        .into()
    } else {
        quote! {
            #[derive(Debug, Clone, ta_objects::DaoType, sqlx::FromRow)]
            #input
        }
        .into()
    }
}

/// Check whether any `#[dao(...)]` attribute on the struct contains `update`.
fn is_dao_update_attr(input: &ItemStruct) -> bool {
    input.attrs.iter().any(|a| {
        if !a.path().is_ident("dao") {
            return false;
        }
        // Parse the token list inside #[dao(...)]; look for a bare `update` ident.
        a.parse_args_with(|tokens: syn::parse::ParseStream| {
            while !tokens.is_empty() {
                if tokens.peek(syn::Ident) {
                    let ident: Ident = tokens.parse()?;
                    if ident == "update" {
                        return Ok(true);
                    }
                } else {
                    // Skip non-ident tokens (commas, nested groups, etc.)
                    let _: proc_macro2::TokenTree = tokens.parse()?;
                }
            }
            Ok(false)
        })
        .unwrap_or(false)
    })
}

/// Derive macro that generates the Iden enum, DaoCore, TypedColumns, and trait
/// impls (Dao, VersionedDao, Recursive, States) for a DAO struct.
pub fn dao_type(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let parsed = match DaoArguments::from_derive_input(&input) {
        Ok(p) => p,
        Err(e) => return e.write_errors().into(),
    };
    let item = match parse_input_item_struct(&input) {
        Ok(item) => item,
        Err(e) => return e.to_compile_error().into(),
    };

    let ident = &item.ident;
    let vis = &item.vis;
    let fields = &item.fields;
    let (impl_generics, ty_generics, where_clause) = item.generics.split_for_impl();

    // At most one `#[dao(flatten)]` field: an embedded base DAO whose columns
    // this DAO also projects. All column metadata delegates to it, base first
    // (matching the merged-field order the flattened struct would have).
    let flatten_fields: Vec<&syn::Field> = fields.iter().filter(|f| is_dao_flatten(f)).collect();
    if flatten_fields.len() > 1 {
        return syn::Error::new_spanned(ident, "at most one #[dao(flatten)] field is supported")
            .to_compile_error()
            .into();
    }
    let flatten = match flatten_fields.first().map(|f| FlattenInfo::of(f)) {
        Some(Ok(fi)) => Some(fi),
        Some(Err(e)) => return e.to_compile_error().into(),
        None => None,
    };
    if flatten.is_some() && parsed.update.is_present() {
        return syn::Error::new_spanned(ident, "#[dao(flatten)] is not supported in update DAOs")
            .to_compile_error()
            .into();
    }
    if flatten.is_some() && parsed.logical_pointer.is_some() {
        return syn::Error::new_spanned(
            ident,
            "#[dao(flatten)] is not supported with logical_pointer",
        )
        .to_compile_error()
        .into();
    }

    // snake_case field idents, excluding #[sqlx(skip)] and #[dao(flatten)] fields.
    // These are used for struct field access: self.defined_on, self.id, etc.
    let field_names = field_idents(fields);

    // The Rust types of each field — independent of the field name.
    // AccessTokenId, AtTime, etc. Used only in TypedColumns for TypeId matching.
    let field_types = field_types(fields);

    // Iden variant idents derived from FIELD NAMES (not type names) via UpperCamelCase.
    // "id" → Id, "defined_on" → DefinedOn, regardless of whether the type is
    // AccessTokenId, AtTime, or anything else.
    let field_iden_variants: Vec<Ident> = field_names.iter().map(|f| iden_variant(f)).collect();

    // ---- sql_table ---------------------------------------------------------

    let sql_table = parsed
        .sql_table
        .unwrap_or_else(|| ident.to_string().to_lowercase());

    // ---- Iden enum ---------------------------------------------------------
    let table_iden = format_ident!("{}Iden", ident);
    let field_name_strs: Vec<String> = field_names.iter().map(|f| f.to_string()).collect();
    let iden_enum = match &flatten {
        None => quote! {
            #[derive(Debug, Clone, Copy, PartialEq, Eq, sea_query::Iden)]
            #vis enum #table_iden {
                #[iden = #sql_table]
                Table,
                #(#field_iden_variants,)*
            }
            impl std::str::FromStr for #table_iden {
                type Err = td_error::TdError;
                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    match s {
                        #sql_table => Ok(Self::Table),
                        #(#field_name_strs => Ok(Self::#field_iden_variants),)*
                        _ => Err(td_error::api_error!(
                            td_error::ApiError::InputError,
                            "Unknown column: {}",
                            s
                        )),
                    }
                }
            }
        },
        // The base's columns ride along in a `Base(..)` variant, rendered by
        // delegation via sea_query's `#[iden(flatten)]`.
        Some(fi) => {
            let base_iden = &fi.base_iden;
            quote! {
                #[derive(Debug, Clone, Copy, PartialEq, Eq, sea_query::Iden)]
                #vis enum #table_iden {
                    #[iden = #sql_table]
                    Table,
                    #[iden(flatten)]
                    Base(#base_iden),
                    #(#field_iden_variants,)*
                }
                impl std::str::FromStr for #table_iden {
                    type Err = td_error::TdError;
                    fn from_str(s: &str) -> Result<Self, Self::Err> {
                        match s {
                            #sql_table => Ok(Self::Table),
                            #(#field_name_strs => Ok(Self::#field_iden_variants),)*
                            _ => <#base_iden as std::str::FromStr>::from_str(s).map(Self::Base),
                        }
                    }
                }
            }
        }
    };

    // Resolve a `#[dao(...)]` column name: own field, else a column of the
    // flattened base, resolved through `FromStr` so it reaches any nesting
    // depth (chained flattens).
    let col_ref = |name: &str| -> proc_macro2::TokenStream {
        let variant = iden_variant_from_str(name);
        match &flatten {
            Some(_) if !field_name_strs.iter().any(|f| f == name) => {
                let expect = format!("`{name}` is a column of the flattened base");
                quote! {
                    <#table_iden as std::str::FromStr>::from_str(#name).expect(#expect)
                }
            }
            _ => quote! { #table_iden::#variant },
        }
    };

    // ---- order_by ----------------------------------------------------------
    let order_by_items = match &parsed.order_by {
        Some(o) => {
            let col = col_ref(&o.col);
            let dir = match o.dir.as_str() {
                "asc" => quote! { sea_query::Order::Asc },
                _ => quote! { sea_query::Order::Desc },
            };
            quote! { [(#col, #dir)] }
        }
        None => match &flatten {
            // Default ordering follows the base, like the first-field default
            // would on the merged struct.
            Some(fi) => {
                let base_ty = &fi.base_ty;
                quote! {
                    <#base_ty as ta_objects::Dao>::order_by()
                        .into_iter()
                        .map(|(c, o)| (#table_iden::Base(c), o))
                }
            }
            None => {
                let first = match field_iden_variants.first() {
                    Some(f) => f,
                    None => {
                        return syn::Error::new_spanned(
                            ident,
                            "DAO struct must have at least one non-skipped field",
                        )
                        .to_compile_error()
                        .into();
                    }
                };
                quote! { [(#table_iden::#first, sea_query::Order::Desc)] }
            }
        },
    };

    // ---- DaoCore (minimal) -------------------------------------------------
    let display_name = struct_name_to_display_name(&ident.to_string());
    let db_domain = match &parsed.db {
        Some(path) => quote! { #path },
        None => quote! { ta_objects::Apiserver },
    };
    let display_values_base = match &flatten {
        None => quote! { let mut __values: Vec<(String, String)> = Vec::new(); },
        Some(fi) => {
            let bf = &fi.field;
            quote! { let mut __values = self.#bf.column_display_values(); }
        }
    };
    let dao_core = quote! {
        impl #impl_generics ta_objects::DaoCore for #ident #ty_generics #where_clause {
            type Db = #db_domain;
            fn table() -> Self::Iden {
                #table_iden::Table
            }
            fn display_name() -> &'static str {
                #display_name
            }
            fn column_display_values(&self) -> Vec<(String, String)> {
                #[allow(unused_imports)]
                use ta_objects::SeaValue;
                #display_values_base
                __values.extend([
                    #(
                        (
                            sea_query::Iden::to_string(&#table_iden::#field_iden_variants),
                            self.#field_names.display_value(),
                        ),
                    )*
                ]);
                __values
            }
        }
    };

    // ---- TypedColumns ------------------------------------------------------
    // Base first: matches the merged-field order, so a domain type present in
    // both base and own fields resolves to the base column.
    let col_for_type_base = match &flatten {
        None => quote! {},
        Some(fi) => {
            let base_ty = &fi.base_ty;
            quote! {
                if let Some(c) = <#base_ty as ta_objects::TypedColumns>::col_for_type(type_id) {
                    return Some(#table_iden::Base(c));
                }
            }
        }
    };
    let typed_columns = quote! {
        impl #impl_generics ta_objects::TypedColumns for #ident #ty_generics #where_clause {
            type Iden = #table_iden;
            fn col_for_type(
                type_id: std::any::TypeId,
            ) -> Option<Self::Iden> {
                #col_for_type_base
                match type_id {
                    #(
                        t if t == std::any::TypeId::of::<#field_types>() =>
                            Some(#table_iden::#field_iden_variants),
                    )*
                    _ => None,
                }
            }
        }
    };

    // ---- HasColumn -----------------------------------------------------
    // With a flatten base the merged field list is only visible when the
    // base's `__dxo_fields_*` callback expands, so ban + impls delegate to
    // `__dao_has_columns` through it.
    let has_columns = match &flatten {
        None => {
            let mut seen: HashMap<String, &Ident> = HashMap::new();
            for (name, ty) in field_names.iter().copied().zip(field_types.iter().copied()) {
                if let Some(first) = seen.insert(quote!(#ty).to_string(), name) {
                    let ty_str = quote!(#ty).to_string();
                    return syn::Error::new_spanned(
                        name,
                        format!(
                            "duplicate column type `{ty_str}` on fields `{first}` and `{name}` \
                             — give each column a distinct newtype",
                        ),
                    )
                    .to_compile_error()
                    .into();
                }
            }
            quote! {
                #(
                    impl #impl_generics ta_objects::HasColumn<#field_types>
                        for #ident #ty_generics #where_clause
                    {
                        const COL: Self::Iden = #table_iden::#field_iden_variants;
                    }
                )*
            }
        }
        Some(fi) => {
            let base_cb = callback_ident(&fi.base_last_ident());
            let base_iden = &fi.base_iden;
            quote! {
                #base_cb! {
                    ta_objects::__dao_has_columns => { #ident, #table_iden } [#base_iden]
                    #( [] { #field_names : #field_types } )*
                }
            }
        }
    };

    // ---- fields callback -------------------------------------------------
    let fields_cb = if item.generics.params.is_empty() {
        let pairs: Vec<(&Ident, &Type)> = field_names
            .iter()
            .copied()
            .zip(field_types.iter().copied())
            .collect();
        let forward = flatten
            .as_ref()
            .map(|fi| (callback_ident(&fi.base_last_ident()), &fi.base_iden));
        fields_callback(ident, &pairs, forward)
    } else {
        quote! {}
    };

    // ---- DaoUpdate (update_values) -----------------------------------------
    //
    // For `Option<T>` fields (non-immutable): skip them when `None` so that
    // partial-update DTOs don't SET columns to NULL.
    let update_stmts: Vec<proc_macro2::TokenStream> = fields
        .iter()
        .filter(|f| !is_sqlx_skip(f))
        .map(|f| {
            let name = f.ident.as_ref().unwrap();
            let variant = iden_variant(name);
            if is_dao_flatten(f) {
                return quote! {
                    __values.extend(
                        ta_objects::DaoUpdate::update_values(&self.#name)
                            .into_iter()
                            .map(|(c, e)| (#table_iden::Base(c), e)),
                    );
                };
            }
            if is_dao_immutable(f) {
                // CASE WHEN col IS NULL THEN ? ELSE col END — set-once semantics.
                quote! {
                    __values.push((
                        #table_iden::#variant,
                        sea_query::Expr::case(
                            sea_query::Expr::col(#table_iden::#variant).is_null(),
                            self.#name.as_sea_value()
                        )
                        .finally(sea_query::Expr::col(#table_iden::#variant))
                        .into()
                    ));
                }
            } else if is_option_type(f) {
                // Only include the column when the value is Some.
                quote! {
                    if let Some(ref __v) = self.#name {
                        __values.push((
                            #table_iden::#variant,
                            __v.as_sea_value().into()
                        ));
                    }
                }
            } else {
                quote! {
                    __values.push((
                        #table_iden::#variant,
                        self.#name.as_sea_value().into()
                    ));
                }
            }
        })
        .collect();

    let dao_update_impl = quote! {
        impl #impl_generics ta_objects::DaoUpdate
            for #ident #ty_generics #where_clause {
                fn update_values(&self) -> impl IntoIterator<Item = (Self::Iden, sea_query::Expr)>
                {
                    #[allow(unused_imports)]
                use ta_objects::SeaValue;
                    use sea_query::ExprTrait;
                    let mut __values = Vec::new();
                    #(#update_stmts)*
                    __values
                }
        }
    };

    // ---- Update-only path: skip Dao, VersionedDao, States, Recursive -------
    if parsed.update.is_present() && parsed.versioned.is_some() {
        return syn::Error::new_spanned(
            ident,
            "`update` and `versioned` are incompatible — update DAOs cannot be versioned",
        )
        .to_compile_error()
        .into();
    }
    if parsed.update.is_present() {
        return quote! {
            #iden_enum
            #typed_columns
            #has_columns
            #fields_cb
            #dao_core
            #dao_update_impl
        }
        .into();
    }

    // ---- Dao: column metadata + read/insert capability for all DAOs ------
    // With a flatten field, base columns/values come first, own fields after —
    // the same order the merged struct would have.
    let (columns_items, insert_items) = match &flatten {
        None => (
            quote! { [#(#table_iden::#field_iden_variants),*] },
            quote! { [#(self.#field_names.as_sea_value().into()),*] },
        ),
        Some(fi) => {
            let base_ty = &fi.base_ty;
            let bf = &fi.field;
            (
                quote! {
                    <#base_ty as ta_objects::Dao>::columns()
                        .into_iter()
                        .map(#table_iden::Base)
                        .chain([#(#table_iden::#field_iden_variants),*])
                },
                quote! {
                    ta_objects::Dao::insert_values(&self.#bf)
                        .into_iter()
                        .chain([#(self.#field_names.as_sea_value().into()),*])
                },
            )
        }
    };
    let dao_read = quote! {
        impl #impl_generics ta_objects::Dao for #ident #ty_generics #where_clause {
            fn columns() -> impl IntoIterator<Item = Self::Iden>
            {
                #columns_items
            }
            fn insert_values(&self) -> impl IntoIterator<Item = sea_query::Expr>
            {
                #[allow(unused_imports)]
                use ta_objects::SeaValue;
                #insert_items
            }
            fn order_by() -> impl IntoIterator<Item = (Self::Iden, sea_query::Order)>
            {
                #order_by_items
            }
        }
    };

    // ---- LiveDao marker (opt out via `#[dao(event_log, ...)]`). See the
    // `LiveDao` trait doc for the contract and the insert-once footgun.
    let live_dao_marker = if parsed.event_log.is_present() {
        quote! {}
    } else {
        quote! {
            impl #impl_generics ta_objects::LiveDao for #ident #ty_generics #where_clause {}
        }
    };

    // ---- KeyedDao (emitted from `#[dao(key(cols...))]`). Key = the declared
    // columns; values = every other DAO column, derived so the update set
    // can't drift when columns are added. See the `KeyedDao` trait doc.
    let is_keyed = !parsed.key.is_empty();
    let is_versioned = parsed.versioned.is_some();
    let is_recursive = parsed.recursive.is_some();

    let keyed_dao_marker = if parsed.key.is_empty() {
        quote! {}
    } else {
        let mut key_names: Vec<Ident> = Vec::new();
        for path in parsed.key.iter() {
            let Some(name) = path.get_ident() else {
                return syn::Error::new_spanned(path, "key: expected a plain column name")
                    .to_compile_error()
                    .into();
            };
            if !field_names.contains(&name) && flatten.is_none() {
                return syn::Error::new_spanned(
                    path,
                    format!("key: `{name}` is not a column of this DAO"),
                )
                .to_compile_error()
                .into();
            }
            key_names.push(name.clone());
        }
        let key_cols: Vec<proc_macro2::TokenStream> =
            key_names.iter().map(|n| col_ref(&n.to_string())).collect();
        let value_cols = match &flatten {
            // Runtime filter: base columns aren't enumerable at macro time.
            Some(_) => quote! {
                let __keys = [#(#key_cols),*];
                <Self as ta_objects::Dao>::columns()
                    .into_iter()
                    .filter(move |c| !__keys.contains(c))
            },
            None => {
                let key_variants: Vec<Ident> = key_names.iter().map(iden_variant).collect();
                let value_variants: Vec<&Ident> = field_iden_variants
                    .iter()
                    .filter(|v| !key_variants.contains(v))
                    .collect();
                quote! { [#(#table_iden::#value_variants),*] }
            }
        };
        quote! {
            impl #impl_generics ta_objects::KeyedDao for #ident #ty_generics #where_clause {
                fn key_columns() -> impl IntoIterator<Item = Self::Iden> {
                    [#(#key_cols),*]
                }
                fn value_columns() -> impl IntoIterator<Item = Self::Iden> {
                    #value_cols
                }
            }
        }
    };

    // ---- Access trait: DaoUpdate, VersionedDao, or both --------------------
    let access_trait = match (&parsed.versioned, parsed.data_version.is_present()) {
        (None, false) => dao_update_impl,
        (Some(v), is_data_version) => {
            let pk_str = match &v.pk {
                Some(s) => s,
                None => {
                    return syn::Error::new_spanned(ident, "versioned: `pk` is required")
                        .to_compile_error()
                        .into();
                }
            };
            let version_by_str = match &v.version_by {
                Some(s) => s,
                None => {
                    return syn::Error::new_spanned(ident, "versioned: `version_by` is required")
                        .to_compile_error()
                        .into();
                }
            };
            let partition_by_str = match &v.partition_by {
                Some(s) => s,
                None => {
                    return syn::Error::new_spanned(ident, "versioned: `partition_by` is required")
                        .to_compile_error()
                        .into();
                }
            };
            let pk_col = col_ref(pk_str);
            let version_col = col_ref(version_by_str);
            let partition_col = col_ref(partition_by_str);
            // Structural column types: own field, else the flattened base's
            // corresponding VersionedDao associated type — correct because a
            // base-resolved structural column re-declares the base's own.
            let is_own = |name: &str| field_name_strs.iter().any(|f| f == name);
            let base_ty = flatten.as_ref().map(|fi| &fi.base_ty);
            let pk_type = match (is_own(pk_str), base_ty) {
                (false, Some(b)) => quote! { <#b as ta_objects::VersionedDao>::Pk },
                _ => {
                    let t = require_field!(fields, pk_str, "versioned", ident);
                    quote! { #t }
                }
            };
            let version_type = match (is_own(version_by_str), base_ty) {
                (false, Some(b)) => quote! { <#b as ta_objects::VersionedDao>::VersionOrder },
                _ => {
                    let t = require_field!(fields, version_by_str, "versioned", ident);
                    quote! { #t }
                }
            };
            let partition_type = match (is_own(partition_by_str), base_ty) {
                (false, Some(b)) => quote! { <#b as ta_objects::VersionedDao>::Partition },
                _ => {
                    let t = require_field!(fields, partition_by_str, "versioned", ident);
                    quote! { #t }
                }
            };

            let current_impl = v.current.as_ref().map(|current_table| {
                quote! {
                    impl #impl_generics ta_objects::CurrentDao
                        for #ident #ty_generics #where_clause
                    {
                        fn current_table_name() -> &'static str {
                            #current_table
                        }
                    }
                }
            });

            let versioned_impl = quote! {
                impl #impl_generics ta_objects::VersionedDao
                    for #ident #ty_generics #where_clause
                {
                    type Pk = #pk_type;
                    fn pk_col() -> Self::Iden {
                        #pk_col
                    }

                    type Partition = #partition_type;
                    fn partition_col() -> Self::Iden {
                        #partition_col
                    }

                    type VersionOrder = #version_type;
                    fn version_order_col() -> Self::Iden {
                        #version_col
                    }
                }
                #current_impl
            };

            if is_data_version {
                let data_version_impl = quote! {
                    impl #impl_generics ta_objects::DataVersionDao
                        for #ident #ty_generics #where_clause {}
                };
                quote! { #versioned_impl #dao_update_impl #data_version_impl }
            } else {
                versioned_impl
            }
        }
        (None, true) => {
            return syn::Error::new_spanned(
                ident,
                "data_version requires versioned(...) to be set",
            )
            .to_compile_error()
            .into();
        }
    };

    // ---- Recursive ---------------------------------------------------------
    let recursive = match parsed.recursive {
        None => quote! {},
        Some(r) => {
            let is_own = |name: &str| field_name_strs.iter().any(|f| f == name);
            // Key is the down column's domain type — the reference into the
            // traversal's reference DAO. The up column may be a distinct
            // newtype (it must share the SQL representation).
            let key_type = match (&flatten, is_own(&r.down)) {
                (Some(fi), false) => {
                    let base_ty = &fi.base_ty;
                    quote! { <#base_ty as ta_objects::Recursive>::Key }
                }
                _ => {
                    let down_type = require_field!(fields, &r.down, "recursive", ident);
                    quote! { #down_type }
                }
            };
            let up_col = col_ref(&r.up);
            let down_col = col_ref(&r.down);
            quote! {
                impl #impl_generics ta_objects::Recursive
                    for #ident #ty_generics #where_clause
                {
                    type Key = #key_type;
                    fn recurse_up() -> Self::Iden {
                        #up_col
                    }
                    fn recurse_down() -> Self::Iden {
                        #down_col
                    }
                }
            }
        }
    };

    // ---- LogicalPointerDao impl (emitted from `logical_pointer(...)`) ----
    let logical_pointer_impl = match &parsed.logical_pointer {
        None => quote! {},
        Some(fp) => {
            let logical_id_field = format_ident!("{}", fp.logical_id_col);
            let concrete_id_field = format_ident!("{}", fp.concrete_id_col);
            let logical_id_variant = iden_variant_from_str(&fp.logical_id_col);
            let concrete_id_variant = iden_variant_from_str(&fp.concrete_id_col);
            let status_variant = iden_variant_from_str("status");
            let logical_id_type =
                require_field!(fields, &fp.logical_id_col, "logical_pointer", ident);
            let concrete_id_type =
                require_field!(fields, &fp.concrete_id_col, "logical_pointer", ident);
            let identity_type = &fp.identity;
            let kind = &fp.kind;
            quote! {
                impl #impl_generics td_objects::dxo::logical::LogicalPointerDao
                    for #ident #ty_generics #where_clause
                {
                    type LogicalId = #logical_id_type;
                    type ConcreteId = #concrete_id_type;
                    type IdentityDB = #identity_type;
                    const KIND: &'static str = #kind;

                    fn logical_id_col() -> Self::Iden {
                        #table_iden::#logical_id_variant
                    }
                    fn concrete_id_col() -> Self::Iden {
                        #table_iden::#concrete_id_variant
                    }
                    fn status_col() -> Self::Iden {
                        #table_iden::#status_variant
                    }

                    fn status(&self) -> td_objects::types::basic::LogicalPointerStatus {
                        self.status.clone()
                    }
                    fn logical_id(&self) -> &Self::LogicalId {
                        &self.#logical_id_field
                    }
                    fn concrete_id(&self) -> Option<&Self::ConcreteId> {
                        self.#concrete_id_field.as_ref()
                    }
                    fn build_event(
                        project_id: td_objects::types::basic::ProjectId,
                        logical_id: &Self::LogicalId,
                        status: td_objects::types::basic::LogicalPointerStatus,
                        concrete_id: Option<Self::ConcreteId>,
                        set_on: td_objects::types::basic::AtTime,
                        set_by_id: td_objects::types::basic::UserId,
                        reason: td_objects::types::basic::LogicalPointerReason,
                    ) -> Result<Self, td_error::TdError> {
                        Ok(Self {
                            id: Default::default(),
                            project_id,
                            #logical_id_field: logical_id.clone(),
                            status,
                            #concrete_id_field: concrete_id,
                            set_on,
                            set_by_id,
                            reason,
                        })
                    }
                    fn initial_mint_for(
                        identity: &Self::IdentityDB,
                    ) -> Result<Self, td_error::TdError> {
                        Self::build_event(
                            identity.project_id.clone(),
                            &identity.id,
                            td_objects::types::basic::LogicalPointerStatus::Pending,
                            None,
                            td_objects::types::basic::AtTime::try_from(identity.triggered_on)?,
                            identity.triggered_by_id,
                            td_objects::types::basic::LogicalPointerReason::InitialMint,
                        )
                    }
                }
            }
        }
    };

    // Forces the column references a flatten base resolves at runtime, so a
    // typo panics in the registry-walking test, not on first live use.
    let mut validate_stmts = vec![quote! {
        let _ = <#ident as ta_objects::Dao>::order_by().into_iter().count();
    }];
    if is_keyed {
        validate_stmts.push(quote! {
            let _ = <#ident as ta_objects::KeyedDao>::key_columns().into_iter().count();
            let _ = <#ident as ta_objects::KeyedDao>::value_columns().into_iter().count();
        });
    }
    if is_versioned {
        validate_stmts.push(quote! {
            let _ = <#ident as ta_objects::VersionedDao>::pk_col();
            let _ = <#ident as ta_objects::VersionedDao>::partition_col();
            let _ = <#ident as ta_objects::VersionedDao>::version_order_col();
        });
    }
    if is_recursive {
        validate_stmts.push(quote! {
            let _ = <#ident as ta_objects::Recursive>::recurse_up();
            let _ = <#ident as ta_objects::Recursive>::recurse_down();
        });
    }

    // Generic DAOs have no single concrete type to register against;
    // `cfg(not(test))` keeps a crate's own test-fixture DAOs out of the
    // registry while dependency crates (compiled with cfg(test) off) register.
    let dao_registration = if item.generics.params.is_empty() {
        quote! {
            #[cfg(not(test))]
            ta_objects::inventory::submit! {
                ta_objects::RegisteredDao {
                    describe: ta_objects::dao_projection::<#ident>,
                    validate: || { #(#validate_stmts)* },
                }
            }
        }
    } else {
        quote! {}
    };

    quote! {
        #iden_enum
        #typed_columns
        #has_columns
        #fields_cb
        #dao_core
        #dao_read
        #live_dao_marker
        #keyed_dao_marker
        #access_trait
        #recursive
        #logical_pointer_impl
        #dao_registration
    }
    .into()
}

// ============================================================================
// Helpers
// ============================================================================

/// Convert a snake_case field name string to the UpperCamelCase Iden variant
/// ident that sea_query::enum_def generates.
pub fn iden_variant_from_str(field_name: &str) -> Ident {
    format_ident!("{}", field_name.to_upper_camel_case())
}

/// Same but from an existing Ident.
pub fn iden_variant(field_ident: &Ident) -> Ident {
    iden_variant_from_str(&field_ident.to_string())
}

/// All non-skipped field idents (snake_case, as written in the struct).
/// A `#[dao(flatten)]` field is not a column of its own — excluded here,
/// its base's columns are delegated to instead.
pub fn field_idents(fields: &Fields) -> Vec<&Ident> {
    fields
        .iter()
        .filter(|f| !is_sqlx_skip(f) && !is_dao_flatten(f))
        .filter_map(|f| f.ident.as_ref())
        .collect()
}

/// All non-skipped field types.
pub fn field_types(fields: &Fields) -> Vec<&Type> {
    fields
        .iter()
        .filter(|f| !is_sqlx_skip(f) && !is_dao_flatten(f))
        .map(|f| &f.ty)
        .collect()
}

/// The `#[dao(flatten)]` field: its ident, base DAO type, and the base's
/// Iden enum path (`<Base>Iden`, keeping any module prefix).
struct FlattenInfo {
    field: Ident,
    base_ty: syn::Path,
    base_iden: syn::Path,
}

impl FlattenInfo {
    /// The base type's bare name — the `__dxo_fields_*` callback ident stem.
    fn base_last_ident(&self) -> Ident {
        self.base_ty
            .segments
            .last()
            .expect("non-empty type path")
            .ident
            .clone()
    }

    fn of(f: &syn::Field) -> syn::Result<FlattenInfo> {
        let field = f
            .ident
            .clone()
            .ok_or_else(|| syn::Error::new_spanned(f, "#[dao(flatten)] requires a named field"))?;
        let Type::Path(tp) = &f.ty else {
            return Err(syn::Error::new_spanned(
                &f.ty,
                "#[dao(flatten)] field must be a plain DAO struct type",
            ));
        };
        let base_ty = tp.path.clone();
        let mut base_iden = base_ty.clone();
        let last = base_iden.segments.last_mut().expect("non-empty type path");
        last.ident = format_ident!("{}Iden", last.ident);
        Ok(FlattenInfo {
            field,
            base_ty,
            base_iden,
        })
    }
}

fn is_dao_flatten(f: &syn::Field) -> bool {
    f.attrs.iter().any(|a| {
        if !a.path().is_ident("dao") {
            return false;
        }
        a.parse_args::<Ident>()
            .map(|ident| ident == "flatten")
            .unwrap_or(false)
    })
}

/// Resolve the type of a named field, unwrapping `Option<T>` → `T`.
/// NULL comparisons in SQL are always false, so Option and T are equivalent
/// for column matching purposes.
/// Returns `None` if the field is not found.
pub fn field_type_by_name<'a>(fields: &'a Fields, name: &str) -> Option<&'a Type> {
    let ty = fields
        .iter()
        .find_map(|f| f.ident.as_ref().filter(|i| *i == name).map(|_| &f.ty))?;
    Some(option_inner_type(ty).unwrap_or(ty))
}

/// The `T` of an `Option<T>` type, else `None`.
pub fn option_inner_type(ty: &Type) -> Option<&Type> {
    if let Type::Path(tp) = ty
        && let Some(seg) = tp.path.segments.last()
        && seg.ident == "Option"
        && let syn::PathArguments::AngleBracketed(args) = &seg.arguments
        && let Some(syn::GenericArgument::Type(inner)) = args.args.first()
    {
        return Some(inner);
    }
    None
}

fn is_sqlx_skip(f: &syn::Field) -> bool {
    f.attrs.iter().any(|a| {
        if !a.path().is_ident("sqlx") {
            return false;
        }
        // Parse #[sqlx(skip)] — the inner token is just the ident "skip"
        a.parse_args::<Ident>()
            .map(|ident| ident == "skip")
            .unwrap_or(false)
    })
}

/// Convert a DAO struct name to a human-readable display name.
/// Strips from the first `DB` occurrence onwards, then inserts spaces
/// before uppercase-to-lowercase transitions (CamelCase word boundaries).
/// `UserDB` → `"User"`, `PhysicalFunctionRunDB` → `"Function Run"`,
/// `FunctionDBWithNames` → `"Function"`.
fn struct_name_to_display_name(name: &str) -> String {
    let name = match name.find("DB") {
        Some(pos) if pos > 0 => &name[..pos],
        _ => name,
    };
    let mut result = String::new();
    let chars: Vec<char> = name.chars().collect();
    for (i, &c) in chars.iter().enumerate() {
        if i > 0 && c.is_uppercase() {
            // Only add a space at a real word boundary:
            // uppercase followed by lowercase (e.g. "Run" in "PhysicalFunctionRun")
            // or preceded by lowercase (e.g. "R" after "function")
            let prev_lower = chars[i - 1].is_lowercase();
            let next_lower = chars.get(i + 1).is_some_and(|c| c.is_lowercase());
            if prev_lower || next_lower {
                result.push(' ');
            }
        }
        result.push(c);
    }
    result
}

fn is_dao_immutable(f: &syn::Field) -> bool {
    f.attrs.iter().any(|a| {
        if !a.path().is_ident("dao") {
            return false;
        }
        a.parse_args::<Ident>()
            .map(|ident| ident == "immutable")
            .unwrap_or(false)
    })
}

/// Returns `true` when the field type is `Option<…>`.
fn is_option_type(f: &syn::Field) -> bool {
    if let Type::Path(tp) = &f.ty
        && let Some(seg) = tp.path.segments.last()
    {
        return seg.ident == "Option";
    }
    false
}
