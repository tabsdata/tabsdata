//
// Copyright 2025 Tabs Data Inc.
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

use crate::type_builder::{parse_input_item_struct, td_type};
use darling::util::{Flag, PathList};
use darling::{FromAttributes, FromDeriveInput, FromMeta};
use heck::{ToSnakeCase, ToUpperCamelCase};
use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::{ToTokens, format_ident, quote};
use std::collections::HashMap;
use syn::{DeriveInput, Expr, Fields, ItemStruct, Type, parse_macro_input};

// ---- Darling argument structs ----------------------------------------------

#[derive(Debug, FromDeriveInput, FromAttributes)]
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
    #[darling(default)]
    states: HashMap<Ident, Expr>,
}

impl ToTokens for DaoArguments {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let mut args = Vec::new();
        if self.update.is_present() {
            args.push(quote! { update });
        }
        if let Some(sql_table) = &self.sql_table {
            args.push(quote! { sql_table = #sql_table });
        }
        if let Some(db) = &self.db {
            args.push(quote! { db = #db });
        }
        if let Some(order_by) = &self.order_by {
            let OrderByArguments { col, dir } = order_by;
            args.push(quote! { order_by(col = #col, dir = #dir) });
        }
        if let Some(versioned) = &self.versioned {
            let mut v_args = Vec::new();
            if let Some(pk) = &versioned.pk {
                v_args.push(quote! { pk = #pk });
            }
            if let Some(version_by) = &versioned.version_by {
                v_args.push(quote! { version_by = #version_by });
            }
            if let Some(partition_by) = &versioned.partition_by {
                v_args.push(quote! { partition_by = #partition_by });
            }
            if let Some(current) = &versioned.current {
                v_args.push(quote! { current = #current });
            }
            args.push(quote! { versioned(#(#v_args),*) });
        }
        if let Some(recursive) = &self.recursive {
            let up = &recursive.up;
            let down = &recursive.down;
            args.push(quote! { recursive(up = #up, down = #down) });
        }
        if self.data_version.is_present() {
            args.push(quote! { data_version });
        }
        if self.event_log.is_present() {
            args.push(quote! { event_log });
        }
        if let Some(fp) = &self.logical_pointer {
            let logical_id_col = &fp.logical_id_col;
            let concrete_id_col = &fp.concrete_id_col;
            let identity = &fp.identity;
            let kind = &fp.kind;
            args.push(quote! {
                logical_pointer(
                    logical_id_col = #logical_id_col,
                    concrete_id_col = #concrete_id_col,
                    identity = #identity,
                    kind = #kind
                )
            });
        }
        if !self.key.is_empty() {
            let paths = self.key.iter();
            args.push(quote! { key(#(#paths),*) });
        }
        let states = self
            .states
            .iter()
            .map(|(name, value)| {
                quote! { #name = #value }
            })
            .collect::<Vec<_>>();
        if !states.is_empty() {
            args.push(quote! { states(#(#states),*) });
        }

        tokens.extend(quote! {
            #[dao(#(#args,)*)]
        });
    }
}

impl DaoArguments {
    pub fn override_with(self, other: DaoArguments) -> DaoArguments {
        DaoArguments {
            update: if other.update.is_present() {
                other.update
            } else {
                self.update
            },
            sql_table: other.sql_table.or(self.sql_table),
            db: other.db.or(self.db),
            order_by: other.order_by.or(self.order_by),
            versioned: match (self.versioned, other.versioned) {
                (Some(base), Some(child)) => Some(base.override_with(child)),
                (base, child) => child.or(base),
            },
            recursive: other.recursive.or(self.recursive),
            data_version: if other.data_version.is_present() {
                other.data_version
            } else {
                self.data_version
            },
            event_log: if other.event_log.is_present() {
                other.event_log
            } else {
                self.event_log
            },
            logical_pointer: other.logical_pointer.or(self.logical_pointer),
            key: if other.key.is_empty() {
                self.key
            } else {
                other.key
            },
            states: other.states.into_iter().chain(self.states).collect(),
        }
    }
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

impl VersionedArguments {
    fn override_with(self, other: VersionedArguments) -> VersionedArguments {
        VersionedArguments {
            pk: other.pk.or(self.pk),
            version_by: other.version_by.or(self.version_by),
            partition_by: other.partition_by.or(self.partition_by),
            current: other.current.or(self.current),
        }
    }
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
    let input = parse_macro_input!(item as ItemStruct);
    let is_update = is_dao_update_attr(&input);
    if is_update {
        quote! {
            #[derive(Debug, Clone, ta_objects::DaoType, derive_builder::Builder)]
            #[builder(try_setter, setter(into))]
            #input
        }
        .into()
    } else {
        quote! {
            #[derive(Debug, Clone, ta_objects::DaoType, derive_builder::Builder, sqlx::FromRow)]
            #[builder(try_setter, setter(into))]
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

    // snake_case field idents, excluding #[sqlx(skip)] fields.
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
    let iden_enum = quote! {
        #[derive(Debug, Clone, Copy, sea_query::Iden)]
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
    };

    // ---- order_by ----------------------------------------------------------
    let order_by = match parsed.order_by {
        Some(o) => o,
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
            OrderByArguments {
                col: first.to_string(),
                dir: default_order_dir(),
            }
        }
    };
    // The order_by col is a field name (snake_case) — convert to Iden variant.
    let order_col_variant = iden_variant_from_str(&order_by.col);
    let order_dir = match order_by.dir.as_str() {
        "asc" => quote! { sea_query::Order::Asc },
        _ => quote! { sea_query::Order::Desc },
    };

    // ---- DaoCore (minimal) -------------------------------------------------
    let display_name = struct_name_to_display_name(&ident.to_string());
    let db_domain = match &parsed.db {
        Some(path) => quote! { #path },
        None => quote! { ta_objects::Apiserver },
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
                use ta_objects::SeaValue;
                vec![
                    #(
                        (
                            sea_query::Iden::to_string(&#table_iden::#field_iden_variants),
                            self.#field_names.display_value(),
                        ),
                    )*
                ]
            }
        }
    };

    // ---- TypedColumns ------------------------------------------------------
    let typed_columns = quote! {
        impl #impl_generics ta_objects::TypedColumns for #ident #ty_generics #where_clause {
            type Iden = #table_iden;
            fn col_for_type(
                type_id: std::any::TypeId,
            ) -> Option<Self::Iden> {
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
        let td_type = td_type(&input, &item);
        return quote! {
            #iden_enum
            #typed_columns
            #dao_core
            #dao_update_impl
            #td_type
        }
        .into();
    }

    // ---- Dao: column metadata + read/insert capability for all DAOs ------
    let dao_read = quote! {
        impl #impl_generics ta_objects::Dao for #ident #ty_generics #where_clause {
            fn columns() -> impl IntoIterator<Item = Self::Iden>
            {
                [#(#table_iden::#field_iden_variants),*]
            }
            fn insert_values(&self) -> impl IntoIterator<Item = sea_query::Expr>
            {
                use ta_objects::SeaValue;
                [#(self.#field_names.as_sea_value().into()),*]
            }
            fn order_by() -> impl IntoIterator<Item = (Self::Iden, sea_query::Order)>
            {
                [(#table_iden::#order_col_variant, #order_dir)]
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
    let keyed_dao_marker = if parsed.key.is_empty() {
        quote! {}
    } else {
        let mut key_variants: Vec<Ident> = Vec::new();
        for path in parsed.key.iter() {
            let Some(name) = path.get_ident() else {
                return syn::Error::new_spanned(path, "key: expected a plain column name")
                    .to_compile_error()
                    .into();
            };
            if !field_names.contains(&name) {
                return syn::Error::new_spanned(
                    path,
                    format!("key: `{name}` is not a column of this DAO"),
                )
                .to_compile_error()
                .into();
            }
            key_variants.push(iden_variant(name));
        }
        let value_variants: Vec<Ident> = field_iden_variants
            .iter()
            .filter(|&v| !key_variants.contains(v))
            .cloned()
            .collect();
        quote! {
            impl #impl_generics ta_objects::KeyedDao for #ident #ty_generics #where_clause {
                fn key_columns() -> impl IntoIterator<Item = Self::Iden> {
                    [#(#table_iden::#key_variants),*]
                }
                fn value_columns() -> impl IntoIterator<Item = Self::Iden> {
                    [#(#table_iden::#value_variants),*]
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
            let version_variant = iden_variant_from_str(version_by_str);
            let partition_variant = iden_variant_from_str(partition_by_str);
            let pk_variant = iden_variant_from_str(pk_str);
            let pk_type = require_field!(fields, pk_str, "versioned", ident);
            let version_type = require_field!(fields, version_by_str, "versioned", ident);
            let partition_type = require_field!(fields, partition_by_str, "versioned", ident);

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
                        #table_iden::#pk_variant
                    }

                    type Partition = #partition_type;
                    fn partition_col() -> Self::Iden {
                        #table_iden::#partition_variant
                    }

                    type VersionOrder = #version_type;
                    fn version_order_col() -> Self::Iden {
                        #table_iden::#version_variant
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

    // ---- State filter values ---------------------------------------------------
    // Each `versioned(...)` or `states(...)` DAO gets a `pub fn all() -> StateFilter<Self>`
    // and one `pub fn <state>() -> StateFilter<Self>` per user-defined state. Each method
    // pre-resolves the SQL condition for its state, packaged in a phantom-D wrapper that
    // the query wrappers' `.state(filter)` step accepts (see `ta_objects::StateFilter`).
    //
    // The legacy `pub const X: u8 = N` + `impl States<N> for D` are also emitted for the
    // tower `By::<...>::method::<{X::N}, X>` handlers that are scheduled for removal.
    // New code should reach for the value methods.
    let states = if parsed.versioned.is_some() || !parsed.states.is_empty() {
        let all_state = quote! {
            impl #impl_generics #ident #ty_generics #where_clause {
                /// State filter that admits every row (no extra WHERE).
                #[allow(non_snake_case)]
                pub fn all() -> ta_objects::StateFilter<Self> {
                    ta_objects::StateFilter::new(sea_query::Condition::all())
                }
                #[doc(hidden)]
                #[allow(non_upper_case_globals)]
                pub const All: u8 = 0;
            }
            #[allow(clippy::unused_unit)]
            impl #impl_generics ta_objects::States<0> for #ident #ty_generics #where_clause {
                fn state() -> impl ta_objects::sql_condition::SqlCondition {
                    ()
                }
            }
        };
        // Sort states by name for deterministic ordering across compilations.
        let mut sorted_states: Vec<_> = parsed.states.iter().collect();
        sorted_states.sort_by_key(|(name, _)| name.to_string());
        let user_states = sorted_states
            .into_iter()
            .enumerate()
            .map(|(i, (name, value))| {
                let i = i as u8 + 1;
                let snake_name = format_ident!("{}", name.to_string().to_snake_case());
                quote! {
                    impl #impl_generics #ident #ty_generics #where_clause {
                        /// Macro-generated state filter; see the `states(...)` DAO config.
                        pub fn #snake_name() -> ta_objects::StateFilter<Self> {
                            let resolved = ta_objects::sql_condition::resolve_condition::<Self>(
                                #value
                            ).expect("static state condition must resolve");
                            ta_objects::StateFilter::new(
                                sea_query::Condition::all().add(resolved)
                            )
                        }
                        #[doc(hidden)]
                        #[allow(non_upper_case_globals)]
                        pub const #name: u8 = #i;
                    }
                    impl #impl_generics ta_objects::States<#i>
                        for #ident #ty_generics #where_clause
                    {
                        fn state() -> impl ta_objects::sql_condition::SqlCondition {
                            #value
                        }
                    }
                }
            });
        quote! { #all_state #(#user_states)* }
    } else {
        quote! {}
    };

    // ---- Recursive ---------------------------------------------------------
    let recursive = match parsed.recursive {
        None => quote! {},
        Some(r) => {
            let up_type = require_field!(fields, &r.up, "recursive", ident);
            let down_type = require_field!(fields, &r.down, "recursive", ident);
            if up_type != down_type {
                return syn::Error::new_spanned(
                    ident,
                    "Recursive: up and down columns must have the same type",
                )
                .to_compile_error()
                .into();
            }
            let up_variant = iden_variant_from_str(&r.up);
            let down_variant = iden_variant_from_str(&r.down);
            quote! {
                impl #impl_generics ta_objects::Recursive
                    for #ident #ty_generics #where_clause
                {
                    type Key = #up_type;
                    fn recurse_up() -> Self::Iden {
                        #table_iden::#up_variant
                    }
                    fn recurse_down() -> Self::Iden {
                        #table_iden::#down_variant
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
                        use ta_objects::Buildable;
                        Ok(Self::builder()
                            .project_id(project_id)
                            .#logical_id_field(logical_id.clone())
                            .status(status)
                            .#concrete_id_field(concrete_id)
                            .set_on(set_on)
                            .set_by_id(set_by_id)
                            .reason(reason)
                            .build()?)
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

    let td_type = td_type(&input, &item);

    // Generic DAOs have no single concrete type to register against;
    // `cfg(not(test))` keeps a crate's own test-fixture DAOs out of the
    // registry while dependency crates (compiled with cfg(test) off) register.
    let dao_registration = if item.generics.params.is_empty() {
        quote! {
            #[cfg(not(test))]
            ta_objects::inventory::submit! {
                ta_objects::RegisteredDao { describe: ta_objects::dao_projection::<#ident> }
            }
        }
    } else {
        quote! {}
    };

    quote! {
        #iden_enum
        #typed_columns
        #dao_core
        #dao_read
        #live_dao_marker
        #keyed_dao_marker
        #access_trait
        #states
        #recursive
        #logical_pointer_impl
        #td_type
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
pub fn field_idents(fields: &Fields) -> Vec<&Ident> {
    fields
        .iter()
        .filter(|f| !is_sqlx_skip(f))
        .filter_map(|f| f.ident.as_ref())
        .collect()
}

/// All non-skipped field types.
pub fn field_types(fields: &Fields) -> Vec<&Type> {
    fields
        .iter()
        .filter(|f| !is_sqlx_skip(f))
        .map(|f| &f.ty)
        .collect()
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
